# cq-dashboard

An optional web dashboard for [cq](https://github.com/gnikyt/cq).

cq deliberately holds nothing durably: it is an execution engine, and a restart
takes its state with it. This module is where the memory lives. It listens to
cq's lifecycle hooks, writes them to SQLite, and serves views over the result.
The core library keeps its zero-dependency promise because none of this lives
inside it.

> Status: read-only by default; write controls and the JSON endpoints are
> opt-in. Several processes may share one store.

## What it shows

| View | Source | Survives restart |
| --- | --- | --- |
| Live queues: workers, buffer, tallies, pause/stop state | `Stats()`, read per request | No, and shouldn't... it is a live view |
| Pending: accepted jobs not yet finished | `Queue.Submissions()`, read per request | No... unstarted work lives only in memory |
| Schedules: kind, next fire, attempts, last error | `Scheduler.Describe()`, read per request | No |
| Job history: state, wait/execution time, errors, progress | SQLite, written by the sink | Yes |
| Job detail: retry attempts and reschedule lineage | SQLite | Yes |
| Failures: everything that did not complete | SQLite | Yes |

## Boundaries

This is an observability tool for **what cq executed**. It is not a queue of
record, not a transport, and not a dead-letter queue. Three consequences worth
knowing before you rely on it:

**Pending understates real backlog when something feeds cq.** If jobs arrive
from SQS, a database poller, or any other transport, cq only knows about the
messages already pulled into memory. Ten thousand waiting messages are
invisible here. The dashboard answers "what is this process doing", not "how
much work exists".

**Failures are execution outcomes, not a dead-letter queue.** A job that
exhausts its transport's retries lands in that transport's dead-letter queue,
which remains the record of work still needing redrive. The failures view shows
the cq executions that led there... the attempts, the errors, the timings.

**Retrying belongs to whatever owns the work.** With a transport in front, that
is the transport (redrive from its dead-letter queue). For in-memory-only cq,
`StopDrain` hands unstarted jobs back to your code at shutdown so you can
persist and resubmit them.

What the dashboard has that the layer above does not: per-attempt retry
history, wait time versus execution time, live progress from running jobs,
reschedule and release lineage collapsed into one logical job, and which jobs a
shutdown abandoned.

## Usage

```bash
go get github.com/gnikyt/cq-dashboard
```

Four pieces: a store, a sink that feeds it, your queue with the sink's hooks
attached, and an HTTP handler.

```go
// 1. Where history lives. Put this on a persistent volume, not /tmp.
st, err := sqlite.Open("/var/lib/myapp/cq-dashboard.db")
if err != nil {
	log.Fatal(err)
}
defer st.Close()

// 2. The recorder. Start it before the queue, so no event is missed, and give
//    it a retention policy... history grows without bound otherwise.
sk := sink.New(st,
	sink.WithRetention(24*time.Hour, time.Hour, store.StateCompleted),
	sink.WithErrorHandler(func(err error) { log.Printf("cq-dashboard sink: %v", err) }),
)
interrupted, err := sk.Start(context.Background())
if err != nil {
	log.Fatal(err)
}
if interrupted > 0 {
	log.Printf("%d jobs were left running by a previous process", interrupted)
}

// 3. Your queue, unchanged except for two options.
queue := cq.NewQueue(2, 6, 128,
	cq.WithQueueName("default"),                // Names the queue in the UI.
	cq.WithHooks(sk.Hooks()),                   // Required: the event feed.
	cq.WithMiddleware(sk.ProgressMiddleware("default")), // Optional: progress.
)
queue.Start()

// 4. The UI, mounted anywhere. The prefix and the mount path must agree, and
//    the mount needs its trailing slash.
handler, err := web.New("/cq", st, sk,
	web.WithQueues(queue),
	web.WithSchedulers(scheduler), // Optional: lists recurring work.
)
if err != nil {
	log.Fatal(err)
}
http.Handle("/cq/", handler)
```

Already have a `QueueManager`? Register each queue you want visible:

```go
web.WithQueues(orders, emails, reports)
```

### Shutting down

Close the sink last, so the final batch of events reaches the store:

```go
drained, err := queue.StopDrain(ctx) // Emits OnAbandon for unstarted work.
if err != nil {
	log.Printf("drain bounded: %v", err)
}
persist(drained)
sk.Close()
```

### Getting the most out of it

The dashboard can only show what your jobs tell cq:

- **`cq.WithJobName("import-rows")`** — without a name, every row reads
  "unnamed" and the grouped failures view has nothing to group on. Name the job
  *type*; put identity in attributes.
- **`cq.WithJobAttribute("tenant", id)`** — attributes are how you find a job
  later, and the closest thing to a payload the dashboard can show. They are
  filterable from the jobs and failures pages.
- **`cq.SetProgress(ctx, cq.Progress{...})`** — only jobs that call this report
  progress. Requires `ProgressMiddleware` (or `cq.WithProgress`) to be wired.

Run the demo to see all of it with generated traffic:

```bash
go run ./cmd/demo -addr :8080
```

## Authentication

Nothing is protected by default. The views expose job names, attributes and
error strings, so put a login in front before it leaves your laptop:

```go
handler, err := web.New("/cq", st, sk,
	web.WithQueues(queue),
	web.WithLogin(
		web.StaticPassword("ops", os.Getenv("CQ_DASH_PASSWORD")),
		[]byte(os.Getenv("CQ_DASH_SECRET")), // Signs session cookies.
		12*time.Hour,                        // Session lifetime.
	),
)
http.Handle("/cq/", handler)
```

Every route then redirects to a login form until the visitor signs in, and the
session rides in an HttpOnly, SameSite=Lax cookie signed with your secret. The
top bar shows who is signed in and offers a sign-out, which revokes the session
server side rather than only deleting the browser's copy. Rotating the secret
logs everyone out.

Behind a TLS-terminating proxy `r.TLS` is nil, so the Secure flag is inferred
from `X-Forwarded-Proto`. Set it explicitly in production:

```go
web.WithSecureCookies(true)
```

Failed logins are rate limited per client address and audited with the
attempted username, so a run of guesses is visible rather than silent.

This is a form rather than HTTP basic auth on purpose: basic auth depends on
the browser's native dialog, which cannot be styled, has no sign-out, and does
not appear at all inside embedded webviews.

Real users, real identities? Supply your own check instead of
`StaticPassword` — it is just a function:

```go
web.WithLogin(func(username, password string) (string, bool) {
	user, err := accounts.Authenticate(username, password)
	if err != nil || !user.IsAdmin {
		return "", false
	}
	return user.Email, true // Lands in the audit log.
}, secret, 12*time.Hour)
```

### Scripts and machines

`WithTokens` registers credentials carried in a header. They are tried in order
and satisfy the same gate the login form does, so one deployment serves humans
and scripts at once:

```go
handler, err := web.New("/cq", st, sk,
	web.WithQueues(queue),
	web.WithLogin(web.StaticPassword("ops", pass), secret, 12*time.Hour),
	web.WithTokens(web.BearerToken("ci-bot", os.Getenv("CQ_DASH_TOKEN"))),
)
```

Adding a credential is not the same as demanding one: `WithTokens` alone leaves
the views open, since it only says how a caller *may* identify itself.
`WithLogin` requires an identity, and `RequireSignIn` does the same for a
token-only deployment with no form to redirect to:

| Options | Views | Controls need |
| --- | --- | --- |
| none | public | — (off) |
| `WithTokens` | public | a token |
| `WithTokens` + `RequireSignIn` | token only | a token |
| `WithLogin` | sign-in only | a session |
| `WithLogin` + `WithTokens` | either | either |

Already have sessions or SSO? Write an `Authorizer` against your own check and
register it as a credential... it is just a function:

```go
mine := func(r *http.Request) (string, bool) {
	user, err := mysession.FromRequest(r)
	if err != nil || !user.IsAdmin {
		return "", false
	}
	return user.Email, true // Lands in the audit log.
}

handler, _ := web.New("/cq", st, sk, web.WithQueues(queue),
	web.WithTokens(mine), web.RequireSignIn(), web.WithControls(web.AllowSignedIn))
```

`web.RequireAuth` still gates the whole handler from outside, which is where
`web.BasicAuth` belongs: browsers resend basic credentials by themselves, so it
must not be a `WithTokens` credential (token identities skip CSRF, and that
assumption only holds for headers a browser will not send on its own).

Serve over TLS: session cookies, bearer tokens and basic credentials are all
readable on the wire otherwise.

## Write controls

Off by default. The dashboard is read-only unless you pass `WithControls`, and
that option **requires** a `ControlPolicy` — there is no arrangement in which
exposing them unauthenticated is correct, so it is a compile-time argument
rather than an optional extra:

```go
handler, err := web.New("/cq", st, sk,
	web.WithQueues(queue),
	web.WithLogin(web.StaticPassword("ops", pass), secret, 12*time.Hour),
	web.WithTokens(web.BearerToken("ci-bot", os.Getenv("CQ_DASH_TOKEN"))),
	web.WithControls(web.AllowSignedIn),
	web.WithAudit(func(e web.AuditEntry) {
		log.Printf("audit: %s via %s %s queue=%s allowed=%t err=%v",
			e.Subject, e.Via, e.Action, e.Queue, e.Allowed, e.Err)
	}),
)
```

Credentials and permission are separate ideas here, which is what keeps both
simple. `WithLogin` and `WithTokens` answer *who is this*, producing an
`Identity` of `{Subject, Via}`. `ControlPolicy` answers *what may they do*:

```go
web.WithControls(web.AllowSignedIn)                       // anyone holding a credential
web.WithControls(web.AllowSubjects("ops@example.com"))    // named operators only
web.WithControls(func(id web.Identity) bool {             // or your own rule
	return id.Via == web.ViaSession && strings.HasSuffix(id.Subject, "@example.com")
})
```

Controls with no credential source is a construction error rather than a page of
dead buttons.

What you get: **pause**, **resume**, and **worker range** per queue. All three
are reversible.

What you deliberately do not get: **stop** or **drain**. cq's `Start` is a
no-op on a stopped queue, so stopping from a web page could only be undone by
restarting the process. That belongs in a deployment tool.

Other properties worth knowing:

- Control routes are not registered at all when controls are off... they 404
  rather than 401, so a read-only deployment has no write surface to probe.
- Every attempt is audited, including rejected ones. A refused control action
  is the interesting one.
- Forms carry a per-session CSRF token, compared in constant time and rendered
  only into the page, so a cross-site post cannot supply it. It dies with the
  session rather than living as long as the process.
- Token callers skip CSRF, deliberately: it defends credentials a browser
  attaches by itself, and no other site can make a browser send an
  `Authorization` header. A script is one request, not two.
- Every attempt records `Via`, so the audit log distinguishes a human clicking
  pause from a script calling it.
- The token travels in a header. Serve the dashboard over TLS.

## JSON

Off by default. `WithJSON()` serves the read views as JSON as well as HTML, for
a frontend of your own, a monitoring script, or curl:

```go
handler, err := web.New("/cq", st, sk, web.WithQueues(queue), web.WithJSON())
```

| Endpoint | Returns |
| --- | --- |
| `GET /cq/jobs.json` | A filtered page of history, with `total`, `pages` and the window it was read against |
| `GET /cq/jobs/{key}.json` | One job with its attempts and reschedule lineage |
| `GET /cq/live.json` | Live queue state, pending jobs, schedules, stored counts, sink health |

`jobs.json` takes the same query parameters as the HTML page — `queue`, `name`,
`state`, `q`, `attr`, `value`, `page`, `before` — because it is the same code
path with a different representation.

**Paging is a frozen window.** Every response echoes `before`; pass it back on
the next request and the listing stays consistent while new jobs keep arriving.
Without it, offsets silently repeat and skip rows.

**The field names are a promise.** They come from hand-written structs in the
`web` package, not from marshalling the store types, so internal renames cannot
leak into your client. New fields may appear; existing ones keep their meaning;
a breaking change bumps `api_version`. Times are RFC 3339, durations are integer
microseconds (`wait_us`, `exec_us`, because cq jobs routinely finish in well
under a millisecond), and things that never happened are **absent** rather than
zero... a job with no `progress` object never reported any, which is different
from `0 of 0`.

Authentication is the same gate as the HTML views, with one difference: a JSON
request that is not authenticated gets **401 with a JSON body** rather than a
303 to the login form, since a redirect would answer 200 with HTML and look like
success. A session cookie works, so your own frontend needs nothing extra; a
bearer token works too, and mints no session.

**No CORS headers are ever sent**, deliberately. These endpoints accept the
session cookie, so `Access-Control-Allow-Origin: *` would let any site a
logged-in operator visits read their entire job history. Same-origin only; put
your frontend behind the same host, or proxy it.

Writes stay out of JSON. Controls are form posts, and a script can drive them
with a bearer token and no CSRF token.

If you are in the same Go process, you may not need any of this: `store.Store`
is exported, so `st.Jobs(ctx, filter)`, `st.GroupJobs` and `st.Timeline` are
already yours to call directly.

## Other databases

SQLite and Postgres are both bundled, and neither is assumed. `store.Store` is
the only seam: the sink and the web layer never touch a driver. A new backend is one
package implementing that interface, and `store/storetest` is the conformance
suite it must pass... the same 21 cases both drivers run, covering the
semantics rather than the SQL: out-of-order merges, epoch and queue isolation,
lineage scoping, prune boundaries, frozen pagination windows.

### Bringing your own database handle

`sqlite.Open` uses the bundled pure-Go driver and sets what this store wants.
If you would rather choose the driver, share a pool with the rest of your
application, or pass DSN options of your own, hand it a `*sql.DB` instead:

```go
db, err := sql.Open("sqlite",
	"/var/lib/myapp/cq.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
if err != nil {
	log.Fatal(err)
}
db.SetMaxOpenConns(1)
st := sqlite.New(db)
```

Two settings become yours to get right, and both are load-bearing: WAL, or
readers block behind every write, and `busy_timeout`, or a second process
writing at the same moment fails with `SQLITE_BUSY`. The timeout is
per-connection in SQLite, which is why it belongs in the DSN rather than in a
`PRAGMA` this package could run on one connection of your pool and call it
done. Keep the pool small... the sink batches, so extra connections buy
contention rather than throughput.

The SQL is SQLite dialect, so `New` accepts any SQLite driver, not any
database. A Postgres handle compiles and then fails on the first query.

### Postgres

`store/postgres` is the second bundled backend. There is no `Open` here, only
`New`, so you bring the `*sql.DB` and the driver with it:

```go
import (
	"database/sql"

	"github.com/gnikyt/cq-dashboard/store/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
)

db, err := sql.Open("pgx", "postgres://user:pass@localhost:5432/cqdash")
if err != nil {
	log.Fatal(err)
}
st := postgres.New(db)
```

Everything downstream is identical, because the sink and the handler take
`store.Store`: the line above is the only one in your program that knows which
database is behind the dashboard. There is nothing to tune for correctness
either, since concurrent writers are the server's problem... the WAL and
`busy_timeout` advice above simply does not apply, and a normal pool is fine.

Which is exactly why this package has no `Open`, and the asymmetry with SQLite
is deliberate rather than tidy: `sqlite.Open` exists because those pragmas are
load-bearing and easy to miss, so it is worth bundling an engine to get them
right. A `postgres.Open` would set nothing and cost you a dependency, so the
choice of driver stays yours.

Note the consequence on the SQLite side: the blank import that makes
`sqlite.Open` work sits at package scope, so importing `store/sqlite` links the
pure-Go engine even if you only ever call `sqlite.New` with your own handle.
Nothing links a Postgres driver you did not choose.

It passes the same 21 conformance cases as SQLite. They need a real server, so
point the suite at one:

```bash
CQ_DASH_PG_DSN=postgres://user:pass@localhost:5432/cqdash go test ./store/postgres/
```

The demo runs on either backend, which is the quickest way to see it:

```bash
go run ./cmd/demo -pg 'postgres://user:pass@localhost:5432/cqdash'
```

`store/memory` is the reference implementation: no database, no dependencies,
and it passes the same suite. It is what this module's own tests run against,
and it doubles as a fixture for testing application code that reads the store.
It keeps nothing across a restart, which is the one thing the dashboard exists
to do, so it is not a deployment option.

Several processes can share one database. Each sink heartbeats its epoch, and
`ReconcileEpoch` only marks jobs interrupted when the epoch that owned them has
stopped beating, so a restart cleans up after itself without a live sibling
declaring its running jobs dead. Tune the margin with
`sink.WithHeartbeat(interval, staleAfter)`; the default beats every 15s and
declares an epoch dead after 60s of silence.

On SQLite that means processes on one machine: the bundled driver holds a
single connection and sets a 5s `busy_timeout`, which is what makes concurrent
writers on one file work, but it is still one filesystem. Across machines, use
Postgres... same epochs, same reconciliation, no file locking involved.

## Design notes

**The sink never does I/O in a hook.** cq invokes hooks synchronously on the
submitting or worker goroutine, so a hook that writes to a database steals
worker capacity directly. Hooks here only offer an event to a buffered channel;
one writer goroutine batches into SQLite. When the buffer fills, events are
dropped and counted rather than blocking. Surface `Sink.Dropped()`: a nonzero
value means the history is incomplete, which is the price of never slowing the
queue down.

**Writes are order-tolerant.** cq can dispatch `OnStart` before `OnEnqueue` for
the same job, so every write is an upsert that advances state by rank and never
regresses it. A late "created" event still contributes the fields a terminal
event lacked, without resurrecting a finished job.

**A logical job is a chain, not a row.** `Reschedule` and the release wrappers
mint a new submission ID per hop and carry `cq.reschedule.root_id` forward.
History groups by that root, so a poller reads as one job rather than hundreds
of unrelated ones.

**Restarts are reconciled, not ignored.** Each sink boot stamps rows with an
epoch. On startup, non-terminal rows from earlier epochs become `interrupted` —
an honest "this was running when the process died" rather than a job stuck
active forever.
