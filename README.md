# cq-dashboard

An optional web dashboard for [cq](https://github.com/gnikyt/cq).

cq deliberately holds nothing durably: it is an execution engine, and a restart
takes its state with it. This module is where the memory lives. It listens to
cq's lifecycle hooks, writes them to SQLite, and serves views over the result.
The core library keeps its zero-dependency promise because none of this lives
inside it.

> Status: single process. Read-only by default; write controls are opt-in
> and require an authorizer.

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
	cq.WithMiddleware(sk.ProgressMiddleware()), // Optional: queue-wide progress.
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
top bar shows who is signed in and offers a sign-out. Rotating the secret logs
everyone out.

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

Already have sessions or SSO in front of this handler? Skip the login entirely
and gate it with your own middleware, using `Authorizer` for the controls:

```go
auth := func(r *http.Request) (string, bool) {
	user, err := mysession.FromRequest(r)
	if err != nil || !user.IsAdmin {
		return "", false
	}
	return user.Email, true
}

handler, _ := web.New("/cq", st, sk, web.WithQueues(queue), web.WithControls(auth))
http.Handle("/cq/", web.RequireAuth(handler, auth))
```

`web.RequireAuth` and `web.BearerToken` / `web.BasicAuth` remain for scripted
access and for gating the handler from outside. Serve over TLS: session
cookies and basic credentials are both readable on the wire otherwise.

## Write controls

Off by default. The dashboard is read-only unless you pass `WithControls`, and
that option **requires** an `Authorizer` — there is no arrangement in which
exposing them unauthenticated is correct, so it is a compile-time argument
rather than an optional extra:

```go
handler, err := web.New("/cq", st, sk,
	web.WithQueues(queue),
	web.WithControls(web.BearerToken("ops@example.com", os.Getenv("CQ_DASH_TOKEN"))),
	web.WithAudit(func(e web.AuditEntry) {
		log.Printf("audit: %s %s queue=%s allowed=%t err=%v",
			e.Subject, e.Action, e.Queue, e.Allowed, e.Err)
	}),
)
```

`Authorizer` is `func(*http.Request) (subject string, ok bool)`, so anything
with real users should wire it to an existing session or SSO check rather than
using the bundled bearer token. The `subject` is what lands in the audit log.

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
- Forms carry a per-handler CSRF token, compared in constant time. It is
  rendered only into the page, so a cross-site post cannot supply it.
- The bearer token travels in a header. Serve the dashboard over TLS.
- Authorization covers **controls only**. The read-only views are not
  protected... put your own middleware in front if history is sensitive.

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
