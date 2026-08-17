package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gnikyt/cq-dashboard/sink"
	"github.com/gnikyt/cq-dashboard/store"
	"github.com/gnikyt/cq-dashboard/store/memory"
	cq "github.com/gnikyt/cq/v2"
)

// newJSONHarness wires a handler with the JSON endpoints on and one finished
// job in history, so responses have something in them.
func newJSONHarness(t *testing.T, opts ...Option) (*Handler, store.Store) {
	t.Helper()

	st := memory.Open()
	sk := sink.New(st, sink.WithFlushTick(10*time.Millisecond))
	if _, err := sk.Start(context.Background()); err != nil {
		t.Fatalf("Start(): %v", err)
	}
	queue := cq.NewQueue(1, 2, 8, cq.WithQueueName("demo"), cq.WithHooks(sk.Hooks()))
	queue.Start()

	handler, err := New("/cq", st, sk,
		append([]Option{WithQueues(queue), WithJSON()}, opts...)...)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(func() {
		queue.Stop(false)
		sk.Close()
		st.Close()
	})

	// One job with everything the wire format can carry.
	seed := store.Job{
		ID: "7", Epoch: sk.Epoch(), Queue: "demo", Name: "import-rows",
		State: store.StateCompleted, EnqueuedAt: time.Now().Add(-time.Minute),
		StartedAt: time.Now().Add(-30 * time.Second), FinishedAt: time.Now(),
		WaitUS: 1500, ExecUS: 42, Attempt: 2,
		Attributes:        map[string]string{"tenant": "acme"},
		ProgressCompleted: 14, ProgressTotal: 20, ProgressStage: "importing",
		ProgressAt: time.Now(),
	}
	if err := st.UpsertJobs(context.Background(), []store.Job{seed}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}
	return handler, st
}

// getJSON fetches a path and decodes the body into a generic map, so the
// assertions below are about the wire format rather than about Go types.
func getJSON(t *testing.T, handler *Handler, path string) (map[string]any, *httptest.ResponseRecorder) {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("GET %s: body is not JSON (%v): %s", path, err, rec.Body.String())
	}
	return body, rec
}

// The field names are a promise to whoever wrote a client, so a rename has to
// fail here rather than in their dashboard.
func TestJobsJSONFieldNames(t *testing.T) {
	handler, _ := newJSONHarness(t)

	body, rec := getJSON(t, handler, "/cq/jobs.json")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /cq/jobs.json: got %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json", got)
	}
	for _, field := range []string{"api_version", "jobs", "total", "page", "pages", "per_page"} {
		if _, ok := body[field]; !ok {
			t.Errorf("jobs.json: missing field %q", field)
		}
	}
	if body["api_version"] != float64(apiVersion) {
		t.Errorf("api_version: got %v, want %d", body["api_version"], apiVersion)
	}

	jobs, ok := body["jobs"].([]any)
	if !ok || len(jobs) == 0 {
		t.Fatalf("jobs: got %v, want a non-empty list", body["jobs"])
	}
	job, ok := jobs[0].(map[string]any)
	if !ok {
		t.Fatalf("jobs[0]: got %T, want an object", jobs[0])
	}
	for _, field := range []string{
		"key", "id", "epoch", "queue", "name", "pattern", "state",
		"enqueued_at", "started_at", "finished_at", "wait_us", "exec_us",
		"attempt", "attributes", "progress",
	} {
		if _, ok := job[field]; !ok {
			t.Errorf("jobs[0]: missing field %q", field)
		}
	}

	// Progress is an object rather than four flattened fields, and reports the
	// partial position a running job reached.
	progress, ok := job["progress"].(map[string]any)
	if !ok {
		t.Fatalf("progress: got %T, want an object", job["progress"])
	}
	if progress["completed"] != float64(14) || progress["total"] != float64(20) {
		t.Errorf("progress: got %v of %v, want 14 of 20", progress["completed"], progress["total"])
	}
	if progress["stage"] != "importing" {
		t.Errorf("progress stage: got %v, want importing", progress["stage"])
	}

	// Durations stay integer microseconds: milliseconds would round cq's
	// sub-millisecond jobs to zero.
	if job["exec_us"] != float64(42) {
		t.Errorf("exec_us: got %v, want 42", job["exec_us"])
	}
	// Times are RFC 3339, not epoch numbers.
	enqueued, ok := job["enqueued_at"].(string)
	if !ok {
		t.Fatalf("enqueued_at: got %T, want a string", job["enqueued_at"])
	}
	if _, err := time.Parse(timeFormat, enqueued); err != nil {
		t.Errorf("enqueued_at %q: not RFC 3339: %v", enqueued, err)
	}
}

// A job that never reported progress must omit the object rather than send a
// zeroed one: "never reported" and "0 of 0" are different answers.
func TestJobsJSONOmitsAbsentProgress(t *testing.T) {
	handler, st := newJSONHarness(t)
	if err := st.UpsertJobs(context.Background(), []store.Job{{
		ID: "9", Epoch: "e2", Queue: "demo", Name: "send-email",
		State: store.StateCompleted, EnqueuedAt: time.Now(),
	}}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}

	body, _ := getJSON(t, handler, "/cq/jobs.json?q=send-email")
	jobs := body["jobs"].([]any)
	if len(jobs) == 0 {
		t.Fatal("jobs: got none, want the seeded job")
	}
	job := jobs[0].(map[string]any)
	if _, ok := job["progress"]; ok {
		t.Error("progress: present for a job that never reported any")
	}
	if _, ok := job["finished_at"]; ok {
		t.Error("finished_at: present for a job that never finished")
	}
}

// The JSON filters must agree with the HTML ones... one implementation, two
// representations.
func TestJobsJSONFilters(t *testing.T) {
	handler, st := newJSONHarness(t)
	if err := st.UpsertJobs(context.Background(), []store.Job{{
		ID: "11", Epoch: "e3", Queue: "reports", Name: "nightly-rollup",
		State: store.StateFailed, Err: "source unavailable", EnqueuedAt: time.Now(),
		Attributes: map[string]string{"tenant": "initech"},
	}}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}

	cases := []struct {
		name  string
		query string
		want  string
	}{
		{"by queue", "?queue=reports", "nightly-rollup"},
		{"by state", "?state=failed", "nightly-rollup"},
		{"by search", "?q=import", "import-rows"},
		{"by attribute", "?attr=tenant&value=acme", "import-rows"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body, _ := getJSON(t, handler, "/cq/jobs.json"+tc.query)
			jobs := body["jobs"].([]any)
			if len(jobs) != 1 {
				t.Fatalf("jobs: got %d, want 1", len(jobs))
			}
			if name := jobs[0].(map[string]any)["name"]; name != tc.want {
				t.Errorf("name: got %v, want %v", name, tc.want)
			}
		})
	}
}

func TestJobJSONDetail(t *testing.T) {
	handler, st := newJSONHarness(t)
	key := store.KeyFor("e4", "demo", "21")
	if err := st.UpsertJobs(context.Background(), []store.Job{{
		ID: "21", Epoch: "e4", Queue: "demo", Name: "sync-inventory",
		State: store.StateFailed, Err: "upstream timeout", EnqueuedAt: time.Now(),
	}}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}
	if err := st.SaveAttempts(context.Background(), []store.Attempt{{
		JobID: "21", Epoch: "e4", Queue: "demo", Attempt: 0,
		State: store.StateFailed, Err: "upstream timeout", StartedAt: time.Now(),
	}}); err != nil {
		t.Fatalf("SaveAttempts(): %v", err)
	}

	body, rec := getJSON(t, handler, "/cq/jobs/"+key+".json")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET job.json: got %d, want 200", rec.Code)
	}
	job, ok := body["job"].(map[string]any)
	if !ok {
		t.Fatalf("job: got %T, want an object", body["job"])
	}
	if job["name"] != "sync-inventory" {
		t.Errorf("job name: got %v, want sync-inventory", job["name"])
	}
	attempts, ok := body["attempts"].([]any)
	if !ok || len(attempts) != 1 {
		t.Fatalf("attempts: got %v, want one", body["attempts"])
	}
	if got := attempts[0].(map[string]any)["error"]; got != "upstream timeout" {
		t.Errorf("attempt error: got %v, want upstream timeout", got)
	}

	// A missing job is 404 with a JSON body, not an HTML error page.
	missing, rec := getJSON(t, handler, "/cq/jobs/nope.json")
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET unknown job: got %d, want 404", rec.Code)
	}
	if _, ok := missing["error"]; !ok {
		t.Error("404 body: no error field")
	}
}

func TestLiveJSON(t *testing.T) {
	handler, _ := newJSONHarness(t)

	body, rec := getJSON(t, handler, "/cq/live.json")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /cq/live.json: got %d, want 200", rec.Code)
	}
	for _, field := range []string{"api_version", "epoch", "queues", "pending", "schedules", "counts", "sink"} {
		if _, ok := body[field]; !ok {
			t.Errorf("live.json: missing field %q", field)
		}
	}
	queues, ok := body["queues"].([]any)
	if !ok || len(queues) != 1 {
		t.Fatalf("queues: got %v, want one", body["queues"])
	}
	queue := queues[0].(map[string]any)
	for _, field := range []string{"name", "paused", "stopped", "workers_min", "workers_max", "capacity"} {
		if _, ok := queue[field]; !ok {
			t.Errorf("queues[0]: missing field %q", field)
		}
	}
	if queue["name"] != "demo" {
		t.Errorf("queue name: got %v, want demo", queue["name"])
	}
	if _, ok := body["sink"].(map[string]any)["dropped"]; !ok {
		t.Error("sink: missing dropped")
	}
}

// Off by default, and no routes when off: a deployment that wants no
// machine-readable surface has none to probe.
func TestJSONRoutesAbsentWhenDisabled(t *testing.T) {
	handler, queue, _, _ := newHarness(t)
	_ = queue

	for _, path := range []string{"/cq/jobs.json", "/cq/live.json"} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s with JSON off: got %d, want 404", path, rec.Code)
		}
	}
}

// Responses must never be cached, and must never be readable cross-origin: the
// endpoints accept the session cookie, so a permissive CORS header would hand a
// logged-in user's history to any site they visit.
func TestJSONResponseHeaders(t *testing.T) {
	handler, _ := newJSONHarness(t)

	for _, path := range []string{"/cq/jobs.json", "/cq/live.json"} {
		_, rec := getJSON(t, handler, path)
		if got := rec.Header().Get("Cache-Control"); got != "no-store, private" {
			t.Errorf("GET %s Cache-Control: got %q, want no-store, private", path, got)
		}
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("GET %s: sent CORS header %q", path, got)
		}
	}
}

// An unauthenticated JSON request gets 401 and a JSON body. Redirecting it to
// the login form would answer 200 with HTML, which a naive client reads as
// success.
func TestJSONUnauthenticatedIs401(t *testing.T) {
	handler, _ := newJSONHarness(t,
		WithLogin(StaticPassword("ops", "hunter2"), []byte("test-secret"), time.Hour))

	for _, path := range []string{"/cq/jobs.json", "/cq/live.json"} {
		body, rec := getJSON(t, handler, path)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("GET %s unauthenticated: got %d, want 401", path, rec.Code)
		}
		if body["error"] != "unauthorized" {
			t.Errorf("GET %s body: got %v, want an unauthorized error", path, body["error"])
		}
		if cookies := rec.Result().Cookies(); len(cookies) > 0 {
			t.Errorf("GET %s: set %d cookies for an API caller", path, len(cookies))
		}
	}

	// HTML still gets the form, so the browser experience is unchanged.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cq/jobs", nil))
	if rec.Code != http.StatusSeeOther {
		t.Errorf("GET /cq/jobs unauthenticated: got %d, want 303 to the login form", rec.Code)
	}
}

// A token is enough for the JSON endpoints, and it mints no session.
func TestJSONWithToken(t *testing.T) {
	handler, _ := newJSONHarness(t,
		WithLogin(StaticPassword("ops", "hunter2"), []byte("test-secret"), time.Hour),
		WithTokens(BearerToken("robot", testToken)))

	req := httptest.NewRequest(http.MethodGet, "/cq/jobs.json", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /cq/jobs.json with a token: got %d, want 200", rec.Code)
	}
	if cookies := rec.Result().Cookies(); len(cookies) > 0 {
		t.Errorf("token request: set %d cookies, want none", len(cookies))
	}
}

// A session works too, so the browser and a script read the same endpoint.
func TestJSONWithSession(t *testing.T) {
	handler, _ := newJSONHarness(t,
		WithLogin(StaticPassword("ops", "hunter2"), []byte("test-secret"), time.Hour))
	cookie := signIn(t, handler, "ops", "hunter2")

	req := httptest.NewRequest(http.MethodGet, "/cq/live.json", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /cq/live.json with a session: got %d, want 200", rec.Code)
	}
}

// Paging must be stable while jobs keep arriving, which is what the echoed
// window is for.
func TestJobsJSONPagination(t *testing.T) {
	handler, st := newJSONHarness(t)
	for i := 0; i < perPage+5; i++ {
		if err := st.UpsertJobs(context.Background(), []store.Job{{
			ID: "p" + strconv.Itoa(i), Epoch: "e5", Queue: "demo", Name: "bulk",
			State: store.StateCompleted, EnqueuedAt: time.Now().Add(-time.Duration(i) * time.Second),
		}}); err != nil {
			t.Fatalf("UpsertJobs(): %v", err)
		}
	}

	first, _ := getJSON(t, handler, "/cq/jobs.json?q=bulk")
	if got := len(first["jobs"].([]any)); got != perPage {
		t.Errorf("page 1: got %d jobs, want %d", got, perPage)
	}
	before, ok := first["before"].(string)
	if !ok || before == "" {
		t.Fatal("before: missing, so a client cannot page a frozen window")
	}

	second, _ := getJSON(t, handler,
		"/cq/jobs.json?q=bulk&page=2&before="+url.QueryEscape(before))
	if got := len(second["jobs"].([]any)); got == 0 {
		t.Error("page 2: got no jobs")
	}
	if second["total"] != first["total"] {
		t.Errorf("total changed across pages: %v then %v", first["total"], second["total"])
	}
}
