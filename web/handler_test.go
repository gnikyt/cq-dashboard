package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gnikyt/cq-dashboard/sink"
	"github.com/gnikyt/cq-dashboard/store"
	"github.com/gnikyt/cq-dashboard/store/sqlite"
	cq "github.com/gnikyt/cq/v2"
)

// newHarness wires a real queue, scheduler, sink, and store behind the handler.
// Template bugs only surface on execution, so these tests render for real.
func newHarness(t *testing.T) (*Handler, *cq.Queue, *cq.Scheduler, *sink.Sink) {
	t.Helper()

	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	sk := sink.New(st, sink.WithFlushTick(10*time.Millisecond))
	if _, err := sk.Start(context.Background()); err != nil {
		t.Fatalf("Start(): %v", err)
	}

	queue := cq.NewQueue(1, 1, 8, cq.WithQueueName("demo"), cq.WithHooks(sk.Hooks()))
	queue.Start()
	scheduler := cq.NewScheduler(context.Background(), queue)

	handler, err := New("/cq", st, sk, WithQueues(queue), WithSchedulers(scheduler))
	if err != nil {
		t.Fatalf("New(): %v", err)
	}

	t.Cleanup(func() {
		scheduler.Stop()
		queue.Stop(false)
		sk.Close()
		st.Close()
	})
	return handler, queue, scheduler, sk
}

// get renders one path and fails on any non-200 or template error.
func get(t *testing.T, handler *Handler, path string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s: got %d, want 200 (body: %s)", path, rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	// html/template writes partial output before reporting an execution error,
	// so a truncated response is the tell.
	if !strings.Contains(body, "<") {
		t.Fatalf("GET %s: got no markup, want a rendered page", path)
	}
	return body
}

func TestRendersEmptyViews(t *testing.T) {
	handler, _, _, _ := newHarness(t)

	overview := get(t, handler, "/cq/")
	for _, want := range []string{
		"Nothing in flight",
		"No schedules",
		"events recorded",
	} {
		if !strings.Contains(overview, want) {
			t.Errorf("GET /cq/: missing empty-state %q", want)
		}
	}

	jobs := get(t, handler, "/cq/jobs")
	if !strings.Contains(jobs, "No jobs match") {
		t.Error("GET /cq/jobs: missing the empty state")
	}
}

func TestOverviewShowsPendingSubmissions(t *testing.T) {
	handler, queue, _, _ := newHarness(t)

	release := make(chan struct{})
	started := make(chan struct{})
	running, err := queue.Submit(context.Background(), func(ctx context.Context) error {
		close(started)
		<-release
		return nil
	}, cq.WithJobName("running-job"))
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	<-started

	waiting, err := queue.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	}, cq.WithJobName("waiting-job"))
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}

	body := get(t, handler, "/cq/")
	for _, want := range []string{"running-job", "waiting-job", "demo", "active", "pending"} {
		if !strings.Contains(body, want) {
			t.Errorf("GET /cq/: pending panel missing %q", want)
		}
	}

	close(release)
	<-running.Done()
	<-waiting.Done()
}

func TestOverviewShowsSchedules(t *testing.T) {
	handler, _, scheduler, _ := newHarness(t)

	job := func(ctx context.Context) error { return nil }
	if _, err := scheduler.Every("heartbeat", time.Hour, job); err != nil {
		t.Fatalf("Every(): %v", err)
	}
	cron, err := cq.ParseCron("0 3 * * *")
	if err != nil {
		t.Fatalf("ParseCron(): %v", err)
	}
	if _, err := scheduler.On("nightly", cron, job); err != nil {
		t.Fatalf("On(): %v", err)
	}

	body := get(t, handler, "/cq/")
	for _, want := range []string{"heartbeat", "nightly", "interval", "schedule"} {
		if !strings.Contains(body, want) {
			t.Errorf("GET /cq/: schedules table missing %q", want)
		}
	}
}

func TestLivePartialRenders(t *testing.T) {
	handler, _, scheduler, _ := newHarness(t)
	if _, err := scheduler.Every("heartbeat", time.Hour, func(ctx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("Every(): %v", err)
	}

	body := get(t, handler, "/cq/partials/live")
	if !strings.Contains(body, "heartbeat") {
		t.Error("GET /cq/partials/live: missing the schedule row")
	}
	// The partial is a fragment, not a page.
	if strings.Contains(body, "<body") {
		t.Error("GET /cq/partials/live: got a full page, want a fragment")
	}
}

func TestJobDetailRenders(t *testing.T) {
	handler, queue, _, sk := newHarness(t)

	handle, err := queue.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	}, cq.WithJobName("importer"), cq.WithJobAttribute("tenant", "acme"))
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	<-handle.Done()

	// Wait for the sink to flush the terminal event.
	var body string
	for range 100 {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cq/jobs/"+store.KeyFor(sk.Epoch(), handle.ID()), nil))
		if rec.Code == http.StatusOK {
			body = rec.Body.String()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if body == "" {
		t.Fatal("GET /cq/jobs/{id}: never became available")
	}
	for _, want := range []string{"importer", "tenant", "acme", "completed", "Lineage"} {
		if !strings.Contains(body, want) {
			t.Errorf("GET /cq/jobs/{id}: missing %q", want)
		}
	}
}

func TestMissingJobIs404(t *testing.T) {
	handler, _, _, _ := newHarness(t)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cq/jobs/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET /cq/jobs/nope: got %d, want 404", rec.Code)
	}
}

func TestFailuresViewListsNonCompletedJobs(t *testing.T) {
	handler, queue, _, _ := newHarness(t)

	ok, err := queue.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	}, cq.WithJobName("healthy"))
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	bad, err := queue.Submit(context.Background(), func(ctx context.Context) error {
		return errors.New("invalid payload")
	}, cq.WithJobName("charge-card"))
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	<-ok.Done()
	<-bad.Done()

	var body string
	for range 100 {
		body = get(t, handler, "/cq/failures")
		if strings.Contains(body, "charge-card") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !strings.Contains(body, "charge-card") {
		t.Error("GET /cq/failures: missing the failed job")
	}
	if !strings.Contains(body, "invalid payload") {
		t.Error("GET /cq/failures: missing the failure error")
	}
	if strings.Contains(body, "healthy") {
		t.Error("GET /cq/failures: got a completed job, want failures only")
	}
	// The boundary must be stated on the page itself.
	if !strings.Contains(body, "not a\n\tdead-letter queue") && !strings.Contains(body, "dead-letter") {
		t.Error("GET /cq/failures: missing the dead-letter boundary note")
	}
}

func TestFailuresViewEmpty(t *testing.T) {
	handler, _, _, _ := newHarness(t)

	body := get(t, handler, "/cq/failures")
	if !strings.Contains(body, "Nothing has failed") {
		t.Error("GET /cq/failures: missing the empty state")
	}
}

// Retried jobs must be findable from the list, not only from a detail page.
func TestJobsListShowsTryCount(t *testing.T) {
	handler, queue, _, _ := newHarness(t)

	attempts := 0
	job := cq.WithRetry(func(ctx context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("upstream timeout")
		}
		return nil
	}, 3)

	handle, err := queue.Submit(context.Background(), job, cq.WithJobName("flaky"))
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	<-handle.Done()

	var body string
	for range 100 {
		body = get(t, handler, "/cq/jobs")
		// Wait for the final attempt to land, not merely the first retry.
		if strings.Contains(body, "3 execution attempts") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(body, `data-retried="true"`) {
		t.Error("GET /cq/jobs: retried job is not marked in the Tries column")
	}
	if !strings.Contains(body, "3 execution attempts") {
		t.Error("GET /cq/jobs: try count is not explained on hover")
	}
}

// The in-flight panel reports its own size, so the frame can stay fixed.
func TestInFlightPanelShowsCount(t *testing.T) {
	handler, queue, _, _ := newHarness(t)

	release := make(chan struct{})
	started := make(chan struct{})
	running, err := queue.Submit(context.Background(), func(ctx context.Context) error {
		close(started)
		<-release
		return nil
	})
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	<-started

	body := get(t, handler, "/cq/partials/live")
	if !strings.Contains(body, `class="count-pill">1<`) {
		t.Error("GET /cq/partials/live: in-flight count pill missing or wrong")
	}
	if !strings.Contains(body, "scroll framed") {
		t.Error("GET /cq/partials/live: in-flight region is not height-framed")
	}

	close(release)
	<-running.Done()
}

// Identity in the job name must not explode the grouped view.
func TestFailuresGroupCollapsesIdentifiers(t *testing.T) {
	handler, queue, _, _ := newHarness(t)

	for i := range 4 {
		handle, err := queue.Submit(context.Background(), func(ctx context.Context) error {
			return errors.New("invalid payload")
		}, cq.WithJobName(fmt.Sprintf("import-rows-%d", 8470+i)))
		if err != nil {
			t.Fatalf("Submit(): %v", err)
		}
		<-handle.Done()
	}

	var body string
	for range 100 {
		body = get(t, handler, "/cq/failures")
		if strings.Contains(body, "import-rows-*") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(body, "import-rows-*") {
		t.Fatal("GET /cq/failures: four distinct names did not collapse into one family")
	}
	// One drill-down link for the family, none for the raw names. (The panel's
	// own footnote mentions example identifiers, so assert on links only.)
	if !strings.Contains(body, "pattern=import-rows-%2a") {
		t.Error("GET /cq/failures: no drill-down link for the collapsed family")
	}
	if strings.Contains(body, "pattern=import-rows-8") {
		t.Error("GET /cq/failures: grouped view is linking raw identifiers")
	}
}

// A truncated list must say what it is hiding.
func TestListSaysWhenTruncated(t *testing.T) {
	handler, _, _, sk := newHarness(t)

	// Straight to the store: 120 rows is faster than running 120 jobs.
	jobs := make([]store.Job, 0, 120)
	for i := range 120 {
		jobs = append(jobs, store.Job{
			ID: fmt.Sprintf("j%03d", i), Epoch: sk.Epoch(), Name: "bulk",
			State: store.StateCompleted, EnqueuedAt: time.Now(),
		})
	}
	if err := handler.store.UpsertJobs(context.Background(), jobs); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}

	body := get(t, handler, "/cq/jobs")
	if !strings.Contains(body, "of <b class=\"mono\">120</b>") {
		t.Error("GET /cq/jobs: pager does not report the total")
	}
	if !strings.Contains(body, "Page 1 of 3") {
		t.Error("GET /cq/jobs: pager does not report the page count")
	}
	if !strings.Contains(body, "page=2") {
		t.Error("GET /cq/jobs: no link to the next page")
	}

	// Page two continues where page one stopped, and filters survive paging.
	second := get(t, handler, "/cq/jobs?page=2")
	if !strings.Contains(second, "Page 2 of 3") {
		t.Error("GET /cq/jobs?page=2: did not land on page two")
	}
	if !strings.Contains(second, "51–100") {
		t.Error("GET /cq/jobs?page=2: wrong row range")
	}
}

// Attributes are the payload substitute, so filtering by them must work.
func TestAttributeFilter(t *testing.T) {
	handler, queue, _, _ := newHarness(t)

	for _, tenant := range []string{"acme", "globex"} {
		handle, err := queue.Submit(context.Background(), func(ctx context.Context) error {
			return nil
		}, cq.WithJobName("send-email"), cq.WithJobAttribute("tenant", tenant))
		if err != nil {
			t.Fatalf("Submit(): %v", err)
		}
		<-handle.Done()
	}

	var body string
	for range 100 {
		body = get(t, handler, "/cq/jobs?attr=tenant&value=acme")
		if strings.Contains(body, "send-email") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(body, "of 1 matches") && !strings.Contains(body, "send-email") {
		t.Fatal("GET /cq/jobs?attr=tenant&value=acme: no results for a known attribute")
	}

	// The other tenant must be excluded, not merely ranked lower.
	all := get(t, handler, "/cq/jobs")
	if strings.Count(all, "send-email") <= strings.Count(body, "send-email") {
		t.Error("attribute filter did not narrow the list")
	}
}

// Filters must survive paging, or page two silently widens the search.
func TestPagerKeepsFilters(t *testing.T) {
	handler, _, _, sk := newHarness(t)

	// 160 rows so the filtered half still spans more than one page.
	jobs := make([]store.Job, 0, 160)
	for i := range 160 {
		name := "alpha"
		if i%2 == 0 {
			name = "beta"
		}
		jobs = append(jobs, store.Job{
			ID: fmt.Sprintf("k%03d", i), Epoch: sk.Epoch(), Name: name,
			State: store.StateCompleted, EnqueuedAt: time.Now(),
			Attributes: map[string]string{"tenant": "acme"},
		})
	}
	if err := handler.store.UpsertJobs(context.Background(), jobs); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}

	body := get(t, handler, "/cq/jobs?q=beta")
	if !strings.Contains(body, "of <b class=\"mono\">80</b>") {
		t.Fatal("GET /cq/jobs?q=beta: filter not applied to the total")
	}
	if !strings.Contains(body, "q=beta") {
		t.Error("GET /cq/jobs?q=beta: pager link dropped the filter")
	}
}

// Multi-term search matches in any order, so "rows import" finds the job.
func TestSearchMatchesTermsInAnyOrder(t *testing.T) {
	handler, queue, _, _ := newHarness(t)

	handle, err := queue.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	}, cq.WithJobName("import-rows-8471"))
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	<-handle.Done()

	var body string
	for range 100 {
		body = get(t, handler, "/cq/jobs?q=rows+import")
		if strings.Contains(body, "import-rows-8471") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(body, "import-rows-8471") {
		t.Error(`GET /cq/jobs?q=rows+import: terms out of order found nothing`)
	}
}
