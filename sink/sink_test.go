package sink

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gnikyt/cq-dashboard/store"
	"github.com/gnikyt/cq-dashboard/store/memory"
	cq "github.com/gnikyt/cq/v2"
)

func newSink(t *testing.T, opts ...Option) (*Sink, *memory.Store) {
	t.Helper()
	st := memory.Open()
	opts = append([]Option{WithFlushTick(10 * time.Millisecond)}, opts...)
	sk := New(st, opts...)
	if _, err := sk.Start(context.Background()); err != nil {
		t.Fatalf("Start(): %v", err)
	}
	t.Cleanup(func() {
		sk.Close()
		st.Close()
	})
	return sk, st
}

// End-to-end: a real queue's events must land as queryable history.
func TestSinkRecordsQueueLifecycle(t *testing.T) {
	sk, st := newSink(t)

	queue := cq.NewQueue(1, 1, 8, cq.WithQueueName("demo"), cq.WithHooks(sk.Hooks()))
	queue.Start()

	handle, err := queue.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	}, cq.WithJobName("importer"), cq.WithJobAttribute("tenant", "acme"))
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	<-handle.Done()
	queue.Stop(true)
	sk.Close()

	job, _, err := st.Job(context.Background(), store.KeyFor(sk.Epoch(), queue.Stats().Name, handle.ID()))
	if err != nil {
		t.Fatalf("Job(): %v", err)
	}
	if job.State != store.StateCompleted {
		t.Errorf("Job().State: got %v, want completed", job.State)
	}
	if job.Name != "importer" {
		t.Errorf("Job().Name: got %q, want %q", job.Name, "importer")
	}
	if job.Queue != "demo" {
		t.Errorf("Job().Queue: got %q, want %q", job.Queue, "demo")
	}
	if job.Attributes["tenant"] != "acme" {
		t.Errorf("Job().Attributes[tenant]: got %q, want %q", job.Attributes["tenant"], "acme")
	}
	if job.RootID != job.ID {
		t.Errorf("Job().RootID: got %q, want its own ID %q", job.RootID, job.ID)
	}
}

// Retries must land as attempt rows against one submission.
func TestSinkRecordsAttempts(t *testing.T) {
	sk, st := newSink(t)

	queue := cq.NewQueue(1, 1, 8, cq.WithHooks(sk.Hooks()))
	queue.Start()

	attempts := 0
	job := cq.WithRetry(func(ctx context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("boom")
		}
		return nil
	}, 3)

	handle, err := queue.Submit(context.Background(), job)
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	<-handle.Done()
	queue.Stop(true)
	sk.Close()

	stored, rows, err := st.Job(context.Background(), store.KeyFor(sk.Epoch(), queue.Stats().Name, handle.ID()))
	if err != nil {
		t.Fatalf("Job(): %v", err)
	}
	if stored.State != store.StateCompleted {
		t.Errorf("Job().State: got %v, want completed", stored.State)
	}
	if len(rows) != 3 {
		t.Fatalf("Job() attempts: got %d, want 3", len(rows))
	}
}

// StopDrain must leave abandoned jobs terminal, not stuck in created.
func TestSinkRecordsAbandonedJobs(t *testing.T) {
	sk, st := newSink(t)

	queue := cq.NewQueue(0, 0, 8, cq.WithHooks(sk.Hooks())) // No workers... nothing starts.
	queue.Start()

	handle, err := queue.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	if _, err := queue.StopDrain(context.Background()); err != nil {
		t.Fatalf("StopDrain(): %v", err)
	}
	sk.Close()

	job, _, err := st.Job(context.Background(), store.KeyFor(sk.Epoch(), queue.Stats().Name, handle.ID()))
	if err != nil {
		t.Fatalf("Job(): %v", err)
	}
	if job.State != store.StateAbandoned {
		t.Errorf("Job().State: got %v, want abandoned", job.State)
	}
	if !job.State.Terminal() {
		t.Error("Job().State.Terminal(): got false, want true")
	}
}

// A reschedule chain must be walkable from any hop via its root.
func TestSinkLinksRescheduleLineage(t *testing.T) {
	sk, st := newSink(t)

	queue := cq.NewQueue(1, 1, 8, cq.WithHooks(sk.Hooks()))
	queue.Start()

	var (
		mu   sync.Mutex
		hops int
		job  cq.Job
	)
	done := make(chan struct{})
	job = func(ctx context.Context) error {
		mu.Lock()
		hops++
		current := hops
		mu.Unlock()
		if current < 3 {
			_, err := cq.Reschedule(ctx, queue, job, time.Millisecond, "backoff")
			return err
		}
		close(done)
		return nil
	}

	handle, err := queue.Submit(context.Background(), job, cq.WithJobName("poller"))
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	<-handle.Done()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the reschedule chain")
	}
	queue.Stop(true)
	sk.Close()

	chain, err := st.Lineage(context.Background(), sk.Epoch(), queue.Stats().Name, handle.ID())
	if err != nil {
		t.Fatalf("Lineage(): %v", err)
	}
	if len(chain) != 3 {
		t.Fatalf("Lineage(): got %d submissions, want 3", len(chain))
	}
	if chain[1].Parent != chain[0].ID {
		t.Errorf("Lineage()[1].Parent: got %q, want %q", chain[1].Parent, chain[0].ID)
	}
}

// Queue-wide progress via middleware, rather than wrapping each job.
func TestSinkRecordsProgress(t *testing.T) {
	sk, st := newSink(t)

	queue := cq.NewQueue(1, 1, 8,
		cq.WithHooks(sk.Hooks()),
		cq.WithMiddleware(sk.ProgressMiddleware("")),
	)
	queue.Start()

	handle, err := queue.Submit(context.Background(), func(ctx context.Context) error {
		cq.SetProgress(ctx, cq.Progress{Completed: 42, Total: 100, Stage: "importing"})
		return nil
	})
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	<-handle.Done()
	queue.Stop(true)
	sk.Close()

	job, _, err := st.Job(context.Background(), store.KeyFor(sk.Epoch(), queue.Stats().Name, handle.ID()))
	if err != nil {
		t.Fatalf("Job(): %v", err)
	}
	if job.ProgressCompleted != 42 || job.ProgressTotal != 100 {
		t.Errorf("Job() progress: got %d/%d, want 42/100", job.ProgressCompleted, job.ProgressTotal)
	}
	if job.ProgressStage != "importing" {
		t.Errorf("Job().ProgressStage: got %q, want %q", job.ProgressStage, "importing")
	}
	// Progress must not hold a finished job open.
	if job.State != store.StateCompleted {
		t.Errorf("Job().State: got %v, want completed", job.State)
	}
}

// The sink sheds load rather than blocking a worker.
func TestSinkDropsRatherThanBlocking(t *testing.T) {
	st := memory.Open()
	defer st.Close()

	// Buffer of 1 with no writer running: every extra offer must be dropped,
	// never block.
	sk := New(st, WithBuffer(1))
	for range 100 {
		sk.offerJob(cq.JobEvent{ID: "1"}, store.StateCreated)
	}
	if dropped := sk.Dropped(); dropped != 99 {
		t.Errorf("Dropped(): got %d, want 99", dropped)
	}
}

// Regressions for the review findings.

// A failed Start must not consume the one chance to start.
func TestStartCanBeRetriedAfterFailure(t *testing.T) {
	st := memory.Open()
	defer st.Close()

	broken := &flakyStore{Store: st, fail: true}
	sk := New(broken, WithFlushTick(10*time.Millisecond))
	if _, err := sk.Start(context.Background()); err == nil {
		t.Fatal("first Start: got nil error, want the migrate failure")
	}

	broken.fail = false
	if _, err := sk.Start(context.Background()); err != nil {
		t.Fatalf("retried Start: %v", err)
	}
	defer sk.Close()

	// The writer must actually be running now.
	sk.offerJob(cq.JobEvent{ID: "1", Name: "after-retry"}, store.StateCompleted)
	deadline := time.Now().Add(2 * time.Second)
	for sk.Written() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if sk.Written() == 0 {
		t.Error("Written(): got 0 after a successful retry, want the event recorded")
	}
}

// Close must not hang when the writer was never launched.
func TestCloseWithoutStartReturns(t *testing.T) {
	st := memory.Open()
	defer st.Close()

	sk := New(st)
	done := make(chan struct{})
	go func() {
		sk.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() on an unstarted sink deadlocked")
	}
}

// A write the store rejected must not count as recorded.
func TestWrittenExcludesFailedWrites(t *testing.T) {
	st := memory.Open()
	defer st.Close()

	broken := &flakyStore{Store: st}
	sk := New(broken, WithFlushTick(10*time.Millisecond))
	if _, err := sk.Start(context.Background()); err != nil {
		t.Fatalf("Start(): %v", err)
	}
	broken.failWrites = true

	sk.offerJob(cq.JobEvent{ID: "1"}, store.StateCompleted)
	time.Sleep(200 * time.Millisecond)
	sk.Close()

	if written := sk.Written(); written != 0 {
		t.Errorf("Written(): got %d after every write failed, want 0", written)
	}
}

// Events offered after Close are counted, not silently swallowed.
func TestOfferAfterCloseCountsAsDropped(t *testing.T) {
	sk, _ := newSink(t)
	sk.Close()

	sk.offerJob(cq.JobEvent{ID: "1"}, store.StateCreated)
	if dropped := sk.Dropped(); dropped != 1 {
		t.Errorf("Dropped(): got %d after offering post-Close, want 1", dropped)
	}
}

// flakyStore fails on demand, so failure paths are testable.
type flakyStore struct {
	store.Store
	fail       bool
	failWrites bool
}

func (f *flakyStore) Migrate(ctx context.Context) error {
	if f.fail {
		return errors.New("migrate unavailable")
	}
	return f.Store.Migrate(ctx)
}

func (f *flakyStore) UpsertJobs(ctx context.Context, jobs []store.Job) error {
	if f.failWrites {
		return errors.New("database is read-only")
	}
	return f.Store.UpsertJobs(ctx, jobs)
}

func (f *flakyStore) SaveAttempts(ctx context.Context, attempts []store.Attempt) error {
	if f.failWrites {
		return errors.New("database is read-only")
	}
	return f.Store.SaveAttempts(ctx, attempts)
}
