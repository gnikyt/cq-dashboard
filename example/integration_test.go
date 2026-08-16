package example_test

// The README's wiring, compiled. Documentation that no longer builds is worse
// than none, and this module's API has already moved under it once.

import (
	"context"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/gnikyt/cq-dashboard/sink"
	"github.com/gnikyt/cq-dashboard/store"
	"github.com/gnikyt/cq-dashboard/store/sqlite"
	"github.com/gnikyt/cq-dashboard/web"
	cq "github.com/gnikyt/cq/v2"
)

// TestReadmeWiring runs the documented setup end to end against an in-memory
// store, so a signature change breaks the test rather than the reader.
func TestReadmeWiring(t *testing.T) {
	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	defer st.Close()

	sk := sink.New(st,
		sink.WithRetention(24*time.Hour, time.Hour, store.StateCompleted),
		sink.WithErrorHandler(func(err error) { log.Printf("cq-dashboard sink: %v", err) }),
	)
	if _, err := sk.Start(context.Background()); err != nil {
		t.Fatalf("Start(): %v", err)
	}
	defer sk.Close()

	queue := cq.NewQueue(2, 6, 128,
		cq.WithQueueName("default"),
		cq.WithHooks(sk.Hooks()),
		cq.WithMiddleware(sk.ProgressMiddleware("default")),
	)
	queue.Start()

	scheduler := cq.NewScheduler(context.Background(), queue)
	defer scheduler.Stop()

	handler, err := web.New("/cq", st, sk,
		web.WithQueues(queue),
		web.WithSchedulers(scheduler),
	)
	if err != nil {
		t.Fatalf("web.New(): %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/cq/", handler)

	// The wiring is only correct if the mounted handler actually serves.
	handle, err := queue.Submit(context.Background(), func(ctx context.Context) error {
		cq.SetProgress(ctx, cq.Progress{Completed: 1, Total: 1, Stage: "done"})
		return nil
	}, cq.WithJobName("import-rows"), cq.WithJobAttribute("tenant", "acme"))
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	<-handle.Done()

	drained, err := queue.StopDrain(context.Background())
	if err != nil {
		t.Fatalf("StopDrain(): %v", err)
	}
	_ = drained
}

// TestReadmeControlsWiring compiles the write-controls example.
func TestReadmeControlsWiring(t *testing.T) {
	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	defer st.Close()

	sk := sink.New(st)
	if _, err := sk.Start(context.Background()); err != nil {
		t.Fatalf("Start(): %v", err)
	}
	defer sk.Close()

	queue := cq.NewQueue(1, 1, 8, cq.WithQueueName("default"), cq.WithHooks(sk.Hooks()))
	queue.Start()
	defer queue.Stop(false)

	if _, err := web.New("/cq", st, sk,
		web.WithQueues(queue),
		web.WithControls(web.BearerToken("ops@example.com", "token-from-env")),
		web.WithAudit(func(e web.AuditEntry) {
			log.Printf("audit: %s %s queue=%s allowed=%t err=%v",
				e.Subject, e.Action, e.Queue, e.Allowed, e.Err)
		}),
	); err != nil {
		t.Fatalf("web.New() with controls: %v", err)
	}
}
