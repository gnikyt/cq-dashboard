// Command demo runs a cq queue producing mixed traffic behind the dashboard.
//
// It exists to exercise every view with realistic data: successes, retries,
// failures, discards, progress reporting, and reschedule chains.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gnikyt/cq-dashboard/sink"
	"github.com/gnikyt/cq-dashboard/store"
	"github.com/gnikyt/cq-dashboard/store/sqlite"
	"github.com/gnikyt/cq-dashboard/web"
	cq "github.com/gnikyt/cq/v2"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dsn := flag.String("db", "cq-dashboard.db", "SQLite path")
	rate := flag.Duration("rate", 300*time.Millisecond, "submission interval")
	retain := flag.Duration("retain", time.Hour, "how long completed jobs are kept")
	token := flag.String("token", "", "bearer token enabling write controls (empty disables them)")
	user := flag.String("user", "", "username for the login form (empty leaves the dashboard open)")
	pass := flag.String("pass", "", "password for the login form")
	flag.Parse()

	st, err := sqlite.Open(*dsn)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close() //nolint:errcheck // Demo teardown, nothing to recover.

	sk := sink.New(st,
		sink.WithErrorHandler(func(err error) {
			log.Printf("sink: %v", err)
		}),
		// Keep successes briefly, failures for as long as the default.
		sink.WithRetention(*retain, time.Minute, store.StateCompleted),
	)
	reconciled, err := sk.Start(context.Background())
	if err != nil {
		log.Fatalf("start sink: %v", err)
	}
	if reconciled > 0 {
		log.Printf("reconciled %d jobs left running by a previous process", reconciled)
	}

	queue := cq.NewQueue(2, 6, 128,
		cq.WithQueueName("demo"),
		cq.WithHooks(sk.Hooks()),
		cq.WithMiddleware(sk.ProgressMiddleware()),
	)
	queue.Start()

	scheduler := cq.NewScheduler(context.Background(), queue)
	defer scheduler.Stop()
	registerSchedules(scheduler)

	opts := []web.Option{
		web.WithQueues(queue),
		web.WithSchedulers(scheduler),
	}
	if *user != "" && *pass != "" {
		// A login form beats basic auth: it works in embedded browsers and
		// offers a way to sign out.
		opts = append(opts, web.WithLogin(
			web.StaticPassword(*user, *pass), []byte("demo-signing-secret"), time.Hour))
		log.Printf("login required for user %q", *user)
	}
	if *token != "" {
		// Controls act on the running queue, so they stay off without a token.
		opts = append(opts,
			web.WithControls(web.BearerToken("demo-operator", *token)),
			web.WithAudit(func(entry web.AuditEntry) {
				log.Printf("audit: subject=%q action=%s queue=%s detail=%q allowed=%t err=%v",
					entry.Subject, entry.Action, entry.Queue, entry.Detail, entry.Allowed, entry.Err)
			}),
		)
		log.Print("write controls enabled")
	}

	handler, err := web.New("/cq", st, sk, opts...)
	if err != nil {
		log.Fatalf("web: %v", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/cq/", handler)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/cq/", http.StatusFound)
	})
	server := &http.Server{Addr: *addr, Handler: mux}

	go func() {
		log.Printf("dashboard on http://localhost%s/cq/", *addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan struct{})
	go produce(queue, *rate, stop)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals

	log.Print("shutting down...")
	close(stop)

	// StopDrain hands back unstarted work and emits OnAbandon for each, so the
	// dashboard shows them as abandoned rather than stuck in created.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	drained, err := queue.StopDrain(ctx)
	if err != nil {
		log.Printf("drain bounded: %v", err)
	}
	log.Printf("drained %d unstarted jobs", len(drained))

	if err := sk.Close(); err != nil {
		log.Printf("sink close: %v", err)
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
}

// registerSchedules adds one of each schedule kind so the schedules table
// shows interval, cron-driven, and one-time entries.
func registerSchedules(scheduler *cq.Scheduler) {
	heartbeat := func(ctx context.Context) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	}
	if _, err := scheduler.Every("heartbeat", 30*time.Second, heartbeat,
		cq.WithJobName("heartbeat")); err != nil {
		log.Printf("schedule heartbeat: %v", err)
	}

	nightly, err := cq.ParseCron("0 3 * * *")
	if err != nil {
		log.Printf("parse cron: %v", err)
		return
	}
	if _, err := scheduler.On("nightly-report", nightly, heartbeat,
		cq.WithJobName("nightly-report")); err != nil {
		log.Printf("schedule nightly-report: %v", err)
	}
	if _, err := scheduler.At("warmup", time.Now().Add(2*time.Minute), heartbeat,
		cq.WithJobName("warmup")); err != nil {
		log.Printf("schedule warmup: %v", err)
	}
}

// produce submits a mix of job shapes until stop closes.
func produce(queue *cq.Queue, rate time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(rate)
	defer ticker.Stop()

	kinds := []func(*cq.Queue){submitFast, submitSlowWithProgress, submitFlaky, submitDoomed, submitPoller}
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			kinds[rand.Intn(len(kinds))](queue)
		}
	}
}

// submitFast is an ordinary quick success.
func submitFast(queue *cq.Queue) {
	_, _ = queue.Submit(context.Background(), func(ctx context.Context) error {
		time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
		return nil
	}, cq.WithJobName("send-email"), cq.WithJobAttribute("tenant", "acme"))
}

// submitSlowWithProgress reports progress while it runs.
func submitSlowWithProgress(queue *cq.Queue) {
	_, _ = queue.Submit(context.Background(), func(ctx context.Context) error {
		total := int64(20)
		for i := int64(1); i <= total; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
			cq.SetProgress(ctx, cq.Progress{Completed: i, Total: total, Stage: "importing"})
		}
		return nil
	}, cq.WithJobName("import-rows"), cq.WithJobAttribute("tenant", "globex"))
}

// submitFlaky fails a few times before succeeding, exercising attempt events.
func submitFlaky(queue *cq.Queue) {
	attempts := 0
	job := cq.WithRetry(func(ctx context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("upstream timeout")
		}
		return nil
	}, 3)
	_, _ = queue.Submit(context.Background(), job,
		cq.WithJobName("sync-inventory"), cq.WithJobAttribute("tenant", "acme"))
}

// submitDoomed always fails, so the failed state is populated.
func submitDoomed(queue *cq.Queue) {
	_, _ = queue.Submit(context.Background(), func(ctx context.Context) error {
		return errors.New("invalid payload")
	}, cq.WithJobName("charge-card"), cq.WithJobAttribute("tenant", "initech"))
}

// submitPoller reschedules itself twice, producing a lineage chain.
func submitPoller(queue *cq.Queue) {
	hops := 0
	var job cq.Job
	job = func(ctx context.Context) error {
		hops++
		if hops <= 2 {
			_, err := cq.Reschedule(ctx, queue, job, 500*time.Millisecond, "awaiting-upstream")
			return err
		}
		return nil
	}
	_, _ = queue.Submit(context.Background(), job,
		cq.WithJobName("poll-status"), cq.WithJobAttribute("tenant", "globex"))
}
