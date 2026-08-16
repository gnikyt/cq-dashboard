// Package sink turns cq lifecycle hooks into durable dashboard history.
//
// cq invokes hooks synchronously on the submitting or worker goroutine, so a
// hook that writes to a database steals worker capacity. This package never
// does I/O in a hook: events are offered to a buffered channel and a single
// writer goroutine batches them into the store. When the buffer is full events
// are dropped and counted... a dashboard must never be able to slow the queue.
package sink

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gnikyt/cq-dashboard/store"
	cq "github.com/gnikyt/cq/v2"
)

// Defaults for buffering and batching.
const (
	DefaultBuffer    = 4096
	DefaultBatchSize = 200
	DefaultFlushTick = 250 * time.Millisecond

	// DefaultHeartbeat is how often this process tells the store it is alive,
	// and DefaultStaleAfter how long silence means it is not. The gap allows
	// several missed beats before a live process is declared dead.
	DefaultHeartbeat  = 15 * time.Second
	DefaultStaleAfter = 60 * time.Second
)

// Option configures a Sink.
type Option func(*Sink)

// WithBuffer sets how many events may queue before the sink sheds load.
func WithBuffer(n int) Option {
	return func(s *Sink) {
		if n > 0 {
			s.buffer = n
		}
	}
}

// WithBatchSize sets the maximum events written per transaction.
func WithBatchSize(n int) Option {
	return func(s *Sink) {
		if n > 0 {
			s.batchSize = n
		}
	}
}

// WithFlushTick sets how long a partial batch waits before being written.
func WithFlushTick(tt time.Duration) Option {
	return func(s *Sink) {
		if tt > 0 {
			s.flushTick = tt
		}
	}
}

// WithErrorHandler receives write errors, which are otherwise silent.
func WithErrorHandler(fn func(error)) Option {
	return func(s *Sink) {
		s.onError = fn
	}
}

// WithHeartbeat sets how often this process reports itself alive, and how
// long another process may go silent before its unfinished jobs are treated as
// interrupted. It only matters when several processes share one store.
func WithHeartbeat(interval time.Duration, staleAfter time.Duration) Option {
	return func(s *Sink) {
		if interval > 0 {
			s.heartbeat = interval
		}
		if staleAfter > 0 {
			s.staleAfter = staleAfter
		}
	}
}

// WithRetention prunes terminal jobs older than age, on every interval and
// once at startup. History grows without bound otherwise.
//
// Pass states to keep failures longer than successes... for example retaining
// only completed jobs for a short window and reviewing the rest by hand.
func WithRetention(age time.Duration, interval time.Duration, states ...store.State) Option {
	return func(s *Sink) {
		if age <= 0 || interval <= 0 {
			return
		}
		s.retentionAge = age
		s.retentionTick = interval
		s.retentionStates = states
	}
}

// record is one buffered write... either a job upsert or an attempt row.
type record struct {
	job     store.Job
	attempt store.Attempt
	isJob   bool
}

// Sink records cq events into a store.Store.
type Sink struct {
	store store.Store
	epoch string

	buffer    int
	batchSize int
	flushTick time.Duration
	onError   func(error)

	heartbeat       time.Duration
	staleAfter      time.Duration
	retentionAge    time.Duration
	retentionTick   time.Duration
	retentionStates []store.State

	events  chan record
	dropped atomic.Uint64
	written atomic.Uint64
	pruned  atomic.Uint64

	mut       sync.Mutex // Guards started.
	started   bool       // Whether run() is live, so Close knows what to wait for.
	closeOnce sync.Once
	done      chan struct{}
	stopped   chan struct{}
}

// New creates a sink writing to st.
// Call Start before registering the hooks with a queue.
func New(st store.Store, opts ...Option) *Sink {
	s := &Sink{
		store:      st,
		epoch:      newEpoch(),
		buffer:     DefaultBuffer,
		batchSize:  DefaultBatchSize,
		flushTick:  DefaultFlushTick,
		heartbeat:  DefaultHeartbeat,
		staleAfter: DefaultStaleAfter,
		done:       make(chan struct{}),
		stopped:    make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.events = make(chan record, s.buffer)
	return s
}

// Start migrates the store, reconciles jobs left non-terminal by an earlier
// process, and launches the writer. It returns the number of reconciled jobs.
func (s *Sink) Start(ctx context.Context) (int, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.started {
		return 0, nil // Already recording.
	}
	select {
	case <-s.done:
		return 0, errors.New("sink: closed")
	default:
	}

	// Only a successful Start marks the sink started, so a caller that fixes
	// the underlying problem can retry. Marking it here regardless would leave
	// the writer unlaunched while every later Start reported success.
	if err := s.store.Migrate(ctx); err != nil {
		return 0, err
	}
	// Claim this epoch before reconciling, so a sibling starting at the same
	// moment sees us as alive rather than as abandoned work.
	if err := s.store.Heartbeat(ctx, s.epoch, time.Now()); err != nil {
		return 0, err
	}
	reconciled, err := s.store.ReconcileEpoch(ctx, s.epoch, s.staleAfter)
	if err != nil {
		return 0, err
	}

	s.started = true
	go s.run()
	go s.beat()
	if s.retentionAge > 0 {
		go s.retain()
	}
	return reconciled, nil
}

// beat reports this process alive until Close, then retires the epoch so a
// restart reconciles immediately instead of waiting out the staleness window.
func (s *Sink) beat() {
	ticker := time.NewTicker(s.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.store.Heartbeat(context.Background(), s.epoch, time.Now()); err != nil {
				s.reportError(err)
			}
		case <-s.done:
			if err := s.store.Heartbeat(context.Background(), s.epoch, time.Time{}); err != nil {
				s.reportError(err)
			}
			return
		}
	}
}

// retain prunes aged history until Close.
func (s *Sink) retain() {
	s.prune()

	ticker := time.NewTicker(s.retentionTick)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.prune()
		case <-s.done:
			return
		}
	}
}

// prune removes history older than the configured retention age.
func (s *Sink) prune() {
	removed, err := s.store.Prune(context.Background(),
		time.Now().Add(-s.retentionAge), s.retentionStates)
	if err != nil {
		s.reportError(err)
		return
	}
	s.pruned.Add(uint64(removed))
}

// Pruned returns how many jobs retention has removed.
func (s *Sink) Pruned() uint64 {
	return s.pruned.Load()
}

// Epoch returns this sink's boot identifier.
func (s *Sink) Epoch() string {
	return s.epoch
}

// Dropped returns how many events were shed because the buffer was full.
// Surface this: a nonzero value means the dashboard's history is incomplete.
func (s *Sink) Dropped() uint64 {
	return s.dropped.Load()
}

// Written returns how many records reached the store.
func (s *Sink) Written() uint64 {
	return s.written.Load()
}

// Close flushes buffered events and stops the writer. It is safe on a sink
// that was never started, or whose Start failed... there is simply nothing to
// wait for.
func (s *Sink) Close() error {
	s.closeOnce.Do(func() {
		s.mut.Lock()
		started := s.started
		s.mut.Unlock()

		close(s.done)
		if started {
			<-s.stopped
		}
	})
	return nil
}

// offer buffers a record without blocking the caller.
//
// Anything offered after Close would sit in a channel nobody drains, so it is
// counted as dropped rather than lost silently... the dropped figure is the
// dashboard's own signal that history has gaps.
func (s *Sink) offer(rec record) {
	select {
	case <-s.done:
		s.dropped.Add(1)
		return
	default:
	}

	select {
	case s.events <- rec:
	default:
		s.dropped.Add(1) // Shed load rather than slow a worker.
	}
}

// run batches buffered records into the store until Close.
func (s *Sink) run() {
	defer close(s.stopped)

	ticker := time.NewTicker(s.flushTick)
	defer ticker.Stop()

	batch := make([]record, 0, s.batchSize)
	for {
		select {
		case rec := <-s.events:
			batch = append(batch, rec)
			if len(batch) >= s.batchSize {
				batch = s.flush(batch)
			}
		case <-ticker.C:
			batch = s.flush(batch)
		case <-s.done:
			// Drain whatever is buffered, then write it out.
			for {
				select {
				case rec := <-s.events:
					batch = append(batch, rec)
					if len(batch) >= s.batchSize {
						batch = s.flush(batch)
					}
					continue
				default:
				}
				break
			}
			s.flush(batch)
			return
		}
	}
}

// flush writes one batch and returns the emptied slice.
func (s *Sink) flush(batch []record) []record {
	if len(batch) == 0 {
		return batch
	}

	jobs := make([]store.Job, 0, len(batch))
	attempts := make([]store.Attempt, 0)
	for _, rec := range batch {
		if rec.isJob {
			jobs = append(jobs, rec.job)
			continue
		}
		attempts = append(attempts, rec.attempt)
	}

	// Detached context... shutdown must still be able to write its final batch.
	ctx := context.Background()
	// Count only what the store accepted: a read-only disk must not report a
	// healthy, climbing "events recorded" figure.
	if err := s.store.UpsertJobs(ctx, jobs); err != nil {
		s.reportError(err)
	} else {
		s.written.Add(uint64(len(jobs)))
	}
	if err := s.store.SaveAttempts(ctx, attempts); err != nil {
		s.reportError(err)
	} else {
		s.written.Add(uint64(len(attempts)))
	}
	return batch[:0]
}

// reportError routes a write failure to the configured handler.
func (s *Sink) reportError(err error) {
	if s.onError != nil {
		s.onError(err)
	}
}

// Hooks returns the cq hooks that feed this sink.
func (s *Sink) Hooks() cq.Hooks {
	return cq.Hooks{
		OnEnqueue:        func(_ context.Context, e cq.JobEvent) { s.offerJob(e, store.StateCreated) },
		OnStart:          func(_ context.Context, e cq.JobEvent) { s.offerJob(e, store.StateActive) },
		OnSuccess:        func(_ context.Context, e cq.JobEvent) { s.offerJob(e, store.StateCompleted) },
		OnFailure:        func(_ context.Context, e cq.JobEvent) { s.offerJob(e, failureState(e)) },
		OnDiscard:        func(_ context.Context, e cq.JobEvent) { s.offerJob(e, store.StateDiscarded) },
		OnAbandon:        func(_ context.Context, e cq.JobEvent) { s.offerJob(e, store.StateAbandoned) },
		OnAttemptStart:   func(_ context.Context, e cq.JobEvent) { s.offerAttempt(e, store.StateActive) },
		OnAttemptSuccess: func(_ context.Context, e cq.JobEvent) { s.offerAttempt(e, store.StateCompleted) },
		OnAttemptFailure: func(_ context.Context, e cq.JobEvent) { s.offerAttempt(e, store.StateFailed) },
	}
}

// ProgressMiddleware wraps every job on a queue so SetProgress reports here.
// Register it with cq.WithMiddleware to cover a whole queue rather than
// wrapping each job individually.
//
// The queue name is required because cq's JobMeta does not carry it, and a
// job's identity is only unique within its queue... without it, progress from
// one queue would land on another queue's job.
func (s *Sink) ProgressMiddleware(queueName string) cq.Middleware {
	return func(job cq.Job) cq.Job {
		return cq.WithProgress(job, queueReporter{sink: s, queue: queueName})
	}
}

// queueReporter binds progress reports to the queue that produced them.
type queueReporter struct {
	sink  *Sink
	queue string
}

// ReportProgress implements cq.ProgressReporter.
func (q queueReporter) ReportProgress(_ context.Context, meta cq.JobMeta, progress cq.Progress) {
	q.sink.offer(record{
		isJob: true,
		job: store.Job{
			ID:                meta.ID,
			Epoch:             q.sink.epoch,
			Queue:             q.queue,
			Name:              meta.Name,
			State:             store.StateActive,
			Attributes:        meta.Attributes,
			RootID:            rootID(meta.ID, meta.Attributes),
			Parent:            meta.Attributes[cq.RescheduleAttributeParentID],
			EnqueuedAt:        meta.EnqueuedAt,
			Attempt:           meta.Attempt,
			ProgressCompleted: progress.Completed,
			ProgressTotal:     progress.Total,
			ProgressStage:     progress.Stage,
			ProgressAt:        time.Now(),
		},
	})
}

// offerJob buffers a job record derived from a queue event.
func (s *Sink) offerJob(e cq.JobEvent, state store.State) {
	job := store.Job{
		ID:         e.ID,
		Epoch:      s.epoch,
		Queue:      e.QueueName,
		Name:       e.Name,
		State:      state,
		Attributes: e.Attributes,
		RootID:     rootID(e.ID, e.Attributes),
		Parent:     e.Attributes[cq.RescheduleAttributeParentID],
		EnqueuedAt: e.EnqueuedAt,
		StartedAt:  e.StartedAt,
		FinishedAt: e.FinishedAt,
		WaitUS:     e.WaitDuration.Microseconds(),
		ExecUS:     e.ExecutionDuration.Microseconds(),
		Attempt:    e.Attempt,
	}
	if e.Err != nil {
		job.Err = e.Err.Error()
	}
	s.offer(record{isJob: true, job: job})
}

// offerAttempt buffers one retry attempt record, and lifts the attempt number
// onto the job itself. cq carries the attempt index on attempt events only, so
// without this a retried job would report a single try in any list view.
func (s *Sink) offerAttempt(e cq.JobEvent, state store.State) {
	s.offer(record{isJob: true, job: store.Job{
		ID:      e.ID,
		Epoch:   s.epoch,
		Queue:   e.QueueName,
		State:   store.StateActive, // An attempt means it started.
		RootID:  rootID(e.ID, e.Attributes),
		Attempt: e.Attempt,
	}})

	attempt := store.Attempt{
		JobID:      e.ID,
		Epoch:      s.epoch,
		Queue:      e.QueueName,
		Attempt:    e.Attempt,
		State:      state,
		StartedAt:  e.StartedAt,
		FinishedAt: e.FinishedAt,
	}
	if e.Err != nil {
		attempt.Err = e.Err.Error()
	}
	s.offer(record{attempt: attempt})
}

// failureState separates cancellation from ordinary failure.
func failureState(e cq.JobEvent) store.State {
	if errors.Is(e.Err, cq.ErrJobCancelled) {
		return store.StateCancelled
	}
	return store.StateFailed
}

// rootID resolves the logical job identity. Reschedule and release mint a new
// submission ID per hop, so the chain is grouped by its root attribute... a
// job that never rescheduled is its own root.
func rootID(id string, attributes map[string]string) string {
	if root := attributes[cq.RescheduleAttributeRootID]; root != "" {
		return root
	}
	return id
}

// newEpoch returns an identifier unique to this process boot. Boot time plus
// PID is enough: reconciliation only needs to tell "this process" from "not
// this process".
func newEpoch() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())
}
