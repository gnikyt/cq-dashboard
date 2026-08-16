// Package store defines the persistence contract for dashboard history.
//
// cq holds nothing durably... the dashboard's memory lives here. A Store
// receives job records derived from queue hooks and answers the queries the
// web views need.
package store

import (
	"context"
	"time"
)

// State is a job's lifecycle state as recorded by the dashboard.
// It mirrors cq.JobState with one addition: StateInterrupted, which marks a
// job that was still running when its process died.
type State string

const (
	StateCreated     State = "created"
	StatePending     State = "pending"
	StateActive      State = "active"
	StateFailed      State = "failed"
	StateCancelled   State = "cancelled"
	StateCompleted   State = "completed"
	StateDiscarded   State = "discarded"
	StateAbandoned   State = "abandoned"
	StateInterrupted State = "interrupted"
)

// Rank orders states so an upsert can advance a job without regressing it.
// Hook delivery is not ordered: cq may report a job's start before its
// enqueue, so a write must never move a job backwards.
func (s State) Rank() int {
	switch s {
	case StateCreated:
		return 0
	case StatePending:
		return 1
	case StateActive:
		return 2
	default:
		return 3 // Terminal.
	}
}

// Terminal reports whether the state ends a job's life.
func (s State) Terminal() bool {
	return s.Rank() == 3
}

// Job is one cq submission. A logical job that reschedules or releases spans
// several of these, linked by RootID.
//
// Identity is (Epoch, Queue, ID), not ID alone. cq's default IDs are counters
// scoped to one queue: they restart at 1 on every boot, and two queues in the
// same process both mint "1". Key carries that composite and is what links and
// lookups use.
type Job struct {
	Key     string // Stable row identity: "<epoch>:<id>". Set by the store.
	ID      string // cq's submission ID, unique only within an epoch.
	Epoch   string // Sink boot that observed the job... used for restart reconciliation.
	Queue   string
	Name    string
	State   State
	Err     string
	Pattern string // Name with identifying segments masked. Set by the store.
	RootID  string // First submission in a reschedule chain (ID when there was none).
	Parent  string // Submission that rescheduled into this one.

	Attributes map[string]string

	EnqueuedAt time.Time
	StartedAt  time.Time
	FinishedAt time.Time
	WaitUS     int64 // Microseconds: cq jobs routinely finish in well under 1ms.
	ExecUS     int64

	Attempt int

	// Latest progress report, when the job reported any.
	ProgressCompleted int64
	ProgressTotal     int64
	ProgressStage     string
	ProgressAt        time.Time
}

// Attempt is one execution attempt within a submission, from cq's
// OnAttemptStart/Success/Failure hooks.
type Attempt struct {
	JobID      string // cq's submission ID, scoped to Epoch and Queue.
	Epoch      string
	Queue      string
	Attempt    int
	State      State
	Err        string
	StartedAt  time.Time
	FinishedAt time.Time
}

// Filter narrows a job listing. Zero values mean "no constraint".
type Filter struct {
	Queue   string
	Name    string
	Pattern string // Exact match on a normalized name pattern.
	States  []State
	Search  string // Matches ID or name.

	// Before excludes anything enqueued after it, so a paginated read sees a
	// stable window while new jobs keep arriving.
	Before time.Time

	// AttrKey and AttrValue filter on one job attribute. An empty AttrValue
	// matches any job carrying the key at all.
	AttrKey   string
	AttrValue string

	Limit  int
	Offset int
}

// Active reports whether the filter constrains anything.
func (f Filter) Active() bool {
	return f.Queue != "" || f.Name != "" || f.Pattern != "" ||
		f.Search != "" || f.AttrKey != "" || len(f.States) > 0
}

// Group is one family of jobs sharing a normalized name and error, as shown
// on the failures page.
type Group struct {
	Pattern  string
	State    State
	Err      string
	Count    int
	LastSeen time.Time
}

// Bucket is one slice of the throughput timeline.
type Bucket struct {
	Start     time.Time
	Completed int
	Failed    int
}

// Counts is a per-state tally over stored history.
type Counts map[State]int

// FailureStates are the terminal states worth reviewing: everything that did
// not complete successfully.
//
// These are cq execution outcomes, not a dead-letter queue. Where a transport
// sits in front of cq, its own dead-letter queue remains the record of work
// that still needs redriving.
func FailureStates() []State {
	return []State{
		StateFailed,
		StateCancelled,
		StateDiscarded,
		StateAbandoned,
		StateInterrupted,
	}
}

// Store persists and queries dashboard history.
// Implementations must be safe for concurrent use.
type Store interface {
	// Migrate creates or upgrades the schema.
	Migrate(ctx context.Context) error

	// UpsertJobs applies job records, advancing state per State.Rank and
	// merging fields that arrive across separate events.
	UpsertJobs(ctx context.Context, jobs []Job) error

	// SaveAttempts appends attempt records.
	SaveAttempts(ctx context.Context, attempts []Attempt) error

	// Heartbeat records that an epoch is alive as of seenAt. A zero seenAt
	// retires the epoch, so a restart can reconcile its jobs immediately
	// rather than waiting for them to go stale.
	Heartbeat(ctx context.Context, epoch string, seenAt time.Time) error

	// ReconcileEpoch marks non-terminal jobs as interrupted when the epoch
	// that owned them is neither the caller's nor still beating, returning how
	// many rows changed.
	//
	// staleAfter is how long an epoch may go without a heartbeat before it
	// counts as dead. Epochs that never heartbeated at all are dead: they
	// predate this bookkeeping, or their process died before its first beat.
	//
	// A live sibling's jobs must survive this. Two processes sharing one store
	// is the whole reason the liveness check exists... without it, every
	// deploy would mark the running instance's in-flight work interrupted.
	ReconcileEpoch(ctx context.Context, epoch string, staleAfter time.Duration) (int, error)

	// Jobs lists jobs newest first.
	Jobs(ctx context.Context, filter Filter) ([]Job, error)

	// CountJobs returns how many jobs match, ignoring limit and offset, so a
	// truncated listing can say what it is hiding.
	CountJobs(ctx context.Context, filter Filter) (int, error)

	// GroupJobs summarizes matching jobs by normalized name and error,
	// largest group first.
	GroupJobs(ctx context.Context, filter Filter, limit int) ([]Group, error)

	// Timeline buckets completions and failures since a cutoff, oldest first.
	Timeline(ctx context.Context, since time.Time, bucket time.Duration) ([]Bucket, error)

	// AttributeKeys lists attribute keys present in history, for filter hints.
	AttributeKeys(ctx context.Context, limit int) ([]string, error)

	// Job returns one job with its attempts, by composite key.
	Job(ctx context.Context, key string) (Job, []Attempt, error)

	// Lineage returns every submission sharing a root within one epoch,
	// oldest first. Roots are cq IDs, so they only mean anything inside the
	// epoch that minted them.
	Lineage(ctx context.Context, epoch string, rootID string) ([]Job, error)

	// Counts tallies stored jobs by state.
	Counts(ctx context.Context) (Counts, error)

	// Prune deletes terminal jobs, and their attempts, last active before the
	// cutoff. Empty states prunes every terminal state. Non-terminal jobs are
	// never pruned... they have not finished yet.
	Prune(ctx context.Context, before time.Time, states []State) (int, error)

	// Close releases resources.
	Close() error
}

// KeyFor builds a job's stable row identity.
//
// All three parts are needed: the epoch separates process runs, the queue
// separates counters within one run, and the ID is unique only inside that
// pair. Two unnamed queues in one process cannot be told apart... cq reports
// no name for them, so nothing downstream can.
func KeyFor(epoch string, queue string, id string) string {
	return epoch + ":" + queue + ":" + id
}
