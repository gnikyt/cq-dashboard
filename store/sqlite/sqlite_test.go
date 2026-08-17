package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/gnikyt/cq-dashboard/store"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate(): %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// cq can report a job's start before its enqueue, so a late "created" write
// must not regress a job already recorded as completed.
func TestUpsertDoesNotRegressState(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	now := time.Now()
	if err := st.UpsertJobs(ctx, []store.Job{
		{ID: "1", Epoch: "e1", State: store.StateActive, StartedAt: now},
		{ID: "1", Epoch: "e1", State: store.StateCompleted, FinishedAt: now, ExecUS: 12000},
	}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}

	// The out-of-order enqueue event arrives last.
	if err := st.UpsertJobs(ctx, []store.Job{
		{ID: "1", Epoch: "e1", State: store.StateCreated, Name: "importer", EnqueuedAt: now},
	}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}

	job, _, err := st.Job(ctx, store.KeyFor("e1", "", "1"))
	if err != nil {
		t.Fatalf("Job(): %v", err)
	}
	if job.State != store.StateCompleted {
		t.Errorf("Job().State: got %v, want completed", job.State)
	}
	// The late event still contributes fields the terminal event lacked.
	if job.Name != "importer" {
		t.Errorf("Job().Name: got %q, want %q", job.Name, "importer")
	}
	if job.EnqueuedAt.IsZero() {
		t.Error("Job().EnqueuedAt: got zero, want the enqueue timestamp")
	}
	if job.ExecUS != 12000 {
		t.Errorf("Job().ExecUS: got %d, want 12000", job.ExecUS)
	}
}

func TestReconcileEpochMarksInterrupted(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	if err := st.UpsertJobs(ctx, []store.Job{
		{ID: "1", Epoch: "old", State: store.StateActive},
		{ID: "2", Epoch: "old", State: store.StateCompleted},
		{ID: "3", Epoch: "old", State: store.StateCreated},
	}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}

	changed, err := st.ReconcileEpoch(ctx, "new", time.Minute)
	if err != nil {
		t.Fatalf("ReconcileEpoch(): %v", err)
	}
	if changed != 2 {
		t.Errorf("ReconcileEpoch(): got %d changed, want 2", changed)
	}

	for _, tc := range []struct {
		id   string
		want store.State
	}{
		{"1", store.StateInterrupted},
		{"2", store.StateCompleted}, // Terminal jobs are left alone.
		{"3", store.StateInterrupted},
	} {
		job, _, err := st.Job(ctx, store.KeyFor("old", "", tc.id))
		if err != nil {
			t.Fatalf("Job(%q): %v", tc.id, err)
		}
		if job.State != tc.want {
			t.Errorf("Job(%q).State: got %v, want %v", tc.id, job.State, tc.want)
		}
	}
}

func TestLineageGroupsRescheduleChain(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	base := time.Now()
	if err := st.UpsertJobs(ctx, []store.Job{
		{ID: "1", Epoch: "e", State: store.StateCompleted, RootID: "1", EnqueuedAt: base},
		{ID: "2", Epoch: "e", State: store.StateCompleted, RootID: "1", Parent: "1", EnqueuedAt: base.Add(time.Second)},
		{ID: "3", Epoch: "e", State: store.StateActive, RootID: "1", Parent: "2", EnqueuedAt: base.Add(2 * time.Second)},
		{ID: "9", Epoch: "e", State: store.StateCompleted, RootID: "9", EnqueuedAt: base},
	}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}

	chain, err := st.Lineage(ctx, "e", "", "1")
	if err != nil {
		t.Fatalf("Lineage(): %v", err)
	}
	if len(chain) != 3 {
		t.Fatalf("Lineage(): got %d jobs, want 3", len(chain))
	}
	for i, want := range []string{"1", "2", "3"} {
		if chain[i].ID != want {
			t.Errorf("Lineage()[%d].ID: got %q, want %q", i, chain[i].ID, want)
		}
	}
}

func TestJobsFilterByState(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	if err := st.UpsertJobs(ctx, []store.Job{
		{ID: "1", Epoch: "e", State: store.StateCompleted, Name: "a"},
		{ID: "2", Epoch: "e", State: store.StateFailed, Name: "b"},
	}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}

	jobs, err := st.Jobs(ctx, store.Filter{States: []store.State{store.StateFailed}})
	if err != nil {
		t.Fatalf("Jobs(): %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "2" {
		t.Fatalf("Jobs(): got %+v, want only job 2", jobs)
	}
}

func TestCountsByState(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	if err := st.UpsertJobs(ctx, []store.Job{
		{ID: "1", Epoch: "e", State: store.StateCompleted},
		{ID: "2", Epoch: "e", State: store.StateCompleted},
		{ID: "3", Epoch: "e", State: store.StateFailed},
	}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}

	counts, err := st.Counts(ctx)
	if err != nil {
		t.Fatalf("Counts(): %v", err)
	}
	if counts[store.StateCompleted] != 2 {
		t.Errorf("Counts()[completed]: got %d, want 2", counts[store.StateCompleted])
	}
	if counts[store.StateFailed] != 1 {
		t.Errorf("Counts()[failed]: got %d, want 1", counts[store.StateFailed])
	}
}

func TestPruneRemovesAgedTerminalJobs(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	old := time.Now().Add(-48 * time.Hour)
	recent := time.Now().Add(-time.Minute)

	if err := st.UpsertJobs(ctx, []store.Job{
		{ID: "old-done", Epoch: "e", State: store.StateCompleted, EnqueuedAt: old, FinishedAt: old},
		{ID: "old-failed", Epoch: "e", State: store.StateFailed, EnqueuedAt: old, FinishedAt: old},
		{ID: "old-active", Epoch: "e", State: store.StateActive, EnqueuedAt: old},
		{ID: "new-done", Epoch: "e", State: store.StateCompleted, EnqueuedAt: recent, FinishedAt: recent},
	}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}
	if err := st.SaveAttempts(ctx, []store.Attempt{
		{JobID: "old-failed", Epoch: "e", Attempt: 0, State: store.StateFailed, StartedAt: old},
	}); err != nil {
		t.Fatalf("SaveAttempts(): %v", err)
	}

	removed, err := st.Prune(ctx, time.Now().Add(-24*time.Hour), nil)
	if err != nil {
		t.Fatalf("Prune(): %v", err)
	}
	if removed != 2 {
		t.Errorf("Prune(): got %d removed, want 2", removed)
	}

	for _, id := range []string{"old-done", "old-failed"} {
		if _, _, err := st.Job(ctx, store.KeyFor("e", "", id)); err == nil {
			t.Errorf("Job(%q): still present after prune", id)
		}
	}
	// Unfinished jobs are never pruned, however old.
	if _, _, err := st.Job(ctx, store.KeyFor("e", "", "old-active")); err != nil {
		t.Errorf("Job(old-active): got %v, want the unfinished job kept", err)
	}
	if _, _, err := st.Job(ctx, store.KeyFor("e", "", "new-done")); err != nil {
		t.Errorf("Job(new-done): got %v, want the recent job kept", err)
	}

	// Attempts of pruned jobs go with them.
	var orphans int
	if err := st.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM attempts WHERE job_id = 'old-failed'").Scan(&orphans); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if orphans != 0 {
		t.Errorf("attempts after prune: got %d, want 0", orphans)
	}
}

func TestPruneRespectsStates(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	old := time.Now().Add(-48 * time.Hour)
	if err := st.UpsertJobs(ctx, []store.Job{
		{ID: "done", Epoch: "e", State: store.StateCompleted, EnqueuedAt: old, FinishedAt: old},
		{ID: "failed", Epoch: "e", State: store.StateFailed, EnqueuedAt: old, FinishedAt: old},
	}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}

	removed, err := st.Prune(ctx, time.Now(), []store.State{store.StateCompleted})
	if err != nil {
		t.Fatalf("Prune(): %v", err)
	}
	if removed != 1 {
		t.Errorf("Prune(): got %d removed, want 1", removed)
	}
	if _, _, err := st.Job(ctx, store.KeyFor("e", "", "failed")); err != nil {
		t.Errorf("Job(failed): got %v, want failures kept", err)
	}
}

func TestPruneZeroCutoffIsNoop(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	if err := st.UpsertJobs(ctx, []store.Job{
		{ID: "done", Epoch: "e", State: store.StateCompleted, EnqueuedAt: time.Now()},
	}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}
	removed, err := st.Prune(ctx, time.Time{}, nil)
	if err != nil {
		t.Fatalf("Prune(): %v", err)
	}
	if removed != 0 {
		t.Errorf("Prune(zero): got %d removed, want 0", removed)
	}
}

// cq's default IDs are per-process counters that restart at 1, so two runs
// sharing a store both produce a job "1". They must stay separate rows.
func TestJobsFromDifferentEpochsDoNotCollide(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	if err := st.UpsertJobs(ctx, []store.Job{
		{ID: "1", Epoch: "run-a", Name: "send-email", State: store.StateCompleted,
			Attributes: map[string]string{"tenant": "acme"}},
		{ID: "1", Epoch: "run-b", Name: "poll-status", State: store.StateFailed,
			Attributes: map[string]string{"tenant": "globex"}},
	}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}

	first, _, err := st.Job(ctx, store.KeyFor("run-a", "", "1"))
	if err != nil {
		t.Fatalf("Job(run-a): %v", err)
	}
	second, _, err := st.Job(ctx, store.KeyFor("run-b", "", "1"))
	if err != nil {
		t.Fatalf("Job(run-b): %v", err)
	}

	if first.Name != "send-email" || first.State != store.StateCompleted {
		t.Errorf("run-a job: got %q/%v, want send-email/completed", first.Name, first.State)
	}
	if second.Name != "poll-status" || second.State != store.StateFailed {
		t.Errorf("run-b job: got %q/%v, want poll-status/failed", second.Name, second.State)
	}
	if first.Attributes["tenant"] != "acme" || second.Attributes["tenant"] != "globex" {
		t.Errorf("attributes bled across epochs: %v / %v", first.Attributes, second.Attributes)
	}
}

// Lineage is scoped to one epoch: a later run reusing the same root ID must
// not appear in an earlier run's chain.
func TestLineageIsScopedToEpoch(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	base := time.Now()
	if err := st.UpsertJobs(ctx, []store.Job{
		{ID: "1", Epoch: "run-a", RootID: "1", State: store.StateCompleted, EnqueuedAt: base},
		{ID: "2", Epoch: "run-a", RootID: "1", Parent: "1", State: store.StateCompleted, EnqueuedAt: base.Add(time.Second)},
		{ID: "1", Epoch: "run-b", RootID: "1", State: store.StateCompleted, EnqueuedAt: base},
	}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}

	chain, err := st.Lineage(ctx, "run-a", "", "1")
	if err != nil {
		t.Fatalf("Lineage(): %v", err)
	}
	if len(chain) != 2 {
		t.Fatalf("Lineage(run-a): got %d, want 2 (the other run's job leaked in)", len(chain))
	}
	for _, job := range chain {
		if job.Epoch != "run-a" {
			t.Errorf("Lineage(run-a): got a job from epoch %q", job.Epoch)
		}
	}
}

// Timeline must refuse a bucket count it would choke on.
func TestTimelineRefusesAbsurdBucketCounts(t *testing.T) {
	st := newStore(t)

	if _, err := st.Timeline(context.Background(), time.Now().Add(-7*24*time.Hour), time.Second); err == nil {
		t.Error("Timeline(week, 1s): got nil error, want a refusal")
	}
	if _, err := st.Timeline(context.Background(), time.Now().Add(-time.Hour), time.Minute); err != nil {
		t.Errorf("Timeline(hour, 1m): %v", err)
	}
}

// Before freezes the window, so later inserts cannot shift a paginated read.
func TestFilterBeforeExcludesNewerJobs(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	cutoff := time.Now()
	if err := st.UpsertJobs(ctx, []store.Job{
		{ID: "old", Epoch: "e", State: store.StateCompleted, EnqueuedAt: cutoff.Add(-time.Minute)},
		{ID: "new", Epoch: "e", State: store.StateCompleted, EnqueuedAt: cutoff.Add(time.Minute)},
	}); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}

	jobs, err := st.Jobs(ctx, store.Filter{Before: cutoff})
	if err != nil {
		t.Fatalf("Jobs(): %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "old" {
		t.Fatalf("Jobs(Before): got %+v, want only the older job", jobs)
	}
	total, err := st.CountJobs(ctx, store.Filter{Before: cutoff})
	if err != nil {
		t.Fatalf("CountJobs(): %v", err)
	}
	if total != 1 {
		t.Errorf("CountJobs(Before): got %d, want 1", total)
	}
}
