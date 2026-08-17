// Package storetest is the conformance suite every store.Store must pass.
//
// The hard part of a backend is not the SQL dialect, it is the semantics: how
// an out-of-order event merges, which rows a prune may touch, what an epoch
// isolates. Those rules are what the dashboard's correctness rests on, so they
// are asserted here once and every implementation runs the same suite.
//
// A new driver starts by calling RunSuite and making it pass:
//
//	func TestConformance(t *testing.T) {
//		storetest.RunSuite(t, func(t *testing.T) store.Store { ... })
//	}
package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/gnikyt/cq-dashboard/store"
)

// Factory returns a fresh, migrated, empty store for one subtest. Cleanup is
// the factory's responsibility, usually through t.Cleanup.
type Factory func(t *testing.T) store.Store

// RunSuite runs every conformance case against the given implementation.
func RunSuite(t *testing.T, newStore Factory) {
	t.Helper()

	cases := []struct {
		name string
		run  func(*testing.T, store.Store)
	}{
		{"UpsertMergesOutOfOrderEvents", upsertMergesOutOfOrderEvents},
		{"UpsertNeverRegressesState", upsertNeverRegressesState},
		{"UpsertKeepsLatestProgress", upsertKeepsLatestProgress},
		{"EpochsIsolateIdenticalIDs", epochsIsolateIdenticalIDs},
		{"QueuesIsolateIdenticalIDs", queuesIsolateIdenticalIDs},
		{"AttemptsRoundTrip", attemptsRoundTrip},
		{"LineageIsScopedAndOrdered", lineageIsScopedAndOrdered},
		{"FilterByQueueNameAndState", filterByQueueNameAndState},
		{"SearchMatchesAllTerms", searchMatchesAllTerms},
		{"FilterByAttribute", filterByAttribute},
		{"BeforeFreezesTheWindow", beforeFreezesTheWindow},
		{"PaginationIsStableAndCounted", paginationIsStableAndCounted},
		{"GroupsCollapseIdentifiers", groupsCollapseIdentifiers},
		{"CountsByState", countsByState},
		{"ReconcileMarksDeadEpochs", reconcileMarksDeadEpochs},
		{"ReconcileSparesLiveSiblings", reconcileSparesLiveSiblings},
		{"PruneRemovesOnlyAgedTerminal", pruneRemovesOnlyAgedTerminal},
		{"PruneRespectsStates", pruneRespectsStates},
		{"TimelineBucketsAndRefusesAbsurdRanges", timelineBucketsAndRefusesAbsurdRanges},
		{"AttributeKeysAreDiscovered", attributeKeysAreDiscovered},
		{"MissingJobIsAnError", missingJobIsAnError},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, newStore(t))
		})
	}
}

// job builds a minimal record, so cases state only what they care about.
func job(id string, opts ...func(*store.Job)) store.Job {
	out := store.Job{
		ID:         id,
		Epoch:      "epoch-a",
		Queue:      "default",
		Name:       "worker",
		State:      store.StateCompleted,
		EnqueuedAt: time.Now(),
	}
	for _, opt := range opts {
		opt(&out)
	}
	return out
}

func withEpoch(epoch string) func(*store.Job)    { return func(j *store.Job) { j.Epoch = epoch } }
func withName(name string) func(*store.Job)      { return func(j *store.Job) { j.Name = name } }
func withQueue(queue string) func(*store.Job)    { return func(j *store.Job) { j.Queue = queue } }
func withState(s store.State) func(*store.Job)   { return func(j *store.Job) { j.State = s } }
func withErr(msg string) func(*store.Job)        { return func(j *store.Job) { j.Err = msg } }
func withEnqueued(at time.Time) func(*store.Job) { return func(j *store.Job) { j.EnqueuedAt = at } }

func withFinished(at time.Time) func(*store.Job) {
	return func(j *store.Job) { j.FinishedAt = at }
}

func withAttrs(kv map[string]string) func(*store.Job) {
	return func(j *store.Job) { j.Attributes = kv }
}

func withLineage(root string, parent string) func(*store.Job) {
	return func(j *store.Job) {
		j.RootID = root
		j.Parent = parent
	}
}

// save writes records, failing the test on error.
func save(t *testing.T, st store.Store, jobs ...store.Job) {
	t.Helper()
	if err := st.UpsertJobs(context.Background(), jobs); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}
}

// load reads one job by its epoch and ID, on the default queue.
func load(t *testing.T, st store.Store, epoch string, id string) store.Job {
	t.Helper()
	return loadFrom(t, st, epoch, "default", id)
}

// loadFrom reads one job by its full identity.
func loadFrom(t *testing.T, st store.Store, epoch string, queue string, id string) store.Job {
	t.Helper()
	found, _, err := st.Job(context.Background(), store.KeyFor(epoch, queue, id))
	if err != nil {
		t.Fatalf("Job(%s/%s): %v", epoch, id, err)
	}
	return found
}

// ids extracts IDs for order-sensitive assertions.
func ids(jobs []store.Job) []string {
	out := make([]string, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.ID)
	}
	return out
}

// cq does not order its hooks: a worker can report a start before the
// submitting goroutine reports the enqueue. A late event must contribute the
// fields it uniquely knows without undoing what already landed.
func upsertMergesOutOfOrderEvents(t *testing.T, st store.Store) {
	now := time.Now()
	save(t, st,
		job("1", withState(store.StateActive), withName("")),
		job("1", withState(store.StateCompleted), withName(""), withFinished(now)),
	)
	save(t, st, job("1", withState(store.StateCreated), withName("importer"), withEnqueued(now)))

	found := load(t, st, "epoch-a", "1")
	if found.State != store.StateCompleted {
		t.Errorf("State: got %v, want completed... a late created event regressed it", found.State)
	}
	if found.Name != "importer" {
		t.Errorf("Name: got %q, want importer... the late event should still fill blanks", found.Name)
	}
	if found.EnqueuedAt.IsZero() {
		t.Error("EnqueuedAt: got zero, want the late event's timestamp")
	}
}

func upsertNeverRegressesState(t *testing.T, st store.Store) {
	for _, terminal := range []store.State{
		store.StateCompleted, store.StateFailed, store.StateDiscarded, store.StateAbandoned,
	} {
		id := string(terminal)
		save(t, st, job(id, withState(terminal)))
		save(t, st, job(id, withState(store.StateCreated)))
		save(t, st, job(id, withState(store.StateActive)))

		if got := load(t, st, "epoch-a", id).State; got != terminal {
			t.Errorf("State after replaying earlier events: got %v, want %v", got, terminal)
		}
	}
}

// Progress arrives repeatedly for one job; the newest report wins, and an
// older one arriving late must not overwrite it.
func upsertKeepsLatestProgress(t *testing.T, st store.Store) {
	now := time.Now()
	newer := job("1", withState(store.StateActive))
	newer.ProgressCompleted, newer.ProgressTotal, newer.ProgressStage = 90, 100, "finishing"
	newer.ProgressAt = now

	older := job("1", withState(store.StateActive))
	older.ProgressCompleted, older.ProgressTotal, older.ProgressStage = 10, 100, "starting"
	older.ProgressAt = now.Add(-time.Minute)

	save(t, st, newer, older)

	found := load(t, st, "epoch-a", "1")
	if found.ProgressCompleted != 90 || found.ProgressStage != "finishing" {
		t.Errorf("progress: got %d/%s, want 90/finishing", found.ProgressCompleted, found.ProgressStage)
	}
}

// cq's default IDs are per-process counters that restart at 1, so two runs
// sharing a store both produce a job "1". They must stay separate rows.
func epochsIsolateIdenticalIDs(t *testing.T, st store.Store) {
	save(t, st,
		job("1", withEpoch("run-a"), withName("send-email"), withState(store.StateCompleted)),
		job("1", withEpoch("run-b"), withName("poll-status"), withState(store.StateFailed)),
	)

	if got := load(t, st, "run-a", "1"); got.Name != "send-email" || got.State != store.StateCompleted {
		t.Errorf("run-a job: got %q/%v, want send-email/completed", got.Name, got.State)
	}
	if got := load(t, st, "run-b", "1"); got.Name != "poll-status" || got.State != store.StateFailed {
		t.Errorf("run-b job: got %q/%v, want poll-status/failed", got.Name, got.State)
	}
}

func attemptsRoundTrip(t *testing.T, st store.Store) {
	ctx := context.Background()
	now := time.Now()
	save(t, st, job("1", withState(store.StateCompleted)))

	attempts := []store.Attempt{
		{JobID: "1", Epoch: "epoch-a", Queue: "default", Attempt: 0, State: store.StateFailed, Err: "boom", StartedAt: now},
		{JobID: "1", Epoch: "epoch-a", Queue: "default", Attempt: 1, State: store.StateCompleted, StartedAt: now.Add(time.Second)},
		{JobID: "1", Epoch: "other", Queue: "default", Attempt: 0, State: store.StateFailed, StartedAt: now},
	}
	if err := st.SaveAttempts(ctx, attempts); err != nil {
		t.Fatalf("SaveAttempts(): %v", err)
	}

	_, got, err := st.Job(ctx, store.KeyFor("epoch-a", "default", "1"))
	if err != nil {
		t.Fatalf("Job(): %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("attempts: got %d, want 2... another epoch's attempts leaked in", len(got))
	}
	if got[0].Attempt != 0 || got[0].Err != "boom" {
		t.Errorf("first attempt: got %+v, want attempt 0 carrying its error", got[0])
	}
	if got[1].State != store.StateCompleted {
		t.Errorf("second attempt: got %v, want completed", got[1].State)
	}
}

// Reschedule and release mint a new submission per hop. A chain is grouped by
// its root, and roots only mean anything inside the epoch that minted them.
func lineageIsScopedAndOrdered(t *testing.T, st store.Store) {
	base := time.Now().Add(-time.Hour)
	save(t, st,
		job("1", withLineage("1", ""), withEnqueued(base)),
		job("2", withLineage("1", "1"), withEnqueued(base.Add(time.Second))),
		job("3", withLineage("1", "2"), withEnqueued(base.Add(2*time.Second))),
		job("9", withLineage("9", ""), withEnqueued(base)),
		job("1", withEpoch("other"), withLineage("1", ""), withEnqueued(base)),
		// cq IDs are per-queue counters, so a sibling queue has its own job 1.
		// It is a different job and must not join this chain.
		job("1", withQueue("other-queue"), withLineage("1", ""), withEnqueued(base)),
	)

	chain, err := st.Lineage(context.Background(), "epoch-a", "default", "1")
	if err != nil {
		t.Fatalf("Lineage(): %v", err)
	}
	if got := ids(chain); len(got) != 3 || got[0] != "1" || got[1] != "2" || got[2] != "3" {
		t.Fatalf("Lineage(): got %v, want [1 2 3] oldest first", got)
	}
	for _, hop := range chain {
		if hop.Epoch != "epoch-a" {
			t.Errorf("Lineage(): got a hop from epoch %q", hop.Epoch)
		}
		if hop.Queue != "default" {
			t.Errorf("Lineage(): got a hop from queue %q", hop.Queue)
		}
	}

	// The sibling queue's job 1 has its own chain of one.
	other, err := st.Lineage(context.Background(), "epoch-a", "other-queue", "1")
	if err != nil {
		t.Fatalf("Lineage(other-queue): %v", err)
	}
	if len(other) != 1 {
		t.Errorf("Lineage(other-queue): got %d hops, want 1", len(other))
	}
}

func filterByQueueNameAndState(t *testing.T, st store.Store) {
	ctx := context.Background()
	save(t, st,
		job("1", withQueue("emails"), withName("send"), withState(store.StateCompleted)),
		job("2", withQueue("orders"), withName("send"), withState(store.StateFailed)),
		job("3", withQueue("orders"), withName("import"), withState(store.StateCompleted)),
	)

	for _, tc := range []struct {
		name   string
		filter store.Filter
		want   int
	}{
		{"queue", store.Filter{Queue: "orders"}, 2},
		{"name", store.Filter{Name: "send"}, 2},
		{"state", store.Filter{States: []store.State{store.StateFailed}}, 1},
		{"several states", store.Filter{States: store.FailureStates()}, 1},
		{"combined", store.Filter{Queue: "orders", States: []store.State{store.StateCompleted}}, 1},
		{"no match", store.Filter{Queue: "nope"}, 0},
	} {
		found, err := st.Jobs(ctx, tc.filter)
		if err != nil {
			t.Fatalf("Jobs(%s): %v", tc.name, err)
		}
		if len(found) != tc.want {
			t.Errorf("Jobs(%s): got %d, want %d", tc.name, len(found), tc.want)
		}
	}
}

// Terms may arrive in any order, and each must match.
func searchMatchesAllTerms(t *testing.T, st store.Store) {
	ctx := context.Background()
	save(t, st,
		job("100", withName("import-rows-8471")),
		job("200", withName("send-email")),
	)

	for _, tc := range []struct {
		search string
		want   int
	}{
		{"import", 1},
		{"rows import", 1},
		{"import missing", 0},
		{"100", 1},
	} {
		found, err := st.Jobs(ctx, store.Filter{Search: tc.search})
		if err != nil {
			t.Fatalf("Jobs(search=%q): %v", tc.search, err)
		}
		if len(found) != tc.want {
			t.Errorf("Jobs(search=%q): got %d, want %d", tc.search, len(found), tc.want)
		}
	}
}

// Attributes are how an operator finds a job, so both "has this key" and
// "has this value" must work.
func filterByAttribute(t *testing.T, st store.Store) {
	ctx := context.Background()
	save(t, st,
		job("1", withAttrs(map[string]string{"tenant": "acme", "region": "us"})),
		job("2", withAttrs(map[string]string{"tenant": "globex"})),
		job("3"),
	)

	for _, tc := range []struct {
		key, value string
		want       int
	}{
		{"tenant", "acme", 1},
		{"tenant", "", 2},
		{"region", "", 1},
		{"tenant", "nobody", 0},
		{"missing", "", 0},
	} {
		found, err := st.Jobs(ctx, store.Filter{AttrKey: tc.key, AttrValue: tc.value})
		if err != nil {
			t.Fatalf("Jobs(attr %s=%s): %v", tc.key, tc.value, err)
		}
		if len(found) != tc.want {
			t.Errorf("Jobs(attr %s=%s): got %d, want %d", tc.key, tc.value, len(found), tc.want)
		}
	}
}

func beforeFreezesTheWindow(t *testing.T, st store.Store) {
	ctx := context.Background()
	cutoff := time.Now()
	save(t, st,
		job("old", withEnqueued(cutoff.Add(-time.Minute))),
		job("new", withEnqueued(cutoff.Add(time.Minute))),
	)

	found, err := st.Jobs(ctx, store.Filter{Before: cutoff})
	if err != nil {
		t.Fatalf("Jobs(Before): %v", err)
	}
	if got := ids(found); len(got) != 1 || got[0] != "old" {
		t.Fatalf("Jobs(Before): got %v, want [old]", got)
	}
	total, err := st.CountJobs(ctx, store.Filter{Before: cutoff})
	if err != nil {
		t.Fatalf("CountJobs(Before): %v", err)
	}
	if total != 1 {
		t.Errorf("CountJobs(Before): got %d, want 1", total)
	}
}

// Newest first, limit and offset carve non-overlapping pages, and the count
// ignores both so a pager can say what it is hiding.
func paginationIsStableAndCounted(t *testing.T, st store.Store) {
	ctx := context.Background()
	base := time.Now().Add(-time.Hour)
	jobs := make([]store.Job, 0, 10)
	for i := range 10 {
		jobs = append(jobs, job(string(rune('a'+i)), withEnqueued(base.Add(time.Duration(i)*time.Minute))))
	}
	save(t, st, jobs...)

	first, err := st.Jobs(ctx, store.Filter{Limit: 4})
	if err != nil {
		t.Fatalf("Jobs(page 1): %v", err)
	}
	second, err := st.Jobs(ctx, store.Filter{Limit: 4, Offset: 4})
	if err != nil {
		t.Fatalf("Jobs(page 2): %v", err)
	}
	if len(first) != 4 || len(second) != 4 {
		t.Fatalf("page sizes: got %d and %d, want 4 and 4", len(first), len(second))
	}
	if first[0].ID != "j" {
		t.Errorf("first row: got %q, want the newest job", first[0].ID)
	}
	seen := map[string]bool{}
	for _, id := range append(ids(first), ids(second)...) {
		if seen[id] {
			t.Errorf("id %q appears on both pages", id)
		}
		seen[id] = true
	}

	total, err := st.CountJobs(ctx, store.Filter{Limit: 4})
	if err != nil {
		t.Fatalf("CountJobs(): %v", err)
	}
	if total != 10 {
		t.Errorf("CountJobs(): got %d, want 10... limit must not apply", total)
	}
}

// Identity often ends up in the job name, so grouping collapses the varying
// parts... otherwise a grouped view is thousands of groups of one.
func groupsCollapseIdentifiers(t *testing.T, st store.Store) {
	ctx := context.Background()
	jobs := make([]store.Job, 0, 6)
	for i := range 5 {
		jobs = append(jobs, job(
			string(rune('a'+i)),
			withName("import-rows-"+string(rune('0'+i))),
			withState(store.StateFailed),
			withErr("invalid payload"),
		))
	}
	jobs = append(jobs, job("z", withName("send-email"), withState(store.StateFailed), withErr("timeout")))
	save(t, st, jobs...)

	groups, err := st.GroupJobs(ctx, store.Filter{States: store.FailureStates()}, 10)
	if err != nil {
		t.Fatalf("GroupJobs(): %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("GroupJobs(): got %d groups, want 2", len(groups))
	}
	if groups[0].Count != 5 {
		t.Errorf("largest group: got %d, want 5 first", groups[0].Count)
	}
	if groups[0].Pattern != store.NormalizeName("import-rows-0") {
		t.Errorf("group pattern: got %q, want the masked family", groups[0].Pattern)
	}
	if groups[0].Err != "invalid payload" {
		t.Errorf("group error: got %q, want the shared error", groups[0].Err)
	}
}

func countsByState(t *testing.T, st store.Store) {
	save(t, st,
		job("1", withState(store.StateCompleted)),
		job("2", withState(store.StateCompleted)),
		job("3", withState(store.StateFailed)),
	)

	counts, err := st.Counts(context.Background())
	if err != nil {
		t.Fatalf("Counts(): %v", err)
	}
	if counts[store.StateCompleted] != 2 || counts[store.StateFailed] != 1 {
		t.Errorf("Counts(): got %v, want 2 completed and 1 failed", counts)
	}
}

// A job left mid-flight by a dead process cannot resolve itself. An epoch
// that never heartbeated is dead by definition.
func reconcileMarksDeadEpochs(t *testing.T, st store.Store) {
	save(t, st,
		job("1", withEpoch("old"), withState(store.StateActive)),
		job("2", withEpoch("old"), withState(store.StateCreated)),
		job("3", withEpoch("old"), withState(store.StateCompleted)),
	)

	changed, err := st.ReconcileEpoch(context.Background(), "new", time.Minute)
	if err != nil {
		t.Fatalf("ReconcileEpoch(): %v", err)
	}
	if changed != 2 {
		t.Errorf("ReconcileEpoch(): got %d changed, want 2", changed)
	}
	for id, want := range map[string]store.State{
		"1": store.StateInterrupted,
		"2": store.StateInterrupted,
		"3": store.StateCompleted,
	} {
		if got := load(t, st, "old", id).State; got != want {
			t.Errorf("job %s: got %v, want %v", id, got, want)
		}
	}
}

func pruneRemovesOnlyAgedTerminal(t *testing.T, st store.Store) {
	ctx := context.Background()
	old := time.Now().Add(-48 * time.Hour)
	recent := time.Now().Add(-time.Minute)

	save(t, st,
		job("old-done", withState(store.StateCompleted), withEnqueued(old), withFinished(old)),
		job("old-active", withState(store.StateActive), withEnqueued(old)),
		job("new-done", withState(store.StateCompleted), withEnqueued(recent), withFinished(recent)),
	)
	if err := st.SaveAttempts(ctx, []store.Attempt{
		{JobID: "old-done", Epoch: "epoch-a", Queue: "default", Attempt: 0, State: store.StateFailed, StartedAt: old},
	}); err != nil {
		t.Fatalf("SaveAttempts(): %v", err)
	}

	removed, err := st.Prune(ctx, time.Now().Add(-24*time.Hour), nil)
	if err != nil {
		t.Fatalf("Prune(): %v", err)
	}
	if removed != 1 {
		t.Errorf("Prune(): got %d removed, want 1", removed)
	}
	if _, _, err := st.Job(ctx, store.KeyFor("epoch-a", "default", "old-done")); err == nil {
		t.Error("old-done survived the prune")
	}
	// Unfinished work is never pruned, however old... it has not finished yet.
	if got := load(t, st, "epoch-a", "old-active").State; got != store.StateActive {
		t.Errorf("old-active: got %v, want it kept", got)
	}
	if got := load(t, st, "epoch-a", "new-done").State; got != store.StateCompleted {
		t.Errorf("new-done: got %v, want it kept", got)
	}

	// A zero cutoff prunes nothing rather than everything.
	removed, err = st.Prune(ctx, time.Time{}, nil)
	if err != nil {
		t.Fatalf("Prune(zero): %v", err)
	}
	if removed != 0 {
		t.Errorf("Prune(zero): got %d removed, want 0", removed)
	}
}

func pruneRespectsStates(t *testing.T, st store.Store) {
	ctx := context.Background()
	old := time.Now().Add(-48 * time.Hour)
	save(t, st,
		job("done", withState(store.StateCompleted), withEnqueued(old), withFinished(old)),
		job("failed", withState(store.StateFailed), withEnqueued(old), withFinished(old)),
	)

	removed, err := st.Prune(ctx, time.Now(), []store.State{store.StateCompleted})
	if err != nil {
		t.Fatalf("Prune(): %v", err)
	}
	if removed != 1 {
		t.Errorf("Prune(completed only): got %d removed, want 1", removed)
	}
	if got := load(t, st, "epoch-a", "failed").State; got != store.StateFailed {
		t.Errorf("failed job: got %v, want it kept", got)
	}
}

// Buckets cover the whole window, including quiet stretches, and an absurd
// ratio is refused rather than materialized.
func timelineBucketsAndRefusesAbsurdRanges(t *testing.T, st store.Store) {
	ctx := context.Background()
	now := time.Now()
	save(t, st,
		job("1", withState(store.StateCompleted), withEnqueued(now), withFinished(now)),
		job("2", withState(store.StateFailed), withEnqueued(now), withFinished(now)),
	)

	buckets, err := st.Timeline(ctx, now.Add(-10*time.Minute), time.Minute)
	if err != nil {
		t.Fatalf("Timeline(): %v", err)
	}
	if len(buckets) < 10 {
		t.Errorf("Timeline(): got %d buckets, want the quiet minutes filled too", len(buckets))
	}
	var completed, failed int
	for _, bucket := range buckets {
		completed += bucket.Completed
		failed += bucket.Failed
	}
	if completed != 1 || failed != 1 {
		t.Errorf("Timeline(): got %d completed and %d failed, want 1 and 1", completed, failed)
	}
	for i := 1; i < len(buckets); i++ {
		if !buckets[i].Start.After(buckets[i-1].Start) {
			t.Fatalf("Timeline(): buckets are not ordered oldest first")
		}
	}

	if _, err := st.Timeline(ctx, now.Add(-30*24*time.Hour), time.Second); err == nil {
		t.Error("Timeline(month of seconds): got nil error, want a refusal")
	}
}

func attributeKeysAreDiscovered(t *testing.T, st store.Store) {
	save(t, st,
		job("1", withAttrs(map[string]string{"tenant": "acme"})),
		job("2", withAttrs(map[string]string{"tenant": "globex", "shop": "x"})),
		job("3"),
	)

	keys, err := st.AttributeKeys(context.Background(), 10)
	if err != nil {
		t.Fatalf("AttributeKeys(): %v", err)
	}
	found := map[string]bool{}
	for _, key := range keys {
		found[key] = true
	}
	if !found["tenant"] || !found["shop"] {
		t.Errorf("AttributeKeys(): got %v, want tenant and shop", keys)
	}
}

func missingJobIsAnError(t *testing.T, st store.Store) {
	if _, _, err := st.Job(context.Background(), store.KeyFor("nope", "nope", "nope")); err == nil {
		t.Error("Job(missing): got nil error, want a lookup failure")
	}
}

// Two processes sharing one store is the reason liveness exists: a sibling
// that is still beating owns its unfinished jobs, and a restart elsewhere
// must not declare them interrupted.
func reconcileSparesLiveSiblings(t *testing.T, st store.Store) {
	ctx := context.Background()
	save(t, st,
		job("1", withEpoch("live-sibling"), withState(store.StateActive)),
		job("2", withEpoch("dead-sibling"), withState(store.StateActive)),
		job("3", withEpoch("retired"), withState(store.StateActive)),
	)

	if err := st.Heartbeat(ctx, "live-sibling", time.Now()); err != nil {
		t.Fatalf("Heartbeat(live): %v", err)
	}
	if err := st.Heartbeat(ctx, "dead-sibling", time.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("Heartbeat(dead): %v", err)
	}
	// A zero heartbeat retires an epoch, so a clean shutdown is reconciled at
	// once rather than after the staleness window.
	if err := st.Heartbeat(ctx, "retired", time.Time{}); err != nil {
		t.Fatalf("Heartbeat(retired): %v", err)
	}

	changed, err := st.ReconcileEpoch(ctx, "mine", time.Minute)
	if err != nil {
		t.Fatalf("ReconcileEpoch(): %v", err)
	}
	if changed != 2 {
		t.Errorf("ReconcileEpoch(): got %d changed, want 2... the live sibling must be spared", changed)
	}

	for epoch, want := range map[string]store.State{
		"live-sibling": store.StateActive,
		"dead-sibling": store.StateInterrupted,
		"retired":      store.StateInterrupted,
	} {
		id := map[string]string{"live-sibling": "1", "dead-sibling": "2", "retired": "3"}[epoch]
		if got := load(t, st, epoch, id).State; got != want {
			t.Errorf("%s job: got %v, want %v", epoch, got, want)
		}
	}
}

// cq's job IDs are per-queue counters, so two queues in one process both mint
// "1". Keying on the epoch alone merges them: one queue's history disappears
// into the other's rows.
func queuesIsolateIdenticalIDs(t *testing.T, st store.Store) {
	save(t, st,
		job("1", withQueue("emails"), withName("send-email"), withState(store.StateCompleted)),
		job("1", withQueue("reports"), withName("nightly-rollup"), withState(store.StateFailed)),
	)

	if got := loadFrom(t, st, "epoch-a", "emails", "1"); got.Name != "send-email" {
		t.Errorf("emails job: got %q, want send-email", got.Name)
	}
	if got := loadFrom(t, st, "epoch-a", "reports", "1"); got.Name != "nightly-rollup" {
		t.Errorf("reports job: got %q, want nightly-rollup", got.Name)
	}

	for queue, want := range map[string]int{"emails": 1, "reports": 1} {
		found, err := st.Jobs(context.Background(), store.Filter{Queue: queue})
		if err != nil {
			t.Fatalf("Jobs(queue=%s): %v", queue, err)
		}
		if len(found) != want {
			t.Errorf("Jobs(queue=%s): got %d, want %d", queue, len(found), want)
		}
	}
}
