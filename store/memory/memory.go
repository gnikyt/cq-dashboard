// Package memory implements store.Store without a database.
//
// It exists for three reasons: it is the reference implementation the
// conformance suite is checked against, so the suite cannot quietly encode
// SQLite's behaviour; it lets this module's own tests run without pulling in a
// driver; and it gives applications a fixture for testing code that reads the
// dashboard's store.
//
// It is not for production. History lives only as long as the process, which
// is the very thing the dashboard exists to survive.
package memory

import (
	"context"
	"errors"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gnikyt/cq-dashboard/store"
)

// maxBuckets caps what Timeline will materialize in one call, matching the
// SQLite driver so callers see one behaviour.
const maxBuckets = 10_000

// ErrNotFound is returned for a key that was never recorded.
var ErrNotFound = errors.New("memory: job not found")

// Store keeps dashboard history in maps. Safe for concurrent use.
type Store struct {
	mut      sync.RWMutex
	jobs     map[string]store.Job       // Composite key to job.
	attempts map[string][]store.Attempt // Composite key to attempts.
	epochs   map[string]time.Time       // Epoch to its last heartbeat.
}

// Open creates an empty store.
func Open() *Store {
	return &Store{
		jobs:     make(map[string]store.Job),
		attempts: make(map[string][]store.Attempt),
		epochs:   make(map[string]time.Time),
	}
}

// Migrate satisfies the interface. There is no schema to create.
func (s *Store) Migrate(context.Context) error { return nil }

// Close satisfies the interface. There is nothing to release.
func (s *Store) Close() error { return nil }

// UpsertJobs merges records, advancing state without regressing it.
//
// The merge rules mirror the contract, not any one database: state only moves
// forward by rank, timestamps fill in once, identity fields fill in while
// blank, and progress is kept only when it is newer.
func (s *Store) UpsertJobs(_ context.Context, jobs []store.Job) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	for _, incoming := range jobs {
		key := store.KeyFor(incoming.Epoch, incoming.Queue, incoming.ID)
		incoming.Key = key
		incoming.Pattern = store.NormalizeName(incoming.Name)

		existing, found := s.jobs[key]
		if !found {
			incoming.Attributes = maps.Clone(incoming.Attributes)
			s.jobs[key] = incoming
			continue
		}
		s.jobs[key] = merge(existing, incoming)
	}
	return nil
}

// merge folds an incoming record into the stored one.
func merge(existing store.Job, incoming store.Job) store.Job {
	if incoming.State.Rank() > existing.State.Rank() {
		existing.State = incoming.State
	}
	if existing.Queue == "" {
		existing.Queue = incoming.Queue
	}
	if existing.Name == "" {
		existing.Name = incoming.Name
		existing.Pattern = incoming.Pattern
	}
	if incoming.Err != "" {
		existing.Err = incoming.Err
	}
	if existing.RootID == "" {
		existing.RootID = incoming.RootID
	}
	if existing.Parent == "" {
		existing.Parent = incoming.Parent
	}
	if len(incoming.Attributes) > 0 {
		existing.Attributes = maps.Clone(incoming.Attributes)
	}
	if existing.EnqueuedAt.IsZero() {
		existing.EnqueuedAt = incoming.EnqueuedAt
	}
	if existing.StartedAt.IsZero() {
		existing.StartedAt = incoming.StartedAt
	}
	if !incoming.FinishedAt.IsZero() {
		existing.FinishedAt = incoming.FinishedAt
	}
	if incoming.WaitUS != 0 {
		existing.WaitUS = incoming.WaitUS
	}
	if incoming.ExecUS != 0 {
		existing.ExecUS = incoming.ExecUS
	}
	if incoming.Attempt > existing.Attempt {
		existing.Attempt = incoming.Attempt
	}
	if incoming.ProgressAt.After(existing.ProgressAt) {
		existing.ProgressCompleted = incoming.ProgressCompleted
		existing.ProgressTotal = incoming.ProgressTotal
		existing.ProgressStage = incoming.ProgressStage
		existing.ProgressAt = incoming.ProgressAt
	}
	return existing
}

// SaveAttempts appends attempt records, replacing one already recorded for the
// same job, attempt number and start.
func (s *Store) SaveAttempts(_ context.Context, attempts []store.Attempt) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	for _, incoming := range attempts {
		key := store.KeyFor(incoming.Epoch, incoming.Queue, incoming.JobID)
		replaced := false
		for i, existing := range s.attempts[key] {
			if existing.Attempt != incoming.Attempt || !existing.StartedAt.Equal(incoming.StartedAt) {
				continue
			}
			if incoming.Err == "" {
				incoming.Err = existing.Err
			}
			if incoming.FinishedAt.IsZero() {
				incoming.FinishedAt = existing.FinishedAt
			}
			s.attempts[key][i] = incoming
			replaced = true
			break
		}
		if !replaced {
			s.attempts[key] = append(s.attempts[key], incoming)
		}
	}
	return nil
}

// Heartbeat records that an epoch is alive as of seenAt.
func (s *Store) Heartbeat(_ context.Context, epoch string, seenAt time.Time) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.epochs[epoch] = seenAt
	return nil
}

// ReconcileEpoch marks non-terminal jobs as interrupted when the epoch that
// owned them is neither the caller's nor still beating.
func (s *Store) ReconcileEpoch(_ context.Context, epoch string, staleAfter time.Duration) (int, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	cutoff := time.Now().Add(-staleAfter)
	changed := 0
	for key, job := range s.jobs {
		if job.Epoch == epoch || job.State.Terminal() {
			continue
		}
		// A sibling still beating owns its jobs... leave them alone.
		if seen, known := s.epochs[job.Epoch]; known && !seen.IsZero() && !seen.Before(cutoff) {
			continue
		}
		job.State = store.StateInterrupted
		s.jobs[key] = job
		changed++
	}
	return changed, nil
}

// Jobs lists matching jobs newest first.
func (s *Store) Jobs(_ context.Context, filter store.Filter) ([]store.Job, error) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	found := s.matching(filter)
	sort.Slice(found, func(i, j int) bool {
		if found[i].EnqueuedAt.Equal(found[j].EnqueuedAt) {
			return found[i].Key > found[j].Key
		}
		return found[i].EnqueuedAt.After(found[j].EnqueuedAt)
	})

	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if filter.Offset >= len(found) {
		return nil, nil
	}
	found = found[filter.Offset:]
	if len(found) > limit {
		found = found[:limit]
	}
	return found, nil
}

// CountJobs returns how many jobs match, ignoring limit and offset.
func (s *Store) CountJobs(_ context.Context, filter store.Filter) (int, error) {
	s.mut.RLock()
	defer s.mut.RUnlock()
	return len(s.matching(filter)), nil
}

// GroupJobs summarizes matching jobs by pattern, state and error.
func (s *Store) GroupJobs(_ context.Context, filter store.Filter, limit int) ([]store.Group, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	s.mut.RLock()
	defer s.mut.RUnlock()

	type groupKey struct {
		pattern string
		state   store.State
		err     string
	}
	tally := map[groupKey]*store.Group{}
	for _, job := range s.matching(filter) {
		pattern := job.Pattern
		if pattern == "" {
			pattern = "(unnamed)"
		}
		key := groupKey{pattern, job.State, job.Err}
		group, found := tally[key]
		if !found {
			group = &store.Group{Pattern: pattern, State: job.State, Err: job.Err}
			tally[key] = group
		}
		group.Count++
		if seen := lastActive(job); seen.After(group.LastSeen) {
			group.LastSeen = seen
		}
	}

	groups := make([]store.Group, 0, len(tally))
	for _, group := range tally {
		groups = append(groups, *group)
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].Count == groups[j].Count {
			return groups[i].LastSeen.After(groups[j].LastSeen)
		}
		return groups[i].Count > groups[j].Count
	})
	if len(groups) > limit {
		groups = groups[:limit]
	}
	return groups, nil
}

// Timeline buckets completions and failures since a cutoff, oldest first.
func (s *Store) Timeline(_ context.Context, since time.Time, bucket time.Duration) ([]store.Bucket, error) {
	if bucket <= 0 {
		bucket = time.Minute
	}
	if since.IsZero() {
		return nil, nil
	}
	if span := time.Since(since); span/bucket > maxBuckets {
		return nil, errors.New("memory: timeline range exceeds the bucket limit")
	}

	s.mut.RLock()
	defer s.mut.RUnlock()

	failure := map[store.State]bool{}
	for _, state := range store.FailureStates() {
		failure[state] = true
	}

	width := bucket.Nanoseconds()
	counts := map[int64]*store.Bucket{}
	for _, job := range s.jobs {
		if !job.State.Terminal() {
			continue
		}
		seen := lastActive(job)
		if seen.Before(since) {
			continue
		}
		slot := seen.UnixNano() / width
		found, ok := counts[slot]
		if !ok {
			found = &store.Bucket{Start: time.Unix(0, slot*width)}
			counts[slot] = found
		}
		switch {
		case job.State == store.StateCompleted:
			found.Completed++
		case failure[job.State]:
			found.Failed++
		}
	}

	// Fill the range, so a quiet stretch reads as a gap rather than a slope.
	first, last := since.UnixNano()/width, time.Now().UnixNano()/width
	buckets := make([]store.Bucket, 0, last-first+1)
	for slot := first; slot <= last; slot++ {
		if found, ok := counts[slot]; ok {
			buckets = append(buckets, *found)
			continue
		}
		buckets = append(buckets, store.Bucket{Start: time.Unix(0, slot*width)})
	}
	return buckets, nil
}

// AttributeKeys lists attribute keys present in history, most common first.
func (s *Store) AttributeKeys(_ context.Context, limit int) ([]string, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}

	s.mut.RLock()
	defer s.mut.RUnlock()

	tally := map[string]int{}
	for _, job := range s.jobs {
		for key := range job.Attributes {
			tally[key]++
		}
	}
	keys := make([]string, 0, len(tally))
	for key := range tally {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if tally[keys[i]] == tally[keys[j]] {
			return keys[i] < keys[j]
		}
		return tally[keys[i]] > tally[keys[j]]
	})
	if len(keys) > limit {
		keys = keys[:limit]
	}
	return keys, nil
}

// Job returns one job with its attempts, by composite key.
func (s *Store) Job(_ context.Context, key string) (store.Job, []store.Attempt, error) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	job, found := s.jobs[key]
	if !found {
		return store.Job{}, nil, ErrNotFound
	}
	attempts := append([]store.Attempt(nil), s.attempts[key]...)
	sort.Slice(attempts, func(i, j int) bool {
		return attempts[i].StartedAt.Before(attempts[j].StartedAt)
	})
	job.Attributes = maps.Clone(job.Attributes)
	return job, attempts, nil
}

// Lineage returns every submission sharing a root within one epoch.
func (s *Store) Lineage(_ context.Context, epoch string, rootID string) ([]store.Job, error) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	var chain []store.Job
	for _, job := range s.jobs {
		if job.Epoch != epoch || (job.RootID != rootID && job.ID != rootID) {
			continue
		}
		job.Attributes = maps.Clone(job.Attributes)
		chain = append(chain, job)
	}
	sort.Slice(chain, func(i, j int) bool {
		if chain[i].EnqueuedAt.Equal(chain[j].EnqueuedAt) {
			return chain[i].Key < chain[j].Key
		}
		return chain[i].EnqueuedAt.Before(chain[j].EnqueuedAt)
	})
	return chain, nil
}

// Counts tallies stored jobs by state.
func (s *Store) Counts(context.Context) (store.Counts, error) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	counts := store.Counts{}
	for _, job := range s.jobs {
		counts[job.State]++
	}
	return counts, nil
}

// Prune deletes terminal jobs, and their attempts, last active before the
// cutoff. Unfinished work is never pruned.
func (s *Store) Prune(_ context.Context, before time.Time, states []store.State) (int, error) {
	if before.IsZero() {
		return 0, nil // A zero cutoff must prune nothing, not everything.
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	wanted := map[store.State]bool{}
	for _, state := range states {
		wanted[state] = true
	}

	removed := 0
	for key, job := range s.jobs {
		if !job.State.Terminal() || !lastActive(job).Before(before) {
			continue
		}
		if len(wanted) > 0 && !wanted[job.State] {
			continue
		}
		delete(s.jobs, key)
		delete(s.attempts, key)
		removed++
	}
	return removed, nil
}

// matching applies a filter. Callers hold the read lock.
func (s *Store) matching(filter store.Filter) []store.Job {
	states := map[store.State]bool{}
	for _, state := range filter.States {
		states[state] = true
	}
	terms := strings.Fields(strings.ToLower(filter.Search))

	var found []store.Job
	for _, job := range s.jobs {
		if filter.Queue != "" && job.Queue != filter.Queue {
			continue
		}
		if filter.Name != "" && job.Name != filter.Name {
			continue
		}
		if filter.Pattern != "" && job.Pattern != filter.Pattern {
			continue
		}
		if len(states) > 0 && !states[job.State] {
			continue
		}
		if !filter.Before.IsZero() && job.EnqueuedAt.After(filter.Before) {
			continue
		}
		if !matchesTerms(job, terms) || !matchesAttribute(job, filter) {
			continue
		}
		job.Attributes = maps.Clone(job.Attributes)
		found = append(found, job)
	}
	return found
}

// matchesTerms reports whether every search term appears in the ID or name.
func matchesTerms(job store.Job, terms []string) bool {
	if len(terms) == 0 {
		return true
	}
	id, name := strings.ToLower(job.ID), strings.ToLower(job.Name)
	for _, term := range terms {
		if !strings.Contains(id, term) && !strings.Contains(name, term) {
			return false
		}
	}
	return true
}

// matchesAttribute reports whether the job carries the filtered attribute. An
// empty value matches any job carrying the key at all.
func matchesAttribute(job store.Job, filter store.Filter) bool {
	if filter.AttrKey == "" {
		return true
	}
	value, found := job.Attributes[filter.AttrKey]
	if !found {
		return false
	}
	return filter.AttrValue == "" || value == filter.AttrValue
}

// lastActive is a job's finish time, falling back to its enqueue time for one
// that never ran.
func lastActive(job store.Job) time.Time {
	if job.FinishedAt.After(job.EnqueuedAt) {
		return job.FinishedAt
	}
	return job.EnqueuedAt
}
