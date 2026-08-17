// Package sqlite implements store.Store on SQLite via a pure-Go driver.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gnikyt/cq-dashboard/store"
	_ "modernc.org/sqlite"
)

// Store is a SQLite-backed store.Store.
type Store struct {
	db *sql.DB
}

// Open opens (creating if needed) the database at dsn, using the bundled
// pure-Go driver and the settings this store wants.
// Use ":memory:" for an ephemeral store.
func Open(dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open: %w", err)
	}
	// One connection serialises this process's writers... the sink batches, so
	// this is not a throughput concern. It says nothing about other processes
	// sharing the file, which is what busy_timeout is for: without it a second
	// process writing concurrently fails the batch outright with SQLITE_BUSY.
	//
	// The single connection is also what makes setting busy_timeout here work
	// at all: it is a per-connection setting, so a pool would leave every
	// other connection without one. See New.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = NORMAL;
		PRAGMA busy_timeout = 5000;
	`); err != nil {
		return nil, fmt.Errorf("sqlite: pragma: %w", err)
	}
	return &Store{db: db}, nil
}

// New wraps a database you opened yourself, for a driver of your choosing, a
// pool shared with the rest of your application, or a DSN carrying options
// this package knows nothing about.
//
// The SQL is SQLite dialect (upserts, json_extract), not portable SQL, so db
// must be some SQLite driver... a Postgres handle will compile and then fail
// on the first query. Two settings are yours to get right:
//
//   - journal_mode=WAL, or concurrent readers block behind every write. It is
//     recorded in the database file, so setting it once is enough.
//   - busy_timeout, or a second process writing at the same moment fails the
//     batch with SQLITE_BUSY. It is per-connection, so it belongs in the DSN
//     rather than in a PRAGMA this package could run for you.
//
// Keeping the pool small is wise for the same reason Open caps it at one: the
// sink batches its writes, so extra connections buy contention, not speed.
//
//	db, err := sql.Open("sqlite",
//		"/var/lib/myapp/cq.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
//	st := sqlite.New(db)
//
// Closing the store closes db.
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
	key         TEXT PRIMARY KEY,
	id          TEXT NOT NULL,
	epoch       TEXT NOT NULL,
	queue       TEXT NOT NULL DEFAULT '',
	name        TEXT NOT NULL DEFAULT '',
	pattern     TEXT NOT NULL DEFAULT '',
	state       TEXT NOT NULL,
	state_rank  INTEGER NOT NULL,
	err         TEXT NOT NULL DEFAULT '',
	root_id     TEXT NOT NULL DEFAULT '',
	parent_id   TEXT NOT NULL DEFAULT '',
	attributes  TEXT NOT NULL DEFAULT '{}',
	enqueued_at INTEGER NOT NULL DEFAULT 0,
	started_at  INTEGER NOT NULL DEFAULT 0,
	finished_at INTEGER NOT NULL DEFAULT 0,
	wait_us     INTEGER NOT NULL DEFAULT 0,
	exec_us     INTEGER NOT NULL DEFAULT 0,
	attempt     INTEGER NOT NULL DEFAULT 0,
	pg_done     INTEGER NOT NULL DEFAULT 0,
	pg_total    INTEGER NOT NULL DEFAULT 0,
	pg_stage    TEXT NOT NULL DEFAULT '',
	pg_at       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS jobs_state_idx ON jobs(state);
CREATE INDEX IF NOT EXISTS jobs_pattern_idx ON jobs(pattern);
CREATE INDEX IF NOT EXISTS jobs_root_idx ON jobs(epoch, root_id);
CREATE INDEX IF NOT EXISTS jobs_enqueued_idx ON jobs(enqueued_at DESC);

CREATE TABLE IF NOT EXISTS epochs (
	epoch   TEXT PRIMARY KEY,
	seen_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS attempts (
	job_key     TEXT NOT NULL,
	job_id      TEXT NOT NULL,
	attempt     INTEGER NOT NULL,
	state       TEXT NOT NULL,
	err         TEXT NOT NULL DEFAULT '',
	started_at  INTEGER NOT NULL DEFAULT 0,
	finished_at INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (job_key, attempt, started_at)
);
`

// maxBuckets caps what Timeline will materialize in one call.
const maxBuckets = 10_000

// Migrate creates the schema.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("sqlite: migrate: %w", err)
	}
	return nil
}

// upsertSQL advances a job without regressing it.
//
// The guards matter because hook delivery is unordered: a late-arriving
// "created" event must not overwrite a job already recorded as completed.
// State moves only forward by rank, timestamps fill in only when still unset,
// and identity fields fill in only when still blank.
const upsertSQL = `
INSERT INTO jobs (
	key, id, epoch, queue, name, pattern, state, state_rank, err, root_id, parent_id,
	attributes, enqueued_at, started_at, finished_at, wait_us, exec_us, attempt,
	pg_done, pg_total, pg_stage, pg_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(key) DO UPDATE SET
	queue       = CASE WHEN jobs.queue = '' THEN excluded.queue ELSE jobs.queue END,
	name        = CASE WHEN jobs.name = '' THEN excluded.name ELSE jobs.name END,
	pattern     = CASE WHEN jobs.pattern = '' THEN excluded.pattern ELSE jobs.pattern END,
	state       = CASE WHEN excluded.state_rank > jobs.state_rank THEN excluded.state ELSE jobs.state END,
	state_rank  = MAX(jobs.state_rank, excluded.state_rank),
	err         = CASE WHEN excluded.err != '' THEN excluded.err ELSE jobs.err END,
	root_id     = CASE WHEN jobs.root_id = '' THEN excluded.root_id ELSE jobs.root_id END,
	parent_id   = CASE WHEN jobs.parent_id = '' THEN excluded.parent_id ELSE jobs.parent_id END,
	attributes  = CASE WHEN excluded.attributes != '{}' THEN excluded.attributes ELSE jobs.attributes END,
	enqueued_at = CASE WHEN jobs.enqueued_at = 0 THEN excluded.enqueued_at ELSE jobs.enqueued_at END,
	started_at  = CASE WHEN jobs.started_at = 0 THEN excluded.started_at ELSE jobs.started_at END,
	finished_at = CASE WHEN excluded.finished_at != 0 THEN excluded.finished_at ELSE jobs.finished_at END,
	wait_us     = CASE WHEN excluded.wait_us != 0 THEN excluded.wait_us ELSE jobs.wait_us END,
	exec_us     = CASE WHEN excluded.exec_us != 0 THEN excluded.exec_us ELSE jobs.exec_us END,
	attempt     = MAX(jobs.attempt, excluded.attempt),
	pg_done     = CASE WHEN excluded.pg_at > jobs.pg_at THEN excluded.pg_done ELSE jobs.pg_done END,
	pg_total    = CASE WHEN excluded.pg_at > jobs.pg_at THEN excluded.pg_total ELSE jobs.pg_total END,
	pg_stage    = CASE WHEN excluded.pg_at > jobs.pg_at THEN excluded.pg_stage ELSE jobs.pg_stage END,
	pg_at       = MAX(jobs.pg_at, excluded.pg_at)
`

// UpsertJobs applies job records in one transaction.
func (s *Store) UpsertJobs(ctx context.Context, jobs []store.Job) error {
	if len(jobs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: upsert jobs: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // No-op once committed.

	stmt, err := tx.PrepareContext(ctx, upsertSQL)
	if err != nil {
		return fmt.Errorf("sqlite: upsert jobs: %w", err)
	}
	defer stmt.Close() //nolint:errcheck // Statement close cannot fail usefully here.

	for _, job := range jobs {
		attrs := "{}"
		if len(job.Attributes) > 0 {
			encoded, err := json.Marshal(job.Attributes)
			if err != nil {
				return fmt.Errorf("sqlite: upsert jobs: attributes: %w", err)
			}
			attrs = string(encoded)
		}
		if _, err := stmt.ExecContext(ctx,
			store.KeyFor(job.Epoch, job.Queue, job.ID), job.ID, job.Epoch,
			job.Queue, job.Name, store.NormalizeName(job.Name),
			string(job.State), job.State.Rank(),
			job.Err, job.RootID, job.Parent, attrs,
			unix(job.EnqueuedAt), unix(job.StartedAt), unix(job.FinishedAt),
			job.WaitUS, job.ExecUS, job.Attempt,
			job.ProgressCompleted, job.ProgressTotal, job.ProgressStage, unix(job.ProgressAt),
		); err != nil {
			return fmt.Errorf("sqlite: upsert jobs: %w", err)
		}
	}
	return tx.Commit()
}

// SaveAttempts appends attempt rows.
func (s *Store) SaveAttempts(ctx context.Context, attempts []store.Attempt) error {
	if len(attempts) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: save attempts: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // No-op once committed.

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO attempts (job_key, job_id, attempt, state, err, started_at, finished_at)
		VALUES (?,?,?,?,?,?,?)
		ON CONFLICT(job_key, attempt, started_at) DO UPDATE SET
			state       = excluded.state,
			err         = CASE WHEN excluded.err != '' THEN excluded.err ELSE attempts.err END,
			finished_at = CASE WHEN excluded.finished_at != 0 THEN excluded.finished_at ELSE attempts.finished_at END
	`)
	if err != nil {
		return fmt.Errorf("sqlite: save attempts: %w", err)
	}
	defer stmt.Close() //nolint:errcheck // Statement close cannot fail usefully here.

	for _, attempt := range attempts {
		if _, err := stmt.ExecContext(ctx,
			store.KeyFor(attempt.Epoch, attempt.Queue, attempt.JobID), attempt.JobID, attempt.Attempt,
			string(attempt.State), attempt.Err,
			unix(attempt.StartedAt), unix(attempt.FinishedAt)); err != nil {
			return fmt.Errorf("sqlite: save attempts: %w", err)
		}
	}
	return tx.Commit()
}

// Heartbeat records that an epoch is alive as of seenAt.
func (s *Store) Heartbeat(ctx context.Context, epoch string, seenAt time.Time) error {
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO epochs (epoch, seen_at) VALUES (?, ?)
		ON CONFLICT(epoch) DO UPDATE SET seen_at = excluded.seen_at
	`, epoch, unix(seenAt)); err != nil {
		return fmt.Errorf("sqlite: heartbeat: %w", err)
	}
	return nil
}

// ReconcileEpoch marks non-terminal jobs as interrupted when the epoch that
// owned them is neither the caller's nor still beating.
//
// A job left "active" by a crashed process cannot resolve itself. A job held
// by a live sibling can, so the liveness join is what makes one store safe to
// share between processes.
func (s *Store) ReconcileEpoch(ctx context.Context, epoch string, staleAfter time.Duration) (int, error) {
	cutoff := unix(time.Now().Add(-staleAfter))
	res, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET state = ?, state_rank = 3
		WHERE epoch != ?
		  AND state_rank < 3
		  AND epoch NOT IN (
			SELECT epoch FROM epochs WHERE seen_at > 0 AND seen_at >= ?
		  )
	`, string(store.StateInterrupted), epoch, cutoff)
	if err != nil {
		return 0, fmt.Errorf("sqlite: reconcile epoch: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sqlite: reconcile epoch: %w", err)
	}
	return int(affected), nil
}

const jobColumns = `key, id, epoch, queue, name, pattern, state, err, root_id, parent_id, attributes,
	enqueued_at, started_at, finished_at, wait_us, exec_us, attempt,
	pg_done, pg_total, pg_stage, pg_at`

// predicate builds the shared WHERE clause for listing, counting and grouping.
func predicate(filter store.Filter) (string, []any) {
	var (
		where []string
		args  []any
	)
	if filter.Queue != "" {
		where = append(where, "queue = ?")
		args = append(args, filter.Queue)
	}
	if filter.Name != "" {
		where = append(where, "name = ?")
		args = append(args, filter.Name)
	}
	if !filter.Before.IsZero() {
		where = append(where, "enqueued_at <= ?")
		args = append(args, unix(filter.Before))
	}
	if filter.Pattern != "" {
		where = append(where, "pattern = ?")
		args = append(args, filter.Pattern)
	}
	if len(filter.States) > 0 {
		placeholders := make([]string, 0, len(filter.States))
		for _, state := range filter.States {
			placeholders = append(placeholders, "?")
			args = append(args, string(state))
		}
		where = append(where, "state IN ("+strings.Join(placeholders, ",")+")")
	}
	// Each whitespace-separated term must appear in the ID or the name, so
	// "import rows" finds "import-rows-8471" and order does not matter.
	for _, term := range strings.Fields(filter.Search) {
		where = append(where, "(id LIKE ? OR name LIKE ?)")
		like := "%" + term + "%"
		args = append(args, like, like)
	}
	// Attributes are JSON, so the key is a path lookup. An empty value means
	// "carries this key at all".
	if filter.AttrKey != "" {
		if filter.AttrValue == "" {
			where = append(where, "json_extract(attributes, '$.' || ?) IS NOT NULL")
			args = append(args, filter.AttrKey)
		} else {
			where = append(where, "json_extract(attributes, '$.' || ?) = ?")
			args = append(args, filter.AttrKey, filter.AttrValue)
		}
	}
	if len(where) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(where, " AND "), args
}

// Jobs lists jobs newest first.
func (s *Store) Jobs(ctx context.Context, filter store.Filter) ([]store.Job, error) {
	clause, args := predicate(filter)
	query := "SELECT " + jobColumns + " FROM jobs" + clause
	query += " ORDER BY enqueued_at DESC, rowid DESC"

	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query += " LIMIT ? OFFSET ?"
	args = append(args, limit, filter.Offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: jobs: %w", err)
	}
	defer rows.Close() //nolint:errcheck // Read errors surface through rows.Err.

	var jobs []store.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// Job returns one job with its attempts, by composite key.
func (s *Store) Job(ctx context.Context, key string) (store.Job, []store.Attempt, error) {
	row := s.db.QueryRowContext(ctx, "SELECT "+jobColumns+" FROM jobs WHERE key = ?", key)
	job, err := scanJob(row)
	if err != nil {
		return store.Job{}, nil, err
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT job_id, attempt, state, err, started_at, finished_at
		FROM attempts WHERE job_key = ? ORDER BY started_at ASC
	`, key)
	if err != nil {
		return store.Job{}, nil, fmt.Errorf("sqlite: job attempts: %w", err)
	}
	defer rows.Close() //nolint:errcheck // Read errors surface through rows.Err.

	var attempts []store.Attempt
	for rows.Next() {
		var (
			attempt               store.Attempt
			state                 string
			startedAt, finishedAt int64
		)
		if err := rows.Scan(&attempt.JobID, &attempt.Attempt, &state, &attempt.Err,
			&startedAt, &finishedAt); err != nil {
			return store.Job{}, nil, fmt.Errorf("sqlite: job attempts: %w", err)
		}
		attempt.Epoch = job.Epoch
		attempt.State = store.State(state)
		attempt.StartedAt = fromUnix(startedAt)
		attempt.FinishedAt = fromUnix(finishedAt)
		attempts = append(attempts, attempt)
	}
	return job, attempts, rows.Err()
}

// Lineage returns every submission sharing a root within one epoch and queue,
// oldest first. Roots are cq IDs, meaningful only inside the epoch and queue
// that minted them: per-queue counters collide between siblings.
func (s *Store) Lineage(ctx context.Context, epoch string, queue string, rootID string) ([]store.Job, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+jobColumns+`
		FROM jobs WHERE epoch = ? AND queue = ? AND (root_id = ? OR id = ?)
		ORDER BY enqueued_at ASC, rowid ASC
	`, epoch, queue, rootID, rootID)
	if err != nil {
		return nil, fmt.Errorf("sqlite: lineage: %w", err)
	}
	defer rows.Close() //nolint:errcheck // Read errors surface through rows.Err.

	var jobs []store.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// Counts tallies stored jobs by state.
func (s *Store) Counts(ctx context.Context) (store.Counts, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT state, COUNT(*) FROM jobs GROUP BY state")
	if err != nil {
		return nil, fmt.Errorf("sqlite: counts: %w", err)
	}
	defer rows.Close() //nolint:errcheck // Read errors surface through rows.Err.

	counts := store.Counts{}
	for rows.Next() {
		var (
			state string
			total int
		)
		if err := rows.Scan(&state, &total); err != nil {
			return nil, fmt.Errorf("sqlite: counts: %w", err)
		}
		counts[store.State(state)] = total
	}
	return counts, rows.Err()
}

// Prune deletes terminal jobs, and their attempts, last active before the
// cutoff. A job's age is its finish time, falling back to its enqueue time for
// rows that never ran.
func (s *Store) Prune(ctx context.Context, before time.Time, states []store.State) (int, error) {
	cutoff := unix(before)
	if cutoff == 0 {
		return 0, nil // A zero cutoff would prune nothing... treat as a no-op.
	}

	// state_rank 3 is terminal. Unfinished jobs are never pruned.
	where := "state_rank = 3 AND MAX(finished_at, enqueued_at) < ?"
	args := []any{cutoff}
	if len(states) > 0 {
		placeholders := make([]string, 0, len(states))
		for _, state := range states {
			placeholders = append(placeholders, "?")
			args = append(args, string(state))
		}
		where += " AND state IN (" + strings.Join(placeholders, ",") + ")"
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("sqlite: prune: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // No-op once committed.

	if _, err := tx.ExecContext(ctx,
		"DELETE FROM attempts WHERE job_key IN (SELECT key FROM jobs WHERE "+where+")",
		args...); err != nil {
		return 0, fmt.Errorf("sqlite: prune attempts: %w", err)
	}
	res, err := tx.ExecContext(ctx, "DELETE FROM jobs WHERE "+where, args...)
	if err != nil {
		return 0, fmt.Errorf("sqlite: prune jobs: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sqlite: prune: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sqlite: prune: %w", err)
	}
	return int(affected), nil
}

// CountJobs returns how many jobs match, ignoring limit and offset.
func (s *Store) CountJobs(ctx context.Context, filter store.Filter) (int, error) {
	clause, args := predicate(filter)
	var total int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM jobs"+clause, args...).Scan(&total); err != nil {
		return 0, fmt.Errorf("sqlite: count jobs: %w", err)
	}
	return total, nil
}

// GroupJobs summarizes matching jobs by normalized name, state and error.
//
// Grouping on the pattern rather than the raw name is what makes this useful
// when identity lives in the job name: "import-rows-8471" and
// "import-rows-8472" both fall under "import-rows-*".
func (s *Store) GroupJobs(ctx context.Context, filter store.Filter, limit int) ([]store.Group, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	clause, args := predicate(filter)
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			CASE WHEN pattern != '' THEN pattern ELSE '(unnamed)' END AS family,
			state,
			err,
			COUNT(*) AS total,
			MAX(MAX(finished_at, enqueued_at)) AS last_seen
		FROM jobs`+clause+`
		GROUP BY family, state, err
		ORDER BY total DESC, last_seen DESC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: group jobs: %w", err)
	}
	defer rows.Close() //nolint:errcheck // Read errors surface through rows.Err.

	var groups []store.Group
	for rows.Next() {
		var (
			group    store.Group
			state    string
			lastSeen int64
		)
		if err := rows.Scan(&group.Pattern, &state, &group.Err, &group.Count, &lastSeen); err != nil {
			return nil, fmt.Errorf("sqlite: group jobs: %w", err)
		}
		group.State = store.State(state)
		group.LastSeen = fromUnix(lastSeen)
		groups = append(groups, group)
	}
	return groups, rows.Err()
}

// Timeline buckets completions and failures since a cutoff, oldest first.
// Empty buckets are included so a gap reads as a gap rather than a gentle slope.
func (s *Store) Timeline(ctx context.Context, since time.Time, bucket time.Duration) ([]store.Bucket, error) {
	if bucket <= 0 {
		bucket = time.Minute
	}
	// The fill loop emits one struct per bucket, so an absurd ratio (a week of
	// one-second buckets) would allocate hundreds of thousands of them.
	if span := time.Since(since); span/bucket > maxBuckets {
		return nil, fmt.Errorf("sqlite: timeline: %v of %v buckets exceeds the %d bucket limit",
			span, bucket, maxBuckets)
	}
	width := bucket.Microseconds()
	start := unix(since)
	if start == 0 {
		return nil, nil
	}

	// Placeholder order must match the statement: bucket width, then the
	// failure states, then the cutoff.
	failed := make([]string, 0, len(store.FailureStates()))
	args := []any{width}
	for _, state := range store.FailureStates() {
		failed = append(failed, "?")
		args = append(args, string(state))
	}
	args = append(args, start)

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			(MAX(finished_at, enqueued_at) / ?) AS slot,
			SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END) AS done,
			SUM(CASE WHEN state IN (`+strings.Join(failed, ",")+`) THEN 1 ELSE 0 END) AS bad
		FROM jobs
		WHERE state_rank = 3 AND MAX(finished_at, enqueued_at) >= ?
		GROUP BY slot
		ORDER BY slot ASC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: timeline: %w", err)
	}
	defer rows.Close() //nolint:errcheck // Read errors surface through rows.Err.

	seen := map[int64]store.Bucket{}
	for rows.Next() {
		var slot int64
		var done, bad int
		if err := rows.Scan(&slot, &done, &bad); err != nil {
			return nil, fmt.Errorf("sqlite: timeline: %w", err)
		}
		seen[slot] = store.Bucket{Start: fromUnix(slot * width), Completed: done, Failed: bad}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Fill the range so quiet periods are visible.
	first, last := start/width, unix(time.Now())/width
	buckets := make([]store.Bucket, 0, last-first+1)
	for slot := first; slot <= last; slot++ {
		if b, ok := seen[slot]; ok {
			buckets = append(buckets, b)
			continue
		}
		buckets = append(buckets, store.Bucket{Start: fromUnix(slot * width)})
	}
	return buckets, nil
}

// AttributeKeys lists attribute keys present in history, most common first.
func (s *Store) AttributeKeys(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT j.key AS attr, COUNT(*) AS total
		FROM jobs, json_each(jobs.attributes) AS j
		WHERE jobs.attributes != '{}'
		GROUP BY attr
		ORDER BY total DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("sqlite: attribute keys: %w", err)
	}
	defer rows.Close() //nolint:errcheck // Read errors surface through rows.Err.

	var keys []string
	for rows.Next() {
		var (
			key   string
			total int
		)
		if err := rows.Scan(&key, &total); err != nil {
			return nil, fmt.Errorf("sqlite: attribute keys: %w", err)
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

// Close closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

// scanJob reads one job row.
func scanJob(src scanner) (store.Job, error) {
	var (
		job                             store.Job
		state, attrs                    string
		enqueuedAt, startedAt, finished int64
		progressAt                      int64
	)
	if err := src.Scan(&job.Key, &job.ID, &job.Epoch, &job.Queue, &job.Name, &job.Pattern, &state, &job.Err,
		&job.RootID, &job.Parent, &attrs,
		&enqueuedAt, &startedAt, &finished, &job.WaitUS, &job.ExecUS, &job.Attempt,
		&job.ProgressCompleted, &job.ProgressTotal, &job.ProgressStage, &progressAt); err != nil {
		return store.Job{}, fmt.Errorf("sqlite: scan job: %w", err)
	}
	job.State = store.State(state)
	job.EnqueuedAt = fromUnix(enqueuedAt)
	job.StartedAt = fromUnix(startedAt)
	job.FinishedAt = fromUnix(finished)
	job.ProgressAt = fromUnix(progressAt)
	if attrs != "" && attrs != "{}" {
		if err := json.Unmarshal([]byte(attrs), &job.Attributes); err != nil {
			return store.Job{}, fmt.Errorf("sqlite: scan job: attributes: %w", err)
		}
	}
	return job, nil
}

// unix converts a time to microseconds, with zero for the zero time.
func unix(tt time.Time) int64 {
	if tt.IsZero() {
		return 0
	}
	return tt.UnixMicro()
}

// fromUnix converts microseconds back to a time, with zero for zero.
func fromUnix(us int64) time.Time {
	if us == 0 {
		return time.Time{}
	}
	return time.UnixMicro(us)
}
