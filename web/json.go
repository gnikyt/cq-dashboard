package web

// The JSON surface is deliberately hand-written rather than marshalled straight
// from the store types. Those are internal shapes, free to change; these are a
// promise to whoever wrote a client. Keeping them apart is what lets both be
// true at once.
//
// The promise: field names never change meaning, new fields may appear, and a
// breaking change bumps apiVersion. Times are RFC 3339, durations are integer
// microseconds (cq jobs routinely finish in well under a millisecond), and
// absent times are omitted rather than sent as a zero date.

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gnikyt/cq-dashboard/store"
)

// apiVersion is the wire contract's version, carried on every response so a
// client can refuse a payload it does not understand.
const apiVersion = 1

// jobJSON is one job as the API presents it.
type jobJSON struct {
	Key     string `json:"key"`
	ID      string `json:"id"`
	Epoch   string `json:"epoch"`
	Queue   string `json:"queue"`
	Name    string `json:"name"`
	Pattern string `json:"pattern"`
	State   string `json:"state"`
	Err     string `json:"error,omitempty"`

	RootID string `json:"root_id,omitempty"`
	Parent string `json:"parent_id,omitempty"`

	Attributes map[string]string `json:"attributes,omitempty"`

	EnqueuedAt *string `json:"enqueued_at,omitempty"`
	StartedAt  *string `json:"started_at,omitempty"`
	FinishedAt *string `json:"finished_at,omitempty"`
	WaitUS     int64   `json:"wait_us"`
	ExecUS     int64   `json:"exec_us"`
	Attempt    int     `json:"attempt"`

	Progress *progressJSON `json:"progress,omitempty"`
}

// progressJSON is a job's latest progress report, absent when it never made
// one. Total is zero when the job reports a stage without a count, which is
// why complete is only meaningful alongside it.
type progressJSON struct {
	Completed int64   `json:"completed"`
	Total     int64   `json:"total"`
	Stage     string  `json:"stage,omitempty"`
	At        *string `json:"at,omitempty"`
}

// attemptJSON is one execution attempt of a job.
type attemptJSON struct {
	Attempt    int     `json:"attempt"`
	State      string  `json:"state"`
	Err        string  `json:"error,omitempty"`
	StartedAt  *string `json:"started_at,omitempty"`
	FinishedAt *string `json:"finished_at,omitempty"`
}

// jobsResponse is a filtered page of history.
//
// Before echoes the window the page was read against: pass it back on the next
// request and the listing stays consistent while jobs keep arriving.
type jobsResponse struct {
	APIVersion int       `json:"api_version"`
	Jobs       []jobJSON `json:"jobs"`
	Total      int       `json:"total"`
	Page       int       `json:"page"`
	Pages      int       `json:"pages"`
	PerPage    int       `json:"per_page"`
	Before     string    `json:"before,omitempty"`
}

// jobResponse is one job with everything known about it.
type jobResponse struct {
	APIVersion int           `json:"api_version"`
	Job        jobJSON       `json:"job"`
	Attempts   []attemptJSON `json:"attempts"`
	Lineage    []jobJSON     `json:"lineage,omitempty"`
}

// liveResponse is the overview: what is happening now, plus stored tallies.
type liveResponse struct {
	APIVersion int            `json:"api_version"`
	Epoch      string         `json:"epoch"`
	Queues     []queueJSON    `json:"queues"`
	Pending    []pendingJSON  `json:"pending"`
	Schedules  []scheduleJSON `json:"schedules"`
	Counts     map[string]int `json:"counts"`
	Sink       sinkJSON       `json:"sink"`
}

// queueJSON is one queue's live state, as cq reports it. Tallies are this
// process's own counters, so they reset with it... history is what survives.
type queueJSON struct {
	Name    string `json:"name"`
	Paused  bool   `json:"paused"`
	Stopped bool   `json:"stopped"`

	WorkersMin     int `json:"workers_min"`
	WorkersMax     int `json:"workers_max"`
	WorkersRunning int `json:"workers_running"`
	WorkersIdle    int `json:"workers_idle"`
	Capacity       int `json:"capacity"`

	Created     int `json:"created"`
	Pending     int `json:"pending"`
	Active      int `json:"active"`
	Completed   int `json:"completed"`
	Failed      int `json:"failed"`
	Discarded   int `json:"discarded"`
	Cancelled   int `json:"cancelled"`
	Rescheduled int `json:"rescheduled"`
	Released    int `json:"released"`
}

// pendingJSON is one accepted job that has not finished, read from the queue
// rather than the store: it lives only in memory, so a restart loses it.
type pendingJSON struct {
	Queue      string  `json:"queue"`
	Key        string  `json:"key"`
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	State      string  `json:"state"`
	EnqueuedAt *string `json:"enqueued_at,omitempty"`
}

// scheduleJSON is one registered schedule. Interval and run_at are each set
// only for the kind that uses them.
type scheduleJSON struct {
	ID          string  `json:"id"`
	Kind        string  `json:"kind"`
	IntervalUS  int64   `json:"interval_us,omitempty"`
	RunAt       *string `json:"run_at,omitempty"`
	NextFireAt  *string `json:"next_fire_at,omitempty"`
	Submissions uint64  `json:"submissions"`
	LastErr     string  `json:"last_error,omitempty"`
}

// sinkJSON reports the recorder's own health. Dropped above zero means the
// history is incomplete, which is the price of never blocking a worker.
type sinkJSON struct {
	Dropped uint64 `json:"dropped"`
	Written uint64 `json:"written"`
	Pruned  uint64 `json:"pruned"`
}

// errorJSON is the body of every failed API response.
type errorJSON struct {
	APIVersion int    `json:"api_version"`
	Error      string `json:"error"`
}

// wantsJSON reports whether the request asked for JSON, by path suffix rather
// than by sniffing Accept: the suffix is what the routes advertise, and a
// browser sends Accept headers that are hard to read intent from.
func wantsJSON(r *http.Request) bool {
	return strings.HasSuffix(r.URL.Path, ".json")
}

// writeJSON sends a response, buffering first so a marshal failure cannot leave
// a half-written body behind a 200.
func writeJSON(w http.ResponseWriter, status int, payload any) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "encoding failed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// History is per-request and often sensitive... no caches, no proxies.
	w.Header().Set("Cache-Control", "no-store, private")
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}

// writeJSONError sends a failure body. No CORS headers are ever set here or
// anywhere else: these endpoints accept the session cookie, so allowing another
// origin to read the response would hand a logged-in user's history to any site
// they visit.
func writeJSONError(w http.ResponseWriter, status int, message string) {
	encoded, err := json.Marshal(errorJSON{APIVersion: apiVersion, Error: message})
	if err != nil {
		http.Error(w, message, status)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store, private")
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}

// jobsJSON serves a filtered page of history.
func (h *Handler) jobsJSON(w http.ResponseWriter, r *http.Request) {
	filter := filterFrom(r)
	// Freeze the window on the first page too, unlike the HTML pager: a client
	// has nowhere to read it from later, and paging without it silently repeats
	// and skips rows as new jobs arrive.
	if filter.Before.IsZero() {
		filter.Before = time.Now()
	}
	rows, err := h.list(r, filter, "/jobs.json")
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	jobs := make([]jobJSON, 0, len(rows.Jobs))
	for _, job := range rows.Jobs {
		jobs = append(jobs, toJobJSON(job))
	}
	payload := jobsResponse{
		APIVersion: apiVersion,
		Jobs:       jobs,
		Total:      rows.Total,
		Page:       rows.Page,
		Pages:      rows.Pages,
		PerPage:    perPage,
	}
	// The window the page was read against. Send it back on the next request
	// and the listing stays consistent while new jobs keep arriving.
	if before := jsonTime(rows.Filter.Before); before != nil {
		payload.Before = *before
	}
	writeJSON(w, http.StatusOK, payload)
}

// jobJSONByKey serves one job, its attempts and its reschedule lineage.
func (h *Handler) jobJSONByKey(w http.ResponseWriter, r *http.Request, key string) {
	job, attempts, err := h.store.Job(r.Context(), key)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "job not found")
		return
	}

	payload := jobResponse{APIVersion: apiVersion, Job: toJobJSON(job)}
	payload.Attempts = make([]attemptJSON, 0, len(attempts))
	for _, attempt := range attempts {
		payload.Attempts = append(payload.Attempts, attemptJSON{
			Attempt:    attempt.Attempt,
			State:      string(attempt.State),
			Err:        attempt.Err,
			StartedAt:  jsonTime(attempt.StartedAt),
			FinishedAt: jsonTime(attempt.FinishedAt),
		})
	}

	// A reschedule chain is one logical job across many submissions, so the
	// hops belong with it rather than behind another request.
	root := job.RootID
	if root == "" {
		root = job.ID
	}
	if lineage, err := h.store.Lineage(r.Context(), job.Epoch, job.Queue, root); err == nil && len(lineage) > 1 {
		for _, hop := range lineage {
			payload.Lineage = append(payload.Lineage, toJobJSON(hop))
		}
	}
	writeJSON(w, http.StatusOK, payload)
}

// liveJSON serves the overview: live queue state plus stored tallies.
func (h *Handler) liveJSON(w http.ResponseWriter, r *http.Request) {
	counts, err := h.store.Counts(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	payload := liveResponse{
		APIVersion: apiVersion,
		Epoch:      h.sink.Epoch(),
		Counts:     make(map[string]int, len(counts)),
		Sink: sinkJSON{
			Dropped: h.sink.Dropped(),
			Written: h.sink.Written(),
			Pruned:  h.sink.Pruned(),
		},
	}
	for state, total := range counts {
		payload.Counts[string(state)] = total
	}

	payload.Queues = make([]queueJSON, 0, len(h.queues))
	for _, view := range h.queueViews() {
		stats := view.Stats
		payload.Queues = append(payload.Queues, queueJSON{
			Name:           stats.Name,
			Paused:         stats.Paused,
			Stopped:        stats.Stopped,
			WorkersMin:     stats.WorkersMin,
			WorkersMax:     stats.WorkersMax,
			WorkersRunning: stats.RunningWorkers,
			WorkersIdle:    stats.IdleWorkers,
			Capacity:       stats.Capacity,
			Created:        stats.CreatedJobs,
			Pending:        stats.PendingJobs,
			Active:         stats.ActiveJobs,
			Completed:      stats.CompletedJobs,
			Failed:         stats.FailedJobs,
			Discarded:      stats.DiscardedJobs,
			Cancelled:      stats.CancelledJobs,
			Rescheduled:    stats.RescheduledJobs,
			Released:       stats.ReleasedJobs,
		})
	}

	payload.Pending = make([]pendingJSON, 0)
	for _, pending := range h.pendingViews() {
		payload.Pending = append(payload.Pending, pendingJSON{
			Queue:      pending.Queue,
			Key:        pending.Key,
			ID:         pending.Submission.Meta.ID,
			Name:       pending.Submission.Meta.Name,
			State:      string(pending.State),
			EnqueuedAt: jsonTime(pending.Submission.Meta.EnqueuedAt),
		})
	}

	payload.Schedules = make([]scheduleJSON, 0)
	for _, info := range h.scheduleViews() {
		schedule := scheduleJSON{
			ID:          info.ID,
			Kind:        string(info.Kind),
			IntervalUS:  info.Interval.Microseconds(),
			RunAt:       jsonTime(info.RunAt),
			NextFireAt:  jsonTime(info.NextFireAt),
			Submissions: info.Submissions,
		}
		if info.LastErr != nil {
			schedule.LastErr = info.LastErr.Error()
		}
		payload.Schedules = append(payload.Schedules, schedule)
	}
	writeJSON(w, http.StatusOK, payload)
}

// toJobJSON converts a stored job to its wire form.
func toJobJSON(job store.Job) jobJSON {
	out := jobJSON{
		Key:        job.Key,
		ID:         job.ID,
		Epoch:      job.Epoch,
		Queue:      job.Queue,
		Name:       job.Name,
		Pattern:    job.Pattern,
		State:      string(job.State),
		Err:        job.Err,
		RootID:     job.RootID,
		Parent:     job.Parent,
		Attributes: job.Attributes,
		EnqueuedAt: jsonTime(job.EnqueuedAt),
		StartedAt:  jsonTime(job.StartedAt),
		FinishedAt: jsonTime(job.FinishedAt),
		WaitUS:     job.WaitUS,
		ExecUS:     job.ExecUS,
		Attempt:    job.Attempt,
	}
	if !job.ProgressAt.IsZero() {
		out.Progress = &progressJSON{
			Completed: job.ProgressCompleted,
			Total:     job.ProgressTotal,
			Stage:     job.ProgressStage,
			At:        jsonTime(job.ProgressAt),
		}
	}
	return out
}

// timeFormat is RFC 3339 with microseconds, matching what the store keeps.
const timeFormat = "2006-01-02T15:04:05.000000Z07:00"

// jsonTime renders a time, or nothing at all when it was never set. A pointer
// rather than an empty string: "this never happened" and "this happened at the
// zero time" are different answers, and only one of them is real.
func jsonTime(tt time.Time) *string {
	if tt.IsZero() {
		return nil
	}
	formatted := tt.UTC().Format(timeFormat)
	return &formatted
}
