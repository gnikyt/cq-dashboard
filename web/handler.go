// Package web serves the dashboard views.
//
// Live queue state (workers, tallies, pause status, pending work, schedules)
// is read straight from cq on each request. Everything historical comes from
// the store, which is the only component that remembers anything across a
// restart.
package web

import (
	"bytes"
	"embed"
	"errors"
	"html/template"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gnikyt/cq-dashboard/sink"
	"github.com/gnikyt/cq-dashboard/store"
	cq "github.com/gnikyt/cq/v2"
)

//go:embed templates/*.html
var templateFS embed.FS

// Handler serves the dashboard.
type Handler struct {
	store      store.Store
	sink       *sink.Sink
	queues     []*cq.Queue
	schedulers []*cq.Scheduler
	templates  *template.Template
	mux        *http.ServeMux
	prefix     string

	queueByName map[string]*cq.Queue
	controls    bool
	auth        Authorizer
	onAudit     func(AuditEntry)
	csrf        string

	login      PasswordCheck
	secret     []byte
	sessionTTL time.Duration

	optErr error // First misconfiguration found while applying options.
}

// Option configures a Handler.
type Option func(*Handler)

// WithQueues registers queues to read live: stats and pending submissions.
func WithQueues(queues ...*cq.Queue) Option {
	return func(h *Handler) {
		h.queues = append(h.queues, queues...)
	}
}

// WithSchedulers registers schedulers whose schedules are listed live.
func WithSchedulers(schedulers ...*cq.Scheduler) Option {
	return func(h *Handler) {
		h.schedulers = append(h.schedulers, schedulers...)
	}
}

// WithControls enables the write actions: pause, resume, and worker scaling.
//
// An Authorizer is required, not optional. These act on a running queue, so
// there is no configuration in which exposing them unauthenticated is correct.
// Controls are off entirely unless this option is passed.
func WithControls(auth Authorizer) Option {
	return func(h *Handler) {
		h.controls = true
		h.auth = auth
	}
}

// WithAudit receives every attempted control action, including rejected ones.
func WithAudit(fn func(AuditEntry)) Option {
	return func(h *Handler) {
		h.onAudit = fn
	}
}

// New creates a handler mounted at prefix (for example "/cq").
// Registered queues and schedulers are read live... history comes from st.
func New(prefix string, st store.Store, sk *sink.Sink, opts ...Option) (*Handler, error) {
	prefix = strings.TrimSuffix(prefix, "/")

	h := &Handler{store: st, sink: sk, prefix: prefix}
	for _, opt := range opts {
		opt(h)
	}

	if h.optErr != nil {
		return nil, h.optErr
	}
	if h.controls && h.auth == nil && h.login == nil {
		return nil, errors.New("web: WithControls requires an Authorizer or WithLogin")
	}

	h.queueByName = make(map[string]*cq.Queue, len(h.queues))
	for i, queue := range h.queues {
		name := queue.Stats().Name
		if name == "" {
			name = "queue-" + strconv.Itoa(i)
		}
		h.queueByName[name] = queue
	}

	csrf, err := newCSRFToken()
	if err != nil {
		return nil, err
	}
	h.csrf = csrf

	tmpl, err := template.New("").Funcs(h.funcs()).ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, err
	}
	h.templates = tmpl

	h.mux = http.NewServeMux()
	h.mux.HandleFunc("GET /{$}", h.overview)
	h.mux.HandleFunc("GET /partials/live", h.livePartial)
	h.mux.HandleFunc("GET /jobs", h.jobs)
	h.mux.HandleFunc("GET /jobs/{key}", h.job)
	h.mux.HandleFunc("GET /failures", h.failures)
	h.mux.HandleFunc("GET /reference", h.reference)
	if h.login != nil {
		h.mux.HandleFunc("GET /login", h.loginForm)
		h.mux.HandleFunc("POST /login", h.loginSubmit)
		h.mux.HandleFunc("POST /logout", h.logout)
	}
	if h.controls {
		h.mux.HandleFunc("POST /control/pause", h.pause)
		h.mux.HandleFunc("POST /control/resume", h.resume)
		h.mux.HandleFunc("POST /control/workers", h.workers)
	}
	return h, nil
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	routed := http.StripPrefix(h.prefix, h.mux)
	if h.login != nil {
		h.gate(routed).ServeHTTP(w, r)
		return
	}
	routed.ServeHTTP(w, r)
}

// funcs are the template helpers.
func (h *Handler) funcs() template.FuncMap {
	return template.FuncMap{
		"prefix": func() string { return h.prefix },
		// Go templates cannot build a map inline, and the filter bar is shared
		// between two pages with different data shapes.
		"dict": func(pairs ...any) (map[string]any, error) {
			if len(pairs)%2 != 0 {
				return nil, errors.New("dict: odd number of arguments")
			}
			out := make(map[string]any, len(pairs)/2)
			for i := 0; i < len(pairs); i += 2 {
				key, ok := pairs[i].(string)
				if !ok {
					return nil, errors.New("dict: keys must be strings")
				}
				out[key] = pairs[i+1]
			}
			return out, nil
		},
		"since": func(tt time.Time) string {
			if tt.IsZero() {
				return "—"
			}
			return compactDuration(time.Since(tt)) + " ago"
		},
		"until": func(tt time.Time) string {
			if tt.IsZero() {
				return "—"
			}
			remaining := time.Until(tt)
			if remaining <= 0 {
				return "due"
			}
			return "in " + compactDuration(remaining)
		},
		"us": func(v int64) string {
			if v == 0 {
				return "—"
			}
			return compactDuration(time.Duration(v) * time.Microsecond)
		},
		"dur": func(d time.Duration) string {
			if d == 0 {
				return "—"
			}
			return compactDuration(d)
		},
		"clock": func(tt time.Time) string {
			if tt.IsZero() {
				return "—"
			}
			return tt.Format("15:04:05")
		},
		// cq's Attempt is a 0-based index... humans count executions.
		"tries":       func(attempt int) int { return attempt + 1 },
		"statusGroup": statusGroup,
		"statusHelp":  statusSummary,
		"progress": func(job store.Job) string {
			if job.ProgressTotal <= 0 {
				return job.ProgressStage
			}
			done := float64(job.ProgressCompleted) / float64(job.ProgressTotal) * 100
			label := strconv.FormatFloat(done, 'f', 0, 64) + "%"
			if job.ProgressStage != "" {
				label += " · " + job.ProgressStage
			}
			return label
		},
	}
}

// compactDuration renders a duration for scanning: two units at most.
func compactDuration(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return strconv.FormatInt(d.Microseconds(), 10) + "µs"
	case d < time.Second:
		if d < 10*time.Millisecond {
			return strconv.FormatFloat(float64(d.Microseconds())/1000, 'f', 1, 64) + "ms"
		}
		return strconv.FormatInt(d.Milliseconds(), 10) + "ms"
	case d < time.Minute:
		return strconv.FormatFloat(d.Seconds(), 'f', 1, 64) + "s"
	case d < time.Hour:
		return strconv.Itoa(int(d.Minutes())) + "m " + strconv.Itoa(int(d.Seconds())%60) + "s"
	case d < 24*time.Hour:
		return strconv.Itoa(int(d.Hours())) + "h " + strconv.Itoa(int(d.Minutes())%60) + "m"
	default:
		return strconv.Itoa(int(d.Hours())/24) + "d " + strconv.Itoa(int(d.Hours())%24) + "h"
	}
}

// filterFrom reads the shared listing filters out of a request.
func filterFrom(r *http.Request) store.Filter {
	query := r.URL.Query()
	filter := store.Filter{
		Queue:     query.Get("queue"),
		Pattern:   query.Get("pattern"),
		Search:    query.Get("q"),
		AttrKey:   query.Get("attr"),
		AttrValue: query.Get("value"),
	}
	if state := store.State(query.Get("state")); state != "" {
		filter.States = []store.State{state}
	}
	return filter
}

// page is the chrome every full page needs.
type page struct {
	Title        string
	Nav          string
	FailureCount int
	Subject      string // Signed-in operator, when a login is configured.
	CSRF         string // For the sign-out form.
}

// newPage builds the shared chrome, including the failure badge in the nav.
// counts may be nil, in which case it is fetched... pass the ones the handler
// already has rather than repeating a GROUP BY over the whole table.
func (h *Handler) newPage(r *http.Request, title string, nav string, counts store.Counts) page {
	if counts == nil {
		if fetched, err := h.store.Counts(r.Context()); err == nil {
			counts = fetched
		}
	}
	failures := 0
	for _, state := range store.FailureStates() {
		failures += counts[state]
	}
	out := page{Title: title, Nav: nav, FailureCount: failures}
	if h.login != nil {
		out.Subject, _ = h.sessionSubject(r)
		out.CSRF = h.csrf
	}
	return out
}

// queueView is one queue's live state.
type queueView struct {
	Stats cq.QueueStats
}

// pendingView is one accepted-but-unfinished job, with the queue holding it.
// State is cq's state mapped into the store's vocabulary so it renders with
// the same badge as history does.
type pendingView struct {
	Queue      string
	Key        string
	State      store.State
	Submission cq.Submission
}

// railStat is one figure on the overview's stat rail.
type railStat struct {
	Label string
	Value string
	Sub   string
	Tone  string // "", "ok", "bad".
	Href  string
}

// overviewData backs the overview page.
type overviewData struct {
	page
	Queues    []queueView
	Pending   []pendingView
	Schedules []cq.ScheduleInfo
	Rail      []railStat
	Counts    store.Counts
	Dropped   uint64
	Written   uint64
	Pruned    uint64
	Epoch     string
	Controls  controlsData
	Timeline  timelineView
}

// timelineView is the throughput sparkline: one bar per bucket, sized as a
// percentage so the markup needs no measurement and no charting library.
type timelineView struct {
	Bars      []timelineBar
	Completed int
	Failed    int
	Window    string
	Peak      int
}

// timelineBar is one bucket, as percentages of the tallest bucket.
type timelineBar struct {
	CompletedPct int
	FailedPct    int
	Label        string
}

// overview renders live queue state plus stored totals.
func (h *Handler) overview(w http.ResponseWriter, r *http.Request) {
	counts, err := h.store.Counts(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data := h.liveData()
	data.page = h.newPage(r, "Overview", "overview", counts)
	data.Counts = counts
	data.Written = h.sink.Written()
	data.Pruned = h.sink.Pruned()
	data.Epoch = h.sink.Epoch()
	data.Rail = h.rail(data, counts)
	data.Timeline = h.timeline(r)
	// Dropped events mean the history is lying by omission, so the rail says
	// so... but only when it has actually happened.
	if dropped := h.sink.Dropped(); dropped > 0 {
		data.Rail = append(data.Rail, railStat{
			Label: "Dropped", Value: strconv.FormatUint(dropped, 10),
			Sub: "history has gaps", Tone: "bad",
			Href: h.prefix + "/reference#boundaries",
		})
	}
	if h.controls {
		data.Controls.Notice = r.URL.Query().Get("notice")
		data.Controls.Error = r.URL.Query().Get("error")
	}
	h.render(w, "overview.html", data)
}

// rail summarizes the figures an operator checks first.
func (h *Handler) rail(data overviewData, counts store.Counts) []railStat {
	var running, idle, pending, active, capacity, paused, stopped int
	for _, view := range data.Queues {
		running += view.Stats.RunningWorkers
		idle += view.Stats.IdleWorkers
		pending += view.Stats.PendingJobs
		active += view.Stats.ActiveJobs
		capacity += view.Stats.Capacity
		if view.Stats.Paused {
			paused++
		}
		if view.Stats.Stopped {
			stopped++
		}
	}

	failures := 0
	for _, state := range store.FailureStates() {
		failures += counts[state]
	}

	health, tone := "all running", "ok"
	switch {
	case stopped > 0:
		health, tone = "stopped", "bad"
	case paused > 0:
		health, tone = "paused", "bad"
	}

	return []railStat{
		{Label: "Queues", Value: strconv.Itoa(len(data.Queues)), Sub: health, Tone: tone},
		{Label: "Workers", Value: strconv.Itoa(running), Sub: strconv.Itoa(idle) + " idle"},
		{Label: "Active", Value: strconv.Itoa(active), Sub: "running now"},
		{Label: "Pending", Value: strconv.Itoa(pending), Sub: "of " + strconv.Itoa(capacity) + " buffer"},
		{Label: "Completed", Value: strconv.Itoa(counts[store.StateCompleted]), Sub: "in history"},
		{Label: "Failures", Value: strconv.Itoa(failures), Sub: "in history",
			Tone: toneFor(failures), Href: h.prefix + "/failures"},
	}
}

// timeline builds the last hour of throughput for the sparkline.
func (h *Handler) timeline(r *http.Request) timelineView {
	const window = time.Hour
	buckets, err := h.store.Timeline(r.Context(), time.Now().Add(-window), time.Minute)
	if err != nil || len(buckets) == 0 {
		return timelineView{Window: "last hour"}
	}

	view := timelineView{Window: "last hour", Bars: make([]timelineBar, 0, len(buckets))}
	for _, bucket := range buckets {
		view.Completed += bucket.Completed
		view.Failed += bucket.Failed
		if total := bucket.Completed + bucket.Failed; total > view.Peak {
			view.Peak = total
		}
	}
	if view.Peak == 0 {
		return view
	}

	for _, bucket := range buckets {
		view.Bars = append(view.Bars, timelineBar{
			CompletedPct: bucket.Completed * 100 / view.Peak,
			FailedPct:    bucket.Failed * 100 / view.Peak,
			Label: bucket.Start.Format("15:04") + ": " +
				strconv.Itoa(bucket.Completed) + " done, " +
				strconv.Itoa(bucket.Failed) + " failed",
		})
	}
	return view
}

// toneFor colours a count only when it is worth attention.
func toneFor(count int) string {
	if count > 0 {
		return "bad"
	}
	return ""
}

// livePartial re-renders everything read live from cq, for polling.
func (h *Handler) livePartial(w http.ResponseWriter, r *http.Request) {
	h.render(w, "live.html", h.liveData())
}

// liveData snapshots everything read straight from cq rather than the store.
// The controls ride along because they live inside the queues panel, which is
// part of the polled region.
func (h *Handler) liveData() overviewData {
	data := overviewData{
		Queues:    h.queueViews(),
		Pending:   h.pendingViews(),
		Schedules: h.scheduleViews(),
		Dropped:   h.sink.Dropped(),
	}
	if h.controls {
		data.Controls = controlsData{Queues: h.controlViews(), CSRF: h.csrf}
	}
	return data
}

// perPage is one screen of history. Small enough to scan, large enough that
// paging is rare.
const perPage = 50

// listing is the shared shape of a filtered, paginated job list.
type listing struct {
	Jobs     []store.Job
	Filter   store.Filter
	Total    int
	Shown    int
	Page     int
	Pages    int
	First    int // 1-based index of the first row shown.
	Last     int
	AttrKeys []string
	Queues   []string
	Path     string     // Page path, for pager links.
	Query    url.Values // Active filters, carried across pages.
}

// HasPrev and HasNext drive the pager.
func (l listing) HasPrev() bool { return l.Page > 1 }
func (l listing) HasNext() bool { return l.Page < l.Pages }

// PrevURL and NextURL are built here rather than concatenated in the template:
// html/template escapes "&" inside an href, which silently breaks a hand-built
// query string.
func (l listing) PrevURL() template.URL { return l.pageURL(l.Page - 1) }
func (l listing) NextURL() template.URL { return l.pageURL(l.Page + 1) }

// pageURL renders one pager link with the active filters intact.
func (l listing) pageURL(page int) template.URL {
	values := url.Values{}
	for key, vals := range l.Query {
		values[key] = vals
	}
	// Page one is always live; deeper pages read a frozen window.
	if page > 1 && values.Get("before") == "" {
		values.Set("before", strconv.FormatInt(time.Now().UnixMicro(), 10))
	}
	values.Set("page", strconv.Itoa(page))
	return template.URL(l.Path + "?" + values.Encode())
}

// list runs one filtered, paginated query. path is where pager links point.
func (h *Handler) list(r *http.Request, filter store.Filter, path string) (listing, error) {
	page := 1
	if raw := r.URL.Query().Get("page"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 1 {
			page = parsed
		}
	}
	filter.Limit = perPage
	filter.Offset = (page - 1) * perPage

	// Freeze the window. Without this, jobs arriving between page loads shift
	// every row along and OFFSET silently repeats and skips them.
	if raw := r.URL.Query().Get("before"); raw != "" {
		if micros, err := strconv.ParseInt(raw, 10, 64); err == nil {
			filter.Before = time.UnixMicro(micros)
		}
	} else if page > 1 {
		filter.Before = time.Now()
	}

	total, err := h.store.CountJobs(r.Context(), filter)
	if err != nil {
		return listing{}, err
	}
	pages := (total + perPage - 1) / perPage
	if pages == 0 {
		pages = 1
	}
	// A stale page number from a shrinking list should land on the last page,
	// not on an empty one.
	if page > pages {
		page = pages
		filter.Offset = (page - 1) * perPage
	}

	jobs, err := h.store.Jobs(r.Context(), filter)
	if err != nil {
		return listing{}, err
	}
	keys, err := h.store.AttributeKeys(r.Context(), 12)
	if err != nil {
		return listing{}, err
	}

	first := 0
	if len(jobs) > 0 {
		first = filter.Offset + 1
	}
	return listing{
		Jobs: jobs, Filter: filter, Total: total, Shown: len(jobs),
		Page: page, Pages: pages, First: first, Last: filter.Offset + len(jobs),
		AttrKeys: keys, Queues: h.queueNames(),
		Path: h.prefix + path, Query: filterQuery(filter),
	}, nil
}

// queueNames lists registered queues for the filter dropdown.
func (h *Handler) queueNames() []string {
	names := make([]string, 0, len(h.queueByName))
	for name := range h.queueByName {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// filterQuery rebuilds the active filters, so pager links keep them without
// restating each one.
func filterQuery(filter store.Filter) url.Values {
	values := url.Values{}
	if filter.Queue != "" {
		values.Set("queue", filter.Queue)
	}
	if filter.Pattern != "" {
		values.Set("pattern", filter.Pattern)
	}
	if filter.Search != "" {
		values.Set("q", filter.Search)
	}
	if filter.AttrKey != "" {
		values.Set("attr", filter.AttrKey)
	}
	if filter.AttrValue != "" {
		values.Set("value", filter.AttrValue)
	}
	if len(filter.States) == 1 {
		values.Set("state", string(filter.States[0]))
	}
	if !filter.Before.IsZero() {
		values.Set("before", strconv.FormatInt(filter.Before.UnixMicro(), 10))
	}
	return values
}

// jobsData backs the jobs list.
type jobsData struct {
	page
	listing
	States []store.State
	State  store.State
}

// jobs renders the filtered job list.
func (h *Handler) jobs(w http.ResponseWriter, r *http.Request) {
	filter := filterFrom(r)
	rows, err := h.list(r, filter, "/jobs")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	selected := store.State("")
	if len(filter.States) == 1 {
		selected = filter.States[0]
	}
	h.render(w, "jobs.html", jobsData{
		page:    h.newPage(r, "Jobs", "jobs", nil),
		listing: rows,
		States:  allStates,
		State:   selected,
	})
}

// stateCount is one state's share of the failures.
type stateCount struct {
	State store.State
	Count int
}

// failuresData backs the failures page.
type failuresData struct {
	page
	listing
	Groups  []store.Group
	Counts  []stateCount
	Overall int
	State   store.State
	Grouped bool
}

// failures shows what did not complete, grouped by job family and error.
//
// Thousands of identical rows are unreadable, so the default view collapses
// them by normalized name plus error, and drilling into a family lists the
// individual executions. These are cq execution outcomes, not a transport's
// dead-letter queue.
func (h *Handler) failures(w http.ResponseWriter, r *http.Request) {
	filter := filterFrom(r)
	if len(filter.States) == 0 {
		filter.States = store.FailureStates()
	}

	// A family is chosen, or the operator asked for the flat list.
	grouped := filter.Pattern == "" && r.URL.Query().Get("view") != "list"

	data := failuresData{Grouped: grouped}
	if selected := store.State(r.URL.Query().Get("state")); selected != "" {
		data.State = selected
	}

	if grouped {
		groups, err := h.store.GroupJobs(r.Context(), filter, 50)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data.Groups = groups
		data.Filter = filter
	} else {
		rows, err := h.list(r, filter, "/failures")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data.listing = rows
	}

	counts, err := h.store.Counts(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data.page = h.newPage(r, "Failures", "failures", counts)
	for _, state := range store.FailureStates() {
		if count := counts[state]; count > 0 {
			data.Counts = append(data.Counts, stateCount{State: state, Count: count})
			data.Overall += count
		}
	}
	h.render(w, "failures.html", data)
}

// jobData backs the job detail page.
type jobData struct {
	page
	Job      store.Job
	Attempts []store.Attempt
	Lineage  []store.Job
}

// job renders one submission with its attempts and reschedule lineage.
func (h *Handler) job(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	job, attempts, err := h.store.Job(r.Context(), key)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	// A logical job spans every submission sharing this root... reschedule and
	// release mint a new ID per hop.
	lineage, err := h.store.Lineage(r.Context(), job.Epoch, job.RootID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.render(w, "job.html", jobData{
		page:     h.newPage(r, "Job "+job.ID, "jobs", nil),
		Job:      job,
		Attempts: attempts,
		Lineage:  lineage,
	})
}

// referenceData backs the reference page.
type referenceData struct {
	page
	Statuses []StatusDoc
	Controls bool
}

// reference explains the status vocabulary and this dashboard's boundaries...
// everything that would otherwise be prose crowding the data.
func (h *Handler) reference(w http.ResponseWriter, r *http.Request) {
	h.render(w, "reference.html", referenceData{
		page:     h.newPage(r, "Reference", "reference", nil),
		Statuses: StatusDocs(),
		Controls: h.controls,
	})
}

// queueViews snapshots every registered queue.
func (h *Handler) queueViews() []queueView {
	views := make([]queueView, 0, len(h.queues))
	for _, queue := range h.queues {
		views = append(views, queueView{Stats: queue.Stats()})
	}
	return views
}

// pendingViews lists accepted jobs that have not finished, across all queues.
// This is the one view the store cannot answer: unstarted work exists only in
// the running process.
func (h *Handler) pendingViews() []pendingView {
	var views []pendingView
	for _, queue := range h.queues {
		name := queue.Stats().Name
		for _, submission := range queue.Submissions() {
			views = append(views, pendingView{
				Queue:      name,
				Key:        store.KeyFor(h.sink.Epoch(), submission.Meta.ID),
				State:      store.State(submission.State.String()),
				Submission: submission,
			})
		}
	}
	return views
}

// scheduleViews lists every schedule across all registered schedulers.
func (h *Handler) scheduleViews() []cq.ScheduleInfo {
	var infos []cq.ScheduleInfo
	for _, scheduler := range h.schedulers {
		infos = append(infos, scheduler.Describe()...)
	}
	return infos
}

// render writes one template, reporting failures as a 500.
//
// Rendering to a buffer first matters: html/template streams, so writing
// straight to the ResponseWriter would commit a 200 and half a page before an
// error surfaced, leaving the client a truncated document with a Go error
// stapled to the end.
func (h *Handler) render(w http.ResponseWriter, name string, data any) {
	var buf bytes.Buffer
	if err := h.templates.ExecuteTemplate(&buf, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

// allStates is the filter dropdown's options.
var allStates = []store.State{
	store.StateCreated, store.StatePending, store.StateActive,
	store.StateCompleted, store.StateFailed, store.StateCancelled,
	store.StateDiscarded, store.StateAbandoned, store.StateInterrupted,
}
