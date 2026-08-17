package web

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"

	cq "github.com/gnikyt/cq/v2"
)

// Controls are deliberately limited to reversible actions.
//
// Stopping or draining is not offered: cq's Start is a no-op on a stopped
// queue, so a stop from the UI could only be undone by restarting the process.
// That belongs in a deployment tool, not a dashboard button.

// controlView is one queue's control row.
type controlView struct {
	Name   string
	Paused bool
	Min    int
	Max    int
}

// controlsData backs the controls panel.
type controlsData struct {
	Queues []controlView
	CSRF   string
	Notice string
	Error  string
}

// controlViews snapshots each registered queue's controllable state.
func (h *Handler) controlViews() []controlView {
	views := make([]controlView, 0, len(h.queues))
	for name, queue := range h.queueByName {
		min, max := queue.WorkerRange()
		views = append(views, controlView{
			Name:   name,
			Paused: queue.IsPaused(),
			Min:    min,
			Max:    max,
		})
	}
	sortControlViews(views)
	return views
}

// pause halts execution without shutting the queue down.
func (h *Handler) pause(w http.ResponseWriter, r *http.Request) {
	h.control(w, r, "pause", func(queue *cq.Queue, _ *http.Request) (string, error) {
		return "", queue.Pause()
	})
}

// resume allows execution to continue.
func (h *Handler) resume(w http.ResponseWriter, r *http.Request) {
	h.control(w, r, "resume", func(queue *cq.Queue, _ *http.Request) (string, error) {
		return "", queue.Resume()
	})
}

// workers rescales the queue's worker range.
func (h *Handler) workers(w http.ResponseWriter, r *http.Request) {
	h.control(w, r, "workers", func(queue *cq.Queue, req *http.Request) (string, error) {
		min, err := strconv.Atoi(req.FormValue("min"))
		if err != nil {
			return "", fmt.Errorf("min: %w", err)
		}
		max, err := strconv.Atoi(req.FormValue("max"))
		if err != nil {
			return "", fmt.Errorf("max: %w", err)
		}
		return fmt.Sprintf("%d-%d", min, max), queue.SetWorkerRange(min, max)
	})
}

// control runs one authorized, CSRF-checked action against a named queue and
// redirects back to the overview. Every attempt is audited, including the
// rejected ones... a refused control action is the interesting one.
func (h *Handler) control(
	w http.ResponseWriter,
	r *http.Request,
	action string,
	apply func(*cq.Queue, *http.Request) (string, error),
) {
	if !h.controls {
		http.NotFound(w, r) // Controls disabled... the route should not exist.
		return
	}

	queueName := r.FormValue("queue")

	id, ok := h.mayControl(r)
	if !ok {
		h.audit(AuditEntry{Subject: id.Subject, Via: id.Via, Action: action,
			Queue: queueName, Allowed: false})
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !h.checkCSRF(r, id) {
		h.audit(AuditEntry{Subject: id.Subject, Via: id.Via, Action: action,
			Queue: queueName, Allowed: false})
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}

	queue, found := h.queueByName[queueName]
	if !found {
		h.audit(AuditEntry{Subject: id.Subject, Via: id.Via, Action: action,
			Queue: queueName, Allowed: true, Err: fmt.Errorf("unknown queue")})
		http.Error(w, "unknown queue", http.StatusNotFound)
		return
	}

	detail, err := apply(queue, r)
	h.audit(AuditEntry{
		Subject: id.Subject, Via: id.Via, Action: action, Queue: queueName,
		Detail: detail, Allowed: true, Err: err,
	})

	target := h.prefix + "/"
	if err != nil {
		target += "?error=" + urlSafe(err.Error())
	} else {
		target += "?notice=" + urlSafe(action+" applied to "+queueName)
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

// sortControlViews orders control rows by queue name.
func sortControlViews(views []controlView) {
	sort.Slice(views, func(i int, j int) bool {
		return views[i].Name < views[j].Name
	})
}

// urlSafe escapes a message for use in a redirect query string.
func urlSafe(msg string) string {
	return url.QueryEscape(msg)
}
