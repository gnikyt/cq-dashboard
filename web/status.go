package web

import "github.com/gnikyt/cq-dashboard/store"

// Status vocabulary. Every state a job can be shown in, what produced it, and
// one line an operator can act on. Rendered as tooltips on every badge and in
// full on the reference page... a status nobody can define is noise.

// StatusGroup buckets states that mean the same kind of thing, so eight states
// read as five ideas.
type StatusGroup string

const (
	GroupWaiting  StatusGroup = "waiting"  // Accepted, not running.
	GroupRunning  StatusGroup = "running"  // On a worker now.
	GroupDone     StatusGroup = "done"     // Finished cleanly.
	GroupFailed   StatusGroup = "failed"   // Finished badly.
	GroupDropped  StatusGroup = "dropped"  // Deliberately not completed.
	GroupShutdown StatusGroup = "shutdown" // Ended by the process, not the job.
)

// StatusDoc explains one state.
type StatusDoc struct {
	State   store.State
	Group   StatusGroup
	Source  string // What emits it.
	Summary string // What it means for the reader.
}

// statusDocs is the single source of truth for status meaning.
var statusDocs = []StatusDoc{
	{store.StateCreated, GroupWaiting, "cq OnEnqueue",
		"Accepted by the queue and waiting for a free worker."},
	{store.StatePending, GroupWaiting, "cq",
		"Waiting to run. Delayed submissions sit here until their timer fires."},
	{store.StateActive, GroupRunning, "cq OnStart",
		"A worker is running it now."},
	{store.StateCompleted, GroupDone, "cq OnSuccess",
		"Finished and returned no error."},
	{store.StateFailed, GroupFailed, "cq OnFailure",
		"Returned an error, after exhausting any retries it was wrapped with."},
	{store.StateCancelled, GroupFailed, "cq OnFailure",
		"Cancelled through its handle before or during execution."},
	{store.StateDiscarded, GroupDropped, "cq OnDiscard",
		"Deliberately dropped rather than failed... an expired or discarded outcome. Not retried."},
	{store.StateAbandoned, GroupShutdown, "cq OnAbandon",
		"Shutdown ended it before it ever started. Drained jobs are handed back to your code."},
	{store.StateInterrupted, GroupShutdown, "this dashboard",
		"The process died while this was in flight. Its real outcome was never recorded."},
}

// StatusDocs returns the status vocabulary in lifecycle order.
func StatusDocs() []StatusDoc {
	return statusDocs
}

// statusSummary returns the one-line meaning of a state, for badge tooltips.
func statusSummary(state store.State) string {
	for _, doc := range statusDocs {
		if doc.State == state {
			return string(state) + " ... " + doc.Summary
		}
	}
	return string(state)
}

// statusGroup returns the state's group, which drives its badge color.
func statusGroup(state store.State) string {
	for _, doc := range statusDocs {
		if doc.State == state {
			return string(doc.Group)
		}
	}
	return string(GroupWaiting)
}
