package web

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strings"
	"time"
)

// Authorizer recognizes a caller from a request, returning a subject for the
// audit log... a username, a token label, anything meaningful to the operator
// reading it later.
//
// It answers "who is this", not "what may they do". Pass one to WithTokens to
// register it as a credential; ControlPolicy decides what the resulting
// identity is allowed to do.
type Authorizer func(r *http.Request) (subject string, ok bool)

// Credential is how a request proved who it was.
type Credential string

const (
	// ViaSession is the login form's signed cookie: a human in a browser.
	ViaSession Credential = "session"
	// ViaToken is a credential from WithTokens, carried in a header: a script.
	ViaToken Credential = "token"
)

// Identity is who a request came from, once a credential accepted it.
//
// Via matters beyond the audit trail: session requests ride a cookie, so they
// carry a CSRF token, while token requests cannot be forged by another site
// and do not.
type Identity struct {
	Subject string
	Via     Credential
}

// ControlPolicy decides whether an identity may use the write controls.
type ControlPolicy func(Identity) bool

// AllowSignedIn permits any authenticated caller, human or script. The usual
// choice: the credentials are the gate, and everyone holding one may act.
func AllowSignedIn(Identity) bool { return true }

// AllowSubjects permits only the named subjects, so a shared login can still
// have a smaller set of operators who may pause a queue.
func AllowSubjects(subjects ...string) ControlPolicy {
	allowed := make(map[string]struct{}, len(subjects))
	for _, subject := range subjects {
		allowed[subject] = struct{}{}
	}
	return func(id Identity) bool {
		_, ok := allowed[id.Subject]
		return ok
	}
}

// BearerToken authorizes requests carrying a matching bearer token, compared
// in constant time. Convenience for internal tools... anything with real users
// should wire its own Authorizer against its existing session or SSO.
//
// The token travels in a header, so serve the dashboard over TLS.
func BearerToken(subject string, token string) Authorizer {
	want := []byte(token)
	return func(r *http.Request) (string, bool) {
		if token == "" {
			return "", false // An empty token would authorize everyone.
		}
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), want) != 1 {
			return "", false
		}
		return subject, true
	}
}

// AuditEntry records one attempted control action.
type AuditEntry struct {
	At      time.Time
	Subject string     // Empty when the request was rejected.
	Via     Credential // How the caller authenticated: session or token.
	Action  string     // "pause", "resume", "workers".
	Queue   string
	Detail  string // Action-specific, such as the requested worker range.
	Allowed bool   // False when authorization or CSRF rejected the request.
	Err     error  // Non-nil when the action itself failed.
}

// newCSRFToken returns a random token for control form submissions.
func newCSRFToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// identify resolves who a request is from: the session first, then each
// registered token credential in order. Defaults to nobody.
func (h *Handler) identify(r *http.Request) (Identity, bool) {
	if h.login != nil {
		if subject, ok := h.sessionSubject(r); ok {
			return Identity{Subject: subject, Via: ViaSession}, true
		}
	}
	for _, auth := range h.tokens {
		if auth == nil {
			continue
		}
		if subject, ok := auth(r); ok {
			return Identity{Subject: subject, Via: ViaToken}, true
		}
	}
	return Identity{}, false
}

// csrfFor returns the token this request should carry, and whether the caller
// is entitled to see one at all.
//
// The token is derived from the session, so it differs per operator and dies
// with the session rather than living as long as the process. Only sessions get
// one: a token caller has no cookie to protect and is never sent a form.
func (h *Handler) csrfFor(r *http.Request) (string, bool) {
	if h.login == nil {
		return "", false
	}
	found, ok := h.sessionFrom(r)
	if !ok {
		return "", false
	}
	return h.sign("csrf:" + found.ID), true
}

// checkCSRF reports whether a state-changing request from id is allowed to
// proceed.
//
// Token identities are exempt, and that is not a shortcut: CSRF defends
// credentials the browser attaches by itself, and no other site can make a
// browser send an Authorization header cross-origin. Sessions ride a cookie,
// so they are checked... read from the body only, since a token accepted from
// the query string would weaken the double-submit story, and compared in
// constant time.
func (h *Handler) checkCSRF(r *http.Request, id Identity) bool {
	if id.Via == ViaToken {
		return true
	}
	want, ok := h.csrfFor(r)
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(r.PostFormValue("csrf")), []byte(want)) == 1
}

// mayControl reports whether a request may run a write action, and who it is.
func (h *Handler) mayControl(r *http.Request) (Identity, bool) {
	id, ok := h.identify(r)
	if !ok || h.controlPolicy == nil {
		return Identity{}, false
	}
	return id, h.controlPolicy(id)
}

// audit reports one control attempt to the configured sink.
func (h *Handler) audit(entry AuditEntry) {
	if h.onAudit == nil {
		return
	}
	entry.At = time.Now()
	h.onAudit(entry)
}

// BasicAuth authorizes requests carrying matching HTTP basic credentials,
// compared in constant time. A browser resends them on every request to the
// origin once prompted, which makes them usable for gating the views from
// outside via RequireAuth.
//
// For that same reason do not pass this to WithTokens: token identities skip
// the CSRF check because no other site can make a browser send an
// Authorization header, and browser-cached basic credentials break that
// assumption. Use WithLogin for humans, WithTokens for scripts, and this for
// RequireAuth in front of the handler.
//
// Credentials travel base64-encoded, not encrypted. Serve over TLS.
func BasicAuth(username string, password string) Authorizer {
	wantUser, wantPass := []byte(username), []byte(password)
	return func(r *http.Request) (string, bool) {
		if username == "" || password == "" {
			return "", false // Empty credentials would authorize everyone.
		}
		gotUser, gotPass, ok := r.BasicAuth()
		if !ok {
			return "", false
		}
		// Compare both, always, so timing does not reveal which half matched.
		userOK := subtle.ConstantTimeCompare([]byte(gotUser), wantUser) == 1
		passOK := subtle.ConstantTimeCompare([]byte(gotPass), wantPass) == 1
		if !userOK || !passOK {
			return "", false
		}
		return gotUser, true
	}
}

// RequireAuth gates an entire handler from outside, so the read-only views are
// protected too... history carries job names, attributes and error strings.
//
// Prefer WithLogin or WithTokens inside the handler, which the dashboard knows
// about: they name the caller in the audit log, drive the sign-out control, and
// answer JSON requests with 401 rather than an HTML challenge. This exists for
// gating from the outside, and for BasicAuth.
//
// The challenge header asks browsers for basic credentials. Bearer clients can
// ignore it; they are rejected on the token, not the header.
func RequireAuth(next http.Handler, auth Authorizer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth == nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if _, ok := auth(r); !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="cq dashboard", charset="UTF-8"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
