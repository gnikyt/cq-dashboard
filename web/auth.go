package web

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strings"
	"time"
)

// Authorizer decides whether a request may use the dashboard's controls.
// subject identifies the caller for the audit log... a username, a token
// label, anything meaningful to the operator reading it later.
//
// Read-only views are not authorized here. Put your own middleware in front of
// the handler if the history itself is sensitive.
type Authorizer func(r *http.Request) (subject string, ok bool)

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
	Subject string // From the Authorizer... empty when the request was rejected.
	Action  string // "pause", "resume", "workers".
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

// checkCSRF reports whether a control request carries this handler's token.
//
// The token is rendered only into authorized pages, so a cross-site form post
// cannot supply it. Compared in constant time.
func (h *Handler) checkCSRF(r *http.Request) bool {
	got := r.FormValue("csrf")
	return subtle.ConstantTimeCompare([]byte(got), []byte(h.csrf)) == 1
}

// authorize accepts either a signed session or the configured Authorizer,
// defaulting to deny.
func (h *Handler) authorize(r *http.Request) (string, bool) {
	if h.login != nil {
		if subject, ok := h.sessionSubject(r); ok {
			return subject, true
		}
	}
	if h.auth == nil {
		return "", false
	}
	return h.auth(r)
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
// compared in constant time. Unlike a bearer token, a browser sends these on
// ordinary page loads, which is what makes it usable for protecting the views
// rather than only the control endpoints.
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

// RequireAuth gates an entire handler, so the read-only views are protected
// too... history carries job names, attributes and error strings.
//
// Pass the same Authorizer to WithControls and one decision covers both, with
// the control endpoints additionally requiring their CSRF token.
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
