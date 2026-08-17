package web

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

// A login form with a signed cookie, rather than HTTP basic auth.
//
// Basic auth relies on the browser's native dialog, which cannot be styled,
// offers no way to log out, and simply does not appear in embedded webviews.
// A form works everywhere and can say what went wrong.

const (
	// sessionCookie is where the signed session lives.
	sessionCookie = "cq_dashboard_session"
	// loginCSRFCookie guards the login form itself, which has no session yet.
	loginCSRFCookie = "cq_dashboard_login"

	// DefaultSessionTTL is how long a login lasts before it must be repeated.
	DefaultSessionTTL = 12 * time.Hour

	// loginWindow and loginBurst bound password guessing per client address.
	loginWindow = time.Minute
	loginBurst  = 10
)

// PasswordCheck verifies credentials from the login form. subject identifies
// the operator in the audit log, and must not be empty.
type PasswordCheck func(username string, password string) (subject string, ok bool)

// StaticPassword checks one username and password, compared in constant time.
// Anything with real users should implement PasswordCheck against its own
// store instead.
func StaticPassword(username string, password string) PasswordCheck {
	wantUser, wantPass := []byte(username), []byte(password)
	return func(gotUser string, gotPass string) (string, bool) {
		if username == "" || password == "" {
			return "", false // Empty credentials would admit everyone.
		}
		// Compare both halves always, so timing does not reveal which matched.
		userOK := subtle.ConstantTimeCompare([]byte(gotUser), wantUser) == 1
		passOK := subtle.ConstantTimeCompare([]byte(gotPass), wantPass) == 1
		if !userOK || !passOK {
			return "", false
		}
		return gotUser, true
	}
}

// WithLogin protects every view behind a login form, keeping the session in a
// signed cookie. secret signs those cookies... keep it out of source control,
// and rotating it logs everyone out.
//
// When controls are also enabled, a logged-in operator can use them: the
// session satisfies authorization, and a per-session CSRF token guards each
// form.
func WithLogin(check PasswordCheck, secret []byte, ttl time.Duration) Option {
	return func(h *Handler) {
		// Refuse loudly. Silently skipping the login here would serve the whole
		// dashboard anonymously to an operator who believes it is protected...
		// an unset CQ_DASH_SECRET is exactly how that happens.
		if check == nil {
			h.optErr = errors.New("web: WithLogin requires a PasswordCheck")
			return
		}
		if len(secret) == 0 {
			h.optErr = errors.New("web: WithLogin requires a non-empty signing secret")
			return
		}
		if ttl <= 0 {
			ttl = DefaultSessionTTL
		}
		h.login = check
		h.secret = secret
		h.sessionTTL = ttl
		h.revoked = newRevocations()
		h.attempts = newAttemptLimiter(loginBurst, loginWindow)
	}
}

// WithSecureCookies forces the Secure flag on or off, overriding detection.
//
// Detection cannot see through a TLS-terminating proxy: r.TLS is nil behind
// nginx or an ALB, which is the deployment the docs recommend, so the cookie
// would go out without Secure. Set this explicitly in production.
func WithSecureCookies(secure bool) Option {
	return func(h *Handler) {
		h.secureCookies = &secure
	}
}

// revocations tracks sessions signed out before their expiry.
//
// Signed cookies are stateless, so "sign out" would otherwise only delete the
// browser's copy while the value itself stayed valid for its full lifetime.
// Entries are bounded: nothing outlives the session TTL.
type revocations struct {
	mut  sync.Mutex
	dead map[string]time.Time // Session ID to its original expiry.
}

func newRevocations() *revocations {
	return &revocations{dead: make(map[string]time.Time)}
}

// revoke marks one session dead until its expiry passes.
func (r *revocations) revoke(id string, expiry time.Time) {
	r.mut.Lock()
	defer r.mut.Unlock()
	r.sweep()
	r.dead[id] = expiry
}

// isRevoked reports whether a session was signed out.
func (r *revocations) isRevoked(id string) bool {
	r.mut.Lock()
	defer r.mut.Unlock()
	expiry, found := r.dead[id]
	return found && time.Now().Before(expiry)
}

// sweep drops entries whose sessions expired anyway. Callers hold the lock.
func (r *revocations) sweep() {
	now := time.Now()
	for id, expiry := range r.dead {
		if now.After(expiry) {
			delete(r.dead, id)
		}
	}
}

// attemptLimiter caps failed logins per client address, so a single static
// password is not an unbounded guessing oracle.
type attemptLimiter struct {
	mut    sync.Mutex
	burst  int
	window time.Duration
	seen   map[string]*attemptWindow
}

type attemptWindow struct {
	count int
	until time.Time
}

func newAttemptLimiter(burst int, window time.Duration) *attemptLimiter {
	return &attemptLimiter{burst: burst, window: window, seen: make(map[string]*attemptWindow)}
}

// allow records an attempt and reports whether it may proceed.
func (a *attemptLimiter) allow(client string) bool {
	a.mut.Lock()
	defer a.mut.Unlock()

	now := time.Now()
	for key, win := range a.seen {
		if now.After(win.until) {
			delete(a.seen, key)
		}
	}

	win, found := a.seen[client]
	if !found || now.After(win.until) {
		a.seen[client] = &attemptWindow{count: 1, until: now.Add(a.window)}
		return true
	}
	win.count++
	return win.count <= a.burst
}

// client identifies the caller for rate limiting. It is the transport address,
// deliberately not a forwarded header, which a client can set freely.
func client(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// signSession returns a tamper-proof cookie value: subject, session ID and
// expiry, authenticated together.
func (h *Handler) signSession(subject string, id string, expiry time.Time) string {
	payload := base64.RawURLEncoding.EncodeToString([]byte(subject)) +
		"." + id + "." + strconv.FormatInt(expiry.Unix(), 10)
	return payload + "." + h.sign(payload)
}

// sign returns the hex HMAC of payload.
func (h *Handler) sign(payload string) string {
	mac := hmac.New(sha256.New, h.secret)
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// session is a verified, live login.
type session struct {
	Subject string
	ID      string
	Expiry  time.Time
}

// sessionFrom returns the live session carried by the request, if any.
//
// Every cookie of that name is examined: a shadowing cookie planted on a
// sibling domain must not be able to lock an operator out.
func (h *Handler) sessionFrom(r *http.Request) (session, bool) {
	for _, cookie := range r.CookiesNamed(sessionCookie) {
		if found, ok := h.verifySession(cookie.Value); ok {
			return found, true
		}
	}
	return session{}, false
}

// verifySession authenticates one cookie value.
func (h *Handler) verifySession(value string) (session, bool) {
	parts := strings.Split(value, ".")
	if len(parts) != 4 {
		return session{}, false
	}

	payload := parts[0] + "." + parts[1] + "." + parts[2]
	if subtle.ConstantTimeCompare([]byte(parts[3]), []byte(h.sign(payload))) != 1 {
		return session{}, false // Forged or tampered.
	}
	seconds, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return session{}, false
	}
	expiry := time.Unix(seconds, 0)
	if time.Now().After(expiry) {
		return session{}, false
	}
	if h.revoked != nil && h.revoked.isRevoked(parts[1]) {
		return session{}, false // Signed out before it expired.
	}
	subject, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || len(subject) == 0 {
		return session{}, false
	}
	return session{Subject: string(subject), ID: parts[1], Expiry: expiry}, true
}

// sessionSubject returns the operator carried by a valid, unexpired session.
func (h *Handler) sessionSubject(r *http.Request) (string, bool) {
	found, ok := h.sessionFrom(r)
	return found.Subject, ok
}

// secureCookie reports whether cookies should carry the Secure flag.
func (h *Handler) secureCookie(r *http.Request) bool {
	if h.secureCookies != nil {
		return *h.secureCookies
	}
	// Behind a terminating proxy r.TLS is nil, so trust the forwarded scheme
	// too. A client can only use it to ask for a stricter cookie.
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

// newSessionID returns a random identifier for one login.
func newSessionID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// loginData backs the login page.
type loginData struct {
	page
	Error string
	Next  string
	CSRF  string
}

// loginForm renders the login page, seeding the token that guards it.
func (h *Handler) loginForm(w http.ResponseWriter, r *http.Request) {
	token, err := newCSRFToken()
	if err != nil {
		http.Error(w, "could not start a login", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     loginCSRFCookie,
		Value:    token,
		Path:     h.cookiePath(),
		MaxAge:   int((10 * time.Minute).Seconds()),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   h.secureCookie(r),
	})
	h.renderLogin(w, r, http.StatusOK, "", r.URL.Query().Get("next"), token)
}

// renderLogin writes the login page at the given status.
func (h *Handler) renderLogin(w http.ResponseWriter, r *http.Request, status int, message string, next string, token string) {
	w.WriteHeader(status)
	h.render(w, "login.html", loginData{
		page:  page{Title: "Sign in", Nav: "login"},
		Error: message,
		Next:  next,
		CSRF:  token,
	})
}

// loginSubmit verifies credentials and starts a session.
func (h *Handler) loginSubmit(w http.ResponseWriter, r *http.Request) {
	// Read the body only: r.FormValue would also accept credentials from the
	// query string, where they would land in access logs and Referer headers.
	next := r.PostFormValue("next")

	// A login form posted from another origin could sign the victim into the
	// attacker's account, poisoning the audit trail. The token is issued with
	// the form and echoed back.
	cookie, err := r.Cookie(loginCSRFCookie)
	if err != nil || subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(r.PostFormValue("csrf"))) != 1 {
		h.auditLogin(r, "", false, "csrf")
		h.renderLogin(w, r, http.StatusForbidden,
			"That form expired. Please try again.", next, "")
		return
	}

	if !h.attempts.allow(client(r)) {
		h.auditLogin(r, r.PostFormValue("username"), false, "rate limited")
		h.renderLogin(w, r, http.StatusTooManyRequests,
			"Too many attempts. Wait a minute and try again.", next, "")
		return
	}

	subject, ok := h.login(r.PostFormValue("username"), r.PostFormValue("password"))
	if !ok || subject == "" {
		// An empty subject would mint a session with no operator, which renders
		// without a sign-out control and audits as though it were rejected.
		h.auditLogin(r, r.PostFormValue("username"), false, "")
		h.renderLogin(w, r, http.StatusUnauthorized,
			"Those credentials were not accepted.", next, "")
		return
	}

	id, err := newSessionID()
	if err != nil {
		http.Error(w, "could not start a session", http.StatusInternalServerError)
		return
	}
	expiry := time.Now().Add(h.sessionTTL)
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    h.signSession(subject, id, expiry),
		Path:     h.cookiePath(),
		Expires:  expiry,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   h.secureCookie(r),
	})
	h.clearCookie(w, r, loginCSRFCookie)
	h.audit(AuditEntry{Subject: subject, Action: "login", Allowed: true})

	http.Redirect(w, r, h.safeNext(next), http.StatusSeeOther)
}

// safeNext resolves a post-login destination inside this dashboard, refusing
// anything that could leave the origin.
//
// Prefix matching alone is not enough: with the handler mounted at the root a
// backslash reaches the browser as a slash, so "/\evil.example" becomes a
// protocol-relative URL.
func (h *Handler) safeNext(next string) string {
	home := h.prefix + "/"
	if next == "" || strings.ContainsAny(next, "\\") {
		return home
	}
	parsed, err := url.Parse(next)
	if err != nil || parsed.Scheme != "" || parsed.Host != "" || parsed.Opaque != "" {
		return home
	}
	clean := path.Clean(parsed.EscapedPath())
	if !strings.HasPrefix(clean, home) && clean != strings.TrimSuffix(home, "/") {
		return home
	}
	return clean
}

// auditLogin records one rejected or accepted login attempt.
//
// The username is attacker-supplied and unverified, but without it a rejected
// entry cannot be told apart from a typo or attributed to a target account.
func (h *Handler) auditLogin(r *http.Request, username string, allowed bool, reason string) {
	detail := username
	if reason != "" {
		detail += " (" + reason + ")"
	}
	h.audit(AuditEntry{
		Action:  "login",
		Queue:   client(r),
		Detail:  detail,
		Allowed: allowed,
	})
}

// logout ends the session, server side as well as in the browser.
func (h *Handler) logout(w http.ResponseWriter, r *http.Request) {
	if !h.checkCSRF(r, Identity{Via: ViaSession}) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	// Deleting the browser's copy is not enough: the signed value would stay
	// valid until it expired, so anyone holding a copy stays signed in.
	if found, ok := h.sessionFrom(r); ok {
		h.revoked.revoke(found.ID, found.Expiry)
		h.audit(AuditEntry{Subject: found.Subject, Via: ViaSession,
			Action: "logout", Allowed: true})
	}
	h.clearCookie(w, r, sessionCookie)
	http.Redirect(w, r, h.prefix+"/login", http.StatusSeeOther)
}

// clearCookie expires one cookie in the browser.
func (h *Handler) clearCookie(w http.ResponseWriter, r *http.Request, name string) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    "",
		Path:     h.cookiePath(),
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   h.secureCookie(r),
	})
}

// cookiePath scopes the session to this dashboard's mount point.
func (h *Handler) cookiePath() string {
	if h.prefix == "" {
		return "/"
	}
	return h.prefix + "/"
}

// gate refuses requests without an identity, in the terms the caller can act
// on: a redirect to the form for a browser, 401 for JSON and for deployments
// with no form to offer.
func (h *Handler) gate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, h.prefix)
		if path == "/login" || path == "/logout" {
			next.ServeHTTP(w, r)
			return
		}
		if _, ok := h.identify(r); ok {
			next.ServeHTTP(w, r)
			return
		}
		// A client asking for JSON gets JSON. Redirecting it to the form would
		// answer 200 with an HTML page, which naive clients read as success.
		if wantsJSON(r) {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if h.login == nil {
			// RequireSignIn without a login form: there is nowhere to send them.
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		// Escape the destination: a raw path with a query would break the link.
		target := url.Values{"next": {r.URL.EscapedPath()}}
		http.Redirect(w, r, h.prefix+"/login?"+target.Encode(), http.StatusSeeOther)
	})
}
