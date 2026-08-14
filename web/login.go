package web

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// A login form with a signed cookie, rather than HTTP basic auth.
//
// Basic auth relies on the browser's native dialog, which cannot be styled,
// offers no way to log out, and simply does not appear in embedded webviews.
// A form works everywhere and can say what went wrong.

// sessionCookie is where the signed session lives.
const sessionCookie = "cq_dashboard_session"

// DefaultSessionTTL is how long a login lasts before it must be repeated.
const DefaultSessionTTL = 12 * time.Hour

// PasswordCheck verifies credentials from the login form. subject identifies
// the operator in the audit log.
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
// session satisfies authorization, and the CSRF token still guards each form.
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
	}
}

// signSession returns a tamper-proof cookie value for subject.
func (h *Handler) signSession(subject string, expiry time.Time) string {
	payload := base64.RawURLEncoding.EncodeToString([]byte(subject)) +
		"." + strconv.FormatInt(expiry.Unix(), 10)
	return payload + "." + h.sign(payload)
}

// sign returns the hex HMAC of payload.
func (h *Handler) sign(payload string) string {
	mac := hmac.New(sha256.New, h.secret)
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// sessionSubject returns the operator carried by a valid, unexpired session.
func (h *Handler) sessionSubject(r *http.Request) (string, bool) {
	cookie, err := r.Cookie(sessionCookie)
	if err != nil {
		return "", false
	}
	parts := strings.Split(cookie.Value, ".")
	if len(parts) != 3 {
		return "", false
	}

	payload := parts[0] + "." + parts[1]
	if subtle.ConstantTimeCompare([]byte(parts[2]), []byte(h.sign(payload))) != 1 {
		return "", false // Forged or tampered.
	}
	expiry, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || time.Now().After(time.Unix(expiry, 0)) {
		return "", false
	}
	subject, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", false
	}
	return string(subject), true
}

// loginData backs the login page.
type loginData struct {
	page
	Error string
	Next  string
}

// loginForm renders the login page.
func (h *Handler) loginForm(w http.ResponseWriter, r *http.Request) {
	h.render(w, "login.html", loginData{
		page: page{Title: "Sign in", Nav: "login"},
		Next: r.URL.Query().Get("next"),
	})
}

// loginSubmit verifies credentials and starts a session.
func (h *Handler) loginSubmit(w http.ResponseWriter, r *http.Request) {
	subject, ok := h.login(r.FormValue("username"), r.FormValue("password"))
	if !ok {
		h.audit(AuditEntry{Action: "login", Allowed: false})
		w.WriteHeader(http.StatusUnauthorized)
		h.render(w, "login.html", loginData{
			page:  page{Title: "Sign in", Nav: "login"},
			Error: "Those credentials were not accepted.",
			Next:  r.FormValue("next"),
		})
		return
	}

	expiry := time.Now().Add(h.sessionTTL)
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    h.signSession(subject, expiry),
		Path:     h.cookiePath(),
		Expires:  expiry,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r.TLS != nil,
	})
	h.audit(AuditEntry{Subject: subject, Action: "login", Allowed: true})

	// Only ever redirect within this dashboard, never to a supplied host.
	target := h.prefix + "/"
	if next := r.FormValue("next"); strings.HasPrefix(next, h.prefix+"/") && !strings.HasPrefix(next, "//") {
		target = next
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

// logout clears the session. It checks CSRF like any other state change, so a
// third-party page cannot sign an operator out.
func (h *Handler) logout(w http.ResponseWriter, r *http.Request) {
	if !h.checkCSRF(r) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    "",
		Path:     h.cookiePath(),
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, h.prefix+"/login", http.StatusSeeOther)
}

// cookiePath scopes the session to this dashboard's mount point.
func (h *Handler) cookiePath() string {
	if h.prefix == "" {
		return "/"
	}
	return h.prefix + "/"
}

// gate redirects unauthenticated requests to the login form.
func (h *Handler) gate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, h.prefix)
		if path == "/login" || path == "/logout" {
			next.ServeHTTP(w, r)
			return
		}
		if _, ok := h.sessionSubject(r); ok {
			next.ServeHTTP(w, r)
			return
		}
		// An API client with its own Authorizer still gets through.
		if h.auth != nil {
			if _, ok := h.auth(r); ok {
				next.ServeHTTP(w, r)
				return
			}
		}
		// Escape the destination: a raw path with a query would break the link.
		next := url.Values{"next": {r.URL.EscapedPath()}}
		http.Redirect(w, r, h.prefix+"/login?"+next.Encode(), http.StatusSeeOther)
	})
}
