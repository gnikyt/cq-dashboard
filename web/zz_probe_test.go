package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gnikyt/cq-dashboard/sink"
	"github.com/gnikyt/cq-dashboard/store/sqlite"
	cq "github.com/gnikyt/cq/v2"
)

// rootLoginHarness mounts the dashboard at the root, which is the default
// deployment when the dashboard owns its own port.
func rootLoginHarness(t *testing.T, prefix string) *Handler {
	t.Helper()
	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	sk := sink.New(st, sink.WithFlushTick(10*time.Millisecond))
	if _, err := sk.Start(context.Background()); err != nil {
		t.Fatalf("Start(): %v", err)
	}
	queue := cq.NewQueue(1, 2, 8, cq.WithQueueName("demo"), cq.WithHooks(sk.Hooks()))
	queue.Start()
	h, err := New(prefix, st, sk,
		WithQueues(queue),
		WithLogin(StaticPassword("ops", "hunter2"), []byte("test-secret"), time.Hour),
		WithControls(nil),
	)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(func() { queue.Stop(false); sk.Close(); st.Close() })
	return h
}

func postForm(h *Handler, path string, form url.Values, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for _, c := range cookies {
		req.AddCookie(c)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// PROBE 1: open redirect via backslash, when mounted at the root.
func TestProbeBackslashRedirect(t *testing.T) {
	h := rootLoginHarness(t, "")
	for _, next := range []string{
		`/\evil.example/steal`,
		`/\/evil.example/steal`,
		"/\t//evil.example",
		`/%5Cevil.example`,
	} {
		form := url.Values{"username": {"ops"}, "password": {"hunter2"}, "next": {next}}
		rec := postForm(h, "/login", form)
		t.Logf("next=%q -> %d Location=%q", next, rec.Code, rec.Header().Get("Location"))
	}
}

// PROBE 1b: same, with a /cq prefix.
func TestProbeBackslashRedirectPrefixed(t *testing.T) {
	h := rootLoginHarness(t, "/cq")
	for _, next := range []string{
		`/cq/\/evil.example/steal`,
		`/cq/..//evil.example`,
		`/cq/`,
	} {
		form := url.Values{"username": {"ops"}, "password": {"hunter2"}, "next": {next}}
		rec := postForm(h, "/cq/login", form)
		t.Logf("next=%q -> %d Location=%q", next, rec.Code, rec.Header().Get("Location"))
	}
}

// PROBE 2: does a session cookie still work after sign-out?
func TestProbeReplayAfterLogout(t *testing.T) {
	h := rootLoginHarness(t, "/cq")
	cookie := signIn(t, h, "ops", "hunter2")

	rec := postForm(h, "/cq/logout", url.Values{"csrf": {h.csrf}}, cookie)
	t.Logf("logout -> %d", rec.Code)

	req := httptest.NewRequest(http.MethodGet, "/cq/jobs", nil)
	req.AddCookie(cookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	t.Logf("replay old cookie after logout -> %d (200 means the session is still live)", rec.Code)
	if rec.Code == http.StatusOK {
		t.Log("PROVED: signed-out session cookie is still accepted")
	}
}

// PROBE 3: path shapes that might slip past the gate.
func TestProbeGateBypass(t *testing.T) {
	h := rootLoginHarness(t, "/cq")
	for _, path := range []string{
		"/cq/jobs",
		"/cq",
		"/cq/",
		"//cq/jobs",
		"/cq/login/../jobs",
		"/cq//jobs",
		"/cq/./jobs",
		"/cq/JOBS",
		"/cq/partials/live",
		"/cq/jobs/",
		"/cq/%2e%2e/cq/jobs",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		t.Logf("GET %-24q -> %d  Location=%q  bodylen=%d", path, rec.Code, rec.Header().Get("Location"), rec.Body.Len())
	}
}

// PROBE 4: is the CSRF token handed to anonymous callers when controls are on
// but no login is configured?
func TestProbeAnonymousCSRFDisclosure(t *testing.T) {
	h, _, _ := newControlHarness(t)
	req := httptest.NewRequest(http.MethodGet, "/cq/", nil) // no Authorization header
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if strings.Contains(rec.Body.String(), h.csrf) {
		t.Logf("PROVED: anonymous GET /cq/ (%d) leaks the process-wide CSRF token", rec.Code)
	} else {
		t.Logf("no leak; status %d", rec.Code)
	}
	// And the polled partial too.
	req = httptest.NewRequest(http.MethodGet, "/cq/partials/live", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if strings.Contains(rec.Body.String(), h.csrf) {
		t.Log("PROVED: anonymous GET /cq/partials/live leaks it too")
	}
}

// PROBE 5: cookie shadowing -- two cookies of the same name.
func TestProbeDuplicateCookie(t *testing.T) {
	h := rootLoginHarness(t, "/cq")
	good := signIn(t, h, "ops", "hunter2")

	req := httptest.NewRequest(http.MethodGet, "/cq/jobs", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: "garbage.1.2"})
	req.AddCookie(good)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	t.Logf("garbage-first, valid-second -> %d (303 means only the first cookie is examined)", rec.Code)
}

// PROBE 6: credentials accepted from the query string.
func TestProbeCredentialsInQueryString(t *testing.T) {
	h := rootLoginHarness(t, "/cq")
	req := httptest.NewRequest(http.MethodPost, "/cq/login?username=ops&password=hunter2", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	t.Logf("POST /cq/login?username=..&password=.. -> %d cookies=%d", rec.Code, len(rec.Result().Cookies()))
	// And GET with a session-granting query? The route is POST-only.
	req = httptest.NewRequest(http.MethodGet, "/cq/login?username=ops&password=hunter2", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	t.Logf("GET  /cq/login?username=..&password=.. -> %d cookies=%d", rec.Code, len(rec.Result().Cookies()))
}

// PROBE 7: an empty subject from a custom PasswordCheck.
func TestProbeEmptySubject(t *testing.T) {
	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	sk := sink.New(st, sink.WithFlushTick(10*time.Millisecond))
	if _, err := sk.Start(context.Background()); err != nil {
		t.Fatalf("Start(): %v", err)
	}
	defer func() { sk.Close(); st.Close() }()

	check := func(u, p string) (string, bool) { return "", u == "ops" } // subject omitted
	h, err := New("/cq", st, sk, WithLogin(check, []byte("s"), time.Hour), WithControls(nil))
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	rec := postForm(h, "/cq/login", url.Values{"username": {"ops"}, "password": {"x"}})
	t.Logf("login with empty subject -> %d", rec.Code)
	var cookie *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookie {
			cookie = c
		}
	}
	if cookie == nil {
		t.Fatal("no cookie")
	}
	t.Logf("cookie value = %q", cookie.Value)
	req := httptest.NewRequest(http.MethodGet, "/cq/jobs", nil)
	req.AddCookie(cookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	t.Logf("GET /cq/jobs -> %d, sign-out rendered=%v", rec.Code, strings.Contains(rec.Body.String(), "sign out"))
}

// PROBE 8: cookie flags and response headers on the login response.
func TestProbeCookieFlagsAndHeaders(t *testing.T) {
	h := rootLoginHarness(t, "/cq")
	form := url.Values{"username": {"ops"}, "password": {"hunter2"}}
	req := httptest.NewRequest(http.MethodPost, "/cq/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Forwarded-Proto", "https") // TLS terminated upstream
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	for _, c := range rec.Result().Cookies() {
		t.Logf("Set-Cookie: name=%s secure=%v httponly=%v samesite=%v path=%q expires=%v",
			c.Name, c.Secure, c.HttpOnly, c.SameSite, c.Path, c.Expires)
	}
	t.Logf("headers on login 303: %v", rec.Header())

	// Headers on an authenticated page.
	cookie := signIn(t, h, "ops", "hunter2")
	req = httptest.NewRequest(http.MethodGet, "/cq/jobs", nil)
	req.AddCookie(cookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	t.Logf("headers on an authenticated page: %v", rec.Header())

	// Headers on the 401 login-failure page.
	rec = postForm(h, "/cq/login", url.Values{"username": {"ops"}, "password": {"no"}})
	t.Logf("failed-login response %d headers: %v", rec.Code, rec.Header())
}

// PROBE 9: logout clear-cookie attributes vs the set attributes.
func TestProbeLogoutCookieAttrs(t *testing.T) {
	h := rootLoginHarness(t, "/cq")
	cookie := signIn(t, h, "ops", "hunter2")
	rec := postForm(h, "/cq/logout", url.Values{"csrf": {h.csrf}}, cookie)
	for _, c := range rec.Result().Cookies() {
		t.Logf("logout Set-Cookie: name=%s value=%q maxage=%d secure=%v path=%q", c.Name, c.Value, c.MaxAge, c.Secure, c.Path)
	}
}

// PROBE 10: can an anonymous visitor log a signed-in operator out, or force a
// login (no CSRF on the login form itself)?
func TestProbeLoginCSRF(t *testing.T) {
	h := rootLoginHarness(t, "/cq")
	// No CSRF field on the login form at all:
	req := httptest.NewRequest(http.MethodGet, "/cq/login", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	t.Logf("login form carries a csrf field: %v", strings.Contains(rec.Body.String(), `name="csrf"`))
	// And a cross-site POST with no token succeeds:
	rec = postForm(h, "/cq/login", url.Values{"username": {"ops"}, "password": {"hunter2"}})
	t.Logf("tokenless login POST -> %d, cookies=%d", rec.Code, len(rec.Result().Cookies()))
}
