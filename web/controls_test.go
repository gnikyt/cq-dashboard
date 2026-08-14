package web

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gnikyt/cq-dashboard/sink"
	"github.com/gnikyt/cq-dashboard/store"
	"github.com/gnikyt/cq-dashboard/store/sqlite"
	cq "github.com/gnikyt/cq/v2"
)

const testToken = "s3cret-token"

// auditLog collects control attempts for assertions.
type auditLog struct {
	mut     sync.Mutex
	entries []AuditEntry
}

func (a *auditLog) record(entry AuditEntry) {
	a.mut.Lock()
	defer a.mut.Unlock()
	a.entries = append(a.entries, entry)
}

func (a *auditLog) all() []AuditEntry {
	a.mut.Lock()
	defer a.mut.Unlock()
	return append([]AuditEntry(nil), a.entries...)
}

// newControlHarness wires a handler with controls enabled behind a bearer token.
func newControlHarness(t *testing.T, opts ...Option) (*Handler, *cq.Queue, *auditLog) {
	t.Helper()

	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	sk := sink.New(st, sink.WithFlushTick(10*time.Millisecond))
	if _, err := sk.Start(context.Background()); err != nil {
		t.Fatalf("Start(): %v", err)
	}

	queue := cq.NewQueue(1, 4, 8, cq.WithQueueName("demo"), cq.WithHooks(sk.Hooks()))
	queue.Start()

	log := &auditLog{}
	opts = append([]Option{
		WithQueues(queue),
		WithControls(BearerToken("ops@example.com", testToken)),
		WithAudit(log.record),
	}, opts...)

	handler, err := New("/cq", st, sk, opts...)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(func() {
		queue.Stop(false)
		sk.Close()
		st.Close()
	})
	return handler, queue, log
}

// csrfToken scrapes the token the handler rendered into its control forms.
func csrfToken(t *testing.T, handler *Handler) string {
	t.Helper()
	body := get(t, handler, "/cq/")
	match := regexp.MustCompile(`name="csrf" value="([a-f0-9]+)"`).FindStringSubmatch(body)
	if match == nil {
		t.Fatal("GET /cq/: no csrf token in the control forms")
	}
	return match[1]
}

// post sends a control request, optionally authorized.
func post(handler *Handler, path string, form url.Values, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestControlsRequireAuthorizer(t *testing.T) {
	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	defer st.Close()
	sk := sink.New(st)

	if _, err := New("/cq", st, sk, WithControls(nil)); err == nil {
		t.Error("New() with a nil Authorizer: got nil error, want a refusal")
	}
}

func TestControlRoutesAbsentWhenDisabled(t *testing.T) {
	handler, queue, _, _ := newHarness(t)

	rec := post(handler, "/cq/control/pause", url.Values{"queue": {"demo"}}, testToken)
	if rec.Code != http.StatusNotFound {
		t.Errorf("POST pause with controls disabled: got %d, want 404", rec.Code)
	}
	if queue.IsPaused() {
		t.Error("queue was paused despite controls being disabled")
	}
}

func TestControlRejectsMissingToken(t *testing.T) {
	handler, queue, log := newControlHarness(t)
	csrf := csrfToken(t, handler)

	rec := post(handler, "/cq/control/pause", url.Values{
		"queue": {"demo"}, "csrf": {csrf},
	}, "")
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("POST pause unauthenticated: got %d, want 401", rec.Code)
	}
	if queue.IsPaused() {
		t.Error("queue was paused by an unauthenticated request")
	}

	entries := log.all()
	if len(entries) != 1 {
		t.Fatalf("audit entries: got %d, want 1", len(entries))
	}
	if entries[0].Allowed {
		t.Error("audit entry: got Allowed=true for a rejected request")
	}
}

func TestControlRejectsWrongToken(t *testing.T) {
	handler, queue, _ := newControlHarness(t)
	csrf := csrfToken(t, handler)

	rec := post(handler, "/cq/control/pause", url.Values{
		"queue": {"demo"}, "csrf": {csrf},
	}, "wrong-token")
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("POST pause with a bad token: got %d, want 401", rec.Code)
	}
	if queue.IsPaused() {
		t.Error("queue was paused by a request with the wrong token")
	}
}

// An authorized request without the token cannot be a cross-site form post.
func TestControlRejectsMissingCSRF(t *testing.T) {
	handler, queue, log := newControlHarness(t)

	rec := post(handler, "/cq/control/pause", url.Values{"queue": {"demo"}}, testToken)
	if rec.Code != http.StatusForbidden {
		t.Errorf("POST pause without csrf: got %d, want 403", rec.Code)
	}
	if queue.IsPaused() {
		t.Error("queue was paused without a csrf token")
	}

	entries := log.all()
	if len(entries) != 1 || entries[0].Allowed {
		t.Fatalf("audit entries: got %+v, want one rejected entry", entries)
	}
	if entries[0].Subject != "ops@example.com" {
		t.Errorf("audit subject: got %q, want the authorized subject", entries[0].Subject)
	}
}

func TestPauseAndResume(t *testing.T) {
	handler, queue, log := newControlHarness(t)
	csrf := csrfToken(t, handler)

	rec := post(handler, "/cq/control/pause", url.Values{
		"queue": {"demo"}, "csrf": {csrf},
	}, testToken)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST pause: got %d, want 303", rec.Code)
	}
	if !queue.IsPaused() {
		t.Fatal("IsPaused(): got false, want true after pause")
	}

	rec = post(handler, "/cq/control/resume", url.Values{
		"queue": {"demo"}, "csrf": {csrf},
	}, testToken)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST resume: got %d, want 303", rec.Code)
	}
	if queue.IsPaused() {
		t.Error("IsPaused(): got true, want false after resume")
	}

	entries := log.all()
	if len(entries) != 2 {
		t.Fatalf("audit entries: got %d, want 2", len(entries))
	}
	for _, entry := range entries {
		if !entry.Allowed || entry.Err != nil {
			t.Errorf("audit entry %+v: want allowed with no error", entry)
		}
		if entry.Subject != "ops@example.com" {
			t.Errorf("audit subject: got %q, want ops@example.com", entry.Subject)
		}
	}
}

func TestSetWorkerRange(t *testing.T) {
	handler, queue, log := newControlHarness(t)
	csrf := csrfToken(t, handler)

	rec := post(handler, "/cq/control/workers", url.Values{
		"queue": {"demo"}, "csrf": {csrf}, "min": {"2"}, "max": {"9"},
	}, testToken)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST workers: got %d, want 303", rec.Code)
	}

	min, max := queue.WorkerRange()
	if min != 2 || max != 9 {
		t.Errorf("WorkerRange(): got (%d, %d), want (2, 9)", min, max)
	}
	if entries := log.all(); len(entries) != 1 || entries[0].Detail != "2-9" {
		t.Errorf("audit detail: got %+v, want detail 2-9", entries)
	}
}

// A rejected action must surface as an error, not a silent success.
func TestInvalidWorkerRangeIsReported(t *testing.T) {
	handler, queue, log := newControlHarness(t)
	csrf := csrfToken(t, handler)

	rec := post(handler, "/cq/control/workers", url.Values{
		"queue": {"demo"}, "csrf": {csrf}, "min": {"9"}, "max": {"2"},
	}, testToken)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST workers: got %d, want 303", rec.Code)
	}
	if location := rec.Header().Get("Location"); !strings.Contains(location, "error=") {
		t.Errorf("redirect: got %q, want an error message", location)
	}

	min, max := queue.WorkerRange()
	if min == 9 && max == 2 {
		t.Error("WorkerRange(): an inverted range was applied")
	}

	entries := log.all()
	if len(entries) != 1 || entries[0].Err == nil {
		t.Errorf("audit entries: got %+v, want one entry carrying the error", entries)
	}
}

func TestUnknownQueueIs404(t *testing.T) {
	handler, _, _ := newControlHarness(t)
	csrf := csrfToken(t, handler)

	rec := post(handler, "/cq/control/pause", url.Values{
		"queue": {"nope"}, "csrf": {csrf},
	}, testToken)
	if rec.Code != http.StatusNotFound {
		t.Errorf("POST pause for an unknown queue: got %d, want 404", rec.Code)
	}
}

func TestControlPanelHiddenWhenDisabled(t *testing.T) {
	handler, _, _, _ := newHarness(t)

	body := get(t, handler, "/cq/")
	if strings.Contains(body, "/control/pause") {
		t.Error("GET /cq/: control forms rendered while controls are disabled")
	}
}

func TestBearerTokenRejectsEmptyConfiguredToken(t *testing.T) {
	auth := BearerToken("anyone", "")
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	if _, ok := auth(req); ok {
		t.Error("BearerToken(\"\"): authorized a request, want refusal")
	}
	req.Header.Set("Authorization", "Bearer ")
	if _, ok := auth(req); ok {
		t.Error("BearerToken(\"\"): authorized an empty bearer, want refusal")
	}
}

func TestBasicAuthAcceptsAndRejects(t *testing.T) {
	auth := BasicAuth("ops", "hunter2")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if _, ok := auth(req); ok {
		t.Error("BasicAuth: authorized a request with no credentials")
	}

	req.SetBasicAuth("ops", "wrong")
	if _, ok := auth(req); ok {
		t.Error("BasicAuth: authorized a wrong password")
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("nope", "hunter2")
	if _, ok := auth(req); ok {
		t.Error("BasicAuth: authorized a wrong username")
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("ops", "hunter2")
	subject, ok := auth(req)
	if !ok {
		t.Fatal("BasicAuth: rejected correct credentials")
	}
	if subject != "ops" {
		t.Errorf("BasicAuth subject: got %q, want %q", subject, "ops")
	}
}

func TestBasicAuthRefusesEmptyConfiguration(t *testing.T) {
	for _, tc := range []struct{ user, pass string }{{"", "x"}, {"x", ""}, {"", ""}} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.SetBasicAuth(tc.user, tc.pass)
		if _, ok := BasicAuth(tc.user, tc.pass)(req); ok {
			t.Errorf("BasicAuth(%q, %q): authorized, want refusal", tc.user, tc.pass)
		}
	}
}

// The read-only views leak job names, attributes and errors, so the gate must
// cover them, not only the control endpoints.
func TestRequireAuthGatesTheViews(t *testing.T) {
	handler, _, _, _ := newHarness(t)
	guarded := RequireAuth(handler, BasicAuth("ops", "hunter2"))

	rec := httptest.NewRecorder()
	guarded.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cq/jobs", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("GET /cq/jobs unauthenticated: got %d, want 401", rec.Code)
	}
	if challenge := rec.Header().Get("WWW-Authenticate"); !strings.Contains(challenge, "Basic") {
		t.Errorf("challenge header: got %q, want a Basic challenge so browsers prompt", challenge)
	}

	req := httptest.NewRequest(http.MethodGet, "/cq/jobs", nil)
	req.SetBasicAuth("ops", "hunter2")
	rec = httptest.NewRecorder()
	guarded.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET /cq/jobs authenticated: got %d, want 200", rec.Code)
	}
}

func TestRequireAuthWithNilAuthorizerDenies(t *testing.T) {
	handler, _, _, _ := newHarness(t)
	guarded := RequireAuth(handler, nil)

	rec := httptest.NewRecorder()
	guarded.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cq/", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("nil Authorizer: got %d, want 401 (deny by default)", rec.Code)
	}
}

// One Authorizer can cover both the views and the controls.
func TestSharedAuthorizerCoversViewsAndControls(t *testing.T) {
	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	defer st.Close()
	sk := sink.New(st, sink.WithFlushTick(10*time.Millisecond))
	if _, err := sk.Start(context.Background()); err != nil {
		t.Fatalf("Start(): %v", err)
	}
	defer sk.Close()

	queue := cq.NewQueue(1, 1, 8, cq.WithQueueName("demo"), cq.WithHooks(sk.Hooks()))
	queue.Start()
	defer queue.Stop(false)

	auth := BasicAuth("ops", "hunter2")
	handler, err := New("/cq", st, sk, WithQueues(queue), WithControls(auth))
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	guarded := RequireAuth(handler, auth)

	// The page renders, and carries a CSRF token for the controls.
	req := httptest.NewRequest(http.MethodGet, "/cq/", nil)
	req.SetBasicAuth("ops", "hunter2")
	rec := httptest.NewRecorder()
	guarded.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /cq/ with credentials: got %d, want 200", rec.Code)
	}
	match := regexp.MustCompile(`name="csrf" value="([a-f0-9]+)"`).FindStringSubmatch(rec.Body.String())
	if match == nil {
		t.Fatal("GET /cq/: no csrf token rendered for an authorized viewer")
	}

	// The same credentials drive a control action.
	form := url.Values{"queue": {"demo"}, "csrf": {match[1]}}
	post := httptest.NewRequest(http.MethodPost, "/cq/control/pause", strings.NewReader(form.Encode()))
	post.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	post.SetBasicAuth("ops", "hunter2")
	rec = httptest.NewRecorder()
	guarded.ServeHTTP(rec, post)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST pause with credentials: got %d, want 303", rec.Code)
	}
	if !queue.IsPaused() {
		t.Error("IsPaused(): got false, want true")
	}
}

// newLoginHarness wires a handler behind the login form.
func newLoginHarness(t *testing.T) (*Handler, *cq.Queue) {
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

	handler, err := New("/cq", st, sk,
		WithQueues(queue),
		WithLogin(StaticPassword("ops", "hunter2"), []byte("test-secret"), time.Hour),
		WithControls(nil), // A session is the authorization.
	)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(func() {
		queue.Stop(false)
		sk.Close()
		st.Close()
	})
	return handler, queue
}

// signIn posts the login form and returns the session cookie.
func signIn(t *testing.T, handler *Handler, user, pass string) *http.Cookie {
	t.Helper()
	form := url.Values{"username": {user}, "password": {pass}}
	req := httptest.NewRequest(http.MethodPost, "/cq/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST /cq/login: got %d, want 303 (body: %s)", rec.Code, rec.Body.String())
	}
	for _, cookie := range rec.Result().Cookies() {
		if cookie.Name == sessionCookie {
			return cookie
		}
	}
	t.Fatal("POST /cq/login: no session cookie set")
	return nil
}

func TestLoginRedirectsAnonymousVisitors(t *testing.T) {
	handler, _ := newLoginHarness(t)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cq/jobs", nil))
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("GET /cq/jobs anonymous: got %d, want 303 to the login form", rec.Code)
	}
	if loc := rec.Header().Get("Location"); !strings.Contains(loc, "/cq/login?next=%2Fcq%2Fjobs") {
		t.Errorf("redirect: got %q, want the login form carrying the destination", loc)
	}

	// The form itself must be reachable, or nobody can ever sign in.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cq/login", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("GET /cq/login: got %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `name="password"`) {
		t.Error("GET /cq/login: no password field rendered")
	}
}

func TestLoginRejectsBadCredentials(t *testing.T) {
	handler, _ := newLoginHarness(t)

	form := url.Values{"username": {"ops"}, "password": {"wrong"}}
	req := httptest.NewRequest(http.MethodPost, "/cq/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("POST /cq/login wrong password: got %d, want 401", rec.Code)
	}
	if len(rec.Result().Cookies()) != 0 {
		t.Error("POST /cq/login wrong password: a cookie was set")
	}
	if !strings.Contains(rec.Body.String(), "not accepted") {
		t.Error("POST /cq/login wrong password: no error shown to the operator")
	}
}

func TestSessionGrantsAccessAndSignOut(t *testing.T) {
	handler, _ := newLoginHarness(t)
	cookie := signIn(t, handler, "ops", "hunter2")

	if !cookie.HttpOnly {
		t.Error("session cookie: not HttpOnly")
	}
	if cookie.SameSite != http.SameSiteLaxMode {
		t.Error("session cookie: SameSite is not Lax")
	}

	req := httptest.NewRequest(http.MethodGet, "/cq/jobs", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /cq/jobs with session: got %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "(sign out)") {
		t.Error("GET /cq/jobs with session: no sign-out offered")
	}

	// Signing out clears the cookie. It is a state change, so it needs CSRF.
	form := url.Values{"csrf": {handler.csrf}}
	out := httptest.NewRequest(http.MethodPost, "/cq/logout", strings.NewReader(form.Encode()))
	out.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	out.AddCookie(cookie)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, out)
	cleared := rec.Result().Cookies()
	if len(cleared) == 0 || cleared[0].MaxAge >= 0 {
		t.Error("POST /cq/logout: session cookie was not cleared")
	}
}

// A cookie nobody signed must not be accepted.
func TestForgedSessionIsRejected(t *testing.T) {
	handler, _ := newLoginHarness(t)
	cookie := signIn(t, handler, "ops", "hunter2")

	for _, forged := range []string{
		"b3Bz.9999999999.deadbeef",                      // Wrong signature.
		cookie.Value[:len(cookie.Value)-2],              // Truncated signature.
		"YWRtaW4.9999999999." + strings.Repeat("a", 64), // Different subject.
	} {
		req := httptest.NewRequest(http.MethodGet, "/cq/jobs", nil)
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: forged})
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusSeeOther {
			t.Errorf("forged cookie %q: got %d, want a redirect to login", forged, rec.Code)
		}
	}
}

func TestExpiredSessionIsRejected(t *testing.T) {
	handler, _ := newLoginHarness(t)

	// Correctly signed, but already expired.
	expired := handler.signSession("ops", time.Now().Add(-time.Minute))
	req := httptest.NewRequest(http.MethodGet, "/cq/jobs", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: expired})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Errorf("expired session: got %d, want a redirect to login", rec.Code)
	}
}

// The login must not become an open redirect.
func TestLoginOnlyRedirectsWithinTheDashboard(t *testing.T) {
	handler, _ := newLoginHarness(t)

	for _, next := range []string{"https://evil.example/steal", "//evil.example/steal", "/elsewhere"} {
		form := url.Values{"username": {"ops"}, "password": {"hunter2"}, "next": {next}}
		req := httptest.NewRequest(http.MethodPost, "/cq/login", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if loc := rec.Header().Get("Location"); loc != "/cq/" {
			t.Errorf("next=%q: redirected to %q, want /cq/", next, loc)
		}
	}
}

// A signed-in operator can use the controls without a second credential.
func TestSessionAuthorizesControls(t *testing.T) {
	handler, queue := newLoginHarness(t)
	cookie := signIn(t, handler, "ops", "hunter2")

	req := httptest.NewRequest(http.MethodGet, "/cq/", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	match := regexp.MustCompile(`name="csrf" value="([a-f0-9]+)"`).FindStringSubmatch(rec.Body.String())
	if match == nil {
		t.Fatal("GET /cq/: no csrf token for a signed-in operator")
	}

	form := url.Values{"queue": {"demo"}, "csrf": {match[1]}}
	post := httptest.NewRequest(http.MethodPost, "/cq/control/pause", strings.NewReader(form.Encode()))
	post.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	post.AddCookie(cookie)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, post)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST pause with session: got %d, want 303", rec.Code)
	}
	if !queue.IsPaused() {
		t.Error("IsPaused(): got false, want true")
	}
}

// Regressions for the review findings.

// A misconfigured login must fail loudly, not serve the dashboard publicly.
func TestIncompleteLoginIsRefused(t *testing.T) {
	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	defer st.Close()
	sk := sink.New(st)

	for name, opt := range map[string]Option{
		"empty secret": WithLogin(StaticPassword("ops", "pw"), []byte(""), time.Hour),
		"nil secret":   WithLogin(StaticPassword("ops", "pw"), nil, time.Hour),
		"nil check":    WithLogin(nil, []byte("secret"), time.Hour),
	} {
		if _, err := New("/cq", st, sk, opt); err == nil {
			t.Errorf("New() with %s: got nil error, want a refusal", name)
		}
	}
}

// Logout is a state change and must carry the token, like every other one.
func TestLogoutRequiresCSRF(t *testing.T) {
	handler, _ := newLoginHarness(t)
	cookie := signIn(t, handler, "ops", "hunter2")

	req := httptest.NewRequest(http.MethodPost, "/cq/logout", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("POST /cq/logout without csrf: got %d, want 403", rec.Code)
	}
	// The session must survive, or the attack succeeded anyway.
	probe := httptest.NewRequest(http.MethodGet, "/cq/jobs", nil)
	probe.AddCookie(cookie)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, probe)
	if rec.Code != http.StatusOK {
		t.Errorf("session after a rejected logout: got %d, want it still valid", rec.Code)
	}
}

// Deep pages read a frozen window, so inserts cannot shift rows underneath.
func TestPagerFreezesTheWindow(t *testing.T) {
	handler, _, _, sk := newHarness(t)

	jobs := make([]store.Job, 0, 120)
	base := time.Now().Add(-time.Hour)
	for i := range 120 {
		jobs = append(jobs, store.Job{
			ID: fmt.Sprintf("p%03d", i), Epoch: sk.Epoch(), Name: "bulk",
			State: store.StateCompleted, EnqueuedAt: base.Add(time.Duration(i) * time.Second),
		})
	}
	if err := handler.store.UpsertJobs(context.Background(), jobs); err != nil {
		t.Fatalf("UpsertJobs(): %v", err)
	}

	body := get(t, handler, "/cq/jobs")
	if !strings.Contains(body, "before=") {
		t.Fatal("GET /cq/jobs: pager link does not freeze the window")
	}
}
