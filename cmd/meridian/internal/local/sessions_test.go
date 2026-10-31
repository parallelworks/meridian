package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/internal/sessionproxy"
	"github.com/parallelworks/meridian/problems"
)

// testSessionStore is a store's sessions as a test keeps them: their
// codes, and what a host records of how it serves each.
type testSessionStore struct {
	mu     sync.Mutex
	codes  map[string]bool
	served []string
}

func (s *testSessionStore) AddSessionCode(_ context.Context, id string, mac []byte, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.codes == nil {
		s.codes = map[string]bool{}
	}
	s.codes[id+string(mac)] = true
	return nil
}

func (s *testSessionStore) RedeemSessionCode(_ context.Context, id string, mac []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ok := s.codes[id+string(mac)]
	delete(s.codes, id+string(mac))
	return ok
}

func (s *testSessionStore) SessionServed(_ context.Context, id string, port int, p *problem.Problem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p != nil {
		s.served = append(s.served, id+" "+string(p.Code))
	} else {
		s.served = append(s.served, id+" "+strconv.Itoa(port))
	}
	return nil
}

func (s *testSessionStore) records() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.served...)
}

// testAnnouncer records what a host says it serves.
type testAnnouncer struct {
	mu   sync.Mutex
	said []string
}

func (a *testAnnouncer) Session(run, key, link string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.said = append(a.said, run+" "+key+" "+link)
	return nil
}

func (a *testAnnouncer) lines() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.said...)
}

// newTestSessionHost returns a SessionHost of the user alice, whose
// clusters file has the cluster hpc, whose host is farm, whose dials to a
// cluster's service it records in dials and makes to 127.0.0.1, attached
// to a store of the test's.
func newTestSessionHost(t *testing.T) (*SessionHost, *testSessionStore, *[]string) {
	t.Helper()
	cfg, err := LoadConfig(configDir(t, map[string]string{ClustersFile: "hpc:\n  host: farm\n"}), lookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu    sync.Mutex
		dials []string
	)
	dial := func(ctx context.Context, clusterHost, host string, port int) (net.Conn, error) {
		mu.Lock()
		dials = append(dials, fmt.Sprintf("%s %s %d", clusterHost, host, port))
		mu.Unlock()
		return (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	}
	h, err := NewSessionHost(cfg, "alice", []byte(strings.Repeat("k", sessionproxy.KeyLen)), dial, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	store := &testSessionStore{}
	h.Attach("store", store)
	return h, store, &dials
}

// testSession is a session of the run 7, under the key web, made, and not
// yet pointed anywhere.
func testSession(def map[string]any) coordinator.Session {
	return coordinator.Session{
		ID: "abc", Root: "7", Run: "7", Key: "web", Name: "web-1", Type: "tunnel",
		Label: sessionproxy.NewLabel(), Salt: []byte("salt-1234567890a"), Def: def,
	}
}

// TestCheckSessions checks how meridian run checks and names a workflow's
// sessions: by prompt-for-name's default, a literal one a URL's path holds,
// or by key and random bits; never one meridian run can't serve; and those
// of a workflow a step calls, which the coordinator names, it only checks.
func TestCheckSessions(t *testing.T) {
	t.Parallel()
	names, err := checkSessions("alice", map[string]any{
		"desktop": map[string]any{"prompt-for-name": map[string]any{"default": "my-desktop"}},
		"Web UI!": map[string]any{},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if names["desktop"] != "my-desktop" || !regexp.MustCompile(`^web-ui-[a-z2-7]{8}$`).MatchString(names["Web UI!"]) {
		t.Errorf("the sessions are named %v", names)
	}
	again, _ := checkSessions("alice", map[string]any{"Web UI!": map[string]any{}}, false)
	if again["Web UI!"] == names["Web UI!"] {
		t.Errorf("two runs' sessions are both named %q", again["Web UI!"])
	}
	for _, tc := range []struct {
		def  map[string]any
		kind *problems.Kind
	}{
		{map[string]any{"detach": true}, problems.SessionFieldUnsupported},
		{map[string]any{"openAI": true}, problems.SessionFieldUnsupported},
		{map[string]any{"useCustomDomain": true}, problems.SessionFieldUnsupported},
		{map[string]any{"prompt-for-name": map[string]any{}}, problems.SessionNameNeeded},
		{map[string]any{"prompt-for-name": nil}, problems.SessionNameNeeded},
		{map[string]any{"prompt-for-name": map[string]any{"default": "a/b"}}, problems.SessionNameNeeded},
		{map[string]any{"prompt-for-name": map[string]any{"default": "${{ inputs.x }}"}}, problems.SessionNameNeeded},
	} {
		if _, err := checkSessions("alice", map[string]any{"s": tc.def}, false); !isKind(err, tc.kind) {
			t.Errorf("checkSessions(%v) = %v, want %s", tc.def, err, tc.kind.Code())
		}
	}
	if _, err := checkSessions("a b", map[string]any{"s": map[string]any{}}, false); !isKind(err, problems.SessionUserUnsupported) {
		t.Errorf("a login name a path can't hold = %v", err)
	}
	// A called workflow's: prompt-for-name asks nothing, and detach: is
	// refused all the same.
	if names, err := checkSessions("alice", map[string]any{"s": map[string]any{"prompt-for-name": nil}}, true); err != nil || len(names) != 0 {
		t.Errorf("a called workflow's sessions = %v, %v", names, err)
	}
	if _, err := checkSessions("alice", map[string]any{"s": map[string]any{"detach": true}}, true); !isKind(err, problems.SessionFieldUnsupported) {
		t.Errorf("a called workflow's detached session = %v", err)
	}
	if names, err := (SessionNames{User: "alice"}).Check(t.Context(), map[string]any{"s": map[string]any{}}); err != nil || names["s"] == "" {
		t.Errorf("SessionNames.Check = %v, %v", names, err)
	}
}

// isKind reports whether err is a problem of kind k.
func isKind(err error, k *problems.Kind) bool {
	p, ok := errors.AsType[*problem.Problem](err)
	return ok && p.Code == k.Code()
}

// TestSessionPoint checks how a host reads where update-session points a
// session, as format 1 reads its with:, where it doesn't serve it.
func TestSessionPoint(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestSessionHost(t)
	s := testSession(map[string]any{})
	s.Port = 4321
	point := func(with map[string]any, def map[string]any) (coordinator.SessionPoint, error) {
		s := s
		if def != nil {
			s.Def = def
		}
		return h.Point(t.Context(), s, compile.SessionUpdate{Name: s.Name, With: with}, false)
	}
	for _, tc := range []struct {
		with      map[string]any
		point     string
		port      int
		portGiven bool
	}{
		{map[string]any{"remotePort": float64(8888)}, `{"type":"tunnel","remoteHost":"localhost","remotePort":8888}`, 4321, false},
		{map[string]any{"remotePort": "8888", "remoteHost": "elsewhere", "target": "user-workspace"}, `{"type":"tunnel","remoteHost":"localhost","remotePort":8888}`, 4321, false},
		{map[string]any{"remotePort": "8888", "target": "localhost", "localPort": "9000"}, `{"type":"tunnel","remoteHost":"localhost","remotePort":8888}`, 9000, true},
		{map[string]any{"remotePort": float64(80), "target": "hpc", "remoteHost": "node-1", "slug": "lab?x=1"}, `{"type":"tunnel","cluster":"hpc","remoteHost":"node-1","remotePort":80,"slug":"lab?x=1"}`, 4321, false},
		{map[string]any{"remotePort": float64(80), "target": "hpc"}, `{"type":"tunnel","cluster":"hpc","remoteHost":"localhost","remotePort":80}`, 4321, false},
		{map[string]any{"type": "link", "url": "https://example.com/x"}, `{"type":"link","url":"https://example.com/x"}`, 0, false},
		{map[string]any{"remotePort": float64(1), "status": "running", "targetInfo": nil, "openAI": false}, `{"type":"tunnel","remoteHost":"localhost","remotePort":1}`, 4321, false},
	} {
		got, err := point(tc.with, nil)
		if err != nil || string(got.Point) != tc.point || got.Port != tc.port || got.PortGiven != tc.portGiven {
			t.Errorf("Point(%v) = %s, %d, %t, %v; want %s, %d, %t", tc.with, got.Point, got.Port, got.PortGiven, err, tc.point, tc.port, tc.portGiven)
		}
	}
	for _, tc := range []struct {
		with map[string]any
		def  map[string]any
		kind *problems.Kind
	}{
		{map[string]any{"remotePort": "x"}, nil, problems.SessionPortInvalid},
		{map[string]any{}, nil, problems.SessionPortInvalid},
		{map[string]any{"remotePort": float64(0)}, nil, problems.SessionPortInvalid},
		{map[string]any{"remotePort": float64(65536)}, nil, problems.SessionPortInvalid},
		{map[string]any{"remotePort": float64(1.5)}, nil, problems.SessionPortInvalid},
		{map[string]any{"remotePort": "+80"}, nil, problems.SessionPortInvalid},
		{map[string]any{"remotePort": "-1"}, nil, problems.SessionPortInvalid},
		{map[string]any{"remotePort": float64(1), "localPort": "0"}, nil, problems.SessionPortInvalid},
		{map[string]any{"remotePort": float64(1), "target": "nowhere"}, nil, problems.SessionTargetUnknown},
		{map[string]any{"type": "link", "url": "ftp://example.com/"}, nil, problems.SessionURLInvalid},
		{map[string]any{"type": "link", "url": "/relative"}, nil, problems.SessionURLInvalid},
		{map[string]any{"remotePort": float64(1), "targetInfo": map[string]any{"name": "pod"}}, nil, problems.SessionUpdateUnsupported},
		{map[string]any{"remotePort": float64(1), "openAI": true}, nil, problems.SessionUpdateUnsupported},
		{map[string]any{"remotePort": float64(1), "apiKey": "k"}, nil, problems.SessionUpdateUnsupported},
		{map[string]any{"remotePort": float64(1)}, map[string]any{"useTLS": true}, problems.SessionUpdateUnsupported},
	} {
		if _, err := point(tc.with, tc.def); !isKind(err, tc.kind) {
			t.Errorf("Point(%v) of %v = %v, want %s", tc.with, tc.def, err, tc.kind.Code())
		}
	}
	// A tunnel to a cluster ignores useTLS, as format 1 does.
	if _, err := point(map[string]any{"remotePort": float64(1), "target": "hpc"}, map[string]any{"useTLS": true}); err != nil {
		t.Errorf("a tunnel to a cluster of a session whose useTLS is true = %v", err)
	}
}

// sessionClient returns a client that reaches the port of 127.0.0.1 a
// session is served at, whatever name a URL gives, as a browser resolves
// <label>.localhost, and keeps cookies, as a browser does, and follows no
// redirect.
func sessionClient(t *testing.T) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Client{
		Jar: jar,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			_, port, _ := net.SplitHostPort(addr)
			return (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", port))
		}},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Timeout:       10 * time.Second,
	}
}

// get returns what client gets of u: its status and its body.
func get(t *testing.T, client *http.Client, u string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, u, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// TestSessionHostServes checks a session a host serves as a step points it
// at a service of this machine: it says so, with the command that makes a
// link, or the link itself on a person's terminal; a link's code gives a
// browser the session's cookie, with which it reaches the service, until
// the session's salt changes; a step that points it elsewhere has it go
// there; and once the session ends, nothing listens at its port.
func TestSessionHostServes(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestSessionHost(t)
	said := &testAnnouncer{}
	h.AnnounceTo(said, false)
	service := func(name string) *httptest.Server {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = fmt.Fprintf(w, "%s saw %s under %s", name, r.URL.Path, r.Header.Get("X-Forwarded-Prefix"))
		}))
		t.Cleanup(srv.Close)
		return srv
	}
	portOf := func(srv *httptest.Server) float64 {
		u, _ := url.Parse(srv.URL)
		p, _ := strconv.Atoi(u.Port())
		return float64(p)
	}
	one, two := service("one"), service("two")
	s := testSession(map[string]any{})
	sp, err := h.Point(t.Context(), s, compile.SessionUpdate{Name: s.Name, With: map[string]any{"remotePort": portOf(one), "slug": "lab"}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if lines := said.lines(); len(lines) != 1 || lines[0] != "7 web " {
		t.Errorf("the host said %q", lines)
	}
	s.Point, s.Port = sp.Point, sp.Port
	link, err := h.Link(t.Context(), s)
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + net.JoinHostPort(sessionproxy.HostOf(s.Label), strconv.Itoa(sp.Port)) + "/me/session/alice/web-1/"
	if !strings.HasPrefix(link, base+"lab?"+sessionproxy.CodeParam+"=") {
		t.Errorf("the link is %s, want one under %s", link, base)
	}
	client := sessionClient(t)
	if code, _ := get(t, client, base+"x"); code != http.StatusUnauthorized {
		t.Errorf("without the code, the session answers %d", code)
	}
	if code, _ := get(t, client, link); code != http.StatusOK {
		t.Fatalf("the link's code answers %d", code)
	}
	if code, body := get(t, client, base+"x"); code != http.StatusOK || body != "one saw /x under /me/session/alice/web-1" {
		t.Errorf("with the cookie, the session answers %d %q", code, body)
	}
	// Another step points it elsewhere, at the same port.
	if _, err := h.Point(t.Context(), s, compile.SessionUpdate{Name: s.Name, With: map[string]any{"remotePort": portOf(two)}}, true); err != nil {
		t.Fatal(err)
	}
	if code, body := get(t, client, base+"y"); code != http.StatusOK || !strings.HasPrefix(body, "two saw /y") {
		t.Errorf("pointed elsewhere, the session answers %d %q", code, body)
	}
	// Reset: the cookie works no more.
	s.Salt = []byte("salt-0987654321b")
	h.Serve(t.Context(), []coordinator.Session{s})
	if code, _ := get(t, client, base+"y"); code != http.StatusUnauthorized {
		t.Errorf("once reset, the session answers the old cookie %d", code)
	}
	// On a person's terminal, a link with a code.
	h.AnnounceTo(said, true)
	s2 := testSession(map[string]any{})
	s2.ID, s2.Key, s2.Name = "def", "other", "other-1"
	if _, err := h.Point(t.Context(), s2, compile.SessionUpdate{Name: s2.Name, With: map[string]any{"remotePort": portOf(one)}}, true); err != nil {
		t.Fatal(err)
	}
	if lines := said.lines(); len(lines) != 2 || !strings.Contains(lines[1], "/me/session/alice/other-1/?"+sessionproxy.CodeParam+"=") {
		t.Errorf("on a terminal, the host said %q", lines)
	}
	// Ended: nothing listens at its port.
	h.Serve(t.Context(), nil)
	if conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(sp.Port)), time.Second); err == nil {
		conn.Close()
		t.Error("once its session ended, its port still listens")
	}
}

// TestSessionHostServesAgain checks a session a host takes over, as
// another process stopped serving it: at its port, or, where another
// program holds that now, at one the system picks, unless the step gave
// it, which the store keeps; one it can't serve it says so, once, in the
// store; a cluster's service it reaches through the cluster's host; and a
// link session it says once, and again only where it points elsewhere.
func TestSessionHostServesAgain(t *testing.T) {
	t.Parallel()
	h, store, dials := newTestSessionHost(t)
	said := &testAnnouncer{}
	h.AnnounceTo(said, false)
	held, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	taken := held.Addr().(*net.TCPAddr).Port
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	svcPort, _ := strconv.Atoi(u.Port())

	moved := testSession(map[string]any{})
	moved.Point, moved.Port = []byte(fmt.Sprintf(`{"type":"tunnel","cluster":"hpc","remoteHost":"node-1","remotePort":%d}`, svcPort)), taken
	given := testSession(map[string]any{})
	given.ID, given.Key, given.Name = "given", "given", "given-1"
	given.Point, given.Port, given.PortGiven = []byte(`{"type":"tunnel","remoteHost":"localhost","remotePort":1}`), taken, true
	link := testSession(map[string]any{})
	link.ID, link.Key, link.Name = "link", "docs", "docs-1"
	link.Point = []byte(`{"type":"link","url":"https://example.com/a"}`)
	h.Serve(t.Context(), []coordinator.Session{moved, given, link})
	records := store.records()
	if len(records) != 2 || !strings.HasPrefix(records[0], "abc ") || records[0] == "abc "+strconv.Itoa(taken) ||
		records[1] != "given "+string(problems.SessionLocalPortTaken.Code()) {
		t.Fatalf("the store keeps %q", records)
	}
	newPort, _ := strconv.Atoi(strings.TrimPrefix(records[0], "abc "))
	// The store says so, as the coordinator gives it again: nothing moves,
	// and nothing is said again.
	moved.Port = newPort
	given.Problem = problems.SessionLocalPortTaken.Problem(problems.Params{"port": strconv.Itoa(taken)})
	for range 2 {
		h.Serve(t.Context(), []coordinator.Session{moved, given, link})
	}
	if again := store.records(); len(again) != 2 {
		t.Errorf("the store keeps %q, said again", again)
	}
	if _, err := h.Link(t.Context(), given); !isKind(err, problems.SessionLocalPortTaken) {
		t.Errorf("a link to a session that can't be served = %v", err)
	}
	if lines := said.lines(); len(lines) != 2 || lines[0] != "7 web " || lines[1] != "7 docs https://example.com/a" {
		t.Errorf("the host said %q", lines)
	}
	link.Point = []byte(`{"type":"link","url":"https://example.com/b"}`)
	h.Serve(t.Context(), []coordinator.Session{moved, given, link})
	if lines := said.lines(); len(lines) != 3 || lines[2] != "7 docs https://example.com/b" {
		t.Errorf("pointed elsewhere, the link is said as %q", lines)
	}

	client := sessionClient(t)
	l, err := h.Link(t.Context(), moved)
	if err != nil {
		t.Fatal(err)
	}
	if code, _ := get(t, client, l); code != http.StatusOK {
		t.Fatalf("the link's code answers %d", code)
	}
	base := "http://" + net.JoinHostPort(sessionproxy.HostOf(moved.Label), strconv.Itoa(newPort)) + "/me/session/alice/web-1/"
	if code, body := get(t, client, base); code != http.StatusOK || body != "ok" {
		t.Errorf("the cluster's service answers %d %q", code, body)
	}
	if got := *dials; len(got) == 0 || got[0] != fmt.Sprintf("farm node-1 %d", svcPort) {
		t.Errorf("the host dialed %q", got)
	}
}

// TestSessionHostLink checks the links meridian session url prints: none
// for a session that points nowhere, or isn't served at a port yet; a link
// session's own url; and a tunnel's at its host name, its path and its
// port, with what its slug says after it.
func TestSessionHostLink(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestSessionHost(t)
	s := testSession(map[string]any{})
	if _, err := h.Link(t.Context(), s); !isKind(err, problems.SessionNotReady) {
		t.Errorf("a link to a session that points nowhere = %v", err)
	}
	s.Point = []byte(`{"type":"tunnel","remoteHost":"localhost","remotePort":1}`)
	if _, err := h.Link(t.Context(), s); !isKind(err, problems.SessionNotReady) {
		t.Errorf("a link to a session served at no port yet = %v", err)
	}
	s.Point = []byte(`{"type":"link","url":"https://example.com/"}`)
	if got, err := h.Link(t.Context(), s); err != nil || got != "https://example.com/" {
		t.Errorf("a link session's link = %q, %v", got, err)
	}
	host := sessionproxy.HostOf(s.Label)
	for slug, want := range map[string]string{
		"":             "http://" + host + ":8000/me/session/alice/web-1/?" + sessionproxy.CodeParam + "=c",
		"/lab/tree":    "http://" + host + ":8000/me/session/alice/web-1/lab/tree?" + sessionproxy.CodeParam + "=c",
		"lab?token=x":  "http://" + host + ":8000/me/session/alice/web-1/lab?token=x&" + sessionproxy.CodeParam + "=c",
		"vnc.html#top": "http://" + host + ":8000/me/session/alice/web-1/vnc.html?" + sessionproxy.CodeParam + "=c#top",
		// A code of the slug's own the proxy would read first.
		"lab?meridian-code=x&a=1": "http://" + host + ":8000/me/session/alice/web-1/lab?a=1&" + sessionproxy.CodeParam + "=c",
		"lab?meridian%2Dcode=x":   "http://" + host + ":8000/me/session/alice/web-1/lab?" + sessionproxy.CodeParam + "=c",
		"lab?a=1&&b":              "http://" + host + ":8000/me/session/alice/web-1/lab?a=1&b&" + sessionproxy.CodeParam + "=c",
	} {
		if got := SessionLink("alice", s, 8000, slug, "c"); got != want {
			t.Errorf("SessionLink with the slug %q = %s, want %s", slug, got, want)
		}
	}
	if got := SessionLink("alice", s, 8000, "", ""); strings.Contains(got, sessionproxy.CodeParam) {
		t.Errorf("a link without a code is %s", got)
	}
}

// listenOn listens at port of address, of network, or skips the test where
// the system has no such address, as one without IPv6.
func listenOn(t *testing.T, network, address string, port int) net.Listener {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), network, net.JoinHostPort(address, strconv.Itoa(port)))
	if err != nil {
		if strings.Contains(err.Error(), "cannot assign requested address") || strings.Contains(err.Error(), "address family not supported") {
			t.Skipf("no %s here: %v", address, err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

// serveText answers each request on ln with text.
func serveText(t *testing.T, ln net.Listener, text string) {
	t.Helper()
	srv := &httptest.Server{Listener: ln, Config: &http.Server{ReadHeaderTimeout: 10 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, text)
	})}}
	srv.Start()
	t.Cleanup(srv.Close)
}

// TestSessionDialer checks where a session's requests go: a service of
// this machine's, at 127.0.0.1, even where another program listens at the
// same port of [::1], which a resolver can give for localhost first; at
// [::1] only for one that refuses at 127.0.0.1, as one that listens there
// alone does; and a cluster's localhost, as a step names it, at
// 127.0.0.1 too, by its host.
func TestSessionDialer(t *testing.T) {
	t.Parallel()
	h, _, dials := newTestSessionHost(t)
	service := listenOn(t, "tcp4", "127.0.0.1", 0)
	port := service.Addr().(*net.TCPAddr).Port
	serveText(t, service, "the service")
	serveText(t, listenOn(t, "tcp6", "::1", port), "another program")
	read := func(dial sessionproxy.DialFunc) string {
		t.Helper()
		conn, err := dial(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := io.WriteString(conn, "GET / HTTP/1.0\r\nHost: localhost\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		resp, err := io.ReadAll(conn)
		if err != nil {
			t.Fatal(err)
		}
		_, body, _ := strings.Cut(string(resp), "\r\n\r\n")
		return body
	}
	if got := read(h.dialer(sessionPoint{Type: "tunnel", RemoteHost: "localhost", RemotePort: port})); got != "the service" {
		t.Errorf("a session of this machine's service reached %q", got)
	}
	free := listenOn(t, "tcp4", "127.0.0.1", 0)
	alone := free.Addr().(*net.TCPAddr).Port
	_ = free.Close()
	serveText(t, listenOn(t, "tcp6", "::1", alone), "a service on [::1]")
	if got := read(h.dialer(sessionPoint{Type: "tunnel", RemoteHost: "localhost", RemotePort: alone})); got != "a service on [::1]" {
		t.Errorf("a session of a service on [::1] alone reached %q", got)
	}
	for _, remote := range []string{"localhost", "", "LocalHost", "node-1"} {
		conn, err := h.dialer(sessionPoint{Type: "tunnel", Cluster: "hpc", RemoteHost: remote, RemotePort: port})(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		conn.Close()
	}
	want := []string{
		fmt.Sprintf("farm 127.0.0.1 %d", port), fmt.Sprintf("farm 127.0.0.1 %d", port),
		fmt.Sprintf("farm 127.0.0.1 %d", port), fmt.Sprintf("farm node-1 %d", port),
	}
	if got := *dials; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("the cluster's host was asked for %q, want %q", got, want)
	}
}
