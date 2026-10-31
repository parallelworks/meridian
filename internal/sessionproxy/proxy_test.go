package sessionproxy

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/internal/leaktest"
	"github.com/parallelworks/meridian/problems"
)

// The tests listen on 127.0.0.1 alone: the proxy through Listen, and the
// services they stand up through loopback.

const (
	testUser = "alice"
	testName = "desktop-7k2m9q4x"
	prefix   = "/me/session/" + testUser + "/" + testName
)

// loopback listens on a port of 127.0.0.1 that the system picks.
func loopback(t *testing.T) net.Listener {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// A seen is what the service saw of a request, which it answers with.
type seen struct {
	Method, Host, Path, RawPath, Query string
	Header                             http.Header
}

// service serves h on 127.0.0.1, or, for nil, answers each request with
// what it saw of it, but for /ws, which it upgrades and echoes, as a
// WebSocket's service would once it has answered the upgrade, and a few
// paths that answer as the tests ask. It returns its address.
func service(t *testing.T, h http.Handler) string {
	t.Helper()
	l := loopback(t)
	if h == nil {
		h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if rp := r.URL.Query().Get("rp"); rp != "" {
				w.Header().Set("Referrer-Policy", rp)
			}
			switch r.URL.Path {
			case "/ws":
				if r.Header.Get("Upgrade") != "websocket" {
					http.Error(w, "not an upgrade", http.StatusBadRequest)
					return
				}
				conn, buf, err := http.NewResponseController(w).Hijack()
				if err != nil {
					return
				}
				defer conn.Close()
				_, _ = buf.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
				_ = buf.Flush()
				_, _ = io.Copy(conn, buf)
				return
			case "/cookies":
				for _, c := range r.URL.Query()["c"] {
					w.Header().Add("Set-Cookie", c)
				}
				w.Header().Set("Clear-Site-Data", `"cookies"`)
			case "/redirect":
				w.Header().Set("Location", r.URL.Query().Get("to"))
				w.WriteHeader(http.StatusFound)
				return
			}
			data, _ := json.Marshal(seen{r.Method, r.Host, r.URL.Path, r.URL.RawPath, r.URL.RawQuery, r.Header})
			_, _ = w.Write(data)
		})
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	return l.Addr().String()
}

// memCodes are one-time codes in memory, as the coordinator's store keeps
// them, by session and keyed hash, until each is used or expires, by its
// clock, now.
type memCodes struct {
	mu    sync.Mutex
	codes map[string]time.Time
	now   func() time.Time
}

func (m *memCodes) add(k *Keyring, session string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	code, mac := k.NewCode(session)
	if m.codes == nil {
		m.codes = map[string]time.Time{}
	}
	m.codes[session+" "+hex.EncodeToString(mac)] = m.clock().Add(CodeLifetime)
	return code
}

func (m *memCodes) Redeem(_ context.Context, session string, mac []byte) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := session + " " + hex.EncodeToString(mac)
	expires, ok := m.codes[key]
	delete(m.codes, key)
	return ok && m.clock().Before(expires)
}

func (m *memCodes) clock() time.Time {
	if m.now != nil {
		return m.now()
	}
	return time.Now()
}

// A rig is a proxy that serves a session on 127.0.0.1, at addr, whose
// service the rig's dial reaches, counting its dials, and whose requests
// name the session's own host, host, with addr's port, as a browser sends
// them to it.
type rig struct {
	t       *testing.T
	p       *Proxy
	keys    *Keyring
	codes   *memCodes
	addr    string
	host    string
	dials   atomic.Int32
	client  *http.Client
	service string
}

// newRig serves a session whose service h serves (service), with the
// config cfg changes.
func newRig(t *testing.T, h http.Handler, with ...func(*Config)) *rig {
	t.Helper()
	keys, err := NewKeyring(make([]byte, KeyLen))
	if err != nil {
		t.Fatal(err)
	}
	r := &rig{t: t, keys: keys, codes: &memCodes{}, service: service(t, h)}
	secret, err := keys.Secret("store", "s1", make([]byte, SaltLen))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		User: testUser, Name: testName, Label: NewLabel(), Run: "7", Key: "desktop", Session: "s1",
		Secret: secret, Keyring: keys, Codes: r.codes,
		Dial: func(ctx context.Context) (net.Conn, error) {
			r.dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, "tcp4", r.service)
		},
	}
	for _, f := range with {
		f(&cfg)
	}
	if r.p, err = New(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	l, err := Listen(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	r.addr = net.JoinHostPort("127.0.0.1", strconv.Itoa(l.Port()))
	r.host = net.JoinHostPort(r.p.Host(), strconv.Itoa(l.Port()))
	served := make(chan error, 1)
	go func() { served <- r.p.Serve(l) }()
	t.Cleanup(func() {
		_ = r.p.Close()
		if err := <-served; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	r.client = &http.Client{
		Transport:     &http.Transport{DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return r
}

// An answer is what the proxy answered a request with, but its body,
// which do reads, and closes.
type answer struct {
	StatusCode int
	Status     string
	Header     http.Header
	cookies    []*http.Cookie
}

func (a answer) Cookies() []*http.Cookie { return a.cookies }

// do sends a request of method to path on the proxy, with the headers
// header, and returns its answer, and its body.
func (r *rig) do(method, path string, header http.Header) (answer, string) {
	r.t.Helper()
	req, err := http.NewRequestWithContext(r.t.Context(), method, "http://"+r.addr+path, nil)
	if err != nil {
		r.t.Fatal(err)
	}
	req.Host = r.host
	for k, vs := range header {
		for _, v := range vs {
			if k == "Host" {
				req.Host = v
				continue
			}
			req.Header.Add(k, v)
		}
	}
	resp, err := r.client.Do(req)
	if err != nil {
		r.t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		r.t.Fatal(err)
	}
	return answer{resp.StatusCode, resp.Status, resp.Header, resp.Cookies()}, string(body)
}

// login redeems a code, and returns the cookie it gave, as a Cookie
// header's value.
func (r *rig) login() string {
	r.t.Helper()
	resp, _ := r.do("GET", prefix+"/?"+CodeParam+"="+r.codes.add(r.keys, "s1"), nil)
	cookies := resp.Cookies()
	if resp.StatusCode != http.StatusOK || len(cookies) != 1 {
		r.t.Fatalf("redeeming a code gave %s, cookies %v", resp.Status, cookies)
	}
	return cookies[0].Name + "=" + cookies[0].Value
}

// wantPage checks that an answer is a page of kind k, of status, in
// English, unless the request asked for another language, which no browser
// reads as anything but text, and no cache keeps.
func wantPage(t *testing.T, resp answer, body string, status int, k *problems.Kind) {
	t.Helper()
	h := resp.Header
	if resp.StatusCode != status || !strings.HasSuffix(body, "("+string(k.Code())+")\n") ||
		h.Get("Content-Type") != "text/plain; charset=utf-8" || h.Get("X-Content-Type-Options") != "nosniff" || h.Get("Cache-Control") != "no-store" {
		t.Errorf("got %s %v %q, want a page of %s, %d", resp.Status, h, body, k.Code(), status)
	}
}

// TestCodeAndCookie checks the session's credentials: a link's one-time
// code gives the browser the session's cookie, which holds its secret, goes
// only to the session's host, is held to the session's path, is HttpOnly
// and SameSite=Strict, and is all it needs after; a page sends it on to
// the same URL without the code, from the session's own origin, which no
// cache keeps, with no referrer. A code works once, and only until it
// expires; a request without the cookie, with another session's, or with
// one whose secret was replaced, as reset does, is refused, and told how
// to get a link.
func TestCodeAndCookie(t *testing.T) {
	now := time.Now()
	r := newRig(t, nil)
	r.codes.now = func() time.Time { return now }
	code := r.codes.add(r.keys, "s1")
	resp, body := r.do("GET", prefix+"/x?a=1&"+CodeParam+"="+code+"&b=2", nil)
	cookies := resp.Cookies()
	h := resp.Header
	switch {
	case resp.StatusCode != http.StatusOK || !strings.Contains(body, `<meta http-equiv="refresh" content="0;url=`+prefix+`/x?a=1&amp;b=2">`):
		t.Errorf("redeeming a code gave %s %q", resp.Status, body)
	case h.Get("Cache-Control") != "no-store" || h.Get("Referrer-Policy") != "no-referrer" ||
		h.Get("Content-Security-Policy") != "default-src 'none'" || h.Get("X-Content-Type-Options") != "nosniff":
		t.Errorf("the page's headers are %v", h)
	case len(cookies) != 1:
		t.Fatalf("the cookies set are %v", cookies)
	}
	c := cookies[0]
	set := h.Get("Set-Cookie")
	if !strings.HasPrefix(c.Name, CookiePrefix) || c.Path != prefix+"/" || c.Domain != "" || !c.HttpOnly || c.SameSite != http.SameSiteStrictMode ||
		c.MaxAge != 0 || !c.Expires.IsZero() || strings.Contains(strings.ToLower(set), "domain") {
		t.Errorf("the cookie is %q", set)
	}
	if resp, body := r.do("GET", prefix+"/x", http.Header{"Cookie": {c.Name + "=" + c.Value}}); resp.StatusCode != http.StatusOK || !strings.Contains(body, `"Path":"/x"`) {
		t.Errorf("with the cookie: %s %s", resp.Status, body)
	}

	// A code works once: a browser that holds the cookie is sent on
	// without the code, with the cookie it has, and any other refused.
	resp, body = r.do("GET", prefix+"/x?"+CodeParam+"="+code, http.Header{"Cookie": {c.Name + "=" + c.Value}})
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, `content="0;url=`+prefix+`/x"`) || len(resp.Cookies()) != 0 {
		t.Errorf("a used code, with the cookie, gave %s %q, cookies %v", resp.Status, body, resp.Cookies())
	}
	resp, body = r.do("GET", prefix+"/?"+CodeParam+"="+code, nil)
	wantPage(t, resp, body, http.StatusUnauthorized, problems.SessionUnauthorized)
	if !strings.Contains(body, "meridian session url 7 desktop") || len(resp.Cookies()) != 0 {
		t.Errorf("a used code gave %q, cookies %v", body, resp.Cookies())
	}
	// Until it expires.
	late := r.codes.add(r.keys, "s1")
	now = now.Add(CodeLifetime + time.Second)
	resp, body = r.do("GET", prefix+"/?"+CodeParam+"="+late, nil)
	wantPage(t, resp, body, http.StatusUnauthorized, problems.SessionUnauthorized)
	// Another session's code.
	other := r.codes.add(r.keys, "s2")
	resp, body = r.do("GET", prefix+"/?"+CodeParam+"="+other, nil)
	wantPage(t, resp, body, http.StatusUnauthorized, problems.SessionUnauthorized)

	otherSecret, _ := r.keys.Secret("store", "s2", make([]byte, SaltLen))
	for name, cookie := range map[string]string{
		"no cookie":         "",
		"another session's": c.Name + "=" + base64Of(otherSecret),
		"one changed":       c.Name + "=" + c.Value[:len(c.Value)-2] + "AA",
		"the secret, under a name of another session's": CookiePrefix + "0011223344556677=" + c.Value,
	} {
		header := http.Header{}
		if cookie != "" {
			header.Set("Cookie", cookie)
		}
		resp, body := r.do("GET", prefix+"/x", header)
		if wantPage(t, resp, body, http.StatusUnauthorized, problems.SessionUnauthorized); t.Failed() {
			t.Fatalf("%s: %s", name, body)
		}
	}

	// A reset replaces the secret: the old cookie no longer works.
	fresh, _ := r.keys.Secret("store", "s1", []byte("0123456789abcdef"))
	r.p.SetSecret(fresh)
	resp, body = r.do("GET", prefix+"/x", http.Header{"Cookie": {c.Name + "=" + c.Value}})
	wantPage(t, resp, body, http.StatusUnauthorized, problems.SessionUnauthorized)
	if cookie := r.login(); !strings.HasSuffix(cookie, "="+base64Of(fresh)) {
		t.Errorf("after the reset, a code gives %q", cookie)
	}
}

func base64Of(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// TestNoRootForm checks that nothing of a session is served at /, where a
// cookie would go to every port of 127.0.0.1: / and the session's path
// without its slash send the browser to the session's path, any other
// path outside it is nothing, and a cookie the service sets at / is held
// to the session's path.
func TestNoRootForm(t *testing.T) {
	r := newRig(t, nil)
	cookie := r.login()
	for path, want := range map[string]string{"/": prefix + "/", "/?a=b": prefix + "/?a=b", prefix: prefix + "/"} {
		resp, _ := r.do("GET", path, http.Header{"Cookie": {cookie}})
		if resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != want {
			t.Errorf("%s: %s to %q, want %q", path, resp.Status, resp.Header.Get("Location"), want)
		}
	}
	// A code there goes on to the session's path, which no cache keeps,
	// nor any referrer says.
	resp, _ := r.do("GET", "/?"+CodeParam+"=abc", nil)
	if resp.Header.Get("Location") != prefix+"/?"+CodeParam+"=abc" || resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Referrer-Policy") != "no-referrer" {
		t.Errorf("a code at / gave %s %v", resp.Status, resp.Header)
	}
	for _, path := range []string{"/x", "/me/session/" + testUser + "/other/", prefix + "x/", "/me/session/bob/" + testName + "/"} {
		resp, body := r.do("GET", path, http.Header{"Cookie": {cookie}})
		wantPage(t, resp, body, http.StatusNotFound, problems.SessionNotHere)
		if !strings.Contains(body, prefix+"/") {
			t.Errorf("%s: %q doesn't say where the session is", path, body)
		}
	}
	resp, _ = r.do("GET", prefix+"/cookies?c=app%3D1%3B+Path%3D%2F", http.Header{"Cookie": {cookie}})
	if got := resp.Header.Values("Set-Cookie"); len(got) != 1 || got[0] != "app=1; Path="+prefix+"/" {
		t.Errorf("a cookie at / is set as %q", got)
	}
}

// TestHost checks that only a request addressed to the session's own host
// name, <label>.localhost, case aside, at any port, as ssh -L to another
// one gives it, is served: one to 127.0.0.1, localhost or [::1], which
// any server on this machine has too, to another session's label, or to a
// host name a page made resolve to 127.0.0.1 (DNS rebinding), is refused,
// with the session's cookie too, before anything connects to the service.
func TestHost(t *testing.T) {
	r := newRig(t, nil)
	cookie := r.login()
	own := r.p.Host()
	for _, host := range []string{own + ":9999", strings.ToUpper(own) + ":1", own} {
		if resp, _ := r.do("GET", prefix+"/x", http.Header{"Host": {host}, "Cookie": {cookie}}); resp.StatusCode != http.StatusOK {
			t.Errorf("%s: %s", host, resp.Status)
		}
	}
	dials := r.dials.Load()
	port := strings.Split(r.addr, ":")[1]
	for _, host := range []string{
		"127.0.0.1:" + port, "localhost:" + port, "[::1]:" + port, "127.0.0.1", "localhost",
		HostOf(NewLabel()) + ":" + port, "x." + own, own + ".evil.example",
		"evil.example:" + port, "evil.example", "127.0.0.2:80", "0.0.0.0",
	} {
		resp, body := r.do("GET", prefix+"/x", http.Header{"Host": {host}, "Cookie": {cookie}})
		wantPage(t, resp, body, http.StatusMisdirectedRequest, problems.SessionHostRefused)
	}
	if r.dials.Load() != dials {
		t.Errorf("a refused host's request connected to the service")
	}
}

// TestOrigin checks that a WebSocket, and a request other than GET, HEAD
// or OPTIONS, is passed on only from the session's own pages: its Origin,
// if it has one, is the one it was sent to, with the port the browser sent
// it to, as through ssh -L; and its Sec-Fetch-Site, if it has one, is
// same-origin or none. Every port of 127.0.0.1 is one site, so another
// port's page is refused, with the session's cookie too, before anything
// connects to the service. A GET from anywhere is passed on.
func TestOrigin(t *testing.T) {
	r := newRig(t, nil)
	cookie := r.login()
	own := "http://" + r.host
	tunnel := r.p.Host() + ":9999"
	for name, tc := range map[string]struct {
		method string
		header http.Header
		status int
	}{
		"own origin":                     {"POST", http.Header{"Origin": {own}}, http.StatusOK},
		"no origin":                      {"POST", nil, http.StatusOK},
		"same-origin":                    {"DELETE", http.Header{"Sec-Fetch-Site": {"same-origin"}}, http.StatusOK},
		"none":                           {"PUT", http.Header{"Sec-Fetch-Site": {"none"}}, http.StatusOK},
		"through ssh -L":                 {"POST", http.Header{"Host": {tunnel}, "Origin": {"http://" + tunnel}}, http.StatusOK},
		"another port":                   {"POST", http.Header{"Origin": {"http://127.0.0.1:9999"}}, http.StatusForbidden},
		"127.0.0.1, at the port":         {"POST", http.Header{"Origin": {"http://" + r.addr}}, http.StatusForbidden},
		"another session's":              {"POST", http.Header{"Origin": {"http://" + HostOf(NewLabel()) + ":" + strings.Split(r.addr, ":")[1]}}, http.StatusForbidden},
		"https":                          {"POST", http.Header{"Origin": {"https://" + r.host}}, http.StatusForbidden},
		"null":                           {"POST", http.Header{"Origin": {"null"}}, http.StatusForbidden},
		"two origins":                    {"POST", http.Header{"Origin": {own, own}}, http.StatusForbidden},
		"same-site":                      {"POST", http.Header{"Origin": {own}, "Sec-Fetch-Site": {"same-site"}}, http.StatusForbidden},
		"cross-site":                     {"PATCH", http.Header{"Sec-Fetch-Site": {"cross-site"}}, http.StatusForbidden},
		"a navigation from another site": {"GET", http.Header{"Origin": {"http://127.0.0.1:9999"}, "Sec-Fetch-Site": {"cross-site"}, "Sec-Fetch-Mode": {"navigate"}, "Sec-Fetch-Dest": {"document"}}, http.StatusOK},
	} {
		dials := r.dials.Load()
		header := http.Header{"Cookie": {cookie}}
		maps.Copy(header, tc.header)
		resp, body := r.do(tc.method, prefix+"/x", header)
		switch {
		case tc.status == http.StatusForbidden:
			wantPage(t, resp, body, http.StatusForbidden, problems.SessionCrossOrigin)
			if r.dials.Load() != dials {
				t.Errorf("%s: a refused request connected to the service", name)
			}
		case resp.StatusCode != tc.status:
			t.Errorf("%s: %s %s", name, resp.Status, body)
		}
	}

	// A WebSocket from another port is refused; from the session's own
	// pages, it is passed on, and carries what the service echoes.
	for _, origin := range []string{"http://127.0.0.1:9999", own} {
		dials := r.dials.Load()
		conn, status := r.upgrade(origin, cookie)
		if origin != own {
			if status != http.StatusForbidden || r.dials.Load() != dials {
				t.Errorf("a WebSocket from another port gave %d", status)
			}
			continue
		}
		if status != http.StatusSwitchingProtocols {
			t.Fatalf("a WebSocket from the session's pages gave %d", status)
		}
		echo(t, conn)
		_ = conn.Close()
	}
}

// upgrade asks the proxy for a WebSocket at /ws, from origin, with the
// cookie cookie, and returns the connection and the status it answered.
func (r *rig) upgrade(origin, cookie string) (net.Conn, int) {
	r.t.Helper()
	conn, err := (&net.Dialer{}).DialContext(r.t.Context(), "tcp4", r.addr)
	if err != nil {
		r.t.Fatal(err)
	}
	fmt.Fprintf(conn, "GET %s/ws HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nOrigin: %s\r\nCookie: %s\r\n\r\n", prefix, r.host, origin, cookie)
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		r.t.Fatal(err)
	}
	// What follows a switch of protocols is the WebSocket's, which
	// ReadResponse leaves to br: its body is empty.
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		_ = conn.Close()
		return nil, resp.StatusCode
	}
	return &bufferedConn{Conn: conn, r: br}, resp.StatusCode
}

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) { return c.r.Read(b) }

// echo sends ping over conn, and fails the test unless it reads it back.
func echo(t *testing.T, conn net.Conn) {
	t.Helper()
	if _, err := io.WriteString(conn, "ping"); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("the WebSocket echoed %q, %v", buf, err)
	}
}

// TestNothingBeforeAuthentication checks that nothing connects to the
// service for a request that is refused: one without the session's
// cookie, or with a code that doesn't work, a WebSocket among them.
func TestNothingBeforeAuthentication(t *testing.T) {
	r := newRig(t, nil)
	for _, method := range []string{"GET", "POST", "HEAD", "OPTIONS"} {
		resp, body := r.do(method, prefix+"/x", nil)
		if resp.StatusCode != http.StatusUnauthorized || (method != "HEAD" && !strings.Contains(body, "session_unauthorized")) {
			t.Errorf("%s without the cookie: %s", method, resp.Status)
		}
	}
	r.do("GET", prefix+"/?"+CodeParam+"=nottherightcode", nil)
	if _, status := r.upgrade("http://"+r.host, ""); status != http.StatusUnauthorized {
		t.Errorf("a WebSocket without the cookie gave %d", status)
	}
	if n := r.dials.Load(); n != 0 {
		t.Errorf("%d connections to the service for requests that were refused", n)
	}
}

// TestForwarding checks what reaches the service: the path without the
// session's, which X-Forwarded-Prefix says, escaped as the browser escaped
// it; the query without a code; the cookies without any of the proxy's,
// the session's own or another's, and the rest as they were; the
// service's host; X-Forwarded-For, -Host and -Proto as the proxy saw them,
// over any the browser sent; and the browser's Authorization, which is the
// service's.
func TestForwarding(t *testing.T) {
	r := newRig(t, nil)
	cookie := r.login()
	header := http.Header{
		"Cookie":             {"app=1; " + cookie + "; " + CookiePrefix + "0011223344556677=other; Meridian-Session-AA=x; =nameless; theme=dark"},
		"Authorization":      {"Bearer service-token"},
		"X-Forwarded-For":    {"203.0.113.9"},
		"X-Forwarded-Host":   {"evil.example"},
		"X-Forwarded-Prefix": {"/elsewhere"},
	}
	// A code reaches the proxy alone: here, in a request that isn't a
	// link's, which the proxy redeems none of.
	resp, body := r.do("POST", prefix+"/a%2Fb/c?x=1&"+CodeParam+"=abc&y=%20", header)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s %s", resp.Status, body)
	}
	var s seen
	if err := json.Unmarshal([]byte(body), &s); err != nil {
		t.Fatal(err)
	}
	h := s.Header
	switch {
	case s.Path != "/a/b/c" || s.RawPath != "/a%2Fb/c":
		t.Errorf("the service was asked for %q, raw %q", s.Path, s.RawPath)
	case s.Query != "x=1&y=%20":
		t.Errorf("the service's query is %q", s.Query)
	case s.Host != upstreamHost:
		t.Errorf("the service's host is %q", s.Host)
	case !slices.Equal(h.Values("Cookie"), []string{"app=1; theme=dark"}):
		t.Errorf("the service's cookies are %q", h.Values("Cookie"))
	case h.Get("Authorization") != "Bearer service-token":
		t.Errorf("the service's Authorization is %q", h.Get("Authorization"))
	case h.Get("X-Forwarded-Prefix") != prefix || h.Get("X-Forwarded-Host") != r.host || h.Get("X-Forwarded-Proto") != "http" || h.Get("X-Forwarded-For") != "127.0.0.1":
		t.Errorf("the service's X-Forwarded headers are %v", h)
	}
}

// TestResponse checks what reaches the browser of the service's answer:
// each cookie held to the session's path, without a Domain, and none
// named as the proxy's are; no Clear-Site-Data; and a Location that
// leaves the session's path put back under it.
func TestResponse(t *testing.T) {
	r := newRig(t, nil)
	cookie := r.login()
	header := http.Header{"Cookie": {cookie}}
	query := "c=a%3D1%3B+Path%3D%2F%3B+Domain%3Dlocalhost%3B+HttpOnly" +
		"&c=" + CookiePrefix + "0011%3Devil%3B+Path%3D%2F" +
		"&c=Meridian-Code%3Devil" +
		"&c=%3Dmeridian-session-x%3Dv" +
		"&c=meridian-session-y" +
		"&c=b%3D2%3B+SameSite%3DLax%3B+Path%3D%2Fdeep"
	resp, _ := r.do("GET", prefix+"/cookies?"+query, header)
	want := []string{"a=1; HttpOnly; Path=" + prefix + "/", "b=2; SameSite=Lax; Path=" + prefix + "/"}
	if got := resp.Header.Values("Set-Cookie"); !slices.Equal(got, want) {
		t.Errorf("the cookies set are %q, want %q", got, want)
	}
	if v := resp.Header.Values("Clear-Site-Data"); len(v) != 0 {
		t.Errorf("Clear-Site-Data reached the browser: %q", v)
	}
	for to, want := range map[string]string{
		"/login?next=%2F":                 prefix + "/login?next=%2F",
		"http://localhost/a?b=1":          prefix + "/a?b=1",
		"http://localhost:8080/a?b=1":     "http://localhost:8080/a?b=1",
		prefix + "/already":               prefix + "/already",
		"relative/path":                   "relative/path",
		"https://example.com/elsewhere":   "https://example.com/elsewhere",
		"//example.com/protocol-relative": "//example.com/protocol-relative",
	} {
		resp, _ := r.do("GET", prefix+"/redirect?to="+url.QueryEscape(to), header)
		if got := resp.Header.Get("Location"); got != want {
			t.Errorf("Location %q reached the browser as %q, want %q", to, got, want)
		}
	}
}

// TestPages checks that a page is in the language the browser asks for,
// and that a service that doesn't answer is a page that names the
// session, and nothing of where it is.
func TestPages(t *testing.T) {
	r := newRig(t, nil, func(c *Config) {
		c.Dial = func(ctx context.Context) (net.Conn, error) {
			return nil, errors.New("connect to node-17.cluster.example:8888: refused")
		}
	})
	cookie := r.login()
	resp, body := r.do("GET", prefix+"/x", http.Header{"Cookie": {cookie}})
	wantPage(t, resp, body, http.StatusBadGateway, problems.SessionUnreachable)
	if !strings.Contains(body, testName) || strings.Contains(body, "node-17") {
		t.Errorf("the page is %q", body)
	}
	for _, lang := range []string{"es", "ja", "ko", "zh"} {
		resp, body := r.do("GET", prefix+"/x", http.Header{"Cookie": {cookie}, "Accept-Language": {lang + ";q=0.9, en;q=0.5"}})
		d := problems.SessionUnreachable.At("", problems.Position{}, "", problems.Params{"name": testName})
		if resp.Header.Get("Content-Language") != lang || !strings.HasPrefix(body, d.Localize(lang)) || d.Localize(lang) == d.Localize("en") {
			t.Errorf("%s: %s %q", lang, resp.Header.Get("Content-Language"), body)
		}
	}
}

// TestLimits checks that a session has at most MaxConns connections to
// its service open, or opening, at once: a request waits a bounded time,
// not its own, for one, and then is answered that the service doesn't
// answer; a dial it stops waiting for goes on, holding its place, until
// it ends, so a browser that retries a service that doesn't answer can't
// have more going on; and once they end, a request connects again.
func TestLimits(t *testing.T) {
	leaktest.Check(t)
	defer func(w time.Duration) { dialWait = w }(dialWait)
	dialWait = 200 * time.Millisecond
	var (
		mu          sync.Mutex
		opening, at int
		hold        = make(chan struct{})
		holding     atomic.Bool
	)
	holding.Store(true)
	var r *rig
	r = newRig(t, nil, func(c *Config) {
		c.MaxConns = 2
		c.Dial = func(ctx context.Context) (net.Conn, error) {
			mu.Lock()
			opening++
			at = max(at, opening)
			mu.Unlock()
			defer func() {
				mu.Lock()
				opening--
				mu.Unlock()
			}()
			if holding.Load() {
				select {
				case <-hold:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp4", r.service)
		}
	})
	cookie := r.login()
	var (
		wg              sync.WaitGroup
		unreached, busy atomic.Int32
	)
	start := time.Now()
	for range 5 {
		wg.Go(func() {
			// The two that dial find the service doesn't answer; the
			// rest, that the session has as many connections as it may.
			switch resp, body := r.do("GET", prefix+"/x", http.Header{"Cookie": {cookie}}); {
			case resp.StatusCode == http.StatusBadGateway && strings.Contains(body, "(session_unreachable)"):
				unreached.Add(1)
			case resp.StatusCode == http.StatusServiceUnavailable && strings.Contains(body, "(session_busy)") && strings.Contains(body, " 2 "):
				busy.Add(1)
			default:
				t.Errorf("a request to a service whose dials hang: %s %q", resp.Status, body)
			}
		})
	}
	wg.Wait()
	if unreached.Load() != 2 || busy.Load() != 3 {
		t.Errorf("%d requests found the service didn't answer, and %d the session busy; want 2 and 3", unreached.Load(), busy.Load())
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("the requests took %v, more than the bounded wait", took)
	}
	mu.Lock()
	if at > 2 || opening != 2 {
		t.Errorf("%d dials went on at once, at most, and %d go on now; want 2 and 2", at, opening)
	}
	mu.Unlock()
	holding.Store(false)
	close(hold)
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, _ := r.do("GET", prefix+"/x", http.Header{"Cookie": {cookie}})
		if resp.StatusCode == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("once the dials ended, a request still gives %s", resp.Status)
		}
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo // the abandoned dials giving their places back, which nothing signals
	}
}

// TestTimeouts checks that a browser has a bounded time to send a
// request's headers, and that a connection that waits for its next
// request is closed after a bounded time.
func TestTimeouts(t *testing.T) {
	leaktest.Check(t)
	defer func(h, i time.Duration) { readHeaderTimeout, idleTimeout = h, i }(readHeaderTimeout, idleTimeout)
	readHeaderTimeout, idleTimeout = 200*time.Millisecond, 300*time.Millisecond
	r := newRig(t, nil)
	cookie := r.login()

	slow, err := (&net.Dialer{}).DialContext(t.Context(), "tcp4", r.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	_, _ = io.WriteString(slow, "GET "+prefix+"/x HTTP/1.1\r\nHost: "+r.host+"\r\n")
	closedWithin(t, slow, 5*time.Second, "a request whose headers don't end")

	idle, err := (&net.Dialer{}).DialContext(t.Context(), "tcp4", r.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	fmt.Fprintf(idle, "GET %s/x HTTP/1.1\r\nHost: %s\r\nCookie: %s\r\n\r\n", prefix, r.host, cookie)
	br := bufio.NewReader(idle)
	resp, err := http.ReadResponse(br, nil)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("the first request gave %v, %v", resp, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	closedWithin(t, &bufferedConn{Conn: idle, r: br}, 5*time.Second, "an idle connection")
}

// closedWithin fails the test unless a read of conn ends, as the other end
// closes it, within d.
func closedWithin(t *testing.T, conn net.Conn, d time.Duration, what string) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(d))
	_, err := io.Copy(io.Discard, conn)
	if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
		t.Errorf("%s stayed open for %v", what, d)
	}
}

// TestRepointAndClose checks that a session pointed again connects to its
// new service for new requests, and that closing the proxy closes the
// connections to the service, a WebSocket's among them.
func TestRepointAndClose(t *testing.T) {
	leaktest.Check(t)
	r := newRig(t, nil)
	cookie := r.login()
	conn, status := r.upgrade("http://"+r.host, cookie)
	if status != http.StatusSwitchingProtocols {
		t.Fatalf("the WebSocket gave %d", status)
	}
	defer conn.Close()
	echo(t, conn)

	other := service(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "the new service") }))
	r.p.Repoint(func(ctx context.Context) (net.Conn, error) { return (&net.Dialer{}).DialContext(ctx, "tcp4", other) })
	client := &http.Client{Transport: &http.Transport{}}
	defer client.CloseIdleConnections()
	for range 2 {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+r.addr+prefix+"/x", nil)
		req.Header.Set("Cookie", cookie)
		req.Host = r.host
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if string(body) != "the new service" {
			t.Errorf("after Repoint, the proxy answered %q", body)
		}
	}
	// The WebSocket dialed before goes on until it ends.
	echo(t, conn)

	if err := r.p.Close(); err != nil {
		t.Fatal(err)
	}
	closedWithin(t, conn, 5*time.Second, "a WebSocket, once the proxy closed,")
}

// TestNew checks what a session's config must be: a login name and a
// session's name that a URL's path holds as they are, the login name's
// otherwise refused as session_user_unsupported.
func TestNew(t *testing.T) {
	keys, _ := NewKeyring(make([]byte, KeyLen))
	ok := Config{
		User: testUser, Name: testName, Label: NewLabel(), Secret: []byte("s"), Keyring: keys, Codes: &memCodes{},
		Dial: func(context.Context) (net.Conn, error) { return nil, errors.New("no") },
	}
	for _, user := range []string{"al ice", "alice/x", "ålice", "..", "", "alice%2F"} {
		cfg := ok
		cfg.User = user
		_, err := New(t.Context(), cfg)
		if p, isProblem := errors.AsType[*problem.Problem](err); !isProblem || p.Code != problems.SessionUserUnsupported.Code() || p.Params["user"] != user {
			t.Errorf("user %q: %v", user, err)
		}
	}
	for _, name := range []string{"a/b", "", ".", "a b", "a?b"} {
		cfg := ok
		cfg.Name = name
		if _, err := New(t.Context(), cfg); err == nil {
			t.Errorf("name %q was taken", name)
		}
	}
	for _, label := range []string{"", "short", strings.ToUpper(NewLabel()), NewLabel() + "a", "0" + NewLabel()[1:]} {
		cfg := ok
		cfg.Label = label
		if _, err := New(t.Context(), cfg); err == nil {
			t.Errorf("label %q was taken", label)
		}
	}
	for _, missing := range []func(*Config){
		func(c *Config) { c.Secret = nil }, func(c *Config) { c.Keyring = nil },
		func(c *Config) { c.Codes = nil }, func(c *Config) { c.Dial = nil },
	} {
		cfg := ok
		missing(&cfg)
		if _, err := New(t.Context(), cfg); err == nil {
			t.Error("a config without what a session needs was taken")
		}
	}
	p, err := New(t.Context(), ok)
	if err != nil {
		t.Fatal(err)
	}
	if p.Path() != prefix+"/" {
		t.Errorf("Path = %q", p.Path())
	}
}

// TestListen checks that a session's port listens on 127.0.0.1 alone, and
// that one in use, or one the system won't let the process listen on, is
// the problem that says so; and that Serve refuses a listener on another
// address.
func TestListen(t *testing.T) {
	l, err := Listen(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	a := l.v4.Addr().(*net.TCPAddr) //nolint:forcetypeassert // a TCP listener's
	if !a.IP.Equal(net.IPv4(127, 0, 0, 1)) || a.Port != l.Port() {
		t.Errorf("Listen listens on %v", a)
	}
	if l.v6 != nil {
		if b := l.v6.Addr().(*net.TCPAddr); !b.IP.Equal(net.IPv6loopback) || b.Port != a.Port { //nolint:forcetypeassert // a TCP listener's
			t.Errorf("Listen listens on %v, and %v", a, b)
		}
	}
	_, err = Listen(t.Context(), a.Port)
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.SessionLocalPortTaken.Code() || p.Params["port"] != strconv.Itoa(a.Port) || p.Params["picked"] != "false" {
		t.Errorf("a port in use gave %v", err)
	}
	if privileged, err := Listen(t.Context(), 1); err == nil {
		_ = privileged.Close()
		t.Log("this machine lets the process listen on port 1, so the refusal isn't checked here")
	} else if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.SessionLocalPortRefused.Code() || p.Params["port"] != "1" {
		t.Errorf("a privileged port gave %v", err)
	}

	keys, _ := NewKeyring(make([]byte, KeyLen))
	p, err := New(t.Context(), Config{
		User: testUser, Name: testName, Label: NewLabel(), Secret: []byte("s"), Keyring: keys, Codes: &memCodes{},
		Dial: func(context.Context) (net.Conn, error) { return nil, errors.New("no") },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Serve(&Listener{v4: anyAddress{}}); err == nil {
		t.Error("Serve took a listener on every address")
	}
}

// ipv6 skips a test where this machine has no IPv6 loopback.
func ipv6(t *testing.T) {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback here: %v", err)
	}
	_ = l.Close()
}

// TestListenIPv6 checks that a session's port is [::1]'s too, which a
// browser can try first for the session's host name: another program that
// holds [::1] at the port makes it one in use, rather than leaving it the
// session's first request, its code and its cookie; and the session is
// served over [::1] as over 127.0.0.1.
func TestListenIPv6(t *testing.T) {
	ipv6(t)
	held, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp6", "[::1]:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	port := held.Addr().(*net.TCPAddr).Port //nolint:forcetypeassert // a TCP listener's
	_, err = Listen(t.Context(), port)
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.SessionLocalPortTaken.Code() || p.Params["port"] != strconv.Itoa(port) {
		t.Errorf("a port another program holds on [::1] gave %v", err)
	}

	r := newRig(t, nil)
	cookie := r.login()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+net.JoinHostPort("::1", strings.Split(r.host, ":")[1])+prefix+"/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = r.host
	req.Header.Set("Cookie", cookie)
	resp, err := r.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("over [::1], the session answered %s", resp.Status)
	}
}

// TestListenPicksBoth checks how Listen picks a port: one the system picks
// on 127.0.0.1 that another program holds on [::1] is let go, and another
// picked, until one is free on both, or it has tried as often as it may,
// which is a port in use that the system picked; and where the system has
// no IPv6 loopback, which no program can listen on either, 127.0.0.1 alone
// does. A port the system refuses to pick is one it picked too.
func TestListenPicksBoth(t *testing.T) {
	defer func(f func(context.Context, string, string) (net.Listener, error)) { listen = f }(listen)
	real := listen
	var v6Tries int
	refuse := syscall.EADDRINUSE
	listen = func(ctx context.Context, network, address string) (net.Listener, error) {
		if network == "tcp6" {
			v6Tries++
			if refuse == syscall.EADDRNOTAVAIL || v6Tries < 3 {
				return nil, &net.OpError{Op: "listen", Net: network, Err: refuse}
			}
		}
		return real(ctx, network, address)
	}
	ipv6(t)
	l, err := Listen(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if l.v6 == nil || v6Tries != 3 {
		t.Errorf("after two ports taken on [::1], Listen tried %d, and listens on [::1]: %v", v6Tries, l.v6 != nil)
	}
	_ = l.Close()

	// Every port the system picks another program's on [::1]: one in use,
	// which the system picked.
	refuse, v6Tries = syscall.EADDRINUSE, -portTries
	_, err = Listen(t.Context(), 0)
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.SessionLocalPortTaken.Code() || p.Params["picked"] != "true" {
		t.Errorf("no port free on both in %d tries gave %v", portTries, err)
	}

	refuse = syscall.EADDRNOTAVAIL
	l, err = Listen(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if l.v6 != nil {
		t.Error("without IPv6 loopback, Listen listens on [::1]")
	}
	_ = l.Close()

	listen = func(context.Context, string, string) (net.Listener, error) {
		return nil, &net.OpError{Op: "listen", Net: "tcp4", Err: syscall.EACCES}
	}
	_, err = Listen(t.Context(), 0)
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.SessionLocalPortRefused.Code() || p.Params["picked"] != "true" {
		t.Errorf("a port the system won't pick gave %v", err)
	}
	// One the system picked on 127.0.0.1 that it won't let the process
	// listen on at [::1] is one it picked too.
	listen = func(ctx context.Context, network, address string) (net.Listener, error) {
		if network == "tcp6" {
			return nil, &net.OpError{Op: "listen", Net: network, Err: syscall.EACCES}
		}
		return real(ctx, network, address)
	}
	_, err = Listen(t.Context(), 0)
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.SessionLocalPortRefused.Code() || p.Params["picked"] != "true" || p.Params["port"] == "0" {
		t.Errorf("a picked port refused on [::1] gave %v", err)
	}
}

// anyAddress is a listener that says it listens on every address, which
// it doesn't: Serve refuses it before it accepts anything.
type anyAddress struct{ net.Listener }

func (anyAddress) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4zero, Port: 8080} }

// TestFetchMetadata checks that a request a browser says came from another
// site, as another port's page is, is served only as a navigation of the
// browser's to a page: a frame, or a resource of the other page's, which
// would carry the session's cookie into it, is refused, with the cookie
// too, before anything connects to the service, and a code it carries
// isn't used. A request from the session's own pages, or that the user
// made, or without the metadata, is served as ever, with the cookie.
func TestFetchMetadata(t *testing.T) {
	r := newRig(t, nil)
	cookie := r.login()
	meta := func(site, mode, dest string) http.Header {
		return http.Header{"Cookie": {cookie}, "Sec-Fetch-Site": {site}, "Sec-Fetch-Mode": {mode}, "Sec-Fetch-Dest": {dest}}
	}
	for name, tc := range map[string]struct {
		header http.Header
		status int
	}{
		"an image of another site's":        {meta("cross-site", "no-cors", "image"), http.StatusForbidden},
		"a script of another port's":        {meta("same-site", "no-cors", "script"), http.StatusForbidden},
		"a frame on another port's page":    {meta("same-site", "navigate", "iframe"), http.StatusForbidden},
		"a fetch of another site's":         {meta("cross-site", "cors", "empty"), http.StatusForbidden},
		"a navigation from another site":    {meta("cross-site", "navigate", "document"), http.StatusOK},
		"a navigation from another port":    {meta("same-site", "navigate", "document"), http.StatusOK},
		"a resource of the session's pages": {meta("same-origin", "no-cors", "image"), http.StatusOK},
		"what the user typed":               {meta("none", "navigate", "document"), http.StatusOK},
		"no metadata":                       {http.Header{"Cookie": {cookie}}, http.StatusOK},
	} {
		dials := r.dials.Load()
		resp, body := r.do("GET", prefix+"/x", tc.header)
		switch {
		case tc.status == http.StatusForbidden:
			wantPage(t, resp, body, http.StatusForbidden, problems.SessionCrossOrigin)
			if r.dials.Load() != dials {
				t.Errorf("%s connected to the service", name)
			}
		case resp.StatusCode != tc.status:
			t.Errorf("%s: %s", name, resp.Status)
		}
	}
	if resp, body := r.do("GET", prefix+"/x", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("no metadata, and no cookie: %s %q", resp.Status, body)
	}
	code := r.codes.add(r.keys, "s1")
	header := meta("cross-site", "no-cors", "image")
	header.Del("Cookie")
	resp, body := r.do("GET", prefix+"/?"+CodeParam+"="+code, header)
	wantPage(t, resp, body, http.StatusForbidden, problems.SessionCrossOrigin)
	if resp, _ := r.do("GET", prefix+"/x?"+CodeParam+"="+code, nil); resp.StatusCode != http.StatusOK || len(resp.Cookies()) != 1 {
		t.Errorf("an image of another site's used the code: a navigation then gave %s", resp.Status)
	}
}

// TestRedeemByGetAlone checks that a code is used by a GET alone: a HEAD,
// as a link checker sends, is answered as a request without the cookie
// is, and leaves the code to the GET after it.
func TestRedeemByGetAlone(t *testing.T) {
	r := newRig(t, nil)
	code := r.codes.add(r.keys, "s1")
	resp, _ := r.do("HEAD", prefix+"/x?"+CodeParam+"="+code, nil)
	if resp.StatusCode != http.StatusUnauthorized || len(resp.Cookies()) != 0 {
		t.Errorf("a HEAD with a code gave %s, cookies %v", resp.Status, resp.Cookies())
	}
	if resp, _ := r.do("GET", prefix+"/x?"+CodeParam+"="+code, nil); resp.StatusCode != http.StatusOK || len(resp.Cookies()) != 1 {
		t.Errorf("the GET after the HEAD gave %s, cookies %v", resp.Status, resp.Cookies())
	}
}

// TestRedeemPageEscapes checks that the page that sends a browser on from
// a code holds the URL it sends it to escaped, as HTML, whatever its query
// holds.
func TestRedeemPageEscapes(t *testing.T) {
	r := newRig(t, nil)
	code := r.codes.add(r.keys, "s1")
	resp, body := r.do("GET", prefix+`/x?a="><script>alert(1)</script>&`+CodeParam+"="+code, nil)
	if resp.StatusCode != http.StatusOK || strings.Contains(body, "<script>") || !strings.Contains(body, "&#34;&gt;&lt;script&gt;") {
		t.Errorf("the page is %q", body)
	}
}

// TestRepointInFlight checks that a request going on as the session is
// pointed elsewhere ends at the service it started at, and that every
// request after it goes to the new one, over connections of its own: none
// that the request going on used, which the old service's transport keeps.
func TestRepointInFlight(t *testing.T) {
	leaktest.Check(t)
	release := make(chan struct{})
	var oldHits atomic.Int32
	old := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		oldHits.Add(1)
		if r.URL.Path == "/slow" {
			<-release
		}
		_, _ = io.WriteString(w, "old")
	})
	r := newRig(t, old)
	cookie := r.login()
	client := &http.Client{Transport: &http.Transport{}}
	defer client.CloseIdleConnections()
	get := func(path string) string {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+r.addr+prefix+path, nil)
		req.Host = r.host
		req.Header.Set("Cookie", cookie)
		resp, err := client.Do(req)
		if err != nil {
			t.Error(err)
			return ""
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}
	// A connection to the old service, idle, and one serving a request.
	if got := get("/warm"); got != "old" {
		t.Fatalf("before Repoint, the proxy answered %q", got)
	}
	slow := make(chan string, 1)
	go func() { slow <- get("/slow") }()
	deadline := time.Now().Add(5 * time.Second)
	for oldHits.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the slow request didn't reach the old service")
		}
		time.Sleep(5 * time.Millisecond) //nolint:forbidigo // the request reaching the service, which nothing signals
	}

	newer := service(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "new") }))
	r.p.Repoint(func(ctx context.Context) (net.Conn, error) { return (&net.Dialer{}).DialContext(ctx, "tcp4", newer) })
	// A request between Repoint and the slow one's end, which a transport
	// that the old service and the new shared would take for one to keep
	// its connections idle for again.
	if got := get("/between"); got != "new" {
		t.Errorf("a request after Repoint, as one to the old service went on, answered %q", got)
	}
	close(release)
	if got := <-slow; got != "old" {
		t.Errorf("the request going on as the session was pointed elsewhere answered %q", got)
	}
	for i := range 10 {
		if got := get(fmt.Sprintf("/after/%d", i)); got != "new" {
			t.Errorf("request %d after Repoint answered %q", i, got)
		}
	}
}

// TestOriginNull checks that a change whose Origin is null, as a form of
// the session's own pages sends where the page's referrer policy is
// no-referrer, is served where the browser says it came from the session's
// own origin, and refused otherwise, as from a sandboxed frame.
func TestOriginNull(t *testing.T) {
	r := newRig(t, nil)
	cookie := r.login()
	for site, status := range map[string]int{"same-origin": http.StatusOK, "cross-site": http.StatusForbidden, "same-site": http.StatusForbidden, "": http.StatusForbidden} {
		header := http.Header{"Cookie": {cookie}, "Origin": {"null"}}
		if site != "" {
			header.Set("Sec-Fetch-Site", site)
		}
		if resp, body := r.do("POST", prefix+"/form", header); resp.StatusCode != status {
			t.Errorf("Origin null, Sec-Fetch-Site %q: %s %q", site, resp.Status, body)
		}
	}
}

// TestIdentityHeaders checks that none of the headers with which format
// 1's platform tells a service who asks reaches the service as a browser
// sent it, as a service might trust it; and that Meridian sets none of
// them, which it signs none of.
func TestIdentityHeaders(t *testing.T) {
	r := newRig(t, nil)
	header := http.Header{"Cookie": {r.login()}, "X-Kept": {"kept"}}
	names := []string{"X-PW-User", "X-PW-User-Token", "X-PW-Subject", "X-PW-ImpersonatedBy", "X-PW-Impersonated-By", "x-pw-anything"}
	for _, h := range names {
		header.Set(h, "forged")
	}
	resp, body := r.do("GET", prefix+"/x", header)
	var s seen
	if err := json.Unmarshal([]byte(body), &s); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatal(resp.Status, err)
	}
	for _, h := range names {
		if v := s.Header.Values(h); len(v) != 0 {
			t.Errorf("the service got %s: %q", h, v)
		}
	}
	if s.Header.Get("X-Kept") != "kept" {
		t.Errorf("another header didn't reach the service: %v", s.Header)
	}
}

// TestUnauthorizedScheme checks that a request without the session's
// cookie is answered with WWW-Authenticate, whose scheme is Meridian's own,
// which no browser answers itself.
func TestUnauthorizedScheme(t *testing.T) {
	r := newRig(t, nil)
	resp, _ := r.do("GET", prefix+"/x", nil)
	if got := resp.Header.Get("WWW-Authenticate"); resp.StatusCode != http.StatusUnauthorized || got != `Meridian-Link realm="`+prefix+`/"` {
		t.Errorf("%s, WWW-Authenticate %q", resp.Status, got)
	}
}

// TestReferrerPolicy checks that the browser gets a service's answer that
// sets no Referrer-Policy with same-origin, so that the session's host
// name, whose label no other site is to learn, goes in no Referer to
// another; and one the service sets as it set it.
func TestReferrerPolicy(t *testing.T) {
	r := newRig(t, nil)
	header := http.Header{"Cookie": {r.login()}}
	if resp, _ := r.do("GET", prefix+"/x", header); resp.Header.Get("Referrer-Policy") != "same-origin" {
		t.Errorf("an answer without a policy reached the browser with %q", resp.Header.Get("Referrer-Policy"))
	}
	if resp, _ := r.do("GET", prefix+"/x?rp=no-referrer", header); resp.Header.Get("Referrer-Policy") != "no-referrer" {
		t.Errorf("the service's policy reached the browser as %q", resp.Header.Get("Referrer-Policy"))
	}
}

// lockedLog is a log a test reads, which a proxy's goroutines write.
type lockedLog struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// TestFailureLog checks what the proxy logs of a connection to the
// service that failed: once a minute at most, however often a browser
// retries; and, of a private session, or one made private, only its
// problem's code, never what the error says, as the service's address,
// which can be what the session read.
func TestFailureLog(t *testing.T) {
	refused := problems.SSHTCPConnectFailed.Problem(problems.Params{"host": "farm", "address": "secret-node:8888", "reason": "connection refused"})
	for _, tc := range []struct {
		name          string
		private, mark bool
	}{{"public", false, false}, {"private", true, false}, {"made private", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			var out lockedLog
			r := newRig(t, nil, func(c *Config) {
				c.Private = tc.private
				c.Log = slog.New(slog.NewTextHandler(&out, nil))
				c.Dial = func(context.Context) (net.Conn, error) { return nil, refused }
			})
			if tc.mark {
				r.p.MarkPrivate()
			}
			cookie := r.login()
			for range 3 {
				if resp, _ := r.do("GET", prefix+"/x", http.Header{"Cookie": {cookie}}); resp.StatusCode != http.StatusBadGateway {
					t.Fatalf("a service that refused answered %d", resp.StatusCode)
				}
			}
			logged := out.String()
			if n := strings.Count(logged, "didn't answer"); n != 1 {
				t.Errorf("three failed requests logged %d times:\n%s", n, logged)
			}
			private := tc.private || tc.mark
			if private && (strings.Contains(logged, "secret-node") || !strings.Contains(logged, "problem=ssh_tcp_connect_failed")) ||
				!private && !strings.Contains(logged, "secret-node") {
				t.Errorf("private %t, the log says:\n%s", private, logged)
			}
		})
	}
}

// TestPrivateProxyLog checks that what the reverse proxy logs itself, as a
// body it couldn't copy from the service, and what the server does, a
// private session's proxy logs nothing of, since what an error says can be
// what the session read.
func TestPrivateProxyLog(t *testing.T) {
	for _, private := range []bool{false, true} {
		var out lockedLog
		r := newRig(t, nil, func(c *Config) {
			c.Private = private
			c.Log = slog.New(slog.NewTextHandler(&out, nil))
			c.Dial = func(context.Context) (net.Conn, error) {
				client, server := net.Pipe()
				go func() {
					defer server.Close()
					_, _ = http.ReadRequest(bufio.NewReader(server))
					_, _ = io.WriteString(server, "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort")
				}()
				return client, nil
			}
		})
		cookie := r.login()
		// The proxy aborts the answer it can't finish, which the client
		// sees as an error.
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+r.addr+prefix+"/x", http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = r.host
		req.Header.Set("Cookie", cookie)
		if resp, err := r.client.Do(req); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		_ = r.p.Close()
		// The server's own log too.
		r.p.server.ErrorLog.Print("a server's own message")
		logged := out.String()
		if private == strings.Contains(logged, "body copy") || private == strings.Contains(logged, "a server's own message") {
			t.Errorf("private %t, the reverse proxy and the server logged:\n%s", private, logged)
		}
	}
}
