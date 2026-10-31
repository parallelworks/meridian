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
	"maps"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

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
	code, mac := k.NewCode()
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

// A rig is a proxy that serves a session on 127.0.0.1, whose service the
// rig's dial reaches, counting its dials.
type rig struct {
	t       *testing.T
	p       *Proxy
	keys    *Keyring
	codes   *memCodes
	addr    string
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
		User: testUser, Name: testName, Run: "7", Key: "desktop", Session: "s1",
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
	r.addr = l.Addr().String()
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
	if resp.StatusCode != http.StatusSeeOther || len(cookies) != 1 {
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
// code gives the browser the session's cookie, which holds its secret, is
// held to the session's path, is HttpOnly and SameSite=Strict, and is all
// it needs after; it sends it on to the same URL without the code, which
// no cache keeps, with no referrer. A code works once, and only until it
// expires; a request without the cookie, with another session's, or with
// one whose secret was replaced, as reset does, is refused, and told how
// to get a link.
func TestCodeAndCookie(t *testing.T) {
	now := time.Now()
	r := newRig(t, nil)
	r.codes.now = func() time.Time { return now }
	code := r.codes.add(r.keys, "s1")
	resp, _ := r.do("GET", prefix+"/x?a=1&"+CodeParam+"="+code+"&b=2", nil)
	cookies := resp.Cookies()
	h := resp.Header
	switch {
	case resp.StatusCode != http.StatusSeeOther || h.Get("Location") != prefix+"/x?a=1&b=2":
		t.Errorf("redeeming a code gave %s to %q", resp.Status, h.Get("Location"))
	case h.Get("Cache-Control") != "no-store" || h.Get("Referrer-Policy") != "no-referrer":
		t.Errorf("the redirect's headers are %v", h)
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
	resp, _ = r.do("GET", prefix+"/x?"+CodeParam+"="+code, http.Header{"Cookie": {c.Name + "=" + c.Value}})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != prefix+"/x" || len(resp.Cookies()) != 0 {
		t.Errorf("a used code, with the cookie, gave %s to %q, cookies %v", resp.Status, resp.Header.Get("Location"), resp.Cookies())
	}
	resp, body := r.do("GET", prefix+"/?"+CodeParam+"="+code, nil)
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
	for _, path := range []string{"/x", "/me/session/" + testUser + "/other/", prefix + "x/", "/me/session/bob/" + testName + "/"} {
		resp, body := r.do("GET", path, http.Header{"Cookie": {cookie}})
		wantPage(t, resp, body, http.StatusNotFound, problems.SessionNotHere)
		if !strings.Contains(body, prefix+"/") {
			t.Errorf("%s: %q doesn't say where the session is", path, body)
		}
	}
	resp, _ := r.do("GET", prefix+"/cookies?c=app%3D1%3B+Path%3D%2F", http.Header{"Cookie": {cookie}})
	if got := resp.Header.Values("Set-Cookie"); len(got) != 1 || got[0] != "app=1; Path="+prefix+"/" {
		t.Errorf("a cookie at / is set as %q", got)
	}
}

// TestHost checks that only a request addressed to 127.0.0.1, localhost
// or [::1], at any port, as ssh -L to another one gives it, is served: one
// whose host name a page made resolve to 127.0.0.1 (DNS rebinding) is
// refused, with the session's cookie too, before anything connects to
// the service.
func TestHost(t *testing.T) {
	r := newRig(t, nil)
	cookie := r.login()
	for _, host := range []string{"127.0.0.1:9999", "localhost:1234", "LOCALHOST", "[::1]:5", "[::1]", "127.0.0.1"} {
		if resp, _ := r.do("GET", prefix+"/x", http.Header{"Host": {host}, "Cookie": {cookie}}); resp.StatusCode != http.StatusOK {
			t.Errorf("%s: %s", host, resp.Status)
		}
	}
	dials := r.dials.Load()
	for _, host := range []string{"evil.example:" + strings.Split(r.addr, ":")[1], "evil.example", "127.0.0.2:80", "localhost.evil.example", "0.0.0.0"} {
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
	own := "http://" + r.addr
	tunnel := "127.0.0.1:9999"
	for name, tc := range map[string]struct {
		method string
		header http.Header
		status int
	}{
		"own origin":                 {"POST", http.Header{"Origin": {own}}, http.StatusOK},
		"no origin":                  {"POST", nil, http.StatusOK},
		"same-origin":                {"DELETE", http.Header{"Sec-Fetch-Site": {"same-origin"}}, http.StatusOK},
		"none":                       {"PUT", http.Header{"Sec-Fetch-Site": {"none"}}, http.StatusOK},
		"through ssh -L":             {"POST", http.Header{"Host": {tunnel}, "Origin": {"http://" + tunnel}}, http.StatusOK},
		"another port":               {"POST", http.Header{"Origin": {"http://127.0.0.1:9999"}}, http.StatusForbidden},
		"another port, as localhost": {"POST", http.Header{"Origin": {"http://localhost:" + strings.Split(r.addr, ":")[1]}}, http.StatusForbidden},
		"https":                      {"POST", http.Header{"Origin": {"https://" + r.addr}}, http.StatusForbidden},
		"null":                       {"POST", http.Header{"Origin": {"null"}}, http.StatusForbidden},
		"two origins":                {"POST", http.Header{"Origin": {own, own}}, http.StatusForbidden},
		"same-site":                  {"POST", http.Header{"Origin": {own}, "Sec-Fetch-Site": {"same-site"}}, http.StatusForbidden},
		"cross-site":                 {"PATCH", http.Header{"Sec-Fetch-Site": {"cross-site"}}, http.StatusForbidden},
		"a GET from another port":    {"GET", http.Header{"Origin": {"http://127.0.0.1:9999"}, "Sec-Fetch-Site": {"same-site"}}, http.StatusOK},
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
	fmt.Fprintf(conn, "GET %s/ws HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nOrigin: %s\r\nCookie: %s\r\n\r\n", prefix, r.addr, origin, cookie)
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
	if _, status := r.upgrade("http://"+r.addr, ""); status != http.StatusUnauthorized {
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
		"Cookie":             {"app=1; " + cookie + "; " + CookiePrefix + "0011223344556677=other; theme=dark"},
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
	case h.Get("X-Forwarded-Prefix") != prefix || h.Get("X-Forwarded-Host") != r.addr || h.Get("X-Forwarded-Proto") != "http" || h.Get("X-Forwarded-For") != "127.0.0.1":
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
		"http://localhost:8080/a?b=1":     prefix + "/a?b=1",
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
	var wg sync.WaitGroup
	start := time.Now()
	for range 5 {
		wg.Go(func() {
			resp, body := r.do("GET", prefix+"/x", http.Header{"Cookie": {cookie}})
			if resp.StatusCode != http.StatusBadGateway || !strings.Contains(body, "session_unreachable") {
				t.Errorf("a request to a service whose dials hang: %s", resp.Status)
			}
		})
	}
	wg.Wait()
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
	defer func(h, i time.Duration) { readHeaderTimeout, idleTimeout = h, i }(readHeaderTimeout, idleTimeout)
	readHeaderTimeout, idleTimeout = 200*time.Millisecond, 300*time.Millisecond
	r := newRig(t, nil)
	cookie := r.login()

	slow, err := (&net.Dialer{}).DialContext(t.Context(), "tcp4", r.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	_, _ = io.WriteString(slow, "GET "+prefix+"/x HTTP/1.1\r\nHost: "+r.addr+"\r\n")
	closedWithin(t, slow, 5*time.Second, "a request whose headers don't end")

	idle, err := (&net.Dialer{}).DialContext(t.Context(), "tcp4", r.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	fmt.Fprintf(idle, "GET %s/x HTTP/1.1\r\nHost: %s\r\nCookie: %s\r\n\r\n", prefix, r.addr, cookie)
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
	r := newRig(t, nil)
	cookie := r.login()
	conn, status := r.upgrade("http://"+r.addr, cookie)
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
		User: testUser, Name: testName, Secret: []byte("s"), Keyring: keys, Codes: &memCodes{},
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
	a := l.Addr().(*net.TCPAddr) //nolint:forcetypeassert // a TCP listener's
	if !a.IP.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Errorf("Listen listens on %v", a)
	}
	_, err = Listen(t.Context(), a.Port)
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.SessionLocalPortTaken.Code() || p.Params["port"] != strconv.Itoa(a.Port) {
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
		User: testUser, Name: testName, Secret: []byte("s"), Keyring: keys, Codes: &memCodes{},
		Dial: func(context.Context) (net.Conn, error) { return nil, errors.New("no") },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Serve(anyAddress{}); err == nil {
		t.Error("Serve took a listener on every address")
	}
}

// anyAddress is a listener that says it listens on every address, which
// it doesn't: Serve refuses it before it accepts anything.
type anyAddress struct{ net.Listener }

func (anyAddress) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4zero, Port: 8080} }
