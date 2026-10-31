// Package sessionproxy serves a run's session on this machine: one port of
// 127.0.0.1 for each session, at the path format 1's platform serves it
// at, /me/session/<user>/<name>/, which it strips, passing each request on
// to the session's service, wherever the workflow pointed it, as format
// 1's platform does a tunnel's (docs/design.md, Serving a session).
//
// Anyone on this machine can connect to its loopback address, and, to a
// browser, every port of it is one site, so a session is served only:
//
//   - to a request addressed to 127.0.0.1, localhost or [::1], which a page
//     whose host name was made to resolve to 127.0.0.1 isn't;
//   - to a browser that holds the session's cookie, which a one-time code
//     gives it, that a link Meridian printed holds, and only once, within
//     ten minutes; the cookie holds the session's secret, which works until
//     the session's salt is replaced or the run ends, and goes only to the
//     session's path;
//   - for a WebSocket, or a request other than GET, HEAD or OPTIONS, from
//     the session's own pages, as its Origin and Sec-Fetch-Site say.
//
// Nothing reaches the service, nor is any connection to it opened, until a
// request has passed all three. Meridian's own cookies and code never
// reach it, and the cookies it sets the browser are held to the session's
// path, and can't be Meridian's.
package sessionproxy

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httputil"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/internal/login"
	"github.com/parallelworks/meridian/problems"
)

// A DialFunc connects to a session's service, wherever the workflow
// pointed it, and returns the connection, whose life ctx bounds, not only
// its dial, as the ssh backend's DialTCP does.
type DialFunc func(ctx context.Context) (net.Conn, error)

// Codes are the sessions' one-time codes, each kept as its keyed hash
// (Keyring.CodeMAC), until it is used, or it expires.
type Codes interface {
	// Redeem uses the code of the session session whose keyed hash is
	// mac, and reports whether it was one: kept, unused and unexpired.
	// Each is used once. What can't answer, as a store that can't be
	// read, answers false, which the browser is told to ask for another
	// link for.
	Redeem(ctx context.Context, session string, mac []byte) bool
}

// CodeLifetime is how long a one-time code works for, from when it was
// made.
const CodeLifetime = 10 * time.Minute

// DefaultMaxConns is how many connections to its service a session has
// open, or opening, at once, by default: a quarter of what the ssh
// backend opens over its connection to a host (sshconn.MaxTCPConns), so
// that a browser that retries a service that doesn't answer can't take
// what the host's other sessions need.
const DefaultMaxConns = 16

// What the proxy waits for, set apart for its tests: dialWait is how long a
// request waits for a connection to the service, a place among MaxConns
// and its dial both, after which it is answered that the service doesn't
// answer, while the dial goes on, holding its place, until it ends;
// readHeaderTimeout how long a browser has to send a request's headers;
// and idleTimeout how long a connection waits for the next request.
var (
	dialWait          = 10 * time.Second
	readHeaderTimeout = 10 * time.Second
	idleTimeout       = 2 * time.Minute
)

// Names of what the proxy keeps from the service: CodeParam, the query
// parameter a link's code is in, and CookiePrefix, which its cookies'
// names start with.
const (
	CodeParam    = "meridian-code"
	CookiePrefix = "meridian-session-"
)

// upstreamHost is the host the service is asked for, as format 1's
// platform asks a tunnel's service, whatever its address.
const upstreamHost = "localhost"

// Config is what a Proxy serves.
type Config struct {
	// User and Name are the login name of the user who runs Meridian and
	// the session's name, which the path the session is served at holds,
	// as they are: User as login.ForPath allows it, which a step sees as
	// PW_USER, and Name, of POSIX's portable filename characters.
	User, Name string
	// Run and Key are the run's ID and the session's key in its
	// sessions:, which the page that answers a browser without the
	// session's cookie names, for meridian session url.
	Run, Key string
	// Session is the session's ID, whose one-time codes Codes uses, which
	// Keyring hashes; and Secret is the session's secret
	// (Keyring.Secret), which its cookie holds.
	Session string
	Secret  []byte
	Keyring *Keyring
	Codes   Codes
	// Dial connects to the session's service.
	Dial DialFunc
	// MaxConns is how many connections to the service the proxy has
	// open, or opening, at once: DefaultMaxConns for 0.
	MaxConns int
	// Log hears of what the proxy can't do that no answer says, as a
	// connection to the service that failed. Nil means none.
	Log *slog.Logger
}

// A Proxy serves one session (Serve).
type Proxy struct {
	cfg    Config
	prefix string
	cookie string
	places chan struct{}
	// life is the proxy's, which bounds its connections to the service,
	// and end ends it.
	life context.Context
	end  context.CancelFunc

	mu     sync.Mutex
	secret []byte
	dial   DialFunc
	// conns are the connections to the service open, which Close closes.
	conns map[*placedConn]bool

	transport *http.Transport
	reverse   *httputil.ReverseProxy
	server    *http.Server
}

// New returns a Proxy of cfg, which lives until Close, or until ctx is
// done, which closes its connections to the service, as Close does, but
// for those it serves the browser. A User that a URL's path can't hold as
// it is is problems.SessionUserUnsupported (login.ForPath).
func New(ctx context.Context, cfg Config) (*Proxy, error) {
	if _, err := login.ForPath(cfg.User); err != nil {
		return nil, err
	}
	switch {
	case !login.PathSafe(cfg.Name):
		return nil, fmt.Errorf("sessionproxy: the session's name %q isn't one a URL's path holds as it is", cfg.Name)
	case len(cfg.Secret) == 0 || cfg.Keyring == nil || cfg.Codes == nil || cfg.Dial == nil:
		return nil, errors.New("sessionproxy: a session needs its secret, a keyring, its codes and a dial")
	}
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = DefaultMaxConns
	}
	p := &Proxy{
		cfg: cfg, prefix: "/me/session/" + cfg.User + "/" + cfg.Name,
		places: make(chan struct{}, cfg.MaxConns),
		secret: append([]byte(nil), cfg.Secret...), dial: cfg.Dial, conns: map[*placedConn]bool{},
	}
	sum := sha256.Sum256([]byte(p.prefix))
	p.cookie = CookiePrefix + hex.EncodeToString(sum[:8])
	p.life, p.end = context.WithCancel(ctx)
	errorLog := log.New(io.Discard, "", 0)
	if cfg.Log != nil {
		errorLog = slog.NewLogLogger(cfg.Log.Handler(), slog.LevelWarn)
	}
	p.transport = &http.Transport{
		DialContext:        func(ctx context.Context, _, _ string) (net.Conn, error) { return p.dialService(ctx) },
		DisableCompression: true,
		IdleConnTimeout:    idleTimeout,
	}
	p.reverse = &httputil.ReverseProxy{
		Rewrite: p.rewrite, Transport: p.transport, ModifyResponse: p.modify, ErrorLog: errorLog,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if cfg.Log != nil && !errors.Is(err, context.Canceled) {
				cfg.Log.Warn("sessionproxy: the session's service didn't answer", "session", cfg.Name, "err", err)
			}
			page(w, r, http.StatusBadGateway, problems.SessionUnreachable, problems.Params{"name": cfg.Name})
		},
	}
	p.server = &http.Server{Handler: p, ReadHeaderTimeout: readHeaderTimeout, IdleTimeout: idleTimeout, ErrorLog: errorLog}
	return p, nil
}

// Path returns the path the proxy serves the session at, with a slash
// after it: /me/session/<user>/<name>/.
func (p *Proxy) Path() string { return p.prefix + "/" }

// Serve serves the session on l, which must listen on 127.0.0.1 alone
// (Listen), until Close.
func (p *Proxy) Serve(l net.Listener) error {
	if a, ok := l.Addr().(*net.TCPAddr); !ok || !a.IP.IsLoopback() {
		return fmt.Errorf("sessionproxy: %v isn't this machine's loopback address, which alone serves a session", l.Addr())
	}
	err := p.server.Serve(l)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Close stops serving the session, and closes every connection to its
// service, those of WebSockets among them, which closes theirs to the
// browser.
func (p *Proxy) Close() error {
	p.end()
	err := p.server.Close()
	p.transport.CloseIdleConnections()
	p.mu.Lock()
	conns := slices.Collect(maps.Keys(p.conns))
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	return err
}

// Repoint has the proxy connect to the service with dial from now on, as
// a session that a step points again: idle connections to where it
// pointed close, and those in use, as a WebSocket's, run until they end.
func (p *Proxy) Repoint(dial DialFunc) {
	p.mu.Lock()
	p.dial = dial
	p.mu.Unlock()
	p.transport.CloseIdleConnections()
}

// SetSecret has the session's cookie hold secret from now on, as a
// session whose salt was replaced: a browser that holds the old one has
// to open a new link.
func (p *Proxy) SetSecret(secret []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.secret = append([]byte(nil), secret...)
}

func (p *Proxy) current() ([]byte, DialFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.secret, p.dial
}

// ServeHTTP answers a request: one addressed to another host than
// 127.0.0.1, localhost or [::1] it refuses; one for / or the session's
// path without its slash it redirects to the session's path, and any other
// outside it it answers that nothing is there; a link's code it redeems;
// and the rest it passes on to the service, once it has checked where a
// WebSocket, or a change, comes from, and the session's cookie.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !loopbackHost(r.Host) {
		page(w, r, http.StatusMisdirectedRequest, problems.SessionHostRefused, problems.Params{"host": r.Host})
		return
	}
	switch path := r.URL.Path; {
	case path == "/" || path == p.prefix:
		target := p.Path()
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
		http.Redirect(w, r, target, http.StatusFound) //nolint:gosec // to the session's own path, which starts with /me/session/
		return
	case !strings.HasPrefix(path, p.prefix+"/"):
		page(w, r, http.StatusNotFound, problems.SessionNotHere, problems.Params{"path": p.Path()})
		return
	}
	if code, ok := codeOf(r.URL.RawQuery); ok && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
		p.redeem(w, r, code)
		return
	}
	if changes(r) && !sameOrigin(r) {
		page(w, r, http.StatusForbidden, problems.SessionCrossOrigin, nil)
		return
	}
	if !p.authorized(r) {
		page(w, r, http.StatusUnauthorized, problems.SessionUnauthorized, problems.Params{"run": p.cfg.Run, "key": p.cfg.Key})
		return
	}
	p.reverse.ServeHTTP(w, r)
}

// redeem uses the one-time code code: a code that works gives the browser
// the session's cookie, and sends it to the same URL without the code,
// which its history then keeps, and which no cache keeps the answer of.
// A browser that holds the cookie already, as one that opens a link it
// used before, is sent there too, with the cookie it has; any other is
// refused, as a browser without the cookie is.
func (p *Proxy) redeem(w http.ResponseWriter, r *http.Request, code string) {
	switch {
	case p.cfg.Codes.Redeem(r.Context(), p.cfg.Session, p.cfg.Keyring.CodeMAC(code)):
		secret, _ := p.current()
		// Not Secure: the session is served over HTTP, on this machine's
		// loopback address, which not every browser sends a Secure cookie
		// to.
		http.SetCookie(w, &http.Cookie{ //nolint:gosec // HttpOnly and SameSite are set; Secure can't be, above
			Name: p.cookie, Value: base64.RawURLEncoding.EncodeToString(secret), Path: p.Path(),
			HttpOnly: true, SameSite: http.SameSiteStrictMode,
		})
	case !p.authorized(r):
		page(w, r, http.StatusUnauthorized, problems.SessionUnauthorized, problems.Params{"run": p.cfg.Run, "key": p.cfg.Key})
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	target := r.URL.EscapedPath()
	if q := withoutCode(r.URL.RawQuery); q != "" {
		target += "?" + q
	}
	http.Redirect(w, r, target, http.StatusSeeOther) //nolint:gosec // to the path asked for, under the session's own, which ServeHTTP checked
}

// authorized reports whether r holds the session's cookie, with the
// session's secret: another session's, or anything else, doesn't count.
func (p *Proxy) authorized(r *http.Request) bool {
	secret, _ := p.current()
	for _, c := range r.CookiesNamed(p.cookie) {
		if v, err := base64.RawURLEncoding.DecodeString(c.Value); err == nil && hmac.Equal(v, secret) {
			return true
		}
	}
	return false
}

// dialService connects to the session's service, once the proxy has fewer
// than MaxConns connections to it open or opening, waiting dialWait at
// most for both, or until ctx is done. A dial it stops waiting for goes on,
// with the place it took, until it ends, and closes what it made: so a
// browser that retries a service that doesn't answer has no more dials
// going on than MaxConns, which the host's other sessions don't wait
// behind. A connection lives as long as the proxy, at most, whatever
// request it was dialed for, as the transport keeps it for the next.
func (p *Proxy) dialService(ctx context.Context) (net.Conn, error) {
	wait := time.NewTimer(dialWait)
	defer wait.Stop()
	select {
	case p.places <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-wait.C:
		return nil, errBusy
	case <-p.life.Done():
		return nil, errClosed
	}
	release := sync.OnceFunc(func() { <-p.places })
	_, dial := p.current()
	type dialed struct {
		conn net.Conn
		err  error
	}
	var (
		mu        sync.Mutex
		abandoned bool
	)
	ch := make(chan dialed, 1)
	go func() {
		conn, err := dial(p.life)
		mu.Lock()
		defer mu.Unlock()
		if !abandoned {
			ch <- dialed{conn, err}
			return
		}
		if conn != nil {
			_ = conn.Close()
		}
		release()
	}()
	answer := func(d dialed) (net.Conn, error) {
		if d.err != nil {
			release()
			return nil, d.err
		}
		return p.placed(d.conn, release), nil
	}
	select {
	case d := <-ch:
		return answer(d)
	case <-ctx.Done():
	case <-wait.C:
	}
	mu.Lock()
	defer mu.Unlock()
	select {
	case d := <-ch:
		return answer(d)
	default:
	}
	abandoned = true
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, errBusy
}

var (
	errBusy   = fmt.Errorf("sessionproxy: no connection to the service within %v", dialWait)
	errClosed = errors.New("sessionproxy: closed")
)

// A placedConn is a connection to the service, which gives its place among
// MaxConns back once it is closed, and which the proxy closes as it
// closes.
type placedConn struct {
	net.Conn
	once  sync.Once
	close func() error
	err   error
}

func (c *placedConn) Close() error {
	c.once.Do(func() { c.err = c.close() })
	return c.err
}

// placed returns conn, a connection to the service, which the proxy keeps
// until it is closed, and which then calls release; or, where the proxy
// has closed, closes it, and returns it closed.
func (p *Proxy) placed(conn net.Conn, release func()) *placedConn {
	c := &placedConn{Conn: conn}
	c.close = func() error {
		p.mu.Lock()
		delete(p.conns, c)
		p.mu.Unlock()
		err := conn.Close()
		release()
		return err
	}
	p.mu.Lock()
	closed := p.life.Err() != nil
	if !closed {
		p.conns[c] = true
	}
	p.mu.Unlock()
	if closed {
		_ = c.Close()
	}
	return c
}

// rewrite makes what goes to the service of what the browser sent: its
// path without the session's, as format 1's platform passes a tunnel's on,
// which X-Forwarded-Prefix says; its query without a code; its cookies
// without the proxy's; the service's host; and X-Forwarded-For, -Host and
// -Proto as the proxy saw them, over any the browser sent.
func (p *Proxy) rewrite(pr *httputil.ProxyRequest) {
	in, out := pr.In, pr.Out
	out.URL.Scheme, out.URL.Host, out.Host = "http", upstreamHost, upstreamHost
	out.URL.Path, out.URL.RawPath = strings.TrimPrefix(in.URL.Path, p.prefix), ""
	if raw := in.URL.RawPath; strings.HasPrefix(raw, p.prefix+"/") {
		out.URL.RawPath = strings.TrimPrefix(raw, p.prefix)
	}
	out.URL.RawQuery = withoutCode(in.URL.RawQuery)
	out.Header.Del("Cookie")
	for _, v := range in.Header.Values("Cookie") {
		if kept := withoutOwnCookies(v); kept != "" {
			out.Header.Add("Cookie", kept)
		}
	}
	pr.SetXForwarded()
	out.Header.Set("X-Forwarded-Prefix", p.prefix)
}

// modify makes what goes to the browser of what the service answered: a
// Location that leaves the session's path put back under it; each cookie
// held to the session's path, without a domain, and none of the proxy's
// own names; and no Clear-Site-Data, which, as every port of 127.0.0.1
// shares its cookies, would sign the browser out of every session.
func (p *Proxy) modify(resp *http.Response) error {
	h := resp.Header
	if loc := h.Get("Location"); loc != "" {
		h.Set("Location", p.location(loc))
	}
	if cookies := h.Values("Set-Cookie"); len(cookies) > 0 {
		h.Del("Set-Cookie")
		for _, c := range cookies {
			if kept, ok := p.setCookie(c); ok {
				h.Add("Set-Cookie", kept)
			}
		}
	}
	h.Del("Clear-Site-Data")
	return nil
}

// loopbackHost reports whether a request's Host names this machine's
// loopback address, as 127.0.0.1, localhost or [::1], with any port, as
// ssh -L to another port gives it.
func loopbackHost(hostport string) bool {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = strings.TrimSuffix(strings.TrimPrefix(hostport, "["), "]")
	}
	return host == "127.0.0.1" || host == "::1" || strings.EqualFold(host, "localhost")
}

// changes reports whether r is one whose origin the proxy checks: a
// WebSocket, or any other upgrade, which a browser's same-origin policy
// doesn't hold to a page's origin, or a request other than GET, HEAD or
// OPTIONS, which can change what the service holds.
func changes(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
	default:
		return true
	}
	for _, v := range r.Header.Values("Connection") {
		for token := range strings.SplitSeq(v, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
				return true
			}
		}
	}
	return r.Header.Get("Upgrade") != ""
}

// sameOrigin reports whether r comes from the session's own pages: its
// Origin, if it has one, is the origin it was sent to, http:// and its
// Host, which loopbackHost allowed, with the port the browser sent it to;
// and its Sec-Fetch-Site, if it has one, is same-origin, or none, as a
// request the user made is. Every port of 127.0.0.1 is one site to a
// browser, so SameSite doesn't tell them apart, and the origin must.
func sameOrigin(r *http.Request) bool {
	switch origins := r.Header.Values("Origin"); {
	case len(origins) > 1:
		return false
	case len(origins) == 1 && !strings.EqualFold(origins[0], "http://"+r.Host):
		return false
	}
	switch site := r.Header.Values("Sec-Fetch-Site"); {
	case len(site) > 1:
		return false
	case len(site) == 1 && site[0] != "same-origin" && site[0] != "none":
		return false
	}
	return true
}

// localizer gives a page in the language the browser asks for.
var localizer = &problem.Localizer{Catalogs: []*problem.Catalog{problems.Catalog}}

// page answers with a problem of kind k, with params, as text in the
// language the browser asks for, which no cache keeps, and no browser
// reads as anything but text.
func page(w http.ResponseWriter, r *http.Request, status int, k *problems.Kind, params problems.Params) {
	lang := localizer.Language(r.Header)
	d := k.At("", problems.Position{}, "", params)
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Cache-Control", "no-store")
	h.Set("Content-Language", lang)
	w.WriteHeader(status)
	_, _ = io.WriteString(w, d.Localize(lang)+"\n\n("+string(k.Code())+")\n") //nolint:gosec // text, which nosniff keeps any browser from reading as HTML
}
