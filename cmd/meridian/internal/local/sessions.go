package local

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/login"
	"github.com/parallelworks/meridian/internal/sessionproxy"
	"github.com/parallelworks/meridian/problems"
)

// SessionStore is what a host keeps of a store's sessions in the store, as
// the coordinator keeps it (coordinator.Coordinator's AddSessionCode,
// RedeemSessionCode and SessionServed): their one-time codes, and how it
// serves each, which any process shows.
type SessionStore interface {
	AddSessionCode(ctx context.Context, id string, mac []byte, lifetime time.Duration) error
	RedeemSessionCode(ctx context.Context, id string, mac []byte) bool
	SessionServed(ctx context.Context, id string, port int, p *problem.Problem) error
}

// A SessionHost serves a store's sessions on this machine
// (coordinator.SessionHost), as meridian run and resume do: each at a port
// of its own of this machine's loopback addresses, at a host name of its
// own, behind a link's one-time code and a cookie (internal/sessionproxy,
// docs/design.md, Serving a session). A session that points at a cluster
// of the clusters file is reached over SSH through the cluster's host, as
// its jobs' workers are, and one that points at the workspace, this
// machine, at its loopback address.
type SessionHost struct {
	cfg  *Config
	user string
	keys *sessionproxy.Keyring
	// dialCluster connects to host at port from the host of a cluster of
	// the clusters file, clusterHost, over SSH.
	dialCluster func(ctx context.Context, clusterHost, host string, port int) (net.Conn, error)
	log         *slog.Logger

	mu sync.Mutex
	// store is the store's ID, and codes what it keeps of its sessions,
	// once Attach gives them; announce is what says a session is served,
	// once AnnounceTo gives it, and terminal whether it says so on a
	// person's terminal, where a link with a code is shown, rather than the
	// command that makes one.
	store    string
	codes    SessionStore
	announce Announcer
	terminal bool
	// life is the host's, which its sessions' proxies live as long as, at
	// most, and end ends it (Close).
	life context.Context
	end  context.CancelFunc
	// served are the sessions served, by their IDs, and links the links
	// announced, by the IDs of their sessions.
	served map[string]*served
	links  map[string]string
}

// A served session is one a SessionHost serves: its proxy, on its
// listener, where it points, encoded, and its salt, as it serves it, and
// what its proxy's Serve returned, once it has.
type served struct {
	proxy    *sessionproxy.Proxy
	listener *sessionproxy.Listener
	point    string
	salt     string
	done     chan struct{}
}

// close stops serving the session.
func (s *served) close() {
	_ = s.proxy.Close()
	<-s.done
}

// A sessionPoint is where a session points, as the store keeps it
// (coordinator.SessionPoint.Point).
type sessionPoint struct {
	// Type is tunnel or link.
	Type string `json:"type"`
	// Cluster is the cluster of the clusters file whose host reaches the
	// tunnel's service, or "" for the workspace, this machine; RemoteHost
	// and RemotePort are the service's address, as that host reaches it.
	Cluster    string `json:"cluster,omitzero"`
	RemoteHost string `json:"remoteHost,omitzero"`
	RemotePort int    `json:"remotePort,omitzero"`
	// Slug is what a link to the session ends with, as update-session's
	// slug gives it, and URL a link's.
	Slug string `json:"slug,omitzero"`
	URL  string `json:"url,omitzero"`
}

// NewSessionHost returns a SessionHost of the clusters of cfg, which serves
// sessions at the path of user, the login name of the user who runs it
// (login.Name), as PW_USER gives it, under the key key, derived from the
// state directory's key file under sessionproxy.KeyLabel, reaching a
// cluster's services with dialCluster, and logging what no answer says to
// log. It serves nothing until Attach gives it its store.
func NewSessionHost(cfg *Config, user string, key []byte, dialCluster func(ctx context.Context, clusterHost, host string, port int) (net.Conn, error), log *slog.Logger) (*SessionHost, error) {
	keys, err := sessionproxy.NewKeyring(key)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	h := &SessionHost{
		cfg: cfg, user: user, keys: keys, dialCluster: dialCluster, log: log,
		served: map[string]*served{}, links: map[string]string{},
	}
	h.life, h.end = context.WithCancel(context.Background()) //nolint:forbidigo // the host's own life, which Close ends
	return h, nil
}

// Attach gives h its store's ID and what it keeps of its sessions there,
// codes, which its sessions' proxies redeem codes with, and which records
// how it serves each.
func (h *SessionHost) Attach(store string, codes SessionStore) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.store, h.codes = store, codes
}

// An Announcer says that the session key of the run run is served: at
// link, a link to it, with a one-time code, or a link session's own; or,
// for "", that meridian session url makes one (Progress.Session).
type Announcer interface {
	Session(run, key, link string) error
}

// AnnounceTo has h say each session it serves to a: on a person's
// terminal (terminal), with a link that holds a one-time code; elsewhere,
// with the command that makes one, as a log keeps no link that works.
func (h *SessionHost) AnnounceTo(a Announcer, terminal bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.announce, h.terminal = a, terminal
}

// Close stops serving every session.
func (h *SessionHost) Close() {
	h.end()
	h.mu.Lock()
	served := slices.Collect(maps.Values(h.served))
	clear(h.served)
	h.mu.Unlock()
	for _, s := range served {
		s.close()
	}
}

// Check checks a workflow's sessions:, as they are written, and names
// each: by its prompt-for-name's default, where it gives one, which is
// what format 1's form fills in, and which meridian run asks no one for;
// or by its key, made fit for a URL's path, and 40 random bits, so that
// it is no other session's. A session meridian run can't serve is
// refused: one whose detach: is true, which needs a coordinator that
// outlives its run, as meridian serve will be; one whose openAI: is true,
// which a platform registers; and one whose useCustomDomain: is true,
// which a platform serves at a domain of its own
// (session_field_unsupported). So is one that asks for a name that isn't
// a literal one a URL's path holds as it is (session_name_needed), and any
// where the login name of the user who runs meridian isn't one either
// (session_user_unsupported). The sessions of a workflow a step calls,
// called, it checks, and names none of, as the coordinator names them, and
// prompt-for-name asks nothing of them.
func (h *SessionHost) Check(_ context.Context, sessions map[string]any, called bool) (map[string]string, error) {
	return checkSessions(h.user, sessions, called)
}

// SessionNames is the Sessions meridian validates a workflow, and creates
// a run of it, with, before any coordinator: it checks and names a
// workflow's sessions:, as a SessionHost of the login name User does
// (SessionHost.Check); it makes none, as validate --start, which starts no
// run, doesn't; and a step points none.
type SessionNames struct{ User string }

var _ compile.Sessions = SessionNames{}

// Check checks and names the workflow's sessions:, as SessionHost.Check
// does for a run no step called.
func (n SessionNames) Check(_ context.Context, sessions map[string]any) (map[string]string, error) {
	return checkSessions(n.User, sessions, false)
}

// Make makes nothing: validate --start starts no run.
func (SessionNames) Make(context.Context, string, map[string]any, map[string]string, expr.Read) error {
	return nil
}

// Update refuses: only a run's coordinator points a session.
func (SessionNames) Update(context.Context, compile.SessionUpdate) error {
	return errors.New("local: only a run's coordinator points a session")
}

// checkSessions is SessionHost.Check, for the login name user.
func checkSessions(user string, sessions map[string]any, called bool) (map[string]string, error) {
	if _, err := login.ForPath(user); err != nil {
		return nil, err
	}
	names := make(map[string]string, len(sessions))
	for _, key := range slices.Sorted(maps.Keys(sessions)) {
		def, _ := sessions[key].(map[string]any)
		for _, field := range []string{"detach", "openAI", "useCustomDomain"} {
			if def[field] == true {
				return nil, problems.SessionFieldUnsupported.Problem(problems.Params{"key": key, "field": field})
			}
		}
		if called {
			continue
		}
		prompt, asks := def["prompt-for-name"]
		if !asks {
			names[key] = newSessionName(key)
			continue
		}
		m, _ := prompt.(map[string]any)
		name, _ := m["default"].(string)
		if !login.PathSafe(name) || strings.Contains(name, "${{") {
			return nil, problems.SessionNameNeeded.Problem(problems.Params{"key": key})
		}
		names[key] = name
	}
	return names, nil
}

// newSessionName returns a name for the session key: its key, lowercase,
// with what isn't a letter or a digit as -, at most 32 bytes of it, and 40
// random bits, in lowercase base32.
func newSessionName(key string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(key) {
		switch {
		case b.Len() == 32:
		case 'a' <= r && r <= 'z', '0' <= r && r <= '9':
			b.WriteRune(r)
		case b.Len() > 0 && !strings.HasSuffix(b.String(), "-"):
			b.WriteByte('-')
		}
	}
	base := strings.TrimSuffix(b.String(), "-")
	if base == "" {
		base = "session"
	}
	random := make([]byte, 5)
	_, _ = rand.Read(random) // crypto/rand doesn't fail
	return base + "-" + strings.ToLower(base32.StdEncoding.EncodeToString(random))
}

// Point reads where a step that uses parallelworks/update-session points
// the session s, as format 1 reads its with:, and, where serve is set,
// serves the session there. Where it isn't, as for a session another
// process serves, which serves it there as it next looks at the store, it
// serves nothing, and returns the port the step gave, or the one s is
// served at.
//
//   - A link, type: link, is its url, an http or https URL
//     (session_url_invalid), which is announced, and served by none.
//   - A tunnel, any other type, is a service at remoteHost, localhost by
//     default, and remotePort, as its target reaches it: the workspace,
//     this machine, for none, "", localhost or user-workspace, where
//     remoteHost is localhost whatever it says, as format 1 has it; or the
//     cluster of the clusters file whose name, which is its id, it gives
//     (session_target_unknown), reached over SSH through its host. A port,
//     remotePort or localPort, is a whole number from 1 to 65535, or text
//     of one (session_port_invalid). The session is served at localPort,
//     or, without one, at the port it is served at already, or one the
//     system picks; a step that gives another moves it there.
//
// What a platform alone does, targetInfo's Kubernetes resource, openAI and
// apiKey, is session_update_unsupported, and so is a tunnel to the
// workspace whose session's useTLS: is true, which meridian can't yet
// reach over TLS that it checks: a tunnel to a cluster ignores it, as
// format 1 does. status, which format 1 ignores, it ignores too.
func (h *SessionHost) Point(ctx context.Context, s coordinator.Session, u compile.SessionUpdate, serve bool) (coordinator.SessionPoint, error) {
	w := u.With
	for _, field := range []string{"targetInfo", "openAI", "apiKey"} {
		if v, ok := w[field]; ok && v != nil && v != false {
			return coordinator.SessionPoint{}, problems.SessionUpdateUnsupported.Problem(problems.Params{"name": s.Name, "field": field})
		}
	}
	if expr.Text(w["type"]) == "link" {
		link, err := url.Parse(expr.Text(w["url"]))
		if err != nil || link.Host == "" || link.Scheme != "http" && link.Scheme != "https" {
			return coordinator.SessionPoint{}, problems.SessionURLInvalid.Problem(problems.Params{"value": problems.Show(w["url"])})
		}
		data, err := jsonv2.Marshal(sessionPoint{Type: "link", URL: link.String()})
		if err != nil {
			return coordinator.SessionPoint{}, err
		}
		if serve {
			h.announceLink(s, link.String())
		}
		return coordinator.SessionPoint{Point: data}, nil
	}
	remotePort, ok := portOf(w["remotePort"])
	if !ok {
		return coordinator.SessionPoint{}, problems.SessionPortInvalid.Problem(problems.Params{"key": "remotePort", "value": problems.Show(w["remotePort"])})
	}
	p := sessionPoint{Type: "tunnel", RemoteHost: expr.Text(w["remoteHost"]), RemotePort: remotePort, Slug: expr.Text(w["slug"])}
	switch target := expr.Text(w["target"]); target {
	case "", "localhost", "user-workspace":
		p.RemoteHost = "localhost"
		if s.Def["useTLS"] == true {
			return coordinator.SessionPoint{}, problems.SessionUpdateUnsupported.Problem(problems.Params{"name": s.Name, "field": "useTLS"})
		}
	default:
		if _, ok := h.cfg.ClusterHost(target); !ok {
			return coordinator.SessionPoint{}, problems.SessionTargetUnknown.Problem(problems.Params{"target": target, "clusters": problems.QuoteAll(h.cfg.ClusterNames())})
		}
		p.Cluster = target
		if p.RemoteHost == "" {
			p.RemoteHost = "localhost"
		}
	}
	port, given := 0, false
	if v, ok := w["localPort"]; ok && v != nil {
		if port, ok = portOf(v); !ok {
			return coordinator.SessionPoint{}, problems.SessionPortInvalid.Problem(problems.Params{"key": "localPort", "value": problems.Show(v)})
		}
		given = true
	}
	data, err := jsonv2.Marshal(p)
	if err != nil {
		return coordinator.SessionPoint{}, err
	}
	if !serve {
		if !given {
			port = s.Port
		}
		return coordinator.SessionPoint{Point: data, Port: port, PortGiven: given}, nil
	}
	served, err := h.serve(ctx, s, p, string(data), port)
	if err != nil {
		return coordinator.SessionPoint{}, err
	}
	return coordinator.SessionPoint{Point: data, Port: served, PortGiven: given}, nil
}

// portOf returns v as a port, a whole number from 1 to 65535, as JSON
// gives one, or text of one, as format 1 reads remotePort and localPort,
// and whether it is one.
func portOf(v any) (int, bool) {
	var n float64
	switch v := v.(type) {
	case float64:
		n = v
	case int64:
		n = float64(v)
	case int:
		n = float64(v)
	case string:
		i, err := strconv.Atoi(v)
		if err != nil || v != strconv.Itoa(i) {
			return 0, false
		}
		n = float64(i)
	default:
		return 0, false
	}
	if n != math.Trunc(n) || n < 1 || n > 65535 {
		return 0, false
	}
	return int(n), true
}

// serve serves the session s where p, encoded as point, says, at port, or,
// for 0, at the port it is served at already, or one the system picks; and
// returns the port it serves it at. A session served already at the port
// is pointed there, and one a step moves to another port is served there
// alone, once it is.
func (h *SessionHost) serve(ctx context.Context, s coordinator.Session, p sessionPoint, point string, port int) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	sv := h.served[s.ID]
	if sv != nil && (port == 0 || port == sv.listener.Port()) {
		sv.proxy.Repoint(h.dialer(p))
		sv.point = point
		return sv.listener.Port(), nil
	}
	next, err := h.start(ctx, s, p, point, port)
	if err != nil {
		return 0, err
	}
	if sv != nil {
		sv.close()
	}
	h.served[s.ID] = next
	h.announceTunnel(ctx, s, next.listener.Port(), p)
	return next.listener.Port(), nil
}

// start starts serving the session s, which points where p, encoded as
// point, says, at port, or one the system picks for 0.
func (h *SessionHost) start(ctx context.Context, s coordinator.Session, p sessionPoint, point string, port int) (*served, error) {
	if h.codes == nil {
		return nil, errors.New("local: the session host serves no session before Attach")
	}
	secret, err := h.keys.Secret(h.store, s.ID, s.Salt)
	if err != nil {
		return nil, err
	}
	proxy, err := sessionproxy.New(h.life, sessionproxy.Config{
		User: h.user, Name: s.Name, Label: s.Label, Run: s.Run, Key: s.Key,
		Session: s.ID, Secret: secret, Keyring: h.keys, Codes: redeemer{h.codes}, Dial: h.dialer(p), Log: h.log,
	})
	if err != nil {
		return nil, err
	}
	l, err := sessionproxy.Listen(ctx, port)
	if err != nil {
		_ = proxy.Close()
		return nil, err
	}
	sv := &served{proxy: proxy, listener: l, point: point, salt: string(s.Salt), done: make(chan struct{})}
	go func() {
		defer close(sv.done)
		if err := proxy.Serve(l); err != nil {
			h.log.Warn("local: a session's port stopped", "session", s.Name, "err", err)
		}
	}()
	return sv, nil
}

// redeemer is the store's codes as a proxy redeems them.
type redeemer struct{ codes SessionStore }

func (r redeemer) Redeem(ctx context.Context, session string, mac []byte) bool {
	return r.codes.RedeemSessionCode(ctx, session, mac)
}

// dialer returns what connects to where p points: over SSH through a
// cluster's host, or to this machine's loopback address.
//
// localhost, as a step names the machine its service runs on, is
// 127.0.0.1, where a service that listens on one loopback address alone,
// as most do, listens: never a name that a resolver can turn into [::1]
// first, where another user of the machine could listen at the same port
// and take the session's requests, its cookies and passwords with them.
// On this machine, a service that refuses on 127.0.0.1 is tried on [::1],
// as one that listens there alone does; nothing else is.
func (h *SessionHost) dialer(p sessionPoint) sessionproxy.DialFunc {
	port := strconv.Itoa(p.RemotePort)
	if p.Cluster == "" {
		return func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			conn, err := d.DialContext(ctx, "tcp4", net.JoinHostPort("127.0.0.1", port))
			if errors.Is(err, syscall.ECONNREFUSED) {
				return d.DialContext(ctx, "tcp6", net.JoinHostPort("::1", port))
			}
			return conn, err
		}
	}
	clusterHost, _ := h.cfg.ClusterHost(p.Cluster)
	host := p.RemoteHost
	if host == "" || strings.EqualFold(host, "localhost") {
		host = "127.0.0.1"
	}
	return func(ctx context.Context) (net.Conn, error) {
		return h.dialCluster(ctx, clusterHost, host, p.RemotePort)
	}
}

// Serve serves live, the store's sessions that haven't ended that this
// process serves, as the store has them, and no others
// (coordinator.SessionHost): a session that points somewhere, and isn't
// served yet, as one a process that stopped served, at its port, or, where
// that is taken now and the step didn't give it, one the system picks; one
// whose salt changed, as a reset changes it, with its new secret; one
// pointed elsewhere, or at another port, as another process's step may
// have, there; and none that has ended. What it can't serve it says, and
// the port it picks, in the store (SessionStore.SessionServed), and tries
// again as Serve is called again.
func (h *SessionHost) Serve(ctx context.Context, live []coordinator.Session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.codes == nil {
		return
	}
	seen := make(map[string]bool, len(live))
	for _, s := range live {
		seen[s.ID] = true
		if s.Point == nil {
			continue
		}
		var p sessionPoint
		if err := jsonv2.Unmarshal(s.Point, &p); err != nil {
			h.log.Warn("local: a session's point doesn't decode", "session", s.Name)
			continue
		}
		if p.Type == "link" {
			if h.links[s.ID] != p.URL {
				h.links[s.ID] = p.URL
				h.announceLinkLocked(s, p.URL)
			}
			continue
		}
		sv := h.served[s.ID]
		if sv == nil || s.Port != 0 && s.Port != sv.listener.Port() {
			h.serveAgain(ctx, s, p, sv)
			continue
		}
		if string(s.Salt) != sv.salt {
			secret, err := h.keys.Secret(h.store, s.ID, s.Salt)
			if err == nil {
				sv.proxy.SetSecret(secret)
				sv.salt = string(s.Salt)
			}
		}
		if string(s.Point) != sv.point {
			sv.proxy.Repoint(h.dialer(p))
			sv.point = string(s.Point)
		}
	}
	for id, sv := range h.served {
		if !seen[id] {
			sv.close()
			delete(h.served, id)
		}
	}
	for id := range h.links {
		if !seen[id] {
			delete(h.links, id)
		}
	}
}

// serveAgain serves the session s, which points where p says, and which
// this process doesn't serve yet, as one that took it over finds it, or
// serves, as was, at another port than the store's, as another process's
// step gave: at its port, or, where that is taken now and the step didn't
// give it, at one the system picks, which the store keeps. A session it
// can't serve, as at a port another program holds, it says so in the store,
// and serves as it was, if it was; it tries again as Serve is called again.
func (h *SessionHost) serveAgain(ctx context.Context, s coordinator.Session, p sessionPoint, was *served) {
	next, err := h.start(ctx, s, p, string(s.Point), s.Port)
	if taken, isProblem := errors.AsType[*problem.Problem](err); isProblem && !s.PortGiven && taken.Code == problems.SessionLocalPortTaken.Code() {
		next, err = h.start(ctx, s, p, string(s.Point), 0)
	}
	if err != nil {
		prob, isProblem := errors.AsType[*problem.Problem](err)
		switch {
		case isProblem && s.Problem != nil && s.Problem.Code == prob.Code:
			// The store says so already.
		case isProblem:
			if err := h.codes.SessionServed(ctx, s.ID, 0, prob); err != nil {
				h.log.Warn("local: couldn't say why a session isn't served", "session", s.Name, "err", err)
			}
		default:
			h.log.Warn("local: couldn't serve a session", "session", s.Name, "err", err)
		}
		return
	}
	if port := next.listener.Port(); port != s.Port || s.Problem != nil {
		if err := h.codes.SessionServed(ctx, s.ID, port, nil); err != nil {
			h.log.Warn("local: couldn't store a session's port", "session", s.Name, "err", err)
		}
	}
	if was != nil {
		was.close()
	}
	h.served[s.ID] = next
	h.announceTunnel(ctx, s, next.listener.Port(), p)
}

// announceTunnel says the session s is served at port, where it points:
// with a link that holds a new one-time code, on a person's terminal, and
// otherwise, with the command that makes one. The host's lock is held.
func (h *SessionHost) announceTunnel(ctx context.Context, s coordinator.Session, port int, p sessionPoint) {
	if h.announce == nil {
		return
	}
	link := ""
	if h.terminal {
		var err error
		if link, err = NewSessionLink(ctx, h.codes, h.keys, h.user, s, port, p.Slug); err != nil {
			h.log.Warn("local: couldn't make a session's link", "session", s.Name, "err", err)
			link = ""
		}
	}
	if err := h.announce.Session(s.Run, s.Key, link); err != nil {
		h.log.Warn("local: couldn't say a session is served", "session", s.Name, "err", err)
	}
}

// announceLink says the session s is the link link.
func (h *SessionHost) announceLink(s coordinator.Session, link string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.links[s.ID] = link
	h.announceLinkLocked(s, link)
}

func (h *SessionHost) announceLinkLocked(s coordinator.Session, link string) {
	if h.announce == nil {
		return
	}
	if err := h.announce.Session(s.Run, s.Key, link); err != nil {
		h.log.Warn("local: couldn't say a session is served", "session", s.Name, "err", err)
	}
}

// NewSessionLink keeps a new one-time code of the session s, which codes
// keeps, until sessionproxy.CodeLifetime has passed, and returns a link to
// the session, served at port, with it: at its host name, at its path,
// which the login name user holds, and with what slug, update-session's,
// says after it, its path, its query and its fragment.
func NewSessionLink(ctx context.Context, codes SessionStore, keys *sessionproxy.Keyring, user string, s coordinator.Session, port int, slug string) (string, error) {
	code, mac := keys.NewCode(s.ID)
	if err := codes.AddSessionCode(ctx, s.ID, mac, sessionproxy.CodeLifetime); err != nil {
		return "", err
	}
	return SessionLink(user, s, port, slug, code), nil
}

// SessionLink returns a link to the session s, served at port, at its host
// name and its path, which the login name user holds, with what slug says
// after it, and, unless it is "", the one-time code code. A code the slug
// holds, which the session's proxy would read rather than the link's, it
// leaves out.
func SessionLink(user string, s coordinator.Session, port int, slug, code string) string {
	path, fragment, _ := strings.Cut(slug, "#")
	path, query, _ := strings.Cut(path, "?")
	query = strings.Join(slices.DeleteFunc(strings.Split(query, "&"), func(part string) bool {
		key, _, _ := strings.Cut(part, "=")
		key, err := url.QueryUnescape(key)
		return part == "" || err == nil && key == sessionproxy.CodeParam
	}), "&")
	u := url.URL{
		Scheme: "http", Host: net.JoinHostPort(sessionproxy.HostOf(s.Label), strconv.Itoa(port)),
		Path:     "/me/session/" + user + "/" + s.Name + "/" + strings.TrimPrefix(path, "/"),
		RawQuery: query, Fragment: fragment,
	}
	if code != "" {
		q := url.Values{sessionproxy.CodeParam: {code}}.Encode()
		if u.RawQuery == "" {
			u.RawQuery = q
		} else {
			u.RawQuery += "&" + q
		}
	}
	return u.String()
}

// Link returns a link to the session s, as meridian session url prints
// it: a link session's url, as it is; or, for a tunnel, a link to it at
// the port it is served at, with a new one-time code. A session that
// points nowhere yet, or whose process hasn't served it at a port yet, is
// problems.SessionNotReady; one its process can't serve, as at a port
// another program holds, the problem it said.
func (h *SessionHost) Link(ctx context.Context, s coordinator.Session) (string, error) {
	notReady := problems.SessionNotReady.Problem(problems.Params{"run": s.Run, "key": s.Key})
	if s.Point == nil {
		return "", notReady
	}
	var p sessionPoint
	if err := jsonv2.Unmarshal(s.Point, &p); err != nil {
		return "", fmt.Errorf("local: session %s's point doesn't decode", s.ID)
	}
	switch {
	case p.Type == "link":
		return p.URL, nil
	case s.Problem != nil:
		return "", s.Problem
	case s.Port == 0:
		return "", notReady
	}
	h.mu.Lock()
	codes := h.codes
	h.mu.Unlock()
	if codes == nil {
		return "", errors.New("local: the session host has no store before Attach")
	}
	return NewSessionLink(ctx, codes, h.keys, h.user, s, s.Port, p.Slug)
}
