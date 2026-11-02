package local

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/internal/sessionproxy"
	"github.com/parallelworks/meridian/internal/sockowner"
	"github.com/parallelworks/meridian/internal/sockownertest"
	"github.com/parallelworks/meridian/problems"
)

// foreignUID is a user that is neither the test's nor root.
func foreignUID() uint32 {
	if os.Getuid() == 4242 {
		return 4243
	}
	return 4242
}

// A byteCounter is a listener of the test's own that accepts each
// connection, and counts the bytes that reach it, which it answers
// nothing to, and the connections closed.
type byteCounter struct {
	net.Listener
	got, closed atomic.Int64
}

// countBytes listens at port of address, of network, as listenOn does,
// and counts the bytes that reach it.
func countBytes(t *testing.T, network, address string, port int) *byteCounter {
	t.Helper()
	b := &byteCounter{Listener: listenOn(t, network, address, port)}
	go func() {
		for {
			c, err := b.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				n, _ := io.Copy(io.Discard, c)
				b.got.Add(n)
				b.closed.Add(1)
			}()
		}
	}()
	return b
}

func (b *byteCounter) port() int { return b.Addr().(*net.TCPAddr).Port }

// An asked is the connections an owner was asked of.
type asked struct {
	mu    sync.Mutex
	conns []*net.TCPConn
}

func (a *asked) all() []*net.TCPConn {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]*net.TCPConn(nil), a.conns...)
}

// ownerSays returns an owner that says uid, or err, of a connection to
// port, recording each in a, and asks the system of any other, as of the
// host's own listener.
func ownerSays(port int, uid uint32, err error, a *asked) func(context.Context, *net.TCPConn) (uint32, error) {
	return func(ctx context.Context, c *net.TCPConn) (uint32, error) {
		if c.RemoteAddr().(*net.TCPAddr).Port != port {
			return sockowner.Owner(ctx, c)
		}
		a.mu.Lock()
		a.conns = append(a.conns, c)
		a.mu.Unlock()
		return uid, err
	}
}

// TestSessionDialerChecksOwner checks that a session's connection to a
// service of this machine is passed on only where the system says it is
// this user's, or, on Linux, where the system says who accepted it,
// root's: one of another user's, as one that listened at the service's
// port first, or, on macOS and FreeBSD, of root's, which says who made
// the listener, is closed, before a byte is sent on it, as
// session_service_foreign; one whose owner the system says nothing of is
// session_service_owner_unknown; one where the system can't be asked at
// all is session_service_unchecked; and one no process accepted in time
// is an error of the service, which doesn't answer. A connection to
// [::1], as where 127.0.0.1 refuses, is checked as well.
func TestSessionDialerChecksOwner(t *testing.T) {
	t.Parallel()
	failed := errors.New("sockowner: the socket at the connection's other end is in TCP state 5")
	rootsOK, rootsKind := runtime.GOOS == "linux" || os.Getuid() == 0, problems.SessionServiceForeign
	if rootsOK {
		rootsKind = nil
	}
	for _, tc := range []struct {
		name string
		uid  uint32
		err  error
		want *problems.Kind
		ok   bool
	}{
		{"this user's", uint32(os.Getuid()), nil, nil, true},
		{"root's", 0, nil, rootsKind, rootsOK},
		{"another user's", foreignUID(), nil, problems.SessionServiceForeign, false},
		{"unsupported", 0, sockowner.ErrUnsupported, problems.SessionServiceUnchecked, false},
		{"of no owner said", uint32(os.Getuid()), failed, problems.SessionServiceOwnerUnknown, false},
		{"not accepted", 0, sockowner.ErrNotYet, nil, false},
	} {
		for _, network := range []string{"tcp4", "tcp6"} {
			t.Run(tc.name+" "+network, func(t *testing.T) {
				t.Parallel()
				h, _, _ := newTestSessionHost(t)
				var service *byteCounter
				if network == "tcp4" {
					service = countBytes(t, "tcp4", "127.0.0.1", 0)
				} else {
					// A port 127.0.0.1 refuses at, which [::1] listens at.
					free := listenOn(t, "tcp4", "127.0.0.1", 0)
					port := free.Addr().(*net.TCPAddr).Port
					_ = free.Close()
					service = countBytes(t, "tcp6", "::1", port)
				}
				var a asked
				h.owner = ownerSays(service.port(), tc.uid, tc.err, &a)
				conn, err := h.dialer("web-1", sessionPoint{Type: "tunnel", RemoteHost: "localhost", RemotePort: service.port()})(t.Context())
				got := a.all()
				if len(got) != 1 || !strings.HasPrefix(got[0].RemoteAddr().String(), map[string]string{"tcp4": "127.0.0.1:", "tcp6": "[::1]:"}[network]) {
					t.Fatalf("the owner was asked of connections %v", got)
				}
				switch {
				case tc.ok:
					if err != nil {
						t.Fatalf("the dial = %v", err)
					}
					if _, err := io.WriteString(conn, "GET / HTTP/1.0\r\n\r\n"); err != nil {
						t.Fatal(err)
					}
					_ = conn.Close()
					waitFor(t, func() bool { return service.got.Load() > 0 })
					return
				case conn != nil || err == nil:
					t.Fatalf("the dial = %v, %v; want an error", conn, err)
				case tc.want != nil:
					got := kindtest.Want(t, err, tc.want)
					if tc.want != problems.SessionServiceUnchecked && (got.Params["name"] != "web-1" || got.Params["port"] != strconv.Itoa(service.port())) {
						t.Errorf("the problem's params are %v", got.Params)
					}
				case problems.IsProblem(err) || !errors.Is(err, sockowner.ErrNotYet):
					t.Errorf("a connection no process accepted = %v; want an error that the service doesn't answer", err)
				}
				// The connection was closed, with nothing sent.
				if _, err := got[0].Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
					t.Errorf("the refused connection was open: %v", err)
				}
				waitFor(t, func() bool { return service.closed.Load() == 1 })
				if n := service.got.Load(); n != 0 {
					t.Errorf("%d bytes reached the service", n)
				}
			})
		}
	}
}

// waitFor waits, ten seconds at most, for ok.
func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !ok(); {
		if time.Now().After(deadline) {
			t.Fatal("gave up waiting")
		}
		time.Sleep(time.Millisecond) //nolint:forbidigo // polls for what has no signal of its own
	}
}

// TestSessionForeignService checks a session whose service's port another
// user listens at, as a browser sees it: each request is answered 502,
// with a page that says so, which names the port, but for a private
// session's; nothing of any reaches the port; and the host logs it once
// for each place a step points the session, not for each request.
func TestSessionForeignService(t *testing.T) {
	t.Parallel()
	for _, private := range []bool{false, true} {
		t.Run("private "+strconv.FormatBool(private), func(t *testing.T) {
			t.Parallel()
			cfg, err := LoadConfig(configDir(t, nil), lookup(nil))
			if err != nil {
				t.Fatal(err)
			}
			var (
				mu  sync.Mutex
				out strings.Builder
			)
			logger := slog.New(slog.NewTextHandler(writerFunc(func(b []byte) (int, error) {
				mu.Lock()
				defer mu.Unlock()
				return out.Write(b)
			}), nil))
			logged := func() string {
				mu.Lock()
				defer mu.Unlock()
				return out.String()
			}
			h, err := NewSessionHost(t.Context(), cfg, Login{Name: "alice"}, []byte(strings.Repeat("k", sessionproxy.KeyLen)), nil, logger)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			h.Attach("store", &testSessionStore{})
			service := countBytes(t, "tcp4", "127.0.0.1", 0)
			var a asked
			h.owner = ownerSays(service.port(), foreignUID(), nil, &a)
			s := testSession(map[string]any{})
			point := func() {
				t.Helper()
				sp, err := h.Point(t.Context(), s, compile.SessionUpdate{Name: s.Name, With: map[string]any{"remotePort": float64(service.port())}}, true)
				if err != nil {
					t.Fatal(err)
				}
				s.Point, s.Port = sp.Point, sp.Port
			}
			if private {
				s.Private = true
			}
			point()
			link, err := h.Link(t.Context(), s)
			if err != nil {
				t.Fatal(err)
			}
			client := sessionClient(t)
			if code, _ := get(t, client, link); code != http.StatusOK {
				t.Fatalf("the link answers %d", code)
			}
			u, _ := url.Parse(link)
			base := u.Scheme + "://" + u.Host + u.Path
			for range 3 {
				code, body := get(t, client, base)
				if code != http.StatusBadGateway || !strings.HasSuffix(body, "("+string(problems.SessionServiceForeign.Code())+")\n") {
					t.Fatalf("a session of another user's service answers %d %q", code, body)
				}
				if port := strconv.Itoa(service.port()); strings.Contains(body, port) == private {
					t.Errorf("private %t, the page says %q", private, body)
				}
			}
			if n := strings.Count(logged(), "another user listens"); n != 1 {
				t.Errorf("three requests logged %d times:\n%s", n, logged())
			}
			if strings.Contains(logged(), "user "+strconv.Itoa(int(foreignUID()))) == private {
				t.Errorf("private %t, the log says:\n%s", private, logged())
			}
			// Pointed again, it is logged again, once.
			point()
			for range 2 {
				if code, _ := get(t, client, base); code != http.StatusBadGateway {
					t.Fatalf("pointed again, it answers %d", code)
				}
			}
			if n := strings.Count(logged(), "another user listens"); n != 2 {
				t.Errorf("pointed twice, it logged %d times:\n%s", n, logged())
			}
			// Each request's connection was closed, with nothing sent.
			waitFor(t, func() bool { return service.closed.Load() == 5 })
			if n, conns := service.got.Load(), len(a.all()); n != 0 || conns != 5 {
				t.Errorf("%d bytes of %d connections reached the service", n, conns)
			}
		})
	}
}

// TestSessionHostUnchecked checks a host where the system won't say who
// listens at a port of this machine, or says what can't be: a step that
// points a session at a service on it fails, and one that points one at a
// cluster's doesn't; and one taken over isn't served, which the store
// says, once, and the log says why of, once.
func TestSessionHostUnchecked(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		uid  uint32
		err  error
	}{
		{"unsupported", 0, sockowner.ErrUnsupported},
		{"failing", 0, errors.New("sockowner: NETLINK_SOCK_DIAG: permission denied")},
		{"another user's own", foreignUID(), nil},
		{"root's own", 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := LoadConfig(configDir(t, map[string]string{ClustersFile: "hpc:\n  host: farm\n"}), lookup(nil))
			if err != nil {
				t.Fatal(err)
			}
			var (
				mu  sync.Mutex
				out strings.Builder
			)
			logger := slog.New(slog.NewTextHandler(writerFunc(func(b []byte) (int, error) {
				mu.Lock()
				defer mu.Unlock()
				return out.Write(b)
			}), nil))
			dial := func(context.Context, string, string, int) (net.Conn, error) {
				return nil, errors.New("no cluster here")
			}
			h, err := NewSessionHost(t.Context(), cfg, Login{Name: "alice"}, []byte(strings.Repeat("k", sessionproxy.KeyLen)), dial, logger)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			store := &testSessionStore{}
			h.Attach("store", store)
			if os.Getuid() == 0 && tc.uid == 0 && tc.err == nil {
				t.Skip("root's own listener is root's")
			}
			h.owner = func(context.Context, *net.TCPConn) (uint32, error) { return tc.uid, tc.err }
			s := testSession(map[string]any{})
			// Whether this process serves the session or another does.
			for _, serve := range []bool{false, true} {
				_, err = h.Point(t.Context(), s, compile.SessionUpdate{Name: s.Name, With: map[string]any{"remotePort": float64(8888)}}, serve)
				if got := kindtest.Want(t, err, problems.SessionServiceUnchecked); got.Params["os"] == "" {
					t.Errorf("the problem's params are %v", got.Params)
				}
			}
			if _, err := h.Point(t.Context(), s, compile.SessionUpdate{Name: s.Name, With: map[string]any{"remotePort": float64(8888), "target": "hpc"}}, true); err != nil {
				t.Errorf("a session of a cluster's service = %v", err)
			}
			taken := testSession(map[string]any{})
			taken.ID, taken.Name, taken.Key = "def", "taken-1", "taken"
			taken.Point = []byte(`{"type":"tunnel","remoteHost":"localhost","remotePort":8888}`)
			h.Serve(t.Context(), []coordinator.Session{s, taken})
			taken.Problem = problems.SessionServiceUnchecked.Problem(problems.Params{"os": "x"})
			h.Serve(t.Context(), []coordinator.Session{s, taken})
			if got := store.records(); len(got) != 1 || got[0] != "def "+string(problems.SessionServiceUnchecked.Code()) {
				t.Errorf("the store keeps %q", got)
			}
			mu.Lock()
			logged := out.String()
			mu.Unlock()
			if n := strings.Count(logged, "won't say who listens"); n != 1 {
				t.Errorf("logged %d times:\n%s", n, logged)
			}
		})
	}
}

// TestSessionDialerRefusesAnotherUser checks, where the test runs as
// root, that a connection to a port another user listens at is refused,
// as the system says.
func TestSessionDialerRefusesAnotherUser(t *testing.T) {
	port := sockownertest.ListenAs(t)
	// A dial to this machine reads no config: so the test needs no config
	// directory, whose path's owners, root's among them, an unprivileged
	// user namespace that maps root to its user doesn't see as Meridian
	// trusts them.
	h, err := NewSessionHost(t.Context(), &Config{}, Login{Name: "alice"}, []byte(strings.Repeat("k", sessionproxy.KeyLen)), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	_, err = h.dialer("web-1", sessionPoint{Type: "tunnel", RemoteHost: "localhost", RemotePort: port})(t.Context())
	kindtest.Want(t, err, problems.SessionServiceForeign)
	if !strings.Contains(err.Error(), "user "+strconv.Itoa(sockownertest.UID)) {
		t.Errorf("the error is %v", err)
	}
}

// TestSessionDialerEndsWithItsContext checks that a dial to a service of
// this machine that waits for the service to accept its connection, as on
// Linux, whose system says no owner of a connection until then, gives up
// once its context ends, as once the request it was dialed for has, and
// closes the connection, rather than wait on for its service.
func TestSessionDialerEndsWithItsContext(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("only Linux says no owner of a connection until it is accepted")
	}
	h, _, _ := newTestSessionHost(t)
	// A listener that accepts nothing.
	l := listenOn(t, "tcp4", "127.0.0.1", 0)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	conn, err := h.dialer("web-1", sessionPoint{Type: "tunnel", RemoteHost: "localhost", RemotePort: l.Addr().(*net.TCPAddr).Port})(ctx)
	if conn != nil || !errors.Is(err, context.DeadlineExceeded) || problems.IsProblem(err) || time.Since(start) > time.Second {
		t.Errorf("after %v, a dial whose context ended = %v, %v", time.Since(start), conn, err)
	}
}

// TestSessionHostUncheckedOnceItIs checks that a check of who listens at a
// port of this machine whose context ended first, which says nothing of
// the system, isn't one the host says the system won't say from: it logs
// nothing of it, and logs a check that fails after it.
func TestSessionHostUncheckedOnceItIs(t *testing.T) {
	t.Parallel()
	cfg, err := LoadConfig(configDir(t, nil), lookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu  sync.Mutex
		out strings.Builder
	)
	logger := slog.New(slog.NewTextHandler(writerFunc(func(b []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return out.Write(b)
	}), nil))
	logged := func() int {
		mu.Lock()
		defer mu.Unlock()
		return strings.Count(out.String(), "won't say who listens")
	}
	h, err := NewSessionHost(t.Context(), cfg, Login{Name: "alice"}, []byte(strings.Repeat("k", sessionproxy.KeyLen)), nil, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	h.Attach("store", &testSessionStore{})
	h.owner = func(context.Context, *net.TCPConn) (uint32, error) { return 0, sockowner.ErrUnsupported }
	s := testSession(map[string]any{})
	update := compile.SessionUpdate{Name: s.Name, With: map[string]any{"remotePort": float64(8888)}}
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := h.Point(ended, s, update, false); problems.IsProblem(err) || !errors.Is(err, context.Canceled) || logged() != 0 {
		t.Errorf("of a context that ended: %v, and %d lines logged", err, logged())
	}
	if _, err := h.Point(t.Context(), s, update, false); !isKind(err, problems.SessionServiceUnchecked) || logged() != 1 {
		t.Errorf("after it: %v, and %d lines logged", err, logged())
	}
}
