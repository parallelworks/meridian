package sockowner

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/internal/sockownertest"
)

func TestMain(m *testing.M) {
	// TestOwnerOfAnotherUser starts this binary again, as another user,
	// to listen.
	sockownertest.Main()
	os.Exit(m.Run())
}

// supported reports whether Owner asks the system here.
func supported() bool {
	switch runtime.GOOS {
	case "linux", "darwin", "freebsd":
		return true
	}
	return false
}

// loopbackPair returns a connection to a listener of the test's own on
// network's loopback address, and the listener, which hasn't accepted it;
// or skips the test where the system has no such address.
func loopbackPair(t *testing.T, network string) (*net.TCPConn, *net.TCPListener) {
	t.Helper()
	address := "127.0.0.1:0"
	if network == "tcp6" {
		address = "[::1]:0"
	}
	l, err := new(net.ListenConfig).Listen(t.Context(), network, address)
	if err != nil {
		if network == "tcp6" {
			t.Skipf("no [::1] here: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	conn, err := new(net.Dialer).DialContext(t.Context(), network, l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn.(*net.TCPConn), l.(*net.TCPListener)
}

// TestOwner checks that a connection to a listener of the test's own, at
// 127.0.0.1 or [::1], is the test's user's, and that the connection the
// listener accepted is too, at whose other end is the test's own dial;
// that, before the listener accepts it, it is too on macOS and FreeBSD,
// and on Linux, whose kernel gives such a socket's owner as root, it is
// no one's yet; or, elsewhere, that Owner refuses to say.
func TestOwner(t *testing.T) {
	defer func(s time.Duration) { settle = s }(settle)
	settle = 50 * time.Millisecond
	for _, network := range []string{"tcp4", "tcp6"} {
		t.Run(network, func(t *testing.T) {
			conn, l := loopbackPair(t, network)
			want := func(when string, conn *net.TCPConn) {
				t.Helper()
				uid, err := Owner(t.Context(), conn)
				switch {
				case !supported():
					if !errors.Is(err, ErrUnsupported) || !errors.Is(err, errors.ErrUnsupported) {
						t.Errorf("%s, on %s: user %d, %v; want ErrUnsupported", when, runtime.GOOS, uid, err)
					}
				case when == "before it is accepted" && runtime.GOOS == "linux":
					if !errors.Is(err, ErrNotYet) {
						t.Errorf("%s: user %d, %v; want ErrNotYet", when, uid, err)
					}
				case err != nil || int(uid) != os.Getuid():
					t.Errorf("%s: user %d, %v; want %d", when, uid, err, os.Getuid())
				}
			}
			want("before it is accepted", conn)
			accepted, err := l.AcceptTCP()
			if err != nil {
				t.Fatal(err)
			}
			defer accepted.Close()
			want("once it is accepted", conn)
			want("of the accepted connection", accepted)
		})
	}
}

// TestOwnerWaitsForAccept checks that Owner, asked of a connection its
// listener hasn't accepted yet, says its owner once the listener has.
func TestOwnerWaitsForAccept(t *testing.T) {
	if !supported() {
		t.Skip("Owner doesn't ask the system here")
	}
	conn, l := loopbackPair(t, "tcp4")
	owner := make(chan error, 1)
	go func() {
		uid, err := Owner(t.Context(), conn)
		if err == nil && int(uid) != os.Getuid() {
			err = fmt.Errorf("user %d", uid)
		}
		owner <- err
	}()
	// Owner asks for a while before the listener accepts, at least on
	// Linux, where it waits.
	time.Sleep(20 * time.Millisecond) //nolint:forbidigo // the delay is the test: Owner asks before the accept
	accepted, err := l.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	if err := <-owner; err != nil {
		t.Errorf("once accepted, the connection is %v; want user %d's", err, os.Getuid())
	}
}

// TestOwnerOfAnotherUser checks, where the test runs as root, that a
// connection to a listener of another user's is that user's.
func TestOwnerOfAnotherUser(t *testing.T) {
	port := sockownertest.ListenAs(t)
	conn, err := new(net.Dialer).DialContext(t.Context(), "tcp4", netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port)).String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if uid, err := Owner(t.Context(), conn.(*net.TCPConn)); err != nil || uid != sockownertest.UID {
		t.Errorf("a listener of user %d's is user %d's, %v", sockownertest.UID, uid, err)
	}
}

// TestOtherEnd checks which end Owner asks about: the socket the listener
// accepted, whose local address is the connection's remote one, and its
// remote one the connection's local; and that it asks about none at an
// address other than this machine's loopback address.
func TestOtherEnd(t *testing.T) {
	conn, l := loopbackPair(t, "tcp4")
	e, err := otherEnd(conn)
	if err != nil {
		t.Fatal(err)
	}
	if e.local != l.Addr().(*net.TCPAddr).AddrPort() || e.remote != conn.LocalAddr().(*net.TCPAddr).AddrPort() {
		t.Errorf("the other end of a connection from %v to %v is %v", conn.LocalAddr(), conn.RemoteAddr(), e)
	}
	var other netip.Addr
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			if p, err := netip.ParsePrefix(a.String()); err == nil && p.Addr().Is4() && !p.Addr().IsLoopback() {
				other = p.Addr()
				break
			}
		}
	}
	if !other.IsValid() {
		t.Skip("this machine has no address but its loopback one")
	}
	away, err := new(net.ListenConfig).Listen(t.Context(), "tcp4", netip.AddrPortFrom(other, 0).String())
	if err != nil {
		t.Skipf("can't listen at %v: %v", other, err)
	}
	defer away.Close()
	c, err := new(net.Dialer).DialContext(t.Context(), "tcp4", away.Addr().String())
	if err != nil {
		t.Skipf("can't connect to %v: %v", away.Addr(), err)
	}
	defer c.Close()
	if uid, err := Owner(t.Context(), c.(*net.TCPConn)); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Errorf("a connection to %v is user %d's, %v", away.Addr(), uid, err)
	}
}

// TestOwnerWaits checks that Owner asks again while the system says no
// owner of the connection's other end yet, as of its handshake, for a
// while, and then gives up; and, for a shorter while, while it lists no
// socket there, as for a connection whose socket is made only as the
// handshake's last packet comes; and that it doesn't ask again for any
// other error.
func TestOwnerWaits(t *testing.T) {
	defer func(s, g time.Duration) { settle, grace = s, g }(settle, grace)
	settle, grace = 300*time.Millisecond, 100*time.Millisecond
	conn, _ := loopbackPair(t, "tcp4")
	asked := 0
	notYet := func(times int) func(end) (uint32, error) {
		return func(end) (uint32, error) {
			if asked++; asked <= times {
				return 0, ErrNotYet
			}
			return 42, nil
		}
	}
	if uid, err := ownerBy(t.Context(), conn, notYet(2)); uid != 42 || err != nil || asked != 3 {
		t.Errorf("asked %d times: user %d, %v; want 42 on the third", asked, uid, err)
	}
	asked = 0
	start := time.Now()
	if uid, err := ownerBy(t.Context(), conn, notYet(1<<30)); !errors.Is(err, ErrNotYet) || time.Since(start) < settle {
		t.Errorf("of a socket the system never has, after %v: user %d, %v", time.Since(start), uid, err)
	}
	asked = 0
	unlisted := func(times int) func(end) (uint32, error) {
		return func(end) (uint32, error) {
			if asked++; asked <= times {
				return 0, errUnlisted
			}
			return 43, nil
		}
	}
	if uid, err := ownerBy(t.Context(), conn, unlisted(2)); uid != 43 || err != nil || asked != 3 {
		t.Errorf("asked %d times: user %d, %v; want 43 on the third", asked, uid, err)
	}
	asked = 0
	start = time.Now()
	if uid, err := ownerBy(t.Context(), conn, unlisted(1<<30)); !errors.Is(err, errUnlisted) || time.Since(start) < grace || time.Since(start) >= settle {
		t.Errorf("of a socket the system never lists, after %v: user %d, %v; want to ask for %v", time.Since(start), uid, err, grace)
	}
	asked = 0
	failed := errors.New("no")
	if uid, err := ownerBy(t.Context(), conn, func(end) (uint32, error) { asked++; return 0, failed }); !errors.Is(err, failed) || asked != 1 {
		t.Errorf("asked %d times: user %d, %v; want the error at once", asked, uid, err)
	}
}

// TestOwnerWaitsForItsContext checks that Owner gives up waiting for an
// owner of the connection's other end once its context is done, as a
// request that gave up on its connection's is.
func TestOwnerWaitsForItsContext(t *testing.T) {
	conn, _ := loopbackPair(t, "tcp4")
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(20*time.Millisecond, cancel)
	start := time.Now()
	uid, err := ownerBy(ctx, conn, func(end) (uint32, error) { return 0, ErrNotYet })
	if !errors.Is(err, context.Canceled) || time.Since(start) > settle/2 {
		t.Errorf("after %v of a context done after 20ms: user %d, %v", time.Since(start), uid, err)
	}
}

// TestTrusted checks whom an owner Owner says is trusted as: this user
// alone, and, on Linux, where the owner is the user of the process that
// accepted the connection, root too; but not root on macOS and FreeBSD,
// where the owner is the user who made the listener, as a service of
// root's does for other users' programs; nor any other user.
func TestTrusted(t *testing.T) {
	if !Trusted(uint32(os.Getuid())) {
		t.Errorf("this user, %d, isn't trusted", os.Getuid())
	}
	if root := Trusted(0); root != (runtime.GOOS == "linux" || os.Getuid() == 0) {
		t.Errorf("on %s, root is trusted: %t", runtime.GOOS, root)
	}
	if other := uint32(os.Getuid() + 1); other != 0 && Trusted(other) {
		t.Errorf("user %d is trusted", other)
	}
}
