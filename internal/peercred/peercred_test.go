package peercred_test

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/parallelworks/meridian/internal/peercred"
)

// UID says which user serves a Unix socket, and which connects to it: this
// test's, at both ends. Elsewhere than Linux, macOS and FreeBSD it refuses
// to say.
func TestUID(t *testing.T) {
	// A socket's path is short: t.TempDir's can be too long.
	dir, err := os.MkdirTemp("", "pc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "s.sock")
	l, err := new(net.ListenConfig).Listen(t.Context(), "unix", socket)
	if err != nil {
		t.Skipf("no Unix socket here: %v", err)
	}
	defer l.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()
	conn, err := new(net.Dialer).DialContext(t.Context(), "unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	server, ok := <-accepted
	if !ok {
		t.Fatal("the socket accepted no connection")
	}
	defer server.Close()
	for side, c := range map[string]net.Conn{"the server": conn, "the client": server} {
		uid, err := peercred.UID(c.(*net.UnixConn))
		switch runtime.GOOS {
		case "linux", "darwin", "freebsd":
			if err != nil || uid != os.Getuid() {
				t.Errorf("%s is user %d, %v; want %d", side, uid, err, os.Getuid())
			}
		default:
			if !errors.Is(err, peercred.ErrUnsupported) || !errors.Is(err, errors.ErrUnsupported) {
				t.Errorf("%s on %s: %d, %v; want ErrUnsupported", side, runtime.GOOS, uid, err)
			}
		}
	}
}

// Meridian trusts its own user at the other end of a socket, and root, and
// no other.
func TestTrusted(t *testing.T) {
	if !peercred.Trusted(os.Getuid()) || !peercred.Trusted(0) {
		t.Error("this user, or root, isn't trusted")
	}
	if other := os.Getuid() + 1; other != 0 && peercred.Trusted(other) {
		t.Errorf("user %d is trusted", other)
	}
}
