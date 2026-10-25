//go:build linux || darwin || freebsd || openbsd

package worker

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/parallelworks/meridian/problems"
)

// TestCheckPeer checks that the worker asks the system which user serves
// a Unix socket, and sends a token only to its own user's, or root's.
func TestCheckPeer(t *testing.T) {
	t.Parallel()
	// A socket's path is short: t.TempDir's can be too long.
	dir, err := os.MkdirTemp("", "mw")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "c.sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := checkPeer(conn.(*net.UnixConn), socket); err != nil {
		t.Errorf("a socket this user serves: %v", err)
	}
	if other := os.Getuid() + 1; other != 0 {
		err := checkPeerUID(socket, other)
		if p := problemOf(err, problems.CoordinatorPeerRefused); p == nil || p.Params["uid"] != other {
			t.Errorf("a socket user %d serves: %v", other, err)
		}
	}
	if err := checkPeerUID(socket, 0); err != nil {
		t.Errorf("a socket root serves: %v", err)
	}
}
