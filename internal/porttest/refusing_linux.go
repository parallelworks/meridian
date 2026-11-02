//go:build linux

package porttest

import (
	"syscall"
	"testing"
)

// refusing returns a port that a TCP socket of the test's is bound to,
// at 127.0.0.1, until the test ends. The socket doesn't listen, so Linux
// resets a connection to the port. It has SO_REUSEADDR, as Go's listeners
// do, and so:
//   - no listener that asks for any port is given this one, as the
//     socket conflicts with it (inet_csk_find_open_port), nor is a
//     connect() (__inet_hash_connect);
//   - a listener of the test's own that asks for this port, with
//     SO_REUSEADDR, can listen at it, since the socket doesn't listen
//     (inet_bind_conflict).
func refusing(t testing.TB) int {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Close(fd) })
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	sa, err := syscall.Getsockname(fd)
	if err != nil {
		t.Fatal(err)
	}
	return sa.(*syscall.SockaddrInet4).Port //nolint:forcetypeassert // an AF_INET socket's
}
