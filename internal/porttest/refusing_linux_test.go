package porttest_test

import (
	"errors"
	"syscall"
	"testing"

	"github.com/parallelworks/meridian/internal/porttest"
)

// TestRefusingHeld checks that, on Linux, the port that Refusing gives
// stays bound until the test ends: a socket without SO_REUSEADDR can't be
// bound to it, as it could be to a port that a listener freed.
func TestRefusingHeld(t *testing.T) {
	port := porttest.Refusing(t)
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	err = syscall.Bind(fd, &syscall.SockaddrInet4{Port: port, Addr: [4]byte{127, 0, 0, 1}})
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Errorf("a socket's bind to the port gave %v; want EADDRINUSE", err)
	}
}
