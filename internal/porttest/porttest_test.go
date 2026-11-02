package porttest_test

import (
	"errors"
	"net"
	"strconv"
	"syscall"
	"testing"

	"github.com/parallelworks/meridian/internal/porttest"
)

// TestRefusing checks that 127.0.0.1 refuses a connection at the port
// Refusing gives; that the test can listen at the port, and is connected
// to there; and that 127.0.0.1 refuses a connection again once that
// listener has closed.
func TestRefusing(t *testing.T) {
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(porttest.Refusing(t)))
	refused := func(when string) {
		t.Helper()
		conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp4", address)
		if err == nil {
			_ = conn.Close()
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			t.Errorf("%s, a dial gave %v; want it refused", when, err)
		}
	}
	refused("at first")
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp4", address)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp4", address)
	if err != nil {
		t.Errorf("with the test's listener there, a dial gave %v", err)
	} else {
		_ = conn.Close()
	}
	_ = ln.Close()
	refused("once the test's listener has closed")
}
