//go:build !linux

package porttest

import (
	"net"
	"testing"
)

// refusing returns a port that a listener of the test's freed, which the
// system may give another listener before the test is done with it. A
// socket bound to it without listening would hold it, but macOS doesn't
// refuse a connection to such a socket: it drops the connection's
// packets (XNU's tcp_input, for a socket in TCPS_CLOSED), so that a dial
// there waits until it times out.
func refusing(t testing.TB) int {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port //nolint:forcetypeassert // a TCP listener's
	_ = ln.Close()
	return port
}
