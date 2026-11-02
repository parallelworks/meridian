// Package porttest gives a test a port of 127.0.0.1 that nothing listens
// at, which a connection is refused at.
package porttest

import "testing"

// Refusing returns a port of 127.0.0.1 that nothing listens at, so that a
// connection to it is refused, until the test listens at it itself, as
// it may, and again once that listener has closed. A port that a listener
// asked for and then freed isn't enough on its own: the system can give
// it to any other listener that asks for a port, as another test's does,
// which then has the connections the test meant to be refused, or resets
// them as it closes. So on Linux, the test holds the port until it ends
// (refusing); elsewhere, it is such a freed port.
func Refusing(t testing.TB) int {
	t.Helper()
	return refusing(t)
}
