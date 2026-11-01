// Package sockownertest has a test that runs as root listen at a port of
// this machine's loopback address as another user, so that it can check
// that internal/sockowner says who listens there, and that a session isn't
// passed on to them.
package sockownertest

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
)

// listenEnv is set in the environment of the test binary that ListenAs
// starts, which Main then has listen.
const listenEnv = "MERIDIAN_SOCKOWNERTEST_LISTEN"

// RootEnv is set in the environment of a test that uses ListenAs where
// the test runs as root, as CI runs it (make test-root), so that ListenAs
// fails the test, rather than skip it, where it doesn't.
const RootEnv = "MERIDIAN_TEST_ROOT"

// UID is the user ListenAs listens as: nobody's, on most systems, which
// the system needn't know of.
const UID = 65534

// Main has a test binary that ListenAs started listen at a port of
// 127.0.0.1 that the system picks, say which on its standard output,
// accept every connection, and read it, until its standard input closes,
// when it exits; in any other, it returns at once. A test binary that uses
// ListenAs calls it first in its TestMain.
func Main() {
	if os.Getenv(listenEnv) == "" {
		return
	}
	ctx := context.Background() //nolint:forbidigo // a process of a test's own, which has no test to take a context from
	l, err := new(net.ListenConfig).Listen(ctx, "tcp4", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(io.Discard, c) }()
		}
	}()
	fmt.Println(l.Addr().(*net.TCPAddr).Port)
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}
