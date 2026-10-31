package sshconn_test

import (
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend/ssh/internal/sshconn"
	"github.com/parallelworks/meridian/internal/leaktest"
	"github.com/parallelworks/meridian/internal/sshtest"
)

// ping sends ping over conn, and fails the test unless it reads it back.
func ping(t *testing.T, conn net.Conn) {
	t.Helper()
	if _, err := io.WriteString(conn, "ping"); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("the connection echoed %q, %v", buf, err)
	}
}

// waitClosed waits for a read of conn to fail, as it does once conn has
// closed.
func waitClosed(t *testing.T, conn net.Conn, why string) {
	t.Helper()
	read := make(chan error, 1)
	go func() {
		_, err := conn.Read(make([]byte, 1))
		read <- err
	}()
	select {
	case err := <-read:
		if err == nil {
			t.Errorf("%s, the connection read a byte", why)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("%s, the connection didn't close", why)
	}
}

// closedPort returns a port of the loopback interface that nothing
// listens on.
func closedPort(t *testing.T) int {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port //nolint:forcetypeassert // a TCP listener's
	_ = ln.Close()
	return port
}

// TestDialTCP checks that a host connects to an address for the
// connection, by its IP address or a name it resolves, and carries what
// the connection carries; that a host that couldn't connect is
// ErrTCPConnectFailed, which leaves the connection up; and that the
// connection closes with its context, and with the connection it is
// carried over, leaving no goroutine running.
func TestDialTCP(t *testing.T) {
	start := func(t *testing.T) (*sshtest.Server, *sshconn.Conn, int) {
		t.Helper()
		leaktest.Check(t)
		h := newHome(t)
		s := h.server(sshtest.Options{})
		h.knownHosts(s.KnownHosts(""))
		return s, dial(t, h.dialer(), sshconn.Target{Host: s.Addr()}), sshtest.Echo(t)
	}
	t.Run("address and name", func(t *testing.T) {
		s, c, port := start(t)
		for _, host := range []string{"127.0.0.1", "localhost"} {
			conn, err := c.DialTCP(t.Context(), host, port)
			if err != nil {
				t.Fatalf("%s: %v", host, err)
			}
			ping(t, conn)
			if err := conn.Close(); err != nil {
				t.Errorf("%s: closing gave %v", host, err)
			}
		}
		want := []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), net.JoinHostPort("localhost", strconv.Itoa(port))}
		if got := s.Forwards(); !slices.Equal(got, want) {
			t.Errorf("the host connected to %v, want %v", got, want)
		}
	})
	t.Run("nothing listens", func(t *testing.T) {
		_, c, _ := start(t)
		_, err := c.DialTCP(t.Context(), "127.0.0.1", closedPort(t))
		if !errors.Is(err, sshconn.ErrTCPConnectFailed) || errors.Is(err, sshconn.ErrTCPForwardingRefused) {
			t.Errorf("an address nothing listens on gave %v, want ErrTCPConnectFailed", err)
		}
		if err := c.Err(); err != nil {
			t.Errorf("the host's answer closed the connection: %v", err)
		}
	})
	t.Run("context", func(t *testing.T) {
		_, c, port := start(t)
		ctx, cancel := context.WithCancel(t.Context())
		conn, err := c.DialTCP(ctx, "127.0.0.1", port)
		if err != nil {
			t.Fatal(err)
		}
		ping(t, conn)
		cancel()
		waitClosed(t, conn, "once its context ended")
		if _, err := c.DialTCP(ctx, "127.0.0.1", port); !errors.Is(err, context.Canceled) {
			t.Errorf("a dial whose context has ended gave %v", err)
		}
	})
	t.Run("connection lost", func(t *testing.T) {
		s, c, port := start(t)
		conn, err := c.DialTCP(t.Context(), "127.0.0.1", port)
		if err != nil {
			t.Fatal(err)
		}
		ping(t, conn)
		s.Drop()
		waitClosed(t, conn, "once the connection it was carried over ended")
		waitDone(t, c)
		if _, err := c.DialTCP(t.Context(), "127.0.0.1", port); err == nil {
			t.Error("a dial over a closed connection succeeded")
		}
	})
}

// TestDialTCPRefused checks that a host that won't forward a TCP
// connection is ErrTCPForwardingRefused, and that the connection stays
// up.
func TestDialTCPRefused(t *testing.T) {
	leaktest.Check(t)
	h := newHome(t)
	s := h.server(sshtest.Options{RefuseForwards: true})
	h.knownHosts(s.KnownHosts(""))
	c := dial(t, h.dialer(), sshconn.Target{Host: s.Addr()})
	_, err := c.DialTCP(t.Context(), "127.0.0.1", sshtest.Echo(t))
	if !errors.Is(err, sshconn.ErrTCPForwardingRefused) {
		t.Errorf("a host that won't forward gave %v, want ErrTCPForwardingRefused", err)
	}
	if out := run(t, c, "echo up"); out != "up\n" {
		t.Errorf("after the refusal, the command printed %q", out)
	}
}

// TestDialTCPUnanswered checks that a dial the host doesn't answer ends
// with its context, and leaves no goroutine running once the connection
// closes, which the host's answer, had it come, would have needed.
func TestDialTCPUnanswered(t *testing.T) {
	leaktest.Check(t)
	h := newHome(t)
	s := h.server(sshtest.Options{HoldForwards: true})
	h.knownHosts(s.KnownHosts(""))
	c := dial(t, h.dialer(), sshconn.Target{Host: s.Addr()})
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if _, err := c.DialTCP(ctx, "127.0.0.1", sshtest.Echo(t)); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a dial the host doesn't answer gave %v", err)
	}
	if out := run(t, c, "echo up"); out != "up\n" {
		t.Errorf("after the dial, the command printed %q", out)
	}
}

// TestDialTCPLimit checks that a connection carries at most MaxTCPConns
// connections to addresses at once: one more waits for one to close, and
// for its context.
func TestDialTCPLimit(t *testing.T) {
	leaktest.Check(t)
	h := newHome(t)
	s := h.server(sshtest.Options{})
	h.knownHosts(s.KnownHosts(""))
	c := dial(t, h.dialer(), sshconn.Target{Host: s.Addr()})
	port := sshtest.Echo(t)
	conns := make([]net.Conn, sshconn.MaxTCPConns)
	for i := range conns {
		conn, err := c.DialTCP(t.Context(), "127.0.0.1", port)
		if err != nil {
			t.Fatal(err)
		}
		conns[i] = conn
	}
	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if _, err := c.DialTCP(ctx, "127.0.0.1", port); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a dial past the limit gave %v", err)
	}
	type result struct {
		conn net.Conn
		err  error
	}
	dialed := make(chan result, 1)
	go func() {
		conn, err := c.DialTCP(t.Context(), "127.0.0.1", port)
		dialed <- result{conn, err}
	}()
	select {
	case r := <-dialed:
		t.Fatalf("a dial past the limit ended with %v before a connection closed", r.err)
	case <-time.After(100 * time.Millisecond):
	}
	_ = conns[0].Close()
	select {
	case r := <-dialed:
		if r.err != nil {
			t.Fatalf("once a connection closed, the dial gave %v", r.err)
		}
		ping(t, r.conn)
		_ = r.conn.Close()
	case <-time.After(10 * time.Second):
		t.Fatal("the dial waited on after a connection closed")
	}
}
