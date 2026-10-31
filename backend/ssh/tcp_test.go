package ssh

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"
	"golang.org/x/crypto/ssh"

	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/internal/leaktest"
	"github.com/parallelworks/meridian/internal/sshtest"
	"github.com/parallelworks/meridian/problems"
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

// TestDialTCP checks that m's host connects to an address for the driver,
// over the pool's connection to it, which a later dial shares; that the
// connection closes with its context, and with the connection to the host,
// after which a dial connects again; and that none of it leaves a goroutine
// running.
func TestDialTCP(t *testing.T) {
	leaktest.Check(t)
	r := connectRig(t)
	port := sshtest.Echo(t)
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	conn, err := r.d.DialTCP(t.Context(), r.m, "127.0.0.1", port)
	if err != nil {
		t.Fatal(err)
	}
	ping(t, conn)
	ctx, cancel := context.WithCancel(t.Context())
	again, err := r.d.DialTCP(ctx, r.m, "127.0.0.1", port)
	if err != nil {
		t.Fatal(err)
	}
	ping(t, again)
	if got := r.server.Logins(); len(got) != 1 {
		t.Errorf("the host's logins were %v, want one, which both dials share", got)
	}
	if got := r.server.Forwards(); !slices.Equal(got, []string{addr, addr}) {
		t.Errorf("the host connected to %v", got)
	}

	cancel()
	waitClosed(t, again, "once its context ended")
	ping(t, conn)

	r.server.Drop()
	waitClosed(t, conn, "once the connection to the host ended")
	conn, err = r.d.DialTCP(t.Context(), r.m, "127.0.0.1", port)
	if err != nil {
		t.Fatalf("a dial after the connection to the host ended gave %v", err)
	}
	ping(t, conn)
	_ = conn.Close()
}

// TestDialTCPJump checks that a member whose settings name a jump host
// reaches its host through it, and asks its host, not the jump host, to
// connect to the address.
func TestDialTCPJump(t *testing.T) {
	leaktest.Check(t)
	r := connectRig(t)
	j := sshtest.New(t, sshtest.Options{Authorized: []ssh.PublicKey{r.key}})
	write(t, filepath.Join(r.client, ".ssh", "known_hosts"), []byte(r.server.KnownHosts("")+j.KnownHosts("")))
	m := member(t, "ssh", map[string]any{"remoteHost": r.server.Addr(), "jumpNodeHost": j.Addr()})
	port := sshtest.Echo(t)
	conn, err := r.d.DialTCP(t.Context(), m, "127.0.0.1", port)
	if err != nil {
		t.Fatal(err)
	}
	ping(t, conn)
	_ = conn.Close()
	if got := j.Forwards(); !slices.Equal(got, []string{r.server.Addr()}) {
		t.Errorf("the jump host connected to %v, want the host", got)
	}
	if got := r.server.Forwards(); !slices.Equal(got, []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port))}) {
		t.Errorf("the host connected to %v, want the address", got)
	}
}

// TestDialTCPRefused checks that a host whose sshd won't connect to an
// address is ssh_tcp_forwarding_refused, which names the host and the
// address; and that one that couldn't connect is ssh_tcp_connect_failed,
// which names them too, and says what the host said, as it said it.
func TestDialTCPRefused(t *testing.T) {
	leaktest.Check(t)
	r := connectRig(t, func(o *sshtest.Options) { o.RefuseForwards = true })
	_, err := r.d.DialTCP(t.Context(), r.m, "node-1", 8888)
	p := kindtest.Want(t, err, problems.SSHTCPForwardingRefused).Params
	if p["host"] != r.server.Addr() || p["address"] != "node-1:8888" {
		t.Errorf("the problem's params are %v", p)
	}

	open := connectRig(t)
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedPort := ln.Addr().(*net.TCPAddr).Port //nolint:forcetypeassert // a TCP listener's
	_ = ln.Close()
	_, err = open.d.DialTCP(t.Context(), open.m, "127.0.0.1", closedPort)
	p = kindtest.Want(t, err, problems.SSHTCPConnectFailed).Params
	reason, _ := p["reason"].(string)
	if p["host"] != open.server.Addr() || p["address"] != net.JoinHostPort("127.0.0.1", strconv.Itoa(closedPort)) || !strings.Contains(reason, "refused") {
		t.Errorf("an address nothing listens on gave the params %v", p)
	}
}

// TestDialTCPAddress checks which hosts and ports DialTCP asks a host to
// connect to: a name, or an IP address, and a port from 1 to 65535. Any
// other is ssh_tcp_address_invalid, with the host as it was given, which
// its message escapes, before anything connects.
func TestDialTCPAddress(t *testing.T) {
	leaktest.Check(t)
	r := connectRig(t)
	for _, host := range []string{
		"localhost", "node-01", "c1.cluster.example", "login_2.hpc", "Node7", "127.0.0.1", "::1", "fe80::1", "::ffff:127.0.0.1",
		strings.Repeat("a", 63) + "." + strings.Repeat("b", 63),
	} {
		if !TCPHost(host) {
			t.Errorf("the host %q is refused", host)
		}
	}
	for _, tc := range []struct {
		host string
		port int
		want string
	}{
		{"", 80, ""},
		{"-oProxyCommand=x", 80, "-oProxyCommand=x"},
		{"node 1", 80, "node 1"},
		{"node\n1", 80, "node\n1"},
		{"node\x00", 80, "node\x00"},
		{"node.", 80, "node."},
		{".node", 80, ".node"},
		{"a..b", 80, "a..b"},
		{"[::1]", 80, "[::1]"},
		{"node:22", 80, "node:22"},
		{"fe80::1%eth0", 80, "fe80::1%eth0"},
		{"user@node", 80, "user@node"},
		{"nöde", 80, "nöde"},
		{strings.Repeat("a", 64), 80, strings.Repeat("a", 64)},
		{strings.Repeat("a.", 126) + "ab", 80, strings.Repeat("a.", 126) + "ab"},
		{"node", 0, "node"},
		{"node", -1, "node"},
		{"node", 65536, "node"},
	} {
		_, err := r.d.DialTCP(t.Context(), r.m, tc.host, tc.port)
		p := kindtest.Want(t, err, problems.SSHTCPAddressInvalid).Params
		if p["value"] != tc.want || p["port"] != strconv.Itoa(tc.port) {
			t.Errorf("%q port %d: the problem's params are %v", tc.host, tc.port, p)
		}
	}
	// The message escapes the host's control characters.
	_, err := r.d.DialTCP(t.Context(), r.m, "node\n1", 80)
	if d := kindtest.Want(t, err, problems.SSHTCPAddressInvalid).Detail; !strings.Contains(d, `node\n1`) || strings.Contains(d, "\n") {
		t.Errorf("the message is %q", d)
	}
	if got := r.server.Logins(); len(got) != 0 {
		t.Errorf("the driver logged in to the host as %v, for addresses it refuses", got)
	}
	for _, port := range []int{1, 65535} {
		_, err := r.d.DialTCP(t.Context(), r.m, "127.0.0.1", port)
		if p, ok := errors.AsType[*problem.Problem](err); ok && p.Code == problems.SSHTCPAddressInvalid.Code() {
			t.Errorf("port %d is refused: %v", port, err)
		}
	}
}
