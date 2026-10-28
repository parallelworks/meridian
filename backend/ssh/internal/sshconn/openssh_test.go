package sshconn_test

import (
	"errors"
	"io"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"testing"

	"github.com/parallelworks/meridian/backend/ssh/internal/sshconn"
	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/internal/opensshtest"
	"github.com/parallelworks/meridian/problems"
)

// These tests run OpenSSH's sshd, where opensshtest.EnvSSHD names one, as
// CI's ssh job does, and are skipped otherwise: what sshtest's server
// checks, real OpenSSH checks again.

// openSSH starts an sshd that h's key logs in to, with opts, and returns
// it, with the target that reaches it as the test's user, whom sshd, run
// as that user, logs in as only.
func (h *home) openSSH(opts opensshtest.Options) (*opensshtest.Server, sshconn.Target) {
	h.t.Helper()
	opts.Authorized = append(opts.Authorized, h.key.PublicKey())
	s := opensshtest.New(h.t, opts)
	u, err := user.Current()
	if err != nil {
		h.t.Fatal(err)
	}
	return s, sshconn.Target{Host: s.Addr(), User: u.Username}
}

// TestOpenSSH checks Meridian's connection to OpenSSH's sshd, under FIPS
// 140-only mode: the handshake agrees on what Meridian pins, a command
// runs, and sshd listens on a Unix socket for the connection, which only
// the user can use, and whose connections it forwards.
func TestOpenSSH(t *testing.T) {
	requireSSH(t)
	h := newHome(t)
	s, target := h.openSSH(opensshtest.Options{})
	h.knownHosts(s.KnownHosts(""))
	c := dial(t, h.dialer(), target)
	if out := run(t, c, "echo hello"); out != "hello\n" {
		t.Errorf("the command printed %q", out)
	}
	dir, err := os.MkdirTemp("", "fwd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "c.sock")
	ln, err := c.ListenUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 {
		t.Errorf("sshd's socket is %v, %v; want a socket only its user can use", info, err)
	}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(conn, conn)
	}()
	conn, err := net.Dial("unix", path) //nolint:noctx // the test's, which it ends
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "ping"); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Errorf("the forwarded connection echoed %q, %v", buf, err)
	}
}

// TestOpenSSHForwardingRefused checks that an sshd that forwards no Unix
// sockets (AllowStreamLocalForwarding no) is ErrForwardingRefused, and
// that the connection stays up.
func TestOpenSSHForwardingRefused(t *testing.T) {
	requireSSH(t)
	h := newHome(t)
	s, target := h.openSSH(opensshtest.Options{NoForwarding: true})
	h.knownHosts(s.KnownHosts(""))
	c := dial(t, h.dialer(), target)
	dir, err := os.MkdirTemp("", "fwd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if _, err := c.ListenUnix(filepath.Join(dir, "c.sock")); !errors.Is(err, sshconn.ErrForwardingRefused) {
		t.Errorf("an sshd that forwards no sockets gave %v, want ErrForwardingRefused", err)
	}
	if out := run(t, c, "echo up"); out != "up\n" {
		t.Errorf("after the refusal, the command printed %q", out)
	}
}

// TestOpenSSHUnapproved checks that an sshd that offers only
// chacha20-poly1305 and curve25519, which FIPS 140-3 doesn't approve, is
// ssh_algorithms_unapproved, for the key exchange it offers.
func TestOpenSSHUnapproved(t *testing.T) {
	requireSSH(t)
	h := newHome(t)
	s, target := h.openSSH(opensshtest.Options{
		KeyExchanges: []string{"curve25519-sha256", "curve25519-sha256@libssh.org"},
		Ciphers:      []string{"chacha20-poly1305@openssh.com"},
	})
	h.knownHosts(s.KnownHosts(""))
	_, err := h.dialer().Dial(t.Context(), target)
	p := kindtest.Want(t, err, problems.SSHAlgorithmsUnapproved).Params
	if p["category"] != "key-exchange" || p["offered"] != "curve25519-sha256, curve25519-sha256@libssh.org" || p["host"] != s.Addr() {
		t.Errorf("the problem's params are %v", p)
	}
}
