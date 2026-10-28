package opensshtest_test

import (
	"errors"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/parallelworks/meridian/internal/opensshtest"
	"github.com/parallelworks/meridian/internal/sshtest"
)

// TestServer checks the sshd a test runs: a client with a key it accepts
// logs in, checking its host key against the known_hosts line it gives; a
// session has the environment it sets, HOME over sshd's own; and Drop
// ends the connection, while sshd goes on taking others.
func TestServer(t *testing.T) {
	signer := sshtest.NewKey(t)
	home := t.TempDir()
	s := opensshtest.New(t, opensshtest.Options{
		Authorized: []ssh.PublicKey{signer.PublicKey()},
		Env:        []string{"HOME=" + home, "MERIDIAN_TEST_VALUE=a value"},
	})
	c := dial(t, s, signer)
	out := run(t, c, `printf '%s|%s' "$HOME" "$MERIDIAN_TEST_VALUE"`)
	if want := home + "|a value"; out != want {
		t.Errorf("the session's environment gives %q, want %q", out, want)
	}
	s.Drop()
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the connection outlived Drop")
	}
	if out := run(t, dial(t, s, signer), "echo again"); out != "again\n" {
		t.Errorf("after Drop, a new connection's session gives %q", out)
	}
}

// config is a client's config for s, logging in with signer.
func config(t *testing.T, s *opensshtest.Server, signer ssh.Signer) *ssh.ClientConfig {
	t.Helper()
	file := filepath.Join(t.TempDir(), "known_hosts")
	if err := os.WriteFile(file, []byte(s.KnownHosts("")), 0o600); err != nil {
		t.Fatal(err)
	}
	check, err := knownhosts.New(file)
	if err != nil {
		t.Fatal(err)
	}
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	// What FIPS 140-only mode runs, which x/crypto's defaults aren't all.
	c := &ssh.ClientConfig{
		User:              u.Username,
		Auth:              []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback:   check,
		HostKeyAlgorithms: []string{ssh.KeyAlgoECDSA256},
		Timeout:           10 * time.Second,
	}
	c.KeyExchanges = []string{ssh.KeyExchangeECDHP256}
	c.Ciphers = []string{ssh.CipherAES128CTR}
	c.MACs = []string{ssh.HMACSHA256ETM}
	return c
}

// dial connects to s, logging in with signer.
func dial(t *testing.T, s *opensshtest.Server, signer ssh.Signer) *ssh.Client {
	t.Helper()
	c, err := ssh.Dial("tcp", s.Addr(), config(t, s, signer))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// run runs command in a session of c, and returns what it printed.
func run(t *testing.T, c *ssh.Client, command string) string {
	t.Helper()
	session, err := c.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	out, err := session.Output(command)
	var exit *ssh.ExitError
	if err != nil && !errors.As(err, &exit) && !errors.Is(err, io.EOF) {
		t.Fatalf("%s: %v", command, err)
	}
	return string(out)
}
