package sshconn_test

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/fips140"
	"crypto/rand"
	"crypto/rsa"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode"

	"github.com/parallelworks/foundation/problem"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/parallelworks/meridian/backend/ssh/internal/sshconn"
	"github.com/parallelworks/meridian/internal/sshtest"
	"github.com/parallelworks/meridian/problems"
)

// proxyEnv, set, runs the test binary as a ProxyCommand: it connects to
// the host and port its arguments name, and copies between them and its
// standard input and output, as nc does.
const proxyEnv = "SSHCONN_TEST_PROXY"

func TestMain(m *testing.M) {
	if os.Getenv(proxyEnv) == "1" {
		c, err := net.Dial("tcp", net.JoinHostPort(os.Args[1], os.Args[2]))
		if err != nil {
			os.Exit(1)
		}
		go func() { _, _ = io.Copy(c, os.Stdin) }()
		_, _ = io.Copy(os.Stdout, c)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// A home is a user's home directory for a test, with a key pair in
// ~/.ssh/id_ecdsa, known_hosts beside it, and a config, which only a
// Dialer of its own reads.
type home struct {
	t      *testing.T
	dir    string
	key    ssh.Signer
	config []string
}

func newHome(t *testing.T) *home {
	t.Helper()
	h := &home{t: t, dir: t.TempDir()}
	if err := os.Mkdir(filepath.Join(h.dir, ".ssh"), 0o700); err != nil {
		t.Fatal(err)
	}
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	h.key = h.writeKey("id_ecdsa", k)
	return h
}

// path is a path in the home directory.
func (h *home) path(elem ...string) string { return filepath.Join(append([]string{h.dir}, elem...)...) }

// writeKey writes key as the private key file name in ~/.ssh, with its
// .pub beside it, and returns it.
func (h *home) writeKey(name string, key crypto.Signer) ssh.Signer {
	h.t.Helper()
	block, err := ssh.MarshalPrivateKey(key, "")
	if err != nil {
		h.t.Fatal(err)
	}
	h.write(h.path(".ssh", name), pem.EncodeToMemory(block))
	s, err := ssh.NewSignerFromKey(key)
	if err != nil {
		h.t.Fatal(err)
	}
	h.write(h.path(".ssh", name+".pub"), ssh.MarshalAuthorizedKey(s.PublicKey()))
	return s
}

func (h *home) write(path string, data []byte) {
	h.t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		h.t.Fatal(err)
	}
}

// knownHosts writes ~/.ssh/known_hosts.
func (h *home) knownHosts(lines ...string) {
	h.write(h.path(".ssh", "known_hosts"), []byte(strings.Join(lines, "")))
}

// dialer returns a Dialer of the home's: its config, with the known_hosts
// files the home's, its key files and no agent, unless the config names
// them, and none of the system's.
func (h *home) dialer() *sshconn.Dialer {
	config := strings.Join(h.config, "\n") + fmt.Sprintf(`
Host *
  UserKnownHostsFile %s
  GlobalKnownHostsFile /dev/null
  IdentityAgent none
`, h.path(".ssh", "known_hosts"))
	file := h.path("config")
	h.write(file, []byte(config))
	return &sshconn.Dialer{ConfigFile: file, Home: h.dir, User: "tester", Getenv: func(string) string { return "" }}
}

// requireSSH skips a test that needs an ssh client to read its config.
func requireSSH(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("no ssh client to read the config with")
	}
}

// server starts a test server that h's key logs in to.
func (h *home) server(opts sshtest.Options) *sshtest.Server {
	h.t.Helper()
	opts.Authorized = append(opts.Authorized, h.key.PublicKey())
	return sshtest.New(h.t, opts)
}

// run runs a command on c, and returns what it wrote.
func run(t *testing.T, c *sshconn.Conn, command string) string {
	t.Helper()
	s, err := c.NewSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	out, err := s.Output(command)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

// dial connects with d to t, and fails the test if it can't.
func dial(t *testing.T, d *sshconn.Dialer, target sshconn.Target) *sshconn.Conn {
	t.Helper()
	c, err := d.Dial(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// paramsOf returns the params of err's problem of kind k, or fails the
// test.
func paramsOf(t *testing.T, err error, k *problems.Kind) problems.Params {
	t.Helper()
	p, ok := errors.AsType[*problem.Problem](err)
	if !ok || p.Code != k.Code() {
		t.Fatalf("got %v, want a problem of %s", err, k.Code())
	}
	return p.Params
}

// TestAlgorithms checks that each algorithm Meridian offers runs under
// FIPS 140-only mode, with the certified module, as it does for users: a
// handshake with a host that offers only that one of its kind, and a
// command run.
func TestAlgorithms(t *testing.T) {
	if !fips140.Enforced() {
		t.Fatal("the tests run in FIPS 140-only mode: go.mod sets GODEBUG=fips140=only")
	}
	h := newHome(t)
	d := h.dialer()
	check := func(t *testing.T, opts sshtest.Options) {
		t.Helper()
		s := h.server(opts)
		h.knownHosts(s.KnownHosts(""))
		if out := run(t, dial(t, d, sshconn.Target{Host: s.Addr()}), "echo ok"); out != "ok\n" {
			t.Errorf("the command wrote %q", out)
		}
	}
	for _, kex := range []string{ssh.KeyExchangeECDHP256, ssh.KeyExchangeECDHP384, ssh.KeyExchangeECDHP521} {
		t.Run(kex, func(t *testing.T) { check(t, sshtest.Options{KeyExchanges: []string{kex}}) })
	}
	for _, cipher := range []string{ssh.CipherAES128CTR, ssh.CipherAES192CTR, ssh.CipherAES256CTR} {
		t.Run(cipher, func(t *testing.T) { check(t, sshtest.Options{Ciphers: []string{cipher}}) })
	}
	for _, mac := range []string{ssh.HMACSHA256ETM, ssh.HMACSHA512ETM, ssh.HMACSHA256, ssh.HMACSHA512} {
		t.Run(mac, func(t *testing.T) { check(t, sshtest.Options{MACs: []string{mac}}) })
	}
	for _, tc := range []struct {
		name string
		key  func() crypto.Signer
		alg  string
	}{
		{"ecdsa-p256", func() crypto.Signer { return ecdsaKey(t, elliptic.P256()) }, ""},
		{"ecdsa-p384", func() crypto.Signer { return ecdsaKey(t, elliptic.P384()) }, ""},
		{"ecdsa-p521", func() crypto.Signer { return ecdsaKey(t, elliptic.P521()) }, ""},
		{"ed25519", func() crypto.Signer { _, k, _ := ed25519.GenerateKey(rand.Reader); return k }, ""},
		{"rsa-sha2-512", func() crypto.Signer { return rsaKey(t) }, ssh.KeyAlgoRSASHA512},
		{"rsa-sha2-256", func() crypto.Signer { return rsaKey(t) }, ssh.KeyAlgoRSASHA256},
	} {
		t.Run("host key "+tc.name, func(t *testing.T) {
			s, err := ssh.NewSignerFromKey(tc.key())
			if err != nil {
				t.Fatal(err)
			}
			if tc.alg != "" {
				if s, err = ssh.NewSignerWithAlgorithms(s.(ssh.AlgorithmSigner), []string{tc.alg}); err != nil {
					t.Fatal(err)
				}
			}
			check(t, sshtest.Options{HostKeys: []ssh.Signer{s}})
		})
		if tc.alg == ssh.KeyAlgoRSASHA256 {
			// An RSA user key signs with the algorithm the host
			// prefers, as the rsa-sha2-512 case's does.
			continue
		}
		t.Run("user key "+tc.name, func(t *testing.T) {
			uh := newHome(t)
			uh.key = uh.writeKey("id_ecdsa", tc.key())
			s := uh.server(sshtest.Options{})
			uh.knownHosts(s.KnownHosts(""))
			if out := run(t, dial(t, uh.dialer(), sshconn.Target{Host: s.Addr()}), "echo ok"); out != "ok\n" {
				t.Errorf("the command wrote %q", out)
			}
		})
	}
	t.Run("host certificate", func(t *testing.T) {
		ca := sshtest.NewKey(t)
		hostKey := sshtest.NewKey(t)
		cert := &ssh.Certificate{Key: hostKey.PublicKey(), CertType: ssh.HostCert, ValidPrincipals: []string{"127.0.0.1"}, ValidBefore: ssh.CertTimeInfinity}
		if err := cert.SignCert(rand.Reader, ca); err != nil {
			t.Fatal(err)
		}
		certSigner, err := ssh.NewCertSigner(cert, hostKey)
		if err != nil {
			t.Fatal(err)
		}
		s := h.server(sshtest.Options{HostKeys: []ssh.Signer{certSigner}})
		h.knownHosts("@cert-authority * " + string(ssh.MarshalAuthorizedKey(ca.PublicKey())))
		if out := run(t, dial(t, d, sshconn.Target{Host: s.Addr()}), "echo ok"); out != "ok\n" {
			t.Errorf("the command wrote %q", out)
		}
	})
}

func ecdsaKey(t *testing.T, curve elliptic.Curve) crypto.Signer {
	t.Helper()
	k, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func rsaKey(t *testing.T) crypto.Signer {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// TestUnapproved checks that a host that offers none of the algorithms
// Meridian may run of one kind is a problem, naming the kind and what the
// host offers, rather than a connection made with another.
func TestUnapproved(t *testing.T) {
	approved := sshtest.Algorithms{
		KeyExchanges: []string{ssh.KeyExchangeECDHP256}, HostKeys: []string{ssh.KeyAlgoECDSA256},
		Ciphers: []string{ssh.CipherAES128CTR}, MACs: []string{ssh.HMACSHA256},
	}
	for _, tc := range []struct {
		category string
		offer    func(a *sshtest.Algorithms)
		offered  string
	}{
		// OpenSSH's markers among its key exchanges, which aren't
		// algorithms, aren't shown.
		{"key-exchange", func(a *sshtest.Algorithms) {
			a.KeyExchanges = []string{ssh.KeyExchangeCurve25519, "sntrup761x25519-sha512", "ext-info-s", "kex-strict-s-v00@openssh.com"}
			a.Ciphers = []string{ssh.CipherChaCha20Poly1305}
		}, "curve25519-sha256, sntrup761x25519-sha512"},
		{"host-key", func(a *sshtest.Algorithms) { a.HostKeys = []string{ssh.KeyAlgoRSA} }, "ssh-rsa"},
		{"cipher", func(a *sshtest.Algorithms) {
			a.Ciphers = []string{ssh.CipherChaCha20Poly1305}
		}, "chacha20-poly1305@openssh.com"},
		{"mac", func(a *sshtest.Algorithms) { a.MACs = []string{"umac-128-etm@openssh.com"} }, "umac-128-etm@openssh.com"},
	} {
		t.Run(tc.category, func(t *testing.T) {
			algs := approved
			tc.offer(&algs)
			addr := sshtest.Offering(t, algs)
			h := newHome(t)
			_, err := h.dialer().Dial(t.Context(), sshconn.Target{Host: addr})
			p := paramsOf(t, err, problems.SSHAlgorithmsUnapproved)
			if p["category"] != tc.category || p["offered"] != tc.offered || p["host"] != addr {
				t.Errorf("the problem's params are %v", p)
			}
		})
	}
}

// TestHostKeys checks a host's key against known_hosts: one it doesn't
// have, one that differs from the one it has, one it revokes, and one of a
// type it has none of for the host, which is unknown; and that the type it
// has is the one asked for, under a host's HostKeyAlias, or a hashed name.
func TestHostKeys(t *testing.T) {
	h := newHome(t)
	ed := func() ssh.Signer {
		_, k, _ := ed25519.GenerateKey(rand.Reader)
		s, _ := ssh.NewSignerFromKey(k)
		return s
	}()
	ec := sshtest.NewKey(t)
	s := h.server(sshtest.Options{HostKeys: []ssh.Signer{ec}})
	other := sshtest.NewKey(t)
	line := func(name string, k ssh.PublicKey) string {
		return knownhosts.Line([]string{knownhosts.Normalize(name)}, k) + "\n"
	}
	target := sshconn.Target{Host: s.Addr()}

	t.Run("unknown", func(t *testing.T) {
		h.knownHosts()
		_, err := h.dialer().Dial(t.Context(), target)
		p := paramsOf(t, err, problems.SSHHostKeyUnknown)
		if p["type"] != ssh.KeyAlgoECDSA256 || p["fingerprint"] != ssh.FingerprintSHA256(ec.PublicKey()) || p["host"] != s.Addr() {
			t.Errorf("the problem's params are %v", p)
		}
	})
	t.Run("ignores StrictHostKeyChecking", func(t *testing.T) {
		requireSSH(t)
		h.knownHosts()
		h.config = []string{"Host *\n  StrictHostKeyChecking no"}
		defer func() { h.config = nil }()
		_, err := h.dialer().Dial(t.Context(), target)
		paramsOf(t, err, problems.SSHHostKeyUnknown)
	})
	t.Run("changed", func(t *testing.T) {
		h.knownHosts("# a comment\n", line(s.Addr(), other.PublicKey()))
		_, err := h.dialer().Dial(t.Context(), target)
		p := paramsOf(t, err, problems.SSHHostKeyChanged)
		if p["file"] != h.path(".ssh", "known_hosts") || p["line"] != 2 || p["fingerprint"] != ssh.FingerprintSHA256(ec.PublicKey()) {
			t.Errorf("the problem's params are %v", p)
		}
	})
	t.Run("another type", func(t *testing.T) {
		h.knownHosts(line(s.Addr(), ed.PublicKey()))
		_, err := h.dialer().Dial(t.Context(), target)
		paramsOf(t, err, problems.SSHHostKeyUnknown)
	})
	t.Run("revoked", func(t *testing.T) {
		h.knownHosts(line(s.Addr(), ec.PublicKey()), "@revoked * "+string(ssh.MarshalAuthorizedKey(ec.PublicKey())))
		_, err := h.dialer().Dial(t.Context(), target)
		paramsOf(t, err, problems.SSHHostKeyRevoked)
	})
	t.Run("the type known_hosts has", func(t *testing.T) {
		// The host has both keys; known_hosts has only the one that
		// x/crypto/ssh wouldn't ask for first.
		both := h.server(sshtest.Options{HostKeys: []ssh.Signer{ec, ed}})
		h.knownHosts(line(both.Addr(), ed.PublicKey()))
		dial(t, h.dialer(), sshconn.Target{Host: both.Addr()})
	})
	t.Run("HostKeyAlias", func(t *testing.T) {
		requireSSH(t)
		h.knownHosts(line("farm", ec.PublicKey()))
		h.config = []string{"Host 127.0.0.1\n  HostKeyAlias farm"}
		defer func() { h.config = nil }()
		dial(t, h.dialer(), target)
	})
	t.Run("hashed", func(t *testing.T) {
		// HMAC-SHA1, which hashes them, isn't for FIPS 140-only mode:
		// ssh-keygen hashes them, and finds them.
		if _, err := exec.LookPath("ssh-keygen"); err != nil {
			t.Skip("no ssh-keygen")
		}
		hash := func() {
			t.Helper()
			if out, err := exec.Command("ssh-keygen", "-H", "-f", h.path(".ssh", "known_hosts")).CombinedOutput(); err != nil {
				t.Fatalf("ssh-keygen -H: %v: %s", err, out)
			}
		}
		h.knownHosts("# a comment\n", line("other.invalid", other.PublicKey()), line(s.Addr(), ec.PublicKey()))
		hash()
		data, err := os.ReadFile(h.path(".ssh", "known_hosts"))
		if err != nil || strings.Contains(string(data), "127.0.0.1") {
			t.Fatalf("known_hosts isn't hashed: %s, %v", data, err)
		}
		dial(t, h.dialer(), target)
		// Changed, at the hashed line.
		h.knownHosts(line(s.Addr(), other.PublicKey()))
		hash()
		_, err = h.dialer().Dial(t.Context(), target)
		if p := paramsOf(t, err, problems.SSHHostKeyChanged); p["line"] != 1 {
			t.Errorf("the problem's params are %v", p)
		}
	})
	t.Run("wildcards", func(t *testing.T) {
		h.knownHosts("127.0.0.2,!127.0.0.1 "+string(ssh.MarshalAuthorizedKey(other.PublicKey())), "[127.0.0.?]:* "+string(ssh.MarshalAuthorizedKey(ec.PublicKey())))
		dial(t, h.dialer(), target)
		h.knownHosts("[127.0.0.*]:*,![127.0.0.1]:* " + string(ssh.MarshalAuthorizedKey(ec.PublicKey())))
		_, err := h.dialer().Dial(t.Context(), target)
		paramsOf(t, err, problems.SSHHostKeyUnknown)
	})
}

// TestAuth checks how Meridian logs in: with the agent's keys, the key
// files the config names, and the user's other key pairs, unless
// IdentitiesOnly; never with a key file that needs a passphrase; and that
// a refusal names no key.
func TestAuth(t *testing.T) {
	requireSSH(t)
	t.Run("agent", func(t *testing.T) {
		h := newHome(t)
		s := h.server(sshtest.Options{})
		h.knownHosts(s.KnownHosts(""))
		k := ecdsaKey(t, elliptic.P256())
		keyring := agent.NewKeyring()
		if err := keyring.Add(agent.AddedKey{PrivateKey: k}); err != nil {
			t.Fatal(err)
		}
		sock := serveAgent(t, keyring)
		agentKey, _ := ssh.NewSignerFromKey(k)
		// Only the agent's key logs in.
		s2 := sshtest.New(t, sshtest.Options{Authorized: []ssh.PublicKey{agentKey.PublicKey()}})
		h.knownHosts(s2.KnownHosts(""))
		h.config = []string{"Host *\n  IdentityAgent " + sock}
		d := h.dialer()
		dial(t, d, sshconn.Target{Host: s2.Addr()})
	})
	t.Run("identity file", func(t *testing.T) {
		h := newHome(t)
		named := h.writeKey("named", ecdsaKey(t, elliptic.P384()))
		if err := os.Remove(h.path(".ssh", "named.pub")); err != nil {
			t.Fatal(err)
		}
		s := sshtest.New(t, sshtest.Options{Authorized: []ssh.PublicKey{named.PublicKey()}})
		h.knownHosts(s.KnownHosts(""))
		h.config = []string{"Host *\n  IdentityFile " + h.path(".ssh", "named")}
		dial(t, h.dialer(), sshconn.Target{Host: s.Addr()})
	})
	t.Run("other pairs", func(t *testing.T) {
		h := newHome(t)
		pair := h.writeKey("work", ecdsaKey(t, elliptic.P256()))
		s := sshtest.New(t, sshtest.Options{Authorized: []ssh.PublicKey{pair.PublicKey()}})
		h.knownHosts(s.KnownHosts(""))
		dial(t, h.dialer(), sshconn.Target{Host: s.Addr()})

		h.config = []string{"Host *\n  IdentitiesOnly yes"}
		_, err := h.dialer().Dial(t.Context(), sshconn.Target{Host: s.Addr()})
		p := paramsOf(t, err, problems.SSHAuthFailed)
		// The default key files hold one key, id_ecdsa, which the host
		// doesn't take.
		if p["keys"] != 1 || p["user"] != currentUser(t) || p["host"] != s.Addr() || len(p) != 3 {
			t.Errorf("the problem's params are %v", p)
		}
	})
	t.Run("passphrase", func(t *testing.T) {
		h := newHome(t)
		block, err := ssh.MarshalPrivateKeyWithPassphrase(ecdsaKey(t, elliptic.P256()), "", []byte("secret"))
		if err != nil {
			t.Fatal(err)
		}
		h.write(h.path(".ssh", "id_ecdsa"), pem.EncodeToMemory(block))
		s := h.server(sshtest.Options{})
		h.knownHosts(s.KnownHosts(""))
		_, err = h.dialer().Dial(t.Context(), sshconn.Target{Host: s.Addr()})
		if p := paramsOf(t, err, problems.SSHAuthFailed); p["keys"] != 0 {
			t.Errorf("a key that needs a passphrase was offered: %v", p)
		}
	})
}

// currentUser is the user ssh -G gives when the config names none: the
// process's.
func currentUser(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("id", "-un").Output()
	if err != nil {
		t.Skip("no id command")
	}
	return strings.TrimSpace(string(out))
}

// serveAgent serves keyring as an SSH agent on a Unix socket, and returns
// the socket's path.
func serveAgent(t *testing.T, keyring agent.Agent) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "agent")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "a.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_ = agent.ServeAgent(keyring, c)
			}()
		}
	}()
	return sock
}

// TestConfig checks that the user's config says where and as whom
// Meridian connects, a host:port taken as it is, and that a config ssh
// can't read, or no ssh client, is reported.
func TestConfig(t *testing.T) {
	t.Run("alias", func(t *testing.T) {
		requireSSH(t)
		h := newHome(t)
		s := h.server(sshtest.Options{})
		h.knownHosts(s.KnownHosts(""))
		h.config = []string{fmt.Sprintf("Host farm\n  HostName %s\n  Port %s\n  User alice", s.Host(), s.Port())}
		d := h.dialer()
		dial(t, d, sshconn.Target{Host: "farm"})
		dial(t, d, sshconn.Target{Host: "farm", User: "bob"})
		if got := s.Logins(); !slices.Equal(got, []string{"alice", "bob"}) {
			t.Errorf("the logins were %v", got)
		}
	})
	t.Run("host:port", func(t *testing.T) {
		requireSSH(t)
		h := newHome(t)
		s := h.server(sshtest.Options{})
		h.knownHosts(s.KnownHosts(""))
		// What the config says of the host's name, its HostName and its
		// jumps, doesn't apply to host:port.
		h.config = []string{"Host 127.0.0.1\n  HostName nowhere.invalid\n  ProxyJump nowhere.invalid"}
		dial(t, h.dialer(), sshconn.Target{Host: s.Addr()})
	})
	t.Run("unreadable", func(t *testing.T) {
		requireSSH(t)
		h := newHome(t)
		h.config = []string{"NoSuchOption yes"}
		_, err := h.dialer().Dial(t.Context(), sshconn.Target{Host: "farm"})
		if p := paramsOf(t, err, problems.SSHConfigUnreadable); !strings.Contains(fmt.Sprint(p["reason"]), "nosuchoption") {
			t.Errorf("the reason is %q", p["reason"])
		}
	})
	t.Run("no ssh client", func(t *testing.T) {
		h := newHome(t)
		s := h.server(sshtest.Options{})
		h.knownHosts(s.KnownHosts(""))
		d := h.dialer()
		d.SSH = filepath.Join(h.dir, "no-ssh")
		// A config that there is no ssh client to read isn't ignored: it
		// could revoke the host's key.
		_, err := d.Dial(t.Context(), sshconn.Target{Host: s.Addr()})
		if p := paramsOf(t, err, problems.SSHClientMissing); p["path"] != d.ConfigFile || p["host"] != s.Host() {
			t.Errorf("the problem's params are %v", p)
		}
		// Without one, OpenSSH's defaults apply, with a note.
		d.ConfigFile = h.path("no-config")
		c := dial(t, d, sshconn.Target{Host: s.Addr()})
		notes := c.Notes()
		if len(notes) != 1 || notes[0].Code != problems.SSHConfigNotRead.Code() {
			t.Errorf("the notes are %v", notes)
		}
		if got := s.Logins(); !slices.Equal(got, []string{"tester"}) {
			t.Errorf("the logins were %v, want the local user's", got)
		}
	})
	t.Run("KnownHostsCommand", func(t *testing.T) {
		requireSSH(t)
		h := newHome(t)
		s := h.server(sshtest.Options{})
		h.knownHosts(s.KnownHosts(""))
		h.config = []string{"Host *\n  KnownHostsCommand /bin/true %H"}
		_, err := h.dialer().Dial(t.Context(), sshconn.Target{Host: s.Addr()})
		paramsOf(t, err, problems.SSHKnownHostsCommand)
	})
}

// TestJumps checks connecting through other hosts: a format 1 jump host,
// a ProxyJump of two hosts, a ProxyCommand, each host's key checked
// against known_hosts, and ProxyJumps that lead back to themselves.
func TestJumps(t *testing.T) {
	t.Run("jump host", func(t *testing.T) {
		requireSSH(t)
		h := newHome(t)
		j, s := h.server(sshtest.Options{}), h.server(sshtest.Options{})
		h.knownHosts(j.KnownHosts(""), s.KnownHosts(""))
		dial(t, h.dialer(), sshconn.Target{Host: s.Addr(), JumpHost: j.Addr(), JumpUser: "carol"})
		if got := j.Logins(); !slices.Equal(got, []string{"carol"}) {
			t.Errorf("the jump host's logins were %v", got)
		}
		if got := s.Logins(); len(got) != 1 {
			t.Errorf("the host's logins were %v", got)
		}
		// The jump host's key is checked against its own entry.
		h.knownHosts(s.KnownHosts(""))
		_, err := h.dialer().Dial(t.Context(), sshconn.Target{Host: s.Addr(), JumpHost: j.Addr()})
		if p := paramsOf(t, err, problems.SSHHostKeyUnknown); p["host"] != j.Host() {
			t.Errorf("the problem names %v, want the jump host", p["host"])
		}
	})
	t.Run("ProxyJump", func(t *testing.T) {
		requireSSH(t)
		h := newHome(t)
		j1, j2, s := h.server(sshtest.Options{}), h.server(sshtest.Options{}), h.server(sshtest.Options{})
		h.knownHosts(j1.KnownHosts(""), j2.KnownHosts(""), s.KnownHosts(""))
		h.config = []string{fmt.Sprintf(`Host target
  HostName 127.0.0.1
  Port %s
  ProxyJump j1,j2
Host j1
  HostName 127.0.0.1
  Port %s
Host j2
  HostName 127.0.0.1
  Port %s`, s.Port(), j1.Port(), j2.Port())}
		dial(t, h.dialer(), sshconn.Target{Host: "target"})
		for name, srv := range map[string]*sshtest.Server{"j1": j1, "j2": j2, "target": s} {
			if len(srv.Logins()) != 1 {
				t.Errorf("%s's logins were %v", name, srv.Logins())
			}
		}
	})
	t.Run("ProxyJump loop", func(t *testing.T) {
		requireSSH(t)
		h := newHome(t)
		h.config = []string{"Host a\n  ProxyJump b\nHost b\n  ProxyJump a"}
		_, err := h.dialer().Dial(t.Context(), sshconn.Target{Host: "a"})
		paramsOf(t, err, problems.SSHProxyJumpTooDeep)
	})
	t.Run("ProxyCommand", func(t *testing.T) {
		requireSSH(t)
		h := newHome(t)
		s := h.server(sshtest.Options{})
		h.knownHosts(s.KnownHosts(""))
		self, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		h.config = []string{fmt.Sprintf("Host proxied\n  HostName 127.0.0.1\n  Port %s\n  ProxyCommand env %s=1 '%s' %%h %%p", s.Port(), proxyEnv, self)}
		// It runs in the user's shell, as $SHELL -c "exec <command>".
		shell, ran := h.path("shell"), h.path("shell-ran")
		if err := os.WriteFile(shell, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > '"+ran+"'\nexec /bin/sh \"$@\"\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		d := h.dialer()
		d.Getenv = func(name string) string {
			if name == "SHELL" {
				return shell
			}
			return ""
		}
		c := dial(t, d, sshconn.Target{Host: "proxied"})
		if out := run(t, c, "echo ok"); out != "ok\n" {
			t.Errorf("the command wrote %q", out)
		}
		if got, err := os.ReadFile(ran); err != nil || !strings.HasPrefix(string(got), "-c\nexec env ") {
			t.Errorf("the shell was given %q, %v", got, err)
		}
		// A shell another user could change isn't run.
		if err := os.Chmod(shell, 0o777); err != nil { //nolint:gosec // a shell that isn't safe to run, which the test checks isn't run
			t.Fatal(err)
		}
		_, err = d.Dial(t.Context(), sshconn.Target{Host: "proxied"})
		if p := paramsOf(t, err, problems.SSHCommandUnsafe); p["path"] != shell {
			t.Errorf("the problem's params are %v", p)
		}
	})
}

// TestPool checks a pool's connections: one shared for each target, dialed
// again once its host drops it, and not presenting a host key that
// changed meanwhile; and a target that can't be reached dialed again only
// after a backoff.
func TestPool(t *testing.T) {
	h := newHome(t)
	s := h.server(sshtest.Options{})
	h.knownHosts(s.KnownHosts(""))
	p := &sshconn.Pool{Dialer: h.dialer()}
	t.Cleanup(func() { _ = p.Close() })
	target := sshconn.Target{Host: s.Addr()}
	c1, err := p.Get(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := p.Get(t.Context(), target); err != nil || again != c1 {
		t.Errorf("a second Get gave %p, %v; want the same connection", again, err)
	}
	s.Drop()
	waitDone(t, c1)
	c2, err := p.Get(t.Context(), target)
	if err != nil || c2 == c1 {
		t.Fatalf("after a drop, Get gave %p, %v; want a new connection", c2, err)
	}
	if out := run(t, c2, "echo ok"); out != "ok\n" {
		t.Errorf("the command wrote %q", out)
	}

	s.SetHostKeys(sshtest.NewKey(t))
	s.Drop()
	waitDone(t, c2)
	_, err = p.Get(t.Context(), target)
	paramsOf(t, err, problems.SSHHostKeyChanged)

	// A failure is followed by a backoff, within which a Get answers at
	// once, without dialing, with the last dial's error.
	start := time.Now()
	_, err = p.Get(t.Context(), target)
	if de, ok := errors.AsType[*sshconn.DialError](err); !ok || !de.Waiting || de.Failures != 1 || time.Since(start) > time.Second {
		t.Errorf("a Get within the backoff gave %v after %v", err, time.Since(start))
	}
	paramsOf(t, err, problems.SSHHostKeyChanged)
}

// TestPoolAnswersAtOnce checks that a pool doesn't keep its users waiting
// on a host that doesn't answer: a Get while another dials the host
// answers at once that it is being dialed, and one whose context ends
// before its own dial does returns then, while the dial goes on, and is
// counted when it fails, as the next Get, within the backoff after it,
// answers at once too.
func TestPoolAnswersAtOnce(t *testing.T) {
	h := newHome(t)
	s := h.server(sshtest.Options{})
	h.knownHosts(s.KnownHosts(""))
	d := h.dialer()
	d.ConnectTimeout = time.Second
	p := &sshconn.Pool{Dialer: d}
	t.Cleanup(func() { _ = p.Close() })
	target := sshconn.Target{Host: s.Addr()}
	s.Stall()
	defer s.Resume()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := p.Get(ctx, target); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Errorf("a Get whose context ended as it dialed gave %v after %v", err, time.Since(start))
	}
	start = time.Now()
	_, err := p.Get(t.Context(), target)
	if de, ok := errors.AsType[*sshconn.DialError](err); !ok || !de.Waiting || de.Failures != 0 || !errors.Is(err, sshconn.ErrDialing) || time.Since(start) > time.Second {
		t.Errorf("a Get while the host is being dialed gave %v after %v", err, time.Since(start))
	}
	// The dial goes on until the handshake's timeout, and fails.
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := p.Get(t.Context(), target)
		de, ok := errors.AsType[*sshconn.DialError](err)
		if !ok {
			t.Fatalf("a Get gave %v", err)
		}
		if de.Failures == 1 {
			if !de.Waiting || errors.Is(err, sshconn.ErrDialing) {
				t.Errorf("a Get within the backoff gave %+v", de)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the dial wasn't counted")
		}
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo // the dial in the background, which nothing signals
	}
}

// hangingSSH writes an ssh client to a directory of the test's whose -G
// never answers, as a config's Match exec that hangs, and returns a Dialer
// that reads the config with it, whose connect timeout is a second.
func hangingSSH(t *testing.T) *sshconn.Dialer {
	t.Helper()
	dir := t.TempDir()
	hang := filepath.Join(dir, "ssh")
	if err := os.WriteFile(hang, []byte("#!/bin/sh\nexec sleep 60\n"), 0o700); err != nil { //nolint:gosec // the test's ssh, which hangs
		t.Fatal(err)
	}
	return &sshconn.Dialer{SSH: hang, Home: dir, User: "tester", Getenv: func(string) string { return "" }, ConnectTimeout: time.Second}
}

// TestPoolDialTimeout checks that a dial whose ssh -G doesn't end, as a
// config's Match exec that hangs, ends by itself, at twice the connect
// timeout, and counts as a failure: the pool waits to dial the host
// again, rather than answer for good that it is being dialed.
func TestPoolDialTimeout(t *testing.T) {
	p := &sshconn.Pool{Dialer: hangingSSH(t)}
	t.Cleanup(func() { _ = p.Close() })
	target := sshconn.Target{Host: "farm"}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := p.Get(ctx, target); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a Get whose context ended as ssh -G hung gave %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := p.Get(t.Context(), target)
		de, ok := errors.AsType[*sshconn.DialError](err)
		if !ok {
			t.Fatalf("a Get gave %v", err)
		}
		if de.Failures == 1 {
			if de.Failed != 1 || !de.Waiting || de.RetryAt.IsZero() {
				t.Errorf("a Get after the dial failed gave %+v", de)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a dial whose ssh -G hung never ended")
		}
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo // the dial in the background, which nothing signals
	}
}

// TestDialTimeout checks that a dial whose jump host stalls once its own
// handshake is done, never answering the request to forward a connection
// to the host, which nothing else bounds, ends at the Dialer's
// DialTimeout.
func TestDialTimeout(t *testing.T) {
	requireSSH(t)
	h := newHome(t)
	j, s := h.server(sshtest.Options{HoldForwards: true}), h.server(sshtest.Options{})
	h.knownHosts(j.KnownHosts(""), s.KnownHosts(""))
	d := h.dialer()
	d.DialTimeout = 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	start := time.Now()
	_, err := d.Dial(ctx, sshconn.Target{Host: s.Addr(), JumpHost: j.Addr()})
	if took := time.Since(start); err == nil || took > 3*time.Second || ctx.Err() != nil {
		t.Errorf("a dial through a jump host that stalls gave %v after %v", err, took)
	}
}

// TestPoolCloseWaits checks that a pool's Close ends the dials going on,
// as one whose ssh -G hangs, and returns once they have ended.
func TestPoolCloseWaits(t *testing.T) {
	p := &sshconn.Pool{Dialer: hangingSSH(t)}
	var dialed atomic.Bool
	sshconn.SetDialed(p, func() { dialed.Store(true) })
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := p.Get(ctx, sshconn.Target{Host: "farm"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a Get whose context ended as ssh -G hung gave %v", err)
	}
	start := time.Now()
	_ = p.Close()
	if !dialed.Load() || time.Since(start) > time.Second {
		t.Errorf("Close returned after %v, the dial ended %v", time.Since(start), dialed.Load())
	}
}

// waitDone waits for c to close.
func waitDone(t *testing.T, c *sshconn.Conn) {
	t.Helper()
	select {
	case <-c.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the connection didn't close")
	}
}

// TestKeepAlive checks that a connection that stops carrying anything,
// without being closed, is closed once a keepalive goes unanswered.
func TestKeepAlive(t *testing.T) {
	h := newHome(t)
	s := h.server(sshtest.Options{})
	h.knownHosts(s.KnownHosts(""))
	d := h.dialer()
	d.KeepAlive, d.KeepAliveTimeout = 50*time.Millisecond, time.Second
	c := dial(t, d, sshconn.Target{Host: s.Addr()})
	time.Sleep(300 * time.Millisecond) //nolint:forbidigo // keepalives answered meanwhile
	if err := c.Err(); err != nil {
		t.Fatalf("a connection whose host answers closed: %v", err)
	}
	s.Stall()
	defer s.Resume()
	waitDone(t, c)
	if !errors.Is(c.Err(), sshconn.ErrNotAlive) {
		t.Errorf("the connection closed with %v", c.Err())
	}
}

// TestSessions checks that a connection opens at most its limit of sessions
// at once, and asks again for a session its host refused, until its
// context ends.
func TestSessions(t *testing.T) {
	h := newHome(t)
	s := h.server(sshtest.Options{})
	h.knownHosts(s.KnownHosts(""))
	d := h.dialer()
	d.Sessions = 2
	c := dial(t, d, sshconn.Target{Host: s.Addr()})
	a, err := c.NewSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.NewSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := c.NewSession(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a third session gave %v, want to wait", err)
	}
	_ = a.Close()
	third, err := c.NewSession(t.Context())
	if err != nil {
		t.Fatalf("a session once one closed: %v", err)
	}
	_ = third.Close()
	_ = b.Close()

	s.RefuseSessions(2)
	if out := run(t, c, "echo ok"); out != "ok\n" {
		t.Errorf("the command wrote %q", out)
	}
	s.RefuseSessions(-1)
	ctx, cancel = context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if _, err := c.NewSession(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a session the host keeps refusing gave %v", err)
	}
	if c.Err() != nil {
		t.Errorf("refused sessions closed the connection: %v", c.Err())
	}
}

// TestSessionUnanswered checks that a session the host doesn't answer is
// waited for only until ctx is done, or the connection closes, however
// long the keepalives give it: x/crypto/ssh waits for the answer with no
// bound.
func TestSessionUnanswered(t *testing.T) {
	h := newHome(t)
	s := h.server(sshtest.Options{})
	h.knownHosts(s.KnownHosts(""))
	d := h.dialer()
	d.KeepAlive, d.KeepAliveTimeout = time.Hour, time.Hour
	c := dial(t, d, sshconn.Target{Host: s.Addr()})
	s.Stall()
	defer s.Resume()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := c.NewSession(ctx); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 10*time.Second {
		t.Errorf("a session the host doesn't answer gave %v after %v", err, time.Since(start))
	}
	done := make(chan error, 1)
	go func() {
		_, err := c.NewSession(context.WithoutCancel(t.Context()))
		done <- err
	}()
	_ = c.Close()
	select {
	case err := <-done:
		if !errors.Is(err, sshconn.ErrClosed) {
			t.Errorf("a session on a connection that closed as it waited gave %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a session on a connection that closed as it waited was waited for still")
	}
}

// TestSessionAbandoned checks a session given up on before the host
// answered: once the host answers, the session is closed, and only then
// is its place on the connection given back, so that a connection of one
// place opens the next only once the first is closed, and no session is
// left open.
func TestSessionAbandoned(t *testing.T) {
	h := newHome(t)
	s := h.server(sshtest.Options{})
	h.knownHosts(s.KnownHosts(""))
	d := h.dialer()
	d.Sessions = 1
	d.KeepAlive, d.KeepAliveTimeout = time.Hour, time.Hour
	c := dial(t, d, sshconn.Target{Host: s.Addr()})
	s.Stall()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if _, err := c.NewSession(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a session the host doesn't answer gave %v", err)
	}
	s.Resume()
	next, err := c.NewSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The host reads the first session's close before the next's open, but
	// counts it closed a moment later (sshtest.Server.OpenSessions).
	waitSessions(t, s, 1, "with the next session open")
	_ = next.Close()
	waitSessions(t, s, 0, "once the next session closed")
}

// waitSessions waits, with a deadline, for s to count n sessions open:
// nothing says when it has handled what it read of the client.
func waitSessions(t *testing.T, s *sshtest.Server, n int, when string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for s.OpenSessions() != n {
		if time.Now().After(deadline) {
			t.Fatalf("%s, the host has %d sessions open, want %d", when, s.OpenSessions(), n)
		}
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo // the server's count, which nothing signals
	}
}

// TestListenUnix checks that a host forwards a Unix socket's connections
// over the connection.
func TestListenUnix(t *testing.T) {
	h := newHome(t)
	s := h.server(sshtest.Options{})
	h.knownHosts(s.KnownHosts(""))
	c := dial(t, h.dialer(), sshconn.Target{Host: s.Addr()})
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
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(conn, conn)
	}()
	conn, err := net.Dial("unix", path)
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

// TestListenUnixRefused checks that a host that won't forward a Unix
// socket is ErrForwardingRefused, told by the connection being up rather
// than by x/crypto's words, and that a connection that is closed is not:
// it is an outage, which passes.
func TestListenUnixRefused(t *testing.T) {
	h := newHome(t)
	s := h.server(sshtest.Options{NoForwarding: true})
	h.knownHosts(s.KnownHosts(""))
	c := dial(t, h.dialer(), sshconn.Target{Host: s.Addr()})
	path := filepath.Join(t.TempDir(), "c.sock")
	if _, err := c.ListenUnix(path); !errors.Is(err, sshconn.ErrForwardingRefused) {
		t.Errorf("a host that won't forward gave %v, want ErrForwardingRefused", err)
	}
	if err := c.Err(); err != nil {
		t.Errorf("a refusal closed the connection: %v", err)
	}
	_ = c.Close()
	if _, err := c.ListenUnix(path); err == nil || errors.Is(err, sshconn.ErrForwardingRefused) {
		t.Errorf("a closed connection gave %v, want its own error", err)
	}
}

// fakeSSH writes an ssh client to dir that answers -G as OpenSSH before
// 10.1 can: with the host it is given, and the user -l gives, as they are,
// a newline among them, and then extra, lines of its own. It appends its
// arguments, one to a line, to dir's args, and returns its path.
func fakeSSH(t *testing.T, dir, extra string) string {
	t.Helper()
	fake := filepath.Join(dir, "ssh")
	script := `#!/bin/sh
printf '%s\n' "$@" >> '` + filepath.Join(dir, "args") + `'
user=tester
port=22
while [ $# -gt 0 ]; do
	case "$1" in
	-l) user="$2"; shift ;;
	-p) port="$2"; shift ;;
	-F|-J) shift ;;
	-G|--) ;;
	*) host="$1" ;;
	esac
	shift
done
printf 'host %s\nuser %s\nhostname %s\nport %s\nuserknownhostsfile /dev/null\nglobalknownhostsfile /dev/null\nidentityagent none\n' "$host" "$user" "$host" "$port"
cat <<'EXTRA'
` + extra + `EXTRA
`
	if err := os.WriteFile(fake, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return fake
}

// TestTargetUnsafe checks that a host or a user a workflow names that
// could be read as more than a name is refused before anything runs with
// it, whatever ssh is installed: here one that, as OpenSSH before 10.1
// can, prints what it is given as it is, a user's newline among it, which
// would start a line of the config's, and the host into the user's
// ProxyCommand, whose %n a shell runs. The value is in the problem with
// its control characters escaped, which a terminal would act on.
func TestTargetUnsafe(t *testing.T) {
	dir := t.TempDir()
	pwned := filepath.Join(dir, "pwned")
	fake := fakeSSH(t, dir, "proxycommand true %n\n")
	d := &sshconn.Dialer{SSH: fake, Home: dir, User: "tester", Getenv: func(string) string { return "" }, ConnectTimeout: 2 * time.Second}
	injected := "x\nproxycommand touch " + pwned
	for _, tc := range []struct {
		key    string
		target sshconn.Target
	}{
		{"remoteHost", sshconn.Target{Host: "a;touch " + pwned}},
		{"remoteHost", sshconn.Target{Host: "$(touch " + pwned + ")"}},
		{"remoteHost", sshconn.Target{Host: "-oProxyCommand=touch " + pwned}},
		{"remoteHost", sshconn.Target{Host: injected}},
		{"remoteHost", sshconn.Target{Host: "farm\x1b]0;x\a"}},
		{"remoteUser", sshconn.Target{Host: "farm", User: "a`touch " + pwned + "`"}},
		{"remoteUser", sshconn.Target{Host: "farm", User: injected}},
		{"remoteUser", sshconn.Target{Host: "farm", User: "x\rproxycommand touch " + pwned}},
		{"remoteUser", sshconn.Target{Host: "farm", User: "x\x1b[2J"}},
		{"jumpNodeHost", sshconn.Target{Host: "farm", JumpHost: "j|touch " + pwned}},
		{"jumpNodeHost", sshconn.Target{Host: "farm", JumpHost: injected}},
		{"jumpNodeUser", sshconn.Target{Host: "farm", JumpHost: "j", JumpUser: "u -oProxyCommand=x"}},
		{"jumpNodeUser", sshconn.Target{Host: "farm", JumpHost: "j", JumpUser: injected}},
		{"jumpNodeUser", sshconn.Target{Host: "farm", JumpHost: "j", JumpUser: "u\x00"}},
	} {
		_, err := d.Dial(t.Context(), tc.target)
		p := paramsOf(t, err, problems.SSHTargetUnsafe)
		value := fmt.Sprint(p["value"])
		if p["key"] != tc.key || strings.ContainsFunc(value, unicode.IsControl) {
			t.Errorf("%+v is refused for %v, with the value %q, want %s, its control characters escaped", tc.target, p["key"], value, tc.key)
		}
	}
	if _, err := d.Dial(t.Context(), sshconn.Target{Host: "farm", User: injected}); err == nil {
		t.Fatal("a user with a newline was taken")
	} else if p := paramsOf(t, err, problems.SSHTargetUnsafe); p["value"] != `x\nproxycommand touch `+pwned {
		t.Errorf("the value is %q", p["value"])
	}
	for _, f := range []string{filepath.Join(dir, "args"), pwned} {
		if _, err := os.Stat(f); err == nil {
			t.Errorf("an unsafe target ran something: %s exists", f)
		}
	}
}

// TestHostForms checks that a host given in a form ssh's command line
// takes, but a format 1 ssh: doesn't mean, is refused before anything runs
// with it: a user in it, which ssh -G would take out, and print another
// host than it was asked for; a URL; and a list of jump hosts, which would
// be dialed as one host's name. And that a hop of the config's ProxyJump
// that isn't safe to give ssh, as a target's wouldn't be, is refused.
func TestHostForms(t *testing.T) {
	dir := t.TempDir()
	d := &sshconn.Dialer{SSH: fakeSSH(t, dir, ""), Home: dir, User: "tester", Getenv: func(string) string { return "" }, ConnectTimeout: 2 * time.Second}
	for _, tc := range []struct {
		key    string
		target sshconn.Target
	}{
		{"remoteHost", sshconn.Target{Host: "alice@farm"}},
		{"remoteHost", sshconn.Target{Host: "ssh://alice@farm:2222"}},
		{"remoteHost", sshconn.Target{Host: "ssh://farm"}},
		{"remoteHost", sshconn.Target{Host: "farm,other"}},
		{"jumpNodeHost", sshconn.Target{Host: "farm", JumpHost: "carol@login"}},
		{"jumpNodeHost", sshconn.Target{Host: "farm", JumpHost: "login1,login2"}},
		{"jumpNodeHost", sshconn.Target{Host: "farm", JumpHost: "ssh://login"}},
	} {
		_, err := d.Dial(t.Context(), tc.target)
		if p := paramsOf(t, err, problems.SSHHostFormUnsupported); p["key"] != tc.key {
			t.Errorf("%+v is refused for %v, want %s", tc.target, p["key"], tc.key)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "args")); err == nil {
		t.Error("a host of a form Meridian doesn't take ran ssh")
	}

	d.SSH = fakeSSH(t, dir, "proxyjump ok@j1,a`touch x`@j2\n")
	_, err := d.Dial(t.Context(), sshconn.Target{Host: "farm"})
	if p := paramsOf(t, err, problems.SSHProxyJumpInvalid); p["hop"] != "a`touch x`@j2" {
		t.Errorf("a hop whose user isn't safe gives %v", p)
	}
	d.SSH = fakeSSH(t, dir, "proxyjump j1\x1b[2J\n")
	_, err = d.Dial(t.Context(), sshconn.Target{Host: "farm"})
	if p := paramsOf(t, err, problems.SSHProxyJumpInvalid); p["hop"] != `j1\x1b[2J` {
		t.Errorf("a hop whose host isn't safe gives %v, its control characters escaped", p)
	}
}

// TestUsersGivenSSH checks that ssh -G is given a target's user, as -l,
// once Target.check has checked it, so that ssh reads the config as that
// user's, and its jump host's own resolve its jump user; never the jump
// host as -J, which Meridian connects through itself; and that what it
// prints is taken only as ssh prints it.
func TestUsersGivenSSH(t *testing.T) {
	h := newHome(t)
	j, s := h.server(sshtest.Options{}), h.server(sshtest.Options{})
	h.knownHosts(j.KnownHosts(""), s.KnownHosts(""))
	dir := t.TempDir()
	d := h.dialer()
	d.SSH = fakeSSH(t, dir, "userknownhostsfile "+h.path(".ssh", "known_hosts")+"\n")
	_, err := d.Dial(t.Context(), sshconn.Target{Host: s.Addr(), User: "alice", JumpHost: j.Addr(), JumpUser: "carol"})
	// The fake prints userknownhostsfile twice, which isn't taken.
	if p := paramsOf(t, err, problems.SSHConfigOutputUnexpected); p["keyword"] != "userknownhostsfile" {
		t.Errorf("the problem's params are %v", p)
	}
	if err := os.Remove(filepath.Join(dir, "args")); err != nil {
		t.Fatal(err)
	}
	d.SSH = fakeSSH(t, dir, "")
	_, err = d.Dial(t.Context(), sshconn.Target{Host: s.Addr(), User: "alice", JumpHost: j.Addr(), JumpUser: "carol"})
	// The fake's known_hosts are /dev/null, which knows no host.
	paramsOf(t, err, problems.SSHHostKeyUnknown)
	data, err := os.ReadFile(filepath.Join(dir, "args"))
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Split(strings.TrimSpace(string(data)), "\n")
	want := slices.Concat(
		[]string{"-G", "-F", d.ConfigFile, "-l", "alice", "-p", s.Port(), "--", s.Host()},
		[]string{"-G", "-F", d.ConfigFile, "-l", "carol", "-p", j.Port(), "--", j.Host()},
	)
	if !slices.Equal(args, want) {
		t.Errorf("ssh was given %q, want %q", args, want)
	}
}

// TestCertificates checks host certificates: one an authority known_hosts
// doesn't name is checked as its key, and refused if known_hosts doesn't
// have it; one whose authority known_hosts revokes is refused, named or
// not; and one an authority it names signed, but that has expired, is
// refused, never checked as its key instead, which known_hosts has.
func TestCertificates(t *testing.T) {
	h := newHome(t)
	ca, other := sshtest.NewKey(t), sshtest.NewKey(t)
	hostKey := sshtest.NewKey(t)
	certify := func(before uint64) ssh.Signer {
		cert := &ssh.Certificate{Key: hostKey.PublicKey(), CertType: ssh.HostCert, ValidPrincipals: []string{"127.0.0.1"}, ValidBefore: before}
		if err := cert.SignCert(rand.Reader, ca); err != nil {
			t.Fatal(err)
		}
		s, err := ssh.NewCertSigner(cert, hostKey)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	authority := func(k ssh.Signer) string {
		return "@cert-authority * " + string(ssh.MarshalAuthorizedKey(k.PublicKey()))
	}
	revoked := func(k ssh.Signer) string { return "@revoked * " + string(ssh.MarshalAuthorizedKey(k.PublicKey())) }

	s := h.server(sshtest.Options{HostKeys: []ssh.Signer{certify(ssh.CertTimeInfinity)}})
	target := sshconn.Target{Host: s.Addr()}
	plain := knownhosts.Line([]string{knownhosts.Normalize(s.Addr())}, hostKey.PublicKey()) + "\n"

	h.knownHosts(authority(other))
	_, err := h.dialer().Dial(t.Context(), target)
	paramsOf(t, err, problems.SSHHostKeyUnknown)

	// Signed by an authority known_hosts doesn't name, a certificate is
	// checked as its key, as OpenSSH falls back to it.
	h.knownHosts(authority(other), plain)
	dial(t, h.dialer(), target)

	h.knownHosts(authority(ca), revoked(ca))
	_, err = h.dialer().Dial(t.Context(), target)
	paramsOf(t, err, problems.SSHHostKeyRevoked)
	h.knownHosts(plain, revoked(ca))
	_, err = h.dialer().Dial(t.Context(), target)
	paramsOf(t, err, problems.SSHHostKeyRevoked)

	expired := h.server(sshtest.Options{HostKeys: []ssh.Signer{certify(uint64(time.Now().Add(-time.Hour).Unix()))}})
	h.knownHosts(authority(ca), knownhosts.Line([]string{knownhosts.Normalize(expired.Addr())}, hostKey.PublicKey())+"\n")
	_, err = h.dialer().Dial(t.Context(), sshconn.Target{Host: expired.Addr()})
	paramsOf(t, err, problems.SSHHostCertificateInvalid)
}

// TestHashedUnmatched checks that, where known_hosts has hashed lines and
// no ssh-keygen can match them, no key is checked against the rest: a
// hashed line could revoke the key a plain line has.
func TestHashedUnmatched(t *testing.T) {
	if _, err := exec.LookPath("ssh-keygen"); err != nil {
		t.Skip("no ssh-keygen to hash a line with")
	}
	h := newHome(t)
	s := h.server(sshtest.Options{})
	h.knownHosts(s.KnownHosts("hashed.invalid"))
	if out, err := exec.Command("ssh-keygen", "-H", "-f", h.path(".ssh", "known_hosts")).CombinedOutput(); err != nil {
		t.Fatalf("ssh-keygen -H: %v: %s", err, out)
	}
	hashed, err := os.ReadFile(h.path(".ssh", "known_hosts"))
	if err != nil {
		t.Fatal(err)
	}
	h.knownHosts(string(hashed), s.KnownHosts(""))
	d := h.dialer()
	// No ssh, and so no ssh-keygen beside it, nor a config to read.
	d.SSH, d.ConfigFile = filepath.Join(h.dir, "no-ssh"), h.path("no-config")
	_, err = d.Dial(t.Context(), sshconn.Target{Host: s.Addr()})
	if p := paramsOf(t, err, problems.SSHKnownHostsUnmatched); p["file"] != h.path(".ssh", "known_hosts") {
		t.Errorf("the problem's params are %v", p)
	}
}

// TestPoolClosedWhileDialing checks that a pool closed while it dials
// closes what it dialed, and gets nothing after.
func TestPoolClosedWhileDialing(t *testing.T) {
	h := newHome(t)
	s := h.server(sshtest.Options{})
	h.knownHosts(s.KnownHosts(""))
	p := &sshconn.Pool{Dialer: h.dialer()}
	s.Stall()
	got := make(chan error, 1)
	go func() {
		c, err := p.Get(t.Context(), sshconn.Target{Host: s.Addr()})
		if c != nil {
			err = errors.New("a closed pool gave a connection")
		}
		got <- err
	}()
	time.Sleep(200 * time.Millisecond) //nolint:forbidigo // the dial is held meanwhile
	_ = p.Close()
	s.Resume()
	if err := <-got; !errors.Is(err, sshconn.ErrClosed) {
		t.Errorf("a Get while the pool closed gave %v, want ErrClosed", err)
	}
	if _, err := p.Get(t.Context(), sshconn.Target{Host: s.Addr()}); !errors.Is(err, sshconn.ErrClosed) {
		t.Errorf("a Get after the pool closed gave %v, want ErrClosed", err)
	}
}

// TestRevokedHostKeys checks the config's RevokedHostKeys, as OpenSSH
// reads it: a list of keys, or a KRL, which ssh-keygen -Q checks a key
// against, revoking a host's key, or a certificate's authority; and a
// file that isn't there, or a KRL there is no ssh-keygen to check, which
// revokes every key, since what it revokes can't be known.
func TestRevokedHostKeys(t *testing.T) {
	requireSSH(t)
	if _, err := exec.LookPath("ssh-keygen"); err != nil {
		t.Skip("no ssh-keygen to write a KRL with")
	}
	h := newHome(t)
	hostKey, ca, other := sshtest.NewKey(t), sshtest.NewKey(t), sshtest.NewKey(t)
	s := h.server(sshtest.Options{HostKeys: []ssh.Signer{hostKey}})
	cert := &ssh.Certificate{Key: hostKey.PublicKey(), CertType: ssh.HostCert, ValidPrincipals: []string{"127.0.0.1"}, ValidBefore: ssh.CertTimeInfinity}
	if err := cert.SignCert(rand.Reader, ca); err != nil {
		t.Fatal(err)
	}
	certSigner, err := ssh.NewCertSigner(cert, hostKey)
	if err != nil {
		t.Fatal(err)
	}
	certified := h.server(sshtest.Options{HostKeys: []ssh.Signer{certSigner}})
	h.knownHosts(s.KnownHosts(""), "@cert-authority * "+string(ssh.MarshalAuthorizedKey(ca.PublicKey())))
	revoked := h.path(".ssh", "revoked")
	h.config = []string{"Host *\n  RevokedHostKeys " + revoked}
	list := func(keys ...ssh.Signer) {
		data := []byte("# revoked\n\n")
		for _, k := range keys {
			data = append(data, ssh.MarshalAuthorizedKey(k.PublicKey())...)
		}
		h.write(revoked, data)
	}
	krl := func(k ssh.Signer) {
		pub := h.path("revoke.pub")
		h.write(pub, ssh.MarshalAuthorizedKey(k.PublicKey()))
		_ = os.Remove(revoked)
		if out, err := exec.Command("ssh-keygen", "-k", "-f", revoked, pub).CombinedOutput(); err != nil {
			t.Fatalf("ssh-keygen -k: %v: %s", err, out)
		}
	}
	target, certTarget := sshconn.Target{Host: s.Addr()}, sshconn.Target{Host: certified.Addr()}

	_, err = h.dialer().Dial(t.Context(), target)
	if p := paramsOf(t, err, problems.SSHRevokedHostKeysUnreadable); p["file"] != revoked || p["host"] != s.Addr() {
		t.Errorf("a revoked keys file that isn't there gives %v", p)
	}
	list(other)
	dial(t, h.dialer(), target)
	dial(t, h.dialer(), certTarget)
	list(other, hostKey)
	_, err = h.dialer().Dial(t.Context(), target)
	paramsOf(t, err, problems.SSHHostKeyRevoked)
	_, err = h.dialer().Dial(t.Context(), certTarget)
	paramsOf(t, err, problems.SSHHostKeyRevoked)
	list(ca)
	dial(t, h.dialer(), target)
	_, err = h.dialer().Dial(t.Context(), certTarget)
	paramsOf(t, err, problems.SSHHostKeyRevoked)

	krl(other)
	dial(t, h.dialer(), target)
	krl(hostKey)
	_, err = h.dialer().Dial(t.Context(), target)
	paramsOf(t, err, problems.SSHHostKeyRevoked)
	krl(ca)
	dial(t, h.dialer(), target)
	_, err = h.dialer().Dial(t.Context(), certTarget)
	paramsOf(t, err, problems.SSHHostKeyRevoked)

	// An ssh with no ssh-keygen beside it.
	sshPath, err := exec.LookPath("ssh")
	if err != nil {
		t.Fatal(err)
	}
	alone := filepath.Join(t.TempDir(), "ssh")
	if err := os.Symlink(sshPath, alone); err != nil {
		t.Fatal(err)
	}
	d := h.dialer()
	d.SSH = alone
	_, err = d.Dial(t.Context(), target)
	if p := paramsOf(t, err, problems.SSHRevokedHostKeysUnreadable); p["file"] != revoked {
		t.Errorf("a KRL there is no ssh-keygen to check gives %v", p)
	}
}

// TestCommandUnsafe checks that an ssh client another user could change,
// or that is in a directory another user could change, isn't run, and
// that one a PATH finds is checked as one named is.
func TestCommandUnsafe(t *testing.T) {
	dir := t.TempDir()
	fake := fakeSSH(t, dir, "")
	if err := os.Chmod(fake, 0o722); err != nil { //nolint:gosec // an ssh client that isn't safe to run, which the test checks isn't run
		t.Fatal(err)
	}
	d := &sshconn.Dialer{SSH: fake, Home: dir, User: "tester", Getenv: func(string) string { return "" }}
	_, err := d.Dial(t.Context(), sshconn.Target{Host: "farm"})
	if p := paramsOf(t, err, problems.SSHCommandUnsafe); p["path"] != fake || p["code"] != string(problems.FileChangeable.Code()) {
		t.Errorf("the problem's params are %v", p)
	}
	if err := os.Chmod(fake, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o777); err != nil { //nolint:gosec // a directory another user could change, which the test checks isn't run from
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	t.Setenv("PATH", dir)
	d.SSH = ""
	_, err = d.Dial(t.Context(), sshconn.Target{Host: "farm"})
	if p := paramsOf(t, err, problems.SSHCommandUnsafe); p["path"] != fake {
		t.Errorf("the problem's params are %v", p)
	}
	if _, err := os.Stat(filepath.Join(dir, "args")); err == nil {
		t.Error("an ssh client that isn't safe to run ran")
	}
}
