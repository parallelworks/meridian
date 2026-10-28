// Package opensshtest runs OpenSSH's sshd for tests: as the test's user, on a
// port of the loopback interface, with a host key, a config and the keys
// it accepts made for the test, so that a test can check Meridian against
// real OpenSSH as well as against sshtest's server in process.
//
// It runs the sshd that MERIDIAN_TEST_SSHD names, by its absolute path, as
// CI's openssh job sets it, on Linux only, since it finds sshd's processes
// in /proc. Without it, or on another system, a test that needs one is
// skipped; with it, an sshd that won't start fails the test.
//
// It runs sshd as the test's user, so without PAM (UsePAM no): its
// sessions have no PAM session, so neither pam_systemd's /run/user and
// user manager, nor logind's KillUserProcesses, which how a worker on a
// host outlives its session depends on. Those it doesn't check.
package opensshtest

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/parallelworks/meridian/internal/sshtest"
)

// EnvSSHD names the sshd to run, by its absolute path, which OpenSSH needs
// to run itself again for each connection.
const EnvSSHD = "MERIDIAN_TEST_SSHD"

// Path returns the sshd EnvSSHD names, or "" where it names none.
func Path() string { return os.Getenv(EnvSSHD) }

// Options are what a Server's sshd offers and accepts.
type Options struct {
	// Authorized are the keys the test's user may log in with.
	Authorized []ssh.PublicKey
	// Env is what sshd sets in each session's environment (SetEnv), over
	// what it sets itself, HOME among them.
	Env []string
	// NoForwarding refuses to forward Unix sockets
	// (AllowStreamLocalForwarding no).
	NoForwarding bool
	// KeyExchanges and Ciphers are what sshd offers (KexAlgorithms,
	// Ciphers), or its defaults for nil.
	KeyExchanges []string
	Ciphers      []string
}

// A Server is an sshd on the loopback interface, which stops when the test
// ends.
type Server struct {
	t       testing.TB
	addr    string
	hostKey ssh.Signer
	cmd     *exec.Cmd
	log     *syncBuffer
}

// New starts sshd with opts, as the test's user, and returns once it
// accepts connections. It skips the test where EnvSSHD names no sshd.
func New(t testing.TB, opts Options) *Server {
	t.Helper()
	sshd := Path()
	switch {
	case sshd == "":
		t.Skipf("%s names no sshd", EnvSSHD)
	case runtime.GOOS != "linux":
		t.Skipf("%s is for Linux, where opensshtest finds sshd's processes in /proc", EnvSSHD)
	case !filepath.IsAbs(sshd):
		t.Fatalf("%s is %q, not an absolute path, which sshd needs to run itself again", EnvSSHD, sshd)
	}
	// Short, for the sockets sshd makes under it, and private, for the
	// host key.
	dir, err := os.MkdirTemp("", "sshd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	key, hostKey := sshtest.NewKeyFile(t)
	file := func(name string, data []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	var authorized bytes.Buffer
	for _, k := range opts.Authorized {
		authorized.Write(ssh.MarshalAuthorizedKey(k))
	}
	addr := freeAddr(t)
	data := configOf(opts, addr, file("host_key", key), file("authorized_keys", authorized.Bytes()), filepath.Join(dir, "sshd.pid"))
	config := file("sshd_config", append(data, "PerSourcePenalties no\n"...))
	// OpenSSH 9.8 and later penalise an address whose connections end
	// without logging in, or whose sessions are killed, as Drop kills
	// them, by refusing its connections for a while, which a test's
	// reconnections mustn't meet; one before 9.8 doesn't know the keyword.
	if out, err := exec.Command(sshd, "-t", "-f", config).CombinedOutput(); err != nil { //nolint:noctx,gosec // the sshd the test was given, checking its config, which ends at once
		if !bytes.Contains(out, []byte("PerSourcePenalties")) {
			t.Fatalf("sshd refuses its config: %v\n%s", err, out)
		}
		config = file("sshd_config", data)
	}
	s := &Server{t: t, addr: addr, hostKey: hostKey, log: &syncBuffer{}}
	// -D keeps it in the foreground, as the test's child, and -e logs to
	// stderr, which the test shows when it fails.
	s.cmd = exec.Command(sshd, "-D", "-e", "-f", config) //nolint:noctx,gosec // the sshd the test was given, stopped as the test ends, below
	s.cmd.Stdout, s.cmd.Stderr = s.log, s.log
	if err := s.cmd.Start(); err != nil {
		t.Fatalf("starting %s: %v", sshd, err)
	}
	exited := make(chan struct{})
	go func() {
		_ = s.cmd.Wait()
		close(exited)
	}()
	t.Cleanup(func() {
		s.kill(s.cmd.Process.Pid)
		<-exited
		if t.Failed() {
			t.Logf("sshd's log:\n%s", s.log.String())
		}
	})
	// It says when it listens; a connection to see whether it does would
	// be one that doesn't log in, which it can penalise.
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(s.log.String(), "Server listening on ") {
		select {
		case <-exited:
			t.Fatalf("sshd stopped as it started:\n%s", s.log.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("sshd doesn't listen on %s:\n%s", addr, s.log.String())
		}
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo // sshd's log, which nothing signals
	}
	return s
}

// configOf is the sshd_config of a Server with opts on addr, with its host
// key, the keys it accepts and its pid file at those paths. It reads no
// system config: nothing but what it says, and sshd's defaults, holds.
func configOf(opts Options, addr, hostKey, authorized, pidFile string) []byte {
	var b bytes.Buffer
	line := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }
	line("ListenAddress %s", addr)
	line("HostKey %s", hostKey)
	line("PidFile %s", pidFile)
	line("AuthorizedKeysFile %s", authorized)
	line("AuthenticationMethods publickey")
	line("PasswordAuthentication no")
	line("KbdInteractiveAuthentication no")
	// As the test's user, sshd can neither use PAM nor check the modes of
	// a temporary directory's parents, which anyone can write to.
	line("UsePAM no")
	line("StrictModes no")
	line("PermitTTY no")
	line("PrintMotd no")
	line("LogLevel VERBOSE")
	if opts.NoForwarding {
		line("AllowStreamLocalForwarding no")
	}
	if len(opts.KeyExchanges) > 0 {
		line("KexAlgorithms %s", strings.Join(opts.KeyExchanges, ","))
	}
	if len(opts.Ciphers) > 0 {
		line("Ciphers %s", strings.Join(opts.Ciphers, ","))
	}
	if len(opts.Env) > 0 {
		// One line: sshd takes the first SetEnv, as it does most of its
		// keywords.
		quoted := make([]string, len(opts.Env))
		for i, kv := range opts.Env {
			quoted[i] = strconv.Quote(kv)
		}
		line("SetEnv %s", strings.Join(quoted, " "))
	}
	return b.Bytes()
}

// freeAddr returns an address of the loopback interface whose port no
// one listens on now, for sshd to.
func freeAddr(t testing.TB) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// Addr is the server's address, as host:port.
func (s *Server) Addr() string { return s.addr }

// Host is its address's host.
func (s *Server) Host() string { h, _, _ := net.SplitHostPort(s.addr); return h }

// Port is its address's port.
func (s *Server) Port() string { _, p, _ := net.SplitHostPort(s.addr); return p }

// KnownHosts returns a known_hosts line for the server's host key, under
// name, or its address for "".
func (s *Server) KnownHosts(name string) string {
	if name == "" {
		name = s.addr
	}
	return sshtest.KnownHostsLine(name, s.hostKey.PublicKey())
}

// Drop ends every connection sshd has, as a network that drops them does:
// it kills each process sshd started, and theirs, which hold the
// connections, but not sshd, which goes on listening. What a session
// started in a session of its own, which its parent left, as a worker
// meridian host places, isn't sshd's any more, and goes on.
func (s *Server) Drop() {
	s.t.Helper()
	pids, err := descendants(s.cmd.Process.Pid)
	if err != nil {
		s.t.Fatal(err)
	}
	if len(pids) == 0 {
		s.t.Fatal("sshd has no connections to drop")
	}
	for _, pid := range pids {
		killPid(pid)
	}
}

// kill kills pid, and what it started.
func (s *Server) kill(pid int) {
	pids, _ := descendants(pid)
	for _, p := range append(pids, pid) {
		killPid(p)
	}
}

// killPid kills the process pid, if it is still there.
func killPid(pid int) {
	if p, err := os.FindProcess(pid); err == nil {
		_ = p.Kill()
	}
}

// descendants returns the processes pid started, and theirs, as /proc
// says, which only Linux has.
func descendants(pid int) ([]int, error) {
	if runtime.GOOS != "linux" {
		return nil, errors.New("opensshtest: finding sshd's processes needs Linux's /proc")
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	children := map[int][]int{}
	for _, e := range entries {
		p, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		stat, err := os.ReadFile(filepath.Join("/proc", e.Name(), "stat"))
		if err != nil {
			continue // It has ended.
		}
		// The name, in parentheses, can hold spaces and parentheses; what
		// follows the last of them is the state, then the parent.
		fields := strings.Fields(string(stat[bytes.LastIndexByte(stat, ')')+1:]))
		if len(fields) < 2 {
			continue
		}
		if parent, err := strconv.Atoi(fields[1]); err == nil {
			children[parent] = append(children[parent], p)
		}
	}
	var out []int
	for next := []int{pid}; len(next) > 0; {
		p := next[0]
		next = next[1:]
		out = append(out, children[p]...)
		next = append(next, children[p]...)
	}
	return out, nil
}

// A syncBuffer is a bytes.Buffer that sshd's output and the test can use
// at once.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
