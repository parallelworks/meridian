package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	jsonv2 "encoding/json/v2"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/backend/hostproto"
	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/internal/harden"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// The stub worker the host tests place: the test binary, run as meridian
// worker with hostStubEnv set to where it reports, which takes its token as
// a worker does, reports it, with its arguments, its environment and
// whether it has GODEBUG, and then exits, or, with hostStubModeEnv wait,
// waits for SIGTERM. Neither starts with MERIDIAN_, which the local backend keeps
// from a worker's environment.
const (
	hostStubEnv     = "HOSTTEST_STUB"
	hostStubModeEnv = "HOSTTEST_STUB_MODE"
)

// A hostStubReport is what the stub worker reports.
type hostStubReport struct {
	Token string `json:"token"`
	// Args are its arguments, after its name, and Argv all of them.
	Args        []string `json:"args"`
	Argv        []string `json:"argv"`
	Env         []string `json:"env"`
	GODEBUG     bool     `json:"godebug"`
	Coordinator string   `json:"coordinator"`
}

// leaks returns where else the worker was given token than on its
// socket: those of its arguments and its environment that hold it.
func (r hostStubReport) leaks(token string) []string {
	var out []string
	for _, s := range slices.Concat(r.Argv, r.Env) {
		if strings.Contains(s, token) {
			out = append(out, s)
		}
	}
	return out
}

func hostStub() int {
	terms := make(chan os.Signal, 1)
	signal.Notify(terms, syscall.SIGTERM)
	r := hostStubReport{Args: os.Args[1:], Argv: os.Args, Env: os.Environ(), Coordinator: os.Getenv(workerapi.EnvCoordinator)}
	_, r.GODEBUG = os.LookupEnv("GODEBUG")
	token := os.NewFile(3, "token")
	if _, err := token.Write([]byte{'1'}); err == nil {
		data, _ := io.ReadAll(token)
		r.Token = string(data)
	}
	token.Close()
	data, _ := jsonv2.Marshal(r)
	out := os.Getenv(hostStubEnv)
	_ = os.WriteFile(out+".new", data, 0o600)
	_ = os.Rename(out+".new", out)
	if os.Getenv(hostStubModeEnv) == "wait" {
		select {
		case <-terms:
		case <-time.After(time.Minute):
			return 1
		}
	}
	return 0
}

// testMachineID and testHostname are the host's ID and name in the tests
// that give their own system, and testHostKey its key.
const (
	testMachineID = "0123456789abcdef0123456789abcdef"
	testHostname  = "Node_1.Cluster"
	testHostKey   = testMachineID + "-node-1.cluster"
)

// loginctlSays is a loginctl that gives out for each property it is asked
// for, by its name, or fails as err does: as recorded on systems of each
// kind.
func loginctlSays(err error, out map[string]string) func(context.Context, ...string) (string, error) {
	return func(_ context.Context, args ...string) (string, error) {
		if err != nil {
			return "", err
		}
		return out[args[len(args)-1]], nil
	}
}

// testHost is a system of the test's own: hardened as harden says, whose
// logind runs, and keeps a user's processes after their session, whose
// commands are all in /usr/bin, and whose sockets' directory is made under
// a directory of the test's.
func testHost(t *testing.T) *hostSystem {
	return &hostSystem{
		harden:    func() error { return nil },
		machineID: func(context.Context) (string, error) { return testMachineID, nil },
		hostname:  func() (string, error) { return testHostname, nil },
		logind:    func() bool { return true },
		loginctl:  loginctlSays(nil, map[string]string{"KillUserProcesses": "KillUserProcesses=no\n"}),
		command:   func(name string) (string, error) { return "/usr/bin/" + name, nil },
		version:   func(context.Context, string) (string, error) { return "systemd 252\n", nil },
		tmp:       t.TempDir(),
	}
}

// hostCLI runs meridian host op in this process, on sys, in the
// environment env, with stdin as its standard input, and returns the
// messages it wrote, its exit status and what it wrote on its standard
// error.
func hostCLI(t *testing.T, env map[string]string, sys *hostSystem, stdin io.Reader, op string) ([]hostproto.Message, int, string) {
	t.Helper()
	var stdout, stderr lockedBuffer
	c := &cli{
		stdin: stdin, stdout: &stdout, stderr: &stderr, host: sys,
		lookupEnv: func(name string) (string, bool) { v, ok := env[name]; return v, ok },
	}
	code := run(t.Context(), c, []string{"host", op})
	return hostMessages(t, stdout.String()), code, stderr.String()
}

// hostMessages decodes the lines meridian host wrote.
func hostMessages(t *testing.T, out string) []hostproto.Message {
	t.Helper()
	var msgs []hostproto.Message
	for line := range strings.Lines(out) {
		var m hostproto.Message
		if err := jsonv2.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("%v: %q", err, line)
		}
		msgs = append(msgs, m)
	}
	return msgs
}

// hostRequest is r as a request's line.
func hostRequest(t *testing.T, r hostproto.Request) string {
	t.Helper()
	data, err := jsonv2.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(data) + "\n"
}

// hostDo runs meridian host op with the request r, and returns its
// answer, once it has checked that it said it was ready first.
func hostDo(t *testing.T, env map[string]string, sys *hostSystem, op string, r hostproto.Request) (hostproto.Message, int) {
	t.Helper()
	msgs, code, stderr := hostCLI(t, env, sys, strings.NewReader(hostRequest(t, r)), op)
	if len(msgs) != 2 || !msgs[0].Ready || msgs[0].Protocol != hostproto.Version || msgs[1].Protocol != hostproto.Version || stderr != "" {
		t.Fatalf("meridian host %s wrote %+v, and %q", op, msgs, stderr)
	}
	return msgs[1], code
}

// onHost is the member a job on a host runs on, as the ssh backend gives
// it to meridian host.
var onHost = &backend.Member{Name: "cluster", Backend: "ssh", Isolation: "host"}

// TestHostHello checks what meridian host hello says of the host: its
// system, its user's home, its ID, how it detaches a worker, the binary
// that runs, and the directories, which it makes, only the user's, the
// state directory's its own under hosts/, by its ID and name, and the
// sockets' of a random name in tmp, without a runtime directory.
func TestHostHello(t *testing.T) {
	parallel(t)
	env, sys := runEnv(t), testHost(t)
	h := hostHello(t, env, sys)
	want := hostproto.Paths{
		State:   filepath.Join(env[local.EnvStateDir], "hosts", testHostKey),
		Cache:   filepath.Join(env[local.EnvCacheDir], "bin"),
		Sockets: h.Paths.Sockets,
		Runs:    filepath.Join(env[local.EnvStateDir], "runs"),
	}
	if h.Paths != want || h.Home != env["HOME"] || h.MachineID != testMachineID || h.Detach != hostproto.DetachSession || h.OS == "" || h.Arch == "" || h.Host == "" {
		t.Errorf("meridian host hello said %+v", h)
	}
	if filepath.Dir(want.Sockets) != sys.tmp || !strings.HasPrefix(filepath.Base(want.Sockets), "meridian-") {
		t.Errorf("the sockets' directory is %s, not one of a random name in %s", want.Sockets, sys.tmp)
	}
	if exe, digest := hostBinary(t); h.Binary.Path != exe || h.Binary.Digest != digest {
		t.Errorf("the binary is %+v, want %s, %s", h.Binary, exe, digest)
	}
	for _, dir := range []string{want.State, want.Cache, want.Sockets, want.Runs} {
		if info, err := os.Stat(dir); err != nil || info.Mode().Perm() != 0o700 {
			t.Errorf("%s is %v, %v", dir, info, err)
		}
	}
}

// hostHello runs meridian host hello, and returns what it said of the
// host.
func hostHello(t *testing.T, env map[string]string, sys *hostSystem) *hostproto.Hello {
	t.Helper()
	m, code := hostDo(t, env, sys, hostproto.OpHello, hostproto.Request{})
	if code != exitOK || m.Hello == nil || m.Problem != nil || m.Error != "" {
		t.Fatalf("meridian host hello = %+v, %d", m, code)
	}
	return m.Hello
}

// TestHostSockets checks the sockets' directory hello gives: the runtime
// directory's meridian/, where XDG_RUNTIME_DIR names one only the user can
// use whose path is short enough; otherwise one of a random name in tmp,
// which the host's state records, and every hello gives while it is the
// user's own, and which is made afresh where it is gone, or isn't, as
// where another user made a directory of that name once it was gone.
func TestHostSockets(t *testing.T) {
	parallel(t)
	env, sys := runEnv(t), testHost(t)
	// A runtime directory is the user's own, which only they can use.
	runtimeDir := t.TempDir()
	if err := os.Chmod(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	env["XDG_RUNTIME_DIR"] = runtimeDir
	if want := filepath.Join(runtimeDir, "meridian"); len(want) <= hostproto.MaxSocketsDir {
		if got := hostHello(t, env, sys).Paths.Sockets; got != want {
			t.Errorf("with a runtime directory, the sockets' directory is %s, want %s", got, want)
		}
	}
	for what, dir := range map[string]string{
		"a runtime directory others can use":   filepath.Join(t.TempDir(), "shared"),
		"a runtime directory with a long path": filepath.Join(runtimeDir, strings.Repeat("r", hostproto.MaxSocketsDir)),
		"no runtime directory":                 "",
	} {
		if err := os.MkdirAll(dir, 0o700); dir != "" && err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, 0o755); dir != "" && strings.HasSuffix(dir, "shared") && err != nil {
			t.Fatal(err)
		}
		env["XDG_RUNTIME_DIR"] = dir
		got := hostHello(t, env, sys).Paths.Sockets
		if filepath.Dir(got) != sys.tmp {
			t.Errorf("with %s, the sockets' directory is %s, not one in %s", what, got, sys.tmp)
		}
		if again := hostHello(t, env, sys).Paths.Sockets; again != got {
			t.Errorf("with %s, hello gave %s, then %s", what, got, again)
		}
	}
	first := hostHello(t, env, sys).Paths.Sockets
	if info, err := os.Stat(first); err != nil || info.Mode().Perm() != 0o700 {
		t.Errorf("%s is %v, %v", first, info, err)
	}
	// Gone, and another's of the same name, which here is one of the
	// user's that others can use.
	if err := os.Remove(first); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(first, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(first, 0o755); err != nil {
		t.Fatal(err)
	}
	second := hostHello(t, env, sys).Paths.Sockets
	if second == first || filepath.Dir(second) != sys.tmp {
		t.Errorf("once the sockets' directory %s was taken, hello gave %s", first, second)
	}
	if err := os.Remove(second); err != nil {
		t.Fatal(err)
	}
	if third := hostHello(t, env, sys).Paths.Sockets; third == second || filepath.Dir(third) != sys.tmp {
		t.Errorf("once the sockets' directory %s was gone, hello gave %s", second, third)
	}
}

// TestHostKey checks a host's key: its machine ID and its name, in lower
// case, with what a name has only, and at most 64 bytes of it; and that a
// host without an ID, or a name, is ssh_host_id_unknown, as one that hasn't
// a usable ID.
func TestHostKey(t *testing.T) {
	t.Parallel()
	unsafe := problems.SSHHostCommandUnsafe.Problem(problems.Params{"host": "h", "path": "/x/ioreg", "code": "path_unsafe"})
	for _, tc := range []struct {
		name     string
		id       string
		idErr    error
		hostname string
		nameErr  error
		want     string
		problem  *problems.Kind
	}{
		{"a host", " 0123ABCD\n", nil, "Login-1.Example.COM", nil, "0123abcd-login-1.example.com", nil},
		{"a long name", "0123abcd", nil, strings.Repeat("n", 100), nil, "0123abcd-" + strings.Repeat("n", 64), nil},
		{"no ID", "", nil, "h", nil, "", problems.SSHHostIDUnknown},
		{"an ID no directory can be named", "../x", nil, "h", nil, "", problems.SSHHostIDUnknown},
		{"an ID that can't be read", "", errors.New("permission denied"), "h", nil, "", problems.SSHHostIDUnknown},
		{"an ID from a command that isn't safe", "", unsafe, "h", nil, "", problems.SSHHostCommandUnsafe},
		{"no name", "0123abcd", nil, "", nil, "", problems.SSHHostIDUnknown},
		{"a name that can't be had", "0123abcd", nil, "", errors.New("no name"), "", problems.SSHHostIDUnknown},
	} {
		h := &hostOp{sys: hostSystem{
			machineID: func(context.Context) (string, error) { return tc.id, tc.idErr },
			hostname:  func() (string, error) { return tc.hostname, tc.nameErr },
		}}
		_, key, err := h.hostKey(t.Context())
		switch p := problemOf(err); {
		case tc.problem == nil && (err != nil || key != tc.want):
			t.Errorf("%s: %q, %v; want %q", tc.name, key, err, tc.want)
		case tc.problem != nil && (p == nil || p.Code != tc.problem.Code()):
			t.Errorf("%s: %q, %v; want %s", tc.name, key, err, tc.problem.Code())
		}
	}
}

// TestHostCommand checks the commands meridian host runs: found on the
// PATH, and run by the absolute path of the file, once it is checked as
// the worker's binary is: one first on the PATH in a directory others can
// change, or reached through a link to one there, is
// ssh_host_command_unsafe, though a safe one comes after it.
func TestHostCommand(t *testing.T) {
	safe, shared := t.TempDir(), filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(shared, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0o777); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{safe, shared} {
		if err := os.WriteFile(filepath.Join(dir, "meridian-test-cmd"), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	links := t.TempDir()
	if err := os.Symlink(filepath.Join(shared, "meridian-test-cmd"), filepath.Join(links, "meridian-test-cmd")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", safe)
	if path, err := hostCommand("meridian-test-cmd"); err != nil || path != filepath.Join(safe, "meridian-test-cmd") {
		t.Errorf("a safe command: %q, %v", path, err)
	}
	if _, err := hostCommand("meridian-test-missing"); !errors.Is(err, exec.ErrNotFound) {
		t.Errorf("a command that isn't there: %v", err)
	}
	for _, path := range []string{shared + string(os.PathListSeparator) + safe, links + string(os.PathListSeparator) + safe} {
		t.Setenv("PATH", path)
		_, err := hostCommand("meridian-test-cmd")
		if p := problemOf(err); p == nil || p.Code != problems.SSHHostCommandUnsafe.Code() ||
			p.Params["path"] != filepath.Join(shared, "meridian-test-cmd") || p.Params["code"] != string(problems.PathUnsafe.Code()) {
			t.Errorf("with the PATH %s: %v", path, err)
		}
	}
}

// hostBinary is the binary meridian host runs, this test's, and its
// digest.
func hostBinary(t *testing.T) (string, string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if exe, err = filepath.EvalSymlinks(exe); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return exe, "sha256:" + hex.EncodeToString(sum[:])
}

// TestHostRefuses checks meridian host's refusals: an operation it doesn't
// have; a request that isn't one, or lacks what its operation needs, which
// it couldn't answer; a directory that isn't the user's own,
// ssh_host_dir_unusable; and a home that isn't an absolute path.
func TestHostRefuses(t *testing.T) {
	parallel(t)
	env, sys := runEnv(t), testHost(t)
	if msgs, code, stderr := hostCLI(t, env, sys, strings.NewReader(""), "launch"); code != exitError || len(msgs) != 1 || msgs[0].Protocol != hostproto.Version ||
		msgs[0].Problem == nil || msgs[0].Problem.Code != problems.ArgumentsInvalid.Code() || stderr != "" {
		t.Errorf("meridian host launch = %+v, %d, %s", msgs, code, stderr)
	}
	for what, in := range map[string]string{
		"a request that isn't JSON":     "not json\n",
		"a request with another member": `{"members": {}}` + "\n",
		"no request":                    "",
		"a probe without its member":    `{"demand": {}}` + "\n",
	} {
		msgs, code, _ := hostCLI(t, env, sys, strings.NewReader(in), hostproto.OpProbe)
		if code != exitError || len(msgs) != 2 || !msgs[0].Ready || msgs[1].Error == "" || msgs[1].Problem != nil {
			t.Errorf("meridian host probe, %s = %+v, %d", what, msgs, code)
		}
	}
	if m, code := hostDo(t, env, sys, hostproto.OpCheck, hostproto.Request{Member: onHost}); code != exitError || m.Error == "" {
		t.Errorf("a check without its placement = %+v, %d", m, code)
	}
	if m, code := hostDo(t, env, sys, hostproto.OpPlace, hostproto.Request{Member: onHost, Place: &hostproto.Place{Attempt: "a1", BootstrapToken: "t"}}); code != exitError || m.Error == "" {
		t.Errorf("a place without its run and store = %+v, %d", m, code)
	}

	// A state directory that other users can read.
	shared := runEnv(t)
	state := filepath.Join(shared[local.EnvStateDir], "hosts", testHostKey)
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(state, 0o755); err != nil {
		t.Fatal(err)
	}
	m, code := hostDo(t, shared, sys, hostproto.OpHello, hostproto.Request{})
	if code != exitError || m.Problem == nil || m.Problem.Code != problems.SSHHostDirUnusable.Code() ||
		m.Problem.Params["path"] != state || m.Problem.Params["at"] != state || m.Problem.Params["reason"] != string(problems.DirectoryNotPrivate.Code()) {
		t.Errorf("meridian host hello with a state directory others can read = %+v, %d", m, code)
	}
	// Probe says so of the member.
	m, code = hostDo(t, shared, sys, hostproto.OpProbe, hostproto.Request{Member: onHost, Demand: &hostproto.Demand{}})
	if code != exitOK || m.Fit == nil || m.Fit.Problem == nil || m.Fit.Problem.Code != problems.SSHHostDirUnusable.Code() {
		t.Errorf("meridian host probe with a state directory others can read = %+v, %d", m, code)
	}

	// A directory on the way to it that others can change: the problem
	// names it.
	changeable := runEnv(t)
	if err := os.MkdirAll(changeable[local.EnvStateDir], 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(changeable[local.EnvStateDir], 0o777); err != nil {
		t.Fatal(err)
	}
	if m, code := hostDo(t, changeable, sys, hostproto.OpHello, hostproto.Request{}); code != exitError || m.Problem == nil || m.Problem.Code != problems.SSHHostDirUnusable.Code() ||
		m.Problem.Params["at"] != changeable[local.EnvStateDir] || m.Problem.Params["reason"] != string(problems.PathUnsafe.Code()) {
		t.Errorf("meridian host hello with a state directory others can change = %+v, %d", m, code)
	}

	homeless := runEnv(t)
	homeless["HOME"] = "relative"
	if m, code := hostDo(t, homeless, sys, hostproto.OpHello, hostproto.Request{}); code != exitError || m.Problem == nil || m.Problem.Code != problems.HomeUnknown.Code() {
		t.Errorf("meridian host hello with a relative home = %+v, %d", m, code)
	}
}

// TestHostHardensFirst checks the order of meridian host's handshake: it
// hardens itself, then says it is ready, and only then reads its request,
// which so reaches only a hardened process; and one that can't harden
// itself says so, and reads nothing.
func TestHostHardensFirst(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	var (
		mu     sync.Mutex
		events []string
	)
	note := func(e string) {
		mu.Lock()
		defer mu.Unlock()
		if !slices.Contains(events, e) {
			events = append(events, e)
		}
	}
	sys := testHost(t)
	sys.harden = func() error { note("hardened"); return nil }
	stdin := &noticedReader{r: strings.NewReader(hostRequest(t, hostproto.Request{})), read: func() { note("request read") }}
	stdout := &noticedWriter{written: func(p []byte) {
		if strings.Contains(string(p), `"ready":true`) {
			note("ready")
		}
	}}
	c := &cli{
		stdin: stdin, stdout: stdout, stderr: io.Discard, host: sys,
		lookupEnv: func(name string) (string, bool) { v, ok := env[name]; return v, ok },
	}
	if code := run(t.Context(), c, []string{"host", hostproto.OpHello}); code != exitOK {
		t.Fatalf("meridian host hello exited %d", code)
	}
	if want := []string{"hardened", "ready", "request read"}; !slices.Equal(events, want) {
		t.Errorf("meridian host did %q, want %q", events, want)
	}

	events = nil
	for err, kind := range map[error]*problems.Kind{harden.ErrUnsupported: problems.WorkerSystemUnsupported, errors.New("denied"): problems.WorkerHardenFailed} {
		sys.harden = func() error { return err }
		msgs, code, _ := hostCLI(t, env, sys, &noticedReader{r: strings.NewReader(""), read: func() { note("request read") }}, hostproto.OpPlace)
		if code != exitError || len(msgs) != 1 || msgs[0].Ready || msgs[0].Problem == nil || msgs[0].Problem.Code != kind.Code() || len(events) != 0 {
			t.Errorf("meridian host that can't harden itself (%v) wrote %+v, and %q", err, msgs, events)
		}
	}
}

// A noticedReader is a reader that notes when it is first read.
type noticedReader struct {
	r    io.Reader
	read func()
}

func (n *noticedReader) Read(p []byte) (int, error) {
	n.read()
	return n.r.Read(p)
}

// A noticedWriter is a writer that notes what is written to it.
type noticedWriter struct {
	written func([]byte)
}

func (n *noticedWriter) Write(p []byte) (int, error) {
	n.written(p)
	return len(p), nil
}

// TestHostDetach checks how meridian host starts a worker detached, as
// logind's settings say, from what loginctl says on systems of each kind:
// a session of its own where logind keeps a user's processes after their
// session, or there is no logind; a scope of the user's service manager,
// with the systemd-run hostCommand found, where it doesn't, and they
// linger, as loginctl says, or fails for a user who neither lingers nor is
// logged in; and nowhere where they don't, or there is no systemd-run,
// ssh_worker_would_be_killed. A systemd-run too old for --collect is
// ssh_systemd_too_old, and logind without loginctl ssh_loginctl_missing.
// A loginctl that fails otherwise, as one that can't reach logind, or
// times out, is an error, and a command that isn't safe its problem: what
// logind would do isn't known.
func TestHostDetach(t *testing.T) {
	t.Parallel()
	const systemdRun = "/usr/bin/systemd-run"
	notFound := &exec.Error{Name: "loginctl", Err: exec.ErrNotFound}
	noBus := &exec.ExitError{Stderr: []byte("Failed to connect to bus: No such file or directory\n")}
	notLingering := &exec.ExitError{Stderr: []byte("Failed to get user: User ID 1000 is not logged in or lingering\n")}
	unsafe := problems.SSHHostCommandUnsafe.Problem(problems.Params{"host": "h", "path": "/shared/bin/x", "code": "path_unsafe"})
	killed := map[string]string{"KillUserProcesses": "KillUserProcesses=yes\n", "Linger": "Linger=yes\n"}
	const current = "systemd 252 (252.32-1.el9)\n+PAM +AUDIT +SELINUX\n"
	for _, tc := range []struct {
		name    string
		logind  bool
		say     map[string]string
		err     error
		linger  error
		command error
		version string
		want    string
		problem *problems.Kind
	}{
		{"no logind", false, nil, noBus, nil, nil, current, hostproto.DetachSession, nil},
		{"kept", true, map[string]string{"KillUserProcesses": "KillUserProcesses=no\n"}, nil, nil, nil, current, hostproto.DetachSession, nil},
		{"not said", true, map[string]string{"KillUserProcesses": ""}, nil, nil, nil, current, hostproto.DetachSession, nil},
		{"no loginctl", true, nil, notFound, nil, nil, current, "", problems.SSHLoginctlMissing},
		{"no bus", true, nil, noBus, nil, nil, current, "", nil},
		{"loginctl timed out", true, nil, context.DeadlineExceeded, nil, nil, current, "", nil},
		{"loginctl unsafe", true, nil, unsafe, nil, nil, current, "", problems.SSHHostCommandUnsafe},
		{"killed, lingering", true, killed, nil, nil, nil, current, hostproto.DetachScope, nil},
		{"killed", true, map[string]string{"KillUserProcesses": "KillUserProcesses=yes\n", "Linger": "Linger=no\n"}, nil, nil, nil, current, "", problems.SSHWorkerWouldBeKilled},
		{"killed, not lingering", true, killed, nil, notLingering, nil, current, "", problems.SSHWorkerWouldBeKilled},
		{"killed, lingering unknown", true, killed, nil, noBus, nil, current, "", nil},
		{"killed, lingering, no systemd-run", true, killed, nil, nil, notFound, current, "", problems.SSHWorkerWouldBeKilled},
		{"killed, lingering, systemd-run unsafe", true, killed, nil, nil, unsafe, current, "", problems.SSHHostCommandUnsafe},
		{"killed, lingering, systemd too old", true, killed, nil, nil, nil, "systemd 219\n+PAM\n", "", problems.SSHSystemdTooOld},
		{"killed, lingering, systemd's version unknown", true, killed, nil, nil, nil, "", "", nil},
	} {
		sys := hostSystem{
			logind: func() bool { return tc.logind },
			loginctl: func(ctx context.Context, args ...string) (string, error) {
				if args[len(args)-1] == "Linger" && tc.linger != nil {
					return "", tc.linger
				}
				return loginctlSays(tc.err, tc.say)(ctx, args...)
			},
			command: func(name string) (string, error) {
				if tc.command != nil {
					return "", tc.command
				}
				return "/usr/bin/" + name, nil
			},
			version: func(context.Context, string) (string, error) { return tc.version, nil },
		}
		mode, path, err := decideDetach(t.Context(), sys)
		switch p := problemOf(err); {
		case tc.want != "":
			if err != nil || mode != tc.want || (mode == hostproto.DetachScope) != (path == systemdRun) {
				t.Errorf("%s: %q, %q, %v; want %q", tc.name, mode, path, err, tc.want)
			}
		case tc.problem != nil:
			if p == nil || p.Code != tc.problem.Code() || p.Params["host"] == "" {
				t.Errorf("%s: %q, %v; want %s", tc.name, mode, err, tc.problem.Code())
			}
		case err == nil || p != nil:
			t.Errorf("%s: %q, %v; want an error that isn't a problem", tc.name, mode, err)
		}
	}
	// The scope's systemd is old by the first that takes --collect.
	sys := hostSystem{
		logind:   func() bool { return true },
		loginctl: loginctlSays(nil, killed),
		command:  func(name string) (string, error) { return "/usr/bin/" + name, nil },
		version:  func(context.Context, string) (string, error) { return "systemd 235\n", nil },
	}
	if _, _, err := decideDetach(t.Context(), sys); problemOf(err) == nil || problemOf(err).Params["version"] != "235" || problemOf(err).Params["minimum"] != systemdCollect {
		t.Errorf("systemd 235: %v", err)
	}
}

// TestHostRunsDir checks where runs' directories are on a host: the state
// directory's runs/ that MERIDIAN_STATE_DIR names, and otherwise
// ${XDG_STATE_HOME:-$HOME/.local/state}/meridian/runs on every system,
// macOS among them, whose own state directory's name holds a space; a
// relative XDG_STATE_HOME is ignored, as XDG says.
func TestHostRunsDir(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		env  map[string]string
		want string
	}{
		{map[string]string{local.EnvStateDir: "/state/", "XDG_STATE_HOME": "/xdg"}, "/state/runs"},
		{map[string]string{"XDG_STATE_HOME": "/xdg"}, "/xdg/meridian/runs"},
		{map[string]string{"XDG_STATE_HOME": "xdg"}, "/home/u/.local/state/meridian/runs"},
		{nil, "/home/u/.local/state/meridian/runs"},
	} {
		if got := hostRunsDir(func(name string) string { return tc.env[name] }, "/home/u"); got != tc.want {
			t.Errorf("hostRunsDir(%v) = %s, want %s", tc.env, got, tc.want)
		}
	}
}

// TestHostKilled checks that a host where a worker wouldn't outlive the
// session that placed it is refused: hello and place say why, and probe
// says the member can't run the job.
func TestHostKilled(t *testing.T) {
	parallel(t)
	env, sys := runEnv(t), testHost(t)
	sys.loginctl = loginctlSays(nil, map[string]string{"KillUserProcesses": "KillUserProcesses=yes\n", "Linger": "Linger=no\n"})
	for op, r := range map[string]hostproto.Request{
		hostproto.OpHello: {},
		hostproto.OpPlace: {Member: onHost, Place: &hostproto.Place{Attempt: "a1", BootstrapToken: "t", Run: "1", Store: "s"}},
	} {
		if m, code := hostDo(t, env, sys, op, r); code != exitError || m.Problem == nil || m.Problem.Code != problems.SSHWorkerWouldBeKilled.Code() {
			t.Errorf("meridian host %s = %+v, %d", op, m, code)
		}
	}
	m, code := hostDo(t, env, sys, hostproto.OpProbe, hostproto.Request{Member: onHost, Demand: &hostproto.Demand{}})
	if code != exitOK || m.Fit == nil || m.Fit.Problem == nil || m.Fit.Problem.Code != problems.SSHWorkerWouldBeKilled.Code() {
		t.Errorf("meridian host probe = %+v, %d", m, code)
	}
}

// waitReport waits for the stub worker's report at path.
func waitReport(t *testing.T, path string) hostStubReport {
	t.Helper()
	var r hostStubReport
	waitUntil(t, "the worker's report", func() bool {
		data, err := os.ReadFile(path)
		return err == nil && jsonv2.Unmarshal(data, &r) == nil
	})
	return r
}

// hostPlacements returns the directory of the host's placements, which
// meridian host's local backend keeps in its state directory. It and
// reaped know that backend's private layout, <attempt>.exit beside each
// placement, and change with it.
func hostPlacements(env map[string]string) string {
	return filepath.Join(env[local.EnvStateDir], "hosts", testHostKey, "placements")
}

// reaped waits until the worker of the attempt, which meridian host place
// started in the test's own process (hostDo), has been reaped by that
// process, its parent, which records its exit status beside its
// placement as it does. meridian host cancel returns once the worker has
// ended, which can be before that, and forget, a call of its own, can't
// wait for it: a status written once forget has removed its placement
// stays in the host's placements, and one written once the test has
// ended is in the way of its cleanup.
func reaped(t *testing.T, env map[string]string, attempt string) {
	t.Helper()
	exit := filepath.Join(hostPlacements(env), attempt+".exit")
	waitUntil(t, "the worker of "+attempt+" to be reaped", func() bool {
		_, err := os.Stat(exit)
		return err == nil
	})
}

// withdraw cancels the worker of the placement p, as the coordinator does,
// waits for the test's process to have reaped it (reaped), and forgets it.
func withdraw(t *testing.T, env map[string]string, sys *hostSystem, p *backend.Placement) {
	t.Helper()
	if m, code := hostDo(t, env, sys, hostproto.OpCancel, hostproto.Request{Member: onHost, Placement: p}); code != exitOK || m.Problem != nil || m.Error != "" {
		t.Fatalf("meridian host cancel of %s = %+v, %d", p.Attempt, m, code)
	}
	reaped(t, env, p.Attempt)
	if m, code := hostDo(t, env, sys, hostproto.OpForget, hostproto.Request{Member: onHost, Placement: p}); code != exitOK || m.Problem != nil || m.Error != "" {
		t.Errorf("meridian host forget of %s = %+v, %d", p.Attempt, m, code)
	}
}

// TestHostPlace checks meridian host's operations on a worker: probe; place,
// which starts the worker detached with the process's environment but
// GODEBUG, hands it its token on the socket it inherits before it returns,
// and returns the run's directory it chose under the user's home; find;
// check; cancel, which withdraws it; and forget, which forgets it. Every
// worker it places it withdraws, and forgets, before it ends, so that
// nothing writes in its state directory as the test's cleanup removes it.
func TestHostPlace(t *testing.T) {
	env, sys := runEnv(t), testHost(t)
	report := filepath.Join(t.TempDir(), "report.json")
	t.Setenv(hostStubEnv, report)
	t.Setenv(hostStubModeEnv, "wait")
	t.Setenv("GODEBUG", "fips140=only")

	m, code := hostDo(t, env, sys, hostproto.OpProbe, hostproto.Request{Member: onHost, Demand: &hostproto.Demand{Resources: backend.Resources{MilliCPU: 100}}})
	if code != exitOK || m.Fit == nil || m.Fit.Problem != nil || m.Fit.Fit().Availability != backend.StartsNow {
		t.Fatalf("meridian host probe = %+v, %d", m, code)
	}
	token := "token-" + strconv.Itoa(os.Getpid())
	place := hostproto.Place{Attempt: "run1-main-1", Name: "1 main", Coordinator: hostproto.Endpoint{Address: "/tmp/c.sock"}, BootstrapToken: token, Run: "1", Store: "abc"}
	m, code = hostDo(t, env, sys, hostproto.OpPlace, hostproto.Request{Member: onHost, Place: &place})
	if code != exitOK || m.Placement == nil || m.Placement.Attempt != place.Attempt || m.Placement.ID == "" || m.Placement.WorkerDigest == "" {
		t.Fatalf("meridian host place = %+v, %d", m, code)
	}
	p := *m.Placement
	if want := filepath.Join(env[local.EnvStateDir], "runs", "1-abc"); m.RunDir != want {
		t.Errorf("the run's directory is %q, want %q", m.RunDir, want)
	}
	r := waitReport(t, report)
	if r.Token != token || r.leaks(token) != nil || r.GODEBUG || r.Coordinator != "/tmp/c.sock" || !slices.Equal(r.Args, []string{"worker"}) {
		t.Errorf("the worker reported the token %q, also in %q, the arguments %q, GODEBUG %v and the coordinator %q", r.Token, r.leaks(token), r.Args, r.GODEBUG, r.Coordinator)
	}
	if m, _ := hostDo(t, env, sys, hostproto.OpFind, hostproto.Request{Member: onHost, Attempt: place.Attempt}); !m.Found || m.Placement == nil || *m.Placement != p {
		t.Errorf("meridian host find = %+v", m)
	}
	if m, _ := hostDo(t, env, sys, hostproto.OpFind, hostproto.Request{Member: onHost, Attempt: "other"}); m.Found || m.Placement != nil {
		t.Errorf("meridian host find of another attempt = %+v", m)
	}
	if m, _ := hostDo(t, env, sys, hostproto.OpCheck, hostproto.Request{Member: onHost, Placement: &p}); m.Status == nil || m.Status.Status().State != backend.Running {
		t.Errorf("meridian host check of a worker that runs = %+v", m)
	}
	// The worker holds the placement: it isn't placed again.
	if m, _ := hostDo(t, env, sys, hostproto.OpPlace, hostproto.Request{Member: onHost, Place: &place}); m.Placement == nil || *m.Placement != p {
		t.Errorf("meridian host place again = %+v", m)
	}
	if m, code := hostDo(t, env, sys, hostproto.OpCancel, hostproto.Request{Member: onHost, Placement: &p}); code != exitOK || m.Problem != nil || m.Error != "" {
		t.Fatalf("meridian host cancel = %+v, %d", m, code)
	}
	if m, _ := hostDo(t, env, sys, hostproto.OpCheck, hostproto.Request{Member: onHost, Placement: &p}); m.Status == nil || m.Status.Status() != (backend.Status{State: backend.Dead, End: backend.EndWithdrawn}) {
		t.Errorf("meridian host check of a worker cancelled = %+v", m)
	}
	reaped(t, env, p.Attempt)
	if m, code := hostDo(t, env, sys, hostproto.OpForget, hostproto.Request{Member: onHost, Placement: &p}); code != exitOK || m.Problem != nil || m.Error != "" {
		t.Errorf("meridian host forget = %+v, %d", m, code)
	}
	if m, _ := hostDo(t, env, sys, hostproto.OpFind, hostproto.Request{Member: onHost, Attempt: place.Attempt}); m.Found {
		t.Errorf("meridian host find of a placement forgotten = %+v", m)
	}

	// A child run's directory is its root's, with the child's path in it:
	// the root's is made, private, if none of its jobs ran on the host,
	// and a path outside it is refused.
	child := place
	child.Attempt, child.BootstrapToken, child.Run, child.Root, child.Path = "run2-main-1", token+"-child", "2", "9", "subworkflows/build/step_0"
	m, code = hostDo(t, env, sys, hostproto.OpPlace, hostproto.Request{Member: onHost, Place: &child})
	rootDir := filepath.Join(env[local.EnvStateDir], "runs", "9-abc")
	if code != exitOK || m.Placement == nil || m.RunDir != filepath.Join(rootDir, "subworkflows", "build", "step_0") {
		t.Fatalf("meridian host place of a child = %+v, %d", m, code)
	}
	if info, err := os.Stat(rootDir); err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Errorf("the root's directory: %v, %v", info, err)
	}
	withdraw(t, env, sys, m.Placement)
	for _, rootPath := range [][2]string{{"9", "../x"}, {"9", "/abs"}, {"9", ""}, {"", "x"}} {
		bad := child
		bad.Attempt, bad.Root, bad.Path = "run3-main-1", rootPath[0], rootPath[1]
		if m, code := hostDo(t, env, sys, hostproto.OpPlace, hostproto.Request{Member: onHost, Place: &bad}); code != exitError || m.Error == "" || m.Placement != nil {
			t.Errorf("meridian host place of a child with the root %q and the path %q = %+v, %d", bad.Root, bad.Path, m, code)
		}
	}

	// A root's directory that is there is taken as it is only where it is
	// a directory of the user's that no other user can change: one others
	// may read is, and a symbolic link, which the worker would follow, a
	// file, and one others can change are ssh_host_dir_unusable.
	runs := filepath.Join(env[local.EnvStateDir], "runs")
	if err := os.Symlink(t.TempDir(), filepath.Join(runs, "5-abc")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runs, "6-abc"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for root, perm := range map[string]os.FileMode{"7-abc": 0o777, "8-abc": 0o755} {
		if err := os.Mkdir(filepath.Join(runs, root), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(filepath.Join(runs, root), perm); err != nil {
			t.Fatal(err)
		}
	}
	for i, root := range []string{"5", "6", "7", "8"} {
		in := child
		in.Attempt, in.BootstrapToken, in.Run, in.Root = "run4-main-"+root, token+"-"+root, "4"+root, root
		m, code := hostDo(t, env, sys, hostproto.OpPlace, hostproto.Request{Member: onHost, Place: &in})
		if root == "8" {
			if code != exitOK || m.Placement == nil || m.RunDir != filepath.Join(runs, "8-abc", "subworkflows", "build", "step_0") {
				t.Errorf("meridian host place of a child whose root's directory others may read = %+v, %d", m, code)
			} else {
				withdraw(t, env, sys, m.Placement)
			}
			continue
		}
		if code != exitError || m.Problem == nil || m.Problem.Code != problems.SSHHostDirUnusable.Code() || m.Placement != nil {
			t.Errorf("meridian host place of a child whose root's directory is case %d = %+v, %d", i, m, code)
		}
	}
	if left, err := os.ReadDir(hostPlacements(env)); err != nil || len(left) > 0 {
		names := make([]string, 0, len(left))
		for _, e := range left {
			names = append(names, e.Name())
		}
		t.Errorf("the host keeps %q of its placements, %v", names, err)
	}
}

// TestHostProcess checks meridian host as the ssh backend runs it: a
// process of its own, which says it is ready before it reads the request
// on its standard input, which alone carries the bootstrap token, and
// hands the token to the worker it places on the socket it inherits, and
// not in its arguments or its environment, as the worker sees them.
func TestHostProcess(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	report := filepath.Join(t.TempDir(), "report.json")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), exe, "host", hostproto.OpPlace)
	cmd.Env = []string{envMain + "=1", "PATH=" + os.Getenv("PATH"), hostStubEnv + "=" + report}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr lockedBuffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	lines := bufio.NewScanner(stdout)
	var ready hostproto.Message
	if !lines.Scan() || jsonv2.Unmarshal(lines.Bytes(), &ready) != nil {
		t.Fatalf("meridian host wrote %q first: %s", lines.Text(), stderr.String())
	}
	if !ready.Ready || ready.Protocol != hostproto.Version {
		if ready.Problem != nil && ready.Problem.Code == problems.SSHWorkerWouldBeKilled.Code() {
			t.Skip("this host would kill a worker as its session ended")
		}
		t.Fatalf("meridian host wrote %+v first", ready)
	}
	token := "token-" + strconv.Itoa(os.Getpid())
	place := hostproto.Place{Attempt: "a1", Coordinator: hostproto.Endpoint{Address: "/tmp/c.sock"}, BootstrapToken: token, Run: "1", Store: "s"}
	if _, err := io.WriteString(stdin, hostRequest(t, hostproto.Request{Member: onHost, Place: &place})); err != nil {
		t.Fatal(err)
	}
	var m hostproto.Message
	if !lines.Scan() || jsonv2.Unmarshal(lines.Bytes(), &m) != nil {
		t.Fatalf("meridian host wrote %q: %s", lines.Text(), stderr.String())
	}
	if m.Problem != nil && m.Problem.Code == problems.SSHWorkerWouldBeKilled.Code() {
		t.Skip("this host would kill a worker as its session ended")
	}
	if m.Placement == nil || m.Placement.Attempt != place.Attempt || m.RunDir == "" {
		t.Fatalf("meridian host place = %+v: %s", m, stderr.String())
	}
	if err := cmd.Wait(); err != nil {
		t.Errorf("meridian host place exited: %v", err)
	}
	r := waitReport(t, report)
	if r.Token != token || len(r.Env) == 0 || len(r.Argv) == 0 {
		t.Errorf("the worker reported the token %q, %d arguments and %d variables", r.Token, len(r.Argv), len(r.Env))
	}
	if leaks := r.leaks(token); leaks != nil {
		t.Errorf("the worker was given the token in its arguments or its environment: %q", leaks)
	}
}

// problemOf is the problem err is, or nil.
func problemOf(err error) *problem.Problem {
	p, _ := errors.AsType[*problem.Problem](err)
	return p
}
