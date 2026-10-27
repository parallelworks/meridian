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
// a worker does, reports it, with its arguments and whether it has
// GODEBUG, and then exits, or, with hostStubModeEnv wait, waits for
// SIGTERM. Neither starts with MERIDIAN_, which the local backend keeps
// from a worker's environment.
const (
	hostStubEnv     = "HOSTTEST_STUB"
	hostStubModeEnv = "HOSTTEST_STUB_MODE"
)

// A hostStubReport is what the stub worker reports.
type hostStubReport struct {
	Token       string   `json:"token"`
	Args        []string `json:"args"`
	GODEBUG     bool     `json:"godebug"`
	Coordinator string   `json:"coordinator"`
}

func hostStub() int {
	terms := make(chan os.Signal, 1)
	signal.Notify(terms, syscall.SIGTERM)
	r := hostStubReport{Args: os.Args[1:], Coordinator: os.Getenv(workerapi.EnvCoordinator)}
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

// testMachineID is the host's ID in the tests that give their own system.
const testMachineID = "0123456789abcdef0123456789abcdef"

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
// logind keeps a user's processes after their session, and whose sockets'
// directory is under a directory of the test's.
func testHost(t *testing.T) *hostSystem {
	return &hostSystem{
		harden:    func() error { return nil },
		machineID: func(context.Context) (string, error) { return testMachineID, nil },
		loginctl:  loginctlSays(nil, map[string]string{"KillUserProcesses": "KillUserProcesses=no\n"}),
		scope:     func() bool { return true },
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
// state directory's its own under hosts/.
func TestHostHello(t *testing.T) {
	env, sys := runEnv(t), testHost(t)
	m, code := hostDo(t, env, sys, hostproto.OpHello, hostproto.Request{})
	h := m.Hello
	if code != exitOK || h == nil || m.Problem != nil || m.Error != "" {
		t.Fatalf("meridian host hello = %+v, %d", m, code)
	}
	want := hostproto.Paths{
		State:   filepath.Join(env[local.EnvStateDir], "hosts", testMachineID),
		Cache:   filepath.Join(env[local.EnvCacheDir], "bin"),
		Sockets: filepath.Join(sys.tmp, "meridian-"+strconv.Itoa(os.Getuid())),
		Runs:    filepath.Join(env[local.EnvStateDir], "runs"),
	}
	if h.Paths != want || h.Home != env["HOME"] || h.MachineID != testMachineID || h.Detach != hostproto.DetachSession || h.OS == "" || h.Arch == "" || h.Host == "" {
		t.Errorf("meridian host hello said %+v", h)
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
	env, sys := runEnv(t), testHost(t)
	if msgs, code, stderr := hostCLI(t, env, sys, strings.NewReader(""), "launch"); code != exitError || len(msgs) != 0 || !strings.Contains(stderr, `"code":"arguments_invalid"`) {
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
	if err := os.MkdirAll(filepath.Join(shared[local.EnvStateDir], "hosts", testMachineID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(shared[local.EnvStateDir], "hosts", testMachineID), 0o755); err != nil {
		t.Fatal(err)
	}
	m, code := hostDo(t, shared, sys, hostproto.OpHello, hostproto.Request{})
	if code != exitError || m.Problem == nil || m.Problem.Code != problems.SSHHostDirUnusable.Code() ||
		m.Problem.Params["path"] != filepath.Join(shared[local.EnvStateDir], "hosts", testMachineID) || m.Problem.Params["reason"] != string(problems.DirectoryNotPrivate.Code()) {
		t.Errorf("meridian host hello with a state directory others can read = %+v, %d", m, code)
	}
	// Probe says so of the member.
	m, code = hostDo(t, shared, sys, hostproto.OpProbe, hostproto.Request{Member: onHost, Demand: &hostproto.Demand{}})
	if code != exitOK || m.Fit == nil || m.Fit.Problem == nil || m.Fit.Problem.Code != problems.SSHHostDirUnusable.Code() {
		t.Errorf("meridian host probe with a state directory others can read = %+v, %d", m, code)
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
// session, or there is no logind; a scope of the user's service manager
// where it doesn't, and they linger; and nowhere where they don't, or
// there is no systemd-run, ssh_worker_would_be_killed.
func TestHostDetach(t *testing.T) {
	t.Parallel()
	notFound := &exec.Error{Name: "loginctl", Err: exec.ErrNotFound}
	noBus := errors.New("exit status 1: Failed to connect to bus: No such file or directory")
	notLingering := errors.New("exit status 1: Failed to get user: User ID 1000 is not logged in or lingering")
	for _, tc := range []struct {
		name   string
		say    map[string]string
		err    error
		linger error
		scope  bool
		want   string
	}{
		{"kept", map[string]string{"KillUserProcesses": "KillUserProcesses=no\n"}, nil, nil, true, hostproto.DetachSession},
		{"no loginctl", nil, notFound, nil, true, hostproto.DetachSession},
		{"no logind", nil, noBus, nil, true, hostproto.DetachSession},
		{"not said", map[string]string{"KillUserProcesses": ""}, nil, nil, true, hostproto.DetachSession},
		{"killed, lingering", map[string]string{"KillUserProcesses": "KillUserProcesses=yes\n", "Linger": "Linger=yes\n"}, nil, nil, true, hostproto.DetachScope},
		{"killed", map[string]string{"KillUserProcesses": "KillUserProcesses=yes\n", "Linger": "Linger=no\n"}, nil, nil, true, ""},
		{"killed, not lingering", map[string]string{"KillUserProcesses": "KillUserProcesses=yes\n"}, nil, notLingering, true, ""},
		{"killed, lingering, no systemd-run", map[string]string{"KillUserProcesses": "KillUserProcesses=yes\n", "Linger": "Linger=yes\n"}, nil, nil, false, ""},
	} {
		sys := hostSystem{
			loginctl: func(ctx context.Context, args ...string) (string, error) {
				if args[len(args)-1] == "Linger" && tc.linger != nil {
					return "", tc.linger
				}
				return loginctlSays(tc.err, tc.say)(ctx, args...)
			},
			scope: func() bool { return tc.scope },
		}
		mode, err := decideDetach(t.Context(), sys)
		switch {
		case tc.want != "" && (err != nil || mode != tc.want):
			t.Errorf("%s: %q, %v; want %q", tc.name, mode, err, tc.want)
		case tc.want == "":
			if p := problemOf(err); p == nil || p.Code != problems.SSHWorkerWouldBeKilled.Code() || p.Params["host"] == "" {
				t.Errorf("%s: %q, %v; want ssh_worker_would_be_killed", tc.name, mode, err)
			}
		}
	}
}

// TestHostKilled checks that a host where a worker wouldn't outlive the
// session that placed it is refused: hello and place say why, and probe
// says the member can't run the job.
func TestHostKilled(t *testing.T) {
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

// TestHostPlace checks meridian host's operations on a worker: probe; place,
// which starts the worker detached with the process's environment but
// GODEBUG, hands it its token on the socket it inherits before it returns,
// and returns the run's directory it chose under the user's home; find;
// check; cancel, which withdraws it; and forget, which forgets it.
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
	if r.Token != token || r.GODEBUG || r.Coordinator != "/tmp/c.sock" || !slices.Equal(r.Args, []string{"worker"}) {
		t.Errorf("the worker reported %+v", r)
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
	if m, code := hostDo(t, env, sys, hostproto.OpForget, hostproto.Request{Member: onHost, Placement: &p}); code != exitOK || m.Problem != nil || m.Error != "" {
		t.Errorf("meridian host forget = %+v, %d", m, code)
	}
	if m, _ := hostDo(t, env, sys, hostproto.OpFind, hostproto.Request{Member: onHost, Attempt: place.Attempt}); m.Found {
		t.Errorf("meridian host find of a placement forgotten = %+v", m)
	}
}

// TestHostProcess checks meridian host as the ssh backend runs it: a
// process of its own, which says it is ready before it reads the request
// on its standard input, which alone carries the bootstrap token, and
// hands the token to the worker it places.
func TestHostProcess(t *testing.T) {
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
	if r := waitReport(t, report); r.Token != token {
		t.Errorf("the worker reported %+v", r)
	}
	if slices.ContainsFunc(cmd.Args, func(a string) bool { return strings.Contains(a, token) }) || slices.ContainsFunc(cmd.Env, func(kv string) bool { return strings.Contains(kv, token) }) {
		t.Error("the token was in meridian host's arguments or environment")
	}
}

// problemOf is the problem err is, or nil.
func problemOf(err error) *problem.Problem {
	p, _ := errors.AsType[*problem.Problem](err)
	return p
}
