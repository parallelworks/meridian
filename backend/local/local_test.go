//go:build linux || darwin || freebsd

package local

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	jsonv2 "encoding/json/v2"
	"errors"
	"io"
	"io/fs"
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

	"github.com/parallelworks/foundation/problem"
	"golang.org/x/sys/unix"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/internal/racetest"
	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// The test binary is the driver's executable: started with stubEnv set,
// it is a stub worker, which reports what it was started with to the file
// reportEnv names, appends its process ID to that file's .pids, and then
// does what the env var's value says.
const (
	stubEnv   = "LOCALTEST_STUB"
	reportEnv = "LOCALTEST_REPORT"
)

func TestMain(m *testing.M) {
	if mode := os.Getenv(stubEnv); mode != "" {
		os.Exit(stub(mode))
	}
	os.Exit(m.Run())
}

// A report is what a stub worker was started with.
type report struct {
	PID         int      `json:"pid"`
	Args        []string `json:"args"`
	Dir         string   `json:"dir"`
	Coordinator string   `json:"coordinator"`
	TokenFD     string   `json:"tokenFD"`
	Token       string   `json:"token"`
	// Leaked are the MERIDIAN_ variables it was given but those the driver
	// sets.
	Leaked []string `json:"leaked"`
	// OwnSession says that it leads a session of its own.
	OwnSession bool `json:"ownSession"`
	// Launched is what launchedEnv was set to, as the test's launcher
	// sets it.
	Launched string `json:"launched"`
}

// launchedEnv is what the tests' launcher sets in the stub's environment.
const launchedEnv = "LOCALTEST_LAUNCHED"

// stub is a stub worker: for mode run, it takes its token as the worker
// does, saying it is ready on the socket it inherits and reading the token
// from it, and runs until SIGTERM; for ignore, it does too, but takes no
// action on SIGTERM but to note it in its report's .signals, and runs
// until it is killed; for slow, it waits a second before it says it is
// ready, and then runs as for run; for exit, it exits at once, without its
// token.
func stub(mode string) int {
	out := os.Getenv(reportEnv)
	terms := make(chan os.Signal, 4)
	signal.Notify(terms, syscall.SIGTERM)
	if f, err := os.OpenFile(out+".pids", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600); err == nil {
		_, _ = f.WriteString(strconv.Itoa(os.Getpid()) + "\n")
		f.Close()
	}
	r := report{PID: os.Getpid(), Args: os.Args[1:], Coordinator: os.Getenv(workerapi.EnvCoordinator), TokenFD: os.Getenv(workerapi.EnvBootstrapTokenFD)}
	r.Dir, _ = os.Getwd()
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(name, "MERIDIAN_") && name != workerapi.EnvCoordinator && name != workerapi.EnvBootstrapTokenFD {
			r.Leaked = append(r.Leaked, name)
		}
	}
	sid, err := unix.Getsid(0)
	r.OwnSession = err == nil && sid == os.Getpid()
	r.Launched = os.Getenv(launchedEnv)
	if mode == "slow" {
		time.Sleep(time.Second) //nolint:forbidigo // a worker slow to say it is ready, as the test asks
	}
	if mode != "exit" {
		token := os.NewFile(3, "token")
		if _, err := token.Write([]byte{'1'}); err == nil {
			data, _ := io.ReadAll(token)
			r.Token = string(data)
		}
		token.Close()
	}
	data, _ := jsonv2.Marshal(r)
	_ = os.WriteFile(out+".new", data, 0o600)
	_ = os.Rename(out+".new", out)
	deadline := time.After(time.Minute)
	for {
		if mode == "exit" {
			return 0
		}
		select {
		case <-terms:
			if mode != "ignore" {
				return 0
			}
			if f, err := os.OpenFile(out+".signals", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600); err == nil {
				_, _ = f.WriteString("term\n")
				f.Close()
			}
		case <-deadline:
			return 1
		}
	}
}

// newDriver returns a driver whose workers are stubs of mode, with a
// MERIDIAN_ variable of the test's own that they mustn't get, and the
// path their reports go to.
func newDriver(t testing.TB, mode string) (*Driver, string) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "report.json")
	env := append(os.Environ(), stubEnv+"="+mode, reportEnv+"="+out, "MERIDIAN_LEAK=yes",
		workerapi.EnvBootstrapToken+"=not-this-one", workerapi.EnvBootstrapTokenFile+"=/not/this/one")
	// A race that a worker reports, in its log in the state directory,
	// fails the test.
	dir := t.TempDir()
	racetest.Watch(t, dir)
	return &Driver{State: filepath.Join(dir, "state"), Environ: env}, out
}

// TestEnviron checks a worker's environment: its driver's, but the
// MERIDIAN_ variables and those it withholds, which hold secrets' values,
// with where the coordinator is and the token's descriptor.
func TestEnviron(t *testing.T) {
	d := &Driver{
		Environ:  []string{"PATH=/bin", "HF_TOKEN=secret", "HF_TOKEN_NOT=kept", "BUILD=42", "MERIDIAN_LEAK=yes", "A_TOKEN="},
		Withhold: []string{"HF_TOKEN", "A_TOKEN"},
	}
	got := d.environ(backend.Endpoint{Address: "/c.sock"})
	want := []string{"PATH=/bin", "HF_TOKEN_NOT=kept", "BUILD=42", workerapi.EnvCoordinator + "=/c.sock", workerapi.EnvBootstrapTokenFD + "=3"}
	if !slices.Equal(got, want) {
		t.Errorf("environ = %q, want %q", got, want)
	}
}

// host is a member of the local backend that runs jobs on the host.
var host = &backend.Member{Name: "laptop", Backend: "local", Isolation: "host"}

// place places an attempt on host, and cancels it, and waits for its end,
// when the test ends.
func place(t *testing.T, d *Driver, m *backend.Member, req backend.PlaceRequest) backend.Placement {
	t.Helper()
	if req.Coordinator.Address == "" {
		req.Coordinator.Address = "/nowhere/c.sock"
	}
	p, err := d.Place(t.Context(), m, req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(t, d, p) })
	return p
}

// waitFor waits until cond holds, or fails the test after ten seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(10 * time.Second)
	for !cond() {
		select {
		case <-tick.C:
		case <-deadline:
			t.Fatalf("waited too long for %s", what)
		}
	}
}

// readReport waits for a stub's report, and returns it.
func readReport(t *testing.T, path string) report {
	t.Helper()
	var r report
	waitFor(t, "the worker's report", func() bool {
		data, err := os.ReadFile(path)
		return err == nil && jsonv2.Unmarshal(data, &r) == nil
	})
	return r
}

// check returns what Check says of p.
func check(t *testing.T, d *Driver, p backend.Placement) backend.Status {
	t.Helper()
	s, err := d.Check(t.Context(), host, p)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// exitStatus waits for the exit status the driver keeps of p's worker,
// which it writes once it has reaped it.
func exitStatus(t *testing.T, d *Driver, p backend.Placement) string {
	t.Helper()
	var data []byte
	waitFor(t, "the worker's exit status", func() bool {
		var err error
		data, err = os.ReadFile(d.path(p.Attempt, ".exit"))
		return err == nil
	})
	return string(data)
}

// cancel cancels p, if it runs, and waits until the driver has kept its
// worker's exit status, as it does once it has reaped it, so that the
// test's directories are left alone when they are removed.
func cancel(t *testing.T, d *Driver, p backend.Placement) {
	t.Helper()
	// A cleanup runs once t.Context is done.
	if err := d.Cancel(context.WithoutCancel(t.Context()), host, p); err != nil {
		t.Error(err)
	}
	if _, ok, _ := d.read(p.Attempt); ok {
		exitStatus(t, d, p)
	}
}

// mustStart returns when the process pid started, as the driver says.
// binaryDigest returns the digest of the binary the driver starts as the
// worker, this test's, as the worker reports its own.
func binaryDigest(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err == nil {
		exe, err = filepath.EvalSymlinks(exe)
	}
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(exe) //nolint:gosec // the test's own binary
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func mustStart(t *testing.T, pid int) string {
	t.Helper()
	start, err := processStart(pid)
	if err != nil {
		t.Fatal(err)
	}
	return start
}

// The driver starts the worker detached, in a session of its own, in /,
// with the coordinator's address and none of its own MERIDIAN_ variables,
// and gives it its token on the socket it inherits, once the worker says
// it is ready, and nowhere else; finds it again, and doesn't start another
// for the same attempt; checks that it runs; and cancels it with SIGTERM.
func TestPlace(t *testing.T) {
	t.Parallel()
	d, out := newDriver(t, "run")
	req := backend.PlaceRequest{
		Attempt: "run1-build-1", BootstrapToken: "token-" + strconv.Itoa(os.Getpid()) + "-secret", Resources: backend.Resources{MilliCPU: 500},
		RunDir: "/runs/run1",
	}
	p := place(t, d, host, req)
	r := readReport(t, out)
	switch {
	case !slices.Equal(r.Args, []string{"worker"}):
		t.Errorf("the worker's arguments are %q", r.Args)
	case r.Dir != "/":
		t.Errorf("the worker runs in %s", r.Dir)
	case r.Coordinator != "/nowhere/c.sock":
		t.Errorf("the worker reaches the coordinator at %q", r.Coordinator)
	case r.TokenFD != "3" || r.Token != req.BootstrapToken:
		t.Errorf("the worker's token is %q, on descriptor %q", r.Token, r.TokenFD)
	case len(r.Leaked) > 0:
		t.Errorf("the worker was given %v", r.Leaked)
	case !r.OwnSession:
		t.Error("the worker shares its session, and its terminal's signals")
	case p.Attempt != req.Attempt || p.ID != strconv.Itoa(r.PID)+":"+mustStart(t, r.PID):
		t.Errorf("the placement is %+v, of worker %d", p, r.PID)
	case p.WorkerDigest != binaryDigest(t):
		t.Errorf("the placement says the worker's digest is %q, not %q", p.WorkerDigest, binaryDigest(t))
	case p.RunDir != req.RunDir:
		t.Errorf("the placement says the run's directory is %q", p.RunDir)
	}
	// The token is in no file of the driver's.
	_ = filepath.WalkDir(d.State, func(path string, e fs.DirEntry, err error) error {
		if err == nil && !e.IsDir() {
			if data, err := os.ReadFile(path); err == nil && bytes.Contains(data, []byte(req.BootstrapToken)) {
				t.Errorf("%s holds the token", path)
			}
		}
		return nil
	})
	if found, ok, err := d.Find(t.Context(), host, req.Attempt); err != nil || !ok || found != p {
		t.Errorf("Find gave %+v, %v, %v", found, ok, err)
	}
	if _, ok, err := d.Find(t.Context(), host, "run1-other-1"); ok || err != nil {
		t.Errorf("Find found another attempt: %v", err)
	}
	if s := check(t, d, p); s.State != backend.Running {
		t.Errorf("the worker is %+v, not running", s)
	}
	again, err := d.Place(t.Context(), host, req)
	if err != nil || again != p {
		t.Errorf("placed again, the attempt is %+v, %v", again, err)
	}
	if err := d.Cancel(t.Context(), host, p); err != nil {
		t.Fatal(err)
	}
	if s := check(t, d, p); s.State != backend.Dead || s.End != backend.EndWithdrawn {
		t.Errorf("a cancelled worker is %+v", s)
	}
	// The stub exits 0 on SIGTERM: Cancel didn't need SIGKILL.
	if status := exitStatus(t, d, p); status != "0\n" {
		t.Errorf("the cancelled worker's exit status is %q", status)
	}
	info, err := os.Lstat(filepath.Join(d.State, workersDir, req.Attempt+".log"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("the worker's log: %v, %v", info, err)
	}
	if err := d.Cancel(t.Context(), host, p); err != nil {
		t.Errorf("cancelling an ended placement: %v", err)
	}
}

// Places of one attempt at once start one worker, and give each the same
// placement.
func TestConcurrentPlace(t *testing.T) {
	t.Parallel()
	d, out := newDriver(t, "run")
	req := backend.PlaceRequest{Attempt: "same", Coordinator: backend.Endpoint{Address: "/nowhere/c.sock"}, BootstrapToken: "t"}
	placements := make([]backend.Placement, 8)
	var wg sync.WaitGroup
	for i := range placements {
		wg.Go(func() {
			p, err := d.Place(t.Context(), host, req)
			if err != nil {
				t.Error(err)
			}
			placements[i] = p
		})
	}
	wg.Wait()
	t.Cleanup(func() { cancel(t, d, placements[0]) })
	readReport(t, out)
	for _, p := range placements {
		if p != placements[0] {
			t.Errorf("the placements are %v", placements)
			break
		}
	}
	pids, err := os.ReadFile(out + ".pids")
	if n := strings.Count(string(pids), "\n"); err != nil || n != 1 {
		t.Errorf("%d workers started, not 1: %v", n, err)
	}
}

// A worker that ended of its own is dead, and exited, and stays so after a
// Cancel, which doesn't withdraw it; and Forget removes what the driver
// kept of it, though not of one that runs.
func TestWorkerEnds(t *testing.T) {
	t.Parallel()
	d, out := newDriver(t, "exit")
	p := place(t, d, host, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "unread"})
	readReport(t, out)
	waitFor(t, "the worker to end", func() bool { return check(t, d, p).State == backend.Dead })
	if status := exitStatus(t, d, p); status != "0\n" {
		t.Errorf("the worker's exit status is %q", status)
	}
	if err := d.Cancel(t.Context(), host, p); err != nil {
		t.Fatal(err)
	}
	if s := check(t, d, p); s.State != backend.Dead || s.End != backend.EndExited {
		t.Errorf("an ended worker, cancelled after, is %+v", s)
	}
	if err := d.Forget(t.Context(), host, p); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{d.path("a1", ".json"), d.path("a1", ".exit"), filepath.Join(d.State, workersDir, "a1.log")} {
		if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Forget left %s: %v", path, err)
		}
	}
	if _, ok, err := d.Find(t.Context(), host, "a1"); ok || err != nil {
		t.Errorf("a forgotten placement is found: %v", err)
	}

	running, out := newDriver(t, "run")
	p = place(t, running, host, backend.PlaceRequest{Attempt: "a2", BootstrapToken: "t"})
	readReport(t, out)
	if err := running.Forget(t.Context(), host, p); err == nil {
		t.Error("forgot a worker that runs")
	}
}

// A worker that has ended counts for nothing that a member offers, and an
// attempt whose worker has ended, placed again, as after a coordinator
// that placed it died before it recorded the placement, starts a new one,
// once what the driver kept of the old one is gone.
func TestPlaceAgainAfterEnd(t *testing.T) {
	t.Parallel()
	d, out := newDriver(t, "exit")
	m := &backend.Member{Name: "box", Backend: "local", Isolation: "host", Resources: backend.Resources{MilliCPU: 2000, Memory: 1 << 30}}
	req := backend.PlaceRequest{Attempt: "a1", BootstrapToken: "t", Resources: backend.Resources{MilliCPU: 1500}}
	first := place(t, d, m, req)
	readReport(t, out)
	waitFor(t, "the worker to end", func() bool { return check(t, d, first).State == backend.Dead })
	exitStatus(t, d, first)
	fit, err := d.Probe(t.Context(), m, backend.Demand{Resources: backend.Resources{MilliCPU: 1000}})
	if err != nil || fit.Problem != nil || fit.Availability != backend.StartsNow {
		t.Errorf("1000 thousandths of a CPU of 2000, beside a worker that ended: %+v, %v", fit, err)
	}
	second := place(t, d, m, req)
	if second.ID == first.ID || second.Attempt != req.Attempt {
		t.Errorf("placed again, the attempt is %+v, as it was %+v", second, first)
	}
	waitFor(t, "the second worker to start", func() bool {
		pids, err := os.ReadFile(out + ".pids")
		return err == nil && strings.Count(string(pids), "\n") == 2
	})
	if found, ok, err := d.Find(t.Context(), m, req.Attempt); err != nil || !ok || found != second {
		t.Errorf("Find gave %+v, %v, %v; want %+v", found, ok, err, second)
	}
}

// With a launcher, the driver starts the worker through it, with the
// worker's arguments after it, and records the process the launcher runs
// the worker as, as systemd-run --scope does.
func TestLauncher(t *testing.T) {
	t.Parallel()
	d, out := newDriver(t, "run")
	d.Launcher = []string{"env", launchedEnv + "=yes"}
	p := place(t, d, host, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "t"})
	r := readReport(t, out)
	if r.Launched != "yes" || !slices.Equal(r.Args, []string{"worker"}) || r.Token != "t" || p.ID != strconv.Itoa(r.PID)+":"+mustStart(t, r.PID) {
		t.Errorf("the worker the launcher started reported %+v, placed as %+v", r, p)
	}
	if s := check(t, d, p); s.State != backend.Running {
		t.Errorf("the worker is %+v", s)
	}
}

// With AwaitToken, Place returns only once the worker has taken its
// token, however long it takes to ask for it: a process that ends once it
// has placed the worker, as meridian host does, has handed it over.
func TestAwaitToken(t *testing.T) {
	t.Parallel()
	d, out := newDriver(t, "slow")
	d.AwaitToken = true
	start := time.Now()
	p := place(t, d, host, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "slow-token"})
	if took := time.Since(start); took < time.Second {
		t.Errorf("Place returned %v after it started a worker that asks for its token after a second", took)
	}
	if r := readReport(t, out); r.Token != "slow-token" || p.ID != strconv.Itoa(r.PID)+":"+mustStart(t, r.PID) {
		t.Errorf("the worker reported %+v, placed as %+v", r, p)
	}
}

// With AwaitToken, a worker that ends without taking its token, as one
// that systemd-run couldn't start does, isn't placed: Place says so, with
// its exit status and where what it wrote is, which it keeps, and the
// attempt has no placement left.
func TestTokenNotTaken(t *testing.T) {
	t.Parallel()
	d, out := newDriver(t, "exit")
	d.AwaitToken = true
	_, err := d.Place(t.Context(), host, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "t", Coordinator: backend.Endpoint{Address: "/nowhere/c.sock"}})
	p, _ := errors.AsType[*problem.Problem](err)
	log := filepath.Join(d.State, workersDir, "a1.log")
	wantProblem(t, "a worker that ended without its token", p, problems.WorkerTokenNotTaken, problems.Params{"ended": "true", "exit": 0, "log": log})
	if r := readReport(t, out); r.Token != "" {
		t.Errorf("the worker reported %+v", r)
	}
	if _, found, err := d.Find(t.Context(), host, "a1"); found || err != nil {
		t.Errorf("Find found the placement of a worker without its token: %v", err)
	}
	if _, err := os.Stat(log); err != nil {
		t.Errorf("what the worker wrote is gone: %v", err)
	}
}

// With AwaitToken, a worker that doesn't ask for its token within
// TokenWait is killed, and isn't placed: Place says it didn't end on its
// own, and the attempt has no placement left.
func TestTokenWaitPassed(t *testing.T) {
	t.Parallel()
	d, _ := newDriver(t, "slow")
	d.AwaitToken, d.TokenWait = true, 100*time.Millisecond
	_, err := d.Place(t.Context(), host, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "t", Coordinator: backend.Endpoint{Address: "/nowhere/c.sock"}})
	p, _ := errors.AsType[*problem.Problem](err)
	wantProblem(t, "a worker that didn't ask for its token in time", p, problems.WorkerTokenNotTaken, problems.Params{"ended": "false", "exit": -1})
	if _, found, err := d.Find(t.Context(), host, "a1"); found || err != nil {
		t.Errorf("Find found the placement of a worker without its token: %v", err)
	}
}

// A worker that doesn't stop on SIGTERM gets it, and is killed once
// KillAfter has passed.
func TestCancelKills(t *testing.T) {
	t.Parallel()
	d, out := newDriver(t, "ignore")
	// Time for a loaded machine to deliver SIGTERM, and the stub to note it.
	d.KillAfter = 1500 * time.Millisecond
	p := place(t, d, host, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "t"})
	readReport(t, out)
	start := time.Now()
	if err := d.Cancel(t.Context(), host, p); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < d.KillAfter {
		t.Errorf("the worker was killed after %v, before %v", took, d.KillAfter)
	}
	if s := check(t, d, p); s.State != backend.Dead {
		t.Errorf("a killed worker is %+v", s)
	}
	if signals, err := os.ReadFile(out + ".signals"); err != nil || !strings.HasPrefix(string(signals), "term\n") {
		t.Errorf("the worker didn't get SIGTERM before SIGKILL: %q, %v", signals, err)
	}
	if status := exitStatus(t, d, p); status != "-1\n" {
		t.Errorf("a killed worker's exit status is %q", status)
	}
}

// A placement whose process ID another process has now, which started at
// another time or in another boot, is dead, and Cancel leaves that process
// alone; and a placement without its file is dead.
func TestRecycledPID(t *testing.T) {
	t.Parallel()
	d := &Driver{State: t.TempDir(), KillAfter: 100 * time.Millisecond}
	if err := os.MkdirAll(filepath.Join(d.State, placementsDir), 0o700); err != nil {
		t.Fatal(err)
	}
	boot, err := bootID()
	if err != nil {
		t.Fatal(err)
	}
	other := exec.CommandContext(t.Context(), "sleep", "30")
	if err := other.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = other.Process.Kill()
		_ = other.Wait()
	})
	pid := other.Process.Pid
	for _, tc := range []struct {
		attempt, start, boot string
		want                 backend.State
	}{
		{"same", mustStart(t, pid), boot, backend.Running},
		{"recycled", "1", boot, backend.Dead},
		{"rebooted", mustStart(t, pid), boot + "-before", backend.Dead},
	} {
		r := &record{PID: pid, Start: tc.start, Boot: tc.boot}
		if err := safepath.WriteFile(d.path(tc.attempt, ".json"), mustMarshal(r)); err != nil {
			t.Fatal(err)
		}
		p := backend.Placement{Attempt: tc.attempt, ID: r.id()}
		if s := check(t, d, p); s.State != tc.want {
			t.Errorf("%s: %+v, want %v", tc.attempt, s, tc.want)
		}
		if tc.want == backend.Dead {
			if err := d.Cancel(t.Context(), host, p); err != nil {
				t.Errorf("%s: cancelling: %v", tc.attempt, err)
			}
			if err := other.Process.Signal(syscall.Signal(0)); err != nil {
				t.Errorf("%s: cancelling signalled the process that has its ID now: %v", tc.attempt, err)
			}
		}
	}
	if s := check(t, d, backend.Placement{Attempt: "none", ID: strconv.Itoa(pid) + ":1"}); s.State != backend.Dead {
		t.Errorf("a placement without its file is %+v", s)
	}
}

// wantProblem checks that p is a problem of kind k with params.
func wantProblem(t *testing.T, what string, p *problem.Problem, k *problems.Kind, params problems.Params) {
	t.Helper()
	if p == nil || p.Code != k.Code() {
		t.Errorf("%s: %v, want %s", what, p, k.Code())
		return
	}
	for name, want := range params {
		if got := p.Params[name]; got != want {
			t.Errorf("%s: its %s is %v, not %v", what, name, got, want)
		}
	}
}

// The driver runs jobs on the host only: a member that names any other
// isolation, or none, which is a container, has isolation_not_built, and
// the driver starts nothing for it rather than run it on the host.
func TestIsolation(t *testing.T) {
	t.Parallel()
	d, out := newDriver(t, "run")
	for _, isolation := range []string{"", "podman", "docker", "apptainer", "container"} {
		m := &backend.Member{Name: "box", Backend: "local", Isolation: isolation}
		fit, err := d.Probe(t.Context(), m, backend.Demand{})
		if err != nil {
			t.Fatal(err)
		}
		has := "false"
		if isolation != "" {
			has = "true"
		}
		wantProblem(t, "isolation "+isolation, fit.Problem, problems.IsolationNotBuilt, problems.Params{"member": "box", "isolation": isolation, "has_isolation": has})
		_, err = d.Place(t.Context(), m, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "t"})
		p, _ := errors.AsType[*problem.Problem](err)
		wantProblem(t, "placing with isolation "+isolation, p, problems.IsolationNotBuilt, nil)
	}
	if _, err := os.Lstat(out + ".pids"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a worker started: %v", err)
	}
	if fit, err := d.Probe(t.Context(), host, backend.Demand{}); err != nil || fit.Problem != nil || fit.Availability != backend.StartsNow {
		t.Errorf("a member of host isolation: %+v, %v", fit, err)
	}
}

// A member offers what it declares, or else the host's CPUs and memory,
// and no GPU; a job that asks for more doesn't fit, and one that fits only
// once the live workers end has capacity, but doesn't start now.
func TestResources(t *testing.T) {
	t.Parallel()
	d, out := newDriver(t, "run")
	m := &backend.Member{Name: "box", Backend: "local", Isolation: "host", Resources: backend.Resources{MilliCPU: 2000, Memory: 1 << 30}}
	for _, tc := range []struct {
		resource     string
		asks, offers int64
		demand       backend.Resources
	}{
		{"cpu", 3000, 2000, backend.Resources{MilliCPU: 3000}},
		{"memory", 2 << 30, 1 << 30, backend.Resources{Memory: 2 << 30}},
		{"gpu", 1, 0, backend.Resources{GPU: 1}},
	} {
		fit, err := d.Probe(t.Context(), m, backend.Demand{Resources: tc.demand})
		if err != nil {
			t.Fatal(err)
		}
		wantProblem(t, tc.resource, fit.Problem, problems.ResourcesExceedMember, problems.Params{"member": "box", "resource": tc.resource, "asks": tc.asks, "offers": tc.offers})
	}
	if fit, err := d.Probe(t.Context(), &backend.Member{Name: "host", Isolation: "host"}, backend.Demand{Resources: backend.Resources{MilliCPU: 1000, Memory: 1 << 20}}); err != nil || fit.Problem != nil {
		t.Errorf("a job the host can run: %+v, %v", fit, err)
	}
	place(t, d, m, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "t", Resources: backend.Resources{MilliCPU: 1500}})
	readReport(t, out)
	for demand, want := range map[int64]backend.Availability{500: backend.StartsNow, 1000: backend.HasCapacity} {
		fit, err := d.Probe(t.Context(), m, backend.Demand{Resources: backend.Resources{MilliCPU: demand}})
		if err != nil || fit.Problem != nil || fit.Availability != want {
			t.Errorf("%d thousandths of a CPU beside 1500 of 2000: %+v, %v; want %v", demand, fit, err, want)
		}
	}
	// A parked attempt, whose worker waits for a child run, holds none of
	// what it asked for; an attempt the driver didn't place, parked, frees
	// nothing.
	for _, tc := range []struct {
		parked []string
		want   backend.Availability
	}{
		{[]string{"a1"}, backend.StartsNow},
		{[]string{"elsewhere"}, backend.HasCapacity},
		{nil, backend.HasCapacity},
	} {
		fit, err := d.Probe(t.Context(), m, backend.Demand{Resources: backend.Resources{MilliCPU: 1000}, Parked: tc.parked})
		if err != nil || fit.Problem != nil || fit.Availability != tc.want {
			t.Errorf("1000 thousandths of a CPU beside 1500 of 2000, parked %v: %+v, %v; want %v", tc.parked, fit, err, tc.want)
		}
	}
}

// The driver starts only a binary of the user's or root's that no other
// user can change, nor the way to it, and records its digest.
func TestExecutable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	exe := filepath.Join(dir, "meridian")
	if err := os.WriteFile(exe, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	digest, err := checkExecutable(exe, exe)
	sum := sha256.Sum256([]byte("#!/bin/sh\n"))
	if err != nil || digest != "sha256:"+hex.EncodeToString(sum[:]) {
		t.Errorf("a safe binary: %s, %v", digest, err)
	}
	if err := os.Chmod(exe, 0o775); err != nil {
		t.Fatal(err)
	}
	_, err = checkExecutable(exe, exe)
	p, _ := errors.AsType[*problem.Problem](err)
	wantProblem(t, "a binary others can change", p, problems.WorkerBinaryUnsafe, problems.Params{"path": exe, "code": string(problems.FileChangeable.Code())})
	_, err = checkExecutable(dir, dir)
	p, _ = errors.AsType[*problem.Problem](err)
	wantProblem(t, "a directory", p, problems.WorkerBinaryUnsafe, problems.Params{"code": string(problems.FileNotRegular.Code())})
	shared := filepath.Join(dir, "shared")
	if err := os.Mkdir(shared, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0o777); err != nil {
		t.Fatal(err)
	}
	inShared := filepath.Join(shared, "meridian")
	if err := os.WriteFile(inShared, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err = checkExecutable(inShared, inShared)
	p, _ = errors.AsType[*problem.Problem](err)
	wantProblem(t, "a binary in a directory others can change", p, problems.WorkerBinaryUnsafe, problems.Params{"code": string(problems.PathUnsafe.Code())})
}

// An attempt's ID names its files, so one that can't is refused, and
// nothing is made of it.
// TestDigestKept checks that the digest of the binary a worker runs is
// kept while the file is as it was hashed, and taken again once it
// changed, even with the time its data last changed set back; and that a
// file whose status changed less than a second before isn't kept, so that
// a change within the clock's tick can't go unseen.
func TestDigestKept(t *testing.T) {
	var hashed int
	hash := hashFile
	hashFile = func(r io.Reader) (string, error) {
		hashed++
		return hash(r)
	}
	t.Cleanup(func() { hashFile, settled = hash, time.Second })
	exe := filepath.Join(t.TempDir(), "meridian")
	if err := os.WriteFile(exe, nil, 0o755); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(exe); err != nil {
		t.Fatal(err)
	} else if _, ok := identity(info); !ok {
		t.Skip("the digest is kept only on Linux")
	}
	write := func(content string) string {
		t.Helper()
		if err := os.WriteFile(exe, []byte(content), 0o755); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte(content))
		return "sha256:" + hex.EncodeToString(sum[:])
	}
	check := func(want string, hashes int) {
		t.Helper()
		if got, err := checkExecutable(exe, exe); err != nil || got != want || hashed != hashes {
			t.Errorf("the digest is %s, %v, after %d hashes; want %s after %d", got, err, hashed, want, hashes)
		}
	}

	settled = 0
	one := write("#!/bin/sh\necho 1\n")
	check(one, 1)
	check(one, 1)
	// The same size, with the time its data changed set back: its status
	// changed, which nothing sets back.
	info, err := os.Stat(exe)
	if err != nil {
		t.Fatal(err)
	}
	two := write("#!/bin/sh\necho 2\n")
	if err := os.Chtimes(exe, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	check(two, 2)
	check(two, 2)

	settled = time.Hour
	three := write("#!/bin/sh\necho 3\n")
	check(three, 3)
	check(three, 4)
}

// TestLiveIndex checks what Probe counts of the placements the state
// directory holds: the workers that run, as live/ names them, a placement
// a release before live/ made among them, which the driver marks live the
// first time it probes; not one whose worker ended, which it takes out of
// live/; and that it removes the placements of workers that ended more
// than a day before, with their logs, and keeps those of workers that
// ended since.
func TestLiveIndex(t *testing.T) {
	d := &Driver{State: t.TempDir()}
	for _, dir := range []string{placementsDir, workersDir, liveDir} {
		if err := os.MkdirAll(filepath.Join(d.State, dir), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	boot, err := bootID()
	if err != nil {
		t.Fatal(err)
	}
	running := exec.CommandContext(t.Context(), "sleep", "30")
	if err := running.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = running.Process.Kill()
		_ = running.Wait()
	})
	pid := running.Process.Pid
	put := func(attempt string, r *record, live bool, age time.Duration) {
		t.Helper()
		if err := safepath.WriteFile(d.path(attempt, ".json"), mustMarshal(r)); err != nil {
			t.Fatal(err)
		}
		log := filepath.Join(d.State, workersDir, attempt+".log")
		if err := os.WriteFile(log, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if live {
			if err := d.markLive(attempt); err != nil {
				t.Fatal(err)
			}
		}
		then := time.Now().Add(-age)
		if err := os.Chtimes(d.path(attempt, ".json"), then, then); err != nil {
			t.Fatal(err)
		}
	}
	res := backend.Resources{MilliCPU: 1000}
	put("runs", &record{PID: pid, Start: mustStart(t, pid), Boot: boot, Resources: res}, true, 0)
	put("older-release", &record{PID: pid, Start: mustStart(t, pid), Boot: boot, Resources: res}, false, 0)
	put("ended", &record{PID: pid, Start: "1", Boot: boot, Resources: res}, true, 0)
	put("stale", &record{PID: pid, Start: "1", Boot: boot, Resources: res}, false, 48*time.Hour)

	m := &backend.Member{Name: "box", Backend: "local", Isolation: "host", Resources: backend.Resources{MilliCPU: 2500, Memory: 1 << 30}}
	for demand, want := range map[int64]backend.Availability{500: backend.StartsNow, 600: backend.HasCapacity} {
		fit, err := d.Probe(t.Context(), m, backend.Demand{Resources: backend.Resources{MilliCPU: demand}})
		if err != nil || fit.Problem != nil || fit.Availability != want {
			t.Errorf("%d thousandths of a CPU beside two workers of 1000 each: %+v, %v; want %v", demand, fit, err, want)
		}
	}
	live, err := os.ReadDir(filepath.Join(d.State, liveDir))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range live {
		names = append(names, e.Name())
	}
	if !slices.Equal(names, []string{"older-release", "runs"}) {
		t.Errorf("live/ holds %v", names)
	}
	for attempt, kept := range map[string]bool{"runs": true, "older-release": true, "ended": true, "stale": false} {
		for _, path := range []string{d.path(attempt, ".json"), filepath.Join(d.State, workersDir, attempt+".log")} {
			if _, err := os.Lstat(path); (err == nil) != kept {
				t.Errorf("%s is there: %t, want %t", path, err == nil, kept)
			}
		}
	}
}

func TestAttemptID(t *testing.T) {
	t.Parallel()
	d, out := newDriver(t, "run")
	for _, id := range []string{"", "../escape", "a/b", ".hidden", strings.Repeat("a", 200)} {
		if _, err := d.Place(t.Context(), host, backend.PlaceRequest{Attempt: id, BootstrapToken: "t"}); err == nil {
			t.Errorf("placed attempt %q", id)
		}
		if _, ok, err := d.Find(t.Context(), host, id); ok || err != nil {
			t.Errorf("found attempt %q: %v", id, err)
		}
	}
	if _, err := os.Lstat(out + ".pids"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a worker started: %v", err)
	}
}
