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
}

// stub is a stub worker: for mode run, it takes its token as the worker
// does, saying it is ready on the socket it inherits and reading the token
// from it, and runs until SIGTERM; for ignore, it does too, but takes no
// action on SIGTERM but to note it in its report's .signals, and runs
// until it is killed; for exit, it exits at once, without its token.
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
func newDriver(t *testing.T, mode string) (*Driver, string) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "report.json")
	env := append(os.Environ(), stubEnv+"="+mode, reportEnv+"="+out, "MERIDIAN_LEAK=yes",
		workerapi.EnvBootstrapToken+"=not-this-one", workerapi.EnvBootstrapTokenFile+"=/not/this/one")
	return &Driver{State: filepath.Join(t.TempDir(), "state"), Environ: env}, out
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
	req := backend.PlaceRequest{Attempt: "run1-build-1", BootstrapToken: "token-" + strconv.Itoa(os.Getpid()) + "-secret", Resources: backend.Resources{MilliCPU: 500}}
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

// A worker that doesn't stop on SIGTERM gets it, and is killed once
// KillAfter has passed.
func TestCancelKills(t *testing.T) {
	t.Parallel()
	d, out := newDriver(t, "ignore")
	d.KillAfter = 300 * time.Millisecond
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
		if err := d.writeFile(d.path(tc.attempt, ".json"), mustMarshal(r)); err != nil {
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
