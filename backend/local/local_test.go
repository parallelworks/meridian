//go:build linux || darwin || freebsd

package local

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	jsonv2 "encoding/json/v2"
	"errors"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
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
// reportEnv names, and then does what the env var's value says.
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
	TokenFile   string   `json:"tokenFile"`
	Token       string   `json:"token"`
	TokenMode   uint32   `json:"tokenMode"`
	DirMode     uint32   `json:"dirMode"`
	// Leaked are the MERIDIAN_ variables it was given but those the driver
	// sets.
	Leaked []string `json:"leaked"`
	// OwnSession says that it leads a session of its own.
	OwnSession bool `json:"ownSession"`
}

// stub is a stub worker: for mode run, it reads its token as the worker
// does, removing its file, and runs until SIGTERM; for ignore, it ignores
// SIGTERM, and runs until it is killed; for exit, it exits at once,
// without reading its token.
func stub(mode string) int {
	terms := make(chan os.Signal, 1)
	if mode == "ignore" {
		signal.Ignore(syscall.SIGTERM)
	} else {
		signal.Notify(terms, syscall.SIGTERM)
	}
	r := report{PID: os.Getpid(), Args: os.Args[1:], Coordinator: os.Getenv(workerapi.EnvCoordinator), TokenFile: os.Getenv(workerapi.EnvBootstrapTokenFile)}
	r.Dir, _ = os.Getwd()
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(name, "MERIDIAN_") && name != workerapi.EnvCoordinator && name != workerapi.EnvBootstrapTokenFile {
			r.Leaked = append(r.Leaked, name)
		}
	}
	sid, err := unix.Getsid(0)
	r.OwnSession = err == nil && sid == os.Getpid()
	if info, err := os.Lstat(r.TokenFile); err == nil {
		r.TokenMode = uint32(info.Mode().Perm())
	}
	if info, err := os.Lstat(filepath.Dir(r.TokenFile)); err == nil {
		r.DirMode = uint32(info.Mode().Perm())
	}
	if mode != "exit" {
		data, _ := os.ReadFile(r.TokenFile)
		r.Token = string(data)
		_ = os.Remove(r.TokenFile)
	}
	data, _ := jsonv2.Marshal(r)
	out := os.Getenv(reportEnv)
	_ = os.WriteFile(out+".new", data, 0o600)
	_ = os.Rename(out+".new", out)
	switch mode {
	case "exit":
		return 0
	case "ignore":
		<-time.After(time.Minute)
		return 1
	}
	select {
	case <-terms:
		return 0
	case <-time.After(time.Minute):
		return 1
	}
}

// newDriver returns a driver whose workers are stubs of mode, with a
// MERIDIAN_ variable of the test's own that they mustn't get, and the
// path their reports go to.
func newDriver(t *testing.T, mode string) (*Driver, string) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "report.json")
	env := append(os.Environ(), stubEnv+"="+mode, reportEnv+"="+out, "MERIDIAN_LEAK=yes", workerapi.EnvBootstrapToken+"=not-this-one")
	return &Driver{State: filepath.Join(t.TempDir(), "state"), Environ: env}, out
}

// host is a member of the local backend that runs jobs on the host.
var host = &backend.Member{Name: "laptop", Backend: "local", Isolation: "host"}

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

// The driver starts the worker detached, in a session of its own, with
// the token in a file only the user can read, in a directory only they can
// use, the coordinator's address, and none of its own MERIDIAN_ variables;
// finds it again, and doesn't start another for the same attempt; checks
// that it runs; and cancels it with SIGTERM.
func TestPlace(t *testing.T) {
	t.Parallel()
	d, out := newDriver(t, "run")
	req := backend.PlaceRequest{Attempt: "run1-build-1", Coordinator: backend.Endpoint{Address: "/nowhere/c.sock"}, BootstrapToken: "token-" + strconv.Itoa(os.Getpid()), Resources: backend.Resources{MilliCPU: 500}}
	p, err := d.Place(t.Context(), host, req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(t, d, p) })
	r := readReport(t, out)
	switch {
	case !slices.Equal(r.Args, []string{"worker"}):
		t.Errorf("the worker's arguments are %q", r.Args)
	case r.Dir != "/":
		t.Errorf("the worker runs in %s", r.Dir)
	case r.Coordinator != "/nowhere/c.sock":
		t.Errorf("the worker reaches the coordinator at %q", r.Coordinator)
	case r.Token != req.BootstrapToken:
		t.Errorf("the worker's token is %q", r.Token)
	case r.TokenMode != 0o600 || r.DirMode != 0o700:
		t.Errorf("the token's file is %#o, in a directory of %#o", r.TokenMode, r.DirMode)
	case len(r.Leaked) > 0:
		t.Errorf("the worker was given %v", r.Leaked)
	case !r.OwnSession:
		t.Error("the worker shares its session, and its terminal's signals")
	case p.Attempt != req.Attempt || p.ID != strconv.Itoa(r.PID)+":"+mustStart(t, r.PID):
		t.Errorf("the placement is %+v, of worker %d", p, r.PID)
	}
	if _, err := os.Lstat(r.TokenFile); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the token's file is still there: %v", err)
	}
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
	info, err := os.Lstat(filepath.Join(d.State, workersDir, req.Attempt+".log"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("the worker's log: %v, %v", info, err)
	}
	if err := d.Cancel(t.Context(), host, p); err != nil {
		t.Errorf("cancelling an ended placement: %v", err)
	}
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
	waitFor(t, "the worker's exit status", func() bool {
		_, err := os.Lstat(filepath.Join(d.State, placementsDir, p.Attempt+".exit"))
		return err == nil
	})
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

// A worker that ended without reading its token is dead, and the driver
// removes the token's file.
func TestWorkerEnds(t *testing.T) {
	t.Parallel()
	d, out := newDriver(t, "exit")
	p, err := d.Place(t.Context(), host, backend.PlaceRequest{Attempt: "a1", Coordinator: backend.Endpoint{Address: "/nowhere/c.sock"}, BootstrapToken: "unread"})
	if err != nil {
		t.Fatal(err)
	}
	r := readReport(t, out)
	waitFor(t, "the worker to end", func() bool { return check(t, d, p).State == backend.Dead })
	if s := check(t, d, p); s.End != backend.EndExited {
		t.Errorf("an ended worker is %+v", s)
	}
	if _, err := os.Lstat(r.TokenFile); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the unread token's file is still there: %v", err)
	}
	waitFor(t, "the worker's exit status", func() bool {
		data, err := os.ReadFile(filepath.Join(d.State, placementsDir, "a1.exit"))
		return err == nil && string(data) == "0\n"
	})
}

// A worker that doesn't stop on SIGTERM is killed once KillAfter has
// passed.
func TestCancelKills(t *testing.T) {
	t.Parallel()
	d, out := newDriver(t, "ignore")
	d.KillAfter = 300 * time.Millisecond
	p, err := d.Place(t.Context(), host, backend.PlaceRequest{Attempt: "a1", Coordinator: backend.Endpoint{Address: "/nowhere/c.sock"}, BootstrapToken: "t"})
	if err != nil {
		t.Fatal(err)
	}
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
}

// A placement whose process ID another process has now, which started at
// another time or in another boot, is dead; and so is one without its
// file.
func TestRecycledPID(t *testing.T) {
	t.Parallel()
	d := &Driver{State: t.TempDir()}
	if err := os.MkdirAll(filepath.Join(d.State, placementsDir), 0o700); err != nil {
		t.Fatal(err)
	}
	boot, err := bootID()
	if err != nil {
		t.Fatal(err)
	}
	pid := os.Getpid()
	for _, tc := range []struct {
		attempt, start, boot string
		want                 backend.State
	}{
		{"same", mustStart(t, pid), boot, backend.Running},
		{"recycled", "1", boot, backend.Dead},
		{"rebooted", mustStart(t, pid), boot + "-before", backend.Dead},
	} {
		r := &record{PID: pid, Start: tc.start, Boot: tc.boot}
		if err := d.writeRecord(tc.attempt, r); err != nil {
			t.Fatal(err)
		}
		if s := check(t, d, backend.Placement{Attempt: tc.attempt, ID: r.id()}); s.State != tc.want {
			t.Errorf("%s: %+v, want %v", tc.attempt, s, tc.want)
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
	if _, err := os.Lstat(out); !errors.Is(err, fs.ErrNotExist) {
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
	p, err := d.Place(t.Context(), m, backend.PlaceRequest{Attempt: "a1", Coordinator: backend.Endpoint{Address: "/nowhere/c.sock"}, BootstrapToken: "t", Resources: backend.Resources{MilliCPU: 1500}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(t, d, p) })
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
	got, digest, err := checkExecutable(exe)
	sum := sha256.Sum256([]byte("#!/bin/sh\n"))
	if err != nil || got != exe || digest != "sha256:"+hex.EncodeToString(sum[:]) {
		t.Errorf("a safe binary: %s, %s, %v", got, digest, err)
	}
	if err := os.Chmod(exe, 0o775); err != nil {
		t.Fatal(err)
	}
	_, _, err = checkExecutable(exe)
	p, _ := errors.AsType[*problem.Problem](err)
	wantProblem(t, "a binary others can change", p, problems.WorkerBinaryUnsafe, problems.Params{"path": exe, "code": string(problems.FileChangeable.Code())})
	_, _, err = checkExecutable(dir)
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
	_, _, err = checkExecutable(inShared)
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
	if _, err := os.Lstat(out); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a worker started: %v", err)
	}
}
