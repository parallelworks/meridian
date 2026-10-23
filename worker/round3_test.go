package worker

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// TestOutputsRemoved checks that a step whose $OUTPUTS goes as it runs,
// with the directory it is in, has its event say so, and that the worker
// reads nothing from that path after: not what the step wrote there once
// it was made again, as another user could make it, nor anything else;
// and that the steps after it have a directory, and $OUTPUTS, again.
func TestOutputsRemoved(t *testing.T) {
	t.Parallel()
	removed := command(`d="$(dirname "$OUTPUTS")"; rm -rf "$d"; echo a=1 >> "$OUTPUTS" 2>/dev/null; true`)
	remade := command(`d="$(dirname "$OUTPUTS")"; rm -rf "$d"; mkdir -m 755 "$d"; echo token=s3cret >> "$OUTPUTS"`)
	after := command(`echo b=2 >> "$OUTPUTS"`)
	for _, s := range []*plan.Step{&remade, &after} {
		s.If = cond("always()")
	}
	f := newFakeCoordinator(t, newJob(removed, remade, after))
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success 0", "started 1.0", "finished 1.0 success 0", "started 2.0", "finished 2.0 success 0")
	for step := range 2 {
		e := f.finishedEvent(step, 0, false)
		if !slices.Equal(codes(e.Diagnostics), []string{"outputs_removed"}) || len(e.Outputs) != 0 {
			t.Errorf("step %d's outputs are %v, and its diagnostics %v", step, e.Outputs, codes(e.Diagnostics))
		}
	}
	if e := f.finishedEvent(2, 0, false); e.Outputs["b"] != "2" || len(e.Diagnostics) != 0 {
		t.Errorf("the step after has the outputs %v, and the diagnostics %v", e.Outputs, codes(e.Diagnostics))
	}
	if end := checkEnd(t, f, plan.OutcomeSuccess, ""); end.Outputs["token"] != "" {
		t.Errorf("the worker read the outputs at the remade path: %v", end.Outputs)
	}
}

// TestGlobLeavesDir checks that a step's rm -rf "$TMPDIR"/* passes the
// worker's directory by, as its name starts with a dot, so the step's
// $OUTPUTS is still read.
func TestGlobLeavesDir(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	f := newFakeCoordinator(t, newJob(command(`rm -rf "$TMPDIR"/*; echo a=1 >> "$OUTPUTS"`)))
	if err := f.runWorker(t.Context(), Config{Dir: tmp, Environ: []string{"TMPDIR=" + tmp}}); err != nil {
		t.Fatal(err)
	}
	if e := f.finishedEvent(0, 0, false); e.Outputs["a"] != "1" || len(e.Diagnostics) != 0 {
		t.Errorf("the step's outputs are %v, and its diagnostics %v", e.Outputs, codes(e.Diagnostics))
	}
}

// TestPrivateParent checks where a worker makes its directory: in Dir, if
// the config names one that no other user can change; else in
// meridian/worker in the
// user's cache directory, which it makes, if no other user can change the
// way to it; else in XDG_RUNTIME_DIR, if no other user can change it,
// where it spools less; and else in the system's temporary directory, with
// a warning. It never makes it in the run's directory.
func TestPrivateParent(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	mkdir := func(name string, mode os.FileMode) string {
		t.Helper()
		p := filepath.Join(base, name)
		if err := os.MkdirAll(p, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
		return p
	}
	config, xdg, open := mkdir("config", 0o755), mkdir("xdg", 0o700), mkdir("open", 0o775)
	home, shared := mkdir("home", 0o700), mkdir("shared", 0o777)
	cache := filepath.Join(home, ".cache", "meridian", "worker")
	if runtime.GOOS == "darwin" {
		cache = filepath.Join(home, "Library", "Caches", "meridian", "worker")
	}
	for _, tc := range []struct {
		name, dir, home, xdg string
		want                 string
		memory, warns        bool
	}{
		{name: "config", dir: config, home: home, xdg: xdg, want: config},
		{name: "cache", home: home, xdg: xdg, want: cache},
		{name: "config not private", dir: open, home: home, want: cache, warns: true},
		{name: "runtime", home: shared, xdg: xdg, want: xdg, memory: true, warns: true},
		{name: "temporary", xdg: open, want: os.TempDir(), warns: true},
	} {
		var log bytes.Buffer
		environ := []string{"HOME=" + tc.home, "XDG_RUNTIME_DIR=" + tc.xdg, "XDG_CACHE_HOME="}
		d, err := makePrivateDir(privateParents(tc.dir, environ), slog.New(slog.NewTextHandler(&log, nil)))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		path, _, _, err := d.current()
		if err != nil || filepath.Dir(path) != tc.want || d.inMemory() != tc.memory || (log.Len() > 0) != tc.warns {
			t.Errorf("%s: made %q (%v), in memory %t, warned %q; want it in %q", tc.name, path, err, d.inMemory(), log.String(), tc.want)
		}
		d.removeAll()
	}
	if _, err := checkPrivate(cache); err != nil {
		t.Errorf("the cache's directory: %v", err)
	}
}

// TestParentRemoved checks that, once a step has removed the parent of
// the worker's directory, the worker makes it again, if it made it, or
// else makes its directory in the next parent it would have used.
func TestParentRemoved(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	cache := filepath.Join(base, "cache", "meridian", "worker")
	given := filepath.Join(base, "given")
	if err := os.Mkdir(given, 0o700); err != nil {
		t.Fatal(err)
	}
	d, err := makePrivateDir(privateParents(given, []string{"XDG_CACHE_HOME=" + filepath.Join(base, "cache")}), testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer d.removeAll()
	for _, want := range []string{cache, cache} {
		path, _, _, err := d.current()
		if err != nil {
			t.Fatal(err)
		}
		// The first time, the given directory goes, and the worker moves
		// on to its cache; the second, the cache's, which it makes again.
		if err := os.RemoveAll(filepath.Dir(path)); err != nil {
			t.Fatal(err)
		}
		again, _, _, err := d.current()
		if err != nil || filepath.Dir(again) != want {
			t.Errorf("after %s was removed, the worker uses %q (%v), want one in %s", filepath.Dir(path), again, err, want)
		}
	}
}

// TestCleanupRemovesRunDir checks that a cleanup: that removes the run's
// directory, as rm -r ${PW_JOB_DIR} does in workflows we know of, leaves
// the cleanups after it, and the worker's files, as they were: the
// worker's directory is outside the run's.
func TestCleanupRemovesRunDir(t *testing.T) {
	t.Parallel()
	first := command("true")
	first.Cleanup = plan.Literal("echo cleaned first")
	second := command("echo two=2 >> $OUTPUTS")
	second.Cleanup = plan.Literal(`rm -r "$PW_JOB_DIR"`)
	f := newFakeCoordinator(t, newJob(first, second))
	home := t.TempDir()
	if err := f.runWorker(t.Context(), Config{Environ: []string{"HOME=" + home}}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 success 0",
		"started 1.0", "finished 1.0 success 0",
		"cleanup started 1.0", "cleanup finished 1.0 success 0",
		"cleanup started 0.0", "cleanup finished 0.0 success 0",
	)
	end := checkEnd(t, f, plan.OutcomeSuccess, "")
	if end.Outputs["two"] != "2" {
		t.Errorf("the job's outputs are %v", end.Outputs)
	}
}

// TestProblemsMaskedEscaped checks that a problem the worker makes is
// masked before its message is made, so that a secret with a character
// the message escapes, such as a tab, isn't quoted escaped.
func TestProblemsMaskedEscaped(t *testing.T) {
	t.Parallel()
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	f := newFakeCoordinator(t, newJob(command("true")))
	f.claim.Run = plan.RunValues{Format: 1, Vars: map[string]string{"token": "hun\tter2"}, Secrets: []string{"vars.token"}}
	f.claim.RunDir = filepath.Join(file, "hun\tter2")
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	end := checkEnd(t, f, plan.OutcomeFailure, plan.ReasonSetupFailed)
	if end.Problem == nil || strings.Contains(end.Problem.Detail, "ter2") {
		t.Errorf("the problem quotes the secret: %+v", end.Problem)
	}
	if setup, _ := f.log(logKey{part: workerapi.LogSetup}); strings.Contains(setup, "ter2") {
		t.Errorf("the setup log quotes the secret: %q", setup)
	}
}

// TestNoZombiesLeft checks that once Run returns, the worker has reaped
// every step's process group leader it held, though a step left a process
// in its group. It doesn't run in parallel, as other tests' workers hold
// leaders of their own.
func TestNoZombiesLeft(t *testing.T) {
	f := newFakeCoordinator(t, newJob(command(`nohup sleep 1 >/dev/null 2>&1 </dev/null &`), command("sleep 1.5")))
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	me := strconv.Itoa(os.Getpid())
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Skip("no /proc")
	}
	for _, e := range entries {
		data, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		_, after, ok := bytes.Cut(data, []byte(") "))
		if !ok {
			continue
		}
		if fields := strings.Fields(string(after)); len(fields) > 1 && fields[0] == "Z" && fields[1] == me {
			t.Errorf("a zombie child, %s, is left", e.Name())
		}
	}
}
