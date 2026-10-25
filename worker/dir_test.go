package worker

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// TestDirReplaced checks that once a step removes the worker's directory
// and puts something else at its path, here a link to a directory another
// user could have made, the worker uses none of it: it makes a new
// directory, with a new name.
func TestDirReplaced(t *testing.T) {
	t.Parallel()
	other := t.TempDir()
	f := newFakeCoordinator(t, newJob(
		command(`d="$(dirname "$OUTPUTS")"; rm -rf "$d"; ln -s "$OTHER" "$d"; echo "$OUTPUTS"`),
		command(`ls -A "$OTHER"; echo "$OUTPUTS"; echo a=1 >> "$OUTPUTS"`),
	))
	if err := f.runWorker(t.Context(), Config{Environ: []string{"OTHER=" + other}}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success 0", "started 1.0", "finished 1.0 success 0")
	before, _ := f.log(logKey{workerapi.LogStep, 0, 0})
	after, _ := f.log(logKey{workerapi.LogStep, 1, 0})
	if entries, _ := os.ReadDir(other); len(entries) != 0 || strings.Count(after, "\n") != 1 {
		t.Errorf("the worker wrote %d files in the directory the link names: %q", len(entries), after)
	}
	if filepath.Dir(after) == filepath.Dir(before) {
		t.Errorf("the second step's $OUTPUTS is in the old directory: %q, then %q", before, after)
	}
	if got := f.finishedEvent(1, 0, false).Outputs; got["a"] != "1" {
		t.Errorf("the second step's outputs are %v", got)
	}
}

// TestPrivateDir checks that a directory the worker makes is its user's,
// and only its user can use it, and that it refuses one that isn't.
func TestPrivateDir(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	d, err := makePrivateDir([]parentDir{{path: parent}}, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer d.removeAll()
	path, _, _, err := d.current()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := checkPrivate(path); err != nil {
		t.Error(err)
	}
	open := filepath.Join(parent, "open")
	if err := os.Mkdir(open, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o750); err != nil { //nolint:gosec // a directory others can use, which the worker refuses
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{open, link} {
		if _, err := checkPrivate(p); err == nil {
			t.Errorf("%s passed as private", p)
		}
	}
	// Removed, and made again elsewhere.
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	again, _, _, err := d.current()
	if err != nil || again == path {
		t.Errorf("after its directory was removed and made again, the worker uses %q (%v)", again, err)
	}
}

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

// TestSpoolLimitFollowsParent checks that the spool's default limit is
// that of the directory the worker keeps its files in now: once it has
// moved from a parent on disk to XDG_RUNTIME_DIR, in memory, the smaller.
func TestSpoolLimitFollowsParent(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	given, xdg := filepath.Join(base, "given"), filepath.Join(base, "xdg")
	for _, dir := range []string{given, xdg} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	d, err := makePrivateDir(privateParents(given, []string{"XDG_RUNTIME_DIR=" + xdg}), testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer d.removeAll()
	s := newSpool(d, 0)
	if got := s.limit(); got != DefaultSpoolLimit {
		t.Errorf("on disk, the limit is %d, want %d", got, DefaultSpoolLimit)
	}
	if err := os.RemoveAll(given); err != nil {
		t.Fatal(err)
	}
	if path, _, _, err := d.current(); err != nil || filepath.Dir(path) != xdg {
		t.Fatalf("the worker moved to %q (%v), want %s", path, err, xdg)
	}
	if got := s.limit(); got != memorySpoolLimit {
		t.Errorf("in memory, the limit is %d, want %d", got, memorySpoolLimit)
	}
	if got := newSpool(d, 1000).limit(); got != 1000 {
		t.Errorf("a limit given is %d, want 1000", got)
	}
}

// TestPrivateDirProblems checks that a worker that can make its directory
// in none of its parents says why for each, as a problem, the first for
// the parent it would have used first, and the same again once it has
// none left to try; and that a step that can't start for one of them has
// that problem on its event, at the step.
func TestPrivateDirProblems(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	open, file := filepath.Join(base, "open"), filepath.Join(base, "file")
	if err := os.Mkdir(open, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o775); err != nil { //nolint:gosec // a directory others can change, which the worker refuses
		t.Fatal(err)
	}
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	d := &privateDir{parents: []parentDir{{path: open}, {path: file, shared: true}}, log: testLogger(t)}
	err := d.fresh()
	var codes []string
	for _, e := range err.(interface{ Unwrap() []error }).Unwrap() { //nolint:errorlint // the problems fresh joins, each in turn
		if p, ok := errors.AsType[*problem.Problem](e); ok {
			codes = append(codes, string(p.Code))
		}
	}
	if want := []string{"directory_not_private", "directory_unavailable"}; !slices.Equal(codes, want) {
		t.Errorf("the problems are %v, want %v", codes, want)
	}
	if again := d.fresh(); !errors.Is(again, err) {
		t.Errorf("with no parent left, fresh says %v", again)
	}
	a := &attempt{mask: plan.NewMasker()}
	if diag := a.notStarted(err, "/jobs/j/steps/0"); diag.Code != "worker_dir_unusable" || diag.Path() != "/jobs/j/steps/0" ||
		diag.Params["code"] != "directory_not_private" || diag.Params["path"] != open {
		t.Errorf("a step that can't start for it has %+v", diag)
	}
	if diag := a.notStarted(os.ErrPermission, "/jobs/j/steps/0"); diag.Code != "step_not_started" || diag.Params["reason"] != os.ErrPermission.Error() {
		t.Errorf("a step that can't start for an error of the system's has %+v", diag)
	}
	// A problem that isn't the directory's, such as a file's, isn't taken
	// for one.
	if diag := a.notStarted(problems.FileNotRegular.Problem(problems.Params{"path": "/x"}), "/jobs/j/steps/0"); diag.Code != "step_not_started" {
		t.Errorf("a step that can't start for a file's problem has %+v", diag)
	}
}
