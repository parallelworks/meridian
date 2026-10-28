package main

import (
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/internal/racetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// createdRun stores a run of workflow in the store of the state directory
// env names, as meridian run does, its directory made, and returns its ID
// and its directory, without starting it: as a meridian run killed once it
// had stored the run leaves it.
func createdRun(t *testing.T, env map[string]string, workflow string) (string, string) {
	t.Helper()
	c := &cli{
		stdin: strings.NewReader(""), stdout: io.Discard, stderr: io.Discard,
		lookupEnv: func(name string) (string, bool) { v, ok := env[name]; return v, ok },
	}
	w, err := compileWorkflow(t.Context(), c, &inputOptions{file: new(string)}, workflow)
	if err != nil {
		t.Fatal(err)
	}
	if w.created == nil || w.diags.HasErrors() {
		t.Fatalf("%s has errors: %v", workflow, w.diags)
	}
	db, err := w.dirs.Database()
	if err != nil {
		t.Fatal(err)
	}
	coord, err := coordinate(t.Context(), c, w.dirs, w.cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()
	dirOf, err := runDirs(c, "")
	if err != nil {
		t.Fatal(err)
	}
	dir := func(id string) string { return dirOf(coord.StoreID(), id) }
	id, err := coord.CreateRun(t.Context(), w.created, coordinator.RunOptions{DefaultGroup: local.LocalGroup, RunDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := local.MakeRunDir(dir(id)); err != nil {
		t.Fatal(err)
	}
	return id, dir(id)
}

// TestResume checks that meridian resume takes up a run stored but not
// started, as when the meridian run that created it died, starts it here,
// and coordinates it to its end, as meridian run does; and takes up a run
// that has ended, showing it, from its first event, to its end.
func TestResume(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	id, dir := createdRun(t, env, writeFile(t, "w.yaml", helloWorkflow))
	r := commandTest(t, env, "resume", id)
	if r.code != exitOK || r.stderr != "" || !strings.HasPrefix(r.stdout, "run 1 resumed (run directory "+dir+")\n") ||
		!strings.Contains(r.stdout, "main step 0| hello\n") || !strings.HasSuffix(r.stdout, "run 1 finished: success\n") {
		t.Fatalf("meridian resume of a run not started = %+v", r)
	}
	if log := stepLog(t, env, id); log != "hello\n" {
		t.Errorf("the step's log is %q", log)
	}
	again := commandTest(t, env, "resume", "--logs=false", id)
	if again.code != exitOK || !strings.HasPrefix(again.stdout, "run 1 resumed") || !strings.Contains(again.stdout, "main step 0 finished") ||
		strings.Contains(again.stdout, "hello") || !strings.HasSuffix(again.stdout, "run 1 finished: success\n") {
		t.Errorf("meridian resume of a run that has ended = %+v", again)
	}
	if r := runTest(t, env, nil, writeFile(t, "w.yaml", failWorkflow)); r.code != exitInvalid {
		t.Fatalf("meridian run of a step that fails = %+v", r)
	}
	if r := commandTest(t, env, "resume", "--format", "json", "2"); r.code != exitInvalid || !strings.Contains(r.stdout, `"kind":"run.resumed"`) || !strings.Contains(r.stdout, `"outcome":"failure"`) {
		t.Errorf("meridian resume --format json of a run that failed = %+v", r)
	}
	for _, args := range [][]string{{"resume", "9"}, {"resume", "x"}} {
		if r := commandTest(t, env, args...); r.code != exitError || !strings.Contains(r.stderr, "(run_not_found)") || r.stdout != "" {
			t.Errorf("meridian %s = %+v", strings.Join(args, " "), r)
		}
	}
	if r := commandTest(t, runEnv(t), "resume", "1"); r.code != exitError || !strings.Contains(r.stderr, "(run_not_found)") {
		t.Errorf("meridian resume without a store = %+v", r)
	}
	// Its usage says that a run it refuses that has started goes on.
	if r := commandTest(t, env, "help", "resume"); r.code != exitOK || !strings.Contains(r.stdout, "isn't\nstopped by it") || !strings.Contains(r.stdout, "meridian cancel") {
		t.Errorf("meridian help resume = %+v", r)
	}
}

// TestResumeRunDir checks that a run whose directory isn't its user's own
// private directory any more isn't taken up, and that one that hasn't
// started, which nothing has run in, ends failed with what is wrong.
func TestResumeRunDir(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	id, dir := createdRun(t, env, writeFile(t, "w.yaml", helloWorkflow))
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	r := commandTest(t, env, "resume", id)
	if r.code != exitError || !strings.Contains(r.stderr, "(directory_not_private)") || r.stdout != "" {
		t.Errorf("meridian resume of a run whose directory isn't private = %+v", r)
	}
	coord, err := readStore(t.Context(), &cli{stderr: io.Discard}, local.Dirs{State: env[local.EnvStateDir]})
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()
	s, err := coord.Status(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if s.State != coordinator.RunEnded || s.Outcome != plan.OutcomeFailure || len(s.Diagnostics) != 1 || s.Diagnostics[0].Code != problems.DirectoryNotPrivate.Code() {
		t.Errorf("the run whose directory isn't private is %+v", s)
	}
}

// envMain has TestMain run the test binary as meridian, with its
// arguments, as a test runs it in a process of its own.
const envMain = "MERIDIAN_TEST_MAIN"

// A process is the test binary, run as meridian in a process of its own,
// and what it has written.
type process struct {
	cmd            *exec.Cmd
	stdout, stderr lockedBuffer
}

// meridianProcess starts the test binary as meridian with args, in the
// environment env.
func meridianProcess(t *testing.T, env map[string]string, args ...string) *process {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p := &process{cmd: exec.CommandContext(t.Context(), exe, args...)}
	p.cmd.Env = []string{envMain + "=1", "PATH=" + os.Getenv("PATH"), racetest.Env(env["HOME"])}
	for k, v := range env {
		p.cmd.Env = append(p.cmd.Env, k+"="+v)
	}
	p.cmd.Stdout, p.cmd.Stderr = &p.stdout, &p.stderr
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = p.cmd.Process.Kill()
		_ = p.cmd.Wait()
	})
	return p
}

// TestResumeKilled checks that a run whose meridian run was killed as its
// job's step ran is taken up by meridian resume, once the killed process's
// hold on the run's work has lapsed, which takes hopper's lease, 15
// seconds: the job's worker, which goes on, cut off from the coordinator,
// keeps what its step writes until a coordinator holds the socket again,
// and the run ends, its step's log whole.
func TestResumeKilled(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	workflow, release := waitingWorkflow(t)
	killed := meridianProcess(t, env, "run", "--format", "json", workflow)
	waitUntil(t, "the step to start", func() bool { return strings.Contains(killed.stdout.String(), `"kind":"step.started"`) })
	if err := killed.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = killed.cmd.Wait()
	// The step writes the rest of its log while no coordinator holds the
	// socket.
	release()
	resumed := meridianProcess(t, env, "resume", "1")
	if err := resumed.cmd.Wait(); err != nil {
		t.Fatalf("meridian resume 1: %v\n%s%s", err, resumed.stdout.String(), resumed.stderr.String())
	}
	out := resumed.stdout.String()
	if !strings.HasPrefix(out, "run 1 resumed (run directory ") || strings.Count(out, "main step 0| one\n") != 1 || strings.Count(out, "main step 0| two\n") != 1 ||
		!strings.HasSuffix(out, "run 1 finished: success\n") {
		t.Errorf("meridian resume showed\n%s%s", out, resumed.stderr.String())
	}
	if log := stepLog(t, env, "1"); log != "one\ntwo\n" {
		t.Errorf("the step's log is %q", log)
	}
}
