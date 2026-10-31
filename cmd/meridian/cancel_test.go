package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
)

// TestCancelFollowed checks meridian cancel, in a process of its own, of a
// run that a meridian run coordinates and shows: the cancel stores the
// run's stop and exits at once, and the meridian run sees the run stop,
// and end cancelled, and exits 1; and meridian cancel -f of a run another
// process coordinates shows it to its end, and exits 0.
func TestCancelFollowed(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	for _, follow := range []bool{false, true} {
		workflow, _ := waitingWorkflow(t)
		running := startCommand(t, env, nil, "run", workflow)
		untilSaid(t, running, "main step 0 started")
		id := startedRE.FindStringSubmatch(running.stdout.String())[1]
		args := []string{"cancel", id}
		if follow {
			args = []string{"cancel", "-f", "--format", "json", id}
		}
		cancel := meridianProcess(t, env, args...)
		if err := cancel.cmd.Wait(); err != nil {
			t.Fatalf("meridian %s: %v\n%s%s", strings.Join(args, " "), err, cancel.stdout.String(), cancel.stderr.String())
		}
		out := cancel.stdout.String()
		switch {
		case !follow && out != "run "+id+" stopping (cancelled)\n":
			t.Errorf("meridian cancel %s wrote %q", id, out)
		case follow:
			lines := jsonLines(t, out)
			first, last := lines[0], lines[len(lines)-1]
			if first["kind"] != "run.stopping" || first["reason"] != "cancelled" || last["kind"] != "run.finished" || last["outcome"] != "cancelled" ||
				!strings.Contains(out, `"kind":"step.started"`) {
				t.Errorf("meridian cancel -f --format json %s wrote\n%s", id, out)
			}
		}
		r := <-running.done
		if r.code != exitInvalid || !strings.Contains(r.stdout, "run "+id+" stopping (cancelled)\n") || !strings.HasSuffix(r.stdout, "run "+id+" finished: cancelled (cancelled)\n") {
			t.Errorf("meridian run of the run cancelled = %+v", r)
		}
		if r := commandTest(t, env, "status", id); r.code != exitOK || !strings.HasPrefix(r.stdout, "run "+id+": ended, cancelled (cancelled)\n") {
			t.Errorf("meridian status of the run cancelled = %+v", r)
		}
	}
}

// TestCancelRunEndingAtOnce checks meridian cancel of a started run that
// has ended, and whose meridian run has exited, by the time meridian cancel
// reads it back, as a run can as soon as its worker hears of the stop: it
// says the run is stopping, as of one still running, rather than that the
// stop ended it, and with -f shows the run to its end, rather than only
// the end.
func TestCancelRunEndingAtOnce(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	for _, follow := range []bool{false, true} {
		workflow, _ := waitingWorkflow(t)
		running := startCommand(t, env, nil, "run", workflow)
		untilSaid(t, running, "main step 0 started")
		id := startedRE.FindStringSubmatch(running.stdout.String())[1]
		args := []string{"cancel", id}
		if follow {
			args = []string{"cancel", "-f", "--format", "json", id}
		}
		var ran result
		r := <-startCommand(t, env, func(c *cli) { c.stopped = func() { ran = <-running.done } }, args...).done
		switch {
		case r.code != exitOK:
			t.Errorf("meridian %s = %+v", strings.Join(args, " "), r)
		case !follow && r.stdout != "run "+id+" stopping (cancelled)\n":
			t.Errorf("meridian cancel %s wrote %q", id, r.stdout)
		case follow:
			lines := jsonLines(t, r.stdout)
			first, last := lines[0], lines[len(lines)-1]
			if first["kind"] != "run.stopping" || last["kind"] != "run.finished" || last["outcome"] != "cancelled" ||
				!strings.Contains(r.stdout, `"kind":"step.started"`) {
				t.Errorf("meridian cancel -f --format json %s wrote\n%s", id, r.stdout)
			}
		}
		if ran.code != exitInvalid || !strings.HasSuffix(ran.stdout, "run "+id+" finished: cancelled (cancelled)\n") {
			t.Errorf("meridian run of the run cancelled = %+v", ran)
		}
	}
}

// TestCancelUncoordinated checks meridian cancel of a run that no meridian
// run or resume coordinates, as one whose meridian run left it running: it
// coordinates the run itself until it has ended, so that its worker hears
// of the stop, and exits 0, having said so; and meridian status shows the
// run stopping before then.
func TestCancelUncoordinated(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	workflow, _ := waitingWorkflow(t)
	no := false
	interrupt := make(chan os.Signal, 1)
	running := startCommand(t, env, func(c *cli) { c.interactive, c.signals = &no, interrupt }, "run", workflow)
	untilSaid(t, running, "main step 0 started")
	interrupt <- os.Interrupt
	if r := <-running.done; r.code != exitError || !strings.Contains(r.stdout, "run 1 left running") {
		t.Fatalf("meridian run interrupted = %+v", r)
	}

	// The stop, stored as no one coordinates the run, shows.
	dirs := local.Dirs{State: env[local.EnvStateDir]}
	db, err := dirs.Database()
	if err != nil {
		t.Fatal(err)
	}
	c := &cli{stderr: io.Discard, lookupEnv: func(name string) (string, bool) { v, ok := env[name]; return v, ok }}
	cfg, err := local.LoadConfig(env[local.EnvConfigDir], c.lookupEnv)
	if err != nil {
		t.Fatal(err)
	}
	co, err := coordinate(t.Context(), c, dirs, cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := co.coord.Cancel(t.Context(), "1"); err != nil {
		t.Fatal(err)
	}
	if err := co.close(); err != nil {
		t.Fatal(err)
	}
	if r := commandTest(t, env, "status", "1"); r.code != exitOK || !strings.HasPrefix(r.stdout, "run 1: stopping (cancelled)\n") {
		t.Errorf("meridian status of a run stopping = %+v", r)
	}
	if r := commandTest(t, env, "status"); r.code != exitOK || !regexp.MustCompile(`\n1 +stopping +`).MatchString(r.stdout) {
		t.Errorf("meridian status listing a run stopping = %+v", r)
	}
	r := commandTest(t, env, "status", "--format", "json", "1")
	if lines := jsonLines(t, r.stdout); r.code != exitOK || len(lines) == 0 || lines[0]["stopReason"] != "cancelled" || lines[0]["stopped"] == nil {
		t.Errorf("meridian status --format json of a run stopping = %+v", r)
	}

	r = commandTest(t, env, "cancel", "1")
	if r.code != exitOK || r.stdout != "run 1 stopping (cancelled)\nrun 1 finished: cancelled (cancelled)\n" {
		t.Errorf("meridian cancel of a run no one coordinates = %+v", r)
	}
	if r := commandTest(t, env, "status", "1"); r.code != exitOK || !strings.HasPrefix(r.stdout, "run 1: ended, cancelled (cancelled)\n") {
		t.Errorf("meridian status of the run cancelled = %+v", r)
	}
}

// TestCancelRunDir checks that meridian cancel of a run no one coordinates,
// whose directory is no longer private, stores the stop, but doesn't
// coordinate the run, as meridian resume doesn't take it up.
func TestCancelRunDir(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	workflow, _ := waitingWorkflow(t)
	no := false
	interrupt := make(chan os.Signal, 1)
	running := startCommand(t, env, func(c *cli) { c.interactive, c.signals = &no, interrupt }, "run", workflow)
	untilSaid(t, running, "main step 0 started")
	dir := startedRE.FindStringSubmatch(running.stdout.String())[2]
	interrupt <- os.Interrupt
	if r := <-running.done; r.code != exitError {
		t.Fatalf("meridian run interrupted = %+v", r)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	r := commandTest(t, env, "cancel", "1")
	if r.code != exitError || r.stdout != "run 1 stopping (cancelled)\n" || !strings.Contains(r.stderr, "(directory_not_private)") {
		t.Errorf("meridian cancel of a run whose directory isn't private = %+v", r)
	}
	if r := commandTest(t, env, "status", "1"); r.code != exitOK || !strings.HasPrefix(r.stdout, "run 1: stopping (cancelled)\n") {
		t.Errorf("meridian status of the run = %+v", r)
	}
	// Private again, the run stopped is seen to its end.
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if r := commandTest(t, env, "cancel", "1"); r.code != exitOK || !strings.HasSuffix(r.stdout, "run 1 finished: cancelled (cancelled)\n") {
		t.Errorf("meridian cancel of the run once its directory is private again = %+v", r)
	}
}

// TestCancelAfterTimeout checks meridian cancel of a run whose first stop
// was its timeout, which a job that outlives it keeps running, and which
// no one coordinates: the cancel changes nothing of the stop, coordinates
// the run to its end, which the timeout has failed, and exits 1, as
// meridian run would.
func TestCancelAfterTimeout(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	release := filepath.Join(t.TempDir(), "release")
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0o600) })
	workflow := writeFile(t, "w.yaml", fmt.Sprintf(`timeout: 1s
jobs:
  a:
    steps:
      - run: while true; do sleep 0.05; done
  b:
    if: always()
    steps:
      - run: while [ ! -e %s ]; do sleep 0.05; done
`, quoted(t, release)))
	no := false
	interrupt := make(chan os.Signal, 1)
	running := startCommand(t, env, func(c *cli) { c.interactive, c.signals = &no, interrupt }, "run", workflow)
	untilSaid(t, running, "run 1 stopping (timeout)")
	untilSaid(t, running, "a finished: failure (timeout)")
	interrupt <- os.Interrupt
	if r := <-running.done; r.code != exitError {
		t.Fatalf("meridian run interrupted = %+v", r)
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if r := commandTest(t, env, "cancel", "1"); r.code != exitInvalid || r.stdout != "run 1 stopping (timeout)\nrun 1 finished: failure (timeout)\n" {
		t.Errorf("meridian cancel of a run its timeout stopped = %+v", r)
	}
	if r := commandTest(t, env, "cancel", "1"); r.code != exitOK || r.stdout != "run 1 had already finished: failure (timeout)\n" {
		t.Errorf("meridian cancel of the run once it has ended = %+v", r)
	}
}

// TestCancelEnded checks meridian cancel of a run that has ended, which it
// says, and exits 0; of one that hasn't started, which ends cancelled at
// once; and of one the store hasn't.
func TestCancelEnded(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	if r := commandTest(t, env, "cancel", "1"); r.code != exitError || !strings.Contains(r.stderr, "(run_not_found)") || r.stdout != "" {
		t.Errorf("meridian cancel without a store = %+v", r)
	}
	if r := runTest(t, env, nil, writeFile(t, "w.yaml", helloWorkflow)); r.code != exitOK {
		t.Fatalf("meridian run = %+v", r)
	}
	if r := commandTest(t, env, "cancel", "1"); r.code != exitOK || r.stdout != "run 1 had already finished: success\n" || r.stderr != "" {
		t.Errorf("meridian cancel of a run that has ended = %+v", r)
	}
	r := commandTest(t, env, "cancel", "--format", "json", "1")
	if lines := jsonLines(t, r.stdout); r.code != exitOK || len(lines) != 1 || lines[0]["kind"] != "run.finished" || lines[0]["outcome"] != "success" {
		t.Errorf("meridian cancel --format json of a run that has ended = %+v", r)
	}
	id, _ := createdRun(t, env, writeFile(t, "w.yaml", helloWorkflow))
	if r := commandTest(t, env, "cancel", id); r.code != exitOK || r.stdout != "run "+id+" finished: cancelled (cancelled)\n" {
		t.Errorf("meridian cancel of a run that hasn't started = %+v", r)
	}
	if r := commandTest(t, env, "status", id); r.code != exitOK || !strings.HasPrefix(r.stdout, "run "+id+": ended, cancelled (cancelled)\n") {
		t.Errorf("meridian status of the run cancelled before it started = %+v", r)
	}
	for _, missing := range []string{"9", "x"} {
		if r := commandTest(t, env, "cancel", missing); r.code != exitError || !strings.Contains(r.stderr, "(run_not_found)") || r.stdout != "" {
			t.Errorf("meridian cancel %s = %+v", missing, r)
		}
	}
}
