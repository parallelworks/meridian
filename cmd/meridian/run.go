package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// runPoll is how often meridian run reads where its run is, until the run
// has ended.
const runPoll = 250 * time.Millisecond

// runRun creates a run of a workflow, stores it, and coordinates it in the
// foreground, as one of its store's coordinators, until it ends: it exits
// 0 when the run succeeded, and 1 when the workflow has errors, or the run
// failed or was cancelled. Interrupted in a terminal, it asks whether to
// cancel the run, leave it running or keep watching; otherwise an
// interrupt leaves the run running, and it exits 2, as for any problem of
// its own.
func runRun(ctx context.Context, c *cli, fs *flag.FlagSet, args []string) int {
	inputs := inputFlags(fs)
	group := fs.String("group", local.LocalGroup, "runs the jobs whose runs-on: names no group in the execution `group` named, the built-in one, which runs them on this machine, by default")
	runDir := fs.String("run-dir", "", "makes the run's `directory`, PW_JOB_DIR, which must not be there yet, here, rather than in a directory of the user's own under the system's temporary directory")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return flagError(c, fs, err)
	}
	if len(positional) != 1 {
		return c.failf(problems.ArgumentsInvalid, problems.Params{"command": "run"})
	}

	w, err := compileWorkflow(ctx, c, inputs, positional[0])
	if err != nil {
		return c.fail(err)
	}
	if err := writeText(c.stderr, w.diags); err != nil {
		return c.fail(err)
	}
	if w.diags.HasErrors() || w.created == nil {
		return exitInvalid
	}
	// From here an interrupt is the command's to answer: until the run is
	// stored, it stops the command, through ctx; after, it asks.
	interrupts, stopSignals := c.notify()
	defer stopSignals()

	db, err := w.dirs.Database()
	if err != nil {
		return c.fail(err)
	}
	if err := checkSocketPath(w.dirs.Socket()); err != nil {
		return c.fail(err)
	}
	dirOf, err := runDirs(c, *runDir)
	if err != nil {
		return c.fail(err)
	}
	coord, err := coordinator.Open(ctx, coordinator.Options{
		Database: db, Keys: local.NewKeys(w.dirs.State),
		Variables: w.cfg, Inputs: w.cfg, Groups: w.cfg,
		// The run's Start reads its variables from this process's
		// environment, the shell that ran it.
		StartQueue: func(id string) string { return "meridian.start." + id },
		Log:        slog.New(slog.NewTextHandler(c.stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		return c.fail(err)
	}
	// The coordinator is closed once it has stopped serving.
	defer func() { _ = coord.Close() }()
	storeID := coord.StoreID()
	id, err := coord.CreateRun(ctx, w.created, coordinator.RunOptions{
		DefaultGroup: *group, RunDir: func(id string) string { return dirOf(storeID, id) },
	})
	if err != nil {
		return c.fail(err)
	}
	// The run is stored, and its directory is made new, only now: its
	// number is the run's alone. A run whose directory can't be made ends
	// failed, before anything starts in what is at its path.
	dir := dirOf(storeID, id)
	if err := local.MakeRunDir(dir); err != nil {
		p, ok := errors.AsType[*problem.Problem](err)
		if !ok {
			p = problems.RunDirNotMade.Problem(problems.Params{"path": dir, "reason": err.Error()})
		}
		if ferr := coord.FailRun(context.WithoutCancel(ctx), id, p); ferr != nil {
			return c.fail(errors.Join(p, ferr))
		}
		return c.fail(p)
	}
	fmt.Fprintf(c.stdout, "run %s started (run directory %s)\n", id, dir)
	return supervise(ctx, c, coord, w, id, interrupts)
}

// runDirs returns where runs' directories are, by the store's ID and the
// run's: named, the one directory named, which must not be there yet; and
// otherwise one of their own in the user's own directory under the system's
// temporary directory, which it makes if it isn't there. It checks the way
// to each, before the run is stored.
func runDirs(c *cli, named string) (func(storeID, id string) string, error) {
	if named != "" {
		path, err := filepath.Abs(named)
		if err != nil {
			return nil, problems.RunDirNotMade.Problem(problems.Params{"path": named, "reason": err.Error()})
		}
		if err := local.CheckRunDir(path); err != nil {
			return nil, err
		}
		return func(string, string) string { return path }, nil
	}
	tmp := c.getenv("TMPDIR")
	if !filepath.IsAbs(tmp) {
		tmp = "/tmp"
	}
	parent := local.RunsDir(tmp)
	if err := local.EnsureRunsDir(parent); err != nil {
		return nil, err
	}
	return func(storeID, id string) string { return filepath.Join(parent, local.RunDirName(storeID, id)) }, nil
}

// supervise coordinates the run id, as one of its store's coordinators,
// and holds the coordinator's socket when no other process does, until the
// run ends or an interrupt leaves it running. It returns the command's
// exit status.
func supervise(ctx context.Context, c *cli, coord *coordinator.Coordinator, w *compiled, id string, interrupts <-chan os.Signal) int {
	// What coordinates the run goes on through an interrupt, which only
	// asks what to do, until it stops.
	work, stop := context.WithCancel(context.WithoutCancel(ctx))
	served := make(chan error, 1)
	go func() { served <- coord.Serve(work) }()
	sock := holdSocket(work, coord, w.dirs.Socket(), workerAPIUnavailable())
	defer func() {
		stop()
		<-sock.done
		<-served
	}()
	ended := watchRun(work, coord, id, c.pollEvery())
	in := bufio.NewReader(c.stdin)
	for {
		select {
		case end := <-ended:
			if end.err != nil {
				return c.fail(end.err)
			}
			return finished(c, w, end.status)
		case err := <-served:
			// Serve returns before work is done only when it can't serve.
			served <- err
			return c.fail(err)
		case err := <-sock.errs:
			return c.fail(err)
		case sig := <-interrupts:
			if sig == syscall.SIGTERM || !c.isInteractive() {
				return detach(c, id)
			}
			switch ask(c, in, id, interrupts) {
			case "cancel":
				// Cancelling a run comes with meridian cancel: until
				// then, the run goes on, and so does watching it.
				_ = c.failf(problems.CommandNotBuilt, problems.Params{"command": "cancel"})
			case "leave":
				return detach(c, id)
			}
		}
	}
}

// A runEnd is how a run ended, or why its end can't be told.
type runEnd struct {
	status *coordinator.RunStatus
	err    error
}

// watchRun reads where the run id is, every so often, until it has ended
// or ctx is done, and then sends how it ended on the channel it returns.
func watchRun(ctx context.Context, coord *coordinator.Coordinator, id string, every time.Duration) <-chan runEnd {
	ended := make(chan runEnd, 1)
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			s, err := coord.Status(ctx, id)
			switch {
			case ctx.Err() != nil:
				return
			case err != nil:
				ended <- runEnd{err: err}
				return
			case s.State == coordinator.RunEnded:
				ended <- runEnd{status: s}
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return ended
}

// finished writes how the run ended, its problems as compilers write them,
// where the workflow writes what each is about, and returns the command's
// exit status: 0 for a run that succeeded, and 1 for one that didn't.
func finished(c *cli, w *compiled, s *coordinator.RunStatus) int {
	if err := writeText(c.stderr, located(w.name, w.data, s.Diagnostics)); err != nil {
		return c.fail(err)
	}
	line := fmt.Sprintf("run %s finished: %s", s.ID, s.Outcome)
	if s.Reason != "" {
		line += " (" + string(s.Reason) + ")"
	}
	fmt.Fprintln(c.stdout, line)
	if s.Outcome != plan.OutcomeSuccess {
		return exitInvalid
	}
	return exitOK
}

// detach leaves the run id running, says how to pick it up, and returns
// the exit status of a command that didn't see its work through.
func detach(c *cli, id string) int {
	fmt.Fprintf(c.stdout, "run %s left running: meridian resume %s picks it up, and meridian cancel %s cancels it\n", id, id, id)
	return exitError
}

// ask asks, in a terminal, what to do about the run id once interrupted:
// cancel it, leave it running, or keep watching it, which is the answer
// to anything else. Another interrupt as it asks, or the end of standard
// input, leaves the run running.
func ask(c *cli, in *bufio.Reader, id string, interrupts <-chan os.Signal) string {
	fmt.Fprintf(c.stderr, "\nrun %s is running. Cancel it (c), leave it running (l), or keep watching (w)? ", id)
	type answer struct {
		text string
		err  error
	}
	answered := make(chan answer, 1)
	go func() {
		s, err := in.ReadString('\n')
		answered <- answer{s, err}
	}()
	select {
	case <-interrupts:
		fmt.Fprintln(c.stderr)
		return "leave"
	case a := <-answered:
		if a.err != nil && strings.TrimSpace(a.text) == "" {
			return "leave"
		}
		switch strings.ToLower(strings.TrimSpace(a.text)) {
		case "c", "cancel":
			return "cancel"
		case "l", "leave":
			return "leave"
		}
		return "watch"
	}
}

// notify returns the interrupts, and requests to stop, that the process
// gets from now, and the function that stops them coming: SIGINT, as from
// Ctrl-C, and SIGTERM. A test gives its own.
func (c *cli) notify() (<-chan os.Signal, func()) {
	if c.signals != nil {
		return c.signals, func() {}
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	return ch, func() { signal.Stop(ch) }
}

// isInteractive reports whether a person can answer the command: its
// standard input and its standard error are a terminal.
func (c *cli) isInteractive() bool {
	if c.interactive != nil {
		return *c.interactive
	}
	return isTerminal(c.stdin) && isTerminal(c.stderr)
}

// pollEvery is how often the command reads where its run is.
func (c *cli) pollEvery() time.Duration {
	if c.poll > 0 {
		return c.poll
	}
	return runPoll
}
