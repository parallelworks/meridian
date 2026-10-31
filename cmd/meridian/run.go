package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// runRun creates a run of a workflow, stores it, and coordinates it in the
// foreground, as one of its store's coordinators, until it ends, showing
// its progress, and its steps' logs, as they happen: it exits 0 when the
// run succeeded, and 1 when the workflow has errors, or the run failed or
// was cancelled. Interrupted in a terminal, it asks whether to cancel the
// run, leave it running or keep watching; otherwise an interrupt leaves
// the run running, and it exits 2, as for any problem of its own.
func runRun(ctx context.Context, c *cli, fs *flag.FlagSet, args []string) int {
	inputs := inputFlags(fs)
	group := fs.String("group", local.LocalGroup, "runs the jobs whose runs-on: names no group in the execution `group` named, the built-in one, which runs them on this machine, by default")
	runDir := fs.String("run-dir", "", "makes the run's `directory`, PW_JOB_DIR, which must not be there yet, here, rather than in a directory of the user's own under the system's temporary directory")
	out := fs.String("format", "text", "writes the run's progress, its logs and its end as `text`, lines a person reads, or as json, one object a line, whose version member says how to read it; and the command's problems as compilers write them, or as RFC 9457 problem details")
	withLogs := fs.Bool("logs", true, "writes each step's log as its worker stores it, each line after the job and the step it is of")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return flagError(c, fs, err)
	}
	if len(positional) != 1 {
		return c.failf(problems.ArgumentsInvalid, problems.Params{"command": "run"})
	}
	if !slices.Contains(formats, *out) {
		return c.failf(problems.FlagValueInvalid, problems.Params{"command": "run", "flag": "format", "value": *out})
	}
	// From here, the command's own problems are JSON too.
	c.json = *out == "json"
	// The run's progress is written as the run goes, and the command's
	// problems as they come: neither breaks the other's line.
	lockWriters(c)

	w, err := compileWorkflow(ctx, c, inputs, positional[0])
	if err != nil {
		return c.fail(err)
	}
	switch {
	case len(w.diags) == 0:
	case c.json:
		err = writeProblemJSON(c.stderr, w.diags)
	default:
		err = writeText(c.stderr, w.diags)
	}
	if err != nil {
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
	coord, sessions, err := coordinate(ctx, c, w.dirs, w.cfg, db)
	if err != nil {
		return c.fail(err)
	}
	// The coordinator is closed once it has stopped serving, and its
	// sessions' host before it.
	defer func() { _ = coord.Close() }()
	defer sessions.Close()
	storeID := coord.StoreID()
	logs, err := local.NewLogs(w.dirs.State, storeID)
	if err != nil {
		return c.fail(err)
	}
	// Closed once the socket's server, which stores workers' logs, has
	// stopped.
	defer func() { _ = logs.Close() }()
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
	progress := local.NewProgress(c.stdout, c.stderr, c.json)
	// Each session the run's steps point, this process says it serves
	// as it does: on a person's terminal, with a link.
	sessions.AnnounceTo(progress, c.showsSessionLinks())
	if err := progress.Created(id, dir); err != nil {
		return c.fail(err)
	}
	var follow coordinator.Logs
	if *withLogs {
		follow = logs
	}
	return supervise(ctx, c, coord, w, runSupervision{logs: logs, follow: follow, progress: progress}, id, interrupts)
}

// A runSupervision is what supervise shows a run with: where its workers'
// logs are stored, where it reads the logs it shows from, nil to show none,
// and the progress display it shows the run on.
type runSupervision struct {
	logs, follow coordinator.Logs
	progress     *local.Progress
	// quiet shows nothing of the run but its end, as meridian cancel does.
	quiet bool
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
// and holds the coordinator's socket when no other process does, and the
// sockets forwarded to the hosts its store's workers run on, each when
// no other process does (coordinator.ServeMembers), showing
// the run as it goes, until the run ends or an interrupt leaves it
// running. It returns the command's exit status.
func supervise(ctx context.Context, c *cli, coord *coordinator.Coordinator, w *compiled, rs runSupervision, id string, interrupts <-chan os.Signal) int {
	// What coordinates the run goes on through an interrupt, which only
	// asks what to do, until it stops.
	work, stop := context.WithCancel(context.WithoutCancel(ctx))
	served := make(chan error, 1)
	go func() { served <- coord.Serve(work) }()
	sock := holdSocket(work, coord, w.dirs.Socket(), rs.logs)
	// The workers on hosts reach it through sockets forwarded there, of
	// whichever process of the store's holds each.
	members := make(chan error, 1)
	serveMembers := c.serveMembers
	if serveMembers == nil {
		serveMembers = (*coordinator.Coordinator).ServeMembers
	}
	go func() { members <- serveMembers(coord, work, rs.logs) }()
	// It serves the store's sessions that no other process serves, each
	// at a port of its own of this machine's loopback addresses.
	sessions := make(chan error, 1)
	go func() { sessions <- coord.ServeSessions(work) }()
	defer func() {
		stop()
		<-sock.done
		<-members
		<-sessions
		<-served
	}()
	following, stopFollowing := context.WithCancel(work)
	defer stopFollowing()
	f := followRun(following, coord, id, rs)
	// quiet stops showing the run, once what it showed last is written.
	quiet := func() {
		stopFollowing()
		<-f.done
	}
	in := bufio.NewReader(c.stdin)
	for {
		select {
		case end := <-f.ended:
			if end.err != nil {
				return c.fail(end.err)
			}
			return finished(c, w, rs.progress, end)
		case err := <-served:
			// Serve returns before work is done only when it can't serve.
			served <- err
			quiet()
			return c.fail(err)
		case err := <-members:
			// So does ServeMembers, which leaves the workers on hosts
			// nothing to reach the coordinator through, so that the run
			// would wait out their leases.
			members <- err
			quiet()
			return c.fail(err)
		case err := <-sock.errs:
			quiet()
			return c.fail(err)
		case err := <-sessions:
			// ServeSessions returns before work is done only when it
			// can't serve.
			sessions <- err
			quiet()
			return c.fail(err)
		case sig := <-interrupts:
			if sig == syscall.SIGTERM || !c.isInteractive() {
				quiet()
				return detach(c, rs.progress, id)
			}
			switch ask(c, in, id, interrupts) {
			case "cancel":
				// The run stops, and watching it goes on until it has
				// ended, cancelled.
				if err := coord.Cancel(work, id); err != nil && !errors.Is(err, coordinator.ErrRunEnded) {
					_ = c.fail(err)
				}
			case "leave":
				quiet()
				return detach(c, rs.progress, id)
			}
		}
	}
}

// A runEnd is how a run ended, with the event that says so, or why its end
// can't be told.
type runEnd struct {
	status *coordinator.RunStatus
	event  coordinator.Event
	err    error
}

// A followed run is the run followRun shows: how it ended, sent once it
// has, and done, closed once nothing more of it is shown.
type followed struct {
	ended chan runEnd
	done  chan struct{}
}

// followRun shows the run id on rs.progress as it goes, its events and,
// unless rs.follow is nil, its steps' logs, and those of the runs its steps
// call, under their steps, until it has ended or ctx is done: then it
// sends how it ended, with the event that says so, which it leaves to
// finished to show.
func followRun(ctx context.Context, coord *coordinator.Coordinator, id string, rs runSupervision) followed {
	f := followed{ended: make(chan runEnd, 1), done: make(chan struct{})}
	go func() {
		defer close(f.done)
		var end runEnd
		opts := coordinator.FollowOptions{Tree: true, Event: func(e coordinator.Event) error {
			if e.Kind == coordinator.RunFinished && len(e.Callers) == 0 {
				end.event = e
				return nil
			}
			if rs.quiet {
				return nil
			}
			return rs.progress.Event(e)
		}}
		if rs.follow != nil {
			opts.Logs, opts.Log = rs.follow, rs.progress.Log
		}
		end.status, end.err = coord.Follow(ctx, id, opts)
		if ctx.Err() == nil {
			f.ended <- end
		}
	}()
	return f
}

// finished shows how the run ended, with its problems, each where the
// workflow writes what it is about: as text, its problems as compilers
// write them and a line that says how it ended; as JSON, the event that
// says so. It returns the command's exit status: 0 for a run that
// succeeded, and 1 for one that didn't.
func finished(c *cli, w *compiled, p *local.Progress, end runEnd) int {
	s := end.status
	diags := located(w.name, w.data, s.Diagnostics)
	if c.json {
		e := end.event
		e.Diagnostics = diags
		if err := p.Event(e); err != nil {
			return c.fail(err)
		}
	} else {
		if err := writeText(c.stderr, diags); err != nil {
			return c.fail(err)
		}
		line := fmt.Sprintf("run %s finished: %s", s.ID, s.Outcome)
		if s.Reason != "" {
			line += " (" + string(s.Reason) + ")"
		}
		fmt.Fprintln(c.stdout, line)
	}
	if s.Outcome != plan.OutcomeSuccess {
		return exitInvalid
	}
	return exitOK
}

// detach leaves the run id running, says how to pick it up, and returns
// the exit status of a command that didn't see its work through.
func detach(c *cli, p *local.Progress, id string) int {
	if err := p.Left(id); err != nil {
		return c.fail(err)
	}
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
