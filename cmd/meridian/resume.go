package main

import (
	"context"
	"errors"
	"flag"
	"slices"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/problems"
)

// resumeAbout is what meridian resume's usage says of it besides its
// summary.
const resumeAbout = `It takes the run up as one more of its store's coordinators: it starts the
run, if it hasn't started and no other process that lives starts it,
holds the workers' socket once no other process does, and shows the run
until it ends, exiting as meridian run does.

A run it refuses, as one whose directory is no longer the user's own
private directory, ends failed if it hasn't started; one that has isn't
stopped by it: another meridian run or resume of the store, and the run's
workers, go on with it. meridian cancel stops a run.`

// runResume takes up a run whose meridian run died, or left it running, as
// one of its store's coordinators, and coordinates it in the foreground
// until it ends, as meridian run does: it works the run's Start, if it
// hasn't run, here, reading its variables from this process's environment;
// holds the workers' socket when no other process does; and shows the run,
// from its first event, as it goes. A run whose directory isn't the user's
// own private directory any more is refused, and ended failed if nothing
// has run in it yet; and one whose plan only a newer release reads is left
// as it is.
func runResume(ctx context.Context, c *cli, fs *flag.FlagSet, args []string) int {
	out := fs.String("format", "text", "writes the run's progress, its logs and its end as `text`, lines a person reads, or as json, one object a line, whose version member says how to read it; and the command's problems as compilers write them, or as RFC 9457 problem details")
	withLogs := fs.Bool("logs", true, "writes each step's log as its worker stores it, each line after the job and the step it is of")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return flagError(c, fs, err)
	}
	if len(positional) != 1 {
		return c.failf(problems.ArgumentsInvalid, problems.Params{"command": "resume"})
	}
	if !slices.Contains(formats, *out) {
		return c.failf(problems.FlagValueInvalid, problems.Params{"command": "resume", "flag": "format", "value": *out})
	}
	c.json = *out == "json"
	lockWriters(c)
	id := positional[0]
	dirs, err := local.Locate(c.getenv)
	if err != nil {
		return c.fail(err)
	}
	cfg, err := local.LoadConfig(dirs.Config, c.lookupEnv)
	if err != nil {
		return c.fail(err)
	}
	db, err := storeOf(dirs)
	if err != nil {
		return c.fail(runNotFound(id, err))
	}
	if err := checkSocketPath(dirs.Socket()); err != nil {
		return c.fail(err)
	}
	interrupts, stopSignals := c.notify()
	defer stopSignals()
	coord, sessions, err := coordinate(ctx, c, dirs, cfg, db)
	if err != nil {
		return c.fail(err)
	}
	// The coordinator is closed once it has stopped serving, and its
	// sessions' host before it.
	defer func() { _ = coord.Close() }()
	defer sessions.Close()
	s, err := coord.Resume(ctx, id, func(s *coordinator.RunStatus) error { return checkRunDir(ctx, coord, s) })
	if err != nil {
		return c.fail(runNotFound(id, err))
	}
	logs, err := local.NewLogs(dirs.State, coord.StoreID())
	if err != nil {
		return c.fail(err)
	}
	// Closed once the socket's server, which stores workers' logs, has
	// stopped.
	defer func() { _ = logs.Close() }()
	progress := local.NewProgress(c.stdout, c.stderr, c.json)
	// Each session the run's steps point, this process says it serves
	// as it does: on a person's terminal, with a link.
	sessions.AnnounceTo(progress, c.stdoutTerminal())
	if err := progress.Resumed(id, s.RunDir); err != nil {
		return c.fail(err)
	}
	var follow coordinator.Logs
	if *withLogs {
		follow = logs
	}
	return supervise(ctx, c, coord, &compiled{dirs: dirs}, runSupervision{logs: logs, follow: follow, progress: progress}, id, interrupts)
}

// checkRunDir checks, before a run is taken up again, that its directory,
// which meridian run made, is still the user's own private directory, on a
// way no other user can change (local.CheckMadeRunDir), as its steps will
// run in it. One that isn't refuses the run: and a run that hasn't started,
// which nothing has run in, ends failed with what is wrong. A run that has
// ended has nothing more to run.
func checkRunDir(ctx context.Context, coord *coordinator.Coordinator, s *coordinator.RunStatus) error {
	if s.State == coordinator.RunEnded {
		return nil
	}
	err := local.CheckMadeRunDir(s.RunDir)
	if err == nil || s.State != coordinator.RunCreated {
		return err
	}
	p, ok := errors.AsType[*problem.Problem](err)
	if !ok {
		p = problems.RunDirNotMade.Problem(problems.Params{"path": s.RunDir, "reason": err.Error()})
	}
	if ferr := coord.FailRun(context.WithoutCancel(ctx), s.ID, p); ferr != nil {
		return errors.Join(p, ferr)
	}
	return p
}
