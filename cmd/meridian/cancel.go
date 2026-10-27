package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"slices"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// cancelAbout is what meridian cancel's usage says of it besides its
// summary.
const cancelAbout = `It stores the run's stop, which tells the workers of its jobs to stop, but
for those of jobs whose if: outlives a cancel, which run on; the run then
ends cancelled, once each job has. When a meridian run or resume of the
store is running, it sees the run to its end, and meridian cancel exits
at once; when none is, meridian cancel coordinates the run itself until it
has ended, as meridian resume does, so that its workers hear of the stop,
and the placements of those that don't are withdrawn, once it has checked
the run's directory as meridian resume does. With -f, it shows the run,
as meridian run does, until it ends.

A run whose meridian run or resume exits before it has ended, with no
other waiting to take its place, waits, stopped, for one: meridian resume
<run> sees it to its end.

It exits 0 once the run is stopped, or has ended cancelled, or had ended
already, which it says. A run it coordinates whose first stop was its
timeout, which the cancel then changes nothing of, ends as that has it,
and meridian cancel exits as meridian run would: 0 for one that succeeded,
and 1 otherwise. It exits 2 for a run the store hasn't (run_not_found);
for one whose directory is no longer the user's own private directory,
which it leaves stopped without coordinating it; when an interrupt leaves
a run it coordinates running; and for any other problem of its own.`

// runCancel cancels a run, as Coordinator.Cancel does, and, when no other
// process coordinates the run, or with -f, coordinates it in the
// foreground until it ends, as meridian resume does.
func runCancel(ctx context.Context, c *cli, fs *flag.FlagSet, args []string) int {
	follow := fs.Bool("f", false, "shows the run, once it is cancelled, as meridian run does, until it ends")
	out := fs.String("format", "text", "writes the run's stop, its progress, its logs and its end as `text`, lines a person reads, or as json, one object a line, whose version member says how to read it; and the command's problems as compilers write them, or as RFC 9457 problem details")
	withLogs := fs.Bool("logs", true, "with -f, writes each step's log as its worker stores it, each line after the job and the step it is of")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return flagError(c, fs, err)
	}
	if len(positional) != 1 {
		return c.failf(problems.ArgumentsInvalid, problems.Params{"command": "cancel"})
	}
	if !slices.Contains(formats, *out) {
		return c.failf(problems.FlagValueInvalid, problems.Params{"command": "cancel", "flag": "format", "value": *out})
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
	coord, err := coordinate(ctx, c, dirs, cfg, db)
	if err != nil {
		return c.fail(err)
	}
	// The coordinator is closed once it has stopped serving.
	defer func() { _ = coord.Close() }()
	progress := local.NewProgress(c.stdout, c.stderr, c.json)
	err = coord.Cancel(ctx, id)
	if err != nil && !errors.Is(err, coordinator.ErrRunEnded) {
		return c.fail(runNotFound(id, err))
	}
	if c.stopped != nil {
		c.stopped()
	}
	s, serr := coord.Status(ctx, id)
	if serr != nil {
		return c.fail(runNotFound(id, serr))
	}
	if s.State == coordinator.RunEnded && (errors.Is(err, coordinator.ErrRunEnded) || s.StartedAt.IsZero()) {
		// One that had ended already, or one that hadn't started, which
		// the stop ended at once. One that had started is stopping, as
		// this says, even if it has ended since, as its jobs can as soon
		// as their workers hear of the stop.
		return ended(c, progress, s, errors.Is(err, coordinator.ErrRunEnded))
	}
	if err := progress.Event(coordinator.Event{Kind: coordinator.RunStopping, Run: id, Reason: s.StopReason}); err != nil {
		return c.fail(err)
	}
	// Every meridian run and resume of the store works its runs, and
	// either holds the socket, or waits for it: one that does sees the
	// run to its end.
	held, err := coord.EndpointHeld(socketEndpoint)
	if err != nil {
		return c.fail(err)
	}
	if held && !*follow {
		return exitOK
	}
	if !*follow {
		// One that saw the run to its end may have let go of the socket
		// since, which it does only once the run has ended: then there is
		// nothing left to coordinate.
		if s, err = coord.Status(ctx, id); err != nil {
			return c.fail(runNotFound(id, err))
		}
		if s.State == coordinator.RunEnded {
			return exitOK
		}
	}
	// The run's jobs that outlive the stop may yet be placed, in its
	// directory, which is checked as meridian resume checks it.
	if err := local.CheckMadeRunDir(s.RunDir); err != nil {
		return c.fail(err)
	}
	logs, err := local.NewLogs(dirs.State, coord.StoreID())
	if err != nil {
		return c.fail(err)
	}
	// Closed once the socket's server, which stores workers' logs, has
	// stopped.
	defer func() { _ = logs.Close() }()
	rs := runSupervision{logs: logs, progress: progress, quiet: !*follow}
	if *follow && *withLogs {
		rs.follow = logs
	}
	code := supervise(ctx, c, coord, &compiled{dirs: dirs}, rs, id, interrupts)
	if code != exitInvalid {
		return code
	}
	// The run ended other than successfully: cancelled, as asked, or as its
	// first stop, its timeout, had it end, which meridian run exits 1 for.
	if s, err = coord.Status(context.WithoutCancel(ctx), id); err != nil {
		return c.fail(err)
	}
	if s.Outcome == plan.OutcomeCancelled {
		return exitOK
	}
	return exitInvalid
}

// ended says how the run s ended, which a cancel found ended: before it,
// or since, as one that hadn't started, which the stop ended at once. As
// JSON, it is the run's end, as meridian run writes it. It returns 0 for a
// run that had ended before, or that ended cancelled, and otherwise, as for
// one whose first stop was its timeout, what meridian run would.
func ended(c *cli, p *local.Progress, s *coordinator.RunStatus, before bool) int {
	if c.json {
		if err := p.Event(coordinator.Event{Kind: coordinator.RunFinished, Run: s.ID, Time: s.EndedAt, Outcome: s.Outcome, Reason: s.Reason}); err != nil {
			return c.fail(err)
		}
	} else {
		line := fmt.Sprintf("run %s finished: %s", s.ID, outcomeOf(s.Outcome, s.Reason))
		if before {
			line = fmt.Sprintf("run %s had already finished: %s", s.ID, outcomeOf(s.Outcome, s.Reason))
		}
		if _, err := fmt.Fprintln(c.stdout, line); err != nil {
			return c.fail(err)
		}
	}
	if before || s.Outcome == plan.OutcomeCancelled || s.Outcome == plan.OutcomeSuccess {
		return exitOK
	}
	return exitInvalid
}
