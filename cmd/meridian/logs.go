package main

import (
	"context"
	"flag"
	"slices"
	"strings"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/problems"
)

// runLogs shows the logs of a run's steps, or of one of its jobs, as far as
// they are stored, or, with -f, as they are stored, until the run ends, as
// meridian run shows them: each line after the job and the step it is of,
// or as JSON lines. The run's events say which logs there are. The logs of
// the runs its steps call, or the job's, at any depth, it shows under the
// step that called each; a run a step called it shows the logs of alone,
// with those of the runs its own steps call.
func runLogs(ctx context.Context, c *cli, fs *flag.FlagSet, args []string) int {
	follow := fs.Bool("f", false, "follows the logs as their workers store them, until the run ends")
	out := fs.String("format", "text", "writes each line of a log as `text`, after the job and the step it is of, or as json, one object a line, whose version member says how to read it")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return flagError(c, fs, err)
	}
	if len(positional) < 1 || len(positional) > 2 {
		return c.failf(problems.ArgumentsInvalid, problems.Params{"command": "logs"})
	}
	if !slices.Contains(formats, *out) {
		return c.failf(problems.FlagValueInvalid, problems.Params{"command": "logs", "flag": "format", "value": *out})
	}
	c.json = *out == "json"
	lockWriters(c)
	id := positional[0]
	dirs, err := local.Locate(c.getenv)
	if err != nil {
		return c.fail(err)
	}
	coord, err := readStore(ctx, c, dirs)
	if err != nil {
		return c.fail(runNotFound(id, err))
	}
	defer func() { _ = coord.Close() }()
	jobs, err := coord.Jobs(ctx, id)
	if err != nil {
		return c.fail(runNotFound(id, err))
	}
	// A job's name, or an instance's key, which names that instance alone.
	of := func(string) bool { return true }
	if len(positional) == 2 {
		job := positional[1]
		if !slices.ContainsFunc(jobs, func(j coordinator.JobStatus) bool { return j.Key == job || j.Name == job }) {
			return c.failf(problems.JobNotFound, problems.Params{"run": id, "job": job})
		}
		of = func(key string) bool { return key == job || strings.HasPrefix(key, job+"#") }
	}
	// A log of a run a step called is of the job whose step called the
	// first of them.
	ofPart := func(part coordinator.LogPart) bool {
		if len(part.Callers) > 0 {
			return of(part.Callers[0].Job)
		}
		return of(part.Key.Job)
	}
	logs, err := local.NewLogs(dirs.State, coord.StoreID())
	if err != nil {
		return c.fail(err)
	}
	defer func() { _ = logs.Close() }()
	p := local.NewProgress(c.stdout, c.stderr, c.json)
	if _, err := coord.Follow(ctx, id, coordinator.FollowOptions{
		Event: func(coordinator.Event) error { return nil },
		Logs:  logs,
		Log: func(part coordinator.LogPart) error {
			if !ofPart(part) {
				return nil
			}
			return p.Log(part)
		},
		Stored: !*follow,
		Tree:   true,
	}); err != nil {
		return c.fail(err)
	}
	if err := p.Flush(); err != nil {
		return c.fail(err)
	}
	return exitOK
}
