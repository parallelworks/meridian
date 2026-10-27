package main

import (
	"context"
	jsonv2 "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/problems"
)

// statusRuns is how many runs meridian status lists unless -n says.
const statusRuns = 20

// runStatus shows the store's last runs, or one run and its jobs, as text
// a person reads, or as JSON lines, as meridian run writes them.
func runStatus(ctx context.Context, c *cli, fs *flag.FlagSet, args []string) int {
	out := fs.String("format", "text", "writes the runs, or the run and its jobs, as `text`, a table a person reads, or as json, one object a line, whose version member says how to read it")
	n := fs.Int("n", statusRuns, "lists the last `number` runs")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return flagError(c, fs, err)
	}
	if len(positional) > 1 {
		return c.failf(problems.ArgumentsInvalid, problems.Params{"command": "status"})
	}
	if !slices.Contains(formats, *out) {
		return c.failf(problems.FlagValueInvalid, problems.Params{"command": "status", "flag": "format", "value": *out})
	}
	if *n <= 0 {
		return c.failf(problems.FlagValueInvalid, problems.Params{"command": "status", "flag": "n", "value": strconv.Itoa(*n)})
	}
	c.json = *out == "json"
	dirs, err := local.Locate(c.getenv)
	if err != nil {
		return c.fail(err)
	}
	coord, err := readStore(ctx, c, dirs)
	switch {
	case errors.Is(err, errNoStore) && len(positional) == 0:
		// No store, no runs.
		return writeRuns(c, nil)
	case err != nil:
		if len(positional) > 0 {
			err = runNotFound(positional[0], err)
		}
		return c.fail(err)
	}
	defer func() { _ = coord.Close() }()
	if len(positional) == 0 {
		runs, err := coord.Runs(ctx, *n)
		if err != nil {
			return c.fail(err)
		}
		return writeRuns(c, runs)
	}
	id := positional[0]
	s, err := coord.Status(ctx, id)
	if err != nil {
		return c.fail(runNotFound(id, err))
	}
	jobs, err := coord.Jobs(ctx, id)
	if err != nil {
		return c.fail(runNotFound(id, err))
	}
	return writeRun(c, s, jobs)
}

// A statusLine is one of the JSON lines meridian status writes: a run, or
// one of its jobs, of local.ProgressVersion, as meridian run's are.
type statusLine struct {
	Version     int                  `json:"version"`
	Kind        string               `json:"kind"`
	Run         string               `json:"run"`
	Job         string               `json:"job,omitempty"`
	Name        string               `json:"name,omitempty"`
	Location    string               `json:"location,omitempty"`
	RunDir      string               `json:"runDir,omitempty"`
	State       string               `json:"state"`
	Outcome     string               `json:"outcome,omitempty"`
	Reason      string               `json:"reason,omitempty"`
	Created     time.Time            `json:"created,omitzero"`
	Started     time.Time            `json:"started,omitzero"`
	Ended       time.Time            `json:"ended,omitzero"`
	Stopped     time.Time            `json:"stopped,omitzero"`
	StopReason  string               `json:"stopReason,omitempty"`
	Attempts    int                  `json:"attempts,omitzero"`
	Member      string               `json:"member,omitempty"`
	Diagnostics problems.Diagnostics `json:"diagnostics,omitempty"`
}

func runStatusLine(s *coordinator.RunStatus) statusLine {
	return statusLine{
		Kind: "run", Run: s.ID, Name: s.Name, Location: s.Location, RunDir: s.RunDir, State: string(s.State),
		Outcome: string(s.Outcome), Reason: string(s.Reason), Created: s.CreatedAt, Started: s.StartedAt, Ended: s.EndedAt,
		Stopped: s.StoppedAt, StopReason: string(s.StopReason), Diagnostics: s.Diagnostics,
	}
}

// stateOf is the run's state as a person reads it: stopping for one that
// runs on once it was stopped, as by meridian cancel, until its jobs have
// ended.
func stateOf(s *coordinator.RunStatus) string {
	if s.State == coordinator.RunRunning && !s.StoppedAt.IsZero() {
		return "stopping"
	}
	return string(s.State)
}

func writeStatusJSON(w io.Writer, l statusLine) error {
	l.Version = local.ProgressVersion
	data, err := jsonv2.Marshal(l)
	if err != nil {
		return err
	}
	_, err = w.Write(append(data, '\n'))
	return err
}

// writeRuns writes the runs, the newest first: a table, a run a line, or a
// JSON line for each.
func writeRuns(c *cli, runs []coordinator.RunStatus) int {
	if c.json {
		for i := range runs {
			if err := writeStatusJSON(c.stdout, runStatusLine(&runs[i])); err != nil {
				return c.fail(err)
			}
		}
		return exitOK
	}
	if len(runs) == 0 {
		fmt.Fprintln(c.stdout, "no runs")
		return exitOK
	}
	tw := tabwriter.NewWriter(c.stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "RUN\tSTATE\tOUTCOME\tCREATED\tWORKFLOW")
	for _, r := range runs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", r.ID, stateOf(&r), outcomeOf(r.Outcome, r.Reason), timeOf(r.CreatedAt), r.Location)
	}
	if err := tw.Flush(); err != nil {
		return c.fail(err)
	}
	return exitOK
}

// writeRun writes the run s and its jobs: as text, the run, then a table,
// a job a line, then the run's problems and the jobs', as compilers write
// them; or a JSON line for the run, and one for each job.
func writeRun(c *cli, s *coordinator.RunStatus, jobs []coordinator.JobStatus) int {
	if c.json {
		if err := writeStatusJSON(c.stdout, runStatusLine(s)); err != nil {
			return c.fail(err)
		}
		for _, j := range jobs {
			l := statusLine{
				Kind: "job", Run: s.ID, Job: j.Key, Name: j.Name, State: string(j.State), Outcome: string(j.Outcome), Reason: string(j.Reason),
				Attempts: j.Attempts, Member: j.Member, Diagnostics: j.Diagnostics,
			}
			if err := writeStatusJSON(c.stdout, l); err != nil {
				return c.fail(err)
			}
		}
		return exitOK
	}
	line := fmt.Sprintf("run %s: %s", s.ID, stateOf(s))
	switch {
	case s.Outcome != "":
		line += ", " + outcomeOf(s.Outcome, s.Reason)
	case s.StopReason != "":
		line += " (" + string(s.StopReason) + ")"
	}
	fmt.Fprintln(c.stdout, line)
	fmt.Fprintf(c.stdout, "workflow: %s\n", s.Location)
	fmt.Fprintf(c.stdout, "run directory: %s\n", s.RunDir)
	times := "created " + timeOf(s.CreatedAt)
	if !s.StartedAt.IsZero() {
		times += ", started " + timeOf(s.StartedAt)
	}
	if !s.EndedAt.IsZero() {
		times += ", ended " + timeOf(s.EndedAt)
	}
	fmt.Fprintln(c.stdout, times)
	if len(jobs) > 0 {
		fmt.Fprintln(c.stdout)
		tw := tabwriter.NewWriter(c.stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "JOB\tSTATE\tOUTCOME\tATTEMPTS\tWHERE")
		for _, j := range jobs {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\n", j.Key, j.State, outcomeOf(j.Outcome, j.Reason), j.Attempts, j.Member)
		}
		if err := tw.Flush(); err != nil {
			return c.fail(err)
		}
	}
	var diags []string
	for _, d := range s.Diagnostics {
		diags = append(diags, "meridian: "+d.String())
	}
	for _, j := range jobs {
		for _, d := range j.Diagnostics {
			diags = append(diags, j.Key+": "+d.String())
		}
	}
	if len(diags) > 0 {
		fmt.Fprintln(c.stdout)
		for _, d := range diags {
			fmt.Fprintln(c.stdout, d)
		}
	}
	return exitOK
}

// outcomeOf is how a run or a job ended, with why when it ended other than
// as its steps did, or "" until it has.
func outcomeOf[O, R ~string](outcome O, reason R) string {
	if reason != "" {
		return string(outcome) + " (" + string(reason) + ")"
	}
	return string(outcome)
}

// timeOf is t as a person reads it, in their own time zone, or "" for the
// zero time.
func timeOf(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.In(time.Local).Format(time.DateTime)
}
