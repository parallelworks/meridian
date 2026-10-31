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

// runStatus shows the store's last runs, or one run and its jobs, and the
// runs its steps called, as text a person reads, or as JSON lines, as
// meridian run writes them. The list holds the runs no step called, each
// with how many runs of its tree are below it, unless -all says to list
// every run.
func runStatus(ctx context.Context, c *cli, fs *flag.FlagSet, args []string) int {
	out := fs.String("format", "text", "writes the runs, or the run and its jobs, as `text`, a table a person reads, or as json, one object a line, whose version member says how to read it")
	n := fs.Int("n", statusRuns, "lists the last `number` runs")
	all := fs.Bool("all", false, "lists the runs steps called too, not only those no step did")
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
		runs, err := coord.Runs(ctx, *n, *all)
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
	children, err := coord.Children(ctx, id)
	if err != nil {
		return c.fail(runNotFound(id, err))
	}
	return writeRun(c, s, jobs, children)
}

// A statusLine is one of the JSON lines meridian status writes: a run, one
// of its jobs, or a run one of its steps called, of
// local.ProgressVersion, as meridian run's are.
type statusLine struct {
	Version int    `json:"version"`
	Kind    string `json:"kind"`
	Run     string `json:"run"`
	// Parent is the try of the step that called the run, of a run a step
	// called, and Root the run at the top of its tree; Descendants is how
	// many runs of a root's tree are below it.
	Parent      *statusCaller        `json:"parent,omitempty"`
	Root        string               `json:"root,omitempty"`
	Descendants int                  `json:"descendants,omitzero"`
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

// A statusCaller is the try of a step that called a run, in a JSON line of
// meridian status's, as in meridian run's.
type statusCaller struct {
	Run     string `json:"run"`
	Job     string `json:"job"`
	Attempt int    `json:"attempt"`
	Step    int    `json:"step"`
	Try     int    `json:"try,omitzero"`
}

func runStatusLine(s *coordinator.RunStatus) statusLine {
	l := statusLine{
		Kind: "run", Run: s.ID, Name: s.Name, Location: s.Location, RunDir: s.RunDir, State: string(s.State),
		Outcome: string(s.Outcome), Reason: string(s.Reason), Created: s.CreatedAt, Started: s.StartedAt, Ended: s.EndedAt,
		Stopped: s.StoppedAt, StopReason: string(s.StopReason), Diagnostics: s.Diagnostics, Root: s.Root, Descendants: s.Descendants,
	}
	if p := s.Parent; p != nil {
		l.Parent = &statusCaller{Run: p.Run, Job: p.Job, Attempt: p.Attempt, Step: p.Step, Try: p.Try}
	}
	return l
}

// calledAt names the try of the step that called a run, as a person reads
// it: the run, and the step's try (stepOf), as in run 3 build step 1
// retry 2.
func calledAt(p *coordinator.Caller) string {
	return "run " + p.Run + " " + stepOf(p)
}

// stepOf names the try of the step that called a run, as a person reads
// it: its job, its attempt after the first, the step, and its try after
// the first, as in build attempt 2 step 1 retry 2.
func stepOf(p *coordinator.Caller) string {
	s := p.Job
	if p.Attempt > 1 {
		s += fmt.Sprintf(" attempt %d", p.Attempt)
	}
	s += fmt.Sprintf(" step %d", p.Step)
	if p.Try > 0 {
		s += fmt.Sprintf(" retry %d", p.Try)
	}
	return s
}

// treeOf says, in the list of runs, where a run is in its tree: what
// called a run a step called, or how many runs are below a root, or "".
func treeOf(s *coordinator.RunStatus) string {
	switch {
	case s.Parent != nil:
		return "called by " + calledAt(s.Parent)
	case s.Descendants == 1:
		return "1 run below"
	case s.Descendants > 1:
		return fmt.Sprintf("%d runs below", s.Descendants)
	}
	return ""
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
	// Where each run is in its tree, if one of them is in one.
	tree := slices.ContainsFunc(runs, func(r coordinator.RunStatus) bool { return treeOf(&r) != "" })
	tw := tabwriter.NewWriter(c.stdout, 0, 0, 2, ' ', 0)
	header := "RUN\tSTATE\tOUTCOME\tCREATED\tWORKFLOW"
	if tree {
		header += "\tTREE"
	}
	fmt.Fprintln(tw, header)
	for _, r := range runs {
		row := fmt.Sprintf("%s\t%s\t%s\t%s\t%s", r.ID, stateOf(&r), outcomeOf(r.Outcome, r.Reason), timeOf(r.CreatedAt), problems.EscapeControl(r.Location))
		if tree {
			row += "\t" + treeOf(&r)
		}
		fmt.Fprintln(tw, row)
	}
	if err := tw.Flush(); err != nil {
		return c.fail(err)
	}
	return exitOK
}

// writeRun writes the run s, its jobs, and the runs its steps called,
// children: as text, the run, and what called it, then a table, a job a
// line, then one of the runs its steps called, a run a line, then the
// run's problems and the jobs', as compilers write them; or a JSON line
// for the run, one for each job, and one for each run its steps called.
func writeRun(c *cli, s *coordinator.RunStatus, jobs []coordinator.JobStatus, children []coordinator.RunStatus) int {
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
		for i := range children {
			l := runStatusLine(&children[i])
			l.Kind = "child"
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
	if s.Parent != nil {
		fmt.Fprintf(c.stdout, "called by: %s, of the tree of run %s\n", calledAt(s.Parent), s.Root)
	}
	fmt.Fprintf(c.stdout, "workflow: %s\n", problems.EscapeControl(s.Location))
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
	if len(children) > 0 {
		fmt.Fprintln(c.stdout)
		tw := tabwriter.NewWriter(c.stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "CALLED BY\tRUN\tSTATE\tOUTCOME\tWORKFLOW")
		for _, r := range children {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", stepOf(r.Parent), r.ID, stateOf(&r), outcomeOf(r.Outcome, r.Reason), problems.EscapeControl(r.Location))
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
