package coordinator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// StoreID returns the store's ID, which is random, and which keeps what is
// named after a run's ID, such as its directory or its logs, apart from
// what a store made again, whose runs are numbered from 1 again, names
// after the same ID (RunOptions.RunDir).
func (c *Coordinator) StoreID() string { return c.store.ID() }

// ErrRunNotFound is what Status returns for a run the store doesn't have.
var ErrRunNotFound = errors.New("coordinator: no such run")

// A RunState is where a run is in its life.
type RunState string

// The states of a run.
const (
	// RunCreated is a run stored, which hasn't started.
	RunCreated RunState = RunState(store.RunCreated)
	// RunRunning is a run that has started.
	RunRunning RunState = RunState(store.RunRunning)
	// RunEnded is a run that has ended, with its outcome.
	RunEnded RunState = RunState(store.RunEnded)
)

// A RunStatus is a run as the store holds it.
type RunStatus struct {
	ID    string
	State RunState
	// Outcome and Reason are how the run ended, once it has.
	Outcome plan.Outcome
	Reason  plan.Reason
	// RunDir is the run's directory.
	RunDir string
	// Name and Location are the plan's: the run's name and its workflow's
	// location. PlanVersion is the plan's version.
	Name        string
	Location    string
	PlanVersion int
	// CreatedAt is when the run was stored, StartedAt when it started, and
	// EndedAt when it ended, each the zero time until then, by the store's
	// clock.
	CreatedAt time.Time
	StartedAt time.Time
	EndedAt   time.Time
	// StoppedAt is when the run was stopped early, as by Cancel, and
	// StopReason why, cancelled or timeout, or the zero time and "" for
	// one that hasn't been.
	StoppedAt  time.Time
	StopReason plan.Reason
	// Diagnostics are the run's problems: Start's, and the coordinator's.
	// Runs gives none.
	Diagnostics problems.Diagnostics
	// Parent is the try of the step that called a child run, or nil for a
	// run no step called; and Root the ID of the run at the top of its
	// tree. Descendants is how many runs of a root's tree there are below
	// it, at any depth.
	Parent      *Caller
	Root        string
	Descendants int
}

// statusOf is the run r's status, whose summary is sum.
func statusOf(r *store.Run, sum *store.RunSummary) *RunStatus {
	s := summaryStatus(sum)
	s.Diagnostics = r.Diagnostics
	return s
}

// summaryStatus is the status of the run whose summary is r, without its
// problems.
func summaryStatus(r *store.RunSummary) *RunStatus {
	s := &RunStatus{
		ID: r.ID, State: RunState(r.State), Outcome: r.Outcome, Reason: r.Reason, RunDir: r.RunDir,
		Name: r.Name, Location: r.Location, PlanVersion: r.PlanVersion,
		CreatedAt: r.CreatedAt, StartedAt: r.StartedAt, EndedAt: r.EndedAt,
		StoppedAt: r.StoppedAt, StopReason: r.StopReason, Descendants: r.Descendants,
	}
	if l := r.Parent; l != nil {
		s.Parent, s.Root = &Caller{Run: l.Run, Job: l.Job, Attempt: r.ParentAttempt, Step: l.Step, Try: l.Try}, l.Root
	}
	return s
}

// Status returns the run id as the store holds it now, or ErrRunNotFound.
func (c *Coordinator) Status(ctx context.Context, id string) (*RunStatus, error) {
	var s *RunStatus
	err := c.store.View(ctx, func(tx *store.Tx) error {
		r, err := tx.Run(id)
		if errors.Is(err, store.ErrNotFound) {
			return ErrRunNotFound
		}
		if err != nil {
			return err
		}
		sum, err := tx.RunSummary(id)
		if err != nil {
			return err
		}
		s = statusOf(r, sum)
		return nil
	})
	return s, err
}

// Runs returns the store's last limit runs that no step called, the newest
// first, or, with all, of every run, the runs steps called among them,
// without their problems.
func (c *Coordinator) Runs(ctx context.Context, limit int, all bool) ([]RunStatus, error) {
	var runs []store.RunSummary
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		runs, err = tx.RecentRuns(limit, all)
		return err
	})
	return statuses(runs), err
}

// Children returns the runs the steps of the run id called, its children,
// in the order of their IDs, without their problems: ErrRunNotFound for a
// run the store doesn't have.
func (c *Coordinator) Children(ctx context.Context, id string) ([]RunStatus, error) {
	var runs []store.RunSummary
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		runs, err = tx.Children(id)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrRunNotFound
	}
	return statuses(runs), err
}

// statuses are the statuses of the runs whose summaries are runs.
func statuses(runs []store.RunSummary) []RunStatus {
	out := make([]RunStatus, len(runs))
	for i := range runs {
		out[i] = *summaryStatus(&runs[i])
	}
	return out
}

// A JobState is where a job, or a matrix instance, is in its life.
type JobState string

// The states of a job: waiting for the jobs it needs, or for the run to
// start; ready, once its dependencies have finished, and between its
// attempts; placing, while an attempt's worker is being started; running,
// once the worker has claimed it; and ended, with its outcome.
const (
	JobWaiting JobState = "waiting"
	JobReadied JobState = "ready"
	JobPlaced  JobState = "placing"
	JobRuns    JobState = "running"
	JobEnded   JobState = "ended"
)

// A JobStatus is a job of a run, or a matrix instance, as the store holds
// it.
type JobStatus struct {
	// Key is the job's name, or an instance's key, as in build#2, and Name
	// the job's name.
	Key   string
	Name  string
	State JobState
	// Outcome and Reason are how the job ended, once it has.
	Outcome plan.Outcome
	Reason  plan.Reason
	// Attempts is how many attempts it has had, and Member where its
	// latest was placed, as its group and member joined with a slash, or ""
	// where that is private, or before one is.
	Attempts int
	Member   string
	// Diagnostics are the job's problems, as Ready's or placement's.
	Diagnostics problems.Diagnostics
}

// Jobs returns the run id's jobs, and their instances, in the order of
// their keys: ErrRunNotFound for a run the store doesn't have.
func (c *Coordinator) Jobs(ctx context.Context, id string) ([]JobStatus, error) {
	var out []JobStatus
	err := c.store.View(ctx, func(tx *store.Tx) error {
		jobs, err := tx.Jobs(id)
		if errors.Is(err, store.ErrNotFound) {
			return ErrRunNotFound
		}
		if err != nil {
			return err
		}
		out = make([]JobStatus, len(jobs))
		for i, j := range jobs {
			s := JobStatus{Key: j.Key, Name: j.Name, State: JobWaiting, Outcome: j.Outcome, Reason: j.Reason, Attempts: j.Attempts, Diagnostics: j.Diagnostics}
			if !j.ReadyAt.IsZero() {
				s.State = JobReadied
			}
			if j.Attempts > 0 {
				a, err := tx.LatestAttempt(id, j.Key)
				if err != nil && !errors.Is(err, store.ErrNotFound) {
					return err
				}
				if a != nil {
					if a.Placement.Group != "" {
						s.Member = a.Placement.Group + "/" + a.Placement.Member
					}
					switch a.State {
					case store.AttemptPlacing:
						s.State = JobPlaced
					case store.AttemptClaimed, store.AttemptRunning:
						s.State = JobRuns
					}
				}
			}
			if j.Outcome != "" {
				s.State = JobEnded
			}
			out[i] = s
		}
		return nil
	})
	return out, err
}

// Resume takes up the run id, as a coordinator of its store, for a
// coordinator that stopped working it, as when the meridian run that
// created it died, or left it running: unless the run has started, it
// works the run's start queue of its own (Options.StartQueue), so that the
// run's Start runs here, reading its variables where this coordinator
// does, once no other coordinator that lives holds the run's Start, as
// the one that created it does (store.HoldStart): while one does, Start
// runs there. The rest of the run, Serve works, as every coordinator of
// the store does. check, unless it is nil, is called with the run's status
// before anything of it is worked, as to check that its directory is still
// safe: an error from it is returned, and the run left as it is. A run
// whose plan only a newer release reads is a problem of kind
// run_plan_newer, which leaves it as it is too, and one the store doesn't
// have ErrRunNotFound. It returns the run's status.
func (c *Coordinator) Resume(ctx context.Context, id string, check func(*RunStatus) error) (*RunStatus, error) {
	if err := c.writable(); err != nil {
		return nil, err
	}
	var sum *store.RunSummary
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		sum, err = tx.RunSummary(id)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrRunNotFound
	}
	if err != nil {
		return nil, err
	}
	newer := problems.RunPlanNewer.Problem(problems.Params{"run": id, "version": sum.PlanVersion, "latest": plan.Version})
	if sum.PlanVersion > plan.Version {
		return nil, newer
	}
	s, err := c.Status(ctx, id)
	if errors.Is(err, plan.ErrVersion) {
		// What it started with only a newer release reads.
		return nil, newer
	}
	if err != nil {
		return nil, err
	}
	if check != nil {
		if err := check(s); err != nil {
			return nil, err
		}
	}
	q := c.startQueue(id)
	if q == sharedQueue {
		return s, nil
	}
	release, err := c.store.HoldStart(id)
	switch {
	case errors.Is(err, store.ErrStartHeld):
		// The coordinator that holds it works the run's start queue.
		return s, nil
	case err != nil:
		return nil, err
	case s.State != RunCreated:
		// The run's Start has run: what is let go is a hold that a
		// coordinator that ended without releasing it left.
		release()
		return s, nil
	}
	if err := c.workQueue(ctx, q, release); err != nil {
		return nil, err
	}
	return s, nil
}

// ErrRunStarted is what FailRun returns for a run that has started, or
// ended.
var ErrRunStarted = errors.New("coordinator: the run has started")

// FailRun ends the run id, which hasn't started, as failed, with p, a
// problem of one of the problems package's kinds, as for a run whose
// directory couldn't be made once it was stored: its Start, when a
// coordinator works it, leaves it as it is, and so does its finisher. A
// run that has started, or ended, is ErrRunStarted, and one the store
// doesn't have ErrRunNotFound.
func (c *Coordinator) FailRun(ctx context.Context, id string, p *problem.Problem) error {
	if err := c.writable(); err != nil {
		return err
	}
	k, ok := problems.Lookup(p.Code)
	if !ok {
		return fmt.Errorf("coordinator: %q isn't a problem's kind", p.Code)
	}
	return c.store.Tx(ctx, func(tx *store.Tx) error {
		r, err := tx.Run(id)
		switch {
		case errors.Is(err, store.ErrNotFound):
			return ErrRunNotFound
		case err != nil:
			return err
		case r.State != store.RunCreated:
			return ErrRunStarted
		}
		if err := tx.AddDiagnostics(id, problems.Diagnostics{k.At("", problems.Position{}, "", p.Params)}); err != nil {
			return err
		}
		return tx.EndRun(id, plan.OutcomeFailure, "")
	})
}

// An EndpointHold is an endpoint that workers reach the coordinators of
// its store through, such as the local socket, which this process holds,
// until it releases it, closes the coordinator, or ends (HoldEndpoint).
type EndpointHold struct{ h *store.Hold }

// HoldEndpoint takes the endpoint named name, which is lower-case letters,
// digits and dashes, for holder, which says who holds it, as a person can
// read it. Only one process holds an endpoint at a time, under a lock on a
// file beside the store's database, which the system releases when the
// process ends, however it ends: HoldEndpoint waits for the process that
// holds it for as long as ctx allows. The store records who holds it and
// since when, from which the grace after a takeover is counted.
func (c *Coordinator) HoldEndpoint(ctx context.Context, name, holder string) (*EndpointHold, error) {
	if err := c.writable(); err != nil {
		return nil, err
	}
	h, err := c.store.HoldEndpoint(ctx, name, holder)
	if err != nil {
		return nil, err
	}
	return &EndpointHold{h: h}, nil
}

// EndpointHeld reports whether a process, this one or another, holds the
// endpoint named name now (HoldEndpoint), without taking it: as whether a
// coordinator of the store that serves its workers lives.
func (c *Coordinator) EndpointHeld(name string) (bool, error) {
	return c.store.EndpointHeld(name)
}

// Release stops holding the endpoint, even if ctx has ended.
func (e *EndpointHold) Release(ctx context.Context) error { return e.h.Release(ctx) }
