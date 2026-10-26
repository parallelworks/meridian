package coordinator

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// supervision is how a job's supervisor waits, and how many times it
// tries.
type supervision struct {
	// setup is how long the supervisor waits before it places a job again
	// after the n-th of its attempts failed before a step started:
	// setup[n-1]. The failure after the last, or the second with a
	// problem of the same code, fails the job.
	setup []time.Duration
	// A backend that couldn't answer as a job was placed is asked again
	// after first, doubling up to most, tries times in a row, after which
	// the job fails.
	first, most time.Duration
	tries       int
	// wait is how long the supervisor waits for a live attempt between
	// looks at it, when nothing wakes it sooner (wakeJob).
	wait time.Duration
	// timeout bounds one look, with the placement it makes.
	timeout time.Duration
}

// defaultSupervision places a job again 5 seconds, 30 seconds and 2
// minutes after its first three setup failures, and asks a backend that
// couldn't answer 10 times, from 1 second to 1 minute apart.
var defaultSupervision = supervision{
	setup: []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute},
	first: time.Second, most: time.Minute, tries: 10,
	wait: 10 * time.Second, timeout: 5 * time.Minute,
}

// backoff returns how long to wait after a backend couldn't answer tries
// times in a row.
func (s supervision) backoff(tries int) time.Duration {
	return startRetry{first: s.first, most: s.most}.backoff(tries)
}

// jobWorker works runs' jobs' supervisors (jobArgs).
type jobWorker struct {
	hopper.WorkerDefaults[jobArgs]
	c *Coordinator
}

// Work supervises the job, as supervise does, and completes the hopper job
// in the transaction that ends it.
func (w *jobWorker) Work(ctx context.Context, job *hopper.Job[jobArgs]) error {
	done := func(ctx context.Context, tx *store.Tx) error { return hopper.CompleteTx(ctx, tx.SQL(), job) }
	snooze, err := w.c.supervise(ctx, job.Args.Run, job.Args.Key, done)
	switch {
	case stopping(ctx):
		// What cut the look short is the coordinator's stopping: each of
		// a supervisor's steps can be done again.
		return hopper.Snooze(w.c.supervision.first)
	case errors.Is(err, errSettled):
		return w.c.complete(ctx, done)
	case errors.Is(err, hopper.ErrJobFenced):
		return nil
	case err != nil:
		return err
	case snooze > 0:
		return hopper.Snooze(snooze)
	}
	return nil
}

// Timeout bounds one look at the job.
func (w *jobWorker) Timeout(*hopper.Job[jobArgs]) time.Duration { return w.c.supervision.timeout }

// NextRetry is when the supervisor looks again after the coordinator itself
// failed, as when its store did.
func (w *jobWorker) NextRetry(job *hopper.Job[jobArgs]) time.Time {
	return time.Now().Add(w.c.supervision.backoff(job.Attempt))
}

// errAgain is a step of a supervisor's that found the job changed under
// it, as by another attempt of the supervisor: it looks again.
var errAgain = errors.New("coordinator: the job changed meanwhile")

// supervise drives the run's job key from Ready to its end, as
// docs/design.md says under A job's lifecycle: it readies the job, ends it
// if Ready skips it or fails, places an attempt of it, waits on it, and
// ends the job as its attempt ended, or places another after a setup
// failure. It ends the job in a transaction that done writes to as well.
//
// It returns how long to wait before it looks again, when it waits; or
// errSettled for a job that has ended, or a run that isn't running, for
// which there is nothing to do. Any other error is the coordinator's own,
// which hopper tries again.
//
// Each step can run twice, as a supervisor that hopper rescued from a
// replica still running it does: each acts only on the job and the
// attempt as the store holds them when it acts, and a new attempt fences
// any other, as the job's fence says.
func (c *Coordinator) supervise(ctx context.Context, runID, key string, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	for look := 0; ; look++ {
		var (
			run *store.Run
			job *store.Job
			a   *store.Attempt
		)
		err := c.store.View(ctx, func(tx *store.Tx) error {
			var err error
			if run, err = tx.Run(runID); err != nil || run.State != store.RunRunning {
				return err
			}
			if job, err = tx.Job(runID, key); err != nil || job.Outcome != "" {
				return err
			}
			if a, err = tx.LatestAttempt(runID, key); errors.Is(err, store.ErrNotFound) {
				return nil
			}
			return err
		})
		switch {
		case errors.Is(err, plan.ErrVersion):
			return c.newer(ctx, runID), nil
		case err != nil:
			return 0, err
		case run.State != store.RunRunning || job.Outcome != "":
			return 0, errSettled
		case run.PlanVersion != plan.Version:
			return c.newer(ctx, runID), nil
		}
		wait, err := c.step(ctx, run, job, a, done)
		switch {
		case errors.Is(err, errAgain) && look < 10:
			continue
		case errors.Is(err, plan.ErrVersion):
			return c.newer(ctx, runID), nil
		case err != nil || wait == 0:
			return 0, err
		}
		// What woke the supervisor as it looked, it looks at again, rather
		// than wait for it.
		var now *store.Job
		if err := c.store.View(ctx, func(tx *store.Tx) error {
			var err error
			now, err = tx.Job(runID, key)
			return err
		}); err != nil {
			return 0, err
		}
		if now.Wake == job.Wake || look >= 10 {
			return wait, nil
		}
	}
}

// newer is how long to wait before looking again at the run id, whose plan
// only a newer release reads, as the log says.
func (c *Coordinator) newer(ctx context.Context, id string) time.Duration {
	c.log.WarnContext(ctx, "coordinator: a run's plan is newer than this release reads, so a release that reads it runs its jobs", "run", id, "code", problems.RunPlanNewer.Code())
	return c.retry.newer
}

// step takes the next step of the job, whose latest attempt is a, or nil
// before its first: it places one, waits on it, or ends the job as it
// ended. It returns how long to wait before the next, or 0 once it has
// ended the job.
func (c *Coordinator) step(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	switch {
	case a == nil:
		return c.placeJob(ctx, run, job, done)
	case a.State == store.AttemptEnded:
		return c.attemptEnded(ctx, run, job, a, done)
	case a.State == store.AttemptPlacing && a.Placement.ID == "":
		// Placed by a supervisor that ended before it recorded where.
		return c.resumePlace(ctx, run, job, a)
	default:
		// Placed, claimed or running: its worker reports to the worker
		// API, which wakes the supervisor once the attempt ends.
		return c.supervision.wait, nil
	}
}

// attemptEnded ends the job as its latest attempt, a, ended, or places it
// again after a setup failure.
func (c *Coordinator) attemptEnded(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	switch {
	case a.End.Reason == plan.ReasonSetupFailed:
		return c.setupFailed(ctx, run, job, a, done)
	case a.End.Kind == store.EndCompleted:
		report, err := a.Complete()
		if err != nil {
			return 0, err
		}
		return 0, c.endJob(ctx, run.ID, job.Key, store.JobEnd{Result: report.Result, Reason: report.Reason, Diagnostics: report.Diagnostics}, done)
	default:
		// A worker lost, or a placement withdrawn: what follows them,
		// retry: worker-lost among it, isn't built yet, and until it is
		// the job fails.
		return 0, c.endJob(ctx, run.ID, job.Key, store.JobEnd{
			Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Reason: a.End.Reason,
			Diagnostics: problems.Diagnostics{diagnostic(problems.CoordinatorJobFailed, problems.Params{"job": job.Key})},
		}, done)
	}
}

// setupFailed decides what follows the job's latest attempt, a, which
// failed before a step started: the job fails after as many setup
// failures as the supervisor allows, or after two with a problem of the
// same code; and otherwise it is placed again, once the wait after that
// many failures has passed, by the store's clock.
func (c *Coordinator) setupFailed(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	var (
		failures []*store.Attempt
		now      time.Time
		private  bool
	)
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if failures, err = tx.SetupFailures(run.ID, job.Key); err != nil {
			return err
		}
		place, _, err := tx.ChosenPlace(a.ID)
		if err != nil {
			return err
		}
		private = place.Private != nil
		now, err = tx.Now()
		return err
	})
	if err != nil {
		return 0, err
	}
	n := len(failures)
	if n == 0 {
		return 0, errAgain
	}
	last := failures[n-1].End.Problem
	same := last != nil && slices.ContainsFunc(failures[:n-1], func(f *store.Attempt) bool {
		return f.End.Problem != nil && f.End.Problem.Code == last.Code
	})
	if n > len(c.supervision.setup) || same {
		diags := problems.Diagnostics{diagnostic(problems.SetupFailed, problems.Params{"job": job.Key, "tries": n})}
		if last != nil {
			diags = append(diags, maskedIf(private, problemDiagnostic(last)))
		}
		return 0, c.endJob(ctx, run.ID, job.Key, store.JobEnd{
			Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Reason: plan.ReasonSetupFailed, Diagnostics: diags,
		}, done)
	}
	if wait := a.EndedAt.Add(c.supervision.setup[n-1]).Sub(now); wait > 0 {
		return wait, nil
	}
	return c.placeJob(ctx, run, job, done)
}

// endJob ends the run's job key as end says, in a transaction that done
// writes to as well. A job that has ended meanwhile is errSettled, and one
// that has an attempt that hasn't, as one another supervisor placed,
// errAgain.
func (c *Coordinator) endJob(ctx context.Context, runID, key string, end store.JobEnd, done func(context.Context, *store.Tx) error) error {
	return c.store.Tx(ctx, func(tx *store.Tx) error {
		if err := tx.EndJob(runID, key, end); errors.Is(err, store.ErrState) {
			j, jerr := tx.Job(runID, key)
			switch {
			case jerr != nil:
				return jerr
			case j.Outcome != "":
				return errSettled
			}
			return errAgain
		} else if err != nil {
			return err
		}
		return done(ctx, tx)
	})
}

// wakeJob wakes the supervisor of the run's job key, once something it
// waits for has happened, as an attempt's claim or end: the worker API
// calls it once it has stored the change. A supervisor that is looking
// reads the job's wake again before it waits; one that waits looks at
// once, as hopper's JobRetry makes it.
func (c *Coordinator) wakeJob(ctx context.Context, run, key string) {
	var j *store.Job
	if err := c.store.Tx(ctx, func(tx *store.Tx) error {
		var err error
		j, err = tx.Wake(run, key)
		return err
	}); err != nil {
		c.log.DebugContext(ctx, "coordinator: wake a job's supervisor", "run", run, "job", key, "error", err)
		return
	}
	if j.Outcome != "" {
		// Its supervisor completed as it ended the job, and JobRetry would
		// run it again.
		return
	}
	c.mu.Lock()
	client := c.served
	c.mu.Unlock()
	if client == nil {
		client = c.inserter
	}
	// A supervisor that is running reads the wake before it waits, unless
	// it read the job last just before the wake, and is about to wait: so
	// it is woken again, a few times, briefly, in case it was.
	for try := 0; ; try++ {
		_, err := client.JobRetry(ctx, j.SupervisorID)
		switch {
		case err == nil:
			return
		case !errors.Is(err, hopper.ErrJobRunning):
			c.log.DebugContext(ctx, "coordinator: wake a job's supervisor", "run", run, "job", key, "error", err)
			return
		case try == wakeTries-1:
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wakeAgain):
		}
	}
}

// A supervisor that is running as it is woken is woken again wakeTries
// times in all, wakeAgain apart.
const (
	wakeTries = 5
	wakeAgain = 20 * time.Millisecond
)
