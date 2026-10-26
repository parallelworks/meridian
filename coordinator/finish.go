package coordinator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// finishWorker works runs' finishers (finishArgs), which run once each of
// the run's other hopper jobs has finished, whatever its outcome.
type finishWorker struct {
	hopper.WorkerDefaults[finishArgs]
	c *Coordinator
}

// Work ends the run, as finishRun does, in the transaction that completes
// the job, and stops working the run's start queue, if the coordinator
// still does, as one whose Start hopper gave up on.
func (w *finishWorker) Work(ctx context.Context, job *hopper.Job[finishArgs]) error {
	snooze, err := w.c.finishRun(ctx, job.Args.Run, func(ctx context.Context, tx *store.Tx) error {
		return hopper.CompleteTx(ctx, tx.SQL(), job)
	})
	switch {
	case stopping(ctx):
		return hopper.Snooze(w.c.retry.first)
	case errors.Is(err, hopper.ErrJobFenced):
		return nil
	case err != nil:
		return err
	case snooze > 0:
		return hopper.Snooze(snooze)
	}
	w.c.retireQueue(ctx, w.c.startQueue(job.Args.Run))
	return nil
}

// finishRun ends the run id, once its jobs have, with its outcome, and
// deletes its secret record, in one transaction, which done writes to as
// well. A run that has ended, as one whose Start failed, is left as it is;
// one that never started, as one whose Start the coordinator gave up on,
// ends failed, with run_not_started. A run that has started ends failed if
// one of its jobs failed, or was skipped because a job it needs failed,
// and with success otherwise; a job that never ended, as one whose
// supervisor hopper gave up on, fails first (failUnended). A run of a plan only a newer release reads
// is left to one, as Start leaves it, and finishRun returns how long to
// wait before trying again.
func (c *Coordinator) finishRun(ctx context.Context, id string, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	err := c.store.Tx(ctx, func(tx *store.Tx) error {
		// A newer run's start state, of a newer version, doesn't decode:
		// plan.ErrVersion, as its plan's version says.
		r, err := tx.Run(id)
		if err != nil {
			return err
		}
		if r.PlanVersion != plan.Version {
			return fmt.Errorf("%w %d of run %s's plan", plan.ErrVersion, r.PlanVersion, id)
		}
		switch r.State {
		case store.RunCreated:
			if err := tx.AddDiagnostics(id, problems.Diagnostics{diagnostic(problems.RunNotStarted, problems.Params{"run": id})}); err != nil {
				return err
			}
			if err := tx.EndRun(id, plan.OutcomeFailure, ""); err != nil {
				return err
			}
		case store.RunRunning:
			if err := failUnended(tx, id); err != nil {
				return err
			}
			outcome, err := runOutcome(tx, id)
			if err != nil {
				return err
			}
			if err := tx.EndRun(id, outcome, ""); err != nil {
				return err
			}
		}
		return done(ctx, tx)
	})
	if errors.Is(err, plan.ErrVersion) {
		c.log.WarnContext(ctx, "coordinator: a run's plan is newer than this release reads, so a release that reads it ends the run", "run", id, "code", problems.RunPlanNewer.Code())
		return c.retry.newer, nil
	}
	return 0, err
}

// failUnended fails each job of the run id that hasn't ended, with
// coordinator_job_failed: one whose supervisor hopper gave up on, as after
// the coordinator itself failed again and again. Its attempt that hasn't
// ended is withdrawn, so that its worker's reports are refused.
func failUnended(tx *store.Tx, id string) error {
	jobs, err := tx.Jobs(id)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if j.Outcome != "" {
			continue
		}
		switch a, err := tx.LatestAttempt(id, j.Key); {
		case errors.Is(err, store.ErrNotFound):
		case err != nil:
			return err
		case a.State != store.AttemptEnded:
			if err := tx.EndAttempt(a.ID, store.AttemptEnd{Kind: store.EndWithdrawn}); err != nil {
				return err
			}
		}
		if err := tx.EndJob(id, j.Key, store.JobEnd{
			Result:      plan.JobResult{Outcome: plan.OutcomeFailure},
			Diagnostics: problems.Diagnostics{diagnostic(problems.CoordinatorJobFailed, problems.Params{"job": j.Key})},
		}); err != nil {
			return err
		}
	}
	return nil
}

// runOutcome is how the run id ended, from how its jobs did: failure if one
// failed, or was skipped because a job it needs failed, as format 1 has
// it, and success otherwise.
func runOutcome(tx *store.Tx, id string) (plan.Outcome, error) {
	jobs, err := tx.Jobs(id)
	if err != nil {
		return "", err
	}
	for _, j := range jobs {
		if j.Outcome == plan.OutcomeFailure || j.Result != nil && j.Result.NeedsFailed {
			return plan.OutcomeFailure, nil
		}
	}
	return plan.OutcomeSuccess, nil
}
