package coordinator

import (
	"context"
	"errors"

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
// the job.
func (w *finishWorker) Work(ctx context.Context, job *hopper.Job[finishArgs]) error {
	err := w.c.finishRun(ctx, job.Args.Run, func(ctx context.Context, tx *store.Tx) error {
		return hopper.CompleteTx(ctx, tx.SQL(), job)
	})
	if errors.Is(err, hopper.ErrJobFenced) {
		return nil
	}
	return err
}

// finishRun ends the run id, once its jobs have, with its outcome, and
// deletes its secret record, in one transaction, which done writes to as
// well. A run that has ended, as one whose Start failed, is left as it is;
// one that never started, as one whose Start the coordinator gave up on,
// ends failed, with run_not_started. A run that has started ends failed if
// one of its jobs failed, or was skipped because a job it needs failed,
// and with success otherwise.
func (c *Coordinator) finishRun(ctx context.Context, id string, done func(context.Context, *store.Tx) error) error {
	return c.store.Tx(ctx, func(tx *store.Tx) error {
		r, err := tx.Run(id)
		if err != nil {
			return err
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
