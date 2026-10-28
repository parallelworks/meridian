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

// treeWait is how long a root run's finisher waits before it looks again
// whether the runs of its tree have ended, which the end of each wakes it
// for sooner (wakeRootTx).
const treeWait = 30 * time.Second

// finishRun ends the run id, once its jobs have, with its outcome, and
// deletes its secret record, in one transaction, which done writes to as
// well. A root run, which no step called, waits first for every run of its
// tree to end, at any depth: their variables are in its secret record,
// which they read as they are created and started, so it is kept until
// they have; finishRun notes that it waits (store.Tx.WaitForDescendants),
// and returns how long to wait before looking again. The end of a child
// run wakes its root's finisher, if it waits. A run that has ended, as one
// whose Start failed, is left as it is;
// one that never started, as one whose Start the coordinator gave up on,
// ends failed, with run_not_started. A run that has started ends failed if
// one of its jobs failed, or was skipped because a job it needs failed,
// and with success otherwise; a job that never ended, as one whose
// supervisor hopper gave up on, fails first (failUnended), and the
// placement of its attempt it withdraws is cancelled once the transaction
// commits. A run of a plan only a newer release reads is left to one, as
// Start leaves it, and finishRun returns how long to wait before trying
// again.
func (c *Coordinator) finishRun(ctx context.Context, id string, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	ws, err := c.unendedWithdrawals(ctx, id)
	if err != nil {
		return 0, err
	}
	var (
		withdrawn []string
		waits     bool
		woke      *waking
	)
	err = c.store.Tx(ctx, func(tx *store.Tx) error {
		// A newer run's start state, of a newer version, doesn't decode:
		// plan.ErrVersion, as its plan's version says.
		r, err := tx.Run(id)
		if err != nil {
			return err
		}
		if r.PlanVersion > plan.Version {
			return fmt.Errorf("%w %d of run %s's plan", plan.ErrVersion, r.PlanVersion, id)
		}
		if r.State != store.RunEnded && r.Parent == nil {
			live, err := tx.LiveDescendants(id)
			if err != nil {
				return err
			}
			if live > 0 {
				waits = true
				return tx.WaitForDescendants(id)
			}
		}
		if r.State != store.RunEnded && r.Parent != nil {
			if woke, err = c.wakeRootTx(ctx, tx, r.Parent.Root, id); err != nil {
				return err
			}
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
			if withdrawn, err = failUnended(tx, id); err != nil {
				return err
			}
			outcome, err := runOutcome(tx, id)
			if err != nil {
				return err
			}
			// A run cancelled is, whatever its jobs did after; one past its
			// timeout: ends as they did, and says why it failed.
			var reason plan.Reason
			switch {
			case r.StopReason == plan.ReasonCancelled:
				outcome, reason = plan.OutcomeCancelled, plan.ReasonCancelled
			case r.StopReason == plan.ReasonTimeout && outcome == plan.OutcomeFailure:
				reason = plan.ReasonTimeout
			}
			if err := tx.EndRun(id, outcome, reason); err != nil {
				return err
			}
		}
		return done(ctx, tx)
	})
	if errors.Is(err, plan.ErrVersion) {
		c.log.WarnContext(ctx, "coordinator: a run's plan is newer than this release reads, so a release that reads it ends the run", "run", id, "code", problems.RunPlanNewer.Code())
		return c.retry.newer, nil
	}
	if err != nil {
		return 0, err
	}
	if waits {
		return treeWait, nil
	}
	c.cancelPlacements(ctx, id, ws, withdrawn)
	// The root's finisher is woken again only once the placements the run
	// withdrew are cancelled, not as the transaction commits
	// (wakeAgainAfter): woken first, it could end the root, and with it a
	// meridian run that serves the connections those cancels need, before
	// they are sent.
	c.wakeAgain(ctx, woke)
	return 0, nil
}

// wakeRootTx wakes, in tx, the finisher of the root run root, if it waits
// for the runs of its tree to end (store.Run.WaitedAt), as the run child
// of the tree ends in tx, as wakeJobTx wakes a supervisor: it returns the
// finisher if hopper was running it, for the caller to wake again once tx
// commits (wakeAgain), or nil.
func (c *Coordinator) wakeRootTx(ctx context.Context, tx *store.Tx, root, child string) (*waking, error) {
	r, err := tx.Run(root)
	if err != nil || r.State == store.RunEnded || r.WaitedAt.IsZero() || r.FinisherID.IsZero() {
		return nil, err
	}
	return c.retryTx(ctx, tx, &waking{id: r.FinisherID, run: root, key: "the end of run " + child, waits: rootWaits(root)}), nil
}

// rootWaits reports, in a transaction, whether the root run root hasn't
// ended: whether its finisher waits.
func rootWaits(root string) func(*store.Tx) (bool, error) {
	return func(tx *store.Tx) (bool, error) {
		r, err := tx.Run(root)
		return err == nil && r.State != store.RunEnded, err
	}
}

// unendedWithdrawals returns the placements of the attempts that haven't
// ended of the run id's jobs that haven't, which failUnended withdraws,
// as withdrawals finds them: none for a run that isn't running, or whose
// plan only a newer release reads.
func (c *Coordinator) unendedWithdrawals(ctx context.Context, id string) (map[string]*withdrawal, error) {
	var (
		run  *store.Run
		keys []string
	)
	err := c.store.View(ctx, func(tx *store.Tx) error {
		r, err := tx.Run(id)
		if errors.Is(err, plan.ErrVersion) {
			return nil
		}
		if err != nil || r.State != store.RunRunning || r.PlanVersion > plan.Version {
			return err
		}
		jobs, err := tx.Jobs(id)
		if err != nil {
			return err
		}
		run = r
		for _, j := range jobs {
			if j.Outcome == "" {
				keys = append(keys, j.Key)
			}
		}
		return nil
	})
	if err != nil || len(keys) == 0 {
		return nil, err
	}
	return c.withdrawals(ctx, run, keys)
}

// failUnended fails each job of the run id that hasn't ended (failJob),
// and returns the attempts it withdrew.
func failUnended(tx *store.Tx, id string) ([]string, error) {
	jobs, err := tx.Jobs(id)
	if err != nil {
		return nil, err
	}
	var withdrawn []string
	for _, j := range jobs {
		a, err := failJob(tx, id, j.Key)
		if err != nil {
			return nil, err
		}
		if a != "" {
			withdrawn = append(withdrawn, a)
		}
	}
	return withdrawn, nil
}

// failJob fails the run's job key, unless it has ended, with
// coordinator_job_failed: one whose supervisor hopper gave up on, as after
// the coordinator itself failed again and again. Its attempt that hasn't
// ended is withdrawn, so that its worker's reports are refused, and its ID
// returned, so that its placement is cancelled once the transaction
// commits (cancelPlacements). It marks the run as one a job of which
// failed, which changes its attempts' controls, whose polls the store
// has answered once the transaction commits (store.Options.OnControlsChanged).
func failJob(tx *store.Tx, run, key string) (string, error) {
	j, err := tx.Job(run, key)
	if err != nil || j.Outcome != "" {
		return "", err
	}
	var withdrawn string
	switch a, err := tx.LatestAttempt(run, key); {
	case errors.Is(err, store.ErrNotFound):
	case err != nil:
		return "", err
	case a.State != store.AttemptEnded:
		if err := tx.EndAttempt(a.ID, store.AttemptEnd{Kind: store.EndWithdrawn}); err != nil {
			return "", err
		}
		withdrawn = a.ID
	}
	if err := tx.EndJob(run, key, store.JobEnd{
		Result:      plan.JobResult{Outcome: plan.OutcomeFailure},
		Diagnostics: problems.Diagnostics{diagnostic(problems.CoordinatorJobFailed, problems.Params{"job": key})},
	}); err != nil {
		return "", err
	}
	// As any job that fails does (endJob).
	_, err = tx.MarkJobFailed(run)
	return withdrawn, err
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
