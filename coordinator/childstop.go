package coordinator

import (
	"context"
	"errors"
	"strings"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
)

// A child run is stopped with the step that called it, as docs/design.md
// says under Workflows a step calls: its step's worker stops it as the
// step is stopped, and waits for its end; and the coordinator stops it too,
// in the transaction that ends or stops what the step's try was part of,
// so that no child outlives what waits for it, whatever its worker does:
//
//   - An attempt that ends, whatever ends it, as one lost, withdrawn or
//     superseded, without its worker, stops the children of its steps
//     that haven't ended, cancelled (attemptsEnded): nothing waits for
//     them any more. Its job's next attempt runs its steps again, with
//     children of their own.
//   - A stopped run, or a job of it its timeout:, or a cancel-jobs step,
//     stops, stops the children of the steps of the attempts it tells to
//     stop that don't outlive that, as their if: says, for the same
//     reason (stopStepChildrenTx), as their worker would.
//
// A child stopped so stops its own jobs as any run stopped does, and its
// steps their children in turn. Its root waits for it to end before it
// ends itself.

// attemptsEnded stops, in tx, the children of the steps' tries of the
// attempts ids, which tx ended, that haven't ended (stopChildTx), as the
// store asks once it ends them (store.Options.OnAttemptsEnded), and wakes
// again, once tx commits, the supervisors it woke that hopper was running
// (wakeAgain).
func (c *Coordinator) attemptsEnded(ctx context.Context, tx *store.Tx, ids []string) error {
	children, err := tx.LiveChildren(ids...)
	if err != nil {
		return err
	}
	var woken []*waking
	for _, child := range children {
		ws, err := c.stopChildTx(ctx, tx, child.ID, plan.ReasonCancelled)
		if unreadable(err) {
			// The attempt ends all the same: the child runs on, until its
			// own jobs end, as a release that reads it, or its root's
			// finisher, sees to.
			c.log.WarnContext(ctx, "coordinator: a child of an attempt that ended couldn't be read, so it isn't stopped", "run", child.ID, "attempt", child.Attempt, "error", err)
			continue
		}
		if err != nil {
			return err
		}
		woken = append(woken, ws...)
	}
	c.wakeAgainAfter(tx, woken)
	return nil
}

// unreadable reports whether err says a run couldn't be read, by this
// release, as one whose plan only a newer release reads, or at all, as one
// the store holds damaged, rather than that the store's transaction
// failed: what can't be read of a child doesn't keep its caller's attempt
// from ending.
func unreadable(err error) bool {
	return errors.Is(err, plan.ErrVersion) || errors.Is(err, store.ErrMalformed)
}

// wakeAgainAfter wakes again the supervisors woken, once tx commits, as
// wakeAgain does, if there are any.
func (c *Coordinator) wakeAgainAfter(tx *store.Tx, woken []*waking) {
	if len(woken) == 0 {
		return
	}
	if c.rewake != nil {
		c.rewake(woken)
	}
	tx.AfterCommit(func(ctx context.Context) { c.wakeAgain(ctx, woken...) })
}

// stopStepChildrenTx stops, in tx, for reason, the children of the steps'
// tries of the attempt a, of run, that a stop of its job stops, as its
// worker would: those whose step doesn't outlive its job's cancellation,
// as its if: says (plan.OutlivesCancel). Those whose step does run on. It
// returns the supervisors it woke that hopper was running (waking).
func (c *Coordinator) stopStepChildrenTx(ctx context.Context, tx *store.Tx, run *store.Run, a *store.Attempt, reason plan.Reason) ([]*waking, error) {
	children, err := tx.LiveChildren(a.ID)
	if err != nil || len(children) == 0 {
		return nil, err
	}
	p, err := c.plans.get(run)
	if unreadable(err) {
		// Its steps' workers stop their children, as their steps stop.
		c.log.WarnContext(ctx, "coordinator: the plan of a run that stops couldn't be read, so its steps' workers stop their children", "run", run.ID, "error", err)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	name, _, _ := strings.Cut(a.Job, "#")
	job := p.Job(name)
	var woken []*waking
	for _, child := range children {
		if job != nil && child.Step < len(job.Steps) && plan.OutlivesCancel(job.Steps[child.Step].If) {
			continue
		}
		ws, err := c.stopChildTx(ctx, tx, child.ID, reason)
		if unreadable(err) {
			c.log.WarnContext(ctx, "coordinator: a child of a step that stops couldn't be read, so its step's worker stops it", "run", child.ID, "error", err)
			continue
		}
		if err != nil {
			return nil, err
		}
		woken = append(woken, ws...)
	}
	return woken, nil
}

// stopChildTx stops, in tx, the child run id, for reason, as a person's
// cancel stops a run: one that hasn't started ends at once, and wakes its
// root's finisher, if it waits; one that runs has its stop stored, and its
// readied jobs' supervisors woken, each of which sees the stop through for
// its job (stoppedAttempt). One that has ended, or was stopped already, is
// left as it is. It returns the supervisors, or the root's finisher, it
// woke that hopper was running (waking).
func (c *Coordinator) stopChildTx(ctx context.Context, tx *store.Tx, id string, reason plan.Reason) ([]*waking, error) {
	r, err := tx.Run(id)
	switch {
	case err != nil:
		return nil, err
	case r.State == store.RunEnded || !r.StoppedAt.IsZero():
		return nil, nil
	case r.State == store.RunCreated:
		if _, err := tx.StopRun(id, reason); err != nil {
			return nil, err
		}
		if err := tx.EndRun(id, plan.OutcomeCancelled, reason); err != nil {
			return nil, err
		}
		if r.Parent == nil {
			return nil, nil
		}
		w, err := c.wakeRootTx(ctx, tx, r.Parent.Root, id)
		return []*waking{w}, err
	}
	// Each job's supervisor decides whether its job outlives the stop.
	_, woken, err := c.stopTx(ctx, tx, id, reason, &cancelPaths{outlives: map[string]bool{}})
	return woken, err
}
