package coordinator

import (
	"context"
	"errors"
	"fmt"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// childAPI is the coordinator's workerapisrv.Children: it makes the child
// runs that attempts' steps ask for (createChild), and stops them as their
// steps do.
type childAPI struct{ c *Coordinator }

var _ workerapisrv.Children = childAPI{}

// Child gives the child run of a try of a step of the attempt a, of run,
// that req asks for, as createChild makes it: its ID, the one the try has
// already whatever changed since, or how the step's child wasn't made,
// ended, as the worker reports the step's end. What it reads of the step,
// as whether it outlives its job's cancellation, it reads from the run's
// plan, never from req.
//
//   - A step that doesn't outlive its job's cancellation, as its if: says
//     (plan.OutlivesCancel), of an attempt whose control asks to cancel its
//     job, for any reason but the run's timeout, gets none, cancelled, with
//     the control's reason, as the worker would have stopped the step had
//     it read the control first; and so does one of a run that was stopped
//     (errRunStopped), with its stop's. So, with no reason, does a step
//     whose early-cancel: is any-job-failed, once a job of the run has
//     failed (workerapi.Control.AnyJobFailed). createChild reads each of
//     them again as it stores the child (stepStoppedError).
//   - A child whose Create failed, failure, with child_not_created at the
//     step's uses:, and its own problems after it.
//   - One the coordinator refused, as child_dir_outside, failure, with
//     that problem at the step's uses:.
//
// An attempt that no longer holds its job, or whose run has ended
// (errStepGone), is store.ErrFenced, which the server answers Fenced; and
// a step whose uses: names no workflow, or a try past the step's retry:,
// is workerapisrv.ErrNotChild, which it answers 400.
func (k childAPI) Child(ctx context.Context, run *store.Run, a *store.Attempt, req *workerapi.ChildRequest) (*workerapi.Child, error) {
	c := k.c
	p, step, err := k.stepOf(run, a, req.Step, req.Try)
	if err != nil {
		return nil, err
	}
	// createChild answers with the try's child, if it has one, whatever has
	// changed since it was made, as a repeat of the ask is.
	id, diags, err := c.createChild(ctx, a, p, step, req)
	if errors.Is(err, errStepGone) {
		return nil, fmt.Errorf("%w: %w", store.ErrFenced, err)
	}
	switch reason, stopped, serr := c.stopReason(ctx, run, err); {
	case serr != nil:
		return nil, serr
	case stopped:
		return &workerapi.Child{Ended: true, Outcome: plan.OutcomeCancelled, Reason: reason}, nil
	}
	uses := step.Path + "/uses"
	if p, ok := errors.AsType[*problem.Problem](err); ok {
		if kind, known := problems.Lookup(p.Code); known {
			return &workerapi.Child{Ended: true, Outcome: plan.OutcomeFailure, Diagnostics: problems.Diagnostics{kind.At("", problems.Position{}, uses, p.Params)}}, nil
		}
	}
	switch {
	case err != nil:
		return nil, err
	case id == "":
		// The child's own problems are in its workflow's file, after the
		// step's.
		failed := problems.Diagnostics{problems.ChildNotCreated.At("", problems.Position{}, uses, nil)}
		return &workerapi.Child{Ended: true, Outcome: plan.OutcomeFailure, Diagnostics: append(failed, diags...)}, nil
	}
	return &workerapi.Child{Run: id}, nil
}

// stopReason returns why a step of run, which createChild or createStepRun
// refused with err, was stopped, and true: the reason a step's stop gives
// (stepStoppedError), or the run's stop's, or cancelled for a run stopped
// without one (errRunStopped); and false for any other err.
func (c *Coordinator) stopReason(ctx context.Context, run *store.Run, err error) (plan.Reason, bool, error) {
	if stopped, ok := errors.AsType[*stepStoppedError](err); ok {
		return stopped.reason, true, nil
	}
	if !errors.Is(err, errRunStopped) {
		return "", false, nil
	}
	reason := plan.ReasonCancelled
	err = c.store.View(ctx, func(tx *store.Tx) error {
		r, err := tx.Run(run.ID)
		if err == nil && r.StopReason != "" {
			reason = r.StopReason
		}
		return err
	})
	return reason, true, err
}

// stepOf returns run's plan, and the step at index in the job of the
// attempt a, of run, as the plan has it, which createChild takes as it is:
// an error that wraps workerapisrv.ErrNotChild if the job has no such
// step, or it calls no workflow, or try is no try of the step's: past its
// retry:, where the plan writes that out (plan.Step.LastTry), so that a
// worker can't make a child for each number.
func (k childAPI) stepOf(run *store.Run, a *store.Attempt, index, try int) (*plan.Plan, *plan.Step, error) {
	p, step, err := k.c.planStep(run, a, index)
	switch {
	case err != nil:
		return nil, nil, err
	case step == nil || step.Source == nil:
		return nil, nil, fmt.Errorf("%w: job %q's step %d calls no workflow", workerapisrv.ErrNotChild, a.Job, index)
	case try < 0:
		return nil, nil, fmt.Errorf("%w: job %q's step %d has no try %d", workerapisrv.ErrNotChild, a.Job, index, try)
	}
	if try > 0 {
		if last, known := step.LastTry(try - 1); known && last {
			return nil, nil, fmt.Errorf("%w: job %q's step %d has no try %d", workerapisrv.ErrNotChild, a.Job, index, try)
		}
	}
	return p, step, nil
}

// StopChild stops the child run id, for reason, as its step asks, as a
// person's cancel stops a run: one that has ended, or was stopped already,
// is left as it is.
func (k childAPI) StopChild(ctx context.Context, id string, reason plan.Reason) error {
	if err := k.c.stop(ctx, id, reason); err != nil && !errors.Is(err, ErrRunEnded) {
		return err
	}
	return nil
}
