package coordinator

import (
	"context"
	"fmt"
	"io"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// actions is the coordinator's workerapisrv.Actions: it performs the
// built-in action a step asks for, which it reads from the step in its
// run's plan, never from the request.
type actions struct{ c *Coordinator }

var _ workerapisrv.Actions = actions{}

// Act performs the action the step of call's attempt uses: checkout
// (checkout), cancel-jobs (cancelJobs) or update-session (updateSession).
func (r actions) Act(ctx context.Context, call *workerapisrv.ActionCall) (workerapi.ActionAnswer, error) {
	step, err := r.c.actionStep(call)
	if err != nil {
		return workerapi.ActionAnswer{}, err
	}
	switch step.Uses {
	case plan.CheckoutUses:
		return r.c.checkout(ctx, call, step)
	case plan.CancelJobsUses:
		return r.c.cancelJobs(ctx, call, step)
	case plan.UpdateSessionUses:
		return r.c.updateSession(ctx, call, step)
	}
	return workerapi.ActionAnswer{}, fmt.Errorf("%w: %q", workerapisrv.ErrNotAction, step.Uses)
}

// updateSession performs step's parallelworks/update-session, as call asks:
// it updates the session with:'s name names, through Options.Sessions,
// which decides whether the run may, from who runs it and its
// permissions:, as format 1's platform does (compile.UpdateSession). The
// provider isn't the store's, so it asks it before Commit stores the
// answer, and asks it nothing for a step that its job's cancel or its
// early-cancel: stops, which Commit answers cancelled
// (ActionCall.Cancelled). A refusal of the provider's, as of a session the
// run may not update, and no provider fail the step; a provider that
// can't answer is an error, which the worker asks again after.
func (c *Coordinator) updateSession(ctx context.Context, call *workerapisrv.ActionCall, step *plan.Step) (workerapi.ActionAnswer, error) {
	answer := workerapi.ActionAnswer{Outcome: plan.OutcomeSuccess}
	cancelled, err := call.Cancelled(ctx, step)
	if err != nil {
		return workerapi.ActionAnswer{}, err
	}
	if !cancelled {
		u, err := c.sessionRun(ctx, call.Run)
		if err != nil {
			return workerapi.ActionAnswer{}, err
		}
		u.Read = expr.Read{Private: call.Request.Private, Marked: call.Request.Secret}
		with, _ := call.Request.With.Get().(map[string]any)
		diags, err := compile.UpdateSession(ctx, c.opts.Sessions, u, step.Path, with)
		if err != nil {
			return workerapi.ActionAnswer{}, err
		}
		answer.Diagnostics = diags
		if diags.HasErrors() {
			answer.Outcome = plan.OutcomeFailure
		}
	}
	return call.Commit(ctx, step, func(*store.Tx) (workerapi.ActionAnswer, error) { return answer, nil })
}

// sessionRun returns what update-session tells the session provider of
// run, which it authorizes the step by (compile.Sessions.Update): its ID,
// its tree's root's, and the permissions: of the root's workflow. Format 1
// gates the top workflow's alone, and a child runs with its root's token,
// so a workflow a step calls that asks for more than its root did is given
// none of it.
func (c *Coordinator) sessionRun(ctx context.Context, run *store.Run) (compile.SessionUpdate, error) {
	root := run
	if run.Parent != nil {
		if err := c.store.View(ctx, func(tx *store.Tx) error {
			var err error
			root, err = tx.Run(run.Parent.Root)
			return err
		}); err != nil {
			return compile.SessionUpdate{}, err
		}
	}
	p, err := c.plans.get(root)
	if err != nil {
		return compile.SessionUpdate{}, err
	}
	return compile.SessionUpdate{Run: run.ID, Root: root.ID, Permissions: p.Run.Permissions}, nil
}

// Tree writes the tree of the parallelworks/checkout step of call's
// attempt, as the run's plan pins it.
func (r actions) Tree(ctx context.Context, call *workerapisrv.ActionCall, w io.Writer) error {
	step, err := r.c.actionStep(call)
	if err != nil {
		return err
	}
	if step.Uses != plan.CheckoutUses {
		return fmt.Errorf("%w: %q has no tree", workerapisrv.ErrNotAction, step.Uses)
	}
	return r.c.tree(ctx, call, step, w)
}

// actionStep returns the step of call's attempt's job that call names, as
// the run's plan has it: an error that wraps workerapisrv.ErrNotAction if
// the job has no such step.
func (c *Coordinator) actionStep(call *workerapisrv.ActionCall) (*plan.Step, error) {
	_, _, step, err := c.planStep(call.Run, call.Attempt, call.Request.Step)
	if err == nil && step == nil {
		err = fmt.Errorf("%w: job %q has no step %d", workerapisrv.ErrNotAction, call.Attempt.Job, call.Request.Step)
	}
	return step, err
}

// actionFailed is the answer of an action that failed, for diags.
func actionFailed(diags ...problems.Diagnostic) workerapi.ActionAnswer {
	return workerapi.ActionAnswer{Outcome: plan.OutcomeFailure, Diagnostics: diags}
}
