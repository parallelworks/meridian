package coordinator

import (
	"context"
	"fmt"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// The built-in actions the coordinator performs, as their steps' uses:
// names them, which a worker asks it for (workerapi.PathAction).
const (
	actionCancelJobs    = "parallelworks/cancel-jobs"
	actionUpdateSession = "parallelworks/update-session"
)

// actions is the coordinator's workerapisrv.Actions: it performs the
// built-in action a step asks for, which it reads from the step in its
// run's plan, never from the request.
type actions struct{ c *Coordinator }

var _ workerapisrv.Actions = actions{}

// Act performs the action the step of call's attempt uses: cancel-jobs
// (cancelJobs) or update-session (updateSession).
func (r actions) Act(ctx context.Context, call *workerapisrv.ActionCall) (workerapi.ActionAnswer, error) {
	step, err := r.c.actionStep(ctx, call)
	if err != nil {
		return workerapi.ActionAnswer{}, err
	}
	switch step.Uses {
	case actionCancelJobs:
		return r.c.cancelJobs(ctx, call, step)
	case actionUpdateSession:
		return r.c.updateSession(ctx, call, step)
	}
	return workerapi.ActionAnswer{}, fmt.Errorf("%w: %q", workerapisrv.ErrNotAction, step.Uses)
}

// updateSession performs step's parallelworks/update-session, as call asks:
// it updates one of the run's own sessions, which with:'s name names,
// through Options.Sessions (compile.UpdateSession). The provider isn't the
// store's, so it asks it before Commit stores the answer, and asks it
// nothing for a step that its job's cancel or its early-cancel: stops,
// which Commit answers cancelled (ActionCall.Cancelled). A refusal of the
// provider's, a session that isn't the run's, and no provider fail the
// step; a provider that can't answer is an error, which the worker asks
// again after.
func (c *Coordinator) updateSession(ctx context.Context, call *workerapisrv.ActionCall, step *plan.Step) (workerapi.ActionAnswer, error) {
	answer := workerapi.ActionAnswer{Outcome: plan.OutcomeSuccess}
	cancelled, err := call.Cancelled(ctx, step)
	if err != nil {
		return workerapi.ActionAnswer{}, err
	}
	if !cancelled {
		p, err := c.plans.get(call.Run)
		if err != nil {
			return workerapi.ActionAnswer{}, err
		}
		with, _ := call.Request.With.Get().(map[string]any)
		read := expr.Read{Private: call.Request.Private, Marked: call.Request.Secret}
		diags, err := compile.UpdateSession(ctx, c.opts.Sessions, call.Run.ID, p, step.Path, with, read)
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

// actionStep returns the step of call's attempt's job that call names, as
// the run's plan has it: an error that wraps workerapisrv.ErrNotAction if
// the job has no such step.
func (c *Coordinator) actionStep(ctx context.Context, call *workerapisrv.ActionCall) (*plan.Step, error) {
	p, err := c.plans.get(call.Run)
	if err != nil {
		return nil, err
	}
	var job *store.Job
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		job, err = tx.Job(call.Run.ID, call.Attempt.Job)
		return err
	}); err != nil {
		return nil, err
	}
	pj := p.Job(job.Name)
	if pj == nil {
		return nil, fmt.Errorf("coordinator: run %s's plan has no job %q", call.Run.ID, job.Name)
	}
	if i := call.Request.Step; i >= len(pj.Steps) {
		return nil, fmt.Errorf("%w: job %q has no step %d", workerapisrv.ErrNotAction, job.Name, i)
	}
	return &pj.Steps[call.Request.Step], nil
}

// actionFailed is the answer of an action that failed, for diags.
func actionFailed(diags ...problems.Diagnostic) workerapi.ActionAnswer {
	return workerapi.ActionAnswer{Outcome: plan.OutcomeFailure, Diagnostics: diags}
}
