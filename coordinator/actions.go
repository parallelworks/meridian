package coordinator

import (
	"context"
	"fmt"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
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

// Act performs the action the step of call's attempt uses. An action of
// the coordinator's that it doesn't perform yet fails the step, as the
// worker failed it before it asked the coordinator.
func (r actions) Act(ctx context.Context, call *workerapisrv.ActionCall) (workerapi.ActionAnswer, error) {
	step, err := r.c.actionStep(ctx, call)
	if err != nil {
		return workerapi.ActionAnswer{}, err
	}
	switch step.Uses {
	case actionCancelJobs, actionUpdateSession:
		return call.Commit(ctx, step, func(*store.Tx) (workerapi.ActionAnswer, error) {
			return actionFailed(problems.UsesNotSupported.At("", problems.Position{}, step.Path, problems.Params{"uses": step.Uses})), nil
		})
	}
	return workerapi.ActionAnswer{}, fmt.Errorf("%w: %q", workerapisrv.ErrNotAction, step.Uses)
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
