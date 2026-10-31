package coordinator

import (
	"context"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// StepRun returns the step run of the try of a step, or of its cleanup:,
// of the attempt a, of run, that req asks for (createStepRun), with the
// step as the plan has it; or how it wasn't made, ended: failed, with its
// problems, as one whose ssh: the coordinator can't evaluate is, or
// cancelled, as the step's stop is, as Child answers for a child.
func (k childAPI) StepRun(ctx context.Context, run *store.Run, a *store.Attempt, req *workerapi.StepRunRequest) (*workerapi.StepRun, error) {
	c := k.c
	p, j, step, err := c.planStep(run, a, req.Step)
	switch {
	case err != nil:
		return nil, err
	case step == nil:
		return nil, fmt.Errorf("%w: job %q has no step %d", workerapisrv.ErrNotStepRun, a.Job, req.Step)
	}
	// The state holds the result of each step before the try's, or of
	// every step, for a cleanup's, which evaluating the step reads.
	want := req.Step
	if req.Cleanup {
		want = len(j.Steps)
	}
	if len(req.State.Steps) != want {
		return nil, fmt.Errorf("%w: a state of %d steps' results, not %d", workerapisrv.ErrNotStepRun, len(req.State.Steps), want)
	}
	st, err := c.stepRunState(ctx, a, j, req)
	if err != nil {
		return nil, err
	}
	state, err := stepRunStateData(st)
	if err != nil {
		return nil, err
	}
	id, diags, err := c.createStepRun(ctx, a, p, j, step, &stepRunRequest{
		Step: req.Step, Try: req.Try, Cleanup: req.Cleanup, SSH: req.SSH, State: state, Masked: req.Masked,
	})
	if errors.Is(err, errNotStepRun) {
		return nil, fmt.Errorf("%w: %w", workerapisrv.ErrNotStepRun, err)
	}
	if errors.Is(err, errStepGone) {
		return nil, fmt.Errorf("%w: %w", store.ErrFenced, err)
	}
	switch reason, stopped, serr := c.stopReason(ctx, run, err); {
	case serr != nil:
		return nil, serr
	case stopped:
		return &workerapi.StepRun{Ended: true, Outcome: plan.OutcomeCancelled, Reason: reason, ExitCode: -1}, nil
	}
	switch {
	case err != nil:
		return nil, err
	case diags.HasErrors():
		return &workerapi.StepRun{Ended: true, Outcome: plan.OutcomeFailure, ExitCode: -1, Diagnostics: diags}, nil
	}
	return &workerapi.StepRun{Run: id}, nil
}

// StepRunEnd returns how the step run id, which has ended, ended
// (stepRunEnd).
func (k childAPI) StepRunEnd(ctx context.Context, id string) (*workerapi.StepRun, error) {
	return k.c.stepRunEnd(ctx, id)
}

// stepRunState returns the state a step run of the attempt a's job j is
// given, of what req asks: of the outputs of the job's steps, only those
// its step, or its cleanup:, and the job's env: read
// (plan.Job.StepRunReads), as the plan says, never the worker, since an
// output can hold what was made from a secret, which the state doesn't
// mark; and the job's retry counter and attempt as the caller's claim
// gives them, whatever req says.
func (c *Coordinator) stepRunState(ctx context.Context, a *store.Attempt, j *plan.Job, req *workerapi.StepRunRequest) (workerapi.StepRunState, error) {
	st := req.State
	st.Steps = workerapi.StepRunResults(j.StepRunReads(req.Step, req.Cleanup, st.PlanSteps()))
	st.JobAttempt = a.Number
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		st.JobRetry, err = tx.StartedBefore(a)
		return err
	})
	return st, err
}

// stepRunStateData returns state, as the worker API gives it, as the
// mapping a step run's secret record keeps (plan.StepRun.State).
func stepRunStateData(state workerapi.StepRunState) (plan.Data, error) {
	encoded, err := workerapi.Marshal(state)
	if err != nil {
		return plan.Data{}, err
	}
	var m map[string]any
	if err := jsonv2.Unmarshal(encoded, &m); err != nil {
		return plan.Data{}, err
	}
	return plan.DataOf(m), nil
}

// stepRunClaim returns what an attempt of the step run run has of the try
// of the step it runs (workerapi.Claim.StepRun), from its link and rec, its
// secret record, and rj, the job it runs, with that step alone of its
// steps whole, at its own index, and no outputs:, which the step's own job
// has. The steps before it are there by their id alone, which evaluating
// the step reads, as steps.<id>, and none after it: the step's cleanup:
// is cleared, which a step run of its own runs. A cleanup's step run has
// every step so, but that one, whose cleanup: it runs, which reads the
// job's steps and cleanups as they ended.
func stepRunClaim(run *store.Run, rec *plan.Record, rj *plan.ReadyJob) (*workerapi.StepRunClaim, *plan.ReadyJob, error) {
	if rec.StepRun == nil {
		return nil, nil, fmt.Errorf("%w: step run %s's secret record says nothing of its step", plan.ErrMalformed, run.ID)
	}
	var state workerapi.StepRunState
	if !rec.StepRun.State.IsZero() {
		encoded, err := jsonv2.Marshal(rec.StepRun.State)
		if err != nil {
			return nil, nil, err
		}
		if err := workerapi.Unmarshal(encoded, &state); err != nil {
			return nil, nil, fmt.Errorf("%w: step run %s's state: %w", plan.ErrMalformed, run.ID, err)
		}
	}
	l := run.Parent
	if l.Step >= len(rj.Steps) {
		return nil, nil, fmt.Errorf("coordinator: step run %s's job has no step %d", run.ID, l.Step)
	}
	j := *rj
	j.Steps, j.Outputs = plan.StepRunSteps(rj.Steps, l.Step, l.Cleanup), nil
	return &workerapi.StepRunClaim{Step: l.Step, Try: l.Try, Cleanup: l.Cleanup, State: state}, &j, nil
}
