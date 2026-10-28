package coordinator

import (
	"context"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// StepRun returns the step run of the try of a step, or of its cleanup:,
// of the attempt a, of run, that req asks for (createStepRun), with the
// step as the plan has it; or how it wasn't made, ended: failed, with its
// problems, as one whose ssh: the coordinator can't evaluate is, or
// cancelled, as the step's stop is, as Child answers for a child.
func (k childAPI) StepRun(ctx context.Context, run *store.Run, a *store.Attempt, req *workerapi.StepRunRequest) (*workerapi.StepRun, error) {
	c := k.c
	p, step, err := c.planStep(run, a, req.Step)
	switch {
	case err != nil:
		return nil, err
	case step == nil:
		return nil, fmt.Errorf("%w: job %q has no step %d", workerapisrv.ErrNotStepRun, a.Job, req.Step)
	}
	// The state holds the result of each step before the try's, or of
	// every step, for a cleanup's, which evaluating the step reads.
	_, j, err := c.planJob(run, a)
	if err != nil {
		return nil, err
	}
	want := req.Step
	if req.Cleanup {
		want = len(j.Steps)
	}
	if len(req.State.Steps) != want {
		return nil, fmt.Errorf("%w: a state of %d steps' results, not %d", workerapisrv.ErrNotStepRun, len(req.State.Steps), want)
	}
	state, err := stepRunStateData(req.State)
	if err != nil {
		return nil, err
	}
	id, diags, err := c.createStepRun(ctx, a, p, step, &stepRunRequest{
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
// (stepRunEnd); but one whose step wrote more than a worker reads of
// $OUTPUTS (workerapi.MaxOutputs), failed, with outputs_too_large at its
// step, and none of them, as a step of the caller's own job would. The
// server refuses such an end of a step run's attempt, so only a store
// that holds one otherwise, as a release before that check stored, has
// one.
func (k childAPI) StepRunEnd(ctx context.Context, id string) (*workerapi.StepRun, error) {
	end, err := k.c.stepRunEnd(ctx, id)
	if err != nil {
		return nil, err
	}
	if !end.Ended {
		return &workerapi.StepRun{Run: id}, nil
	}
	if size := workerapi.OutputsSize(end.Outputs); size > workerapi.MaxOutputs {
		path, err := k.c.stepRunPath(ctx, id)
		if err != nil {
			return nil, err
		}
		tooLarge := problems.OutputsTooLarge.At("", problems.Position{}, path, problems.Params{"size": size, "limit": workerapi.MaxOutputs})
		return &workerapi.StepRun{
			Run: id, Ended: true, Outcome: plan.OutcomeFailure, ExitCode: end.ExitCode,
			Diagnostics: append(slices.Clone(end.Diagnostics), tooLarge),
		}, nil
	}
	return &workerapi.StepRun{
		Run: id, Ended: true, Outcome: end.Outcome, Reason: end.Reason, ExitCode: end.ExitCode,
		Outputs: end.Outputs, Private: end.Private, Secrets: end.Secrets, Diagnostics: end.Diagnostics,
	}, nil
}

// stepRunPath returns the path, in its caller's workflow, of the step the
// step run id runs.
func (c *Coordinator) stepRunPath(ctx context.Context, id string) (string, error) {
	var run *store.Run
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		run, err = tx.Run(id)
		return err
	}); err != nil {
		return "", err
	}
	p, err := c.plans.get(run)
	if err != nil {
		return "", err
	}
	name, _, _ := strings.Cut(run.Parent.Job, "#")
	j := p.Job(name)
	if j == nil || run.Parent.Step >= len(j.Steps) {
		return "", fmt.Errorf("coordinator: step run %s's caller's plan has no step %d of job %q", id, run.Parent.Step, name)
	}
	return j.Steps[run.Parent.Step].Path, nil
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
// of the step it runs (workerapi.Claim.StepRun), from its link and its
// secret record, and rj, the job it runs, with that step alone of its
// steps whole, at its own index, and no outputs:, which the step's own job
// has. The steps before it are there by their id alone, which evaluating
// the step reads, as steps.<id>, and none after it: the step's cleanup:
// is cleared, which a step run of its own runs. A cleanup's step run has
// every step so, but that one, whose cleanup: it runs, which reads the
// job's steps and cleanups as they ended.
func (c *Coordinator) stepRunClaim(ctx context.Context, run *store.Run, rj *plan.ReadyJob) (*workerapi.StepRunClaim, *plan.ReadyJob, error) {
	rec, err := c.openRecord(ctx, run.ID)
	if err != nil {
		return nil, nil, err
	}
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
	n := l.Step + 1
	if l.Cleanup {
		n = len(rj.Steps)
	}
	steps := make([]plan.Step, n)
	for i := range steps {
		steps[i] = plan.Step{ID: rj.Steps[i].ID, Path: rj.Steps[i].Path}
	}
	steps[l.Step] = rj.Steps[l.Step]
	if !l.Cleanup {
		steps[l.Step].Cleanup = plan.Value{}
	}
	j := *rj
	j.Steps, j.Outputs = steps, nil
	return &workerapi.StepRunClaim{Step: l.Step, Try: l.Try, Cleanup: l.Cleanup, State: state}, &j, nil
}
