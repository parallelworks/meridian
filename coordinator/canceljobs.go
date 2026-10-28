package coordinator

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strconv"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/internal/hint"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// A runName is what format 1 names a run by in a cancel-jobs step's with:
// (its slug, or its workflow and number): of a Meridian run, its workflow's
// name (plan.Run.Name) and its ID, a number, and no slug, which only a
// platform gives.
type runName struct {
	workflow string
	number   int64
	slug     string
}

// otherRun says, as format 1 reads a cancel-jobs step's with:, whether it
// names another run than own, the step's, and by which key, or what is
// wrong with it: a slug that isn't own's names another run; a workflow
// names its run number, run, which format 1 requires, and another run
// unless both are own's; and a run alone names one of own's workflow,
// another unless it is own's number, or negative, which format 1 takes for
// own. Anything else names own run, as no slug, workflow or run does: null
// and "" are as if not given, as format 1 reads them, but a run of 0,
// which is a number. A slug or a workflow that isn't a string, or a run
// that isn't a whole number, is a problem, as format 1 refuses it too.
func otherRun(with map[string]any, own runName, at string) (string, *problems.Diagnostic) {
	slug, wrongSlug := withText(with["slug"], at+"/slug")
	workflow, wrongWorkflow := withText(with["workflow"], at+"/workflow")
	run, hasRun, wrongRun := runNumber(with["run"], at+"/run")
	switch wrong := cmp.Or(wrongSlug, wrongWorkflow, wrongRun); {
	case wrong != nil:
		return "", wrong
	case slug != "":
		if slug != own.slug {
			return "slug", nil
		}
	case workflow != "":
		if !hasRun || run < 0 {
			d := problems.CancelJobsRunMissing.At("", problems.Position{}, at+"/workflow", nil)
			return "", &d
		}
		if workflow != own.workflow || run != own.number {
			return "workflow", nil
		}
	case hasRun && run >= 0 && run != own.number:
		return "run", nil
	}
	return "", nil
}

// withText reads a cancel-jobs step's with: slug or workflow, v, as
// evaluated, at pointer: a string, or "" for null; or a problem for
// anything else.
func withText(v any, pointer string) (string, *problems.Diagnostic) {
	switch s := v.(type) {
	case nil:
		return "", nil
	case string:
		return s, nil
	}
	d := wrongType(pointer, v, "string")
	return "", &d
}

// runNumber reads a cancel-jobs step's with: run, v, as evaluated, at
// pointer: a whole number, and whether it is given, as one that isn't null;
// or a problem for one that isn't a whole number.
func runNumber(v any, pointer string) (int64, bool, *problems.Diagnostic) {
	switch n := v.(type) {
	case nil:
		return 0, false, nil
	case int64:
		return n, true, nil
	case float64:
		if n == float64(int64(n)) {
			return int64(n), true, nil
		}
	}
	d := wrongType(pointer, v, "integer")
	return 0, false, &d
}

// cancelJobs performs the parallelworks/cancel-jobs step of call's attempt,
// step, as docs/design.md says under Built-in actions: it cancels the jobs
// of the step's run that its jobs: names, or, without jobs:, or with an
// empty one, the run, in the one transaction that stores its answer
// (workerapisrv.ActionCall.Commit).
//
// A job it cancels is marked, and told to cancel through the control of
// its attempt that hasn't ended (store.Tx.CancelJob), as a cancel tells a
// running job; one placed and not yet claimed is claimed with that
// control. One without such an attempt ends cancelled at once, and never
// starts: one waiting on the jobs it needs, or for its first placement,
// or to be placed again. One that has ended, or was cancelled already, is
// left as it is. A name that is no job of the run, or a matrix job's,
// warns, and cancels nothing, as in format 1; a cancel of another run's
// jobs fails the step, as one Meridian doesn't make yet.
//
// A child run is named as its root is, as format 1 names a subworkflow's
// run, which is its root's, as its PW_JOB_NUMBER is (targetRun): a
// workflow and run that name the root cancel the root's jobs, or the root,
// as format 1's do from a subworkflow; one that names nothing, or a run
// alone that names the root, the child's own, as format 1's do.
//
// Of two jobs that cancel each other at once, the second step's answer is
// cancelled, and cancels nothing, since the first cancelled its job
// already: Commit answers so for a step that doesn't outlive the cancel,
// of an attempt whose control asks it.
func (c *Coordinator) cancelJobs(ctx context.Context, call *workerapisrv.ActionCall, step *plan.Step) (workerapi.ActionAnswer, error) {
	with, _ := call.Request.With.Get().(map[string]any)
	run, p, own, err := c.targetRun(ctx, call.Run, with)
	if err != nil {
		return workerapi.ActionAnswer{}, err
	}
	switch key, wrong := otherRun(with, own, step.Path+"/with"); {
	case wrong != nil:
		return call.Commit(ctx, step, func(*store.Tx) (workerapi.ActionAnswer, error) { return actionFailed(*wrong), nil })
	case key != "":
		return call.Commit(ctx, step, func(*store.Tx) (workerapi.ActionAnswer, error) {
			return actionFailed(problems.CancelJobsOtherRun.At("", problems.Position{}, step.Path+"/with/"+key, problems.Params{"key": key})), nil
		})
	}
	names, wrong := jobNames(with["jobs"], step.Path+"/with/jobs")
	if wrong != nil {
		return call.Commit(ctx, step, func(*store.Tx) (workerapi.ActionAnswer, error) { return actionFailed(*wrong), nil })
	}
	if len(names) == 0 {
		return c.cancelRun(ctx, call, step, run)
	}
	var (
		targets []string
		warned  problems.Diagnostics
	)
	cancellable := cancellableJobs(p)
	for i, name := range names {
		if !slices.Contains(cancellable, name) {
			suggestion, ok := hint.Suggest(name, cancellable)
			warned = append(warned, problems.CancelJobsJobUnknown.At("", problems.Position{}, step.Path+"/with/jobs/"+strconv.Itoa(i),
				problems.WithSuggestion(problems.Params{"job": name}, suggestion, ok)))
			continue
		}
		if !slices.Contains(targets, name) {
			targets = append(targets, name)
		}
	}
	by := "/jobs/" + call.Attempt.Job + "/steps/" + strconv.Itoa(call.Request.Step)
	if run.ID != call.Run.ID {
		// A step of a run of the tree's, which names the root's jobs.
		by = call.Run.ID + ":" + by
	}
	var woken []*waking
	answer, err := call.Commit(ctx, step, func(tx *store.Tx) (workerapi.ActionAnswer, error) {
		woken = woken[:0]
		for _, key := range targets {
			ws, err := c.cancelJobTx(ctx, tx, run.ID, key, by)
			if err != nil {
				return workerapi.ActionAnswer{}, err
			}
			woken = append(woken, ws...)
		}
		return workerapi.ActionAnswer{Outcome: plan.OutcomeSuccess, Diagnostics: warned}, nil
	})
	if err == nil {
		c.wakeAgain(context.WithoutCancel(ctx), woken...)
	}
	return answer, err
}

// targetRun returns the run whose jobs the cancel-jobs step of a step of
// own, its run, cancels, as its with:, with, names it, with its plan, and
// what format 1 names own as (runName): own, by its workflow's name and its
// ID; but for a child run, its root's, as format 1 names a subworkflow's
// run, and the root itself for a with: that names it by its workflow,
// which a subworkflow's step cancels the root's jobs with in format 1.
func (c *Coordinator) targetRun(ctx context.Context, own *store.Run, with map[string]any) (*store.Run, *plan.Plan, runName, error) {
	p, err := c.plans.get(own)
	if err != nil {
		return nil, nil, runName{}, err
	}
	if own.Parent == nil {
		name := runName{workflow: p.Run.Name}
		name.number, _ = strconv.ParseInt(own.ID, 10, 64)
		return own, p, name, nil
	}
	var root *store.Run
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		root, err = tx.Run(own.Parent.Root)
		return err
	}); err != nil {
		return nil, nil, runName{}, err
	}
	rootPlan, err := c.plans.get(root)
	if err != nil {
		return nil, nil, runName{}, err
	}
	name := runName{workflow: rootPlan.Run.Name}
	name.number, _ = strconv.ParseInt(root.ID, 10, 64)
	if workflow, _ := with["workflow"].(string); workflow != "" {
		return root, rootPlan, name, nil
	}
	return own, p, name, nil
}

// cancelRun cancels the run run, the run of call's attempt or its root, as
// a cancel-jobs step without jobs: does, as a person cancels it (Cancel),
// in the transaction that stores the step's answer, which is success,
// whether it stopped the run or the run was stopped already.
func (c *Coordinator) cancelRun(ctx context.Context, call *workerapisrv.ActionCall, step *plan.Step, run *store.Run) (workerapi.ActionAnswer, error) {
	paths, err := c.cancelPaths(ctx, run)
	if err != nil {
		return workerapi.ActionAnswer{}, err
	}
	var (
		stopped bool
		woken   []*waking
	)
	answer, err := call.Commit(ctx, step, func(tx *store.Tx) (workerapi.ActionAnswer, error) {
		var err error
		if stopped, woken, err = c.stopTx(ctx, tx, run.ID, plan.ReasonCancelled, paths); err != nil {
			return workerapi.ActionAnswer{}, err
		}
		return workerapi.ActionAnswer{Outcome: plan.OutcomeSuccess}, nil
	})
	if err == nil && stopped {
		stoppedRun := *run
		c.stopped(context.WithoutCancel(ctx), &stoppedRun, plan.ReasonCancelled, paths, woken)
	}
	return answer, err
}

// cancelJobTx cancels, in tx, the run's job key, for the cancel-jobs step
// by (store.Tx.CancelJob): a job without an attempt that hasn't ended, and
// none whose worker's end is the job's (betweenAttempts), it ends
// cancelled at once, waking what waits for it to end. It returns the
// supervisors and markers it woke that hopper was running (waking).
func (c *Coordinator) cancelJobTx(ctx context.Context, tx *store.Tx, run, key, by string) ([]*waking, error) {
	marked, j, err := tx.CancelJob(run, key, by)
	if err != nil || !marked {
		return nil, err
	}
	a, err := tx.LatestAttempt(run, key)
	switch {
	case errors.Is(err, store.ErrNotFound):
		a = nil
	case err != nil:
		return nil, err
	}
	if !betweenAttempts(a) {
		// Its worker, told, ends it, or its supervisor does, as its
		// attempt ended; and the children of its steps the cancel stops
		// stop now, as its worker would stop them.
		if a == nil || a.State == store.AttemptEnded {
			return nil, nil
		}
		r, err := tx.Run(run)
		if err != nil {
			return nil, err
		}
		return c.stopStepChildrenTx(ctx, tx, r, a, plan.ReasonCancelled)
	}
	var woken []*waking
	if !j.ReadyAt.IsZero() {
		// Its supervisor waits for nothing it needs, but its next look:
		// woken, it finds the job ended. One that waits for the jobs its
		// job needs finds it so once they have ended.
		w, err := c.wakeJobTx(ctx, tx, run, key)
		if err != nil {
			return nil, err
		}
		woken = append(woken, w)
	}
	if err := tx.EndJob(run, key, cancelledEnd); err != nil {
		return nil, err
	}
	w, err := c.wakeAnyTx(ctx, tx, run, j.Name)
	return append(woken, w), err
}

// cancelledEnd is how a job a cancel-jobs step cancelled ends, as no
// worker ended it.
var cancelledEnd = store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeCancelled}, Reason: plan.ReasonCancelled}

// cancellableJobs returns the names of the jobs of p that a cancel-jobs
// step can cancel: every job without a matrix, whose instances format 1
// names otherwise.
func cancellableJobs(p *plan.Plan) []string {
	var out []string
	for _, j := range p.Jobs {
		if j.Strategy == nil {
			out = append(out, j.Name)
		}
	}
	return out
}

// jobNames returns the names a cancel-jobs step's jobs:, v, as evaluated,
// lists, at pointer: none for no jobs:, null, or an empty list; or a
// problem for one that isn't a list of strings.
func jobNames(v any, pointer string) ([]string, *problems.Diagnostic) {
	if v == nil {
		return nil, nil
	}
	list, ok := v.([]any)
	if !ok {
		d := wrongType(pointer, v, "array")
		return nil, &d
	}
	names := make([]string, 0, len(list))
	for i, item := range list {
		name, ok := item.(string)
		if !ok {
			d := wrongType(pointer+"/"+strconv.Itoa(i), item, "string")
			return nil, &d
		}
		names = append(names, name)
	}
	return names, nil
}

// wrongType is the problem of a value, v, at pointer, of a type other than
// want, by its name in a schema.
func wrongType(pointer string, v any, want string) problems.Diagnostic {
	return problems.WrongType.At("", problems.Position{}, pointer, problems.WrongTypeParams(plan.TypeName(v), want))
}
