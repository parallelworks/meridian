package coordinator

import (
	"bytes"
	"cmp"
	"context"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// A stepRunRequest is a step's try's ask for a step run, a run of its own
// that runs the step on another host than its job's (docs/design.md, Steps
// on another host), as the step's worker evaluated the try: the step's
// index in its job, and the try, from 0; where the step's ssh: put the
// try, the host it names, or nil for one that is null, or gives no
// remoteHost, which the coordinator checks against its own evaluation of
// it; the state of the job at the try, a mapping, which the step run's
// worker runs the step with, and which the coordinator keeps, sealed, but
// doesn't read; and the strings the step's worker masks at the try, which
// the step run's worker masks too.
type stepRunRequest struct {
	Step, Try int
	SSH       *plan.Inline
	State     plan.Data
	Masked    []string
}

// errNotStepRun is what createStepRun returns for a step that can't run as
// a step run, as the plan has it (plan.Job.StepElsewhere), or a host that
// isn't one an ssh: names.
var errNotStepRun = errors.New("coordinator: the step can't run as a run of its own")

// createStepRun creates the step run of the try of a step, of the attempt
// a, that req asks for, and returns its ID: the next number in the store,
// or the ID of the run that the try has already, which a repeated ask
// finds again; or, for a step whose ssh: the coordinator can't evaluate,
// the problems that say so. step is the step at req.Step of a's job, and p
// the plan of a's run. As docs/design.md says under Steps on another host:
//
//   - Only a step that the plan says can run so gets one
//     (plan.Job.StepElsewhere), for a try of its retry:, and never a step
//     run's own step, nor one whose ssh: names no host in a job without
//     ssh:, which runs in its job.
//   - Where it runs is where the step's ssh: puts the try as the
//     coordinator evaluates it, from the plan, with what Ready readies the
//     caller's job with (plan.Plan.StepTarget), never what the caller's
//     worker says: a request whose host isn't that one is refused, so that
//     a host never chooses where the coordinator connects with the user's
//     keys. Whether it is private is the coordinator's evaluation's too.
//   - It has no plan of its own: its plan is its caller's, which the store
//     reads from the caller's run, and its one job is the caller's, the
//     step's, which Ready readies as the caller's, but for where it runs
//     (readyStepRun).
//   - Its directory is its caller's run's, so a member that chooses a
//     run's directory itself, as a host does, places it in the caller's
//     place in its root's there, where the caller's run's jobs on that
//     member run.
//   - Its secret record holds where it runs, the job's state at the try
//     and what the caller's worker masks, which the plan doesn't, and what
//     the caller's run inherited, which its jobs mask too.
//   - One transaction stores it, linked to the step's try, with its Start
//     in the queue every coordinator works, as a workflow a step calls is.
//
// It returns the errors createChild returns for an attempt that no longer
// holds its job, a run that has ended or was stopped, and a step its
// attempt's control stops; and errNotStepRun for a step, a try or a host
// that can't have one.
func (c *Coordinator) createStepRun(ctx context.Context, a *store.Attempt, p *plan.Plan, step *plan.Step, req *stepRunRequest) (string, problems.Diagnostics, error) {
	if err := c.writable(); err != nil {
		return "", nil, err
	}
	if req.Try < 0 {
		return "", nil, fmt.Errorf("%w: try %d", errNotStepRun, req.Try)
	}
	if req.Try > 0 {
		if last, known := step.LastTry(req.Try - 1); known && last {
			return "", nil, fmt.Errorf("%w: the step has no try %d", errNotStepRun, req.Try)
		}
	}
	if _, ok := req.State.Get().(map[string]any); !ok && !req.State.IsZero() {
		return "", nil, fmt.Errorf("%w: the job's state isn't a mapping", errNotStepRun)
	}
	cl, existing, err := c.readCaller(ctx, a.ID, req.Step, req.Try)
	switch {
	case err != nil:
		return "", nil, err
	case existing != "":
		return existing, nil, nil
	case cl.run.StepRun:
		return "", nil, fmt.Errorf("%w: step run %s's own step", errNotStepRun, cl.run.ID)
	}
	if err := stepLives(cl.attempt, cl.run, step, cl.control); err != nil {
		return "", nil, err
	}
	name, _, _ := strings.Cut(cl.attempt.Job, "#")
	j := p.Job(name)
	if j == nil || !j.StepElsewhere(step) {
		return "", nil, fmt.Errorf("%w: run %s's job %q's step %d", errNotStepRun, cl.run.ID, name, req.Step)
	}
	if err := c.triedBefore(ctx, a.ID, req.Step, req.Try); err != nil {
		return "", nil, err
	}
	target, onHost, private, diags, err := c.stepTarget(ctx, cl.run, cl.attempt.Job, req.Step, req.Try)
	switch {
	case err != nil:
		return "", nil, err
	case diags.HasErrors():
		return "", diags, nil
	case target == nil && !onHost:
		return "", nil, fmt.Errorf("%w: a step whose ssh: names no host runs in its job, which has none", errNotStepRun)
	case !sameTarget(req.SSH, target):
		return "", nil, fmt.Errorf("%w: the host its worker gave isn't the one its ssh: names", errNotStepRun)
	}
	callers, err := c.openRecord(ctx, cl.run.ID)
	if err != nil {
		return "", nil, err
	}
	depth := 1
	if cl.run.Parent != nil {
		depth = cl.run.Parent.Depth + 1
	}
	link := store.Link{Run: cl.run.ID, Job: cl.attempt.Job, Attempt: cl.attempt.ID, Step: req.Step, Try: req.Try, Root: cl.root.ID, Depth: depth}
	record := plan.Record{
		Version: plan.Version, PlanDigest: callers.PlanDigest, Inherited: callers.Inherited,
		StepRun: &plan.StepRun{SSH: target, Private: private, State: req.State, Masked: req.Masked},
	}
	id, err := c.storeChild(ctx, step, &link, func(tx *store.Tx, id string) error {
		return c.insertStepRun(ctx, tx, id, p, &record, cl.run, &link, name)
	})
	return id, nil, err
}

// triedBefore checks that the try try of the step step, of the attempt
// attempt, follows the one before it, which had a step run that has
// ended, and not in success, as tries come one after another: so that a
// worker can't choose a try, and with it a host, where the step's ssh:
// reads its retry counter, whatever its retry: says.
func (c *Coordinator) triedBefore(ctx context.Context, attempt string, step, try int) error {
	if try == 0 {
		return nil
	}
	return c.store.View(ctx, func(tx *store.Tx) error {
		id, err := tx.Child(attempt, step, try-1)
		if errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("%w: try %d without a step run of try %d", errNotStepRun, try, try-1)
		}
		if err != nil {
			return err
		}
		r, err := tx.Run(id)
		switch {
		case err != nil:
			return err
		case r.State != store.RunEnded || r.Outcome == plan.OutcomeSuccess:
			return fmt.Errorf("%w: try %d while the step run of try %d hasn't failed", errNotStepRun, try, try-1)
		}
		return nil
	})
}

// stepTarget evaluates, as the coordinator, where the ssh: of the step at
// index of the job key, of run, puts its try try: with what Ready readies
// the job with, as a claim of it reads it (readyInputOf). It reports
// whether the job, as Ready readies it, runs on a host, too.
func (c *Coordinator) stepTarget(ctx context.Context, run *store.Run, key string, index, try int) (*plan.Inline, bool, bool, problems.Diagnostics, error) {
	p, from, in, err := c.readyInputOf(ctx, run, key)
	if err != nil {
		return nil, false, false, nil, err
	}
	res, _, err := p.Ready(ctx, from.job.Name, in)
	if err != nil {
		return nil, false, false, nil, err
	}
	onHost := res != nil && res.Job != nil && res.Job.Target.Inline != nil
	target, private, diags, err := p.StepTarget(ctx, from.job.Name, index, try, in)
	return target, onHost, private, diags, err
}

// sameTarget reports whether a, the host a step's worker says its ssh:
// gave the try, is b, the coordinator's: both none, or the same ssh:, as
// their encodings, which leave out how each decoded its numbers.
func sameTarget(a, b *plan.Inline) bool {
	if a == nil || b == nil {
		return a == b
	}
	ea, err1 := jsonv2.Marshal(a, jsonv2.Deterministic(true))
	eb, err2 := jsonv2.Marshal(b, jsonv2.Deterministic(true))
	return err1 == nil && err2 == nil && bytes.Equal(ea, eb)
}

// insertStepRun stores, in tx, the step run id, of the job named name,
// whose plan is p, its caller's, and whose secret record is record, as
// link links it to the step's try of the run caller: with a hopper
// workflow of its Start, its job's supervisor and its finisher, as
// insertRun stores a run, in the caller's run's directory and default
// group.
func (c *Coordinator) insertStepRun(ctx context.Context, tx *store.Tx, id string, p *plan.Plan, record *plan.Record, caller *store.Run, link *store.Link, name string) error {
	sealed, gen, err := c.sealer.Seal(ctx, id, record)
	if err != nil {
		return err
	}
	insert := func(after ...hopper.Step) *hopper.StepOpts {
		return &hopper.StepOpts{
			After: after, OnDependencyFailure: hopper.DependencyIgnore,
			Insert: &hopper.InsertOpts{Queue: sharedQueue, MaxAttempts: c.retry.attempts},
		}
	}
	wf := hopper.NewWorkflow("meridian run "+id, nil)
	start := wf.Add(startArgs{Run: id}, &hopper.StepOpts{Insert: &hopper.InsertOpts{Queue: sharedQueue, MaxAttempts: c.retry.attempts}})
	supervisor := wf.Add(jobArgs{Run: id, Key: name}, insert(start))
	wf.Add(finishArgs{Run: id}, &hopper.StepOpts{
		After: []hopper.Step{start, supervisor}, OnDependencyFailure: hopper.DependencyIgnore,
		Insert: &hopper.InsertOpts{Queue: sharedQueue, Await: true},
	})
	res, err := c.inserter.InsertWorkflowTx(ctx, tx.SQL(), wf)
	if err != nil {
		return err
	}
	if err := tx.InsertRun(store.NewRun{
		ID: id, Plan: p, StepRun: true, DefaultGroup: caller.DefaultGroup, RunDir: caller.RunDir,
		WorkflowID: res.ID, FinisherID: res.Jobs[2].Job.ID, Parent: link,
	}); err != nil {
		return err
	}
	if err := tx.InsertJob(store.NewJob{Run: id, Name: name, Key: name, SupervisorID: res.Jobs[1].Job.ID}); err != nil {
		return err
	}
	return tx.InsertRecord(id, gen, sealed.Key, sealed.Record)
}

// startStepRun starts the step run run, which Start doesn't compile: its
// plan is its caller's, which its caller's Start started, and it starts
// with what that Start gave, as its job is its caller's.
func (c *Coordinator) startStepRun(ctx context.Context, run *store.Run, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	return 0, c.store.Tx(ctx, func(tx *store.Tx) error {
		r, err := tx.Run(run.ID)
		if err != nil || r.State != store.RunCreated {
			return cmp.Or(err, errSettled)
		}
		caller, err := tx.Run(r.Parent.Run)
		if err != nil {
			return err
		}
		if caller.StartState == nil {
			return fmt.Errorf("coordinator: step run %s's caller, run %s, hasn't started", run.ID, caller.ID)
		}
		if err := tx.StartRun(run.ID, caller.StartState, nil); err != nil {
			return err
		}
		if done != nil {
			return done(ctx, tx)
		}
		return nil
	})
}

// readyStepRun readies the job of the step run run, as readyJob does, or
// readyEarly when early is set: its caller's job, as Ready readies it, in
// its caller's run, with that run's values and the results of the jobs it
// needs; but where the step's ssh: put the try (stepRunJob).
func (c *Coordinator) readyStepRun(ctx context.Context, run *store.Run, early bool) (*readied, error) {
	caller, err := c.callerOf(ctx, run)
	if err != nil {
		return nil, err
	}
	r, err := c.ready(ctx, caller, run.Parent.Job, early)
	if err != nil || r.result == nil || r.result.Job == nil {
		return r, err
	}
	if r.result.Job, err = c.stepRunJob(ctx, run, r.result.Job); err != nil {
		return nil, err
	}
	return r, nil
}

// callerOf returns the run whose step's try the step run run runs.
func (c *Coordinator) callerOf(ctx context.Context, run *store.Run) (*store.Run, error) {
	var caller *store.Run
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		caller, err = tx.Run(run.Parent.Run)
		return err
	})
	return caller, err
}

// stepRunJob returns rj, the job of the step run run as its caller's
// readies, as the step run runs it: where its secret record says the
// step's ssh: put the try, on the host it names, or, for one that names
// none, where the job would run without its ssh:, which for a job on a
// host is the run's default group; and private where evaluating the ssh:
// read a variable or a password.
func (c *Coordinator) stepRunJob(ctx context.Context, run *store.Run, rj *plan.ReadyJob) (*plan.ReadyJob, error) {
	rec, err := c.openRecord(ctx, run.ID)
	if err != nil {
		return nil, err
	}
	sr := rec.StepRun
	if sr == nil {
		return nil, fmt.Errorf("%w: step run %s's secret record says nothing of its step", plan.ErrMalformed, run.ID)
	}
	j := *rj
	switch {
	case sr.SSH != nil:
		j.Target = plan.Target{Inline: sr.SSH}
	case j.Target.Inline != nil:
		j.Target = plan.Target{}
	}
	if sr.Private && !slices.Contains(j.PrivateFields, "target") {
		j.PrivateFields = append(slices.Clone(j.PrivateFields), "target")
	}
	return &j, nil
}
