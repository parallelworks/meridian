package coordinator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// A ChildRequest is a try of a step whose uses: names a workflow, which
// asks for the child run that runs it (docs/design.md, Workflows a step
// calls): what the worker that runs the step gives of it, as Step gave it
// (plan.ReadyStep).
type ChildRequest struct {
	// Attempt is the ID of the attempt that runs the step, Step the step's
	// index in its job, and Try the try, from 0.
	Attempt   string
	Step, Try int
	// With is the step's with:, and Env its environment: Step's, with the
	// variables the worker defines for the step that format 1 defines
	// (plan.IsFormatVariable) over it.
	With map[string]any
	Env  map[string]string
	// WithPrivate, WithSecrets, EnvPrivate and EnvSecrets name what of With
	// and Env is private, as plan.ReadyStep's do, and Masked are the
	// strings the worker masks for the step.
	WithPrivate, WithSecrets, EnvPrivate, EnvSecrets []string
	Masked                                           []string
	// WorkingDirectory is the step's working directory, as Step gave it
	// (plan.ReadyStep.WorkingDirectory): "" for its run's directory, or a
	// path relative to it, or absolute.
	WorkingDirectory string
}

// ErrStepGone is what CreateChild returns for a step's try that can't have
// a child: its attempt no longer holds its job, as one that has ended, or
// that another superseded, or whose job has ended; or its run has ended.
var ErrStepGone = errors.New("coordinator: the step's attempt no longer holds its job, or its run has ended")

// ErrRunStopped is what CreateChild returns for a step of a run that was
// stopped, as by a cancel or its timeout:, that doesn't outlive that, as
// its if: says (plan.OutlivesCancel).
var ErrRunStopped = errors.New("coordinator: the step's run was stopped")

// A StepStoppedError is what CreateChild returns for a step that its
// attempt's control stops, as its worker would have stopped it had it read
// the control first: one that doesn't outlive its job's cancellation, as
// its if: says (plan.OutlivesCancel), of a job the control cancels, for
// Reason, any but the run's timeout; or, with no Reason, one whose
// early-cancel: is any-job-failed, once a job of the run has failed
// (workerapi.Control.AnyJobFailed).
type StepStoppedError struct{ Reason plan.Reason }

func (e *StepStoppedError) Error() string {
	if e.Reason == "" {
		return "coordinator: the step's early-cancel: stops it, as a job of its run failed"
	}
	return "coordinator: the step's job is being cancelled, " + string(e.Reason)
}

// CreateChild creates the child run of a try of a step whose uses: names a
// workflow, and returns its ID: the next number in the store, or the
// child's that the try has already, which a repeated ask finds again. As
// docs/design.md says under Workflows a step calls:
//
//   - The workflow is the one the run's root pinned for the step as it was
//     created, by its text in the store, kept with the root (store.Tx.Text),
//     and read by its pin through Options.Sources only for a root that a
//     release created that kept none: a step whose workflow is neither is
//     child_not_pinned.
//   - Create compiles it, as the run the step's with: is submitted to,
//     with what is private of the step (compile.Parent), the remote context
//     its pin has (plan.Source.Remote), the root's limits, and its
//     variables from the root's secret record, which the root's Start read
//     once for the whole tree (treeVariables). Its own steps' workflows are
//     those the root pinned for them (plan.Plan.FindCall). Create's
//     problems are the child's: it gives them, and stores no run.
//   - Its directory is subworkflows/<job>/step_<n> in the step's working
//     directory, as format 1's is, where <job> is the step's job's name, a
//     hyphen and the instance's index for a matrix job's: one outside its
//     root's run directory is child_dir_outside.
//   - It runs in the group the step's attempt was placed in, or, for one
//     whose placement is private, its run's default group.
//   - One transaction stores it, linked to the step's try (store.Link), as
//     CreateRun stores a run, with its Start in the queue every coordinator
//     works: its variables are its root's, so any replica starts it.
//
// It returns ErrStepGone for an attempt that no longer holds its job, or a
// run that has ended; ErrRunStopped for a step of a run that was stopped
// that doesn't outlive that, as the plan says of its if:
// (plan.OutlivesCancel), never the worker; and a *StepStoppedError for a
// step its attempt's control stops. Its transaction checks each again as
// it stores the child, so that no zombie attempt, nor a cancelled run or
// job, gets one. It returns an error for a request that names no step
// whose uses: names a workflow, or a try past the step's retry:, where the
// plan writes it out (plan.Step.LastTry), which are its caller's mistakes.
func (c *Coordinator) CreateChild(ctx context.Context, req ChildRequest) (string, problems.Diagnostics, error) {
	if err := c.writable(); err != nil {
		return "", nil, err
	}
	var (
		attempt      *store.Attempt
		parent, root *store.Run
		text         []byte
		existing     string
		textFound    bool
		control      workerapi.Control
	)
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if attempt, err = tx.Attempt(req.Attempt); err != nil {
			return err
		}
		if existing, err = tx.Child(req.Attempt, req.Step, req.Try); err == nil || !errors.Is(err, store.ErrNotFound) {
			return err
		}
		if !attempt.Holds {
			return ErrStepGone
		}
		if parent, err = tx.Run(attempt.Run); err != nil {
			return err
		}
		if parent.State != store.RunRunning {
			return ErrStepGone
		}
		if control, err = tx.Control(attempt.ID); err != nil {
			return err
		}
		root = parent
		if parent.Parent != nil {
			root, err = tx.Run(parent.Parent.Root)
		}
		return err
	})
	switch {
	case err != nil:
		return "", nil, err
	case existing != "":
		return existing, nil, nil
	}
	p, err := c.plans.get(parent)
	if err != nil {
		return "", nil, err
	}
	name, instance, isInstance := strings.Cut(attempt.Job, "#")
	job := p.Job(name)
	if job == nil || req.Step < 0 || req.Step >= len(job.Steps) || req.Try < 0 {
		return "", nil, fmt.Errorf("coordinator: run %s's job %s has no step %d", parent.ID, attempt.Job, req.Step)
	}
	step := &job.Steps[req.Step]
	source := step.Source
	if source == nil {
		return "", nil, fmt.Errorf("coordinator: step %d of run %s's job %s calls no workflow", req.Step, parent.ID, attempt.Job)
	}
	// A try past the step's retry:, where the plan writes it out, is no
	// try of the step's: a worker can't make a child for each number.
	if req.Try > 0 {
		if last, known := step.LastTry(req.Try - 1); known && last {
			return "", nil, fmt.Errorf("coordinator: step %d of run %s's job %s has no try %d", req.Step, parent.ID, attempt.Job, req.Try)
		}
	}
	// Whether the step outlives its run's stop, or its job's, is the plan's
	// to say, from its if:, as Step decides it, never the worker's.
	if err := stepLives(attempt, parent, step, control); err != nil {
		return "", nil, err
	}
	rootPlan := p
	if root != parent {
		if rootPlan, err = c.plans.get(root); err != nil {
			return "", nil, err
		}
	}
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		text, err = tx.Text(root.ID, source.Digest)
		textFound = err == nil
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}); err != nil {
		return "", nil, err
	}
	if !textFound {
		// A root a release created that kept no texts: the workflow is read
		// by its pin, which checks its digest.
		if c.opts.Sources == nil {
			return "", nil, problems.ChildNotPinned.Problem(problems.Params{"uses": source.Ref.Uses})
		}
		w, err := c.opts.Sources.Read(ctx, *source)
		if err != nil {
			return "", nil, err
		}
		text = w.Text
	}
	dir := childDir(parent.RunDir, req.WorkingDirectory, name, instance, isInstance, req.Step)
	if rel, err := filepath.Rel(root.RunDir, dir); err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", nil, problems.ChildDirOutside.Problem(problems.Params{"dir": dir})
	}
	vars, err := c.rootVariables(ctx, root.ID)
	if err != nil {
		return "", nil, err
	}
	depth := 1
	if p.Run.Parent != nil {
		depth = p.Run.Parent.Depth + 1
	}
	link := store.Link{Run: parent.ID, Job: attempt.Job, Attempt: attempt.ID, Step: req.Step, Try: req.Try, Root: root.ID, Depth: depth}
	remote := make(map[string]any, len(source.Remote))
	for k, v := range source.Remote {
		remote[k] = v
	}
	created, diags, err := compile.Create(ctx, source.Location, text, req.With, compile.Options{
		Source: *source, Contexts: map[string]any{"remote": remote}, Variables: vars,
		Inputs: c.opts.Inputs, Groups: c.opts.Groups, Sessions: c.opts.Sessions, Limits: p.Run.Limits,
		Parent: &compile.Parent{
			Run: link.Run, Job: link.Job, Step: link.Step, Try: link.Try, Root: link.Root, Depth: link.Depth,
			Env: req.Env, Private: privateNames(req.WithPrivate, req.EnvPrivate), Secrets: privateNames(req.WithSecrets, req.EnvSecrets),
			Masked: req.Masked, Pin: rootPlan.FindCall,
		},
	})
	if err != nil || created == nil {
		return "", diags, err
	}
	group := attempt.Placement.Group
	if group == "" {
		group = parent.DefaultGroup
	}
	if c.beforeChildStore != nil {
		c.beforeChildStore()
	}
	var id string
	err = c.store.Tx(ctx, func(tx *store.Tx) error {
		// What another ask stored meanwhile is the try's child.
		var err error
		if id, err = tx.Child(req.Attempt, req.Step, req.Try); err == nil || !errors.Is(err, store.ErrNotFound) {
			return err
		}
		// What changed meanwhile, as the attempt's being superseded or
		// the run's being stopped, is read again here, where the child is
		// stored.
		a, err := tx.Attempt(req.Attempt)
		if err != nil {
			return err
		}
		r, err := tx.Run(parent.ID)
		if err != nil {
			return err
		}
		ctl, err := tx.Control(req.Attempt)
		if err != nil {
			return err
		}
		if err := stepLives(a, r, step, ctl); err != nil {
			return err
		}
		if id, err = tx.NewRunID(); err != nil {
			return err
		}
		return c.insertRun(ctx, tx, id, created, sharedQueue, group, dir, &link)
	})
	if err != nil {
		return "", nil, err
	}
	return id, diags, nil
}

// stepLives returns ErrStepGone for an attempt a, of run, that no longer
// holds its job, or a run that has ended; ErrRunStopped for a run that was
// stopped, of a step that doesn't outlive that, as its if: says
// (plan.OutlivesCancel); and a *StepStoppedError for a step that ctl, the
// attempt's control, stops: a cancel of its job, for any reason but the
// run's timeout, of such a step, or, for a step whose early-cancel: is
// any-job-failed, a job of the run having failed.
func stepLives(a *store.Attempt, run *store.Run, step *plan.Step, ctl workerapi.Control) error {
	outlives := plan.OutlivesCancel(step.If)
	switch {
	case !a.Holds || run.State != store.RunRunning:
		return ErrStepGone
	case !run.StoppedAt.IsZero() && !outlives:
		return ErrRunStopped
	case ctl.Cancel && ctl.Reason != plan.ReasonTimeout && !outlives:
		return &StepStoppedError{Reason: ctl.Reason}
	case ctl.AnyJobFailed && step.EarlyCancel == plan.EarlyCancelAnyJobFailed:
		return &StepStoppedError{}
	}
	return nil
}

// privateNames returns the names of what a step gives a child run that is private
// (compile.Parent.Private and Secrets): of its with:, by their paths with
// inputs. before them, and of its environment, by name, with env. before.
func privateNames(with, env []string) []string {
	out := make([]string, 0, len(with)+len(env))
	for _, path := range with {
		out = append(out, "inputs."+path)
	}
	for _, name := range env {
		out = append(out, plan.JoinPath("env", name))
	}
	return out
}

// childDir is the directory of the child run of the step at index of the
// job name, of the instance instance of it if isInstance, of a run whose
// directory is runDir: subworkflows/<job>/step_<index> in the step's
// working directory, wd, or the run's directory for none, as format 1
// makes it, where <job> is name, or for an instance of a matrix job, its
// name, a hyphen and its index.
func childDir(runDir, wd, name, instance string, isInstance bool, index int) string {
	base := runDir
	switch {
	case filepath.IsAbs(wd):
		base = wd
	case wd != "":
		base = filepath.Join(runDir, wd)
	}
	if isInstance {
		name += "-" + instance
	}
	return filepath.Join(base, "subworkflows", name, "step_"+strconv.Itoa(index))
}

// treeVariables are a child run's organization and user variables: those
// its root's Start read once for the whole tree, into the root's secret
// record, as format 1 reads them once for the tree. A child's Create and
// Start read them so, never through Options.Variables, so that any
// replica creates and starts a child, resume among them.
type treeVariables struct {
	org, vars map[string]string
	secret    map[string]bool
}

// rootVariables returns the variables of the tree of the run root, which
// its Start read, from its secret record and its run state: an error for
// a root that hasn't started, or has ended, whose record is gone.
func (c *Coordinator) rootVariables(ctx context.Context, root string) (treeVariables, error) {
	var (
		run    *store.Run
		stored *store.Record
	)
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if run, err = tx.Run(root); err != nil {
			return err
		}
		stored, err = tx.Record(root)
		return err
	})
	if err != nil {
		return treeVariables{}, fmt.Errorf("coordinator: the secret record of run %s, the root of a tree: %w", root, err)
	}
	record, err := c.sealer.Open(ctx, root, stored.Generation, sealedOf(stored))
	if err != nil {
		return treeVariables{}, err
	}
	if run.StartState == nil || record.Run == nil {
		return treeVariables{}, fmt.Errorf("coordinator: run %s, the root of a tree, hasn't started", root)
	}
	values := plan.JoinRunValues(*record.Run, run.StartState.Run)
	secret := make(map[string]bool, len(values.Secrets))
	for _, name := range values.Secrets {
		secret[name] = true
	}
	return treeVariables{org: values.Org, vars: values.Vars, secret: secret}, nil
}

// Read gives the variables names names, as compile.Variables does, from
// those the root read: one it didn't is left out, as one that isn't set.
func (v treeVariables) Read(_ context.Context, names []string, secrets bool) (map[string]compile.Variable, error) {
	out := map[string]compile.Variable{}
	add := func(context, name, value string) {
		secret := v.secret[plan.JoinPath(context, name)]
		if secret && !secrets {
			value = ""
		}
		out[context+"."+name] = compile.Variable{Value: value, Secret: secret}
	}
	for _, name := range names {
		context, rest, _ := strings.Cut(name, ".")
		var values map[string]string
		switch context {
		case "org":
			values = v.org
		case "vars":
			values = v.vars
		default:
			continue
		}
		if rest == "" {
			for k, value := range values {
				add(context, k, value)
			}
			continue
		}
		if value, ok := values[rest]; ok {
			add(context, rest, value)
		}
	}
	return out, nil
}
