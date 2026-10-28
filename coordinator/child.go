package coordinator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// errStepGone is what createChild returns for a step's try that can't have
// a child: its attempt no longer holds its job, as one that has ended, or
// that another superseded, or whose job has ended; or its run has ended.
var errStepGone = errors.New("coordinator: the step's attempt no longer holds its job, or its run has ended")

// errRunStopped is what createChild returns for a step of a run that was
// stopped, as by a cancel or its timeout:, that doesn't outlive that, as
// its if: says (plan.OutlivesCancel).
var errRunStopped = errors.New("coordinator: the step's run was stopped")

// A stepStoppedError is what createChild returns for a step that its
// attempt's control stops, as its worker would have stopped it had it read
// the control first: one that doesn't outlive its job's cancellation, as
// its if: says (plan.OutlivesCancel), of a job the control cancels, for
// reason, any but the run's timeout; or, with no reason, one whose
// early-cancel: is any-job-failed, once a job of the run has failed
// (workerapi.Control.AnyJobFailed).
type stepStoppedError struct{ reason plan.Reason }

func (e *stepStoppedError) Error() string {
	if e.reason == "" {
		return "coordinator: the step's early-cancel: stops it, as a job of its run failed"
	}
	return "coordinator: the step's job is being cancelled, " + string(e.reason)
}

// createChild creates the child run of the try of a step whose uses: names
// a workflow that req asks for, of the attempt a, and returns its ID: the
// next number in the store, or the child's that the try has already, which
// a repeated ask finds again. step is the step at req.Step of a's job, and
// p the plan of a's run, as the worker API resolved them
// (childAPI.stepOf), which checked that the step calls a workflow and has
// the try: createChild doesn't check them again. As docs/design.md says
// under Workflows a step calls:
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
//   - Its directory is format 1's: the step's own working-directory:, in
//     its caller's run directory or absolute, or else
//     subworkflows/<job>/step_<n> in that, where <job> is the step's job's
//     name, a hyphen and the instance's index for a matrix job's
//     (plan.ChildDir). One outside its root's run directory is
//     child_dir_outside. The run's directory is on the machine the run
//     started on, even where the step runs on a host, as format 1
//     prepares a subworkflow there (childPlace); a member that chooses a
//     run's directory itself, as a host does, places the child's jobs in
//     the same place in its root's there (backend.PlaceRequest.Path).
//   - It runs in the group the step's attempt was placed in, or, for one
//     whose placement is private, or is on the host a format 1 ssh:
//     names, its run's default group (childPlace).
//   - One transaction stores it, linked to the step's try (store.Link), as
//     CreateRun stores a run, with its Start in the queue every coordinator
//     works: its variables are its root's, so any replica starts it.
//
// It returns errStepGone for an attempt that no longer holds its job, or a
// run that has ended; errRunStopped for a step of a run that was stopped
// that doesn't outlive that, as the plan says of its if:
// (plan.OutlivesCancel), never the worker; and a *stepStoppedError for a
// step its attempt's control stops. Its transaction checks each again as
// it stores the child, so that no zombie attempt, nor a cancelled run or
// job, gets one.
func (c *Coordinator) createChild(ctx context.Context, a *store.Attempt, p *plan.Plan, step *plan.Step, req *workerapi.ChildRequest) (string, problems.Diagnostics, error) {
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
		if attempt, err = tx.Attempt(a.ID); err != nil {
			return err
		}
		if existing, err = tx.Child(a.ID, req.Step, req.Try); err == nil || !errors.Is(err, store.ErrNotFound) {
			return err
		}
		if !attempt.Holds {
			return errStepGone
		}
		if parent, err = tx.Run(attempt.Run); err != nil {
			return err
		}
		if parent.State != store.RunRunning {
			return errStepGone
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
	// Whether the step outlives its run's stop, or its job's, is the plan's
	// to say, from its if:, as Step decides it, never the worker's.
	if err := stepLives(attempt, parent, step, control); err != nil {
		return "", nil, err
	}
	source := step.Source
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
	wd, group, err := c.childPlace(ctx, parent, root, attempt, req.WorkingDirectory)
	if err != nil {
		return "", nil, err
	}
	dir := plan.ChildDir(parent.RunDir, wd, attempt.Job, req.Step)
	if !within(root.RunDir, dir) {
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
	with, _ := req.With.Get().(map[string]any)
	created, diags, err := compile.Create(ctx, source.Location, text, with, compile.Options{
		Source: *source, Contexts: map[string]any{"remote": remote}, Variables: vars,
		Inputs: c.opts.Inputs, Groups: c.opts.Groups, Sessions: c.opts.Sessions, Limits: p.Run.Limits,
		Parent: &compile.Parent{
			Run: link.Run, Job: link.Job, Step: link.Step, Try: link.Try, Root: link.Root, Depth: link.Depth,
			Env: req.Env, Private: privateNames(req.WithPrivate, req.EnvPrivate), Secrets: privateNames(req.WithSecrets, req.EnvSecrets),
			Masked: req.Masked, Pin: rootPlan.FindCall, Checkout: rootPlan.FindCheckout,
		},
	})
	if err != nil || created == nil {
		return "", diags, err
	}
	if c.beforeChildStore != nil {
		c.beforeChildStore()
	}
	var id string
	err = c.store.Tx(ctx, func(tx *store.Tx) error {
		// What another ask stored meanwhile is the try's child.
		var err error
		if id, err = tx.Child(a.ID, req.Step, req.Try); err == nil || !errors.Is(err, store.ErrNotFound) {
			return err
		}
		// What changed meanwhile, as the attempt's being superseded or
		// the run's being stopped, is read again here, where the child is
		// stored.
		now, err := tx.Attempt(a.ID)
		if err != nil {
			return err
		}
		r, err := tx.Run(parent.ID)
		if err != nil {
			return err
		}
		ctl, err := tx.Control(a.ID)
		if err != nil {
			return err
		}
		if err := stepLives(now, r, step, ctl); err != nil {
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

// stepLives returns errStepGone for an attempt a, of run, that no longer
// holds its job, or a run that has ended; errRunStopped for a run that was
// stopped, of a step that doesn't outlive that, as its if: says
// (plan.OutlivesCancel); and a *stepStoppedError for a step that ctl, the
// attempt's control, stops: a cancel of its job, for any reason but the
// run's timeout, of such a step, or, for a step whose early-cancel: is
// any-job-failed, a job of the run having failed.
func stepLives(a *store.Attempt, run *store.Run, step *plan.Step, ctl workerapi.Control) error {
	outlives := plan.OutlivesCancel(step.If)
	switch {
	case !a.Holds || run.State != store.RunRunning:
		return errStepGone
	case !run.StoppedAt.IsZero() && !outlives:
		return errRunStopped
	case ctl.Cancel && ctl.Reason != plan.ReasonTimeout && !outlives:
		return &stepStoppedError{reason: ctl.Reason}
	case ctl.AnyJobFailed && step.EarlyCancel == plan.EarlyCancelAnyJobFailed:
		return &stepStoppedError{}
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

// childPlace returns where the child of a step of the attempt a, of run,
// whose tree's root is root, is prepared, as format 1 prepares a
// subworkflow: the step's own working directory, wd, as the step's worker
// gives it, as the child's directory takes it (plan.ChildDir), and the
// group the child runs in.
//
//   - Format 1 prepares a subworkflow on the machine the run started on,
//     even for a step on a host, and so resolves the step's
//     working-directory: there, before it maps the run's directory to the
//     host's. A worker on a host gives a working directory on the host,
//     which, where it is inside the root's run directory there
//     (memberDirs), as one that PW_JOB_DIR or PW_PARENT_JOB_DIR gives is,
//     is taken to the same place in the root's run directory on the
//     coordinator's machine. One elsewhere is as it is.
//   - The child runs in the group the attempt was placed in, as the
//     group's other members can run its jobs, but for one placed in the
//     group of one member that a format 1 ssh: makes (plan.Target.Inline),
//     which no name finds, and one whose placement is private, whose group
//     the store doesn't hold in the clear: then in its run's default
//     group, as format 1 runs a subworkflow's jobs where their own ssh:
//     says, or else on the machine the run started on, and never on its
//     caller's host because the caller ran there.
func (c *Coordinator) childPlace(ctx context.Context, run, root *store.Run, a *store.Attempt, wd string) (string, string, error) {
	if filepath.IsAbs(wd) {
		dirs, err := c.memberDirs(ctx, run, root, a)
		if err != nil {
			return "", "", err
		}
		if dirs.own && within(dirs.root, wd) {
			rel, err := filepath.Rel(dirs.root, wd)
			if err != nil {
				return "", "", err
			}
			wd = filepath.Join(root.RunDir, rel)
		}
	}
	group := a.Placement.Group
	if group == "" {
		return wd, run.DefaultGroup, nil
	}
	r, err := c.readyJob(ctx, run, a.Job)
	if err != nil {
		return "", "", err
	}
	if r.result != nil && r.result.Job != nil && r.result.Job.Target.Inline != nil {
		group = run.DefaultGroup
	}
	return wd, group, nil
}

// within reports whether path is dir, or inside it.
func within(dir, path string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, "../") && !filepath.IsAbs(rel)
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
