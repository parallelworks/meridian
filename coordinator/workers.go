package coordinator

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"slices"
	"strings"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// ServeWorkers serves the worker API to the workers of the attempts the
// coordinator places, on l, a Unix socket's listener, where Endpoint says
// they reach it, until ctx is done, and then closes l: it takes
// connections only from the process's own user, or root, and stores the
// logs they upload with logs. A claim readies the attempt's job again as
// its supervisor readied it, and each change to an attempt that its
// supervisor acts on, as its claim or its end, wakes the supervisor. The
// polls it holds of attempts' controls are answered as soon as the
// coordinator changes them, rather than at the server's next read of them.
// Its server is ServeMembers' too, while both run, which stores the logs
// with the first's logs.
func (c *Coordinator) ServeWorkers(ctx context.Context, l net.Listener, logs Logs) error {
	if err := c.writable(); err != nil {
		return err
	}
	srv, release, err := c.useServer(logs)
	if err != nil {
		return err
	}
	defer release()
	return srv.Serve(ctx, l)
}

// wakeWorkerJob wakes, in tx, the supervisor of the run's job, once the
// worker API stores a change to its attempt that the supervisor acts on
// (workerapisrv.Options.Wake), in the transaction that stores it, and has
// it woken again once that commits, if hopper runs it (wakeAgainAfter).
func (c *Coordinator) wakeWorkerJob(ctx context.Context, tx *store.Tx, run, job string) error {
	w, err := c.wakeJobTx(ctx, tx, run, job)
	if err != nil {
		return err
	}
	c.wakeAgainAfter(tx, []*waking{w})
	return nil
}

// claimReadier is the coordinator's workerapisrv.Readier: it readies the
// job of an attempt a worker claims from what its supervisor pinned as it
// first readied the job, read and opened as the supervisor reads them
// (readReadyFrom, readyInput), so that every claim gives the job its
// supervisor placed, byte for byte.
type claimReadier struct{ c *Coordinator }

var _ workerapisrv.Readier = claimReadier{}

// ReadyAttempt returns the job of the attempt a, as Ready gives it again,
// with the part of the run's values and of its needs' results it reads.
// A step run's job is its caller's, which its caller's run readies, but for
// where it runs (stepRunJob).
func (r claimReadier) ReadyAttempt(ctx context.Context, run *store.Run, a *store.Attempt) (*workerapisrv.Ready, error) {
	c := r.c
	readyRun, key := run, a.Job
	if run.StepRun {
		caller, err := c.callerOf(ctx, run)
		if err != nil {
			return nil, err
		}
		readyRun, key = caller, run.Parent.Job
	}
	p, from, in, err := c.readyInputOf(ctx, readyRun, key)
	if err != nil {
		return nil, err
	}
	pj := p.Job(from.job.Name)
	res, diags, err := p.Ready(ctx, from.job.Name, in)
	switch {
	case err != nil:
		return nil, err
	case diags.HasErrors():
		// Ready gave the job once with the same inputs: this is a bug, and
		// the diagnostics, which show no private value, are logged with it.
		return nil, fmt.Errorf("coordinator: run %s's job %q doesn't ready again as it did: %w", readyRun.ID, key, diags)
	case res.Job == nil:
		return nil, fmt.Errorf("coordinator: run %s's job %q readies again as no job to run", readyRun.ID, key)
	}
	var stepRun *workerapi.StepRunClaim
	if run.StepRun {
		if res.Job, err = c.stepRunJob(ctx, run, res.Job); err != nil {
			return nil, err
		}
		// Its attempt runs the one step, with its caller's job's state.
		if stepRun, res.Job, err = c.stepRunClaim(ctx, run, res.Job); err != nil {
			return nil, err
		}
	}
	root, err := c.rootOf(ctx, run)
	if err != nil {
		return nil, err
	}
	dirs, err := c.memberDirs(ctx, run, root, a)
	if err != nil {
		return nil, err
	}
	tree, err := r.tree(ctx, run, root, dirs.root, p)
	if err != nil {
		return nil, err
	}
	return &workerapisrv.Ready{
		Job: res.Job, Run: pj.Reads(in.Run), Needs: pj.ReadsNeeds(in.Needs), Tree: tree, StepRun: stepRun,
		RunDir: dirs.run, RunDirOnMember: dirs.own,
	}, nil
}

// readyInputOf returns the plan of run, and what Ready readies its job key
// with, as its supervisor pinned it and a claim reads it: so that what is
// evaluated again with it, as a claim's Ready, or the coordinator's
// evaluation of a step's ssh: (stepTarget), reads what the job was
// readied with.
func (c *Coordinator) readyInputOf(ctx context.Context, run *store.Run, key string) (*plan.Plan, *readyFrom, plan.ReadyInput, error) {
	p, err := c.plans.get(run)
	if err != nil {
		return nil, nil, plan.ReadyInput{}, err
	}
	var from *readyFrom
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		job, err := tx.Job(run.ID, key)
		if err != nil {
			return err
		}
		pj := p.Job(job.Name)
		if pj == nil {
			return fmt.Errorf("coordinator: run %s's plan has no job %q", run.ID, job.Name)
		}
		from, err = readReadyFrom(tx, run, pj, job, false)
		return err
	}); err != nil {
		return nil, nil, plan.ReadyInput{}, err
	}
	in, err := c.readyInput(ctx, run, from)
	return p, from, in, err
}

// tree returns what an attempt of the child run run, of the plan p, has of
// its tree (workerapi.Tree): its root's identity, and its root's
// directory, rootDir, where the attempt runs (memberDirs), format 1's name
// for the call, and the strings the worker of the step that called it
// masked, from its secret record (plan.Inherited.Masked); or nil for a run
// no step called.
func (r claimReadier) tree(ctx context.Context, run, root *store.Run, rootDir string, p *plan.Plan) (*workerapi.Tree, error) {
	if run.Parent == nil {
		return nil, nil
	}
	rec, err := r.c.openRecord(ctx, run.ID)
	if err != nil {
		return nil, err
	}
	tree := &workerapi.Tree{JobID: root.ID, RootDir: rootDir, WorkflowName: p.Run.Source.Ref.CallName()}
	if rec.Inherited != nil {
		tree.Masked = rec.Inherited.Masked
	}
	if rec.StepRun != nil {
		// A step run's worker masks what its caller's did at the try.
		tree.Masked = append(slices.Clone(tree.Masked), rec.StepRun.Masked...)
	}
	return tree, nil
}

// Needs returns the oldest version of the worker API that has all the
// attempt's job uses: workerapi.Version3, of child runs and of the actions
// the coordinator performs, for a job of a child run, whose claim says
// what it has of its tree, a step run among them, for one with a step
// whose uses: names a workflow, which asks for its child, for one with a
// step whose own ssh: can make a try of it a step run, as the plan says it
// without evaluating it (plan.Job.StepElsewhere), which asks for it, and
// for one with a step that uses an action the coordinator performs
// (plan.CoordinatorAction), which asks for it; and 1 for any other.
func (r claimReadier) Needs(ctx context.Context, run *store.Run, a *store.Attempt) (int, error) {
	if run.Parent != nil {
		return workerapi.Version3, nil
	}
	_, j, err := r.c.planJob(run, a)
	if err != nil {
		return 0, err
	}
	if j != nil {
		for i := range j.Steps {
			s := &j.Steps[i]
			if s.Source != nil || plan.CoordinatorAction(s.Uses) || j.StepElsewhere(s) {
				return workerapi.Version3, nil
			}
		}
	}
	return 1, nil
}

// planJob returns run's plan, and the job of it that the attempt a, of
// run, runs: the job a matrix instance is of, for an instance's attempt;
// or nil if the plan has no job of that name.
func (c *Coordinator) planJob(run *store.Run, a *store.Attempt) (*plan.Plan, *plan.Job, error) {
	p, err := c.plans.get(run)
	if err != nil {
		return nil, nil, err
	}
	name, _, _ := strings.Cut(a.Job, "#")
	return p, p.Job(name), nil
}

// planStep returns run's plan, and the step at index of the job of it
// that the attempt a, of run, runs (planJob): nil if the job has no such
// step, or, for a step run, if it isn't the step its link names, the one
// step of the job its attempt runs; and an error if the plan has no such
// job.
func (c *Coordinator) planStep(run *store.Run, a *store.Attempt, index int) (*plan.Plan, *plan.Step, error) {
	p, job, err := c.planJob(run, a)
	switch {
	case err != nil:
		return nil, nil, err
	case job == nil:
		return nil, nil, fmt.Errorf("coordinator: run %s's plan has no job %q", run.ID, a.Job)
	case index < 0 || index >= len(job.Steps), run.StepRun && index != run.Parent.Step:
		return p, nil, nil
	}
	return p, &job.Steps[index], nil
}

// rootOf returns the root of run's tree: run itself, for a run no step
// called.
func (c *Coordinator) rootOf(ctx context.Context, run *store.Run) (*store.Run, error) {
	if run.Parent == nil {
		return run, nil
	}
	var root *store.Run
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		root, err = tx.Run(run.Parent.Root)
		return err
	})
	return root, err
}

// treePath returns the path of the directory of run, of the tree whose
// root is root, relative to the root's, as plan.InDir gives it: "." for
// a root, or a child whose directory is its root's. A child's is always
// in its root's, as createChild makes it (child_dir_outside).
func treePath(root, run *store.Run) (string, error) {
	path, ok := plan.InDir(root.RunDir, run.RunDir)
	if !ok {
		return "", fmt.Errorf("coordinator: run %s's directory isn't in its root's, run %s's", run.ID, root.ID)
	}
	return path, nil
}

// placeTree returns the root, and the path in its directory, that a
// PlaceRequest gives of an attempt of run (backend.PlaceRequest.Root and
// Path): its tree's root's ID and treePath, or "" and "" for a root.
func (c *Coordinator) placeTree(ctx context.Context, run *store.Run) (string, string, error) {
	if run.Parent == nil {
		return "", "", nil
	}
	root, err := c.rootOf(ctx, run)
	if err != nil {
		return "", "", err
	}
	path, err := treePath(root, run)
	return root.ID, path, err
}

// A memberDirs is where the directories of an attempt's run, and of its
// tree's root, are on the member the attempt runs on (memberDirs).
type memberDirs struct {
	// run is the run's directory there, and root its root's, the run's
	// own for a root.
	run, root string
	// own is set when the member chose them itself
	// (backend.Placement.RunDir), apart from the run's directory where
	// the run started, as a host the ssh backend reaches does: format
	// 1's copy of the tree there.
	own bool
}

// memberDirs returns where the directories of the attempt a's run, run,
// and of its root, root, are on the member where a runs: the run's as its
// member chose it (attemptRunDir), and its root's, which is that without
// the run's path in its root's (treePath), as a member that chooses a
// child's directory places it in its root's there
// (backend.PlaceRequest.Path); or the run's and the root's own where the
// member took the run's directory as it was, or gave none. A member's
// directory that isn't so is an error, which names no directory of the
// member, which a private placement keeps sealed.
func (c *Coordinator) memberDirs(ctx context.Context, run, root *store.Run, a *store.Attempt) (memberDirs, error) {
	dir, err := c.attemptRunDir(ctx, run, a)
	if err != nil {
		return memberDirs{}, err
	}
	// A member that gives the run's own directory took it as it was, as
	// the local backend does. One that chose its own gives another path:
	// a host's is in its state directory, named by the run's and the
	// store's IDs, where the meridian command makes a run's in the
	// system's temporary directory, or where its --run-dir says. Barring
	// a --run-dir that names a host's very path, the two are one path only
	// where they are one directory, on the machine the run started on,
	// where format 1 cleans a child's.
	if dir == "" || dir == run.RunDir {
		return memberDirs{run: run.RunDir, root: root.RunDir}, nil
	}
	path, err := treePath(root, run)
	if err != nil {
		return memberDirs{}, err
	}
	rootDir := filepath.Clean(dir)
	if path != "." {
		var ok bool
		if rootDir, ok = strings.CutSuffix(rootDir, string(filepath.Separator)+path); !ok || rootDir == "" {
			return memberDirs{}, fmt.Errorf("coordinator: attempt %s's member placed run %s's directory outside its root's there, at another path than %s", a.ID, run.ID, path)
		}
	}
	return memberDirs{run: dir, root: rootDir, own: true}, nil
}

// attemptRunDir returns the run's directory where the attempt a runs,
// where its member chose it (backend.Placement.RunDir): as the store
// holds it, or, for a private placement, as its place holds it sealed; or
// "" for the run's.
func (c *Coordinator) attemptRunDir(ctx context.Context, run *store.Run, a *store.Attempt) (string, error) {
	if !a.Private {
		return a.Placement.RunDir, nil
	}
	var (
		place   store.Place
		chosen  bool
		wrapped []byte
	)
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if place, chosen, err = tx.ChosenPlace(a.ID); err != nil || !chosen {
			return err
		}
		wrapped, err = tx.WrappedKey(run.ID)
		return err
	}); err != nil || !chosen {
		return "", err
	}
	opened, err := c.openPlaces(ctx, run.ID, wrapped, map[string]store.Place{a.ID: place})
	if err != nil {
		return "", err
	}
	return opened[a.ID].RunDir, nil
}

// Steps returns how many steps the attempt's job has, as the plan says.
func (r claimReadier) Steps(ctx context.Context, run *store.Run, a *store.Attempt) (int, error) {
	p, err := r.c.plans.get(run)
	if err != nil {
		return 0, err
	}
	var job *store.Job
	if err := r.c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		job, err = tx.Job(run.ID, a.Job)
		return err
	}); err != nil {
		return 0, err
	}
	pj := p.Job(job.Name)
	if pj == nil {
		return 0, fmt.Errorf("coordinator: run %s's plan has no job %q", run.ID, job.Name)
	}
	return len(pj.Steps), nil
}
