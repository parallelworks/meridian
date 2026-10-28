package coordinator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"maps"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// childSources gives the workflows a test's steps call, by their uses:, at
// a commit, with a remote context of their own; and reads one again by its
// pin.
type childSources map[string]string

func (s childSources) workflow(ref plan.SourceRef) (*compile.Workflow, error) {
	text, ok := s[ref.Uses]
	if !ok {
		return nil, compile.ErrNotFound
	}
	sum := sha256.Sum256([]byte(text))
	return &compile.Workflow{
		Source: plan.Source{Location: ref.Uses + ":" + ref.Path, Revision: "0123456789abcdef0123456789abcdef01234567", Digest: "sha256:" + hex.EncodeToString(sum[:])},
		Text:   []byte(text),
		Remote: map[string]string{"repo": "https://example.com/" + ref.Uses, "yaml": ref.Path},
	}, nil
}

func (s childSources) Resolve(_ context.Context, ref plan.SourceRef) (*compile.Workflow, error) {
	return s.workflow(ref)
}

func (s childSources) Read(_ context.Context, source plan.Source) (*compile.Workflow, error) {
	return s.workflow(source.Ref)
}

// Pin pins every checkout to childCommit.
func (s childSources) Pin(context.Context, string, string) (string, error) {
	return childCommit, nil
}

// childCommit is the commit childSources pins every checkout to.
const childCommit = "fedcba9876543210fedcba9876543210fedcba98"

// closingVariables are a test's variables, which refuse to be read once
// closed: a child run reads its root's, never these.
type closingVariables struct {
	conformancetest.Variables
	closed atomic.Bool
}

func (v *closingVariables) Read(ctx context.Context, names []string, secrets bool) (map[string]compile.Variable, error) {
	if v.closed.Load() {
		return nil, errors.New("the variables were read after the root started")
	}
	return v.Variables.Read(ctx, names, secrets)
}

// treeSources are the workflows of a tree: a child that reads a variable
// its root doesn't, in its env: and its step, calls a grandchild, and
// checks out a repository.
var treeSources = childSources{
	"github/t/child@main": `on:
  execute:
    inputs:
      region: {type: string}
env:
  FROM_VAR: ${{ vars.CHILD_VAR }}-${{ inputs.region }}
jobs:
  main:
    steps:
      - run: echo ${{ vars.CHILD_VAR }}
      - uses: github/t/grand@main
        with: {$yaml: w.yaml}
      - uses: parallelworks/checkout
        with: {repo: https://example.com/app.git, branch: main}
`,
	"github/t/grand@main": "jobs:\n  g:\n    steps:\n      - run: echo grand\n",
}

// treeRoot is the root of the tree: its job build's first step calls the
// child, tried again 20 times at most; and its third does too, and
// outlives its run's stop; and its fourth does too, and stops once a job
// of its run has failed.
const treeRoot = `jobs:
  build:
    steps:
      - uses: github/t/child@main
        with: {$yaml: w.yaml, region: east}
        retry: {max-retries: 20}
      - run: echo after
      - uses: github/t/child@main
        if: always()
        with: {$yaml: w.yaml, region: east}
        retry: {max-retries: 20}
      - uses: github/t/child@main
        early-cancel: any-job-failed
        with: {$yaml: w.yaml, region: east}
`

// createTree creates a run of treeRoot, as the meridian command does, with
// c's Sources, and returns its ID and what Create gave.
func createTree(t *testing.T, c *Coordinator, vars compile.Variables) (string, *compile.Created) {
	t.Helper()
	created, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(treeRoot), nil,
		compile.Options{Source: plan.Source{Location: "workflow.yaml"}, Sources: treeSources, Variables: vars})
	if err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	id, err := c.CreateRun(t.Context(), created, runOptions)
	if err != nil {
		t.Fatal(err)
	}
	return id, created
}

// TestCreateChild checks that a step's try gets a child run, created from
// the text its root pinned, kept in the store, linked to the try, in its
// directory under the step's, which a repeated ask finds again; that the
// root's Start reads the variables of its whole tree once, and the child's
// Create and Start read them from the root's secret record, never from the
// coordinator's Variables; that the child's own steps are pinned as the
// root pinned them, its checkout among them, and its remote context is
// its pin's; and that the root
// doesn't end before the child, which wakes it as it ends.
func TestCreateChild(t *testing.T) {
	parallel(t)
	vars := &closingVariables{Variables: conformancetest.Variables{"vars.CHILD_VAR": {Value: "from-the-root"}}}
	c := open(t, Options{Variables: vars})
	buildGo, mainGo := make(chan struct{}), make(chan struct{})
	byJob(c, map[string]testEnd{
		"build": {wait: buildGo, result: plan.JobResult{Outcome: plan.OutcomeSuccess}},
		"main":  {wait: mainGo, result: plan.JobResult{Outcome: plan.OutcomeSuccess}},
	})
	root, created := createTree(t, c, vars)
	if !slices.Equal(created.Plan.Run.TreeVariables, []string{"vars.CHILD_VAR"}) {
		t.Errorf("the root's tree variables are %q", created.Plan.Run.TreeVariables)
	}
	serve(t, c)
	var attempt *store.Attempt
	waitFor(t, func() bool {
		return c.store.View(t.Context(), func(tx *store.Tx) error {
			var err error
			attempt, err = tx.LatestAttempt(root, "build")
			return err
		}) == nil
	})
	if rec := storedRecord(t, c, root); rec == nil || rec.Run == nil || rec.Run.Vars["CHILD_VAR"] != "from-the-root" {
		t.Fatalf("the root's record holds %+v", rec)
	}
	vars.closed.Store(true)

	req := ChildRequest{
		Attempt: attempt.ID, Step: 0, Try: 0,
		With: map[string]any{"$yaml": "w.yaml", "region": "east"},
		Env:  map[string]string{"STAGE": "prod"},
	}
	child, diags, err := c.CreateChild(t.Context(), req)
	if err != nil || diags.HasErrors() || child == "" {
		t.Fatal(child, diags, err)
	}
	if again, _, err := c.CreateChild(t.Context(), req); err != nil || again != child {
		t.Errorf("the try's child asked again is %q, %v, want %s", again, err, child)
	}
	r := storedRun(t, c, child)
	if want := (store.Link{Run: root, Job: "build", Attempt: attempt.ID, Step: 0, Try: 0, Root: root, Depth: 1}); r.Parent == nil || *r.Parent != want {
		t.Errorf("the child's link is %+v, want %+v", r.Parent, want)
	}
	if want := "/runs/" + root + "/subworkflows/build/step_0"; r.RunDir != want || r.DefaultGroup != "local" {
		t.Errorf("the child runs in %s, in %s, want %s", r.RunDir, r.DefaultGroup, want)
	}
	p, err := plan.Decode(r.Plan)
	if err != nil {
		t.Fatal(err)
	}
	rootPlan := created.Plan
	pin := rootPlan.Jobs[0].Steps[0].Source
	if p.Run.Parent == nil || p.Run.Parent.Root != root || p.Run.Source.Digest != pin.Digest || !maps.Equal(p.Run.Remote, pin.Remote) {
		t.Errorf("the child's plan is of %+v, with %+v and the remote context %v, want the pin %+v", p.Run.Source, p.Run.Parent, p.Run.Remote, pin)
	}
	grand, ok := rootPlan.FindCall(pin.Digest, "/jobs/main/steps/1")
	if s := p.Jobs[0].Steps[1].Source; !ok || s == nil || s.Digest != grand.Digest {
		t.Errorf("the child's step is pinned to %+v, want the root's pin %+v", s, grand)
	}
	checkout, ok := rootPlan.FindCheckout(pin.Digest, "/jobs/main/steps/2")
	if co := p.Jobs[0].Steps[2].Checkout; !ok || co == nil || *co != checkout || co.Commit != childCommit {
		t.Errorf("the child's checkout is pinned to %+v, want the root's pin %+v", co, checkout)
	}
	// The child starts with the root's variables, and its job is placed,
	// its worker waiting to claim it.
	waitFor(t, func() bool {
		return storedRun(t, c, child).StartState != nil && c.store.View(t.Context(), func(tx *store.Tx) error {
			_, err := tx.LatestAttempt(child, "main")
			return err
		}) == nil
	})
	if rec := storedRecord(t, c, child); rec == nil || rec.Run == nil || rec.Run.Env["FROM_VAR"] != "from-the-root-east" {
		t.Errorf("the child's record holds %+v", rec)
	}

	// The root's job ends, which stops the child of its step, as the end
	// of the attempt that called it does, and its finisher waits for the
	// child, whose job's placement no worker has claimed yet, so it runs
	// on until one does.
	close(buildGo)
	waitFor(t, func() bool { return !storedRun(t, c, root).WaitedAt.IsZero() })
	if r := storedRun(t, c, root); r.State == store.RunEnded || storedRecord(t, c, root) == nil {
		t.Errorf("the root ended before its child: %+v", r)
	}
	if r := storedRun(t, c, child); r.State == store.RunEnded || r.StopReason != plan.ReasonCancelled {
		t.Errorf("the child of the ended attempt's step is %s, stopped for %q", r.State, r.StopReason)
	}
	// A step of an attempt that has ended gets no child.
	if _, _, err := c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, Step: 0, Try: 1, With: req.With}); !errors.Is(err, ErrStepGone) {
		t.Errorf("an ended attempt's step got a child: %v", err)
	}
	// The child's end wakes the root's finisher, well before it would look
	// again (treeWait).
	close(mainGo)
	waitFor(t, func() bool { return storedRun(t, c, root).State == store.RunEnded })
	if r := storedRun(t, c, root); r.Outcome != plan.OutcomeSuccess {
		t.Errorf("the root ended %s", r.Outcome)
	}
	if r := storedRun(t, c, child); r.State != store.RunEnded {
		t.Errorf("the child is %s once its root ended", r.State)
	}
}

// TestCreateChildOnHost checks the child of a step whose job runs on the
// host its format 1 ssh: names: it is prepared on the coordinator's
// machine, under its caller's run directory there, as format 1 prepares a
// subworkflow, in the step's own working directory, or else in
// subworkflows/<job>/step_<n>, so a working directory the worker on the
// host gives in its root's run directory there is the same place in the
// root's run directory here, and one elsewhere is outside it; the child
// runs in its run's default group, not in the group of the ssh: host,
// which no name finds; and a member that chooses run directories itself
// is asked to place the child's job in the same place in its root's
// there, which the claim gives as the run's and its root's directories,
// as the member's own.
func TestCreateChildOnHost(t *testing.T) {
	parallel(t)
	vars := conformancetest.Variables{"vars.CHILD_VAR": {Value: "v"}}
	d := newTestDriver()
	d.runDir = "/home/alice/.local/state/meridian/runs/1-store"
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	d.worker = func(backend.PlaceRequest) testEnd {
		return testEnd{wait: hold, result: plan.JobResult{Outcome: plan.OutcomeSuccess}}
	}
	c := open(t, Options{Variables: vars, Drivers: map[string]backend.Driver{testBackend: d, "ssh": d}})
	created, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(`jobs:
  build:
    ssh:
      remoteHost: farm
    steps:
      - uses: github/t/child@main
        with: {$yaml: w.yaml, region: east}
        retry: {max-retries: 20}
`), nil, compile.Options{Source: plan.Source{Location: "workflow.yaml"}, Sources: treeSources, Variables: vars})
	if err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	root, err := c.CreateRun(t.Context(), created, runOptions)
	if err != nil {
		t.Fatal(err)
	}
	serve(t, c)
	var attempt *store.Attempt
	waitFor(t, func() bool {
		return c.store.View(t.Context(), func(tx *store.Tx) error {
			var err error
			attempt, err = tx.LatestAttempt(root, "build")
			return err
		}) == nil && attempt.Placed() && storedRun(t, c, root).StartState != nil
	})
	if attempt.Placement.Backend != "ssh" || attempt.Placement.RunDir != d.runDir {
		t.Fatalf("the job was placed at %+v", attempt.Placement)
	}
	with := map[string]any{"$yaml": "w.yaml", "region": "east"}
	var children []string
	for try, tc := range []struct{ wd, dir string }{
		{"", "/runs/" + root + "/subworkflows/build/step_0"},
		{"work", "/runs/" + root + "/work"},
		// As PW_JOB_DIR gives them on the host.
		{d.runDir, "/runs/" + root},
		{d.runDir + "/work", "/runs/" + root + "/work"},
	} {
		child, diags, err := c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, Step: 0, Try: try, With: with, WorkingDirectory: tc.wd})
		if err != nil || diags.HasErrors() {
			t.Fatalf("working directory %q: %v, %v", tc.wd, diags, err)
		}
		if r := storedRun(t, c, child); r.RunDir != tc.dir || r.DefaultGroup != "local" {
			t.Errorf("working directory %q: the child runs in %s, in %s, want %s, in local", tc.wd, r.RunDir, r.DefaultGroup, tc.dir)
		}
		children = append(children, child)
	}
	_, _, err = c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, Step: 0, Try: 4, With: with, WorkingDirectory: "/scratch/work"})
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.ChildDirOutside.Code() || p.Params["dir"] != "/scratch/work" {
		t.Errorf("a working directory outside the run's on the host: %v", err)
	}

	// The child's job is placed in the same place in its root's directory
	// on the member, and its claim says so.
	child := children[0]
	var childAttempt *store.Attempt
	waitFor(t, func() bool {
		return c.store.View(t.Context(), func(tx *store.Tx) error {
			var err error
			childAttempt, err = tx.LatestAttempt(child, "main")
			return err
		}) == nil && childAttempt.Placed() && storedRun(t, c, child).StartState != nil
	})
	reqs, _ := d.requests()
	for _, req := range reqs {
		switch req.Attempt {
		case attempt.ID:
			if req.Root != "" || req.Path != "" {
				t.Errorf("the root's job was placed in %q of %q", req.Path, req.Root)
			}
		case childAttempt.ID:
			if req.Root != root || req.Path != "subworkflows/build/step_0" {
				t.Errorf("the child's job was placed in %q of %q", req.Path, req.Root)
			}
		}
	}
	ready, err := claimReadier{c}.ReadyAttempt(t.Context(), storedRun(t, c, child), childAttempt)
	if err != nil || ready.RunDir != d.runDir+"/subworkflows/build/step_0" || !ready.RunDirOnMember || ready.Tree == nil || ready.Tree.RootDir != d.runDir {
		t.Errorf("the child's claim gives %+v, %v", ready, err)
	}
	ready, err = claimReadier{c}.ReadyAttempt(t.Context(), storedRun(t, c, root), attempt)
	if err != nil || ready.RunDir != d.runDir || !ready.RunDirOnMember || ready.Tree != nil {
		t.Errorf("the root's claim gives %+v, %v", ready, err)
	}

	// A step of the child on the host whose working directory is in its
	// root's directory there, as PW_PARENT_JOB_DIR gives it, but not in
	// its own run's, calls a run in the same place in its root's here.
	grand, diags, err := c.CreateChild(t.Context(), ChildRequest{Attempt: childAttempt.ID, Step: 1, With: map[string]any{"$yaml": "w.yaml"}, WorkingDirectory: d.runDir + "/shared"})
	if err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	if r := storedRun(t, c, grand); r.RunDir != "/runs/"+root+"/shared" {
		t.Errorf("the child's child runs in %s", r.RunDir)
	}
}

// TestMemberDirs checks where an attempt's run, and its root, have their
// directories on its member: the run's and the root's own where the
// member took the run's directory as it was, or gave none; and where it
// chose them, the member's, the root's being the run's without the run's
// path in it, which is an error where the member put the run's
// elsewhere.
func TestMemberDirs(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	root := &store.Run{ID: "1", RunDir: "/runs/1"}
	child := &store.Run{ID: "2", RunDir: "/runs/1/subworkflows/build/step_0", Parent: &store.Link{Root: "1"}}
	same := &store.Run{ID: "3", RunDir: "/runs/1", Parent: &store.Link{Root: "1"}}
	host := "/home/alice/runs/1-store"
	for _, tc := range []struct {
		name      string
		run       *store.Run
		dir       string
		want      memberDirs
		wantError bool
	}{
		{"a root where the run started", root, "/runs/1", memberDirs{run: "/runs/1", root: "/runs/1"}, false},
		{"a root without a directory", root, "", memberDirs{run: "/runs/1", root: "/runs/1"}, false},
		{"a root on a host", root, host, memberDirs{run: host, root: host, own: true}, false},
		{"a child where the run started", child, child.RunDir, memberDirs{run: child.RunDir, root: "/runs/1"}, false},
		{"a child on a host", child, host + "/subworkflows/build/step_0", memberDirs{run: host + "/subworkflows/build/step_0", root: host, own: true}, false},
		{"a child in its root's directory on a host", same, host, memberDirs{run: host, root: host, own: true}, false},
		{"a child elsewhere on a host", child, "/home/alice/runs/2-store", memberDirs{}, true},
	} {
		a := &store.Attempt{ID: "a", Placement: store.Placement{RunDir: tc.dir}}
		got, err := c.memberDirs(t.Context(), tc.run, root, a)
		if (err != nil) != tc.wantError || got != tc.want {
			t.Errorf("%s: %+v, %v, want %+v", tc.name, got, err, tc.want)
		}
	}
}

// TestCreateChildRefuses checks what doesn't get a child: a step whose
// child's directory would be outside its root's, whose child's Create
// fails, which gives the child's problems and stores no run, and whose
// workflow the store doesn't keep with its root, without Sources to read
// it by its pin, which with them it reads; and a request that names no
// step that calls a workflow.
func TestCreateChildRefuses(t *testing.T) {
	parallel(t)
	vars := conformancetest.Variables{"vars.CHILD_VAR": {Value: "v"}}
	c := open(t, Options{Variables: vars})
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	byJob(c, map[string]testEnd{"build": {wait: hold, result: plan.JobResult{Outcome: plan.OutcomeSuccess}}, "main": {wait: hold}})
	root, _ := createTree(t, c, vars)
	serve(t, c)
	var attempt *store.Attempt
	waitFor(t, func() bool {
		return c.store.View(t.Context(), func(tx *store.Tx) error {
			var err error
			attempt, err = tx.LatestAttempt(root, "build")
			return err
		}) == nil && storedRun(t, c, root).StartState != nil
	})
	with := map[string]any{"$yaml": "w.yaml", "region": "east"}
	runs := func() int {
		n := 0
		_ = c.store.View(t.Context(), func(tx *store.Tx) error {
			return tx.SQL().QueryRowContext(t.Context(), "SELECT count(*) FROM meridian_runs").Scan(&n)
		})
		return n
	}
	before := runs()

	_, _, err := c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, With: with, WorkingDirectory: "/elsewhere"})
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.ChildDirOutside.Code() || p.Params["dir"] != "/elsewhere" {
		t.Errorf("a child outside its root's directory: %v", err)
	}
	if _, diags, err := c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, Try: 1, With: map[string]any{"$yaml": "w.yaml"}}); err != nil || !diags.HasErrors() || diags.Errors()[0].Code != problems.InputRequired.Code() {
		t.Errorf("a child without its required input: %v, %v", diags, err)
	}
	for name, req := range map[string]ChildRequest{
		"a step that calls no workflow": {Attempt: attempt.ID, Step: 1},
		"a step the job hasn't":         {Attempt: attempt.ID, Step: 4},
		"a try past the step's retry:":  {Attempt: attempt.ID, Step: 0, Try: 21, With: with},
		"no attempt":                    {Attempt: "0123", Step: 0},
	} {
		if _, _, err := c.CreateChild(t.Context(), req); err == nil {
			t.Errorf("%s got a child", name)
		}
	}
	if n := runs(); n != before {
		t.Errorf("the store holds %d runs, %d before", n, before)
	}

	// A root a release kept no texts for: without Sources, its step's
	// child isn't created; with them, its workflow is read by its pin.
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		_, err := tx.SQL().ExecContext(t.Context(), "DELETE FROM meridian_run_sources")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	_, _, err = c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, Try: 2, With: with})
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.ChildNotPinned.Code() || p.Params["uses"] != "github/t/child@main" {
		t.Errorf("a child whose text the store doesn't keep: %v", err)
	}
	c.opts.Sources = treeSources
	if id, diags, err := c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, Try: 3, With: with}); err != nil || diags.HasErrors() || id == "" {
		t.Errorf("a child read by its pin: %q, %v, %v", id, diags, err)
	}

	// Two asks that race get one child: the one that stores second finds
	// the first's, in its transaction.
	var (
		raced   atomic.Bool
		otherID string
		other   error
	)
	c.beforeChildStore = func() {
		if raced.CompareAndSwap(false, true) {
			otherID, _, other = c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, Try: 4, With: with})
		}
	}
	id, _, err := c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, Try: 4, With: with})
	c.beforeChildStore = nil
	if err != nil || other != nil || id == "" || id != otherID {
		t.Errorf("two asks that raced got %q, %v and %q, %v", id, err, otherID, other)
	}

	// What changes between Create and the transaction that would store the
	// child is read again there: an attempt superseded meanwhile, or a run
	// stopped meanwhile, gets none, and no run is stored.
	supersede := func() {
		if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
			_, _, err := tx.NewAttempt(store.NewAttempt{Run: root, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	before = runs()
	c.beforeChildStore = supersede
	_, _, err = c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, Try: 5, With: with})
	c.beforeChildStore = nil
	if !errors.Is(err, ErrStepGone) || runs() != before {
		t.Errorf("an attempt superseded as its child was stored: %v, with %d runs, %d before", err, runs(), before)
	}
	attempt = latestAttempt(t, c, root, "build")
	c.beforeChildStore = func() {
		if err := c.Cancel(t.Context(), root); err != nil {
			t.Error(err)
		}
	}
	_, _, err = c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, Try: 6, With: with})
	c.beforeChildStore = nil
	if !errors.Is(err, ErrRunStopped) || runs() != before {
		t.Errorf("a run stopped as its child was stored: %v, with %d runs, %d before", err, runs(), before)
	}

	// A step of a run that was stopped gets none, unless its if: outlives
	// that, as the plan says, whatever the worker would; and neither does
	// a step of an attempt that another superseded, which no longer holds
	// its job, though it hasn't ended.
	if _, _, err := c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, Try: 7, With: with}); !errors.Is(err, ErrRunStopped) {
		t.Errorf("a step of a stopped run got a child: %v", err)
	}
	if id, _, err := c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, Step: 2, Try: 8, With: with}); err != nil || id == "" {
		t.Errorf("a step whose if: outlives its run's stop: %q, %v", id, err)
	}
	supersede()
	if _, _, err := c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, Step: 2, Try: 9, With: with}); !errors.Is(err, ErrStepGone) {
		t.Errorf("a superseded attempt's step got a child: %v", err)
	}
}
