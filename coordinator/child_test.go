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
// its root doesn't, in its env: and its step, and calls a grandchild.
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
`,
	"github/t/grand@main": "jobs:\n  g:\n    steps:\n      - run: echo grand\n",
}

// treeRoot is the root of the tree: its job build's first step calls the
// child, tried again 20 times at most; and its third does too, and
// outlives its run's stop.
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
// root pinned them, and its remote context is its pin's; and that the root
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
	// The child starts with the root's variables.
	waitFor(t, func() bool { return storedRun(t, c, child).StartState != nil })
	if rec := storedRecord(t, c, child); rec == nil || rec.Run == nil || rec.Run.Env["FROM_VAR"] != "from-the-root-east" {
		t.Errorf("the child's record holds %+v", rec)
	}

	// The root's job ends, and its finisher waits for the child.
	close(buildGo)
	waitFor(t, func() bool { return !storedRun(t, c, root).WaitedAt.IsZero() })
	if r := storedRun(t, c, root); r.State == store.RunEnded || storedRecord(t, c, root) == nil {
		t.Errorf("the root ended before its child: %+v", r)
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
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.ChildDirOutside.Code() || p.Params["dir"] != "/elsewhere/subworkflows/build/step_0" {
		t.Errorf("a child outside its root's directory: %v", err)
	}
	if _, diags, err := c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, Try: 1, With: map[string]any{"$yaml": "w.yaml"}}); err != nil || !diags.HasErrors() || diags.Errors()[0].Code != problems.InputRequired.Code() {
		t.Errorf("a child without its required input: %v, %v", diags, err)
	}
	for name, req := range map[string]ChildRequest{
		"a step that calls no workflow": {Attempt: attempt.ID, Step: 1},
		"a step the job hasn't":         {Attempt: attempt.ID, Step: 3},
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
