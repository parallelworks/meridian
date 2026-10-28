package coordinator

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// stepRunRoot is a run whose job build runs where the run started, with
// steps on the host farm: a checkout, a command, a step that calls a
// workflow, which can't run so, a command whose host a variable names, a
// command whose ssh: is null, which runs in its job, an action the
// coordinator performs, whose ssh: places nothing, and a command whose
// host only the job's worker knows; whose job hosted runs on farm, with a
// step whose ssh: is null; and whose matrix job fan has a step on farm.
const stepRunRoot = `jobs:
  build:
    steps:
      - run: echo here
      - uses: parallelworks/checkout
        ssh: {remoteHost: farm}
        with: {repo: https://example.com/app.git, branch: main}
      - run: echo there
        ssh: {remoteHost: farm}
      - uses: github/t/grand@main
        ssh: {remoteHost: farm}
        with: {$yaml: w.yaml}
      - run: echo private
        ssh: {remoteHost: "${{ vars.FARM }}"}
      - run: echo null
        ssh: null
      - uses: parallelworks/cancel-jobs
        ssh: {remoteHost: farm}
        with: {jobs: [hosted]}
      - run: echo unknowable
        ssh: {remoteHost: "${{ env.HOST }}"}
      - run: echo retried
        retry: {max-retries: 2}
        ssh: {remoteHost: "farm${{ env.PW_WORKFLOW_STEP_CURRENT_RETRY }}"}
      - run: echo once
        ssh: {remoteHost: "farm${{ env.PW_WORKFLOW_STEP_CURRENT_RETRY }}"}
  blank:
    ssh: {remoteHost: ""}
    steps:
      - run: echo null
        ssh: null
  hosted:
    ssh: {remoteHost: farm}
    steps:
      - run: echo off
        ssh: null
  fan:
    strategy:
      matrix: {n: [1, 2]}
    steps:
      - run: echo fan
        ssh: {remoteHost: farm}
`

// farm is the host the steps' ssh: puts their tries on.
var farm = &plan.Inline{Name: "ssh", Backend: "ssh", Isolation: "host", SSH: plan.DataOf(map[string]any{"remoteHost": "farm"})}

// stepRunRig is a coordinator that runs stepRunRoot, whose jobs' attempts
// hold their places until the test ends, on d, as the local backend and as
// ssh: the run's ID, and its jobs' attempts, once placed.
type stepRunRig struct {
	c             *Coordinator
	d             *testDriver
	root          string
	build, hosted *store.Attempt
}

func newStepRunRig(t *testing.T) *stepRunRig {
	t.Helper()
	vars := conformancetest.Variables{"vars.FARM": {Value: "farm"}}
	d := newTestDriver()
	d.runDir = "/home/alice/.local/state/meridian/runs/1-store"
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	d.worker = func(backend.PlaceRequest) testEnd {
		return testEnd{wait: hold, result: plan.JobResult{Outcome: plan.OutcomeSuccess}}
	}
	c := open(t, Options{Variables: vars, Drivers: map[string]backend.Driver{testBackend: d, "ssh": d}})
	created, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(stepRunRoot), nil,
		compile.Options{Source: plan.Source{Location: "workflow.yaml"}, Sources: treeSources, Variables: vars})
	if err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	root, err := c.CreateRun(t.Context(), created, runOptions)
	if err != nil {
		t.Fatal(err)
	}
	serve(t, c)
	r := &stepRunRig{c: c, d: d, root: root}
	r.build, r.hosted = r.placed(t, root, "build"), r.placed(t, root, "hosted")
	return r
}

// placed waits for the latest attempt of the run's job key to be placed,
// and the run started, and returns it.
func (r *stepRunRig) placed(t *testing.T, run, key string) *store.Attempt {
	t.Helper()
	var a *store.Attempt
	waitFor(t, func() bool {
		return r.c.store.View(t.Context(), func(tx *store.Tx) error {
			var err error
			a, err = tx.LatestAttempt(run, key)
			return err
		}) == nil && a.Placed() && storedRun(t, r.c, run).StartState != nil
	})
	return a
}

// ask asks for the step run of the try req names, of the attempt a, as the
// worker API does: the step as the run's plan has it, then createStepRun,
// whose problems it fails the test for.
func (r *stepRunRig) ask(t *testing.T, a *store.Attempt, req stepRunRequest) (string, error) {
	t.Helper()
	id, diags, err := r.askDiags(t, a, req)
	if diags.HasErrors() {
		t.Fatalf("asking for a step run: %v", diags)
	}
	return id, err
}

// askDiags is ask, which gives createStepRun's problems.
func (r *stepRunRig) askDiags(t *testing.T, a *store.Attempt, req stepRunRequest) (string, problems.Diagnostics, error) {
	t.Helper()
	run := storedRun(t, r.c, a.Run)
	p, step, err := r.c.planStep(run, a, req.Step)
	if err != nil {
		return "", nil, err
	}
	if step == nil {
		return "", nil, errNotStepRun
	}
	return r.c.createStepRun(t.Context(), a, p, step, &req)
}

// TestStepRun checks a step run: a child of the step's try, which a
// repeated ask finds again, that names its caller's plan, job and step,
// in its caller's directory and default group; whose job is its caller's,
// placed through the ssh backend on the host the step's ssh: gave the
// try, in the same place in its root's directory there as its caller's
// run; and whose attempt's claim gives that job, and reads only its own
// step of the plan, its checkout's pin among them.
func TestStepRun(t *testing.T) {
	parallel(t)
	r := newStepRunRig(t)
	c, root := r.c, storedRun(t, r.c, r.root)
	state := plan.DataOf(map[string]any{"steps": []any{}})
	id, err := r.ask(t, r.build, stepRunRequest{Step: 2, SSH: farm, State: state})
	if err != nil {
		t.Fatal(err)
	}
	again, err := r.ask(t, r.build, stepRunRequest{Step: 2, SSH: farm, State: state})
	if err != nil || again != id {
		t.Errorf("asked again, the try's step run is %q, %v, want %s", again, err, id)
	}
	run := storedRun(t, c, id)
	want := store.Link{Run: root.ID, Job: "build", Attempt: r.build.ID, Step: 2, Root: root.ID, Depth: 1}
	switch {
	case !run.StepRun || run.Parent == nil || *run.Parent != want:
		t.Errorf("the step run is %+v, linked to %+v", run, run.Parent)
	case run.PlanDigest != root.PlanDigest || !bytes.Equal(run.Plan, root.Plan):
		t.Error("the step run's plan isn't its caller's")
	case run.RunDir != root.RunDir || run.DefaultGroup != root.DefaultGroup:
		t.Errorf("the step run is in %s, %s, want its caller's %s, %s", run.RunDir, run.DefaultGroup, root.RunDir, root.DefaultGroup)
	}

	// Its job is placed through the ssh backend, in its caller's place in
	// the root's directory on the host.
	a := r.placed(t, id, "build")
	if a.Placement.Backend != "ssh" {
		t.Errorf("the step run's job was placed on %+v", a.Placement)
	}
	reqs, _ := r.d.requests()
	found := false
	for _, req := range reqs {
		if req.Attempt == a.ID {
			found = true
			if req.Root != root.ID || req.Path != "." {
				t.Errorf("the step run's job was placed in %q of %q", req.Path, req.Root)
			}
		}
	}
	if !found {
		t.Error("the step run's job wasn't placed")
	}
	// A step run's own step has none of its own.
	if _, err := r.ask(t, a, stepRunRequest{Step: 2, SSH: farm}); !errors.Is(err, errNotStepRun) {
		t.Errorf("a step run's step asked for a step run: %v", err)
	}
	ready, err := claimReadier{c}.ReadyAttempt(t.Context(), run, a)
	switch {
	case err != nil:
		t.Fatal(err)
	case ready.Job.Target.Inline == nil || ready.Job.Target.Inline.SSH.Get().(map[string]any)["remoteHost"] != "farm":
		t.Errorf("the step run's job runs on %+v", ready.Job.Target)
	case ready.RunDir != r.d.runDir || !ready.RunDirOnMember || ready.Tree == nil || ready.Tree.RootDir != r.d.runDir:
		t.Errorf("the step run's claim gives %+v", ready)
	}
	if v, err := (claimReadier{c}).Needs(t.Context(), run, a); err != nil || v != workerapi.Version3 {
		t.Errorf("the step run's attempt needs version %d, %v", v, err)
	}

	// It reads its own step of the plan, and no other.
	for i, ok := range []bool{false, false, true, false, false, false, false, false, false, false} {
		if _, step, err := c.planStep(run, a, i); err != nil || (step != nil) != ok {
			t.Errorf("the step run's step %d is %+v, %v", i, step, err)
		}
	}
	checkout, err := r.ask(t, r.build, stepRunRequest{Step: 1, SSH: farm})
	if err != nil {
		t.Fatal(err)
	}
	if _, step, err := c.planStep(storedRun(t, c, checkout), &store.Attempt{Job: "build"}, 1); err != nil || step == nil || step.Checkout == nil || step.Checkout.Commit != childCommit {
		t.Errorf("the checkout's step run reads the step %+v, %v", step, err)
	}
	// A job with a step whose own ssh: can make it a step run needs
	// version 3 too, as the plan says it, its ssh: null among them.
	if v, err := (claimReadier{c}).Needs(t.Context(), root, r.hosted); err != nil || v != workerapi.Version3 {
		t.Errorf("an attempt of a job with a step whose ssh: is null needs version %d, %v", v, err)
	}
}

// TestPlanCacheStepRun checks that a step run shares its caller's decoded
// plan, which the cache keeps once.
func TestPlanCacheStepRun(t *testing.T) {
	created, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(oneJob), nil, compile.Options{Source: plan.Source{Location: "workflow.yaml"}})
	if err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	encoded, err := plan.Encode(created.Plan)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := plan.Digest(created.Plan)
	if err != nil {
		t.Fatal(err)
	}
	cache := newPlanCache(4, 1<<20)
	caller := &store.Run{ID: "1", PlanVersion: created.Plan.Version, PlanDigest: digest, Plan: encoded}
	stepRun := &store.Run{ID: "2", StepRun: true, Parent: &store.Link{Run: "1"}, PlanVersion: created.Plan.Version, PlanDigest: digest, Plan: encoded}
	p, err := cache.get(caller)
	if err != nil {
		t.Fatal(err)
	}
	if q, err := cache.get(stepRun); err != nil || q != p || cache.runs.Len() != 1 {
		t.Errorf("the step run's plan is %p, %v, its caller's %p, of %d kept", q, err, p, cache.runs.Len())
	}
}

// TestStepRunOfInstance checks that a step run of a matrix instance's step
// runs that one step of the instance's, as any step run does, rather than
// the matrix its job's strategy: makes.
func TestStepRunOfInstance(t *testing.T) {
	parallel(t)
	r := newStepRunRig(t)
	instance := r.placed(t, r.root, "fan#1")
	id, err := r.ask(t, instance, stepRunRequest{Step: 0, SSH: farm})
	if err != nil {
		t.Fatal(err)
	}
	run := storedRun(t, r.c, id)
	if run.Parent == nil || run.Parent.Job != "fan#1" {
		t.Fatalf("the step run is linked to %+v", run.Parent)
	}
	a := r.placed(t, id, "fan")
	ready, err := claimReadier{r.c}.ReadyAttempt(t.Context(), run, a)
	if err != nil || ready.Job.Target.Inline == nil || ready.Job.Key != "fan#1" {
		t.Errorf("the instance's step run's claim gives %+v, %v", ready, err)
	}
}

// TestStepRunRefuses checks the steps a step run is refused for, as the
// plan has them: one without an ssh: of its own, and one that calls a
// workflow; and the hosts: one that isn't an ssh: group's, or that names
// none, and a state that isn't a mapping. And that a step whose ssh: is
// null, in a job on a host, has one that runs where its job would without
// its ssh:, the run's default group.
func TestStepRunRefuses(t *testing.T) {
	parallel(t)
	r := newStepRunRig(t)
	host := func(ssh map[string]any) *plan.Inline {
		return &plan.Inline{Name: "ssh", Backend: "ssh", Isolation: "host", SSH: plan.DataOf(ssh)}
	}
	local := &plan.Inline{Name: "ssh", Backend: "local", Isolation: "host", SSH: plan.DataOf(map[string]any{"remoteHost": "farm"})}
	for name, req := range map[string]stepRunRequest{
		"a step without ssh:":                {Step: 0, SSH: farm},
		"a step that calls a workflow":       {Step: 3, SSH: farm},
		"another backend":                    {Step: 2, SSH: local},
		"another host":                       {Step: 2, SSH: host(map[string]any{"remoteHost": "elsewhere.example", "remoteUser": "root", "jumpNodeHost": "jump.example"})},
		"another login":                      {Step: 2, SSH: host(map[string]any{"remoteHost": "farm", "remoteUser": "root"})},
		"no host where the plan names one":   {Step: 2},
		"no host":                            {Step: 2, SSH: host(map[string]any{"remoteHost": ""})},
		"a try past its retry:":              {Step: 2, Try: 1, SSH: farm},
		"a negative try":                     {Step: 2, Try: -1, SSH: farm},
		"a state that isn't a mapping":       {Step: 2, SSH: farm, State: plan.DataOf([]any{"steps"})},
		"a null ssh: in a job without ssh:":  {Step: 5},
		"an action the coordinator performs": {Step: 6, SSH: farm},
	} {
		if id, err := r.ask(t, r.build, req); !errors.Is(err, errNotStepRun) {
			t.Errorf("%s: %q, %v", name, id, err)
		}
	}
	// A step's retry counter is the try's, for a step with a retry: alone,
	// as its worker defines it; and its tries come in order, each after
	// the one before has failed.
	retried, err := r.ask(t, r.build, stepRunRequest{Step: 8, SSH: host(map[string]any{"remoteHost": "farm0"})})
	if err != nil {
		t.Errorf("a retried step's first try on farm0: %v", err)
	}
	if _, err := r.ask(t, r.build, stepRunRequest{Step: 9, SSH: farm}); err != nil {
		t.Errorf("a step without a retry: on farm, its counter unset: %v", err)
	}
	if _, err := r.ask(t, r.build, stepRunRequest{Step: 8, Try: 2, SSH: host(map[string]any{"remoteHost": "farm2"})}); !errors.Is(err, errNotStepRun) {
		t.Errorf("a try whose try before has no step run: %v", err)
	}
	if _, err := r.ask(t, r.build, stepRunRequest{Step: 8, Try: 1, SSH: host(map[string]any{"remoteHost": "farm1"})}); !errors.Is(err, errNotStepRun) || retried == "" {
		t.Errorf("a try while the try before runs: %v", err)
	}
	// A null ssh: in a job whose ssh: names no host runs in its job.
	if _, err := r.ask(t, r.placed(t, r.root, "blank"), stepRunRequest{Step: 0}); !errors.Is(err, errNotStepRun) {
		t.Errorf("a null ssh: in a job on no host: %v", err)
	}
	// One whose host only the job's worker knows is a problem of the
	// step's, which says what it reads.
	if id, diags, err := r.askDiags(t, r.build, stepRunRequest{Step: 7}); err != nil || id != "" || len(diags) != 1 || diags[0].Code != problems.StepSSHReadsWorker.Code() || diags[0].Params["ref"] != "env.HOST" {
		t.Errorf("a step whose ssh: reads the job's env gives %q, %v, %v", id, diags, err)
	}

	id, err := r.ask(t, r.hosted, stepRunRequest{Step: 0})
	if err != nil {
		t.Fatal(err)
	}
	run := storedRun(t, r.c, id)
	res, err := r.c.readyJob(t.Context(), run, "hosted")
	if err != nil || res.result == nil || res.result.Job == nil || res.result.Job.Target.Inline != nil || res.result.Job.Target.Group != "" {
		t.Errorf("a step whose ssh: is null, in a job on a host, runs at %+v, %v", res, err)
	}
	if a := r.placed(t, id, "hosted"); a.Placement.Backend != testBackend || a.Placement.Group != run.DefaultGroup {
		t.Errorf("it was placed on %+v", a.Placement)
	}
}

// TestStepRunPrivate checks that a step run whose host the step's ssh:
// reads from a variable is placed privately, as the coordinator's own
// evaluation of it says, that its claim masks what its caller's worker
// masked at the try, and that what the try gave, the host and the job's
// state, is in its secret record, sealed, never in the clear.
func TestStepRunPrivate(t *testing.T) {
	parallel(t)
	r := newStepRunRig(t)
	state := plan.DataOf(map[string]any{"token": "t0k3n-of-the-job"})
	id, err := r.ask(t, r.build, stepRunRequest{Step: 4, SSH: farm, State: state, Masked: []string{"t0k3n-of-the-job"}})
	if err != nil {
		t.Fatal(err)
	}
	run := storedRun(t, r.c, id)
	res, err := r.c.readyJob(t.Context(), run, "build")
	if err != nil || res.result.Job == nil || !placementPrivate(res.result.Job) {
		t.Errorf("the step run's job isn't private: %+v, %v", res, err)
	}
	a := r.placed(t, id, "build")
	if !a.Private || a.Placement.Group != "" || a.Placement.Member != "" {
		t.Errorf("the private step run's placement is %+v in the clear", a.Placement)
	}
	// Its worker masks what the caller's did.
	if ready, err := (claimReadier{r.c}).ReadyAttempt(t.Context(), run, a); err != nil || ready.Tree == nil || !slices.Contains(ready.Tree.Masked, "t0k3n-of-the-job") {
		t.Errorf("the step run's claim masks %+v, %v", ready, err)
	}
	var sealed *store.Record
	if err := r.c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		sealed, err = tx.Record(id)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed.Sealed, []byte("t0k3n-of-the-job")) || bytes.Contains(sealed.Sealed, []byte("farm")) {
		t.Error("the step run's secret record holds the try's values in the clear")
	}
	rec, err := r.c.openRecord(t.Context(), id)
	if err != nil || rec.StepRun == nil || !rec.StepRun.Private || rec.StepRun.State.Get().(map[string]any)["token"] != "t0k3n-of-the-job" {
		t.Errorf("the step run's record is %+v, %v", rec, err)
	}
}
