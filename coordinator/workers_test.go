package coordinator

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// TestClaimReadier checks what a claim of an attempt gives of its job, as
// the worker API's server readies it: the job, with the private output of
// the job it needs, which that job's part of the run's secret record holds,
// and only the part of the run's values and of its needs' results that the
// job reads; the variables the platform gives every job's steps
// (Options.Env); and how many steps it has.
func TestClaimReadier(t *testing.T) {
	parallel(t)
	vars := conformancetest.Variables{"vars.SHOWN": {Value: "yes"}, "vars.OTHER": {Value: "no"}}
	env := map[string]workerapi.EnvVar{"PW_USER": {Value: "alice"}, "PW_API_KEY": {Value: "k3y", Secret: true}}
	c := open(t, Options{Variables: vars, Env: env})
	driverOf(c).set(func(d *testDriver) {
		d.worker = func(req backend.PlaceRequest) testEnd {
			if strings.HasSuffix(req.Name, "-prep") {
				return testEnd{result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": "private-x"}, Private: []string{"x"}}}
			}
			return testEnd{result: plan.JobResult{Outcome: plan.OutcomeSuccess}}
		}
	})
	var (
		mu    sync.Mutex
		ready *workerapisrv.Ready
		steps int
	)
	c.placed = func(run *store.Run, attempt string, _ *plan.ReadyJob) {
		ctx := context.WithoutCancel(t.Context())
		var a *store.Attempt
		if err := c.store.View(ctx, func(tx *store.Tx) error {
			var err error
			a, err = tx.Attempt(attempt)
			return err
		}); err != nil {
			t.Error(err)
			return
		}
		if a.Job != "build" {
			return
		}
		r, err := claimReadier{c}.ReadyAttempt(ctx, run, a)
		if err != nil {
			t.Error(err)
			return
		}
		n, err := claimReadier{c}.Steps(ctx, run, a)
		if err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		ready, steps = r, n
		mu.Unlock()
	}
	id, _ := create(t, c, `
jobs:
  prep:
    steps:
      - run: echo prep
  build:
    needs: [prep]
    steps:
      - run: echo ${{ needs.prep.outputs.x }}
      - run: echo ${{ vars.SHOWN }}
      - run: echo three
`, nil, vars)
	serve(t, c)
	if r := waitEnded(t, c, id); r.Outcome != plan.OutcomeSuccess {
		t.Fatalf("the run ended %s", r.Outcome)
	}
	mu.Lock()
	defer mu.Unlock()
	switch {
	case ready == nil:
		t.Fatal("build's claim wasn't readied")
	case ready.Job.Key != "build" || len(ready.Job.Steps) != 3 || steps != 3:
		t.Errorf("the job readied is %s, of %d steps, and Steps says %d", ready.Job.Key, len(ready.Job.Steps), steps)
	}
	if ready.Run.Vars["SHOWN"] != "yes" || len(ready.Run.Vars) != 1 {
		t.Errorf("the run's values the job reads are %+v", ready.Run)
	}
	if !maps.Equal(ready.Env, env) {
		t.Errorf("the claim's variables are %v, want %v", ready.Env, env)
	}
	prep := ready.Needs["prep"]
	if prep.Outcome != plan.OutcomeSuccess || prep.Outputs["x"] != "private-x" || !slices.Equal(prep.Private, []string{"x"}) {
		t.Errorf("the job reads %+v of prep", prep)
	}
}

// TestNeedsActions checks the version of the worker API a job's attempt
// needs: version 3 for a job with a step that uses an action the
// coordinator performs, which the step asks it for through a path of that
// version, as for one that calls a workflow; and version 1 for a job of
// run: steps alone, or one whose step uses an action only a platform
// provides, which the worker fails itself.
func TestNeedsActions(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	created, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(`
jobs:
  checkout:
    steps:
      - uses: parallelworks/checkout
        with: {repo: "https://example.com/app.git", branch: main}
  cancel:
    steps:
      - run: echo first
      - uses: parallelworks/cancel-jobs
        with: {jobs: [plain]}
  session:
    steps:
      - uses: parallelworks/update-session
        with: {name: desktop}
  agent:
    steps:
      - uses: parallelworks/wait-for-agent
        with: {agentId: a}
  plain:
    steps:
      - run: echo plain
`), nil, compile.Options{Source: plan.Source{Location: "workflow.yaml"}, Sources: pinSources{}})
	if err != nil || created == nil || diags.HasErrors() {
		t.Fatalf("the workflow doesn't create: %v, %v", diags, err)
	}
	id, err := c.CreateRun(t.Context(), created, runOptions)
	if err != nil {
		t.Fatal(err)
	}
	run := storedRun(t, c, id)
	for job, want := range map[string]int{"checkout": workerapi.Version3, "cancel": workerapi.Version3, "session": workerapi.Version3, "agent": 1, "plain": 1} {
		if got, err := (claimReadier{c}).Needs(t.Context(), run, &store.Attempt{Run: id, Job: job}); err != nil || got != want {
			t.Errorf("the job %s needs version %d, %v; want %d", job, got, err, want)
		}
	}
}
