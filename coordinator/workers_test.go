package coordinator

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
)

// TestClaimReadier checks what a claim of an attempt gives of its job, as
// the worker API's server readies it: the job, with the private output of
// the job it needs, which that job's part of the run's secret record holds,
// and only the part of the run's values and of its needs' results that the
// job reads; and how many steps it has.
func TestClaimReadier(t *testing.T) {
	parallel(t)
	vars := conformancetest.Variables{"vars.SHOWN": {Value: "yes"}, "vars.OTHER": {Value: "no"}}
	c := open(t, Options{Variables: vars})
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
	prep := ready.Needs["prep"]
	if prep.Outcome != plan.OutcomeSuccess || prep.Outputs["x"] != "private-x" || !slices.Equal(prep.Private, []string{"x"}) {
		t.Errorf("the job reads %+v of prep", prep)
	}
}
