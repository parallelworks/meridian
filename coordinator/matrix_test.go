package coordinator

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// instancesOf returns the run's matrix job name's instances, by index.
func instancesOf(t *testing.T, c *Coordinator, id, name string) []*store.Job {
	t.Helper()
	var jobs []*store.Job
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		jobs, err = tx.Instances(id, name)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return jobs
}

// succeeds is how a job whose steps succeeded ends.
var succeeds = testEnd{result: plan.JobResult{Outcome: plan.OutcomeSuccess}}

// TestMatrixIncludeExclude checks a matrix written out, which Start
// expands: its exclude and include entries give its instances, each of
// which is placed and run, and the job ends as they did.
func TestMatrixIncludeExclude(t *testing.T) {
	c := open(t, Options{})
	r := runToEnd(t, c, `
jobs:
  main:
    strategy:
      matrix:
        os: [linux, mac]
        size: [1, 2]
        exclude:
          - os: mac
            size: 2
        include:
          - os: windows
            size: 1
    steps:
      - run: echo ${{ matrix.os }} ${{ matrix.size }}
`, nil)
	if j := storedJob(t, c, r.ID, "main"); r.Outcome != plan.OutcomeSuccess || j.Outcome != plan.OutcomeSuccess {
		t.Fatalf("the run ended with %s, and its job with %s", r.Outcome, j.Outcome)
	}
	x := r.StartState.Expansions["main"]
	var got []map[string]any
	for _, i := range x.Instances {
		got = append(got, i.Matrix.Get().(map[string]any))
	}
	if want := "[map[os:linux size:1] map[os:linux size:2] map[os:mac size:1] map[os:windows size:1]]"; fmt.Sprint(got) != want {
		t.Errorf("the instances are %v, want %s", got, want)
	}
	if instances := instancesOf(t, c, r.ID, "main"); len(instances) != 4 {
		t.Errorf("the job has %d instances", len(instances))
	}
	placed := placedJobs(c)
	slices.Sort(placed)
	if !slices.Equal(placed, []string{"main#0", "main#1", "main#2", "main#3"}) {
		t.Errorf("the jobs placed are %v", placed)
	}
}

// TestMatrixOutputs checks a matrix job's result: its instances' outputs,
// each from the instance with the highest index that set it, whatever
// order they ended in.
func TestMatrixOutputs(t *testing.T) {
	c := open(t, Options{})
	byJob(c, map[string]testEnd{
		"main#0": {result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": "0", "first": "only 0"}}, delay: 60 * time.Millisecond},
		"main#1": {result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": "1"}}, delay: 30 * time.Millisecond},
		"main#2": {result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": "2"}}},
	})
	r := runToEnd(t, c, `
jobs:
  main:
    strategy:
      matrix:
        n: [0, 1, 2]
    steps:
      - run: echo x=${{ matrix.n }} >> $OUTPUTS
`, nil)
	j := storedJob(t, c, r.ID, "main")
	if j.Outcome != plan.OutcomeSuccess || j.Result.Outputs["x"] != "2" || j.Result.Outputs["first"] != "only 0" {
		t.Errorf("the matrix job ended with %+v", j.Result)
	}
}

// TestMatrixFromNeeds checks a matrix that reads what a job it needs gave,
// which its supervisor expands once that job has ended, as Ready does, and
// keeps as run state.
func TestMatrixFromNeeds(t *testing.T) {
	c := open(t, Options{})
	byJob(c, map[string]testEnd{
		"first": {result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"parts": `["p", "q"]`}}},
	})
	r := runToEnd(t, c, `
jobs:
  first:
    steps:
      - id: parts
        run: echo parts='["p", "q"]' >> $OUTPUTS
    outputs:
      parts: ${{ steps.parts.outputs.parts }}
  after:
    needs: [first]
    strategy:
      matrix:
        part: ${{ fromJSON(needs.first.outputs.parts) }}
    steps:
      - run: echo ${{ matrix.part }}
`, nil)
	j := storedJob(t, c, r.ID, "after")
	if r.Outcome != plan.OutcomeSuccess || j.Outcome != plan.OutcomeSuccess || j.Expansion == nil || len(j.Expansion.Instances) != 2 {
		t.Fatalf("the run ended with %s, and the matrix job is %+v", r.Outcome, j)
	}
	if got := j.Expansion.Instances[1].Matrix.Get(); !maps.Equal(got.(map[string]any), map[string]any{"part": "q"}) {
		t.Errorf("the second instance is %v", got)
	}
}

// TestPrivateMatrix checks a matrix made from a variable: run state holds
// its instances' keys alone, the run's secret record their values, and
// each instance is readied with its own.
func TestPrivateMatrix(t *testing.T) {
	vars := conformancetest.Variables{"vars.SITES": {Value: `["a", "b"]`}}
	c := open(t, Options{Variables: vars})
	release := make(chan struct{})
	byJob(c, map[string]testEnd{"main#1": {result: plan.JobResult{Outcome: plan.OutcomeSuccess}, wait: release}})
	id, _ := create(t, c, `
jobs:
  main:
    strategy:
      matrix:
        site: ${{ fromJSON(vars.SITES) }}
    steps:
      - run: echo ${{ matrix.site }}
`, nil, vars)
	serve(t, c)
	waitFor(t, func() bool { return len(placedJobs(c)) == 2 })
	run := storedRun(t, c, id)
	state := run.StartState.Expansions["main"]
	if state == nil || len(state.Instances) != 2 || !state.Instances[0].Private || state.Instances[0].Matrix.Get() != nil {
		t.Fatalf("run state holds the matrix as %+v", state)
	}
	if rec := storedRecord(t, c, id); rec.Expansions["main"] == nil || rec.Expansions["main"].Instances[1].Matrix.Get().(map[string]any)["site"] != "b" {
		t.Errorf("the record holds the matrix as %+v", rec.Expansions["main"])
	}
	r, err := c.readyJob(t.Context(), run, "main#1")
	if err != nil {
		t.Fatal(err)
	}
	if r.result.Job == nil || r.result.Job.Matrix.Get().(map[string]any)["site"] != "b" {
		t.Errorf("the instance is readied as %+v", r.result.Job)
	}
	close(release)
	if r := waitEnded(t, c, id); r.Outcome != plan.OutcomeSuccess {
		t.Errorf("the run ended with %s", r.Outcome)
	}
}

// anyWorkflow is a workflow one of whose jobs needs any instance of a
// matrix job.
const anyWorkflow = `
jobs:
  build:
    strategy:
      fail-fast: false
      matrix:
        n: [0, 1, 2]
    steps:
      - run: echo ${{ matrix.n }}
  deploy:
    needs: [build:any]
    steps:
      - run: echo deploy
`

// TestAnyFirstSuccess checks a need on any instance of a matrix job: the
// job that needs it is readied once one instance succeeds, with that one's
// result, which it pins, while the rest still run.
func TestAnyFirstSuccess(t *testing.T) {
	c := open(t, Options{})
	release := make(chan struct{})
	byJob(c, map[string]testEnd{
		"build#0": {result: plan.JobResult{Outcome: plan.OutcomeFailure}, wait: release},
		"build#1": succeeds,
		"build#2": {result: plan.JobResult{Outcome: plan.OutcomeFailure}, wait: release},
	})
	id, _ := create(t, c, anyWorkflow, nil, nil)
	serve(t, c)
	waitFor(t, func() bool { return slices.Contains(placedJobs(c), "deploy") })
	if j := storedJob(t, c, id, "deploy"); j.AnyChoices["build"] != "build#1" {
		t.Errorf("deploy was readied with %v", j.AnyChoices)
	}
	if j := storedJob(t, c, id, "build"); j.Outcome != "" {
		t.Errorf("the matrix job ended with %s before its instances", j.Outcome)
	}
	close(release)
	r := waitEnded(t, c, id)
	if j := storedJob(t, c, id, "deploy"); r.Outcome != plan.OutcomeFailure || j.Outcome != plan.OutcomeSuccess {
		t.Errorf("the run ended with %s, and deploy with %s", r.Outcome, j.Outcome)
	}
}

// TestAnyFailure checks a need on any instance of a matrix job none of
// whose instances succeeded: once all have ended, the job that needs it
// reads the first to fail, and is skipped because it failed.
func TestAnyFailure(t *testing.T) {
	c := open(t, Options{})
	// They fail in the order 1, 2, 0, each once the one before has ended.
	second, third := make(chan struct{}), make(chan struct{})
	byJob(c, map[string]testEnd{
		"build#0": {result: plan.JobResult{Outcome: plan.OutcomeFailure}, wait: third},
		"build#1": {result: plan.JobResult{Outcome: plan.OutcomeFailure}},
		"build#2": {result: plan.JobResult{Outcome: plan.OutcomeFailure}, wait: second},
	})
	id, _ := create(t, c, anyWorkflow, nil, nil)
	serve(t, c)
	ended := func(key string) func() bool {
		return func() bool {
			instances := instancesOf(t, c, id, "build")
			i := slices.IndexFunc(instances, func(j *store.Job) bool { return j.Key == key })
			return i >= 0 && instances[i].Outcome != ""
		}
	}
	waitFor(t, ended("build#1"))
	close(second)
	waitFor(t, ended("build#2"))
	close(third)
	r := waitEnded(t, c, id)
	j := storedJob(t, c, r.ID, "deploy")
	if r.Outcome != plan.OutcomeFailure || j.Outcome != plan.OutcomeSkipped || !j.Result.NeedsFailed || j.AnyChoices["build"] != "build#1" {
		t.Errorf("the run ended with %s, and deploy is %+v, readied with %v", r.Outcome, j.Result, j.AnyChoices)
	}
}

// TestFailFast checks fail-fast: an instance that fails stops the rest: one
// whose worker runs is told to cancel, with the reason fail-fast, and one
// waiting for its turn under max-parallel: ends cancelled, never placed.
func TestFailFast(t *testing.T) {
	c := open(t, Options{})
	running := make(chan struct{})
	byJob(c, map[string]testEnd{
		"main#0": {result: plan.JobResult{Outcome: plan.OutcomeFailure}, wait: running},
		"main#1": {running: true, started: running},
	})
	id, _ := create(t, c, `
jobs:
  main:
    strategy:
      max-parallel: 2
      matrix:
        n: [0, 1, 2]
    steps:
      - run: echo ${{ matrix.n }}
`, nil, nil)
	serve(t, c)
	waitFor(t, func() bool {
		instances := instancesOf(t, c, id, "main")
		return len(instances) == 3 && instances[2].Outcome != ""
	})
	if j := storedJob(t, c, id, "main#2"); j.Outcome != plan.OutcomeCancelled || j.Reason != plan.ReasonFailFast || j.Attempts != 0 {
		t.Errorf("the instance waiting for its turn is %+v", j)
	}
	if j := storedJob(t, c, id, "main"); j.FailFastAt.IsZero() || j.Outcome != "" {
		t.Errorf("the matrix job is %+v", j)
	}
	a := latestAttempt(t, c, id, "main#1")
	var ctl workerapi.Control
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		ctl, err = tx.Control(a.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !ctl.Cancel || ctl.Reason != plan.ReasonFailFast {
		t.Errorf("the running instance's worker is told %+v", ctl)
	}
	if placed := placedJobs(c); slices.Contains(placed, "main#2") {
		t.Errorf("the jobs placed are %v", placed)
	}
}

// TestMaxParallel checks max-parallel: no more of a matrix job's instances
// are placed at a time than it allows, as the times their attempts were
// made and ended say, and they have their turns in the order of their
// indexes.
func TestMaxParallel(t *testing.T) {
	c := open(t, Options{})
	driverOf(c).set(func(d *testDriver) {
		d.worker = func(req backend.PlaceRequest) testEnd {
			return testEnd{result: plan.JobResult{Outcome: plan.OutcomeSuccess}, delay: 30 * time.Millisecond}
		}
	})
	r := runToEnd(t, c, `
jobs:
  main:
    strategy:
      max-parallel: 2
      matrix:
        n: [0, 1, 2, 3, 4]
    steps:
      - run: echo ${{ matrix.n }}
`, nil)
	if r.Outcome != plan.OutcomeSuccess {
		t.Fatalf("the run ended with %s", r.Outcome)
	}
	type event struct {
		at    time.Time
		delta int
		key   string
	}
	var events []event
	for i := range 5 {
		key := "main#" + string(rune('0'+i))
		a := latestAttempt(t, c, r.ID, key)
		events = append(events, event{a.CreatedAt, 1, key}, event{a.EndedAt, -1, key})
	}
	// An end before a start at the same time, as the store's clock gives.
	slices.SortFunc(events, func(a, b event) int {
		if c := a.at.Compare(b.at); c != 0 {
			return c
		}
		return a.delta - b.delta
	})
	live, most := 0, 0
	var order []string
	for _, e := range events {
		live += e.delta
		most = max(most, live)
		if e.delta > 0 {
			order = append(order, e.key)
		}
	}
	if most > 2 {
		t.Errorf("%d instances were placed at a time: %v", most, events)
	}
	if !slices.Equal(order[:2], []string{"main#0", "main#1"}) && !slices.Equal(order[:2], []string{"main#1", "main#0"}) {
		t.Errorf("the instances were placed in the order %v", order)
	}
	if last := slices.Sorted(slices.Values(order[3:])); !slices.Equal(last, []string{"main#3", "main#4"}) {
		t.Errorf("the last instances placed are %v, of %v", last, order)
	}
}

// TestPickAny checks which instance a need on any instance reads.
func TestPickAny(t *testing.T) {
	at := func(s int) time.Time { return time.Unix(int64(s), 0) }
	job := func(key string, outcome plan.Outcome, ended int, needsFailed bool) *store.Job {
		j := &store.Job{Key: key, Outcome: outcome, EndedAt: at(ended)}
		if outcome != "" {
			j.Result = &plan.JobResult{Outcome: outcome, NeedsFailed: needsFailed}
		}
		return j
	}
	for _, tc := range []struct {
		name      string
		job       *store.Job
		instances []*store.Job
		want      string
		chosen    bool
	}{
		{"no instances yet", job("b", "", 0, false), nil, "", false},
		{"ended without instances", job("b", plan.OutcomeSkipped, 1, false), nil, "", true},
		{"the first to succeed", job("b", "", 0, false), []*store.Job{
			job("b#0", plan.OutcomeSuccess, 5, false), job("b#1", plan.OutcomeSuccess, 3, false), job("b#2", "", 0, false),
		}, "b#1", true},
		{"skipped by its own if:", job("b", "", 0, false), []*store.Job{
			job("b#0", plan.OutcomeSkipped, 2, false), job("b#1", "", 0, false),
		}, "b#0", true},
		{"skipped because its needs failed, still running", job("b", "", 0, false), []*store.Job{
			job("b#0", plan.OutcomeSkipped, 2, true), job("b#1", "", 0, false),
		}, "", false},
		{"all failed: the first to fail", job("b", "", 0, false), []*store.Job{
			job("b#0", plan.OutcomeCancelled, 1, false), job("b#1", plan.OutcomeFailure, 4, false), job("b#2", plan.OutcomeFailure, 3, false),
		}, "b#2", true},
		{"none failed: the first to end", job("b", "", 0, false), []*store.Job{
			job("b#0", plan.OutcomeCancelled, 4, false), job("b#1", plan.OutcomeSkipped, 2, true),
		}, "b#1", true},
	} {
		got, chosen := pickAny(tc.job, tc.instances)
		if got != tc.want || chosen != tc.chosen {
			t.Errorf("%s: pickAny = %q, %v, want %q, %v", tc.name, got, chosen, tc.want, tc.chosen)
		}
	}
}
