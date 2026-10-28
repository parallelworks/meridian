package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/coordtest"

	"github.com/parallelworks/foundation/problem"
	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// regionWorkflow reads a variable as it starts, and has a password.
const regionWorkflow = `
on:
  execute:
    inputs:
      region:
        type: string
        default: ${{ vars.REGION }}
      pw:
        type: password
jobs:
  main:
    steps:
      - run: echo ${{ inputs.region }}
`

var regionVars = conformancetest.Variables{"vars.REGION": {Value: "us"}}

var regionSubmitted = map[string]any{"pw": "HUNTER2SECRET"}

// TestRun checks a run from its creation to its end: Start keeps what it
// gave, the private part in the secret record and the rest as run state,
// as Start does in memory, and the finisher ends the run with its outcome
// and deletes its record.
func TestRun(t *testing.T) {
	parallel(t)
	c := open(t, Options{Variables: regionVars})
	id, created := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	if id != "1" {
		t.Errorf("the first run is %q", id)
	}
	r := storedRun(t, c, id)
	if r.State != store.RunCreated || r.DefaultGroup != "local" || r.RunDir != "/runs/1" || r.RecordGeneration != 1 {
		t.Errorf("a run created is %+v", r)
	}

	serve(t, c)
	r = waitEnded(t, c, id)
	if r.State != store.RunEnded || r.Outcome != plan.OutcomeSuccess || r.StartTries != 0 || len(r.Diagnostics) != 0 {
		t.Errorf("the run ended %s, %s, after %d tries, with %v", r.State, r.Outcome, r.StartTries, r.Diagnostics)
	}
	if rec := storedRecord(t, c, id); rec != nil {
		t.Error("the run's secret record is there once it has ended")
	}
	// What run state holds of Start's result is what Start keeps in
	// memory, and holds no private value.
	res, _, err := compile.Start(t.Context(), created.Plan, compile.StartInput{RunID: id, Variables: regionVars, Record: created.Record})
	if err != nil {
		t.Fatal(err)
	}
	record := created.Record
	want, err := compile.EncodeStartState(new(res.Keep(&record)))
	if err != nil {
		t.Fatal(err)
	}
	got, err := compile.EncodeStartState(r.StartState)
	if err != nil || string(got) != string(want) {
		t.Errorf("the run's start state is %s (%v), want %s", got, err, want)
	}
	if !slices.Contains(r.StartState.Run.Secrets, "inputs.pw") || r.StartState.Run.Inputs.Get().(map[string]any)["pw"] != nil {
		t.Errorf("run state holds %v", r.StartState.Run)
	}
	// The run's start queue was the shared one, which the coordinator
	// keeps working.
	c.mu.Lock()
	queues := len(c.queues)
	c.mu.Unlock()
	if queues != 0 {
		t.Errorf("the coordinator works %d start queues", queues)
	}
}

// TestStartKeeps checks what startRun stores: the record sealed again,
// with the next generation, holding the private part of what Start gave.
func TestStartKeeps(t *testing.T) {
	parallel(t)
	c := open(t, Options{Variables: regionVars})
	id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	if _, err := c.startRun(t.Context(), id, nil); err != nil {
		t.Fatal(err)
	}
	r := storedRun(t, c, id)
	rec := storedRecord(t, c, id)
	if r.State != store.RunRunning || r.RecordGeneration != 2 || rec == nil || rec.Run == nil {
		t.Fatalf("a run started is %s, record generation %d, holding %+v", r.State, r.RecordGeneration, rec)
	}
	if rec.Run.Vars["REGION"] != "us" {
		t.Errorf("the record holds the variables %v", rec.Run.Vars)
	}
	if inputs, _ := rec.Run.Inputs.Get().(map[string]any); inputs["pw"] != "HUNTER2SECRET" {
		t.Errorf("the record holds the inputs %v", inputs)
	}
	if _, isState := r.StartState.Run.Inputs.Get().(map[string]any)["region"]; isState {
		t.Error("run state holds the region, which a variable made")
	}
}

// failingVariables fails as many times as fail says, and then gives vars.
type failingVariables struct {
	vars  conformancetest.Variables
	fail  int
	calls atomic.Int64
}

func (v *failingVariables) Read(ctx context.Context, names []string, secrets bool) (map[string]compile.Variable, error) {
	if v.calls.Add(1) <= int64(v.fail) {
		return nil, errors.New("the variables can't be reached")
	}
	return v.vars.Read(ctx, names, secrets)
}

// TestStartFails checks each way Start fails a run: the run ends failed,
// with a diagnostic that says why, and without its secret record.
func TestStartFails(t *testing.T) {
	parallel(t)
	for _, tc := range []struct {
		name string
		opts Options
		// workflow is regionWorkflow unless it says otherwise.
		workflow string
		// before changes the run, or what the coordinator works with,
		// once the run is stored.
		before func(t *testing.T, c *Coordinator, id string, keys *coordtest.Keys)
		// change changes what Create gave before the run is stored.
		change func(*compile.Created)
		// createVars are the variables Create reads, regionVars unless
		// it says otherwise.
		createVars conformancetest.Variables
		code       problem.Code
		tries      int
	}{
		{
			name: "Start's own problem", workflow: `
on:
  execute:
    inputs:
      debug:
        type: boolean
        default: false
        ignore: ${{ org.ENV == 'prod' }}
jobs:
  main:
    steps:
      - run: echo ok
`, createVars: conformancetest.Variables{"org.ENV": {Value: "prod"}},
			opts: Options{Variables: conformancetest.Variables{"org.ENV": {Value: "dev"}}}, code: problems.RunShapeChanged.Code(),
		},
		{name: "a record of another plan", change: func(cr *compile.Created) { cr.Record.PlanDigest = "sha256:00" }, code: problems.RunRecordMismatch.Code()},
		{name: "a record changed", before: func(t *testing.T, c *Coordinator, id string, _ *coordtest.Keys) {
			exec(t, c, "UPDATE meridian_records SET sealed = x'01000000000000000000000000000000000000000000'")
		}, code: problems.RecordUnreadable.Code()},
		{name: "a record older than the run's", before: func(t *testing.T, c *Coordinator, _ string, _ *coordtest.Keys) {
			// The run's generation is the record's next: the record is one
			// that a later one replaced, put back.
			exec(t, c, "UPDATE meridian_runs SET record_gen = 2")
		}, code: problems.RecordUnreadable.Code()},
		{name: "a problem of Keys'", before: func(_ *testing.T, _ *Coordinator, _ string, keys *coordtest.Keys) {
			keys.Fail(nil, problems.KeyFileMissing.Problem(problems.Params{"path": "/keys/secret-record.key"}))
		}, code: problems.KeyFileMissing.Code()},
		{name: "a plan that doesn't decode", before: func(t *testing.T, c *Coordinator, _ string, _ *coordtest.Keys) {
			exec(t, c, "DROP TRIGGER meridian_runs_plan_fixed")
			exec(t, c, `UPDATE meridian_runs SET plan = CAST('{"version":1}' AS BLOB)`)
		}, code: problems.RunMalformed.Code()},
		{
			name: "variables that can't be read", opts: Options{Variables: &failingVariables{vars: regionVars, fail: 1 << 30}},
			code: problems.RunStartUnanswered.Code(), tries: fastStart.tries,
		},
		{name: "a Start the coordinator can't run", before: func(t *testing.T, c *Coordinator, _ string, _ *coordtest.Keys) {
			exec(t, c, "DELETE FROM meridian_records")
		}, code: problems.RunNotStarted.Code()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys := coordtest.NewKeys(t)
			opts := tc.opts
			opts.Keys = keys
			if opts.Variables == nil {
				opts.Variables = regionVars
			}
			c := open(t, opts)
			workflow := tc.workflow
			if workflow == "" {
				workflow = regionWorkflow
			}
			createVars := tc.createVars
			if createVars == nil {
				createVars = regionVars
			}
			created, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(workflow), regionSubmitted,
				compile.Options{Source: plan.Source{Location: "workflow.yaml"}, Contexts: createVars.Contexts()})
			if err != nil || diags.HasErrors() {
				t.Fatalf("Create = %v, %v", diags, err)
			}
			if tc.change != nil {
				tc.change(created)
			}
			id, err := c.CreateRun(t.Context(), created, runOptions)
			if err != nil {
				t.Fatal(err)
			}
			if tc.before != nil {
				tc.before(t, c, id, keys)
			}
			serve(t, c)
			r := waitEnded(t, c, id)
			if r.State != store.RunEnded || r.Outcome != plan.OutcomeFailure {
				t.Errorf("the run is %s, %s, want ended with failure", r.State, r.Outcome)
			}
			if !slices.ContainsFunc(r.Diagnostics, func(d problems.Diagnostic) bool { return d.Code == tc.code }) {
				t.Errorf("the run's diagnostics are %v, want one of kind %s", r.Diagnostics, tc.code)
			}
			if r.StartTries != tc.tries {
				t.Errorf("the coordinator tried to start the run %d times, want %d", r.StartTries, tc.tries)
			}
			if storedRecord(t, c, id) != nil {
				t.Error("the run's secret record is there once it has ended")
			}
		})
	}
}

// exec runs a statement on c's store, as a test changes what it holds.
func exec(t *testing.T, c *Coordinator, query string) {
	t.Helper()
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		_, err := tx.SQL().ExecContext(t.Context(), query)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// TestStartTriesAgain checks that a run whose variables can't be read at
// first starts once they can, after as many tries as it took.
func TestStartTriesAgain(t *testing.T) {
	parallel(t)
	c := open(t, Options{Variables: &failingVariables{vars: regionVars, fail: 2}})
	id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	serve(t, c)
	r := waitEnded(t, c, id)
	if r.Outcome != plan.OutcomeSuccess || r.StartTries != 2 {
		t.Errorf("the run ended with %s after %d tries, want success after 2", r.Outcome, r.StartTries)
	}
}

// TestPlanNewer checks that a run whose plan only a newer release reads is
// left as it is, as the coordinator says in its log.
func TestPlanNewer(t *testing.T) {
	parallel(t)
	log := &logRecords{}
	c := open(t, Options{Variables: regionVars, Log: slog.New(log)})
	id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	exec(t, c, "DROP TRIGGER meridian_runs_plan_fixed")
	exec(t, c, fmt.Sprintf("UPDATE meridian_runs SET plan_version = %d", plan.Version+1))
	serve(t, c)
	waitFor(t, func() bool { return log.has(string(problems.RunPlanNewer.Code())) })
	// Long enough for a few more tries, which leave it as it is too.
	time.Sleep(4 * fastStart.newer) //nolint:forbidigo // what the test waits for is that nothing happens
	if r := storedRun(t, c, id); r.State != store.RunCreated || r.StartTries != 0 || len(r.Diagnostics) != 0 {
		t.Errorf("the run is %s, after %d tries, with %v", r.State, r.StartTries, r.Diagnostics)
	}
	if storedRecord(t, c, id) == nil {
		t.Error("the run's secret record is gone")
	}
}

// waitFor waits until ok, for 10 seconds at most.
func waitFor(t testing.TB, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !ok(); {
		if time.Now().After(deadline) {
			t.Fatal("it never happened")
		}
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo // what is waited for has no signal
	}
}

// TestStartQueue checks that a run whose Start is in a queue of its own is
// started only by the coordinator that created it, which works the queue
// until it has started, and not by another of the store's.
func TestStartQueue(t *testing.T) {
	parallel(t)
	path := filepath.Join(coordtest.PrivateDir(t), "meridian.db")
	keys := coordtest.NewKeys(t)
	ownQueue := func(id string) string { return "meridian.start." + id }
	creator := open(t, Options{Database: path, Keys: keys, Variables: regionVars, StartQueue: ownQueue})
	other := open(t, Options{Database: path, Keys: keys, Variables: regionVars})
	id, _ := create(t, creator, regionWorkflow, regionSubmitted, regionVars)

	serve(t, other)
	// Long enough for the other coordinator to have started it, if it
	// could: ten polls of its queues.
	time.Sleep(10 * testPollInterval) //nolint:forbidigo // what the test waits for is that nothing happens
	if r := storedRun(t, creator, id); r.State != store.RunCreated {
		t.Fatalf("a coordinator that didn't create the run started it: %s", r.State)
	}

	serve(t, creator)
	if r := waitEnded(t, creator, id); r.Outcome != plan.OutcomeSuccess {
		t.Errorf("the run ended with %s", r.Outcome)
	}
	waitFor(t, func() bool {
		creator.mu.Lock()
		defer creator.mu.Unlock()
		return len(creator.queues) == 0
	})
}

// TestStartOnce checks that a run's Start, run at once by attempts that
// each think they hold it, as a Start hopper rescued from a replica still
// running it, stores what it gave once.
func TestStartOnce(t *testing.T) {
	parallel(t)
	c := open(t, Options{Variables: regionVars})
	id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	var (
		wg      sync.WaitGroup
		started atomic.Int64
	)
	for range 4 {
		wg.Go(func() {
			_, err := c.startRun(t.Context(), id, nil)
			switch {
			case err == nil:
				started.Add(1)
			case !errors.Is(err, errSettled):
				t.Error(err)
			}
		})
	}
	wg.Wait()
	r := storedRun(t, c, id)
	if started.Load() != 1 || r.State != store.RunRunning || r.RecordGeneration != 2 {
		t.Errorf("%d attempts started the run, which is %s, record generation %d", started.Load(), r.State, r.RecordGeneration)
	}
	// A run that has started neither starts again nor ends as one that
	// didn't.
	if _, err := c.startRun(t.Context(), id, nil); !errors.Is(err, errSettled) {
		t.Errorf("startRun of a run started = %v, want errSettled", err)
	}
	failed := problems.Diagnostics{diagnostic(problems.RunRecordMismatch, problems.Params{"run": id})}
	if err := c.endStart(t.Context(), id, failed, nil); !errors.Is(err, errSettled) {
		t.Errorf("endStart of a run started = %v, want errSettled", err)
	}
	// The finisher ends it once, failing its job, which no supervisor
	// ended; a second does nothing.
	noop := func(context.Context, *store.Tx) error { return nil }
	for range 2 {
		if _, err := c.finishRun(t.Context(), id, noop); err != nil {
			t.Fatal(err)
		}
	}
	if r := storedRun(t, c, id); r.State != store.RunEnded || r.Outcome != plan.OutcomeFailure || len(r.Diagnostics) != 0 {
		t.Errorf("the run is %s, %s, with %v", r.State, r.Outcome, r.Diagnostics)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeFailure || !hasCode(j.Diagnostics, problems.CoordinatorJobFailed) {
		t.Errorf("the job no supervisor ended is %s, with %v", j.Outcome, j.Diagnostics)
	}
}

// storedJob returns the run id's job key, as the store holds it.
func storedJob(t *testing.T, c *Coordinator, id, key string) *store.Job {
	t.Helper()
	var j *store.Job
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		j, err = tx.Job(id, key)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return j
}

// TestCreateRunRollsBack checks that a run that can't be stored whole
// isn't stored at all: neither the run, nor its record, nor its hopper
// jobs, and its number is given to the next.
func TestCreateRunRollsBack(t *testing.T) {
	parallel(t)
	keys := coordtest.NewKeys(t)
	c := open(t, Options{Keys: keys, Variables: regionVars})
	created, _, err := compile.Create(t.Context(), "workflow.yaml", []byte(regionWorkflow), regionSubmitted,
		compile.Options{Source: plan.Source{Location: "workflow.yaml"}, Contexts: regionVars.Contexts()})
	if err != nil {
		t.Fatal(err)
	}
	keys.Fail(errors.New("the key service can't be reached"), nil)
	if _, err := c.CreateRun(t.Context(), created, runOptions); err == nil {
		t.Fatal("CreateRun succeeded without Keys")
	}
	jobs := 0
	for _, err := range c.inserter.Jobs(t.Context(), hopper.JobFilter{}) {
		if err != nil {
			t.Fatal(err)
		}
		jobs++
	}
	if jobs != 0 {
		t.Errorf("a run not created left %d hopper jobs", jobs)
	}
	keys.Fail(nil, nil)
	if id, err := c.CreateRun(t.Context(), created, runOptions); err != nil || id != "1" {
		t.Errorf("CreateRun = %q, %v; want 1", id, err)
	}
	for _, opts := range []RunOptions{
		{RunDir: runOptions.RunDir},
		{DefaultGroup: "local"},
	} {
		if _, err := c.CreateRun(t.Context(), created, opts); err == nil {
			t.Errorf("CreateRun with %+v succeeded", opts)
		}
	}
	if _, err := c.CreateRun(t.Context(), nil, runOptions); err == nil {
		t.Error("CreateRun of nothing succeeded")
	}
}

// groups has the group local alone.
type groups struct{}

func (groups) Group(_ context.Context, name string) (*backend.Group, error) {
	if name != "local" {
		return nil, compile.ErrNotFound
	}
	return &backend.Group{Name: name}, nil
}

func (groups) Environment(context.Context, plan.Environment) (*backend.Group, error) {
	return nil, compile.ErrNotFound
}

func (groups) ComputeTarget(context.Context, plan.ComputeTarget) (*backend.Group, error) {
	return nil, compile.ErrNotFound
}

func TestDefaultGroupUnknown(t *testing.T) {
	parallel(t)
	c := open(t, Options{Variables: regionVars, Groups: groups{}})
	created, _, err := compile.Create(t.Context(), "workflow.yaml", []byte(regionWorkflow), regionSubmitted,
		compile.Options{Source: plan.Source{Location: "workflow.yaml"}, Contexts: regionVars.Contexts()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.CreateRun(t.Context(), created, RunOptions{DefaultGroup: "gpu", RunDir: runOptions.RunDir})
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.GroupUnknown.Code() || p.Params["group"] != "gpu" {
		t.Errorf("CreateRun with an unknown group = %v", err)
	}
	if _, err := c.CreateRun(t.Context(), created, runOptions); err != nil {
		t.Errorf("CreateRun with a known group = %v", err)
	}
}
