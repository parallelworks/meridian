package coordinator

import (
	"maps"
	"testing"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// TestStepRunEnd checks how a step run's end is its step's try's: once it
// has ended, its outcome, and what its step wrote to $OUTPUTS, as its job's
// result holds it, for one that succeeded; and none for one that failed,
// as format 1 copies the file back only from a try that succeeded.
func TestStepRunEnd(t *testing.T) {
	// Its subtests hold the places, which it would hold one of otherwise
	// as they wait (parallel).
	t.Parallel()
	written := map[string]string{"RUNDIR": "/home/alice/work", "TOKEN": "t0k3n"}
	for _, outcome := range []plan.Outcome{plan.OutcomeSuccess, plan.OutcomeFailure} {
		t.Run(string(outcome), func(t *testing.T) {
			parallel(t)
			r := newStepRunRigWith(t, func(backend.PlaceRequest) testEnd {
				return testEnd{result: plan.JobResult{Outcome: outcome, Outputs: maps.Clone(written), Private: []string{"TOKEN"}}}
			})
			id, err := r.ask(t, r.build, stepRunRequest{Step: 2, SSH: farm})
			if err != nil {
				t.Fatal(err)
			}
			var end *stepRunEnd
			waitFor(t, func() bool {
				end, err = r.c.stepRunEnd(t.Context(), id)
				return err != nil || end.Ended
			})
			switch {
			case err != nil:
				t.Fatal(err)
			case end.Outcome != outcome:
				t.Errorf("the step run ended %s", end.Outcome)
			case outcome == plan.OutcomeSuccess && (!maps.Equal(end.Outputs, written) || len(end.Private) != 1 || end.Private[0] != "TOKEN"):
				t.Errorf("the step run that succeeded gives the outputs %v, private %v", end.Outputs, end.Private)
			case outcome != plan.OutcomeSuccess && end.Outputs != nil:
				t.Errorf("the step run that failed gives the outputs %v", end.Outputs)
			}
		})
	}
}

// TestStepRunStops checks the stops of a step run: its step's own, as the
// worker API's stop of a child makes it, of a run whose store holds one
// job of its plan's several; a stop of its caller's job, which a step
// run of the step's cleanup outlives; and the end of its caller's
// attempt, which stops both, in the transaction that ends it.
func TestStepRunStops(t *testing.T) {
	parallel(t)
	r := newStepRunRig(t)
	ctx := t.Context()
	own, err := r.ask(t, r.build, stepRunRequest{Step: 2, SSH: farm})
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := r.ask(t, r.build, stepRunRequest{Step: 2, Cleanup: true, SSH: farm})
	if err != nil || cleanup == own {
		t.Fatalf("the step's cleanup's step run is %q, %v", cleanup, err)
	}
	if again, err := r.ask(t, r.build, stepRunRequest{Step: 2, Cleanup: true, SSH: farm}); err != nil || again != cleanup {
		t.Errorf("asked again, the cleanup's step run is %q, %v", again, err)
	}
	if _, err := r.ask(t, r.build, stepRunRequest{Step: 1, Cleanup: true, SSH: farm}); err == nil {
		t.Error("a step without a cleanup: has a cleanup's step run")
	}
	other, err := r.ask(t, r.build, stepRunRequest{Step: 1, SSH: farm})
	if err != nil {
		t.Fatal(err)
	}
	r.placed(t, other, "build")
	if err := (childAPI{r.c}).StopChild(ctx, other, plan.ReasonTimeout); err != nil {
		t.Fatal(err)
	}
	if run := storedRun(t, r.c, other); run.StoppedAt.IsZero() || run.StopReason != plan.ReasonTimeout {
		t.Errorf("the step run its step stopped is stopped at %v, for %q", run.StoppedAt, run.StopReason)
	}

	// A stop of the caller's job stops the try's step run, but not the
	// cleanup's.
	root := storedRun(t, r.c, r.root)
	if err := r.c.store.Tx(ctx, func(tx *store.Tx) error {
		_, err := r.c.stopStepChildrenTx(ctx, tx, root, r.build, plan.ReasonCancelled)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if run := storedRun(t, r.c, own); run.StoppedAt.IsZero() {
		t.Error("a stop of its job didn't stop the step's step run")
	}
	if run := storedRun(t, r.c, cleanup); !run.StoppedAt.IsZero() {
		t.Error("a stop of its job stopped the step's cleanup's step run")
	}
	// Stopped, a step run outlives its stop as its step does, and the
	// cleanup's always.
	for id, want := range map[string]bool{own: false, cleanup: true} {
		if got, err := r.c.stepRunOutlives(storedRun(t, r.c, id)); err != nil || got != want {
			t.Errorf("step run %s outlives its stop: %v, %v", id, got, err)
		}
	}

	// The end of the caller's attempt stops the cleanup's too.
	if err := r.c.store.Tx(ctx, func(tx *store.Tx) error {
		if err := tx.EndAttempt(r.build.ID, store.AttemptEnd{Kind: store.EndLost, Reason: plan.ReasonWorkerLost}); err != nil {
			return err
		}
		run, err := tx.Run(cleanup)
		if err == nil && run.StoppedAt.IsZero() {
			t.Error("the end of its caller's attempt didn't stop the cleanup's step run")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// TestStepRunsAcrossHosts checks that two jobs, each on its own host, each
// with a step on the other's host, don't wait on each other where each
// member is past its capacity: neither backend queues a job, so each step
// run is placed on the busy member, as any job is.
func TestStepRunsAcrossHosts(t *testing.T) {
	parallel(t)
	d := newTestDriver()
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	d.worker = func(backend.PlaceRequest) testEnd {
		return testEnd{wait: hold, result: plan.JobResult{Outcome: plan.OutcomeSuccess}}
	}
	d.probe = func(*backend.Member) (backend.Fit, error) { return backend.Fit{Availability: backend.HasCapacity}, nil }
	c := open(t, Options{Drivers: map[string]backend.Driver{testBackend: d, "ssh": d}})
	created, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(`jobs:
  east:
    ssh: {remoteHost: east}
    steps:
      - run: echo west
        ssh: {remoteHost: west}
  west:
    ssh: {remoteHost: west}
    steps:
      - run: echo east
        ssh: {remoteHost: east}
`), nil, compile.Options{Source: plan.Source{Location: "workflow.yaml"}})
	if err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	root, err := c.CreateRun(t.Context(), created, runOptions)
	if err != nil {
		t.Fatal(err)
	}
	serve(t, c)
	r := &stepRunRig{c: c, d: d, root: root}
	host := func(name string) *plan.Inline {
		return &plan.Inline{Name: "ssh", Backend: "ssh", Isolation: "host", SSH: plan.DataOf(map[string]any{"remoteHost": name})}
	}
	for job, other := range map[string]string{"east": "west", "west": "east"} {
		id, err := r.ask(t, r.placed(t, root, job), stepRunRequest{SSH: host(other)})
		if err != nil {
			t.Fatal(err)
		}
		if a := r.placed(t, id, job); a.Placement.Backend != "ssh" {
			t.Errorf("%s's step run was placed on %+v", job, a.Placement)
		}
	}
}

// TestStepRunOneTry checks that a step run is one try of its step: one
// whose setup fails ends failed at once, rather than be placed again, as
// format 1's try fails when it can't connect, and its caller's step's
// retry: is what tries again, with a step run of the next try.
func TestStepRunOneTry(t *testing.T) {
	parallel(t)
	r := newStepRunRigWith(t, func(backend.PlaceRequest) testEnd {
		return testEnd{problem: problems.SSHUnreachable.Problem(problems.Params{"host": "farm", "reason": "refused"})}
	})
	id, err := r.ask(t, r.build, stepRunRequest{Step: 2, SSH: farm})
	if err != nil {
		t.Fatal(err)
	}
	var end *stepRunEnd
	waitFor(t, func() bool {
		end, err = r.c.stepRunEnd(t.Context(), id)
		return err != nil || end.Ended
	})
	if err != nil || end.Outcome != plan.OutcomeFailure {
		t.Fatalf("the step run whose setup failed ended %+v, %v", end, err)
	}
	var failures []*store.Attempt
	if err := r.c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		failures, err = tx.SetupFailures(id, "build")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(failures) != 1 {
		t.Errorf("the step run was placed %d times", len(failures))
	}

	// The step's next try is a step run of its own, once the one before
	// has failed, on the host its retry counter names then.
	hostOf := func(name string) *plan.Inline {
		return &plan.Inline{Name: "ssh", Backend: "ssh", Isolation: "host", SSH: plan.DataOf(map[string]any{"remoteHost": name})}
	}
	first, err := r.ask(t, r.build, stepRunRequest{Step: 8, SSH: hostOf("farm0")})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		end, err := r.c.stepRunEnd(t.Context(), first)
		return err != nil || end.Ended
	})
	if second, err := r.ask(t, r.build, stepRunRequest{Step: 8, Try: 1, SSH: hostOf("farm1")}); err != nil || second == first {
		t.Errorf("the second try's step run, once the first failed: %q, %v", second, err)
	}
}
