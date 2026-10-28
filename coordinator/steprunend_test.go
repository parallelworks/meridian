package coordinator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// TestStepRunEnd checks how a step run's end is its step's try's: once it
// has ended, its outcome, and what its step wrote to $OUTPUTS, as its job's
// result holds it, with which are private and which secret, for one that
// succeeded; and none for one that failed, as format 1 copies the file
// back only from a try that succeeded.
func TestStepRunEnd(t *testing.T) {
	// Its subtests hold the places, which it would hold one of otherwise
	// as they wait (parallel).
	t.Parallel()
	written := map[string]string{"RUNDIR": "/home/alice/work", "TOKEN": "t0k3n", "KEY": "k3y"}
	for _, outcome := range []plan.Outcome{plan.OutcomeSuccess, plan.OutcomeFailure} {
		t.Run(string(outcome), func(t *testing.T) {
			parallel(t)
			r := newStepRunRigWith(t, func(backend.PlaceRequest) testEnd {
				return testEnd{result: plan.JobResult{Outcome: outcome, Outputs: maps.Clone(written), Private: []string{"TOKEN"}, Secrets: []string{"KEY"}}}
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
			case outcome == plan.OutcomeSuccess && (!maps.Equal(end.Outputs, written) || !slices.Equal(end.Private, []string{"TOKEN"}) || !slices.Equal(end.Secrets, []string{"KEY"})):
				t.Errorf("the step run that succeeded gives the outputs %v, private %v, secret %v", end.Outputs, end.Private, end.Secrets)
			case outcome != plan.OutcomeSuccess && end.Outputs != nil:
				t.Errorf("the step run that failed gives the outputs %v", end.Outputs)
			}
		})
	}
}

// TestStepRunStops checks that a stop that reaches a step run reaches its
// worker, in the transaction that stores it, whatever its step's if:
// says: its step's own, its timeout among them; a person's cancel of it;
// a stop of its caller's job, which the step runs of a step's cleanup and
// of a step that outlives its job's cancel outlive, and after which only
// those are made; and a stop of its caller's run, which they outlive too.
func TestStepRunStops(t *testing.T) {
	parallel(t)
	r := newStepRunRig(t)
	ctx := t.Context()
	ask := func(a *store.Attempt, req stepRunRequest) string {
		t.Helper()
		id, err := r.ask(t, a, req)
		if err != nil {
			t.Fatal(err)
		}
		r.placed(t, id, a.Job)
		return id
	}
	own := ask(r.build, stepRunRequest{Step: 2, SSH: farm})
	always := ask(r.build, stepRunRequest{Step: 10, SSH: farm})
	other := ask(r.build, stepRunRequest{Step: 1, SSH: farm})
	retried := ask(r.build, stepRunRequest{Step: 8, SSH: &plan.Inline{Name: "ssh", Backend: "ssh", Isolation: "host", SSH: plan.DataOf(map[string]any{"remoteHost": "farm0"})}})
	hosted := ask(r.hosted, stepRunRequest{Step: 0})

	// Its step's own stop.
	if err := (childAPI{r.c}).StopChild(ctx, other, plan.ReasonCancelled); err != nil {
		t.Fatal(err)
	}
	r.told(t, other, "build", plan.ReasonCancelled)
	// A person's cancel of it.
	if err := r.c.Cancel(ctx, retried); err != nil {
		t.Fatal(err)
	}
	r.told(t, retried, "build", plan.ReasonCancelled)

	// A stop of its caller's job.
	if err := r.c.tell(ctx, storedRun(t, r.c, r.root), r.build, plan.ReasonCancelled); err != nil {
		t.Fatal(err)
	}
	r.told(t, own, "build", plan.ReasonCancelled)
	r.untold(t, always, "build")
	if _, err := r.ask(t, r.build, stepRunRequest{Step: 4, SSH: farm}); !errors.As(err, new(*stepStoppedError)) {
		t.Errorf("a step of a job that was stopped gets a step run: %v", err)
	}
	cleanup := ask(r.build, stepRunRequest{Step: 2, Cleanup: true, SSH: farm})
	if cleanup == own {
		t.Fatal("the step's cleanup's step run is its try's")
	}
	if again, err := r.ask(t, r.build, stepRunRequest{Step: 2, Cleanup: true, SSH: farm}); err != nil || again != cleanup {
		t.Errorf("asked again, the cleanup's step run is %q, %v", again, err)
	}
	if _, err := r.ask(t, r.build, stepRunRequest{Step: 1, Cleanup: true, SSH: farm}); !errors.Is(err, errNotStepRun) {
		t.Errorf("a step without a cleanup: has a cleanup's step run: %v", err)
	}

	// A stop of its caller's run.
	if err := r.c.Cancel(ctx, r.root); err != nil {
		t.Fatal(err)
	}
	r.told(t, hosted, "hosted", plan.ReasonCancelled)
	r.untold(t, always, "build")
	r.untold(t, cleanup, "build")

	// Its step's timeout, whatever its if: says.
	if err := (childAPI{r.c}).StopChild(ctx, always, plan.ReasonTimeout); err != nil {
		t.Fatal(err)
	}
	r.told(t, always, "build", plan.ReasonTimeout)
}

// TestStepRunCallerEnds checks that the end of its caller's attempt stops
// each of its step runs, a cleanup's and one of a step that outlives its
// job's cancel among them, and tells their workers, in the transaction
// that ends it.
func TestStepRunCallerEnds(t *testing.T) {
	parallel(t)
	r := newStepRunRig(t)
	var ids []string
	for _, req := range []stepRunRequest{{Step: 2, SSH: farm}, {Step: 2, Cleanup: true, SSH: farm}, {Step: 10, SSH: farm}} {
		id, err := r.ask(t, r.build, req)
		if err != nil {
			t.Fatal(err)
		}
		r.placed(t, id, "build")
		ids = append(ids, id)
	}
	if err := r.c.store.Tx(t.Context(), func(tx *store.Tx) error {
		return tx.EndAttempt(r.build.ID, store.AttemptEnd{Kind: store.EndLost, Reason: plan.ReasonWorkerLost})
	}); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		r.told(t, id, "build", plan.ReasonCancelled)
	}
}

// TestStepRunStoppedUnplaced checks that a step run stopped once its
// supervisor has looked for where to place it, and before it placed it,
// is never placed: its job ends cancelled, as a job its run's stop ends
// between attempts does.
func TestStepRunStoppedUnplaced(t *testing.T) {
	parallel(t)
	r := newStepRunRig(t)
	asked := make(chan string, 1)
	var once sync.Once
	r.d.set(func(d *testDriver) {
		d.probe = func(m *backend.Member) (backend.Fit, error) {
			// The step run is placed on farm, through ssh; the root's jobs
			// still being placed elsewhere are let be.
			if m.Name == "ssh" {
				once.Do(func() { stopRunning(t, r.c, <-asked) })
			}
			return backend.Fit{Availability: backend.StartsNow}, nil
		}
	})
	id, err := r.ask(t, r.build, stepRunRequest{Step: 2, SSH: farm})
	if err != nil {
		t.Fatal(err)
	}
	asked <- id
	if run := waitEnded(t, r.c, id); run.Outcome != plan.OutcomeCancelled {
		t.Errorf("the step run ended %s", run.Outcome)
	}
	if j := storedJob(t, r.c, id, "build"); j.Outcome != plan.OutcomeCancelled || j.Attempts != 0 {
		t.Errorf("the step run's job is %+v", j)
	}
}

// TestStepRunCleanupOfTry checks that a step's cleanup has one step run,
// of try 0, however many tries the step had: one of a later try is
// refused, though the step has that try, and the try before it has ended.
func TestStepRunCleanupOfTry(t *testing.T) {
	parallel(t)
	r := newStepRunRig(t)
	first, err := r.ask(t, r.build, stepRunRequest{Step: 11, SSH: farm})
	if err != nil {
		t.Fatal(err)
	}
	r.placed(t, first, "build")
	r.endStepRun(t, first, plan.OutcomeFailure)
	if _, err := r.ask(t, r.build, stepRunRequest{Step: 11, Try: 1, Cleanup: true, SSH: farm}); !errors.Is(err, errNotStepRun) {
		t.Errorf("a cleanup of the step's second try: %v", err)
	}
	if _, err := r.ask(t, r.build, stepRunRequest{Step: 11, Try: 1, SSH: farm}); err != nil {
		t.Errorf("the step's second try: %v", err)
	}
}

// stopRunning stops the step run id, as its step's stop does, once it is
// running, as it is once its supervisor looks: one stopped before is
// ended as a run that never started is. It runs where the driver probes,
// which isn't the test's goroutine, so it fails the test, but doesn't
// end it.
func stopRunning(t *testing.T, c *Coordinator, id string) {
	ctx := context.WithoutCancel(t.Context())
	for {
		var run *store.Run
		if err := c.store.View(ctx, func(tx *store.Tx) error {
			var err error
			run, err = tx.Run(id)
			return err
		}); err != nil {
			t.Error(err)
			return
		}
		if run.State != store.RunCreated {
			break
		}
		select {
		case <-t.Context().Done():
			return
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := (childAPI{c}).StopChild(ctx, id, plan.ReasonCancelled); err != nil {
		t.Error(err)
	}
}

// TestStepRunLost checks that a step run whose worker is lost ends failed,
// worker_lost, rather than run again, though its caller's job's retry:
// lists worker-lost: it is one try of its step, which its caller's step's
// retry: tries again.
func TestStepRunLost(t *testing.T) {
	parallel(t)
	r := newStepRunRig(t)
	id, err := r.ask(t, r.placed(t, r.root, "failover"), stepRunRequest{SSH: farm})
	if err != nil {
		t.Fatal(err)
	}
	a := r.placed(t, id, "failover")
	if err := r.c.store.Tx(t.Context(), func(tx *store.Tx) error {
		return tx.EndAttempt(a.ID, store.AttemptEnd{Kind: store.EndLost, Reason: plan.ReasonWorkerLost})
	}); err != nil {
		t.Fatal(err)
	}
	run, job, lost := storedRun(t, r.c, id), storedJob(t, r.c, id, "failover"), latestAttempt(t, r.c, id, "failover")
	if _, err := r.c.workerLost(t.Context(), run, job, lost, func(context.Context, *store.Tx) error { return nil }); err != nil && !errors.Is(err, errSettled) {
		t.Fatal(err)
	}
	if j := storedJob(t, r.c, id, "failover"); j.Outcome != plan.OutcomeFailure || !slices.Equal(jobCodes(j), []string{"worker_lost"}) {
		t.Errorf("the step run whose worker was lost: %+v", j)
	}
}

// TestStepRunExitCode checks that a step run's end gives the exit status
// its worker reported for the step it runs, and a cleanup's step run's
// for the step's cleanup.
func TestStepRunExitCode(t *testing.T) {
	parallel(t)
	r := newStepRunRigWith(t, func(backend.PlaceRequest) testEnd { return testEnd{running: true} })
	for cleanup, code := range map[bool]int{false: 3, true: 5} {
		id, err := r.ask(t, r.build, stepRunRequest{Step: 2, Cleanup: cleanup, SSH: farm})
		if err != nil {
			t.Fatal(err)
		}
		var a *store.Attempt
		waitFor(t, func() bool {
			a, err = latestAttemptOf(t, r.c, id, "build")
			return err == nil && a.State == store.AttemptRunning
		})
		// The other's, which the worker doesn't run, comes last.
		events := []workerapi.Event{
			{Seq: 1, Step: 2, Cleanup: cleanup},
			{Seq: 2, Finished: true, Step: 2, Cleanup: cleanup, Outcome: plan.OutcomeFailure, ExitCode: code},
			{Seq: 3, Finished: true, Step: 2, Cleanup: !cleanup, Outcome: plan.OutcomeFailure, ExitCode: 9},
		}
		if err := r.c.store.Tx(t.Context(), func(tx *store.Tx) error {
			_, err := tx.AppendEvents(a.ID, events)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		r.endStepRun(t, id, plan.OutcomeFailure)
		if end, err := r.c.stepRunEnd(t.Context(), id); err != nil || end.ExitCode != code {
			t.Errorf("the step run, cleanup %v, ended %+v, %v", cleanup, end, err)
		}
	}
}

// TestStepRunStoppedSucceeds checks that a step run that was stopped,
// whose step succeeded all the same, gives no outputs: it ended
// cancelled, as its run did, whatever its job did.
func TestStepRunStoppedSucceeds(t *testing.T) {
	parallel(t)
	release := make(chan struct{})
	r := newStepRunRigWith(t, func(backend.PlaceRequest) testEnd {
		return testEnd{wait: release, result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": "1"}}}
	})
	id, err := r.ask(t, r.build, stepRunRequest{Step: 2, SSH: farm})
	if err != nil {
		t.Fatal(err)
	}
	r.placed(t, id, "build")
	if err := (childAPI{r.c}).StopChild(t.Context(), id, plan.ReasonCancelled); err != nil {
		t.Fatal(err)
	}
	close(release)
	var end *stepRunEnd
	waitFor(t, func() bool {
		end, err = r.c.stepRunEnd(t.Context(), id)
		return err != nil || end.Ended
	})
	if j := storedJob(t, r.c, id, "build"); j.Outcome != plan.OutcomeSuccess {
		t.Fatalf("the step run's job ended %s", j.Outcome)
	}
	if err != nil || end.Outcome != plan.OutcomeCancelled || end.Outputs != nil {
		t.Errorf("the stopped step run whose step succeeded ended %+v, %v", end, err)
	}
}

// told checks that the step run id is stopped, and that the worker of the
// latest attempt of its job key is told to stop, for reason.
func (r *stepRunRig) told(t *testing.T, id, key string, reason plan.Reason) {
	t.Helper()
	if run := storedRun(t, r.c, id); run.StoppedAt.IsZero() {
		t.Errorf("step run %s isn't stopped", id)
	}
	if ctl := controlOf(t, r.c, latestAttempt(t, r.c, id, key).ID); !ctl.Cancel || ctl.Reason != reason {
		t.Errorf("step run %s's worker is told %+v, not to stop for %q", id, ctl, reason)
	}
}

// untold checks that the step run id isn't stopped, and that the worker of
// the latest attempt of its job key isn't told to stop.
func (r *stepRunRig) untold(t *testing.T, id, key string) {
	t.Helper()
	if run := storedRun(t, r.c, id); !run.StoppedAt.IsZero() {
		t.Errorf("step run %s is stopped, for %q", id, run.StopReason)
	}
	if ctl := controlOf(t, r.c, latestAttempt(t, r.c, id, key).ID); ctl.Cancel {
		t.Errorf("step run %s's worker is told %+v", id, ctl)
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

// TestStepRunAPI checks the worker API's side of a step run:
//   - an ask makes it, with the job's state and masks, and is refused for a
//     state without a result for each step before the try's, or for every
//     step, for a cleanup's;
//   - its claim gives the step's try and the state, and the job with the
//     step at its own index, the steps before it by their id and path
//     alone, and none after it, without the step's cleanup: or the job's
//     outputs:; and of the run's values only what the step and the job's
//     env: read, never what another step does;
//   - the step evaluates from that claim alone, reading the steps before
//     it, the job's outputs, a variable and the job's status; and so does a
//     cleanup's step run's cleanup:, whose claim has every step so;
//   - its end gives what its step wrote; an ask for a step that can't have
//     one is refused, one whose host the coordinator can't evaluate ends
//     failed, with why, and one of a run that was stopped ends cancelled,
//     with its stop's reason.
func TestStepRunAPI(t *testing.T) {
	parallel(t)
	r := newStepRunRigWith(t, func(backend.PlaceRequest) testEnd {
		return testEnd{result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"RUNDIR": "/w"}}}
	})
	api := childAPI{r.c}
	ctx := t.Context()
	root := storedRun(t, r.c, r.root)
	state := workerapi.StepRunState{Steps: []workerapi.StepRunResult{
		{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": "1"}},
		{Outcome: plan.OutcomeFailure, Outputs: map[string]string{"y": "2"}},
	}}
	for name, st := range map[string]workerapi.StepRunState{
		"too few":  {Steps: state.Steps[:1]},
		"too many": {Steps: append(slices.Clone(state.Steps), workerapi.StepRunResult{Outcome: plan.OutcomeSuccess})},
	} {
		if _, err := api.StepRun(ctx, root, r.build, &workerapi.StepRunRequest{Step: 2, SSH: farm, State: st}); !errors.Is(err, workerapisrv.ErrNotStepRun) {
			t.Errorf("a state of %s steps' results: %v", name, err)
		}
	}
	sr, err := api.StepRun(ctx, root, r.build, &workerapi.StepRunRequest{Step: 2, SSH: farm, State: state, Masked: []string{"t0k3n-masked"}})
	if err != nil || sr.Run == "" {
		t.Fatalf("the ask gave %+v, %v", sr, err)
	}
	ready := r.readyStepRun(t, sr.Run)
	switch {
	case ready.StepRun == nil || ready.StepRun.Step != 2 || ready.StepRun.Try != 0 || ready.StepRun.Cleanup || len(ready.StepRun.State.Steps) != 2 || ready.StepRun.State.Steps[0].Outputs["x"] != "1":
		t.Errorf("the step run's claim gives %+v", ready.StepRun)
	case ready.Tree == nil || !slices.Contains(ready.Tree.Masked, "t0k3n-masked"):
		t.Errorf("the step run's claim masks %+v", ready.Tree)
	case !maps.Equal(ready.Run.Vars, map[string]string{"MINE": "mine", "JOBV": "jobv"}):
		t.Errorf("the step run's claim reads the variables %v", ready.Run.Vars)
	}
	checkStepRunJob(t, ready.Job, 2, 3, false)
	if rs, diags, err := ready.Job.Step(ctx, 2, stepRunJobState(ready)); err != nil || len(diags) > 0 || rs.Run != "echo there 1 2 mine true" {
		t.Errorf("the step evaluated from its step run's claim: %+v, %v, %v", rs, diags, err)
	}
	if callers, err := (claimReadier{r.c}).ReadyAttempt(ctx, root, r.build); err != nil || callers.StepRun != nil {
		t.Errorf("the caller's claim gives a step run: %+v, %v", callers, err)
	}

	p, err := r.c.plans.get(root)
	if err != nil {
		t.Fatal(err)
	}
	n := len(p.Job("build").Steps)
	all := make([]workerapi.StepRunResult, n)
	for i := range all {
		all[i] = workerapi.StepRunResult{Outcome: plan.OutcomeSuccess}
	}
	copy(all, state.Steps)
	all[2].Outputs = map[string]string{"z": "3"}
	if _, err := api.StepRun(ctx, root, r.build, &workerapi.StepRunRequest{Step: 2, Cleanup: true, SSH: farm, State: state}); !errors.Is(err, workerapisrv.ErrNotStepRun) {
		t.Errorf("a cleanup's state without every step's result: %v", err)
	}
	sr, err = api.StepRun(ctx, root, r.build, &workerapi.StepRunRequest{Step: 2, Cleanup: true, SSH: farm, State: workerapi.StepRunState{Steps: all}})
	if err != nil || sr.Run == "" {
		t.Fatalf("the ask for the cleanup's gave %+v, %v", sr, err)
	}
	cleanup := r.readyStepRun(t, sr.Run)
	if cleanup.StepRun == nil || cleanup.StepRun.Step != 2 || !cleanup.StepRun.Cleanup {
		t.Errorf("the cleanup's step run's claim gives %+v", cleanup.StepRun)
	}
	checkStepRunJob(t, cleanup.Job, 2, n, true)
	if rs, diags, err := cleanup.Job.Cleanup(ctx, 2, stepRunJobState(cleanup)); err != nil || len(diags) > 0 || rs.Run != "echo tidy 3 2" {
		t.Errorf("the cleanup evaluated from its step run's claim: %+v, %v, %v", rs, diags, err)
	}

	var end *workerapi.StepRun
	waitFor(t, func() bool {
		end, err = api.StepRunEnd(ctx, sr.Run)
		return err != nil || end.Ended
	})
	if err != nil || end.Outcome != plan.OutcomeSuccess || end.Outputs["RUNDIR"] != "/w" {
		t.Errorf("the step run's end is %+v, %v", end, err)
	}

	if _, err := api.StepRun(ctx, root, r.build, &workerapi.StepRunRequest{Step: 0, SSH: farm}); !errors.Is(err, workerapisrv.ErrNotStepRun) {
		t.Errorf("a step without ssh: gave %v", err)
	}
	seven := workerapi.StepRunState{Steps: all[:7]}
	if sr, err := api.StepRun(ctx, root, r.build, &workerapi.StepRunRequest{Step: 7, State: seven}); err != nil || !sr.Ended || sr.Outcome != plan.OutcomeFailure || len(sr.Diagnostics) != 1 || sr.Diagnostics[0].Code != problems.StepSSHReadsWorker.Code() {
		t.Errorf("a step whose host only its worker knows gave %+v, %v", sr, err)
	}
	// A step of a run that was stopped gets none, cancelled, with its
	// stop's reason.
	if err := r.c.stop(ctx, r.root, plan.ReasonTimeout); err != nil {
		t.Fatal(err)
	}
	one := workerapi.StepRunState{Steps: all[:1]}
	if sr, err := api.StepRun(ctx, root, r.build, &workerapi.StepRunRequest{Step: 1, SSH: farm, State: one}); err != nil || sr.Run != "" || !sr.Ended || sr.Outcome != plan.OutcomeCancelled || sr.Reason != plan.ReasonTimeout || sr.ExitCode != -1 {
		t.Errorf("a step of a stopped run gave %+v, %v", sr, err)
	}
}

// readyStepRun returns what the claim of the step run id's attempt has, once
// it is placed.
func (r *stepRunRig) readyStepRun(t *testing.T, id string) *workerapisrv.Ready {
	t.Helper()
	ready, err := claimReadier{r.c}.ReadyAttempt(t.Context(), storedRun(t, r.c, id), r.placedOrEnded(t, id, "build"))
	if err != nil {
		t.Fatal(err)
	}
	return ready
}

// stepRunJobState returns the state a step run's worker evaluates its step
// in, from its claim alone, ready.
func stepRunJobState(ready *workerapisrv.Ready) plan.JobState {
	st := ready.StepRun.State
	results := make([]plan.StepResult, len(st.Steps))
	for i, s := range st.Steps {
		results[i] = plan.StepResult{Outcome: s.Outcome, Outputs: s.Outputs, CleanupOutcome: s.CleanupOutcome, CleanupOutputs: s.CleanupOutputs}
	}
	return plan.JobState{
		Run: ready.Run, Needs: ready.Needs, Steps: results, Cancelled: st.Cancelled,
		FirstTry: &plan.ReadyStep{IfAlways: st.IfAlways}, Env: map[string]string{},
	}
}

// checkStepRunJob checks the job of a step run's claim, j: that it has n
// steps, without the job's outputs:, the one at index whole, with its
// cleanup: only for a cleanup's, and the rest by their id and path alone.
func checkStepRunJob(t *testing.T, j *plan.ReadyJob, index, n int, cleanup bool) {
	t.Helper()
	if len(j.Steps) != n || j.Outputs != nil {
		t.Errorf("the step run's claim's job has %d steps, and the outputs %v", len(j.Steps), j.Outputs)
		return
	}
	for i, s := range j.Steps {
		switch {
		case i == index && (s.Run.IsZero() || s.Cleanup.IsZero() == cleanup):
			t.Errorf("the step run's claim's step %d is %+v", i, s)
		case i != index && !reflect.DeepEqual(s, plan.Step{ID: s.ID, Path: s.Path}):
			t.Errorf("the step run's claim's step %d carries more than its id and path: %+v", i, s)
		case s.Path != fmt.Sprintf("/jobs/build/steps/%d", i):
			t.Errorf("the step run's claim's step %d is at %s", i, s.Path)
		}
	}
}

// placedOrEnded waits for the latest attempt of the run's job key to be
// placed, and returns it, however it has ended since.
func (r *stepRunRig) placedOrEnded(t *testing.T, run, key string) *store.Attempt {
	t.Helper()
	var a *store.Attempt
	waitFor(t, func() bool {
		return r.c.store.View(t.Context(), func(tx *store.Tx) error {
			var err error
			a, err = tx.LatestAttempt(run, key)
			return err
		}) == nil && a.Placed()
	})
	return a
}

// TestStepRunOutputsBound checks that a step run whose step wrote more to
// $OUTPUTS than a worker reads, as a step run's worker that doesn't keep
// to it reports, ends failed, with outputs_too_large at its step, and
// gives none of them.
func TestStepRunOutputsBound(t *testing.T) {
	parallel(t)
	large := map[string]string{"x": strings.Repeat("y", workerapi.MaxOutputs)}
	r := newStepRunRigWith(t, func(backend.PlaceRequest) testEnd {
		return testEnd{result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: large}}
	})
	api := childAPI{r.c}
	two := workerapi.StepRunState{Steps: []workerapi.StepRunResult{{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": "1"}}, {Outcome: plan.OutcomeSuccess}}}
	sr, err := api.StepRun(t.Context(), storedRun(t, r.c, r.root), r.build, &workerapi.StepRunRequest{Step: 2, SSH: farm, State: two})
	if err != nil || sr.Run == "" {
		t.Fatalf("the ask gave %+v, %v", sr, err)
	}
	var end *workerapi.StepRun
	waitFor(t, func() bool {
		end, err = api.StepRunEnd(t.Context(), sr.Run)
		return err != nil || end.Ended
	})
	switch {
	case err != nil:
		t.Fatal(err)
	case end.Outcome != plan.OutcomeFailure || end.Outputs != nil || len(end.Diagnostics) == 0:
		t.Errorf("the step run whose step wrote too much ended %+v", end)
	default:
		d := end.Diagnostics[len(end.Diagnostics)-1]
		if d.Code != problems.OutputsTooLarge.Code() || d.Path() != "/jobs/build/steps/2" || d.Params["size"] != workerapi.MaxOutputs+3 {
			t.Errorf("its problem is %+v", d)
		}
	}
}

// TestStepRunStateStrict checks that a step run whose secret record holds
// a state that isn't a step run's, as the worker API decodes one strictly,
// with a member it doesn't have, has no claim: its worker is never given
// more than a step run's state.
func TestStepRunStateStrict(t *testing.T) {
	parallel(t)
	r := newStepRunRig(t)
	state := plan.DataOf(map[string]any{"steps": []any{}, "env": map[string]any{"BASH_ENV": "/tmp/x"}})
	id, err := r.ask(t, r.build, stepRunRequest{Step: 2, SSH: farm, State: state})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (claimReadier{r.c}).ReadyAttempt(t.Context(), storedRun(t, r.c, id), r.placedOrEnded(t, id, "build")); !errors.Is(err, plan.ErrMalformed) {
		t.Errorf("the claim of a step run whose state isn't one: %v", err)
	}
}
