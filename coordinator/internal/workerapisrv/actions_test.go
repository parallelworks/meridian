package workerapisrv_test

import (
	"bytes"
	"errors"
	"net/http"
	"slices"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// claimedAction claims, through c, an attempt of a job whose step 1 uses a
// built-in action, and returns it and its credential.
func (r *rig) claimedAction(c *client) (*store.Attempt, string) {
	r.t.Helper()
	run := r.newRun(nil)
	job := &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{
		{Run: plan.Literal("true"), Path: "/jobs/build/steps/0"},
		{Uses: "parallelworks/cancel-jobs", With: plan.Literal(map[string]any{"jobs": []any{"x"}}), Path: "/jobs/build/steps/1"},
		{Uses: "parallelworks/cancel-jobs", If: plan.Literal("always"), With: plan.Literal(map[string]any{"jobs": []any{"x"}}), Path: "/jobs/build/steps/2"},
	}}
	r.readier.set(run, &workerapisrv.Ready{Job: job, Run: plan.RunValues{Format: 1}})
	return r.claimedIn(c, run, "build", placed{})
}

// actionRequest is a request for the action of step 1's try.
func actionRequest(t *testing.T, try int) []byte {
	t.Helper()
	body, err := workerapi.Marshal(&workerapi.ActionRequest{Step: 1, Try: try, With: plan.DataOf(map[string]any{"jobs": []any{"x"}})})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// An action that Actions can't perform now is answered 500, and nothing is
// stored of it: the worker's next request has it performed. One that
// Actions answers without storing, or with what an action can't answer,
// is answered 500 too.
func TestActionNotPerformed(t *testing.T) {
	parallel(t)
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	_, credential := r.claimedAction(c)

	for _, tc := range []struct {
		name   string
		answer func(*store.Tx, *workerapisrv.ActionCall) (workerapi.ActionAnswer, error)
	}{
		{"an error", func(*store.Tx, *workerapisrv.ActionCall) (workerapi.ActionAnswer, error) {
			// Wraps one the server answers otherwise, were it Commit's.
			return workerapi.ActionAnswer{}, errors.Join(errors.New("the platform didn't answer"), store.ErrNotFound)
		}},
		{"no outcome", func(*store.Tx, *workerapisrv.ActionCall) (workerapi.ActionAnswer, error) {
			return workerapi.ActionAnswer{}, nil
		}},
		{"cancelled for no reason", func(*store.Tx, *workerapisrv.ActionCall) (workerapi.ActionAnswer, error) {
			return workerapi.ActionAnswer{Outcome: plan.OutcomeCancelled}, nil
		}},
		{"succeeded for a reason", func(*store.Tx, *workerapisrv.ActionCall) (workerapi.ActionAnswer, error) {
			return workerapi.ActionAnswer{Outcome: plan.OutcomeSuccess, Reason: plan.ReasonCancelled}, nil
		}},
	} {
		r.actions.setAnswer(tc.answer)
		if status, body, err := c.send(http.MethodPost, workerapi.PathAction, actionRequest(t, 0), credential); err != nil || status != http.StatusInternalServerError {
			t.Errorf("an action answered with %s was answered %d, %v: %s", tc.name, status, err, body)
		}
	}
	r.actions.setAnswer(nil)
	if status, body, err := c.send(http.MethodPost, workerapi.PathAction, actionRequest(t, 0), credential); err != nil || status != http.StatusOK || !bytes.Contains(body, []byte(`"success"`)) {
		t.Errorf("the action asked for again was answered %d, %v: %s", status, err, body)
	}

	r.actions.setUncommitted(true)
	if status, body, err := c.send(http.MethodPost, workerapi.PathAction, actionRequest(t, 1), credential); err != nil || status != http.StatusInternalServerError {
		t.Errorf("an action answered without being stored was answered %d, %v: %s", status, err, body)
	}
}

// An action is performed once for a step's try, however many requests for
// it come at once, or after: each is given the answer stored.
func TestActionPerformedOnce(t *testing.T) {
	parallel(t)
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	_, credential := r.claimedAction(c)
	const requests = 8
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		answers = map[string]int{}
	)
	for range requests {
		wg.Go(func() {
			status, body, err := c.send(http.MethodPost, workerapi.PathAction, actionRequest(t, 0), credential)
			if err != nil || status != http.StatusOK {
				t.Errorf("the action was answered %d, %v: %s", status, err, body)
			}
			mu.Lock()
			answers[string(body)]++
			mu.Unlock()
		})
	}
	wg.Wait()
	if status, body, err := c.send(http.MethodPost, workerapi.PathAction, actionRequest(t, 0), credential); err != nil || status != http.StatusOK {
		t.Errorf("the action asked for again was answered %d, %v: %s", status, err, body)
	}
	if n := r.actions.times(); n != 1 || len(answers) != 1 {
		t.Errorf("the action was performed %d times, with the answers %v", n, answers)
	}
}

// A step whose if: doesn't outlive its job's cancellation, as the run's
// plan has it, of an attempt whose control asks to cancel its job, is
// answered cancelled, for the control's reason, without performing its
// action; but not for the run's timeout, which stops the job as its own
// timeout does. One whose if: outlives it is performed, and its answer
// says the cancel; and so does that of an action that cancelled the
// step's own job. A request can't say otherwise: one that says its step
// outlives a cancel is one the API doesn't have, 400.
func TestActionOfACancelledJob(t *testing.T) {
	parallel(t)
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	a, credential := r.claimedAction(c)
	send := func(req workerapi.ActionRequest) string {
		t.Helper()
		req.With = plan.DataOf(map[string]any{"jobs": []any{"x"}})
		body, err := workerapi.Marshal(&req)
		if err != nil {
			t.Fatal(err)
		}
		status, answer, err := c.send(http.MethodPost, workerapi.PathAction, body, credential)
		if err != nil || status != http.StatusOK {
			t.Fatalf("the action was answered %d, %v: %s", status, err, answer)
		}
		return string(answer)
	}

	// An action that cancels its own job, as cancel-jobs can.
	r.actions.setAnswer(func(tx *store.Tx, call *workerapisrv.ActionCall) (workerapi.ActionAnswer, error) {
		return workerapi.ActionAnswer{Outcome: plan.OutcomeSuccess}, askControl(tx, call.Attempt.ID, workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled})
	})
	if got := send(workerapi.ActionRequest{Step: 1}); got != `{"outcome":"success","reason":"cancelled"}` {
		t.Errorf("an action that cancelled its own job was answered %s", got)
	}
	r.actions.setAnswer(nil)
	for try, tc := range []struct {
		reason plan.Reason
		// step is 1, whose if: doesn't outlive a cancel, or 2, whose if:
		// does.
		step int
		want string
	}{
		{plan.ReasonTimeout, 1, `{"outcome":"success"}`},
		{plan.ReasonFailFast, 1, `{"outcome":"cancelled","reason":"fail-fast"}`},
		{plan.ReasonFailFast, 2, `{"outcome":"success","reason":"fail-fast"}`},
	} {
		r.inTx(func(tx *store.Tx) error {
			return askControl(tx, a.ID, workerapi.Control{Cancel: true, Reason: tc.reason})
		})
		if got := send(workerapi.ActionRequest{Step: tc.step, Try: try + 1}); got != tc.want {
			t.Errorf("an action of a job cancelled for %s, of step %d, was answered %s, want %s", tc.reason, tc.step, got, tc.want)
		}
	}
	// Whether a step outlives a cancel is the plan's to say, not the
	// worker's: a request that says so isn't one.
	claims := []byte(`{"step":1,"try":9,"with":{"jobs":["x"]},"outlivesCancel":true}`)
	if status, body, err := c.send(http.MethodPost, workerapi.PathAction, claims, credential); err != nil || status != http.StatusBadRequest {
		t.Errorf("a request that says its step outlives a cancel was answered %d, %v: %s", status, err, body)
	}
	if got := send(workerapi.ActionRequest{Step: 1, Try: 9}); got != `{"outcome":"cancelled","reason":"fail-fast"}` {
		t.Errorf("the step that doesn't outlive a cancel, asked again, was answered %s", got)
	}
	if n := r.actions.times(); n != 3 {
		t.Errorf("the action was performed %d times, not 3", n)
	}
	// Cancelled said, before each Commit, what Commit did, so that an
	// action that asks a platform first, as update-session does, asks it
	// nothing for a step its job's cancel stops: as the plan's if: says,
	// whatever the request.
	if got := r.actions.sawCancelled(); !slices.Equal(got, []bool{false, false, true, false, true}) {
		t.Errorf("Cancelled said %v", got)
	}
}

// A step whose early-cancel: is any-job-failed, as the run's plan has it,
// is answered cancelled, with no reason, without performing its action,
// once a job of the run has failed, as its attempt's control says, though
// it outlives its job's cancellation; and a step without one is performed.
// Cancelled, asked before each Commit, says what Commit then did.
func TestActionEarlyCancelled(t *testing.T) {
	parallel(t)
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	run := r.newRun(nil)
	cancels := plan.Step{Uses: "parallelworks/cancel-jobs", If: plan.Literal("always"), With: plan.Literal(map[string]any{"jobs": []any{"x"}}), Path: "/jobs/build/steps/0", EarlyCancel: plan.EarlyCancelAnyJobFailed}
	plain := cancels
	plain.Path, plain.EarlyCancel = "/jobs/build/steps/1", ""
	job := &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{cancels, plain}}
	r.readier.set(run, &workerapisrv.Ready{Job: job, Run: plan.RunValues{Format: 1}})
	a, credential := r.claimedIn(c, run, "build", placed{})
	r.inTx(func(tx *store.Tx) error { return askControl(tx, a.ID, workerapi.Control{AnyJobFailed: true}) })
	for step, want := range []string{`{"outcome":"cancelled"}`, `{"outcome":"success"}`} {
		body, err := workerapi.Marshal(&workerapi.ActionRequest{Step: step, With: plan.DataOf(map[string]any{"jobs": []any{"x"}})})
		if err != nil {
			t.Fatal(err)
		}
		status, answer, err := c.send(http.MethodPost, workerapi.PathAction, body, credential)
		if err != nil || status != http.StatusOK || string(answer) != want {
			t.Errorf("step %d's action was answered %d, %v: %s, want %s", step, status, err, answer, want)
		}
	}
	if n := r.actions.times(); n != 1 {
		t.Errorf("the action was performed %d times, not once", n)
	}
	if got := r.actions.sawCancelled(); !slices.Equal(got, []bool{true, false}) {
		t.Errorf("Cancelled said %v", got)
	}
}
