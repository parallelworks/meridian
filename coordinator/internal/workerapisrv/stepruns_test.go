package workerapisrv_test

import (
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/coordtest"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// TestStepRunPaths checks the step run's paths:
//   - an ask is answered as the coordinator's Children answers it, for a
//     step's try and for its cleanup; fenced for an attempt Children says
//     no longer holds its job, or that the server sees doesn't; and refused,
//     400, for a step that can't have one, a cleanup of a try, and more
//     strings to mask, in bytes or in number, than a request may hold;
//   - a poll for its end is answered once it has ended, as Children gives
//     it, for the try's or the cleanup's, as the poll names it, and refused
//     for a cleanup that isn't true or false; and its stop is Children's to
//     make, of the one the stop names, for a reason a step is stopped for;
//   - but only for the attempt whose step it is: another attempt's poll for,
//     or stop of, the same step's try, of which it has none, is 404, as is
//     one of a try whose child is a workflow its step calls.
func TestStepRunPaths(t *testing.T) {
	parallel(t)
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	a, _, credential := r.claimed(c, placed{})
	_, _, other := r.claimed(c, placed{})
	ask := func(req workerapi.StepRunRequest) (int, []byte) {
		t.Helper()
		body, err := workerapi.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		status, answer, err := c.send(http.MethodPost, workerapi.PathStepRun, body, credential)
		if err != nil {
			t.Fatal(err)
		}
		return status, answer
	}
	runOf := func(req workerapi.StepRunRequest) string {
		t.Helper()
		status, answer := ask(req)
		var sr workerapi.StepRun
		if status != http.StatusOK || workerapi.Unmarshal(answer, &sr) != nil || sr.Run == "" {
			t.Fatalf("an ask for a step run was answered %d: %s", status, answer)
		}
		return sr.Run
	}
	try := runOf(workerapi.StepRunRequest{State: workerapi.StepRunState{Steps: []workerapi.StepRunResult{{Outcome: plan.OutcomeSuccess}}}})
	cleanup := runOf(workerapi.StepRunRequest{Cleanup: true})
	if cleanup == try {
		t.Error("the step's cleanup's step run is its try's")
	}
	many := make([]string, workerapi.MaxMasks+1)
	for i := range many {
		many[i] = strconv.Itoa(i)
	}
	for name, req := range map[string]workerapi.StepRunRequest{
		"a step that can't have one": {Step: 1},
		"a cleanup of a try":         {Cleanup: true, Try: 1},
		"too much to mask":           {Masked: []string{strings.Repeat("x", workerapi.MaxMaskedBytes+1)}},
		"too many to mask":           {Masked: many},
	} {
		if status, answer := ask(req); status != http.StatusBadRequest {
			t.Errorf("%s: answered %d: %s", name, status, answer)
		}
	}
	status, answer := ask(workerapi.StepRunRequest{Step: 2})
	_ = wantProblem(t, "an ask Children says is fenced", status, answer, workerapi.Fenced)

	// A workflow its step calls is the try's child, but no step run.
	body, err := workerapi.Marshal(workerapi.ChildRequest{Try: 1, With: plan.DataOf(map[string]any{"$yaml": "w.yaml"})})
	if err != nil {
		t.Fatal(err)
	}
	if status, answer, err := c.send(http.MethodPost, workerapi.PathChild, body, credential); err != nil || status != http.StatusOK {
		t.Fatalf("the ask for a child was answered %d, %v: %s", status, err, answer)
	}
	// A child's request masks no more than a step run's may.
	for name, req := range map[string]workerapi.ChildRequest{
		"too much to mask": {Try: 2, Masked: []string{strings.Repeat("x", workerapi.MaxMaskedBytes+1)}},
		"too many to mask": {Try: 2, Masked: many},
	} {
		body, err := workerapi.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		if status, answer, err := c.send(http.MethodPost, workerapi.PathChild, body, credential); err != nil || status != http.StatusBadRequest {
			t.Errorf("a child's request with %s was answered %d, %v: %s", name, status, err, answer)
		}
	}
	// A child's paths reach only a workflow its step calls: the step run
	// of try 0 is none.
	q := url.Values{workerapi.QueryStep: {"0"}, workerapi.QueryTry: {"0"}}
	if status, answer, err := c.send(http.MethodGet, workerapi.PathChildEnd+"?"+q.Encode(), nil, credential); err != nil || status != http.StatusNotFound {
		t.Errorf("a poll for the end of a child that is a step run was answered %d, %v: %s", status, err, answer)
	}
	childStop, err := workerapi.Marshal(workerapi.ChildStop{Reason: plan.ReasonCancelled})
	if err != nil {
		t.Fatal(err)
	}
	if status, answer, err := c.send(http.MethodPost, workerapi.PathChildStop, childStop, credential); err != nil || status != http.StatusNotFound {
		t.Errorf("a child's stop of a step run was answered %d, %v: %s", status, err, answer)
	}

	poll := func(credential string, q url.Values) (int, []byte) {
		t.Helper()
		status, answer, err := c.send(http.MethodGet, workerapi.PathStepRunEnd+"?"+q.Encode(), nil, credential)
		if err != nil {
			t.Fatal(err)
		}
		return status, answer
	}
	for _, try := range []string{"1", "2"} {
		if status, answer := poll(credential, url.Values{workerapi.QueryStep: {"0"}, workerapi.QueryTry: {try}}); status != http.StatusNotFound {
			t.Errorf("a poll for a step run of try %s, which has a workflow's child, or none, was answered %d: %s", try, status, answer)
		}
	}
	if status, answer := poll(credential, url.Values{workerapi.QueryStep: {"0"}, workerapi.QueryTry: {"0"}, workerapi.QueryCleanup: {"yes"}}); status != http.StatusBadRequest {
		t.Errorf("a poll whose cleanup isn't true or false was answered %d: %s", status, answer)
	}
	r.inTx(func(tx *store.Tx) error {
		if err := tx.EndRun(try, plan.OutcomeSuccess, ""); err != nil {
			return err
		}
		return tx.EndRun(cleanup, plan.OutcomeSuccess, "")
	})
	for want, q := range map[string]url.Values{
		try:     {workerapi.QueryStep: {"0"}, workerapi.QueryTry: {"0"}},
		cleanup: {workerapi.QueryStep: {"0"}, workerapi.QueryTry: {"0"}, workerapi.QueryCleanup: {"true"}},
	} {
		status, answer := poll(credential, q)
		var end workerapi.StepRun
		if status != http.StatusOK || workerapi.Unmarshal(answer, &end) != nil || end.Run != want || !end.Ended || end.Outputs["x"] != "1" {
			t.Errorf("a poll for the end of %s, %v, was answered %d: %s", want, q, status, answer)
		}
		if status, answer := poll(other, q); status != http.StatusNotFound {
			t.Errorf("another attempt's poll for the end of %s was answered %d: %s", want, status, answer)
		}
	}

	stop := func(credential string, s workerapi.StepRunStop) int {
		t.Helper()
		body, err := workerapi.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		status, answer, err := c.send(http.MethodPost, workerapi.PathStepRunStop, body, credential)
		if err != nil {
			t.Fatal(err)
		}
		if status != http.StatusNoContent && status != http.StatusNotFound && status != http.StatusBadRequest {
			t.Errorf("a stop was answered %d: %s", status, answer)
		}
		return status
	}
	for name, s := range map[string]workerapi.StepRunStop{
		"no reason":                  {Cleanup: true},
		"a reason no step stops for": {Cleanup: true, Reason: plan.ReasonWorkerLost},
	} {
		if status := stop(credential, s); status != http.StatusBadRequest {
			t.Errorf("a stop for %s was answered %d", name, status)
		}
	}
	if status := stop(other, workerapi.StepRunStop{Cleanup: true, Reason: plan.ReasonCancelled}); status != http.StatusNotFound {
		t.Errorf("another attempt's stop of the cleanup's step run was answered %d", status)
	}
	if status := stop(credential, workerapi.StepRunStop{Try: 1, Reason: plan.ReasonCancelled}); status != http.StatusNotFound {
		t.Errorf("a stop of a try whose child is a workflow's was answered %d", status)
	}
	if status := stop(credential, workerapi.StepRunStop{Cleanup: true, Reason: plan.ReasonTimeout}); status != http.StatusNoContent {
		t.Errorf("the stop of the cleanup's step run was answered %d", status)
	}
	r.children.mu.Lock()
	stops, stopped := r.children.stops[a.ID], slices.Clone(r.children.stopped)
	r.children.mu.Unlock()
	if len(stops) != 1 || stops[0].Reason != plan.ReasonTimeout || !slices.Equal(stopped, []string{cleanup}) {
		t.Errorf("the stops made are %+v, of %v", stops, stopped)
	}

	// An attempt superseded is fenced, before Children is asked, which would
	// refuse this step.
	r.newAttempt(a.Run, "build", placed{})
	status, answer = ask(workerapi.StepRunRequest{Step: 1})
	_ = wantProblem(t, "an ask of an attempt superseded", status, answer, workerapi.Fenced)
}

// TestStepRunPathsVersion checks that the step run's paths are version
// 3's: an attempt of version 2 doesn't have them, 404.
func TestStepRunPathsVersion(t *testing.T) {
	parallel(t)
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	run := r.newRun(nil)
	r.readier.set(run, &workerapisrv.Ready{Job: &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{{Run: plan.Literal("true"), Path: "/jobs/build/steps/0"}}}, Run: plan.RunValues{Format: 1}})
	_, token := r.newAttempt(run, "build", placed{})
	credential := coordtest.Credential(t)
	if status, body, err := c.send(http.MethodPost, workerapi.PathClaim, claimRequestAt(t, credential, conformanceDigest, workerapi.Version2), token); err != nil || status != http.StatusOK {
		t.Fatalf("the claim was answered %d, %v: %s", status, err, body)
	}
	for _, req := range []struct{ method, path string }{
		{http.MethodPost, workerapi.PathStepRun},
		{http.MethodGet, workerapi.PathStepRunEnd + "?" + workerapi.QueryStep + "=0&" + workerapi.QueryTry + "=0"},
		{http.MethodPost, workerapi.PathStepRunStop},
	} {
		if status, body, err := c.send(req.method, req.path, []byte("{}"), credential); err != nil || status != http.StatusNotFound {
			t.Errorf("%s %s of an attempt of version 2 was answered %d, %v: %s", req.method, req.path, status, err, body)
		}
	}
}

// TestStepRunComplete checks that the end of a step run's attempt whose
// step wrote more to $OUTPUTS than a worker reads is refused, 400, and
// nothing of it stored, while one that keeps to it is stored.
func TestStepRunComplete(t *testing.T) {
	parallel(t)
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	caller, _, _ := r.claimed(c, placed{})
	created := created(t)
	var id string
	r.inTx(func(tx *store.Tx) error {
		var err error
		if id, err = tx.NewRunID(); err != nil {
			return err
		}
		link := &store.Link{Run: caller.Run, Job: "build", Attempt: caller.ID, Root: caller.Run, Depth: 1}
		if err := tx.InsertRun(store.NewRun{ID: id, Plan: created.Plan, StepRun: true, DefaultGroup: "local", RunDir: "/runs/" + caller.Run, WorkflowID: fakeJobID(), Parent: link}); err != nil {
			return err
		}
		if err := tx.InsertJob(store.NewJob{Run: id, Name: "build", Key: "build", SupervisorID: fakeJobID()}); err != nil {
			return err
		}
		sealed, gen, err := r.sealer.Seal(t.Context(), id, &created.Record)
		if err != nil {
			return err
		}
		return tx.InsertRecord(id, gen, sealed.Key, sealed.Record)
	})
	r.readier.set(id, &workerapisrv.Ready{Job: &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{{Run: plan.Literal("true"), Path: "/jobs/build/steps/0"}}}, Run: plan.RunValues{Format: 1}})
	a, credential := r.claimedIn(c, id, "build", placed{})
	end := func(value string) []byte {
		body, err := workerapi.Marshal(workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": value}}})
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	if status, body, err := c.send(http.MethodPost, workerapi.PathComplete, end(strings.Repeat("y", workerapi.MaxOutputs)), credential); err != nil || status != http.StatusBadRequest {
		t.Errorf("an end with too many outputs was answered %d, %v: %s", status, err, body)
	}
	if got := r.attempt(a.ID); got.State == store.AttemptEnded {
		t.Error("the end with too many outputs was stored")
	}
	if status, body, err := c.send(http.MethodPost, workerapi.PathComplete, end(strings.Repeat("y", workerapi.MaxOutputs-3)), credential); err != nil || status != http.StatusNoContent {
		t.Errorf("an end with outputs up to the limit was answered %d, %v: %s", status, err, body)
	}
}
