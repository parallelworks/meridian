package workerapisrv_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// TestStepRunPaths checks the step run's paths: an ask answered as the
// coordinator's Children answers it, for a step's try and for its
// cleanup; and refused, 400, for a step that can't have one, a cleanup of
// a try, and more strings to mask than a request may hold, and a poll
// whose cleanup isn't true or false. A poll for its end is answered once
// it has ended, as Children gives it, and its stop is Children's to make;
// but only for the attempt whose step it is: another attempt's poll for,
// or stop of, the same step's try, of which it has none, is 404.
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
	if cleanup := runOf(workerapi.StepRunRequest{Cleanup: true}); cleanup == try {
		t.Error("the step's cleanup's step run is its try's")
	}
	for name, req := range map[string]workerapi.StepRunRequest{
		"a step that can't have one": {Step: 1},
		"a cleanup of a try":         {Cleanup: true, Try: 1},
		"too much to mask":           {Masked: []string{strings.Repeat("x", workerapi.MaxStepRunMasked+1)}},
	} {
		if status, answer := ask(req); status != http.StatusBadRequest {
			t.Errorf("%s: answered %d: %s", name, status, answer)
		}
	}

	poll := func(credential string, q url.Values) (int, []byte) {
		t.Helper()
		status, answer, err := c.send(http.MethodGet, workerapi.PathStepRunEnd+"?"+q.Encode(), nil, credential)
		if err != nil {
			t.Fatal(err)
		}
		return status, answer
	}
	q := url.Values{workerapi.QueryStep: {"0"}, workerapi.QueryTry: {"1"}}
	if status, answer := poll(credential, q); status != http.StatusNotFound {
		t.Errorf("a poll for a step run the try hasn't was answered %d: %s", status, answer)
	}
	q.Set(workerapi.QueryCleanup, "yes")
	if status, answer := poll(credential, q); status != http.StatusBadRequest {
		t.Errorf("a poll whose cleanup isn't true or false was answered %d: %s", status, answer)
	}
	r.inTx(func(tx *store.Tx) error { return tx.EndRun(try, plan.OutcomeSuccess, "") })
	q = url.Values{workerapi.QueryStep: {"0"}, workerapi.QueryTry: {"0"}}
	status, answer := poll(credential, q)
	var end workerapi.StepRun
	if status != http.StatusOK || workerapi.Unmarshal(answer, &end) != nil || end.Run != try || !end.Ended || end.Outputs["x"] != "1" {
		t.Errorf("a poll for the step run's end was answered %d: %s", status, answer)
	}
	if status, answer := poll(other, q); status != http.StatusNotFound {
		t.Errorf("another attempt's poll for the step run's end was answered %d: %s", status, answer)
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
		if status != http.StatusNoContent && status != http.StatusNotFound {
			t.Errorf("a stop was answered %d: %s", status, answer)
		}
		return status
	}
	if status := stop(other, workerapi.StepRunStop{Cleanup: true, Reason: plan.ReasonCancelled}); status != http.StatusNotFound {
		t.Errorf("another attempt's stop of the cleanup's step run was answered %d", status)
	}
	if status := stop(credential, workerapi.StepRunStop{Try: 1, Reason: plan.ReasonCancelled}); status != http.StatusNotFound {
		t.Errorf("a stop of a step run the try hasn't was answered %d", status)
	}
	if status := stop(credential, workerapi.StepRunStop{Cleanup: true, Reason: plan.ReasonTimeout}); status != http.StatusNoContent {
		t.Errorf("the stop of the cleanup's step run was answered %d", status)
	}
	r.children.mu.Lock()
	stops := r.children.stops[a.ID]
	r.children.mu.Unlock()
	if len(stops) != 1 || stops[0].Reason != plan.ReasonTimeout {
		t.Errorf("the stops made are %+v", stops)
	}
}
