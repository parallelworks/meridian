package plan_test

import (
	"testing"

	"github.com/parallelworks/meridian/plan"
)

// TestLastTry checks which try of a step is its last, as its retry: says
// when the plan holds it written out, and that an expression isn't known.
func TestLastTry(t *testing.T) {
	value := func(v any) plan.Value {
		t.Helper()
		out, err := plan.ValueOf(v)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	for _, tc := range []struct {
		name        string
		retry       *plan.StepRetry
		try         int
		last, known bool
	}{
		{"no retry:", nil, 0, true, true},
		{"max-retries: 2, first try", &plan.StepRetry{MaxRetries: value(2.0)}, 0, false, true},
		{"max-retries: 2, last try", &plan.StepRetry{MaxRetries: value(2.0)}, 2, true, true},
		{"max-retries: '2' as text", &plan.StepRetry{MaxRetries: value("2")}, 2, true, true},
		{"max-retries: 0", &plan.StepRetry{MaxRetries: value(0.0)}, 0, true, true},
		{"the default", &plan.StepRetry{}, 9, false, true},
		{"the default's last", &plan.StepRetry{}, 10, true, true},
		{"empty, the default", &plan.StepRetry{MaxRetries: value("")}, 10, true, true},
		{"an expression", &plan.StepRetry{MaxRetries: value("${{ inputs.n }}")}, 3, false, false},
		{"not a count", &plan.StepRetry{MaxRetries: value("many")}, 3, false, false},
	} {
		s := plan.Step{Retry: tc.retry}
		if last, known := s.LastTry(tc.try); last != tc.last || known != tc.known {
			t.Errorf("%s: LastTry(%d) = %v, %v, want %v, %v", tc.name, tc.try, last, known, tc.last, tc.known)
		}
	}
}

// TestRetriesFor checks whether a job's retry: may run it again after an
// attempt ends for a reason: whether it lists the reason.
func TestRetriesFor(t *testing.T) {
	lost := &plan.JobRetry{Reasons: []string{string(plan.ReasonWorkerLost)}}
	for _, tc := range []struct {
		retry  *plan.JobRetry
		reason plan.Reason
		want   bool
	}{
		{lost, plan.ReasonWorkerLost, true},
		{lost, plan.ReasonTimeout, false},
		{nil, plan.ReasonWorkerLost, false},
	} {
		j := plan.Job{Retry: tc.retry}
		if got := j.RetriesFor(tc.reason); got != tc.want {
			t.Errorf("a job whose retry: is %+v runs again after %s: %v, want %v", tc.retry, tc.reason, got, tc.want)
		}
	}
}
