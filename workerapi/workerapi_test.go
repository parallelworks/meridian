package workerapi_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// TestBodiesRoundTrip checks that each body reads back as the API wrote
// it, and writes again to the same bytes: numbers in a run's values that a
// float64 can't hold, diagnostics, a problem, and lists and mappings that
// are empty.
func TestBodiesRoundTrip(t *testing.T) {
	const big = 1<<60 + 1
	diag := problems.ExpressionFailed.At("workflow.yaml", problems.Position{Line: 3, Column: 7}, "/jobs/a/steps/0/run", problems.Params{"reason": "x"})
	bodies := []any{
		&workerapi.ClaimRequest{Credential: strings.Repeat("ab", 32), PlanVersion: plan.Version, Worker: workerapi.Worker{Version: "v1.0.0", Digest: "sha256:00"}},
		&workerapi.Claim{
			RunID: "run", AttemptID: "attempt", PlanVersion: plan.Version, Job: []byte(`{"key":"build#2","name":"build","steps":[],"path":"/jobs/build"}`),
			Attempt: 1, Group: "local", Member: "laptop", LeaseSeconds: 60, HeartbeatSeconds: 15, SelfFencing: true,
			Run: plan.RunValues{
				Format: 1, Inputs: plan.DataOf(map[string]any{"n": int64(big), "list": []any{}, "map": map[string]any{}}),
				Vars: map[string]string{"token": "t"}, Private: []string{"inputs.n"}, Secrets: []string{"vars.token"},
			},
			Needs:  map[string]plan.JobResult{"setup": {Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"a": "b"}}},
			RunDir: "/runs/run", Container: &workerapi.Container{Runtime: "podman", Image: "ghcr.io/parallelworks/meridian-base@sha256:00"},
		},
		&workerapi.Control{Version: 2, Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 10},
		&workerapi.Events{},
		&workerapi.Events{Events: []workerapi.Event{{
			Seq: 1, Time: time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC), Finished: true,
			Outcome: plan.OutcomeFailure, ExitCode: -1, Outputs: map[string]string{"a": "b", "c": ""},
			Diagnostics: problems.Diagnostics{diag},
		}}},
		&workerapi.EventsAck{Seq: 1},
		&workerapi.LogAck{Length: 10, Ended: true},
		&workerapi.Complete{
			Outcome: plan.OutcomeFailure, Reason: plan.ReasonSetupFailed,
			Problem: &problem.Problem{Type: "/problems/worker/image_pull_failed", Title: "Image not pulled", Status: 422, Code: "image_pull_failed", Params: map[string]any{"image": "x"}},
		},
	}
	for _, body := range bodies {
		data, err := workerapi.Marshal(body)
		if err != nil {
			t.Fatalf("%T: %v", body, err)
		}
		back := reflect.New(reflect.TypeOf(body).Elem()).Interface()
		if err := workerapi.Unmarshal(data, back); err != nil {
			t.Fatalf("%T: reading %s: %v", body, data, err)
		}
		again, err := workerapi.Marshal(back)
		if err != nil {
			t.Fatal(err)
		}
		if string(again) != string(data) {
			t.Errorf("%T read back as\n%s\nnot\n%s", body, again, data)
		}
	}
}

// TestEmptyIsEmpty checks that a nil list is written as an empty one, so
// that a reader never meets null where it expects a list.
func TestEmptyIsEmpty(t *testing.T) {
	data, err := workerapi.Marshal(&workerapi.Events{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), `{"events":[]}`; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

// TestBigNumbersStay checks that a whole number in a run's values that a
// float64 can't hold is read back exactly.
func TestBigNumbersStay(t *testing.T) {
	var run plan.RunValues
	if err := workerapi.Unmarshal([]byte(`{"format":1,"inputs":{"n":1152921504606846977}}`), &run); err != nil {
		t.Fatal(err)
	}
	if got := run.Inputs.Get().(map[string]any)["n"]; got != int64(1<<60+1) {
		t.Errorf("got %v (%T)", got, got)
	}
}

// TestUnmarshalRefuses checks that a body with a member its type doesn't
// have, or a member given twice, isn't read: a worker never runs a job
// without a part of it that it doesn't understand.
func TestUnmarshalRefuses(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
		v    any
	}{
		{"unknown member", `{"seq":1,"later":true}`, &workerapi.EventsAck{}},
		{"unknown member in a claim", `{"runID":"r","isolation":"microvm"}`, &workerapi.Claim{}},
		{"unknown member in a run's values", `{"run":{"format":1,"newContext":{}}}`, &workerapi.Claim{}},
		{"member twice", `{"seq":1,"seq":2}`, &workerapi.EventsAck{}},
		{"unknown member in a diagnostic", `{"events":[{"seq":1,"time":"2026-01-02T03:04:05Z","step":0,"diagnostics":[{"code":"x","extra":1}]}]}`, &workerapi.Events{}},
	} {
		if err := workerapi.Unmarshal([]byte(tc.data), tc.v); err == nil {
			t.Errorf("%s: read %s", tc.name, tc.data)
		}
	}
}
