package workerapi_test

import (
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"reflect"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// TestBodiesEncodeAlike checks that encoding/json and encoding/json/v2
// read what each other writes of every body as it was, so that a worker
// and a coordinator agree whichever they use. A time.Duration, which the
// two write differently, or not at all, fails it.
func TestBodiesEncodeAlike(t *testing.T) {
	run, err := plan.ValueOf("echo ${{ inputs.name }}")
	if err != nil {
		t.Fatal(err)
	}
	bodies := []any{
		&workerapi.ClaimRequest{Key: "00ff", Worker: workerapi.Worker{Version: "v1.0.0", Digest: "sha256:00"}},
		&workerapi.Claim{
			RunID: "run", AttemptID: "attempt", Credential: "c", Fence: 3,
			LeaseSeconds: 60, HeartbeatSeconds: 15, SelfFencing: true,
			Job: &plan.ReadyJob{
				Key: "build#2", Name: "build", Matrix: map[string]any{"os": "linux"},
				Retry:          &plan.ReadyJobRetry{Reasons: []plan.Reason{plan.ReasonWorkerLost}, MaxRetries: 3, IntervalSeconds: 60},
				TimeoutSeconds: 3600, Steps: []plan.Step{{Run: run, Path: "/jobs/build/steps/0"}}, Path: "/jobs/build",
			},
			JobContext: plan.JobContext{Name: "build", Key: "build#2", Attempt: 1, Group: "local", Member: "laptop"},
			Run:        plan.RunValues{Format: 1, Inputs: map[string]any{"name": "x"}, Secrets: []string{"vars.token"}},
			Needs:      map[string]plan.JobResult{"setup": {Outcome: plan.Success, Outputs: map[string]string{"a": "b"}}},
			RunDir:     "/runs/run", Container: &workerapi.Container{Runtime: "podman", Image: "sha256:00"},
		},
		&workerapi.Control{Version: 2, Cancel: true, GraceSeconds: 10},
		&workerapi.Events{Events: []workerapi.Event{{
			Seq: 1, Time: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Kind: workerapi.StepFinished,
			Step: 0, Outcome: plan.Failure, ExitCode: -1, Outputs: map[string]string{"a": "b"},
		}}},
		&workerapi.EventsAck{Seq: 1},
		&workerapi.LogAck{Length: 10},
		&workerapi.Complete{Seq: 2, Outcome: plan.Failure, Reason: plan.ReasonTimeout, Outputs: map[string]string{"a": "b"}},
	}
	for _, body := range bodies {
		// encoding/json writes mappings in the order of their keys, so its
		// bytes are what is compared.
		want, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encoding/json: %T: %v", body, err)
		}
		v2, err := jsonv2.Marshal(body)
		if err != nil {
			t.Fatalf("encoding/json/v2: %T: %v", body, err)
		}
		for name, decode := range map[string]func() (any, error){
			"encoding/json/v2 reads encoding/json's": func() (any, error) {
				back := reflect.New(reflect.TypeOf(body).Elem()).Interface()
				return back, jsonv2.Unmarshal(want, back)
			},
			"encoding/json reads encoding/json/v2's": func() (any, error) {
				back := reflect.New(reflect.TypeOf(body).Elem()).Interface()
				return back, json.Unmarshal(v2, back)
			},
		} {
			back, err := decode()
			if err != nil {
				t.Fatalf("%s %T: %v", name, body, err)
			}
			got, err := json.Marshal(back)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(want) {
				t.Errorf("%s %T as\n%s\nnot\n%s", name, body, got, want)
			}
		}
	}
}
