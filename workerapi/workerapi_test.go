package workerapi_test

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/internal/schematest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// TestWriteBody checks that a body is answered as Marshal encodes it, as
// JSON, with 200; and that one Marshal can't encode is its error, with
// nothing written.
func TestWriteBody(t *testing.T) {
	v := workerapi.EventsAck{Seq: 3}
	want, err := workerapi.Marshal(&v)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	if err := workerapi.WriteBody(rec, workerapi.Version, &v); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" || rec.Body.String() != string(want) {
		t.Errorf("WriteBody answered %d, %q: %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body)
	}
	rec = httptest.NewRecorder()
	if err := workerapi.WriteBody(rec, workerapi.Version, make(chan int)); err == nil || rec.Body.Len() != 0 || rec.Header().Get("Content-Type") != "" {
		t.Errorf("WriteBody of what Marshal can't encode = %v, and wrote %q", err, rec.Body)
	}
}

// TestCodesAreKinds checks that each of the API's codes is the code of its
// kind, in problems.WorkerRegistry, and that ParamExpected is OutOfOrder's
// param.
func TestCodesAreKinds(t *testing.T) {
	for code, kind := range map[problem.Code]*problems.Kind{
		workerapi.Fenced:               problems.Fenced,
		workerapi.OutOfOrder:           problems.OutOfOrder,
		workerapi.PlanTooNew:           problems.PlanTooNew,
		workerapi.WorkerAPITooOld:      problems.WorkerAPITooOld,
		workerapi.WorkerDigestMismatch: problems.WorkerDigestMismatch,
	} {
		if kind.Code() != code {
			t.Errorf("%s is the code of the kind %s", code, kind.Code())
		}
		if typ, ok := problems.WorkerRegistry.Lookup(code); !ok || typ != kind.Type() {
			t.Errorf("%s isn't the worker registry's kind", code)
		}
	}
	if got := problems.OutOfOrder.Type().Params; !slices.Equal(got, []string{workerapi.ParamExpected}) {
		t.Errorf("out_of_order's params are %v, want [%s]", got, workerapi.ParamExpected)
	}
	if got := problems.WorkerDigestMismatch.Type().Params; !slices.Equal(got, []string{workerapi.ParamPlaced, workerapi.ParamClaimed}) {
		t.Errorf("worker_digest_mismatch's params are %v, want [%s %s]", got, workerapi.ParamPlaced, workerapi.ParamClaimed)
	}
	if got := problems.WorkerAPITooOld.Type().Params; !slices.Equal(got, []string{workerapi.ParamVersion, workerapi.ParamNeeded}) {
		t.Errorf("worker_api_too_old's params are %v, want [%s %s]", got, workerapi.ParamVersion, workerapi.ParamNeeded)
	}
}

// bodies are a body of each type, which set every member of each type
// they hold, between them: numbers in a run's values that a float64 can't
// hold, diagnostics, a problem, and lists and mappings that are empty.
func bodies() []any {
	const big = 1<<60 + 1
	diag := problems.ExpressionFailed.At("workflow.yaml", problems.Position{Line: 3, Column: 7}, "/jobs/a/steps/0/run", problems.Params{"reason": "x"})
	result := plan.JobResult{
		Outcome: plan.OutcomeFailure, Outputs: map[string]string{"dsn": "postgres://u:p@db", "n": "3"},
		Private: []string{"dsn"}, Secrets: []string{"dsn"}, Derived: []string{"dsn"},
		Steps:       map[string]map[string]string{"login": {"token": "t"}},
		SecretSteps: map[string][]string{"login": {"token"}}, DerivedSteps: map[string][]string{"login": {"token"}},
		NeedsFailed: true, Cancelled: true,
	}
	return []any{
		&workerapi.ClaimRequest{Credential: strings.Repeat("ab", 32), APIVersion: workerapi.Version, PlanVersion: plan.Version, Worker: workerapi.Worker{Version: "v1.0.0", Digest: "sha256:00"}},
		&workerapi.Claim{
			RunID: "run", AttemptID: "attempt", APIVersion: workerapi.Version, PlanVersion: plan.Version, Job: []byte(`{"key":"build#2","name":"build","steps":[],"path":"/jobs/build"}`),
			Attempt: 1, Group: "local", Member: "laptop", LeaseSeconds: 60, HeartbeatSeconds: 15, SelfFencing: true,
			Run: plan.RunValues{
				Format: 1, Inputs: plan.DataOf(map[string]any{"n": int64(big), "list": []any{}, "map": map[string]any{}}),
				Env: map[string]string{"E": "e"}, Org: map[string]string{"R": "r"}, Vars: map[string]string{"token": "t"},
				Remote: map[string]string{"repo": "r"}, Sessions: map[string]string{"desktop": "d"},
				Private: []string{"inputs.n"}, Secrets: []string{"vars.token"}, Derived: []string{"vars.token"},
			},
			Needs:  map[string]plan.JobResult{"setup": {Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"a": "b"}}, "build": result},
			RunDir: "/runs/run",
			Env:    map[string]workerapi.EnvVar{"PW_USER": {Value: "u"}, "PW_API_KEY": {Value: "k", Secret: true}},
			Retry:  2, Container: &workerapi.Container{Runtime: "podman", Image: "ghcr.io/parallelworks/meridian-base@sha256:00"},
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
			Seq: 3, Result: result, Reason: plan.ReasonSetupFailed, Diagnostics: problems.Diagnostics{diag},
			Problem: &problem.Problem{
				Type: "/problems/worker/image_pull_failed", Title: "Image not pulled", Status: 422, Code: "image_pull_failed", Params: map[string]any{"image": "x"},
				Instance: "/runs/run/attempts/attempt", Detail: "no image",
				Errors: []*problem.FieldError{{Type: "/problems/x", Code: "x", Detail: "d", Pointer: "#/a", Parameter: "p", In: "query", Params: map[string]any{"n": 1}}},
			},
		},
	}
}

// Unmarshal takes what the API's schema takes, and refuses what it
// refuses, of each change to each body that schematest.Agree makes: a
// claim's job as the worker reads it, with plan.DecodeReadyJob.
func TestUnmarshalAgreesWithTheSchema(t *testing.T) {
	for _, body := range bodies() {
		data, err := workerapi.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		typ := reflect.TypeOf(body).Elem()
		decode := func(data []byte) error {
			v := reflect.New(typ).Interface()
			if err := workerapi.Unmarshal(data, v); err != nil {
				return err
			}
			if c, ok := v.(*workerapi.Claim); ok {
				_, err := plan.DecodeReadyJob(c.PlanVersion, c.Job)
				return err
			}
			return nil
		}
		t.Run(typ.Name(), func(t *testing.T) {
			t.Parallel()
			schematest.Agree(t, "plan/schema/1/workerapi.schema.json#/$defs/"+typ.Name(), typ.Name(), data, decode, func(string) bool { return false })
		})
	}
}

// TestBodiesRoundTrip checks that each body reads back as the API wrote
// it, and writes again to the same bytes: numbers in a run's values that a
// float64 can't hold, diagnostics, a problem, and lists and mappings that
// are empty; and each fits its definition in the API's schema
// (plan.Schemas).
func TestBodiesRoundTrip(t *testing.T) {
	for _, body := range bodies() {
		data, err := workerapi.Marshal(body)
		if err != nil {
			t.Fatalf("%T: %v", body, err)
		}
		name := reflect.TypeOf(body).Elem().Name()
		schematest.Validate(t, schematest.Schema(t, "plan/schema/1/workerapi.schema.json#/$defs/"+name), name, data)
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

// TestUnmarshalRefuses checks that a body the schema refuses isn't read: one
// with a member its type doesn't have, or a member given twice, so that a
// worker never runs a job without a part of it that it doesn't understand;
// and what Marshal never writes, as a member it always writes missing,
// null, and what a type of package plan's refuses, such as an outcome that
// isn't one, in a result a worker reports.
func TestUnmarshalRefuses(t *testing.T) {
	good, types := map[string]string{}, map[string]reflect.Type{}
	var result string
	for _, body := range bodies() {
		data, err := workerapi.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		name := reflect.TypeOf(body).Elem().Name()
		good[name], types[name] = string(data), reflect.TypeOf(body).Elem()
		if c, ok := body.(*workerapi.Complete); ok {
			data, err := workerapi.Marshal(c.Result)
			if err != nil {
				t.Fatal(err)
			}
			result = `"result":` + string(data)
		}
	}
	for _, tc := range []struct{ name, body, old, new string }{
		{"an unknown member", "EventsAck", `"seq":1`, `"seq":1,"later":true`},
		{"an unknown member in a claim", "Claim", `"runID":"run"`, `"runID":"run","isolation":"microvm"`},
		{"an unknown member in a run's values", "Claim", `"run":{`, `"run":{"newContext":{},`},
		{"an unknown member in a diagnostic", "Events", `"code":"expression_failed"`, `"code":"expression_failed","extra":1`},
		{"a result without its outcome", "Complete", `"outcome":"failure",`, ``},
		{"an outcome that isn't one", "Complete", `"outcome":"failure"`, `"outcome":"failed"`},
		{"an empty outcome", "Complete", `"outcome":"failure"`, `"outcome":""`},
		{"a result that is null", "Complete", result, `"result":null`},
		{"no result", "Complete", result + ",", ``},
		{"outputs that are null", "Complete", `"outputs":{"dsn":"postgres://u:p@db","n":"3"}`, `"outputs":null`},
		{"an output that is null", "Complete", `"n":"3"`, `"n":null`},
		{"a need's outcome that isn't one", "Claim", `"outcome":"success"`, `"outcome":"x"`},
		{"a run's values of format 0", "Claim", `"format":1`, `"format":0`},
		{"a run's empty secret name", "Claim", `"secrets":["vars.token"]`, `"secrets":[""]`},
		{"a claim of another plan version", "Claim", `"planVersion":1`, `"planVersion":2`},
		{"a claim without its API version", "Claim", `"apiVersion":1,`, ``},
		{"a claim request whose API version is text", "ClaimRequest", `"apiVersion":1`, `"apiVersion":"1"`},
		{"a sequence number that is null", "EventsAck", `"seq":1`, `"seq":null`},
		{"a step past 32 bits", "Events", `"step":0`, `"step":2147483648`},
		{"an event's outcome that isn't one", "Events", `"outcome":"failure"`, `"outcome":"ok"`},
		{"a diagnostic without its code", "Events", `"code":"expression_failed",`, ``},
		{"a problem's field's in that isn't one", "Complete", `"in":"query"`, `"in":"body"`},
		{"a problem without its type", "Complete", `"type":"/problems/worker/image_pull_failed",`, ``},
	} {
		data := strings.Replace(good[tc.body], tc.old, tc.new, 1)
		if data == good[tc.body] {
			t.Fatalf("%s: the %s body changed: %s", tc.name, tc.body, good[tc.body])
		}
		decode := func() error {
			v := reflect.New(types[tc.body]).Interface()
			if err := workerapi.Unmarshal([]byte(data), v); err != nil {
				return err
			}
			if c, ok := v.(*workerapi.Claim); ok {
				_, err := plan.DecodeReadyJob(c.PlanVersion, c.Job)
				return err
			}
			return nil
		}
		if err := decode(); err == nil {
			t.Errorf("%s: read %s", tc.name, data)
		}
		if err := schematest.Check(schematest.Schema(t, "plan/schema/1/workerapi.schema.json#/$defs/"+tc.body), []byte(data)); err == nil {
			t.Errorf("%s: fits the schema", tc.name)
		}
	}
	if err := workerapi.Unmarshal([]byte(`{"seq":1,"seq":2}`), &workerapi.EventsAck{}); err == nil {
		t.Error("a member twice is read")
	}
}
