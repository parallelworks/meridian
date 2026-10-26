package plan_test

import (
	"bytes"
	"encoding/json/v2"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/plan"
)

// sampleRecord is a secret record with each of its members set.
func sampleRecord() *plan.Record {
	seconds := int64(3600)
	return &plan.Record{
		Version:    plan.Version,
		PlanDigest: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Submitted: plan.DataOf(map[string]any{
			"name": "Alice", "n": int64(3), "pw": "hunter2", "note": "${{ not.parsed }}",
			"rows": []any{map[string]any{"pass": "p", "user": "a"}, nil},
		}),
		Withheld:  map[string]plan.Data{"/on/execute/inputs/pw/default": plan.DataOf("${{ vars.PW }}")},
		Decisions: map[string]bool{"/on/execute/inputs/x/hidden": true, "/on/execute/inputs/y/ignore": false},
		Means:     map[string]string{"inputs.region": "/on/execute/inputs/region/default"},
		Run: &plan.RunValues{
			Format: 1,
			Inputs: plan.DataOf(map[string]any{"pw": "hunter2"}),
			Vars:   map[string]string{"TOKEN": "t0k3n"},
			Org:    map[string]string{"REGION": "east"},
		},
		TimeoutSeconds: &seconds,
		Expansions: map[string]*plan.Expansion{"build": {Instances: []plan.Instance{
			{Key: "build#0", Index: 0, Matrix: plan.DataOf(map[string]any{"region": "east"}), Private: true},
		}}},
		Jobs: map[string]plan.JobRecord{"test": {
			Outputs: map[string]string{"dsn": "postgres://u:hunter2@db"},
			Steps:   map[string]map[string]string{"login": {"token": "t0k3n"}},
		}},
	}
}

// A secret record encodes to the same bytes each time, as a plan does,
// which testdata/record.json holds, and decodes to what was encoded.
func TestRecordEncodingIsStable(t *testing.T) {
	want, err := os.ReadFile("testdata/record.json")
	if err != nil {
		t.Fatal(err)
	}
	got, err := plan.EncodeRecord(sampleRecord())
	if err != nil || !bytes.Equal(got, want) {
		t.Errorf("the sample record encodes as\n%s, %v; want\n%s", got, err, want)
	}
	back, err := plan.DecodeRecord(want)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, sampleRecord()) {
		t.Errorf("it decodes as %+v", back)
	}
	if again, err := plan.EncodeRecord(back); err != nil || !bytes.Equal(again, want) {
		t.Errorf("decoding and encoding changed it:\n%s\n%s", want, again)
	}
}

// A record of another version doesn't decode, with ErrVersion, as a plan
// doesn't; nor does what isn't a record of this one, with ErrMalformed: one
// with a key this release doesn't know, one that names no version, as an
// empty one doesn't, and one whose submitted values aren't a mapping.
func TestDecodeRecordRefuses(t *testing.T) {
	good, err := plan.EncodeRecord(sampleRecord())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(good), `"submitted":{`) {
		t.Fatalf("the sample record changed: %s", good)
	}
	for name, data := range map[string]string{
		"another version":           strings.Replace(string(good), `"version":1`, `"version":2`, 1),
		"an unknown key":            strings.Replace(string(good), `"version":1`, `"version":1,"later":true`, 1),
		"a key twice":               strings.Replace(string(good), `"version":1`, `"version":1,"version":1`, 1),
		"not JSON":                  "{",
		"empty":                     "{}",
		"null":                      "null",
		"version 0":                 strings.Replace(string(good), `"version":1`, `"version":0`, 1),
		"submitted values, a list":  `{"version":1,"submitted":[]}`,
		"submitted values, a text":  `{"version":1,"submitted":"x"}`,
		"submitted values, a value": `{"version":1,"submitted":1}`,
	} {
		r, err := plan.DecodeRecord([]byte(data))
		if err == nil {
			t.Errorf("%s: decoded as %+v", name, r)
		}
		version := name == "another version"
		if errors.Is(err, plan.ErrVersion) != version || errors.Is(err, plan.ErrMalformed) == version {
			t.Errorf("%s: %v, want ErrVersion %v and ErrMalformed %v", name, err, version, !version)
		}
	}
	if r, err := plan.DecodeRecord([]byte(`{"version":1,"submitted":null}`)); err != nil || !r.Submitted.IsZero() {
		t.Errorf("a record that holds nothing submitted: %+v, %v", r, err)
	}
}

// Split keeps the values of a job's private outputs, those Private or
// Secrets names, and of its steps' outputs that hold a secret, out of run
// state, and JoinJobResult puts them back. Neither shares a mapping or a
// list with what it was given.
func TestJobResultSplit(t *testing.T) {
	res := plan.JobResult{
		Outcome: plan.OutcomeSuccess,
		Outputs: map[string]string{"dsn": "postgres://u:hunter2@db", "count": "3", "region": "east", "key": "k3y"},
		Private: []string{"dsn", "region"},
		Secrets: []string{"dsn", "key"},
		Derived: []string{"dsn"},
		Steps: map[string]map[string]string{
			"login": {"token": "t0k3n", "user": "alice"},
			"count": {"n": "3"},
		},
		SecretSteps:  map[string][]string{"login": {"token"}},
		DerivedSteps: map[string][]string{"login": {"token"}},
	}
	record, state := res.Split()
	if want := (plan.JobRecord{
		Outputs: map[string]string{"dsn": "postgres://u:hunter2@db", "region": "east", "key": "k3y"},
		Steps:   map[string]map[string]string{"login": {"token": "t0k3n"}},
	}); !reflect.DeepEqual(record, want) {
		t.Errorf("record %+v, want %+v", record, want)
	}
	state.Private[0], state.Secrets[0], state.Derived[0] = "x", "x", "x"
	state.SecretSteps["login"][0], state.DerivedSteps["login"][0] = "x", "x"
	state.Steps["login"]["user"] = "x"
	back := plan.JoinJobResult(record, state)
	back.Steps["count"]["n"], back.Private[1] = "x", "x"
	if res.Private[0] != "dsn" || res.Secrets[0] != "dsn" || res.Derived[0] != "dsn" || res.SecretSteps["login"][0] != "token" ||
		res.DerivedSteps["login"][0] != "token" || res.Steps["login"]["user"] != "alice" || res.Steps["count"]["n"] != "3" {
		t.Errorf("Split or JoinJobResult shares what it was given: %+v", res)
	}
	record, state = res.Split()
	for _, s := range []string{"hunter2", "t0k3n", "east", "k3y"} {
		if strings.Contains(canonicalJSON(t, state), s) {
			t.Errorf("the state holds %s: %s", s, canonicalJSON(t, state))
		}
	}
	if !reflect.DeepEqual(state.Private, res.Private) || !reflect.DeepEqual(state.SecretSteps, res.SecretSteps) {
		t.Errorf("the state doesn't name what is private: %+v", state)
	}
	if back := plan.JoinJobResult(record, state); !reflect.DeepEqual(back, res) {
		t.Errorf("joined %+v, want %+v", back, res)
	}
	if res.Outputs["dsn"] == "" || res.Steps["login"]["token"] == "" {
		t.Error("Split changed the result")
	}
}

// canonicalJSON returns v's JSON.
func canonicalJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// A plan's digest is the same for the plan as for what decoding its
// encoding gives, as a coordinator stores it, and another for another
// plan.
func TestDigest(t *testing.T) {
	p := samplePlan(t)
	want, err := plan.Digest(p)
	if err != nil || !strings.HasPrefix(want, "sha256:") || len(want) != len("sha256:")+64 {
		t.Fatalf("digest %q, %v", want, err)
	}
	data, err := plan.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	back, err := plan.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := plan.Digest(back); got != want || err != nil {
		t.Errorf("the plan decoded has the digest %q, %v; want %q", got, err, want)
	}
	back.Run.Source.Revision = "sha256:01"
	if got, err := plan.Digest(back); got == want || err != nil {
		t.Errorf("another plan has the same digest %q, %v", got, err)
	}
}
