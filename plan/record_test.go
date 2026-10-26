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
		Version: plan.Version,
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

// A record of another version, or with a key this release doesn't know,
// doesn't decode, as a plan doesn't.
func TestDecodeRecordRefuses(t *testing.T) {
	good, err := plan.EncodeRecord(sampleRecord())
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{
		"another version": strings.Replace(string(good), `"version":1`, `"version":2`, 1),
		"an unknown key":  strings.Replace(string(good), `"version":1`, `"version":1,"later":true`, 1),
		"a key twice":     strings.Replace(string(good), `"version":1`, `"version":1,"version":1`, 1),
		"not JSON":        "{",
	} {
		r, err := plan.DecodeRecord([]byte(data))
		if err == nil {
			t.Errorf("%s: decoded as %+v", name, r)
		}
		if want := name == "another version"; errors.Is(err, plan.ErrVersion) != want {
			t.Errorf("%s: %v, ErrVersion %v", name, err, want)
		}
	}
}

// Split keeps the values of a job's private outputs, and of its steps'
// outputs that hold a secret, out of run state, and JoinJobResult puts
// them back.
func TestJobResultSplit(t *testing.T) {
	res := plan.JobResult{
		Outcome: plan.OutcomeSuccess,
		Outputs: map[string]string{"dsn": "postgres://u:hunter2@db", "count": "3", "region": "east"},
		Private: []string{"dsn", "region"},
		Secrets: []string{"dsn"},
		Steps: map[string]map[string]string{
			"login": {"token": "t0k3n", "user": "alice"},
			"count": {"n": "3"},
		},
		SecretSteps: map[string][]string{"login": {"token"}},
	}
	record, state := res.Split()
	if want := (plan.JobRecord{
		Outputs: map[string]string{"dsn": "postgres://u:hunter2@db", "region": "east"},
		Steps:   map[string]map[string]string{"login": {"token": "t0k3n"}},
	}); !reflect.DeepEqual(record, want) {
		t.Errorf("record %+v, want %+v", record, want)
	}
	for _, s := range []string{"hunter2", "t0k3n", "east"} {
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
