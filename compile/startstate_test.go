package compile_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
)

// testStartState is a state with each of its members set.
func testStartState() *compile.StartState {
	return &compile.StartState{
		Run: plan.RunValues{
			Format:  1,
			Inputs:  plan.DataOf(map[string]any{"region": "us", "count": 2.0, "creds": []any{nil, map[string]any{"user": "b"}}}),
			Env:     map[string]string{"STAGE": "test"},
			Private: []string{"inputs.region"},
			Secrets: []string{"inputs.creds.0", "vars.TOKEN"},
		},
		TimeoutSeconds: 3600,
		Expansions: map[string]*plan.Expansion{
			"build": {Instances: []plan.Instance{{Key: "build#0", Index: 0, Matrix: plan.DataOf(map[string]any{"os": "linux"})}}, FailFast: true, MaxParallel: 2},
			"test":  {Instances: []plan.Instance{{Key: "test#0", Index: 0, Private: true}}},
		},
	}
}

// startStateJSON is testStartState's encoding, which a release that reads
// plan version 1 reads: changing it is changing the version.
const startStateJSON = `{"version":1,"run":{"format":1,"inputs":{"count":2,"creds":[null,{"user":"b"}],"region":"us"},"env":{"STAGE":"test"},` +
	`"private":["inputs.region"],"secrets":["inputs.creds.0","vars.TOKEN"]},"timeoutSeconds":3600,` +
	`"expansions":{"build":{"instances":[{"key":"build#0","index":0,"matrix":{"os":"linux"}}],"failFast":true,"maxParallel":2},` +
	`"test":{"instances":[{"key":"test#0","index":0,"matrix":null,"private":true}]}}}`

func TestStartStateEncoding(t *testing.T) {
	data, err := compile.EncodeStartState(testStartState())
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != startStateJSON {
		t.Errorf("EncodeStartState =\n%s\nwant\n%s", data, startStateJSON)
	}
	got, err := compile.DecodeStartState(data)
	if err != nil {
		t.Fatal(err)
	}
	again, err := compile.EncodeStartState(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(data) {
		t.Errorf("the state decoded encodes as\n%s\nnot\n%s", again, data)
	}
	if !reflect.DeepEqual(got.Run.Env, testStartState().Run.Env) || got.TimeoutSeconds != 3600 || len(got.Expansions) != 2 {
		t.Errorf("DecodeStartState = %+v", got)
	}
	if _, err := compile.DecodeStartState([]byte(`{"version":1,"run":{"format":1}}`)); err != nil {
		t.Errorf("a state with only what is always written: %v", err)
	}
}

// TestStartStateStrict checks that DecodeStartState reads only what
// EncodeStartState writes, of this release's version.
func TestStartStateStrict(t *testing.T) {
	for _, tc := range []struct {
		data string
		want error
	}{
		{`{"version":2,"run":{"format":1},"later":true}`, plan.ErrVersion},
		{`{"version":1,"run":{"format":1},"later":true}`, plan.ErrMalformed},
		{`{"version":1}`, plan.ErrMalformed},
		{`{"version":1,"run":null}`, plan.ErrMalformed},
		{`{"version":1,"run":{"format":0}}`, plan.ErrMalformed},
		{`{"version":1,"run":{"format":1},"timeoutSeconds":null}`, plan.ErrMalformed},
		{`{"version":1,"run":{"format":1},"expansions":{"build":null}}`, plan.ErrMalformed},
		{`{"version":1,"run":{"format":1},"expansions":{"build":{"instances":[{"key":"build#0","index":0,"matrix":{},"later":1}]}}}`, plan.ErrMalformed},
		{`{"version":1,"version":1,"run":{"format":1}}`, plan.ErrMalformed},
		{`{"run":{"format":1}}`, plan.ErrMalformed},
		{`[]`, plan.ErrMalformed},
		{`not JSON`, plan.ErrMalformed},
	} {
		if _, err := compile.DecodeStartState([]byte(tc.data)); !errors.Is(err, tc.want) {
			t.Errorf("DecodeStartState(%s) = %v, want %v", tc.data, err, tc.want)
		}
	}
	// What a state holds is never in the error.
	_, err := compile.DecodeStartState([]byte(`{"version":1,"run":{"format":1,"inputs":{"pw":"HUNTER2"}},"later":1}`))
	if err == nil || strings.Contains(err.Error(), "HUNTER2") {
		t.Errorf("DecodeStartState = %v", err)
	}
}
