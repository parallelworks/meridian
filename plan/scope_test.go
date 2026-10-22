package plan_test

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
)

// sampleContexts are what each phase knows, all at once.
func sampleContexts() plan.Contexts {
	return plan.Contexts{
		Run: plan.RunValues{
			Format:   1,
			Inputs:   plan.DataOf(map[string]any{"name": "Alice", "creds": []any{map[string]any{"user": "u", "pass": "p"}}}),
			Env:      map[string]string{"WHERE": "workflow", "TOKEN": "t0k"},
			Org:      map[string]string{"site": "east"},
			Vars:     map[string]string{"token": "s3cret", "region": "eu"},
			Remote:   map[string]string{"repo": "r"},
			Sessions: map[string]string{"desk": "d"},
			Private:  []string{"inputs.name"},
			Secrets:  []string{"vars.token", "inputs.creds.0.pass", "env.TOKEN"},
		},
		Needs: map[string]plan.JobResult{
			"build": {Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"o": "1"}, Steps: map[string]map[string]string{"s": {"x": "2"}}},
		},
		Status: expr.Status{Failed: true},
		Matrix: plan.DataOf(map[string]any{"os": "linux"}),
		Job:    plan.JobContext{Name: "test", Key: "test#1", Attempt: 2, Group: "g", Member: "m"},
		JobEnv: map[string]string{"WHERE": "job"},
		StepEnv: map[string]string{
			"WHERE": "step", "TOKEN": "plain",
		},
		Steps: map[string]map[string]string{"a": {"x": "3"}},
	}
}

// evalIn evaluates src, a template, in scope, and gives its value as text,
// or the context it reads that the scope doesn't have.
func evalIn(t *testing.T, src string, scope expr.Scope) string {
	t.Helper()
	tmpl, err := expr.ParseTemplate(src)
	if err != nil {
		t.Fatal(err)
	}
	v, err := tmpl.Eval(scope)
	if ue, ok := errors.AsType[*expr.UnknownError](err); ok {
		return "unknown " + ue.Name
	}
	if err != nil {
		t.Fatalf("%s: %v", src, err)
	}
	return expr.Text(v)
}

// Each phase knows its contexts and those of the phases before it, each in
// its one shape, and no later one's.
func TestNewScopePhases(t *testing.T) {
	c := sampleContexts()
	phases := []plan.Phase{
		plan.PhaseCreate, plan.PhaseStart, plan.PhaseStrategy, plan.PhaseReady, plan.PhaseJobEnv, plan.PhaseStep, plan.PhaseEnd,
	}
	for _, tc := range []struct {
		src  string
		want []string // by phase
	}{
		{
			"${{ inputs.name }} ${{ org.site }} ${{ vars.region }} ${{ remote.repo }} ${{ sessions.desk }}",
			[]string{"Alice east eu r d", "Alice east eu r d", "Alice east eu r d", "Alice east eu r d", "Alice east eu r d", "Alice east eu r d", "Alice east eu r d"},
		},
		{"${{ env.WHERE }}", []string{"unknown env", "workflow", "workflow", "workflow", "job", "step", "job"}},
		{"${{ env.TOKEN }}", []string{"unknown env", "t0k", "t0k", "t0k", "t0k", "plain", "t0k"}},
		{"${{ needs.build.result }} ${{ needs.build.outputs.o }} ${{ needs.build.steps.s.outputs.x }}", []string{
			"unknown needs", "unknown needs", "success 1 2", "success 1 2", "success 1 2", "success 1 2", "success 1 2",
		}},
		{"${{ needs.build }} ${{ needs.build.outputs }} ${{ needs.build.steps.s.result }}", []string{
			"unknown needs", "unknown needs",
			"needs.build needs.build.outputs needs.build.steps.s.result",
			"needs.build needs.build.outputs needs.build.steps.s.result",
			"needs.build needs.build.outputs needs.build.steps.s.result",
			"needs.build needs.build.outputs needs.build.steps.s.result",
			"needs.build needs.build.outputs needs.build.steps.s.result",
		}},
		{"${{ failure() }} ${{ success() }}", []string{"unknown status", "unknown status", "true false", "true false", "true false", "true false", "true false"}},
		{"${{ matrix.os }}", []string{"unknown matrix", "unknown matrix", "unknown matrix", "linux", "linux", "linux", "linux"}},
		{"${{ job }}", []string{"job", "job", "job", "job", "job", "job", "job"}},
		{"${{ job.key }}", []string{"unknown job", "unknown job", "unknown job", "unknown job", "test#1", "test#1", "test#1"}},
		{"${{ job.name }} ${{ job.attempt + 1 }} ${{ job.group }} ${{ job.member }}", []string{
			"unknown job", "unknown job", "unknown job", "unknown job", "test 3 g m", "test 3 g m", "test 3 g m",
		}},
		{"${{ steps.a.outputs.x }}", []string{"unknown steps", "unknown steps", "unknown steps", "unknown steps", "unknown steps", "3", "3"}},
		{"[${{ vars.missing }}${{ org.missing }}${{ remote.missing }}${{ sessions.missing }}]", []string{"[]", "[]", "[]", "[]", "[]", "[]", "[]"}},
		{"${{ inputs.missing == null }}", []string{"true", "true", "true", "true", "true", "true", "true"}},
		{"[${{ needs.other.outputs.x }}${{ needs.build.outputs.missing }}]", []string{"unknown needs", "unknown needs", "[]", "[]", "[]", "[]", "[]"}},
		{"${{ job.missing == null }}", []string{"unknown job", "unknown job", "unknown job", "unknown job", "true", "true", "true"}},
	} {
		for i, phase := range phases {
			if got := evalIn(t, tc.src, plan.NewScope(phase, c)); got != tc.want[i] {
				t.Errorf("%s in %v (%d) = %q, want %q", tc.src, phase, phase, got, tc.want[i])
			}
		}
	}
}

// A context that is known but empty is there all the same: a matrix job's
// matrix, and a job with no needs or steps.
func TestNewScopeEmpty(t *testing.T) {
	scope := plan.NewScope(plan.PhaseEnd, plan.Contexts{})
	for name, want := range map[string]any{
		"inputs": map[string]any{}, "org": map[string]any{}, "vars": map[string]any{}, "remote": map[string]any{},
		"sessions": map[string]any{}, "env": map[string]any{}, "needs": map[string]any{}, "matrix": map[string]any{},
		"steps": map[string]any{},
		"job":   map[string]any{"name": "", "key": "", "attempt": int64(0), "group": "", "member": ""},
	} {
		if got := fmt.Sprint(scope.Contexts[name]); got != fmt.Sprint(want) {
			t.Errorf("%s = %s, want %s", name, got, fmt.Sprint(want))
		}
	}
	if len(scope.Contexts) != 10 || scope.Status == nil || *scope.Status != (expr.Status{}) {
		t.Errorf("contexts %v, status %v", scope.Contexts, scope.Status)
	}
	if s := plan.NewScope(plan.PhaseStart, plan.Contexts{}); s.Status != nil {
		t.Errorf("Start knows the status functions")
	}
}

// A value Unknown names reads as not known yet, and a context it names
// whole isn't there.
func TestNewScopeUnknown(t *testing.T) {
	c := sampleContexts()
	c.Unknown = []string{"vars.token", "inputs.creds.0.pass", "sessions"}
	scope := plan.NewScope(plan.PhaseCreate, c)
	for src, want := range map[string]string{
		"${{ vars.token }}":            "unknown vars",
		"${{ vars.region }}":           "eu",
		"${{ inputs.creds[0].pass }}":  "unknown inputs",
		"${{ inputs.creds[0].user }}":  "u",
		"${{ sessions.desk }}":         "unknown sessions",
		"${{ length(inputs.creds) }}":  "unknown inputs",
		"${{ inputs.creds[0].user }}x": "ux",
	} {
		if got := evalIn(t, src, scope); got != want {
			t.Errorf("%s = %q, want %q", src, got, want)
		}
	}
	// The scope shares nothing with the contexts it was made from.
	if got := c.Run.Inputs.Get().(map[string]any)["creds"].([]any)[0].(map[string]any)["pass"]; got != "p" {
		t.Errorf("the run's inputs changed: pass is %v", got)
	}
}

// A scope marks secrets as secret and private, and what is private as
// private alone: org and vars whole, and what Run and the phase name. An
// env value a job's or a step's env: gives is marked only as the phase
// marks it.
func TestNewScopeMarks(t *testing.T) {
	c := sampleContexts()
	c.Private, c.Secrets = []string{"matrix"}, []string{"env.WHERE"}
	for _, tc := range []struct {
		phase plan.Phase
		src   string
		want  expr.Read
	}{
		{plan.PhaseCreate, "${{ vars.token }}", expr.Read{Marked: true, Private: true}},
		{plan.PhaseCreate, "${{ vars.region }}", expr.Read{Private: true}},
		{plan.PhaseCreate, "${{ org.site }}", expr.Read{Private: true}},
		{plan.PhaseCreate, "${{ inputs.name }}", expr.Read{Private: true}},
		{plan.PhaseCreate, "${{ inputs.creds[0].pass }}", expr.Read{Marked: true, Private: true}},
		{plan.PhaseCreate, "${{ inputs.creds[0].user }}", expr.Read{}},
		{plan.PhaseCreate, "${{ remote.repo }}", expr.Read{}},
		{plan.PhaseStart, "${{ env.TOKEN }}", expr.Read{Marked: true, Private: true}},
		{plan.PhaseReady, "${{ matrix.os }}", expr.Read{Private: true}},
		{plan.PhaseReady, "${{ env.WHERE }}", expr.Read{Marked: true, Private: true}},
		{plan.PhaseStep, "${{ env.TOKEN }}", expr.Read{}},
		{plan.PhaseEnd, "${{ env.TOKEN }}", expr.Read{Marked: true, Private: true}},
		{plan.PhaseStep, "${{ needs.build.outputs.o }} ${{ steps.a.outputs.x }} ${{ job.key }}", expr.Read{}},
	} {
		tmpl, err := expr.ParseTemplate(tc.src)
		if err != nil {
			t.Fatal(err)
		}
		if _, read, err := tmpl.EvalRead(plan.NewScope(tc.phase, c)); err != nil || read != tc.want {
			t.Errorf("%s in %v: read %+v, %v; want %+v", tc.src, tc.phase, read, err, tc.want)
		}
	}
}

// Split keeps what is private in the secret record and the rest as run
// state, which holds none of its values, and JoinRunValues puts them
// together again.
func TestSplitJoin(t *testing.T) {
	r := sampleContexts().Run
	r.Inputs = plan.DataOf(map[string]any{
		"name": "Alice", "plain": "x",
		"creds": []any{map[string]any{"user": "u", "pass": "p"}, map[string]any{"user": "v", "pass": "q"}},
		"rows":  []any{"a", "b", "c"},
	})
	r.Secrets = append(r.Secrets, "inputs.creds.1.pass", "inputs.rows.1")
	record, state := r.Split()
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"Alice", `"p"`, `"q"`, "s3cret", "eu", "east", "t0k", `"b"`} {
		if strings.Contains(string(encoded), private) {
			t.Errorf("run state holds %s: %s", private, encoded)
		}
	}
	for _, public := range []string{`"plain":"x"`, `"user":"u"`, `"user":"v"`, `["a",null,"c"]`, `"WHERE":"workflow"`, `"repo":"r"`} {
		if !strings.Contains(string(encoded), public) {
			t.Errorf("run state doesn't hold %s: %s", public, encoded)
		}
	}
	rec, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	for _, public := range []string{"plain", `"user"`, "WHERE", "repo"} {
		if strings.Contains(string(rec), public) {
			t.Errorf("the secret record holds %s: %s", public, rec)
		}
	}
	joined := plan.JoinRunValues(record, state)
	want, err := json.Marshal(r, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(joined, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Errorf("joined\n%s\nwant\n%s", got, want)
	}
	// Neither shares a list or mapping with the values split.
	state.Inputs.Get().(map[string]any)["creds"].([]any)[0].(map[string]any)["user"] = "changed"
	record.Vars["token"] = "changed"
	if r.Vars["token"] != "s3cret" || r.Inputs.Get().(map[string]any)["creds"].([]any)[0].(map[string]any)["user"] != "u" {
		t.Error("changing what Split gave changed what it split")
	}
}

// Masked gives the strings of each secret, a string as it is and each string
// in a list or a mapping, once, and no empty text: never the text of a
// boolean, a number or null, which would mask that word everywhere.
func TestRunValuesMasked(t *testing.T) {
	r := plan.RunValues{
		Inputs: plan.DataOf(map[string]any{
			"creds": []any{map[string]any{"pass": "p", "n": int64(7)}}, "pin": int64(1234), "empty": "", "pw": nil, "flag": true,
		}),
		Vars:    map[string]string{"token": "s3cret", "again": "s3cret"},
		Secrets: []string{"vars.token", "inputs.creds", "inputs.pin", "vars.again", "inputs.empty", "inputs.missing", "inputs.pw", "inputs.flag"},
	}
	want := []string{"s3cret", "p"}
	if got := r.Masked(); !slices.Equal(got, want) {
		t.Errorf("Masked() = %q, want %q", got, want)
	}
}
