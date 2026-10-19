package plan_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/plan"
)

func mustValue(t *testing.T, v any) plan.Value {
	t.Helper()
	pv, err := plan.ValueOf(v)
	if err != nil {
		t.Fatal(err)
	}
	return pv
}

func samplePlan(t *testing.T) *plan.Plan {
	t.Helper()
	return &plan.Plan{
		Version: plan.Version,
		Run: plan.Run{
			Source:         plan.Source{Location: "workflow.yaml", Revision: "sha256:00"},
			Format:         1,
			Inputs:         plan.DataOf(map[string]any{"name": "Alice", "n": int64(3), "note": "${{ not.parsed }}"}),
			DeferredInputs: map[string]plan.Value{"token": mustValue(t, "${{ vars.TOKEN }}")},
			Env:            map[string]plan.Value{"GREETING": mustValue(t, "hello ${{ inputs.name }}")},
		},
		Jobs: []plan.Job{{
			Key:     "build#1",
			Name:    "build",
			Matrix:  plan.DataOf(map[string]any{"os": "linux"}),
			Needs:   []plan.Need{{Job: "prepare"}, {Job: "probe", Any: true}},
			If:      mustValue(t, "${{ always() && needs.prepare.result == 'failure' }}"),
			Timeout: mustValue(t, "1h"),
			Steps: []plan.Step{{
				ID:               "greet",
				Name:             mustValue(t, "greet on ${{ matrix.os }}"),
				Run:              mustValue(t, "echo ${{ env.GREETING }} on ${{ matrix.os }}"),
				WorkingDirectory: plan.Literal(nil),
				With:             mustValue(t, map[string]any{"count": int64(2), "list": []any{"a", "${{ inputs.n }}"}}),
				Path:             "/jobs/build/steps/0",
			}},
			Outputs: map[string]plan.Value{"out": mustValue(t, "${{ steps.greet.outputs.x }}")},
			Path:    "/jobs/build",
		}},
		Warnings: []format.Diagnostic{{
			File: "workflow.yaml", Position: format.Position{Line: 3, Column: 9}, Severity: format.Warning,
			Code: format.CodeUnknownKey, Path: "/jobs/build/x", Message: "x is ignored",
		}},
	}
}

func TestEncodeDecode(t *testing.T) {
	p := samplePlan(t)
	data, err := plan.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	again, err := plan.Encode(samplePlan(t))
	if err != nil || string(again) != string(data) {
		t.Fatalf("the same plan encodes differently: %s\n%s", data, again)
	}
	back, err := plan.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v\n%s", err, data)
	}
	reencoded, err := plan.Encode(back)
	if err != nil || string(reencoded) != string(data) {
		t.Fatalf("decoding changed the plan:\n%s\n%s", data, reencoded)
	}

	// The decoded plan's values evaluate.
	matrix := back.Jobs[0].Matrix.Get()
	inputs := back.Run.Inputs.Get()
	scope := expr.Scope{Contexts: map[string]any{
		"env":    map[string]any{"GREETING": "hi"},
		"matrix": matrix,
		"inputs": inputs,
	}}
	got, err := back.Jobs[0].Steps[0].Run.Eval(scope)
	if err != nil || got != "echo hi on linux" {
		t.Errorf("run = %v, %v", got, err)
	}
	with, err := back.Jobs[0].Steps[0].With.Eval(scope)
	if err != nil {
		t.Fatal(err)
	}
	if list := with.(map[string]any)["list"].([]any); list[1] != int64(3) {
		t.Errorf("with.list[1] = %#v, want 3", list[1])
	}
	if note := inputs.(map[string]any)["note"]; note != "${{ not.parsed }}" {
		t.Errorf("an input's value is %#v, not its text", note)
	}
	if wd := back.Jobs[0].Steps[0].WorkingDirectory; wd.IsZero() {
		t.Error("working-directory: null was lost")
	}
	if back.Warnings[0].Severity != format.Warning || !strings.Contains(string(data), `"severity":"warning"`) {
		t.Errorf("the warning's severity didn't survive: %s", data)
	}
}

func TestDecodeRefuses(t *testing.T) {
	good, err := plan.Encode(samplePlan(t))
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{
		"another version":       strings.Replace(string(good), `"version":1`, `"version":2`, 1),
		"an unknown key":        strings.Replace(string(good), `"version":1`, `"version":1,"later":true`, 1),
		"a bad tree":            strings.Replace(string(good), `"ref":"inputs"`, `"ref":"secrets"`, 1),
		"a node of two kinds":   strings.Replace(string(good), `"ref":"inputs"`, `"ref":"inputs","word":"x"`, 1),
		"a node's unknown key":  strings.Replace(string(good), `"ref":"inputs"`, `"ref":"inputs","later":1`, 1),
		"a key in another case": strings.Replace(string(good), `"jobs":`, `"JOBS":`, 1),
		"a key twice":           strings.Replace(string(good), `"version":1`, `"version":1,"version":1`, 1),
		"an unmarked $ key":     strings.Replace(string(good), `"timeout":"1h"`, `"timeout":{"$yaml":"x"}`, 1),
		"not JSON":              "{",
	} {
		if !strings.Contains(string(good), `"timeout":"1h"`) {
			t.Fatalf("the sample plan changed: %s", good)
		}
		if _, err := plan.Decode([]byte(data)); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
	_, err = plan.Decode([]byte(strings.Replace(string(good), `"version":1`, `"version":2`, 1)))
	if !errors.Is(err, plan.ErrVersion) {
		t.Errorf("another version: %v, want ErrVersion", err)
	}
}

// Data a run was given, and data in a workflow, are never taken for a
// template, whatever they hold.
func TestDataStaysData(t *testing.T) {
	stored := []any{map[string]any{"src": " vars.KEY ", "tree": map[string]any{"ref": "vars", "path": []any{"KEY"}}}}
	for name, v := range map[string]plan.Value{
		"literal":    plan.Literal(map[string]any{"cfg": map[string]any{"${{": stored}}),
		"a $ key":    plan.Literal(map[string]any{"$": map[string]any{"$yaml": "x"}}),
		"workflow":   mustValue(t, map[string]any{"${{": "${{ inputs.x }}", "list": []any{map[string]any{"$yaml": "y"}}}),
		"top level":  plan.Literal(map[string]any{"${{": stored}),
		"a template": mustValue(t, map[string]any{"$out": "${{ inputs.x }}"}),
	} {
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var back plan.Value
		if err := json.Unmarshal(data, &back); err != nil {
			t.Fatalf("%s: %v\n%s", name, err, data)
		}
		if g, w := len(back.Exprs()), len(v.Exprs()); g != w {
			t.Errorf("%s: %d expressions after a round trip, not %d: %s", name, g, w, data)
		}
		again, err := json.Marshal(back)
		if err != nil || string(again) != string(data) {
			t.Errorf("%s: a round trip changed it:\n%s\n%s", name, data, again)
		}
	}

	data, err := json.Marshal(plan.DataOf(map[string]any{"${{": stored, "n": int64(1) << 60}))
	if err != nil {
		t.Fatal(err)
	}
	var d plan.Data
	if err := json.Unmarshal(data, &d); err != nil {
		t.Fatal(err)
	}
	m := d.Get().(map[string]any)
	if _, ok := m["${{"].([]any); !ok || m["n"] != int64(1)<<60 {
		t.Errorf("Data decoded as %#v", m)
	}
}

func TestValue(t *testing.T) {
	v := mustValue(t, map[string]any{"a": "${{ inputs.x }}", "b": []any{"plain", 1.5}})
	if _, ok := v.Literal(); ok {
		t.Error("a value with a template is literal")
	}
	if len(v.Exprs()) != 1 || v.Exprs()[0].String() != " inputs.x " {
		t.Errorf("Exprs = %v", v.Exprs())
	}
	if _, err := v.Eval(expr.Scope{}); !expr.IsUnknown(err) {
		t.Errorf("Eval without inputs = %v, want inputs unknown", err)
	}
	lit := mustValue(t, map[string]any{"a": "plain"})
	if got, ok := lit.Literal(); !ok || got.(map[string]any)["a"] != "plain" {
		t.Errorf("Literal = %v, %v", got, ok)
	}
	if !(plan.Value{}).IsZero() || lit.IsZero() {
		t.Error("IsZero is wrong")
	}

	// A literal value keeps ${{ }} as text.
	data, err := json.Marshal(plan.Literal("${{ x }}"))
	if err != nil || string(data) != `"${{ x }}"` {
		t.Errorf("Literal encodes as %s, %v", data, err)
	}

	var se *expr.SyntaxError
	if _, err := plan.ValueOf([]any{"ok", "${{ 1 + }}"}); !errors.As(err, &se) || !strings.HasPrefix(err.Error(), "1: ") {
		t.Errorf("a bad expression = %v", err)
	}
	if v, err := plan.ValueOf(nil); err != nil || v.IsZero() {
		t.Errorf("ValueOf(nil) = %v, %v; null is set", v, err)
	}
}

// A Value's templates share one budget for what they build, as one
// expression does.
func TestEvalSharesABudget(t *testing.T) {
	big := strings.Repeat("x", 1<<20)
	scope := expr.Scope{Contexts: map[string]any{"inputs": map[string]any{"big": big}}}
	one := mustValue(t, "${{ inputs.big + inputs.big }}")
	if _, err := one.Eval(scope); err != nil {
		t.Fatalf("one template: %v", err)
	}
	many := mustValue(t, []any{
		"${{ inputs.big + inputs.big }}", "${{ inputs.big + inputs.big }}", "${{ inputs.big + inputs.big }}",
		"${{ inputs.big + inputs.big }}", "${{ inputs.big + inputs.big }}",
	})
	if _, err := many.Eval(scope); err == nil || !strings.Contains(err.Error(), "MiB") {
		t.Errorf("five templates that each build 2 MiB: %v, want the budget's error", err)
	}
}
