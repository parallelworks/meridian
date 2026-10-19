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
	return &plan.Plan{
		Version: plan.Version,
		Run: plan.Run{
			Source: plan.Source{Location: "workflow.yaml", Revision: "sha256:00"},
			Format: 1,
			Inputs: plan.Literal(map[string]any{"name": "Alice", "n": int64(3)}),
			Env:    map[string]plan.Value{"GREETING": mustValue(t, "hello ${{ inputs.name }}")},
		},
		Jobs: []plan.Job{{
			Key:     "build#1",
			Name:    "build",
			Matrix:  plan.Literal(map[string]any{"os": "linux"}),
			Needs:   []plan.Need{{Job: "prepare"}, {Job: "probe", Any: true}},
			If:      mustValue(t, "${{ always() && needs.prepare.result == 'failure' }}"),
			Timeout: mustValue(t, "1h"),
			Steps: []plan.Step{{
				ID:   "greet",
				Run:  mustValue(t, "echo ${{ env.GREETING }} on ${{ matrix.os }}"),
				With: mustValue(t, map[string]any{"count": int64(2), "list": []any{"a", "${{ inputs.n }}"}}),
				Path: "/jobs/build/steps/0",
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
	matrix, _ := back.Jobs[0].Matrix.Literal()
	inputs, _ := back.Run.Inputs.Literal()
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
		"another version": strings.Replace(string(good), `"version":1`, `"version":2`, 1),
		"an unknown key":  strings.Replace(string(good), `"version":1`, `"version":1,"later":true`, 1),
		"a bad tree":      strings.Replace(string(good), `"ref":"inputs"`, `"ref":"secrets"`, 1),
		"not JSON":        "{",
	} {
		if _, err := plan.Decode([]byte(data)); err == nil {
			t.Errorf("%s: decoded", name)
		}
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
	if _, err := plan.ValueOf(map[string]any{"${{": 1}); err == nil {
		t.Error("a mapping whose only key is ${{ was accepted")
	}
}
