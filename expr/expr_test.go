package expr_test

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/expr"
)

func mustParse(t *testing.T, src string) *expr.Expr {
	t.Helper()
	e, err := expr.Parse(src)
	if err != nil {
		t.Fatalf("Parse(%q): %v", src, err)
	}
	return e
}

func TestRefs(t *testing.T) {
	e := mustParse(t, "inputs.a['b-c'] && needs.build.outputs.x || var.v == matrix.os ? steps.s.outputs[0] : inputs.l.[index].*.n")
	var got []string
	for _, r := range e.Refs() {
		got = append(got, fmt.Sprintf("%d %s %s", r.Offset, r.Context, strings.Join(r.Path, "/")))
	}
	want := []string{
		"0 inputs a/b-c",
		"19 needs build/outputs/x",
		"44 vars v",
		"53 matrix os",
		"65 steps s/outputs/0",
		"86 inputs l/[index]/*/n",
	}
	if !slices.Equal(got, want) {
		t.Errorf("Refs =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if mustParse(t, "inputs.a").Relative() || !mustParse(t, "!.hidden").Relative() {
		t.Error("Relative is wrong")
	}
}

func TestStatusChecks(t *testing.T) {
	for _, tc := range []struct {
		src               string
		used, afterCancel bool
	}{
		{"failure()", true, false},
		{"always()", true, true},
		{"inputs.x ? always : never", false, false},
		{"inputs.x", false, false},
		{"failure() && inputs.x", true, false},
		{"always() && inputs.x", true, true},
		{"!success() || inputs.x", true, true},
		{"!cancelled()", true, false},
		{"!failure()", true, true},
		{"!(failure() || cancelled()) && inputs.notify", true, true},
		{"format('{0}', failure())", true, false},
	} {
		used, afterCancel := mustParse(t, tc.src).StatusChecks()
		if used != tc.used || afterCancel != tc.afterCancel {
			t.Errorf("StatusChecks(%q) = %v, %v; want %v, %v", tc.src, used, afterCancel, tc.used, tc.afterCancel)
		}
	}
}

func TestCheckMessages(t *testing.T) {
	problems := mustParse(t, "input.type == x").Check()
	if len(problems) != 1 || !strings.Contains(problems[0].Msg, `input.type is the text "input.type", because input isn't a context`) {
		t.Errorf("Check = %v", problems)
	}
	for src, want := range map[string]string{
		"!completed":         "the if: keyword it was is now success()",
		"error && inputs.x":  "the if: keyword it was is now failure()",
		"inputs.x ? a : b":   `a is the text "a", which is always true; a condition needs a comparison`,
		"always":             "always is the older spelling of always(); use always()",
		"inputs.x && always": "always is the older spelling of always()",
		"always || inputs.x": "always is the older spelling of always()",
	} {
		problems := mustParse(t, src).CheckCondition()
		if len(problems) == 0 || !strings.Contains(problems[0].Msg, want) {
			t.Errorf("CheckCondition(%q) = %v, want a problem containing %q", src, problems, want)
		}
	}
}

func TestTemplateOffsets(t *testing.T) {
	_, err := expr.ParseTemplate("echo ${{ inputs.a = 1 }}")
	if se, ok := errors.AsType[*expr.SyntaxError](err); !ok || se.Offset != 18 {
		t.Errorf("syntax error = %#v, want offset 18", err)
	}
	tmpl, err := expr.ParseTemplate("a ${{ 1 }} b ${{ fromJSON 'x' }}")
	if err != nil {
		t.Fatal(err)
	}
	_, err = tmpl.Eval(expr.Scope{})
	if ee, ok := errors.AsType[*expr.EvalError](err); !ok || ee.Offset != 17 {
		t.Errorf("evaluation error = %#v, want offset 17", err)
	}
	var srcs []string
	var offsets []int
	for at, e := range tmpl.Exprs() {
		srcs, offsets = append(srcs, e.String()), append(offsets, at)
	}
	if !slices.Equal(srcs, []string{" 1 ", " fromJSON 'x' "}) || !slices.Equal(offsets, []int{5, 16}) {
		t.Errorf("Exprs = %q, %v", srcs, offsets)
	}
	if _, ok := tmpl.Expr(); ok {
		t.Error("Expr: a template with text isn't one expression")
	}
	tmpl, err = expr.ParseTemplate("x ${{ needs.a.outputs.b }}")
	if err != nil {
		t.Fatal(err)
	}
	_, err = tmpl.Eval(expr.Scope{})
	if ue, ok := errors.AsType[*expr.UnknownError](err); !ok || ue.Offset != 6 || ue.Name != "needs" || !expr.IsUnknown(err) {
		t.Errorf("unknown error = %#v, want needs at offset 6", err)
	}
	tmpl, err = expr.ParseTemplate("${{ inputs.x }}")
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := tmpl.Expr(); !ok || e.String() != " inputs.x " {
		t.Errorf("Expr = %v, %v; want inputs.x", e, ok)
	}
}

func TestFormatNumbers(t *testing.T) {
	for n, want := range map[float64]string{
		1: "1", -1: "-1", 0.5: "0.5", 1e21: "1e+21", 1e20: "100000000000000000000", 1.5e-7: "1.5e-7",
		0.000001: "0.000001", math.Copysign(0, -1): "0", math.NaN(): "NaN", math.Inf(1): "Infinity", math.Inf(-1): "-Infinity",
		0.30000000000000004: "0.30000000000000004", 123456789012: "123456789012",
	} {
		got, err := mustParse(t, "'' + inputs.n").Eval(expr.Scope{Contexts: map[string]any{"inputs": map[string]any{"n": n}}})
		if err != nil || got != want {
			t.Errorf("'' + %v = %q, %v; want %q", n, got, err, want)
		}
	}
	got, err := mustParse(t, "'' + inputs.n").Eval(expr.Scope{Contexts: map[string]any{"inputs": map[string]any{"n": int64(9007199254740993)}}})
	if err != nil || got != "9007199254740993" {
		t.Errorf("a large int64 = %q, %v; want it exact", got, err)
	}
}

func TestBuildLimit(t *testing.T) {
	big := strings.Repeat("x", 1<<20)
	src := "inputs.s" + strings.Repeat(" + inputs.s", 20)
	_, err := mustParse(t, src).Eval(expr.Scope{Contexts: map[string]any{"inputs": map[string]any{"s": big}}})
	if ee, ok := errors.AsType[*expr.EvalError](err); !ok || !strings.Contains(ee.Msg, "builds more than") {
		t.Errorf("got %v, want the build limit", err)
	}
	_, err = mustParse(t, "(range 10000) join inputs.s").Eval(expr.Scope{Contexts: map[string]any{"inputs": map[string]any{"s": big}}})
	if _, ok := errors.AsType[*expr.EvalError](err); !ok {
		t.Errorf("join: got %v, want the build limit", err)
	}
}

// Expressions that read each other many times are each evaluated once. Each
// one builds a KiB of text, so that evaluating them more than once runs into
// the budget, which they share, rather than taking 2^60 evaluations.
func TestWorkflowEvaluatesEachExpressionOnce(t *testing.T) {
	kib := strings.Repeat("x", 1<<10)
	root := map[string]any{"v0": "${{ 1 }}"}
	for i := 1; i <= 60; i++ {
		root[fmt.Sprintf("v%d", i)] = fmt.Sprintf("${{ .v%d + .v%d + ('%s' + 'x' == '' ? 1 : 0) }}", i-1, i-1, kib)
	}
	w := &expr.Workflow{Root: root}
	got, err := mustParse(t, ".v60").Eval(w.Scope([]string{"x"}, expr.Scope{}))
	if err != nil || got != math.Pow(2, 60) {
		t.Errorf("got %v, %v; want 2**60", got, err)
	}
}

func TestWorkflowErrors(t *testing.T) {
	w := &expr.Workflow{Root: map[string]any{"a": "${{ .b }}", "b": "${{ fromJSON 'x' }}", "c": "${{ needs.x.outputs.y }}"}}
	_, err := mustParse(t, "1 + .a").Eval(w.Scope([]string{"x"}, expr.Scope{}))
	if ee, ok := errors.AsType[*expr.EvalError](err); !ok || ee.Offset != 4 || !strings.Contains(ee.Msg, "in /b") {
		t.Errorf("got %#v, want an error at the reference that mentions /b", err)
	}
	_, err = mustParse(t, "1 + .c").Eval(w.Scope([]string{"x"}, expr.Scope{}))
	if ue, ok := errors.AsType[*expr.UnknownError](err); !ok || ue.Offset != 4 || ue.Name != "needs" {
		t.Errorf("got %#v, want needs unknown at the reference", err)
	}
}

// Results can be changed without changing the expression.
func TestLiteralsAreCopied(t *testing.T) {
	e := mustParse(t, `{"a": [1]}`)
	v, _ := e.Eval(expr.Scope{})
	v.(map[string]any)["a"].([]any)[0] = 2.0
	v, _ = e.Eval(expr.Scope{})
	if got := v.(map[string]any)["a"].([]any)[0]; got != 1.0 {
		t.Errorf("literal changed to %v", got)
	}
}

func FuzzExpression(f *testing.F) {
	for _, seed := range []string{
		"inputs.a == 'x' && !inputs.b", "(fromJSON inputs.j).a[0]", "1 range 5", "a ? b : c ? d : e",
		"'it''s'", "inputs.x[1:3] join ','", "!!failure", "len(inputs.l) + -2 ** 3", "[1, {\"a\": 2}] get 1",
		"inputs.x get (1):(2)", "inputs.a?1:inputs.b?2:3", "inputs.x-1", "inputs.j/2", "(fromJSON inputs.j)['a.b']",
		"needs['j-1'].outputs.o", "env['A-B']", "sessions['s']", "format('{0}{{}}', contains(inputs.x, 1))",
		"case(inputs.b, 1, 2)", "inputs.x.*.a['b']", "failure() && !cancelled()", "../../.x.[index]", "2:(len inputs.x)",
		"${{ inputs.a }} and ${{ 'b' }}", "${{ ['}}'] }}}}",
	} {
		f.Add(seed)
	}
	scope := expr.Scope{
		Contexts: map[string]any{
			"inputs": map[string]any{"a": "x", "b": false, "j": `{"a": [1]}`, "x": []any{1, 2, 3}, "l": []any{}},
			"needs":  map[string]any{"j-1": map[string]any{"outputs": map[string]any{"o": "2"}}},
			"env":    map[string]any{"A-B": "e"},
		},
		Status: &expr.Status{Failed: true},
		Index:  []int{0},
	}
	w := &expr.Workflow{Root: map[string]any{"x": "${{ .y }}", "y": []any{"${{ ..x }}"}}}
	f.Fuzz(func(t *testing.T, src string) {
		if e, err := expr.Parse(src); err == nil {
			_, _ = e.Eval(scope)
			_, _ = e.Eval(w.Scope([]string{"z", "w"}, scope))
			e.Refs()
			e.StatusChecks()
			e.CheckCondition()
		}
		if tmpl, err := expr.ParseTemplate(src); err == nil {
			_, _ = tmpl.Eval(scope)
		}
	})
}

func BenchmarkParse(b *testing.B) {
	src := "inputs.k8s.volumes.pvc_persist == false || inputs.k8s.volumes.pvc != New && 'SLURM' !== inputs.host.jobschedulertype"
	for b.Loop() {
		if _, err := expr.Parse(src); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEval(b *testing.B) {
	e, err := expr.Parse("inputs.workers get env.RETRY get slurm get partition")
	if err != nil {
		b.Fatal(err)
	}
	scope := expr.Scope{Contexts: map[string]any{
		"inputs": map[string]any{"workers": []any{map[string]any{"slurm": map[string]any{"partition": "debug"}}}},
		"env":    map[string]any{"RETRY": "0"},
	}}
	for b.Loop() {
		if _, err := e.Eval(scope); err != nil {
			b.Fatal(err)
		}
	}
}

func TestSplitLimit(t *testing.T) {
	big := strings.Repeat("x", 2<<20)
	_, err := mustParse(t, `inputs.s split ""`).Eval(expr.Scope{Contexts: map[string]any{"inputs": map[string]any{"s": big}}})
	if _, ok := errors.AsType[*expr.EvalError](err); !ok {
		t.Errorf("got %v, want the build limit", err)
	}
}

// A value that isn't known yet, such as a secret's, makes what reads it
// unknown, and leaves the rest of the context readable.
func TestUnknownValues(t *testing.T) {
	scope := expr.Scope{Contexts: map[string]any{
		"vars":   map[string]any{"PUBLIC": "p", "TOKEN": expr.Unknown{Name: "secret"}},
		"inputs": map[string]any{"list": []any{"a", expr.Unknown{Name: "secret"}}},
	}}
	for src, want := range map[string]any{
		"vars.PUBLIC":                 "p",
		"vars.MISSING":                "",
		"inputs.list[0]":              "a",
		"false && vars.TOKEN":         false,
		"vars.TOKEN":                  nil,
		"vars.TOKEN.x":                nil,
		"inputs.list":                 nil,
		"(fromJSON '[1]')[0] + 1":     2.0,
		"format('{0}', vars.TOKEN)":   nil,
		"inputs.list[1] ?? 'default'": nil,
	} {
		got, err := mustParse(t, src).Eval(scope)
		ue, unknown := errors.AsType[*expr.UnknownError](err)
		switch {
		case want == nil && (!unknown || ue.Name != "secret"):
			t.Errorf("%s = %v, %v; want the secret unknown", src, got, err)
		case want != nil && (err != nil || got != want):
			t.Errorf("%s = %v, %v; want %v", src, got, err, want)
		}
	}
}
