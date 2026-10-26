package plan_test

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"math"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

func mustValue(t testing.TB, v any) plan.Value {
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
			Source:    plan.Source{Location: "workflow.yaml", Revision: "sha256:00"},
			Format:    1,
			Inputs:    plan.DataOf(map[string]any{"name": "Alice", "n": int64(3), "note": "${{ not.parsed }}"}),
			Env:       map[string]plan.Value{"GREETING": mustValue(t, "hello ${{ inputs.name }}")},
			Variables: []string{"org.REGION", "vars"},
			Definitions: mustValue(t, map[string]any{
				"token": map[string]any{"type": "string", "default": "${{ vars.TOKEN }}", "hidden": "${{ org.H }}"},
				"$meta": map[string]any{"$wizard": "x"},
			}),
			Withheld:     []string{"/on/execute/inputs/pw/default"},
			Decisions:    []string{"/on/execute/inputs/token/hidden", "/on/execute/inputs/x/ignore"},
			Remote:       map[string]string{"repo": "https://example.com/r", "branch": "main"},
			Sessions:     mustValue(t, map[string]any{"desktop": map[string]any{"useTLS": false, "redirect": "${{ inputs.x }}"}}),
			SessionNames: map[string]string{"desktop": "desktop-1"},
			Limits:       plan.Limits{MaxInstances: 10},
		},
		Jobs: []plan.Job{{
			Key:       "build",
			Name:      "build",
			Needs:     []plan.Need{{Job: "prepare"}, {Job: "probe", Any: true}},
			NeedsExpr: mustValue(t, "${{ fromJSON(vars.NEEDS) }}"),
			If:        mustValue(t, "${{ always() && needs.prepare.result == 'failure' }}"),
			Timeout:   mustValue(t, "1h"),
			Steps: []plan.Step{{
				ID:               "greet",
				Name:             mustValue(t, "greet on ${{ matrix.os }}"),
				If:               plan.Literal(nil),
				Run:              mustValue(t, "echo ${{ env.GREETING }} on ${{ matrix.os }}"),
				WorkingDirectory: plan.Literal(nil),
				With:             mustValue(t, map[string]any{"count": int64(2), "list": []any{"a", "${{ inputs.n }}"}}),
				Path:             "/jobs/build/steps/0",
			}},
			Outputs: map[string]plan.Value{"out": mustValue(t, "${{ steps.greet.outputs.x }}")},
			Path:    "/jobs/build",
		}},
		Warnings: []problems.Diagnostic{withoutDetail(problems.UnknownKey.At(
			"workflow.yaml", problems.Position{Line: 3, Column: 9}, "/jobs/build/x",
			problems.Params{"key": "x", "suggested": "false", "suggestion": ""},
		))},
	}
}

// edgePlan is a plan with what its encoding keeps as encoding/json always
// wrote it: <, >, &, U+2028 and U+2029 escaped, invalid UTF-8 replaced,
// numbers of each kind, nil lists and mappings, which data writes as null and
// a value as empty, marked mappings, and fields at their zero values.
func edgePlan(t testing.TB) *plan.Plan {
	text := "<a href=\"x\">Tom & Jerry</a>\u2028\u2029 \x00\x1f\t\b\f\\ é😀 bad \xff byte"
	return &plan.Plan{
		Version: plan.Version,
		Run: plan.Run{
			Source: plan.Source{Location: "workflow.yaml", Revision: "sha256:00"},
			Format: 1,
			Name:   text,
			Inputs: plan.DataOf(map[string]any{
				"text":    text,
				"numbers": []any{int64(1) << 60, 3, 1e21, 1e-7, 0.1, math.Copysign(0, -1), 123456789.0},
				"empty":   []any{[]any(nil), map[string]any(nil), []any{}, map[string]any{}},
				"${{":     []any{"stays data"},
				"$":       map[string]any{"$x": "also data"},
			}),
			Env:         map[string]plan.Value{"TEXT": mustValue(t, text+" ${{ inputs.text < 'b' && !contains(inputs.text, '&') }}")},
			Timeout:     plan.Literal(map[string]any{"$only": "marked"}),
			Secrets:     []string{"inputs.token"},
			Permissions: []string{},
		},
		Jobs: []plan.Job{{
			Key: "a", Name: "a", Path: "/jobs/a",
			Needs:            []plan.Need{{Job: "b"}, {Job: "c", Any: true}},
			Retry:            &plan.JobRetry{},
			WorkingDirectory: plan.Literal([]any{[]any(nil), map[string]any(nil), 3, int64(4), 0.5}),
		}, {
			Key: "b", Name: "b", Path: "/jobs/b",
			Strategy: &plan.Strategy{Matrix: mustValue(t, map[string]any{"x": "${{ fromJSON(inputs.text) }}", "$y": "${{ 1 }}"})},
			Steps: []plan.Step{
				{Retry: &plan.StepRetry{}, Path: "/jobs/b/steps/0"},
				{
					ID: "w", Uses: "sub.yaml", Path: "/jobs/b/steps/1",
					With:  mustValue(t, map[string]any{"$out": "${{ inputs.x }}", "list": []any{nil, true, 1.5, int64(2)}}),
					Retry: &plan.StepRetry{CleanOnRetry: plan.Literal(true), MaxRetries: plan.Literal(int64(2))},
				},
			},
			Outputs: map[string]plan.Value{},
		}},
		Warnings: []problems.Diagnostic{withoutDetail(problems.UnknownKey.At(
			"workflow.yaml", problems.Position{Line: 3, Column: 9}, "/jobs/b/<x>",
			problems.Params{"key": "<x>", "suggested": "false", "suggestion": ""},
		))},
	}
}

// withoutDetail returns d as a plan holds it: without its text, which each
// reader renders in their own language.
func withoutDetail(d problems.Diagnostic) problems.Diagnostic {
	d.Detail = ""
	return d
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
	inputs := back.Run.Inputs.Get()
	scope := expr.Scope{Contexts: map[string]any{
		"env":    map[string]any{"GREETING": "hi"},
		"matrix": map[string]any{"os": "linux"},
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
	// An if: written empty is null, which is false, where one left out is
	// success().
	if cond := back.Jobs[0].Steps[0].If; cond.IsZero() {
		t.Error("if: null was lost")
	}
	w := back.Warnings[0]
	if w.Severity() != problems.Warning || w.Message() != `The key "x" isn't known here, so it's ignored.` || w.Position() != (problems.Position{Line: 3, Column: 9}) {
		t.Errorf("the warning didn't survive: %s", data)
	}
	if strings.Contains(string(data), `"detail"`) {
		t.Errorf("the plan holds the warning's text: %s", data)
	}
	// What Start reads survives, and the definitions as the workflow
	// writes them.
	if r := back.Run; len(r.Decisions) != 2 || len(r.Withheld) != 1 {
		t.Errorf("decisions %v, withheld %v", r.Decisions, r.Withheld)
	}
	if got := back.Run.Definitions.Written(); !reflect.DeepEqual(got, map[string]any{
		"token": map[string]any{"type": "string", "default": "${{ vars.TOKEN }}", "hidden": "${{ org.H }}"},
		"$meta": map[string]any{"$wizard": "x"},
	}) {
		t.Errorf("definitions %v", got)
	}
}

// A Value as the workflow writes it holds each template as its text, and
// the rest as it is; it is a copy.
func TestValueWritten(t *testing.T) {
	v := mustValue(t, map[string]any{"a": "x ${{ inputs.a }} y", "b": []any{"${{ 1 }}", nil, 2.5}, "$c": "c"})
	got := v.Written().(map[string]any)
	want := map[string]any{"a": "x ${{ inputs.a }} y", "b": []any{"${{ 1 }}", nil, 2.5}, "$c": "c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Written() = %v, want %v", got, want)
	}
	got["b"].([]any)[0] = "changed"
	if again := v.Written().(map[string]any); again["b"].([]any)[0] != "${{ 1 }}" {
		t.Error("Written shares the Value's lists")
	}
	if (plan.Value{}).Written() != nil {
		t.Error("an unset Value isn't nothing")
	}
}

// The workflow's timeout:, as Start evaluates it, is read as a job's is,
// and a problem with one made from a variable doesn't show it.
func TestRunTimeout(t *testing.T) {
	scope := plan.NewScope(plan.PhaseStart, plan.Contexts{Run: plan.RunValues{
		Org: map[string]string{"T": "soon", "U": "2h"}, Inputs: plan.DataOf(map[string]any{"m": int64(5)}),
	}})
	for _, tc := range []struct {
		v       plan.Value
		seconds int64
		private bool
		problem string
	}{
		{plan.Value{}, 0, false, ""},
		{plan.Literal("10m"), 600, false, ""},
		{mustValue(t, "${{ inputs.m * 60 }}"), 300, false, ""},
		{mustValue(t, "${{ org.U }}"), 7200, true, ""},
		{mustValue(t, "${{ org.T }}"), 0, true, "duration_invalid"},
		{plan.Literal(1.5), 0, false, "duration_invalid"},
	} {
		seconds, read, diags := plan.RunTimeout(tc.v, scope)
		if seconds != tc.seconds || read.Private != tc.private || len(diags) != min(len(tc.problem), 1) {
			t.Errorf("%v: %d, %v, %v", tc.v.Written(), seconds, read, diags)
			continue
		}
		if tc.problem == "" {
			continue
		}
		d := diags[0]
		if string(d.Code) != tc.problem || d.Path() != "/timeout" || tc.private && d.Params["value"] != problems.Masked {
			t.Errorf("%v: %s at %s, %v", tc.v.Written(), d.Code, d.Path(), d.Params)
		}
	}
}

// Plans encode to the bytes they always have: testdata holds what Meridian
// wrote before it moved to encoding/json/v2, for the edge plan and for a
// public workflow's plan. Each decodes, and encodes the same.
func TestEncodingIsStable(t *testing.T) {
	want, err := os.ReadFile("testdata/edges.json")
	if err != nil {
		t.Fatal(err)
	}
	got, err := plan.Encode(edgePlan(t))
	if err != nil || !bytes.Equal(got, want) {
		t.Errorf("the edge plan encodes as\n%s, %v; want\n%s", got, err, want)
	}
	for _, name := range []string{"edges", "spack-builder"} {
		data, err := os.ReadFile("testdata/" + name + ".json")
		if err != nil {
			t.Fatal(err)
		}
		p, err := plan.Decode(data)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		// -0 is whole, so it decodes as the int64 0, as it always has.
		want := bytes.Replace(data, []byte(",-0,"), []byte(",0,"), 1)
		if again, err := plan.Encode(p); err != nil || !bytes.Equal(again, want) {
			t.Errorf("%s: decoding and encoding changed it:\n%s\n%s", name, want, again)
		}
	}
	for name, p := range map[string]*plan.Plan{
		"NaN in a value": {Version: plan.Version, Run: plan.Run{Timeout: plan.Literal(math.NaN())}},
		"NaN in data":    {Version: plan.Version, Run: plan.Run{Inputs: plan.DataOf([]any{math.Inf(1)})}},
	} {
		if _, err := plan.Encode(p); err == nil {
			t.Errorf("%s: encoded", name)
		}
	}
}

func TestDecodeRefuses(t *testing.T) {
	good, err := plan.Encode(samplePlan(t))
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{
		"another version":                  strings.Replace(string(good), `"version":1`, `"version":2`, 1),
		"an unknown key":                   strings.Replace(string(good), `"version":1`, `"version":1,"later":true`, 1),
		"a bad tree":                       strings.Replace(string(good), `"ref":"inputs"`, `"ref":"secrets"`, 1),
		"a node of two kinds":              strings.Replace(string(good), `"ref":"inputs"`, `"ref":"inputs","word":"x"`, 1),
		"a node's unknown key":             strings.Replace(string(good), `"ref":"inputs"`, `"ref":"inputs","later":1`, 1),
		"a key in another case":            strings.Replace(string(good), `"jobs":`, `"JOBS":`, 1),
		"a key twice":                      strings.Replace(string(good), `"version":1`, `"version":1,"version":1`, 1),
		"an unmarked $ key":                strings.Replace(string(good), `"timeout":"1h"`, `"timeout":{"$yaml":"x"}`, 1),
		"not JSON":                         "{",
		"a key twice in a value":           strings.Replace(string(good), `"timeout":"1h"`, `"timeout":{"a":1,"a":2}`, 1),
		"a key twice in data":              strings.Replace(string(good), `"n":3`, `"n":3,"n":3`, 1),
		"a key twice in a tree":            strings.Replace(string(good), `"ref":"inputs"`, `"ref":"inputs","ref":"inputs"`, 1),
		"invalid UTF-8":                    strings.Replace(string(good), `"timeout":"1h"`, "\"timeout\":\"\xff\"", 1),
		"a lone surrogate":                 strings.Replace(string(good), `"timeout":"1h"`, `"timeout":"\ud800"`, 1),
		"a number out of range":            strings.Replace(string(good), `"timeout":"1h"`, `"timeout":1e400`, 1),
		"parts that aren't a list":         strings.Replace(string(good), `"timeout":"1h"`, `"timeout":{"${{":"x"}`, 1),
		"a template without an expression": strings.Replace(string(good), `"timeout":"1h"`, `"timeout":{"${{":["x"]}`, 1),
		"a bad template beside a key":      strings.Replace(string(good), `"timeout":"1h"`, `"timeout":{"$x":{"${{":"x"},"y":1}`, 1),
		"a $ that marks nothing":           strings.Replace(string(good), `"timeout":"1h"`, `"timeout":{"$":{"yaml":"x"}}`, 1),
		"a $ that marks two keys":          strings.Replace(string(good), `"timeout":"1h"`, `"timeout":{"$":{"$a":1,"$b":2}}`, 1),
		"a $ that marks an empty mapping":  strings.Replace(string(good), `"timeout":"1h"`, `"timeout":{"$":{}}`, 1),
		// The value of a first key that starts with $, beside another.
		"a $ that marks nothing, nested":             strings.Replace(string(good), `"timeout":"1h"`, `"timeout":{"$x":[{"$":{"yaml":"x"}}],"y":1}`, 1),
		"an unmarked $ key, nested":                  strings.Replace(string(good), `"timeout":"1h"`, `"timeout":{"$x":{"$yaml":"x"},"y":1}`, 1),
		"a number out of range, nested":              strings.Replace(string(good), `"timeout":"1h"`, `"timeout":{"$x":{"a":1e400},"y":1}`, 1),
		"a template without an expression, in parts": strings.Replace(string(good), `"timeout":"1h"`, `"timeout":{"${{":[{"${{":["x"]}],"y":1}`, 1),
		"a key twice, nested":                        strings.Replace(string(good), `"timeout":"1h"`, `"timeout":{"$x":{"a":1,"a":2},"y":1}`, 1),
		"a version that isn't a number":              strings.Replace(string(good), `"version":1`, `"version":"1"`, 1),
	} {
		if !strings.Contains(string(good), `"timeout":"1h"`) {
			t.Fatalf("the sample plan changed: %s", good)
		}
		_, err := plan.Decode([]byte(data))
		if err == nil {
			t.Errorf("%s: decoded", name)
		}
		if want := name != "another version"; errors.Is(err, plan.ErrMalformed) != want {
			t.Errorf("%s: %v, ErrMalformed %v", name, err, want)
		}
	}
	for name, data := range map[string]string{
		"another version":             strings.Replace(string(good), `"version":1`, `"version":2`, 1),
		"another version's keys":      strings.Replace(string(good), `"version":1`, `"version":2,"later":true`, 1),
		"another version, last":       strings.Replace(string(good), `"version":1,`, ``, 1)[:len(good)-len(`"version":1,`)-1] + `,"later":{},"version":2}`,
		"another version's templates": strings.Replace(strings.Replace(string(good), `"version":1`, `"version":2`, 1), `"ref":"inputs"`, `"ref":"later"`, 1),
	} {
		if _, err := plan.Decode([]byte(data)); !errors.Is(err, plan.ErrVersion) {
			t.Errorf("%s: %v, want ErrVersion", name, err)
		}
	}
	// What names no release's version is no release's plan.
	for name, data := range map[string]string{
		"no version": strings.Replace(string(good), `"version":1,`, ``, 1),
		"version 0":  strings.Replace(string(good), `"version":1`, `"version":0`, 1),
		"empty":      `{}`,
		"null":       `null`,
		"a list":     `[]`,
	} {
		if _, err := plan.Decode([]byte(data)); !errors.Is(err, plan.ErrMalformed) || errors.Is(err, plan.ErrVersion) {
			t.Errorf("%s: %v, want ErrMalformed", name, err)
		}
	}
}

// A plan's jobs are in the order of their names, each once, as Ready
// finds a job by its name in that order: Decode refuses a plan whose jobs
// aren't.
func TestDecodeJobOrder(t *testing.T) {
	for _, c := range []struct {
		names []string
		ok    bool
	}{
		{nil, true},
		{[]string{"a"}, true},
		{[]string{"A", "B", "a", "a-b", "b"}, true},
		{[]string{"b", "a"}, false},
		{[]string{"a", "a"}, false},
		{[]string{"b", "a", "a"}, false},
		{[]string{"a", "c", "b"}, false},
	} {
		p := &plan.Plan{Version: plan.Version, Run: plan.Run{Format: 1, Source: plan.Source{Location: "w", Revision: "r"}}}
		for _, name := range c.names {
			p.Jobs = append(p.Jobs, plan.Job{Key: name, Name: name, Path: "/jobs/" + name, Steps: []plan.Step{{Path: "/jobs/" + name + "/steps/0", Run: mustValue(t, "true")}}})
		}
		data, err := plan.Encode(p)
		if err != nil {
			t.Fatal(err)
		}
		back, err := plan.Decode(data)
		if (err == nil) != c.ok {
			t.Errorf("%v: decoded %t, want %t: %v", c.names, err == nil, c.ok, err)
		}
		if err != nil && !errors.Is(err, plan.ErrMalformed) {
			t.Errorf("%v: %v, want ErrMalformed", c.names, err)
		}
		if err == nil && len(c.names) > 0 {
			if r, _, err := back.Ready(t.Context(), c.names[len(c.names)-1], plan.ReadyInput{}); err != nil || r.Job == nil {
				t.Errorf("%v: readying the last job: %v", c.names, err)
			}
		}
	}
}

// A plan's keys can come in any order.
func TestDecodeKeyOrder(t *testing.T) {
	data := `{"jobs":[{"steps":[{"path":"/jobs/a/steps/0","run":{"${{":[{"tree":{"ref":"inputs","path":["x"]},"src":" inputs.x "}]}}],` +
		`"path":"/jobs/a","name":"a","key":"a"}],"run":{"format":1,"source":{"revision":"r","location":"w"}},"version":1}`
	p, err := plan.Decode([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := p.Jobs[0].Steps[0].Run.Eval(expr.Scope{Contexts: map[string]any{"inputs": map[string]any{"x": "y"}}}); err != nil || got != "y" {
		t.Errorf("run = %v, %v", got, err)
	}
}

// A Value or Data decoded on its own is as strict as in a plan.
func TestValueDecodingIsStrict(t *testing.T) {
	for _, data := range []string{`{"a":1,"a":2}`, "\"\xff\"", `"\ud800"`, `[{"${{":[{"src":"","tree":{"lit":1,"lit":2}}]}]`, `1 2`} {
		var v plan.Value
		if err := v.UnmarshalJSON([]byte(data)); err == nil {
			t.Errorf("Value %s: decoded", data)
		}
		var d plan.Data
		if err := d.UnmarshalJSON([]byte(data)); err == nil {
			t.Errorf("Data %s: decoded", data)
		}
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
		data, err := v.MarshalJSON()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var back plan.Value
		if err := back.UnmarshalJSON(data); err != nil {
			t.Fatalf("%s: %v\n%s", name, err, data)
		}
		if g, w := len(back.Exprs()), len(v.Exprs()); g != w {
			t.Errorf("%s: %d expressions after a round trip, not %d: %s", name, g, w, data)
		}
		again, err := back.MarshalJSON()
		if err != nil || string(again) != string(data) {
			t.Errorf("%s: a round trip changed it:\n%s\n%s", name, data, again)
		}
	}

	data, err := plan.DataOf(map[string]any{"${{": stored, "n": int64(1) << 60}).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var d plan.Data
	if err := d.UnmarshalJSON(data); err != nil {
		t.Fatal(err)
	}
	m := d.Get().(map[string]any)
	if _, ok := m["${{"].([]any); !ok || m["n"] != int64(1)<<60 {
		t.Errorf("Data decoded as %#v", m)
	}
}

// nest returns leaf inside depth mappings, each holding the next under key
// beside the keys and values of others.
func nest(depth int, key string, leaf any, others ...any) any {
	v := leaf
	for range depth {
		m := map[string]any{key: v}
		for i := 0; i < len(others); i += 2 {
			m[others[i].(string)] = others[i+1]
		}
		v = m
	}
	return v
}

// Mappings nested in the value of a mapping's first key that starts with $,
// whose meaning is known only once the mapping ends, decode in one pass:
// as deeply as a plan can nest them, as a mapping's only key, beside other
// keys, and as a template's key.
func TestDecodeNestedMarks(t *testing.T) {
	// A list under each ${{ is read as a template's parts until the
	// mapping goes on.
	var templateKeys any = []any{"x"}
	for range plan.MaxDepth/2 - 3 {
		templateKeys = []any{map[string]any{"${{": templateKeys, "b": int64(1)}}
	}
	for name, v := range map[string]any{
		"only keys":        nest(plan.MaxDepth/2-3, "$a", int64(1)),
		"first keys":       nest(plan.MaxDepth-6, "$a", int64(1), "b", int64(1)),
		"template keys":    templateKeys,
		"templates within": nest(plan.MaxDepth/2-5, "$a", map[string]any{"$x": "${{ inputs.x }}"}, "b", int64(1)),
	} {
		value, err := plan.ValueOf(v)
		if err != nil {
			t.Fatal(err)
		}
		p := &plan.Plan{Version: plan.Version, Jobs: []plan.Job{{Steps: []plan.Step{{With: value}}}}}
		data, err := plan.Encode(p)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		back, err := plan.Decode(data)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if again, err := plan.Encode(back); err != nil || !bytes.Equal(again, data) {
			t.Errorf("%s: decoding and encoding changed it (%v)", name, err)
		}
		if got, want := len(back.Exprs()), len(p.Exprs()); got != want || name == "templates within" && got == 0 {
			t.Errorf("%s: %d expressions after a round trip, not %d", name, got, want)
		}
	}
}

// A Value's and Data's Depth is how many levels their JSON nests, and a
// plan holds as many as MaxDepth.
func TestDepth(t *testing.T) {
	deepLiteral := "${{ " + strings.Repeat("[", 50) + strings.Repeat("]", 50) + " }}"
	for _, v := range []plan.Value{
		plan.Literal("text"), plan.Literal(nil), plan.Literal(int64(1)),
		plan.Literal([]any(nil)), plan.Literal(map[string]any(nil)), plan.Literal([]any{}),
		plan.Literal([]any{[]any{1, []any{}}, map[string]any{"a": map[string]any{}}}),
		plan.Literal(map[string]any{"$only": map[string]any{"$only": "x"}}),
		plan.Literal(map[string]any{"$first": map[string]any{"$only": "x"}, "b": 1}),
		plan.Literal([]string{"a"}),
		mustValue(t, "${{ inputs.x }}"),
		mustValue(t, map[string]any{"$out": deepLiteral, "list": []any{"${{ fromJSON('[]') }}"}}),
	} {
		data, err := v.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := v.Depth(), jsonDepth(t, data); got != want {
			t.Errorf("Depth of %s = %d, want %d", data, got, want)
		}
	}
	for _, d := range []plan.Data{
		plan.DataOf(nil), plan.DataOf("x"), plan.DataOf([]any(nil)), plan.DataOf(map[string]any(nil)),
		plan.DataOf([]any{[]any(nil), map[string]any{"$": map[string]any{"$x": []any{}}}}),
	} {
		data, err := d.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := d.Depth(), jsonDepth(t, data); got != want {
			t.Errorf("Depth of %s = %d, want %d", data, got, want)
		}
	}

	// A step's with: is five levels deep in a plan: in the plan, its jobs,
	// a job, its steps and a step.
	for name, tc := range map[string]struct {
		value func(extra int) any
		step  int
	}{
		"lists":           {func(extra int) any { return nestList(plan.MaxDepth - 5 + extra) }, 1},
		"marked mappings": {func(extra int) any { return nest((plan.MaxDepth-5)/2+extra, "$a", int64(1)) }, 2},
	} {
		at := plan.Literal(tc.value(0))
		past := plan.Literal(tc.value(1))
		if 5+at.Depth() > plan.MaxDepth || 5+past.Depth() <= plan.MaxDepth || past.Depth()-at.Depth() != tc.step {
			t.Fatalf("%s: depths %d and %d", name, at.Depth(), past.Depth())
		}
		data, err := plan.Encode(&plan.Plan{Version: plan.Version, Jobs: []plan.Job{{Steps: []plan.Step{{With: at}}}}})
		if err != nil {
			t.Errorf("%s: a plan as deep as MaxDepth: %v", name, err)
		} else if _, err := plan.Decode(data); err != nil {
			t.Errorf("%s: a plan as deep as MaxDepth doesn't decode: %v", name, err)
		}
		if _, err := plan.Encode(&plan.Plan{Version: plan.Version, Jobs: []plan.Job{{Steps: []plan.Step{{With: past}}}}}); err == nil {
			t.Errorf("%s: a plan deeper than MaxDepth encoded", name)
		}
	}
}

func nestList(depth int) any {
	var v any = int64(1)
	for range depth {
		v = []any{v}
	}
	return v
}

// jsonDepth returns how many levels JSON nests.
func jsonDepth(t *testing.T, data []byte) int {
	t.Helper()
	dec := jsontext.NewDecoder(bytes.NewReader(data))
	deepest := 0
	for {
		if _, err := dec.ReadToken(); err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			return deepest
		}
		deepest = max(deepest, dec.StackDepth())
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
	data, err := plan.Literal("${{ x }}").MarshalJSON()
	if err != nil || string(data) != `"${{ x }}"` {
		t.Errorf("Literal encodes as %s, %v", data, err)
	}

	_, err = plan.ValueOf([]any{"ok", "${{ 1 + }}"})
	if _, ok := errors.AsType[*expr.SyntaxError](err); !ok || !strings.HasPrefix(err.Error(), "1: ") {
		t.Errorf("a bad expression = %v", err)
	}
	if v, err := plan.ValueOf(nil); err != nil || v.IsZero() {
		t.Errorf("ValueOf(nil) = %v, %v; null is set", v, err)
	}
}

// A Value is marked if one of its templates reads a value its scope marks.
func TestValueMarked(t *testing.T) {
	scope := expr.Scope{
		Contexts: map[string]any{"vars": map[string]any{"token": "t", "region": "r"}},
		Marked:   expr.NewMarks([]string{"vars", "token"}),
	}
	for v, want := range map[string]bool{
		`{"a": "${{ vars.region }}", "b": ["${{ vars.token * 2 }}"]}`: true,
		`{"a": "${{ vars.region }}", "b": ["vars.token"]}`:            false,
	} {
		var raw any
		if err := json.Unmarshal([]byte(v), &raw); err != nil {
			t.Fatal(err)
		}
		got, marked, err := mustValue(t, raw).EvalMarked(scope)
		if err != nil || marked != want {
			t.Errorf("%s = %v, %v, marked %v; want marked %v", v, got, err, marked, want)
		}
	}
}

// A Value reports what its templates read of each of its scope's mark sets
// apart, all of them together, and what one that failed read before it
// failed.
func TestValueRead(t *testing.T) {
	scope := expr.Scope{
		Contexts: map[string]any{"vars": map[string]any{"token": "t", "region": "r"}},
		Marked:   expr.NewMarks([]string{"vars", "token"}),
		Private:  expr.NewMarks([]string{"vars"}),
	}
	for v, want := range map[string]expr.Read{
		`{"a": "${{ vars.region }}", "b": ["${{ vars.token * 2 }}"]}`: {Marked: true, Private: true},
		`{"a": "${{ vars.region }}", "b": ["vars.token"]}`:            {Private: true},
		`["${{ 1 }}", "text"]`:                          {},
		`["${{ vars.token }}", "${{ fromJSON('x') }}"]`: {Marked: true, Private: true},
		`["${{ fromJSON(vars.region) }}"]`:              {Private: true},
	} {
		var raw any
		if err := json.Unmarshal([]byte(v), &raw); err != nil {
			t.Fatal(err)
		}
		got, read, err := mustValue(t, raw).EvalRead(scope)
		if read != want {
			t.Errorf("%s = %v, %v, read %+v; want %+v", v, got, err, read, want)
		}
	}
}

// A Value's templates are one group. Each has the language's limits on
// what it builds, 8 MiB, and the work it does, 32 MiB, and together they
// share Meridian's budget for a group, 64 MiB and 256 MiB.
func TestEvalGroupsTemplates(t *testing.T) {
	scope := expr.Scope{Contexts: map[string]any{"inputs": map[string]any{
		"five": strings.Repeat("x", 5<<20),
		"four": strings.Repeat("x", 4<<20),
		"half": strings.Repeat("x", 16<<20-20),
	}}}
	two := mustValue(t, []any{"${{ inputs.five + '' }}", "${{ inputs.five + '' }}"})
	if _, err := two.Eval(scope); err != nil {
		t.Errorf("two templates that each build 5 MiB: %v", err)
	}
	nine := mustValue(t, "${{ inputs.five + inputs.four }}")
	if _, err := nine.Eval(scope); !hasKind(err, problems.ExpressionTooLarge) {
		t.Errorf("a template that builds 9 MiB: %v, want %s", err, problems.ExpressionTooLarge.Code())
	}

	// Eight templates that each build 8 MiB, the most one can, build the
	// group's 64 MiB, and a byte more is too much.
	eight := make([]any, 8)
	for i := range eight {
		eight[i] = "${{ inputs.four + inputs.four }}"
	}
	if _, err := mustValue(t, eight).Eval(scope); err != nil {
		t.Errorf("eight templates that each build 8 MiB: %v", err)
	}
	_, err := mustValue(t, append(eight, "${{ 'x' + '' }}")).Eval(scope)
	if be, ok := errors.AsType[*expr.BudgetError](err); !ok || be.Kind != problems.ExpressionsBuildTooMuch {
		t.Errorf("and a byte more: %v, want %s", err, problems.ExpressionsBuildTooMuch.Code())
	}

	// Eight templates that each do 32 MiB of work, the most one can, do the
	// group's 256 MiB, and a step more is too much. Each is ==, two
	// references and half read twice.
	for i := range eight {
		eight[i] = "${{ inputs.half == inputs.half }}"
	}
	if _, err := mustValue(t, eight).Eval(scope); err != nil {
		t.Errorf("eight templates that each do 32 MiB of work: %v", err)
	}
	_, err = mustValue(t, append(eight, "${{ 1 }}")).Eval(scope)
	if be, ok := errors.AsType[*expr.BudgetError](err); !ok || be.Kind != problems.ExpressionsTooMuchWork {
		t.Errorf("and a step more: %v, want %s", err, problems.ExpressionsTooMuchWork.Code())
	}
}

func hasKind(err error, k *problems.Kind) bool {
	ee, ok := errors.AsType[*expr.EvalError](err)
	return ok && ee.Kind == k
}

// passThrough is a platform that takes every value it's given as it is.
type passThrough struct{}

func (passThrough) ResolveInput(_ context.Context, _ string, _ map[string]any, v any) (any, error) {
	return v, nil
}

// benchmarkPlans returns plans to encode and decode: a public workflow's;
// one from a workflow as large as a file can be whose expressions each nest
// as deeply as they can, 200 levels; and one from a file as large whose
// step's with: nests mappings in the value of a first key that starts with
// $, each one in the next, 8,527 levels.
func benchmarkPlans(b *testing.B) map[string]*plan.Plan {
	b.Helper()
	spack, err := (&conformance.Case{Name: "format/public-spack-builder"}).Workflow()
	if err != nil {
		b.Fatal(err)
	}
	var deep strings.Builder
	deep.WriteString("jobs:\n  deep:\n    steps:\n")
	step := "      - run: ${{ " + strings.Repeat("1+", 200) + "1 }}\n"
	for deep.Len()+len(step) <= format.DefaultMaxSize {
		deep.WriteString(step)
	}
	head := "jobs:\n  marked:\n    steps:\n      - uses: marketplace/x\n        with: {k: "
	levels := (format.DefaultMaxSize - len(head) - len("1}\n")) / len("{$a: , b: 1}")
	marked := head + strings.Repeat("{$a: ", levels) + "1" + strings.Repeat(", b: 1}", levels) + "}\n"
	plans := map[string]*plan.Plan{}
	for name, w := range map[string]struct {
		data   []byte
		inputs map[string]any
	}{
		"spack-builder": {spack, map[string]any{"cluster": map[string]any{"resource": "x"}}},
		"deep":          {[]byte(deep.String()), nil},
		"marked":        {[]byte(marked), nil},
	} {
		p, diags := compile.Compile("workflow.yaml", w.data, w.inputs, compile.Options{Inputs: passThrough{}})
		if p == nil {
			b.Fatalf("%s: %v", name, diags)
		}
		plans[name] = p
	}
	return plans
}

func BenchmarkEncode(b *testing.B) {
	for name, p := range benchmarkPlans(b) {
		b.Run(name, func(b *testing.B) {
			data, err := plan.Encode(p)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := plan.Encode(p); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDecode(b *testing.B) {
	for name, p := range benchmarkPlans(b) {
		b.Run(name, func(b *testing.B) {
			data, err := plan.Encode(p)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := plan.Decode(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Every plan the compiler makes decodes, and encodes as it did.
func FuzzPlanRoundTrip(f *testing.F) {
	for _, area := range []string{"compile", "format", "inputs"} {
		cases, err := conformance.Cases(area)
		if err != nil {
			f.Fatal(err)
		}
		for _, c := range cases {
			data, err := c.Workflow()
			if err != nil {
				f.Fatal(err)
			}
			f.Add(data)
		}
	}
	f.Fuzz(func(t *testing.T, workflow []byte) {
		p, _ := compile.Compile("workflow.yaml", workflow, nil, compile.Options{Inputs: passThrough{}})
		roundTrip(t, p)
	})
}

// Every plan the compiler makes with values submitted for a run decodes,
// and encodes as it did: here value, as JSON if it is JSON and as text if
// it isn't, for each of the workflow's inputs.
func FuzzPlanRoundTripWithValues(f *testing.F) {
	values := []string{"text", "", "3", "true", "null", `"22"`, `[{"name": "a"}]`, `{"name": "x", "size": 2}`, `{"${{": [" vars.KEY "]}`}
	i := 0
	for _, area := range []string{"compile", "format", "inputs"} {
		cases, err := conformance.Cases(area)
		if err != nil {
			f.Fatal(err)
		}
		for _, c := range cases {
			data, err := c.Workflow()
			if err != nil {
				f.Fatal(err)
			}
			f.Add(data, values[i%len(values)])
			i++
		}
	}
	f.Fuzz(func(t *testing.T, workflow []byte, value string) {
		doc, _ := format.Load("workflow.yaml", workflow)
		if doc == nil {
			return
		}
		var v any = value
		if err := json.Unmarshal([]byte(value), &v); err != nil {
			v = value
		}
		submitted := map[string]any{}
		on, _ := doc.Value["on"].(map[string]any)
		execute, _ := on["execute"].(map[string]any)
		inputs, _ := execute["inputs"].(map[string]any)
		for name := range inputs {
			submitted[name] = v
		}
		p, _ := compile.Compile("workflow.yaml", workflow, submitted, compile.Options{Inputs: passThrough{}})
		roundTrip(t, p)
	})
}

// roundTrip checks that p, if there is one, encodes, and decodes to a plan
// that encodes to the same bytes.
func roundTrip(t *testing.T, p *plan.Plan) {
	t.Helper()
	if p == nil {
		return
	}
	data, err := plan.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	back, err := plan.Decode(data)
	if err != nil {
		t.Fatalf("%v\n%s", err, data)
	}
	if again, err := plan.Encode(back); err != nil || !bytes.Equal(again, data) {
		t.Fatalf("decoding and encoding changed the plan:\n%s\n%s (%v)", data, again, err)
	}
}

// FuzzDecode checks that Decode never panics, whatever it is given, and
// that a plan it decodes encodes, and decodes again to a plan that encodes
// to the same bytes.
func FuzzDecode(f *testing.F) {
	for _, name := range []string{"edges", "spack-builder"} {
		data, err := os.ReadFile("testdata/" + name + ".json")
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	f.Add([]byte(`{"version":1,"run":{"source":{"location":"w","revision":"r"},"format":1,"timeout":{"$":{"$a":1}}},"jobs":[]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := plan.Decode(data)
		if err != nil {
			return
		}
		encoded, err := plan.Encode(p)
		if err != nil {
			t.Fatalf("a decoded plan doesn't encode: %v\n%s", err, data)
		}
		back, err := plan.Decode(encoded)
		if err != nil {
			t.Fatalf("an encoded plan doesn't decode: %v\n%s", err, encoded)
		}
		if again, err := plan.Encode(back); err != nil || !bytes.Equal(again, encoded) {
			t.Fatalf("decoding and encoding changed the plan:\n%s\n%s (%v)", encoded, again, err)
		}
	})
}

// Exprs finds the expressions of every Value a plan holds: every Value
// field of the run, a job and a step, found by reflection, holds one that
// reads a variable named after it.
func TestExprsReachEveryValue(t *testing.T) {
	value := reflect.TypeFor[plan.Value]()
	var want []string
	var fill func(v reflect.Value, path string)
	fill = func(v reflect.Value, path string) {
		switch typ := v.Type(); {
		case typ == value:
			want = append(want, path)
			v.Set(reflect.ValueOf(mustValue(t, "${{ vars['"+path+"'] }}")))
		case typ.Kind() == reflect.Struct:
			for i := range typ.NumField() {
				if typ.Field(i).IsExported() {
					fill(v.Field(i), path+"."+typ.Field(i).Name)
				}
			}
		case typ.Kind() == reflect.Pointer:
			v.Set(reflect.New(typ.Elem()))
			fill(v.Elem(), path)
		case typ.Kind() == reflect.Slice && typ.Elem().Kind() == reflect.Struct:
			v.Set(reflect.MakeSlice(typ, 1, 1))
			fill(v.Index(0), path)
		case typ.Kind() == reflect.Map && typ.Elem() == value:
			item := reflect.New(value).Elem()
			fill(item, path)
			v.Set(reflect.MakeMap(typ))
			v.SetMapIndex(reflect.ValueOf("k"), item)
		}
	}
	var p plan.Plan
	fill(reflect.ValueOf(&p).Elem(), "plan")

	var got []string
	for _, e := range p.Exprs() {
		for _, r := range e.Refs() {
			got = append(got, r.Path[0])
		}
	}
	slices.Sort(got)
	slices.Sort(want)
	if len(want) < 25 || !slices.Equal(got, want) {
		t.Errorf("Exprs reads\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// Check refuses what format 1 refuses of a reference to a workflow in Git,
// as ACTIVATE does, once it is normal, and says nothing of any other.
func TestSourceRefCheck(t *testing.T) {
	// What is wrong: the reference, its $yaml or its $host, or nothing.
	const invalid, yaml, host = "ref", "$yaml", "$host"
	for _, tc := range []struct {
		ref  plan.SourceRef
		want string
	}{
		{plan.SourceRef{Uses: "github/acme/flows"}, ""},
		{plan.SourceRef{Uses: " github/acme/flows.git/@release/v1.2_x "}, ""},
		{plan.SourceRef{Uses: "github/acme/.github", Path: "/a/w.yaml"}, ""},
		{plan.SourceRef{Uses: "github/acme/flows", Host: "not a server"}, ""},
		{plan.SourceRef{Uses: "gitlab/g/s/p@v1", Host: " Git.Example.COM "}, ""},
		{plan.SourceRef{Uses: "gitlab/g/p", Host: "gitlab"}, ""},
		{plan.SourceRef{Uses: "gitlab/g/p", Path: "a..b/w..yaml"}, ""},
		{plan.SourceRef{Uses: "marketplace/anything at all@"}, ""},
		{plan.SourceRef{Uses: "workflow/w", Path: "../x"}, ""},
		{plan.SourceRef{Uses: "github/acme"}, invalid},
		{plan.SourceRef{Uses: "github/acme/flows/more"}, invalid},
		{plan.SourceRef{Uses: "github/ac.me/flows"}, invalid},
		{plan.SourceRef{Uses: "github/ac_me/flows"}, invalid},
		{plan.SourceRef{Uses: "github/acme/fl+ows"}, invalid},
		{plan.SourceRef{Uses: "github/acme/.."}, invalid},
		{plan.SourceRef{Uses: "github/acme/flows//"}, invalid},
		{plan.SourceRef{Uses: "github/acme/flows@"}, invalid},
		{plan.SourceRef{Uses: "github/acme/flows@a..b"}, invalid},
		{plan.SourceRef{Uses: "github/acme/flows@a~1"}, invalid},
		{plan.SourceRef{Uses: "gitlab/p"}, invalid},
		{plan.SourceRef{Uses: "gitlab/g/./p"}, invalid},
		{plan.SourceRef{Uses: "gitlab/g//p"}, invalid},
		{plan.SourceRef{Uses: "github/acme/flows", Path: "../w.yaml"}, yaml},
		{plan.SourceRef{Uses: "gitlab/g/p", Path: "/a/../../w.yaml"}, yaml},
		{plan.SourceRef{Uses: "gitlab/g/p", Host: "git.example.com:8443"}, host},
		{plan.SourceRef{Uses: "gitlab/g/p", Host: "https://git.example.com"}, host},
		{plan.SourceRef{Uses: "gitlab/g/p", Host: "git..example.com"}, host},
		// What is wrong with the reference is found before its $yaml.
		{plan.SourceRef{Uses: "gitlab/p", Path: "../w.yaml", Host: "x:1"}, invalid},
	} {
		var want error
		switch tc.want {
		case invalid:
			want = problems.SourceRefInvalid.Problem(problems.Params{"ref": tc.ref.Uses})
		case yaml:
			want = problems.SourceWithInvalid.Problem(problems.Params{"key": "$yaml", "what": "file"})
		case host:
			want = problems.SourceWithInvalid.Problem(problems.Params{"key": "$host", "what": "server"})
		}
		got := tc.ref.Check()
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(want)
		if !bytes.Equal(gotJSON, wantJSON) {
			t.Errorf("%+v.Check() = %s, want %s", tc.ref, gotJSON, wantJSON)
		}
	}
	if !plan.ValidHost("git.example.com") || plan.ValidHost("") || plan.ValidHost("git.example.com.") {
		t.Error("ValidHost reads a server otherwise than as a DNS name")
	}
}

// A reference reads as format 1 reads it, as ACTIVATE does: two that name
// the same workflow are equal once they are normal.
func TestSourceRefNormal(t *testing.T) {
	for ref, want := range map[plan.SourceRef]plan.SourceRef{
		{Uses: "github/o/r"}: {Uses: "github/o/r", Path: "workflow.yaml"},
		{Uses: "github/o/r.git/@v1 ", Path: "w.yaml", Host: "x"}: {Uses: "github/o/r@v1", Path: "w.yaml"},
		{Uses: "github/o/r/.git"}:                                {Uses: "github/o/r/", Path: "workflow.yaml"},
		{Uses: "github/ o/r"}:                                    {Uses: "github/ o/r", Path: "workflow.yaml"},
		{Uses: "github/o/r@a.git/"}:                              {Uses: "github/o/r@a.git/", Path: "workflow.yaml"},
		{Uses: "gitlab/ g/s/p.git@v1\t"}:                         {Uses: "gitlab/g/s/p@v1", Path: "workflow.yaml", Host: "gitlab.com"},
		{Uses: "gitlab/g/p", Host: " Git.Example.COM "}:          {Uses: "gitlab/g/p", Path: "workflow.yaml", Host: "git.example.com"},
		{Uses: "gitlab/g/p", Host: " "}:                          {Uses: "gitlab/g/p", Path: "workflow.yaml", Host: "gitlab.com"},
		{Uses: " github/o/r"}:                                    {Uses: " github/o/r"},
		// A $yaml from the root, as ACTIVATE fetches the file.
		{Uses: "github/o/r", Path: "//a/w.yaml"}: {Uses: "github/o/r", Path: "a/w.yaml"},
		{Uses: "gitlab/g/p", Path: "//a/w.yaml"}: {Uses: "gitlab/g/p", Path: "/a/w.yaml", Host: "gitlab.com"},
		{Uses: "github/o/r", Path: "/"}:          {Uses: "github/o/r", Path: ""},
		{Uses: "workflow/w", Path: "x"}:          {Uses: "workflow/w", Path: "x"},
		{Uses: "marketplace/m"}:                  {Uses: "marketplace/m"},
	} {
		if got := ref.Normal(); got != want {
			t.Errorf("%+v.Normal() = %+v, want %+v", ref, got, want)
		}
	}
}
