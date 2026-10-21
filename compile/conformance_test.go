package compile_test

import (
	"encoding/json/v2"
	"maps"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
)

// passThrough is a platform that takes every value it's given as it is.
type passThrough struct{}

func (passThrough) ResolveInput(_ string, _ map[string]any, v any) (any, error) { return v, nil }

func TestInputsConformance(t *testing.T) {
	cases, err := conformance.Cases("inputs")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(strings.TrimPrefix(c.Name, "inputs/"), func(t *testing.T) {
			t.Parallel()
			data, err := c.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			doc, diags := format.Load("workflow.yaml", data)
			if diags.HasErrors() {
				t.Fatalf("the workflow doesn't load: %v", diags)
			}
			opts := compile.Options{Contexts: contexts(c)}
			if !c.NoPlatform {
				opts.Inputs = passThrough{}
			}
			inputs, diags := compile.ResolveInputs(doc, c.Inputs, opts)
			conformancetest.CheckDiagnostics(t, c.Expect.Diagnostics, diags)
			// A run with errors doesn't start, so a case with errors says
			// what it expects of the values only where it means to: how
			// the values that were good were read, say.
			check := func(what string, got any, want map[string]any) {
				t.Helper()
				if want == nil && diags.HasErrors() {
					return
				}
				if g, w := conformancetest.Canonical(t, got), conformancetest.Canonical(t, orEmpty(want)); g != w {
					t.Errorf("%s = %s, want %s", what, g, w)
				}
			}
			check("inputs", inputs.Values, c.Expect.Inputs)
			check("deferred", deferredText(t, inputs.Deferred), c.Expect.Deferred)
			check("secrets", inputs.Secrets, c.Expect.Secrets)
		})
	}
}

// contexts gives the case's contexts, with its secret variables unknown.
func contexts(c conformance.Case) map[string]any {
	out := maps.Clone(c.Contexts)
	if out == nil {
		out = map[string]any{}
	}
	if len(c.SecretVariables) > 0 {
		vars, _ := out["vars"].(map[string]any)
		vars = maps.Clone(vars)
		if vars == nil {
			vars = map[string]any{}
		}
		for _, name := range c.SecretVariables {
			vars[name] = expr.Unknown{Name: "vars"}
		}
		out["vars"] = vars
	}
	return out
}

// deferredText returns the deferred values by their paths, each as its
// text.
func deferredText(t *testing.T, deferred map[string]plan.Value) map[string]any {
	t.Helper()
	out := make(map[string]any, len(deferred))
	for path, v := range deferred {
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		var raw any
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatal(err)
		}
		out[path] = templateText(raw)
	}
	return out
}

// templateText returns v, a Value's JSON, with each template as its text.
func templateText(v any) any {
	switch v := v.(type) {
	case []any:
		for i, item := range v {
			v[i] = templateText(item)
		}
	case map[string]any:
		if parts, ok := v["${{"].([]any); ok && len(v) == 1 {
			var b strings.Builder
			for _, p := range parts {
				if s, ok := p.(string); ok {
					b.WriteString(s)
				} else {
					b.WriteString("${{" + p.(map[string]any)["src"].(string) + "}}")
				}
			}
			return b.String()
		}
		for k, item := range v {
			v[k] = templateText(item)
		}
	}
	return v
}

func orEmpty(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

func TestCompileConformance(t *testing.T) {
	cases, err := conformance.Cases("compile")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(strings.TrimPrefix(c.Name, "compile/"), func(t *testing.T) {
			t.Parallel()
			data, err := c.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			opts := compile.Options{Contexts: contexts(c)}
			if !c.NoPlatform {
				opts.Inputs = passThrough{}
			}
			p, diags := compile.Compile("workflow.yaml", data, c.Inputs, opts)
			conformancetest.CheckDiagnostics(t, c.Expect.Diagnostics, diags)
			if (p == nil) != diags.HasErrors() {
				t.Fatalf("plan is %v with errors %v", p != nil, diags.HasErrors())
			}
			if p == nil {
				if len(c.Expect.Plan) > 0 {
					t.Errorf("no plan to check %v in", c.Expect.Plan)
				}
				return
			}
			encoded, err := plan.Encode(p)
			if err != nil {
				t.Fatal(err)
			}
			var doc any
			if err := json.Unmarshal(encoded, &doc); err != nil {
				t.Fatal(err)
			}
			for ptr, want := range c.Expect.Plan {
				got, ok := conformancetest.Lookup(t, doc, ptr)
				if !ok {
					t.Errorf("%s: the plan has nothing there: %s", ptr, encoded)
					continue
				}
				if g, w := conformancetest.Canonical(t, got), conformancetest.Canonical(t, want); g != w {
					t.Errorf("%s = %s, want %s", ptr, g, w)
				}
			}
		})
	}
}

// Every plan the compiler returns, which it does only without errors,
// encodes: compiling each compile and inputs case, with its values, gives
// a plan that can be stored with the run, or errors and none.
func TestConformancePlansEncode(t *testing.T) {
	for _, area := range []string{"compile", "inputs"} {
		cases, err := conformance.Cases(area)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			t.Run(c.Name, func(t *testing.T) {
				t.Parallel()
				data, err := c.Workflow()
				if err != nil {
					t.Fatal(err)
				}
				opts := compile.Options{Contexts: contexts(c)}
				if !c.NoPlatform {
					opts.Inputs = passThrough{}
				}
				p, diags := compile.Compile("workflow.yaml", data, c.Inputs, opts)
				if (p == nil) != diags.HasErrors() {
					t.Fatalf("plan is %v with errors %v", p != nil, diags.HasErrors())
				}
				if p == nil {
					return
				}
				if _, err := plan.Encode(p); err != nil {
					t.Errorf("the plan doesn't encode: %v", err)
				}
			})
		}
	}
}
