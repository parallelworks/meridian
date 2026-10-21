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
			if diags.HasErrors() {
				return
			}
			if g, w := conformancetest.Canonical(t, withTemplates(t, inputs)), conformancetest.Canonical(t, orEmpty(c.Expect.Inputs)); g != w {
				t.Errorf("inputs = %s, want %s", g, w)
			}
			if g, w := conformancetest.Canonical(t, inputs.Secrets), conformancetest.Canonical(t, orEmpty(c.Expect.Secrets)); g != w {
				t.Errorf("secrets = %s, want %s", g, w)
			}
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

// withTemplates returns the values with each deferred default as its
// text.
func withTemplates(t *testing.T, inputs *compile.Inputs) map[string]any {
	t.Helper()
	values := inputs.Values
	for path, v := range inputs.Deferred {
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		var raw any
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatal(err)
		}
		keys := strings.Split(path, ".")
		m := values
		for _, k := range keys[:len(keys)-1] {
			next, ok := m[k].(map[string]any)
			if !ok {
				next = map[string]any{}
				m[k] = next
			}
			m = next
		}
		m[keys[len(keys)-1]] = templateText(raw)
	}
	return values
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
