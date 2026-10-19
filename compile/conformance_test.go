package compile_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
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
			checkDiagnostics(t, c.Expect.Diagnostics, diags)
			if diags.HasErrors() {
				return
			}
			values, _ := inputs.Values.Literal()
			if values == nil {
				values = withTemplates(t, inputs)
			}
			if g, w := canonical(t, values), canonical(t, orEmpty(c.Expect.Inputs)); g != w {
				t.Errorf("inputs = %s, want %s", g, w)
			}
			if g, w := canonical(t, inputs.Secrets), canonical(t, orEmpty(c.Expect.Secrets)); g != w {
				t.Errorf("secrets = %s, want %s", g, w)
			}
		})
	}
}

// contexts gives the case's contexts, with its secret variables unknown.
func contexts(c conformance.Case) map[string]any {
	out := map[string]any{}
	for k, v := range c.Contexts {
		out[k] = v
	}
	vars, _ := out["vars"].(map[string]any)
	if len(c.SecretVariables) > 0 {
		copied := map[string]any{}
		for k, v := range vars {
			copied[k] = v
		}
		for _, name := range c.SecretVariables {
			copied[name] = expr.Unknown{Name: "vars"}
		}
		out["vars"] = copied
	}
	return out
}

// withTemplates returns the values with each template as its text.
func withTemplates(t *testing.T, inputs *compile.Inputs) any {
	t.Helper()
	data, err := json.Marshal(inputs.Values)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	var text func(v any) any
	text = func(v any) any {
		m, ok := v.(map[string]any)
		if !ok {
			return v
		}
		if parts, ok := m["${{"].([]any); ok && len(m) == 1 {
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
		for k, item := range m {
			m[k] = text(item)
		}
		return m
	}
	return text(v)
}

func orEmpty(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

func checkDiagnostics(t *testing.T, want []conformance.Diagnostic, got format.Diagnostics) {
	t.Helper()
	unmatched := slices.Clone(got)
	for _, w := range want {
		i := slices.IndexFunc(unmatched, func(d format.Diagnostic) bool {
			return d.Severity.String() == w.Severity && string(d.Code) == w.Code && d.Path == w.Path &&
				(w.Line == 0 || d.Position == format.Position{Line: w.Line, Column: w.Column})
		})
		if i < 0 {
			t.Errorf("missing %s %s at %q, line %d, column %d; got %v", w.Severity, w.Code, w.Path, w.Line, w.Column, got)
			continue
		}
		unmatched = slices.Delete(unmatched, i, i+1)
	}
	for _, d := range unmatched {
		t.Errorf("unexpected %s (path %q)", d, d.Path)
	}
}

func canonical(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(fmt.Errorf("%#v: %w", v, err))
	}
	return string(b)
}
