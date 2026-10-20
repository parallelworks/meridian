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
			checkDiagnostics(t, c.Expect.Diagnostics, diags)
			if diags.HasErrors() {
				return
			}
			if g, w := canonical(t, withTemplates(t, inputs)), canonical(t, orEmpty(c.Expect.Inputs)); g != w {
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
			checkDiagnostics(t, c.Expect.Diagnostics, diags)
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
				got, ok := lookupPointer(doc, ptr)
				if !ok {
					t.Errorf("%s: the plan has nothing there: %s", ptr, encoded)
					continue
				}
				if g, w := canonical(t, got), canonical(t, want); g != w {
					t.Errorf("%s = %s, want %s", ptr, g, w)
				}
			}
		})
	}
}

func lookupPointer(v any, ptr string) (any, bool) {
	unescape := strings.NewReplacer("~1", "/", "~0", "~")
	for tok := range strings.SplitSeq(strings.TrimPrefix(ptr, "/"), "/") {
		tok = unescape.Replace(tok)
		switch c := v.(type) {
		case map[string]any:
			var ok bool
			if v, ok = c[tok]; !ok {
				return nil, false
			}
		case []any:
			var i int
			if _, err := fmt.Sscan(tok, &i); err != nil || i < 0 || i >= len(c) {
				return nil, false
			}
			v = c[i]
		default:
			return nil, false
		}
	}
	return v, true
}
