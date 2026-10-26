package compile_test

import (
	"bytes"
	"maps"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/plan"
)

// changedValue returns another value for a variable whose value is v, of
// the same kind: a number another number, and text other text. JSON, which
// a workflow reads with fromJSON, is left as it is.
func changedValue(v string) string {
	if n, err := strconv.ParseFloat(v, 64); err == nil {
		return strconv.FormatFloat(n+1, 'f', -1, 64)
	}
	if t := strings.TrimSpace(v); strings.HasPrefix(t, "[") || strings.HasPrefix(t, "{") {
		return v
	}
	return "changed-" + v
}

// variableContexts returns the case's contexts with each plain variable's
// value as value gives it, and each secret one unknown, as Create reads
// them.
func variableContexts(c conformance.Case, value func(string) string) map[string]any {
	out := maps.Clone(c.Contexts)
	if out == nil {
		out = map[string]any{}
	}
	for _, context := range []string{"org", "vars"} {
		vars, _ := c.Contexts[context].(map[string]any)
		m := map[string]any{}
		for name, v := range vars {
			if s, ok := v.(string); ok {
				m[name] = value(s)
			} else {
				m[name] = v
			}
		}
		if context == "vars" {
			for _, name := range c.SecretVariables {
				m[name] = expr.Unknown{Name: "vars"}
			}
		}
		out[context] = m
	}
	return out
}

// planWithoutGraph returns p's encoding without what its jobs' needs:
// decided, which a variable's value can change (docs/design.md, The plan).
func planWithoutGraph(t *testing.T, p *plan.Plan) []byte {
	t.Helper()
	jobs := make([]plan.Job, len(p.Jobs))
	for i, j := range p.Jobs {
		j.Needs = nil
		jobs[i] = j
	}
	q := *p
	q.Jobs = jobs
	data, err := plan.Encode(&q)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// A plan's bytes don't depend on an organization or user variable's value,
// but for the jobs' graph: each case of the inputs, compile and start
// areas whose contexts give variables is compiled with them, and with
// each plain one's value changed, from what the case submits and from
// what a launch form sends, which evaluates each default it can with the
// variables as they were first. Whether a value a form made from a
// variable is what the variable gives now is the secret record's
// (plan.Record.Means), not the plan's.
func TestPlansDontDependOnVariables(t *testing.T) {
	compared := 0
	for _, area := range []string{"inputs", "compile", "start"} {
		cases, err := conformance.Cases(area)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			if c.Contexts["org"] == nil && c.Contexts["vars"] == nil {
				continue
			}
			data, err := c.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			doc, diags := format.Load("workflow.yaml", data)
			if diags.HasErrors() {
				continue
			}
			before := compile.Options{Contexts: variableContexts(c, func(v string) string { return v })}
			after := compile.Options{Contexts: variableContexts(c, changedValue)}
			if !c.NoPlatform {
				before.Inputs, after.Inputs = passThrough{}, passThrough{}
			}
			for name, submitted := range map[string]map[string]any{
				"submitted": c.Inputs,
				"form":      compile.FormValues(t.Context(), doc, c.Inputs, before),
			} {
				a, _ := compile.Compile("workflow.yaml", data, submitted, before)
				b, _ := compile.Compile("workflow.yaml", data, submitted, after)
				if a == nil || b == nil {
					continue
				}
				compared++
				if pa, pb := planWithoutGraph(t, a), planWithoutGraph(t, b); !bytes.Equal(pa, pb) {
					t.Errorf("%s, %s: the plan changes with the variables:\n%s\n%s", c.Name, name, pa, pb)
				}
			}
		}
	}
	if compared < 20 {
		t.Errorf("compared %d plans, want more", compared)
	}
}
