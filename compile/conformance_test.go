package compile_test

import (
	"encoding/json/v2"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
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
			check := func(what string, got, want map[string]any) {
				t.Helper()
				if !diags.HasErrors() {
					if g, w := conformancetest.Canonical(t, got), conformancetest.Canonical(t, orEmpty(want)); g != w {
						t.Errorf("%s = %s, want %s", what, g, w)
					}
					return
				}
				// A run with errors doesn't start, so a case with errors
				// lists only the values it checks, such as those of the
				// inputs that were given good ones.
				for name, w := range want {
					g, ok := got[name]
					if !ok {
						t.Errorf("%s has no %s, want %s", what, name, conformancetest.Canonical(t, w))
					} else if g, w := conformancetest.Canonical(t, g), conformancetest.Canonical(t, w); g != w {
						t.Errorf("%s.%s = %s, want %s", what, name, g, w)
					}
				}
			}
			check("inputs", inputs.Values, c.Expect.Inputs)
			// The inputs whose values wait until the run starts, all of them,
			// or in a case with errors, those it lists.
			deferred := slices.Sorted(maps.Keys(inputs.Deferred))
			for _, name := range c.Expect.Deferred {
				if !slices.Contains(deferred, name) {
					t.Errorf("deferred %q, want %s among them", deferred, name)
				}
			}
			if want := slices.Sorted(slices.Values(c.Expect.Deferred)); !diags.HasErrors() && !slices.Equal(deferred, want) {
				t.Errorf("deferred %q, want %q", deferred, want)
			}
			check("secrets", inputs.Secrets, c.Expect.Secrets)
			check("private", inputs.Private, c.Expect.Private)
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

func TestStartConformance(t *testing.T) {
	cases, err := conformance.Cases("start")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(strings.TrimPrefix(c.Name, "start/"), func(t *testing.T) {
			t.Parallel()
			data, err := c.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			opts := compile.Options{Contexts: contexts(c)}
			if !c.NoPlatform {
				opts.Inputs = passThrough{}
			}
			ctx := t.Context()
			created, diags, err := compile.Create(ctx, "workflow.yaml", data, c.Inputs, opts)
			if err != nil {
				t.Fatal(err)
			}
			var res *compile.StartResult
			if created != nil {
				var startDiags problems.Diagnostics
				res, startDiags, err = compile.Start(ctx, created.Plan, compile.StartInput{
					RunID: c.Name, Variables: startVariables(c), Record: created.Record, Inputs: opts.Inputs,
				})
				if err != nil {
					t.Fatal(err)
				}
				diags = append(diags, startDiags...)
			}
			conformancetest.CheckDiagnostics(t, c.Expect.Diagnostics, diags)
			if res == nil {
				// A run that doesn't start has no values to check.
				return
			}
			check := func(what string, got, want any) {
				t.Helper()
				if g, w := conformancetest.Canonical(t, got), conformancetest.Canonical(t, want); g != w {
					t.Errorf("%s = %s, want %s", what, g, w)
				}
			}
			e := c.Expect
			check("inputs", res.Run.Inputs.Get(), orEmpty(e.Inputs))
			check("env", orEmptyStrings(res.Run.Env), orEmptyStrings(e.Env))
			if e.Timeout != nil {
				check("timeout", res.TimeoutSeconds, *e.Timeout)
			}
			check("run-secrets", orNone(res.Run.Secrets), orNone(e.RunSecrets))
			check("run-private", orNone(res.Run.Private), orNone(e.RunPrivate))
			check("run-derived", orNone(res.Run.Derived), orNone(e.RunDerived))
			matrices := map[string][]any{}
			for job, x := range res.Expansions {
				for _, i := range x.Instances {
					matrices[job] = append(matrices[job], i.Matrix.Get())
				}
			}
			check("matrices", matrices, orEmptyMatrices(e.Matrices))
		})
	}
}

// startVariables gives the variables of a case in the start area as the
// run reads them as it starts: its contexts', with those its start-contexts
// replace, each secret one secret.
func startVariables(c conformance.Case) compile.Variables {
	out := testVariables{}
	for _, context := range []string{"org", "vars"} {
		vars, _ := c.Contexts[context].(map[string]any)
		if v, ok := c.StartContexts[context].(map[string]any); ok {
			vars = v
		}
		for name, v := range vars {
			if s, ok := v.(string); ok {
				out[context+"."+name] = compile.Variable{Value: s, Secret: context == "vars" && slices.Contains(c.SecretVariables, name)}
			}
		}
	}
	return out
}

func orEmptyStrings(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

func orNone(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func orEmptyMatrices(m map[string][]map[string]any) map[string][]map[string]any {
	if m == nil {
		return map[string][]map[string]any{}
	}
	return m
}

// Every plan the compiler returns, which it does only without errors,
// encodes: compiling each compile, inputs and start case, with its values,
// gives a plan that can be stored with the run, or errors and none.
func TestConformancePlansEncode(t *testing.T) {
	for _, area := range []string{"compile", "inputs", "start"} {
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
