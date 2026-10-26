package compile

import (
	"context"
	"encoding/json/v2"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/problems"
)

// givenAsIs is a platform that takes each value it's given as it is.
type givenAsIs struct{}

func (givenAsIs) ResolveInput(_ string, _ map[string]any, v any) (any, error) { return v, nil }

// caseVariables gives what Start reads of a case's variables: each by
// name, those the case names secret secret.
type caseVariables map[string]Variable

func (v caseVariables) Read(_ context.Context, names []string, _ bool) (map[string]Variable, error) {
	out := map[string]Variable{}
	for _, name := range names {
		_, _, one := strings.Cut(name, ".")
		for k, x := range v {
			if one && k == name || !one && strings.HasPrefix(k, name+".") {
				out[k] = x
			}
		}
	}
	return out, nil
}

// scalarsAsText returns v with each number and boolean in it its text, as
// sameValue compares them.
func scalarsAsText(v any) any {
	switch v := v.(type) {
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = scalarsAsText(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[k] = scalarsAsText(item)
		}
		return out
	case nil, string:
		return v
	}
	return looseKey(v)
}

// canonicalValues returns v's JSON, as it reads once decoded.
func canonicalValues(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	var back any
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	data, _ = json.Marshal(back, json.Deterministic(true))
	return string(data)
}

// Start gives the run's inputs what Create would have given them had it
// known every variable and password: Create with each secret variable
// given as a plain one is the oracle. Each case of the inputs and start
// areas is resolved so, and created with its secret variables not known
// and started, from what it submits and from what a launch form sends for
// it; where the oracle has no error, Start gives the same values.
func TestStartAgreesWithCreate(t *testing.T) {
	compared := 0
	for _, area := range []string{"inputs", "start"} {
		cases, err := conformance.Cases(area)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			if len(c.StartContexts) > 0 {
				continue // Its variables change as it starts.
			}
			data, err := c.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			doc, diags := format.Load("workflow.yaml", data)
			if diags.HasErrors() {
				continue
			}
			known, created := map[string]any{}, map[string]any{}
			vars := caseVariables{}
			for _, context := range []string{"org", "vars"} {
				values, _ := c.Contexts[context].(map[string]any)
				k, cr := map[string]any{}, map[string]any{}
				for name, v := range values {
					s, _ := v.(string)
					secret := context == "vars" && slices.Contains(c.SecretVariables, name)
					k[name], cr[name] = s, s
					if secret {
						cr[name] = expr.Unknown{Name: context}
					}
					vars[context+"."+name] = Variable{Value: s, Secret: secret}
				}
				if context == "vars" {
					for _, name := range c.SecretVariables {
						if _, ok := k[name]; !ok {
							k[name], cr[name] = "", expr.Unknown{Name: context}
							vars["vars."+name] = Variable{Secret: true}
						}
					}
				}
				known[context], created[context] = k, cr
			}
			for _, name := range []string{"remote", "sessions"} {
				if v, ok := c.Contexts[name]; ok {
					known[name], created[name] = v, v
				}
			}
			var inputs InputResolver
			if !c.NoPlatform {
				inputs = givenAsIs{}
			}
			createOpts := Options{Inputs: inputs, Contexts: created}
			for kind, submitted := range map[string]map[string]any{
				"submitted": c.Inputs,
				"form":      FormValues(doc, c.Inputs, createOpts),
			} {
				oracle := newInputResolver(doc, submitted, Options{Inputs: inputs, Contexts: maps.Clone(known)})
				oracle.resolve()
				if oracle.diags.HasErrors() || slices.ContainsFunc(oracle.fields, func(f *field) bool { return f.deferred && !f.ignored }) {
					// It has no values to compare, or doesn't know them, as
					// a default that reads the workflow's env: doesn't.
					continue
				}
				// Create holds an input an ignore that read a variable or a
				// password ignored, whatever it decided, and Start drops it.
				for _, f := range oracle.ignoredLater {
					oracle.drop(f)
				}
				run, _, err := Create(t.Context(), "workflow.yaml", data, submitted, createOpts)
				if err != nil {
					t.Fatal(err)
				}
				if run == nil {
					continue
				}
				res, sdiags, err := Start(t.Context(), run.Plan, StartInput{RunID: "r", Variables: vars, Record: run.Record, Inputs: inputs})
				if err != nil {
					t.Fatalf("%s, %s: %v", c.Name, kind, err)
				}
				if res == nil {
					// It can fail to start for what isn't its inputs, as a
					// matrix with no instances.
					if slices.ContainsFunc(sdiags, func(d problems.Diagnostic) bool { return strings.HasPrefix(d.Path(), "/on/execute/inputs") }) {
						t.Errorf("%s, %s: the run doesn't start: %v", c.Name, kind, sdiags)
					}
					continue
				}
				compared++
				// A value means the expression a form made it from when it
				// is what the expression gives, a number or a boolean the
				// same as its text, and is what the expression gives as the
				// run starts.
				if want, got := canonicalValues(t, scalarsAsText(oracle.values)), canonicalValues(t, scalarsAsText(res.Run.Inputs.Get())); want != got {
					t.Errorf("%s, %s: Start gives\n%s\nwhere Create, knowing everything, gives\n%s", c.Name, kind, got, want)
				}
			}
		}
	}
	if compared < 50 {
		t.Errorf("compared %d runs, want more", compared)
	}
}

// sameExcept compares what Start resolved with what the plan holds but for
// what is skipped: a number as the plan writes it, whatever its type, a
// skipped list item whatever it is, and a skipped key whether either side
// holds it.
func TestSameExcept(t *testing.T) {
	skip := skipped{}
	skip.add([]string{"rows", "1"})
	skip.add([]string{"m", "secret"})
	skip.add([]string{"m", "deep", "x"})
	skip.add([]string{"m", "deep"})
	planned := map[string]any{
		"n":    int64(3),
		"rows": []any{"a", nil},
		"m":    map[string]any{"k": "v", "deep": nil},
	}
	for name, c := range map[string]struct {
		got  map[string]any
		same bool
	}{
		"the same":                 {map[string]any{"n": 3.0, "rows": []any{"a", "s"}, "m": map[string]any{"k": "v", "secret": "s", "deep": map[string]any{"x": 1}}}, true},
		"a number of another type": {map[string]any{"n": float64(3), "rows": []any{"a", nil}, "m": map[string]any{"k": "v"}}, true},
		"another number":           {map[string]any{"n": 4.0, "rows": []any{"a", nil}, "m": map[string]any{"k": "v"}}, false},
		"another list item":        {map[string]any{"n": 3.0, "rows": []any{"b", nil}, "m": map[string]any{"k": "v"}}, false},
		"a list item more":         {map[string]any{"n": 3.0, "rows": []any{"a", nil, "c"}, "m": map[string]any{"k": "v"}}, false},
		"a key more":               {map[string]any{"n": 3.0, "rows": []any{"a", nil}, "m": map[string]any{"k": "v", "other": 1}}, false},
		"a key less":               {map[string]any{"n": 3.0, "rows": []any{"a", nil}, "m": map[string]any{}}, false},
		"a mapping for a text":     {map[string]any{"n": 3.0, "rows": []any{"a", nil}, "m": map[string]any{"k": map[string]any{}}}, false},
	} {
		same := true
		for k, q := range planned {
			if under, ok := skip.under(k); ok && !sameExcept(c.got[k], q, under) {
				same = false
			}
		}
		if same != c.same {
			t.Errorf("%s: same %v, want %v", name, same, c.same)
		}
	}
}
