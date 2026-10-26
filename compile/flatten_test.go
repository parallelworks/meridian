package compile_test

import (
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
)

// A group with flatten true whose ignore reads a variable holds its inputs'
// values, beside its own, and a value given at its own key, out of the plan
// whatever the ignore decided, as it does a group's that doesn't flatten,
// and Start drops all of them if it decides so again: the plan is the same
// either way.
func TestFlattenedGroupIgnoredByVariable(t *testing.T) {
	src := workflow(`g: {type: group, flatten: true, ignore: '${{ org.IGN == ''yes'' }}', items: {a: {type: string, default: d}}}
b: {type: string, optional: true}`)
	submitted := map[string]any{"a": "given", "g": map[string]any{"x": "under-its-name"}, "b": "kept"}
	var plans []string
	for ign, want := range map[string]string{
		"yes": `{"b":"kept"}`,
		"no":  `{"a":"given","b":"kept","g":{"x":"under-its-name"}}`,
	} {
		vars := testVariables{"org.IGN": {Value: ign}}
		c := createRun(t, src, submitted, vars)
		data, err := plan.Encode(c.Plan)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "given") || strings.Contains(string(data), "under-its-name") {
			t.Errorf("IGN=%s: the plan holds a value the ignore decides on: %s", ign, data)
		}
		plans = append(plans, string(data))
		res, diags := startRun(t, c, vars, compile.StartInput{RunID: "run"})
		if res == nil {
			t.Fatalf("IGN=%s: %v", ign, diags)
		}
		if got := canonical(t, res.Run.Inputs.Get()); got != want {
			t.Errorf("IGN=%s: the run's inputs are %s, want %s", ign, got, want)
		}
	}
	if plans[0] != plans[1] {
		t.Errorf("the plans differ by what the ignore decided:\n%s\n%s", plans[0], plans[1])
	}
}

// A value given at the key of a flattened group with a password among its
// inputs is kept as a secret, whatever its shape, as the group's inputs sent
// under its name can hold the password, misspelt or as text: the plan
// doesn't hold it, and the run masks it.
func TestFlattenedGroupValueHoldsPassword(t *testing.T) {
	src := workflow("g: {type: group, flatten: true, items: {pw: {type: password, optional: true}, u: {type: string}}}")
	for name, given := range map[string]any{
		"a misspelt key": map[string]any{"pW": "SECRET-zz9"},
		"text":           `{"pw":"SECRET-zz9"}`,
	} {
		t.Run(name, func(t *testing.T) {
			c, res, diags := started(t, src, map[string]any{"u": "a", "g": given}, testVariables{})
			if res == nil {
				t.Fatal(diags)
			}
			data, err := plan.Encode(c.Plan)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), "SECRET-zz9") {
				t.Errorf("the plan holds the value: %s", data)
			}
			masker := plan.NewMasker(res.Run.Masked()...)
			if got := masker.Mask(canonical(t, res.Run.Inputs.Get())); strings.Contains(got, "SECRET-zz9") {
				t.Errorf("the run's values show it: %s", got)
			}
		})
	}
}
