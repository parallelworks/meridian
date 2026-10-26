package compile_test

import (
	"testing"

	"github.com/parallelworks/meridian/compile"
)

// A group's value is a mapping in the run's values, though none of its
// inputs has a value, as ACTIVATE fills in a run's values, and so is a
// wizard step's whose inputs' values are under its own key; but not one
// that is ignored, flattened, or in a list's row none of whose inputs has
// a value, as ACTIVATE fills in a row. Create's plan and Start agree.
func TestGroupValues(t *testing.T) {
	for name, tc := range map[string]struct {
		defs      string
		submitted map[string]any
		want      string
	}{
		"no value in it":      {"g: {type: group, items: {a: {type: string, optional: true}}}", nil, `{"g":{}}`},
		"a default of ''":     {"g: {type: group, items: {a: {type: string, default: ''}}}", nil, `{"g":{}}`},
		"given null":          {"g: {type: group, items: {a: {type: string, optional: true}}}", map[string]any{"g": nil}, `{"g":{}}`},
		"given {}":            {"g: {type: group, items: {a: {type: string, optional: true}}}", map[string]any{"g": map[string]any{}}, `{"g":{}}`},
		"in a group":          {"g: {type: group, items: {h: {type: group, items: {a: {type: string, optional: true}}}}}", nil, `{"g":{"h":{}}}`},
		"hidden":              {"g: {type: group, hidden: true, items: {a: {type: string, optional: true}}}", nil, `{"g":{}}`},
		"its inputs ignored":  {"g: {type: group, items: {a: {type: string, default: x, ignore: true}}}", nil, `{"g":{}}`},
		"ignored":             {"g: {type: group, ignore: true, items: {a: {type: string, optional: true}}}", nil, `{}`},
		"in an ignored group": {"g: {type: group, ignore: true, items: {h: {type: group, items: {a: {type: string, optional: true}}}}}", nil, `{}`},
		"flattened":           {"g: {type: group, flatten: true, items: {a: {type: string, optional: true}}}", nil, `{}`},
		"in a list's row":     {"l: {type: list, template: {g: {type: group, items: {a: {type: string, optional: true}}}, b: {type: string, default: x}}}", map[string]any{"l": []any{map[string]any{}}}, `{"l":[{"b":"x"}]}`},
		"a wizard step under its key": {
			"$meta: {wizard: {mode: wizard, flatten: false}}\ns: {type: step, title: S, options: {a: {type: string, optional: true}}}", nil, `{"s":{}}`,
		},
		"with a value": {"g: {type: group, items: {a: {type: string, default: x}}}", nil, `{"g":{"a":"x"}}`},
	} {
		t.Run(name, func(t *testing.T) {
			src := workflow(tc.defs)
			c := createRun(t, src, tc.submitted, testVariables{})
			if got := canonical(t, c.Plan.Run.Inputs.Get()); got != tc.want {
				t.Errorf("the plan's inputs are %s, want %s", got, tc.want)
			}
			res, diags := startRun(t, c, testVariables{}, compile.StartInput{})
			if res == nil {
				t.Fatal(diags)
			}
			if got := canonical(t, res.Run.Inputs.Get()); got != tc.want {
				t.Errorf("the run's inputs are %s, want %s", got, tc.want)
			}
		})
	}
}
