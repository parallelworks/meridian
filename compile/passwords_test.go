package compile_test

import (
	"encoding/json/v2"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// sentinel is a password that no plan or diagnostic may hold.
const sentinel = "HUNTER2-SENTINEL"

// No plan, and no diagnostic, holds a password's value. Each conformance
// workflow is compiled with its case's inputs, and the password given for
// each password input it has, and for each password field of a list's or a
// repeated step's rows, in a row of its own if the case gives none: as
// text, and as a list, which a password doesn't take.
func TestPasswordsNeverInPlans(t *testing.T) {
	given, kept := 0, 0
	for _, area := range []string{"format", "inputs", "compile"} {
		cases, err := conformance.Cases(area)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			data, err := c.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			doc, diags := format.Load("workflow.yaml", data)
			if diags.HasErrors() {
				continue
			}
			defs, _ := jsonptr.Lookup(doc.Value, []string{"on", "execute", "inputs"})
			for _, password := range []any{sentinel, []any{sentinel}} {
				submitted := jsonvalue.CloneMap(c.Inputs)
				if !withPassword(defs, submitted, password) {
					break
				}
				given++
				opts := compile.Options{Contexts: contexts(c)}
				if !c.NoPlatform {
					opts.Inputs = passThrough{}
				}
				p, diags := compile.Compile("workflow.yaml", data, submitted, opts)
				for _, d := range diags {
					if holdsSentinel(t, d) {
						t.Errorf("%s: %s shows the password", c.Name, d)
					}
				}
				if p != nil {
					encoded, err := plan.Encode(p)
					if err != nil {
						t.Fatal(err)
					}
					if strings.Contains(string(encoded), sentinel) {
						t.Errorf("%s: the plan holds the password: %s", c.Name, encoded)
					}
				}
				inputs, _ := compile.ResolveInputs(doc, submitted, opts)
				if holdsSentinel(t, inputs.Values) || holdsSentinel(t, inputs.Deferred) {
					t.Errorf("%s: the values %v or the deferred defaults hold the password", c.Name, inputs.Values)
				}
				for _, v := range inputs.Secrets {
					if v == sentinel {
						kept++
					}
				}
			}
		}
	}
	if given == 0 || kept == 0 {
		t.Fatalf("given a password %d times, and %d kept among the secrets", given, kept)
	}
}

// withPassword sets password as the value of each password input that defs,
// input definitions, define in values, and of each password field of a
// list's or a repeated step's rows. It reports whether it set any.
func withPassword(defs any, values map[string]any, password any) bool {
	m, _ := defs.(map[string]any)
	meta, _ := m["$meta"].(map[string]any)
	wizard, _ := meta["wizard"].(map[string]any)
	set := false
	for key, raw := range m {
		def, ok := raw.(map[string]any)
		if !ok || strings.HasPrefix(key, "$") {
			continue
		}
		typ, _ := def["type"].(string)
		switch rows := def["template"]; {
		case typ == "password":
			values[key] = password
			set = true
		case typ == "list" || typ == "step" && def["multi"] == true:
			if typ == "step" {
				rows = def["options"]
			}
			if !withPassword(rows, map[string]any{}, password) {
				continue
			}
			given, _ := values[key].([]any)
			if len(given) == 0 {
				given = []any{map[string]any{}}
			}
			for _, row := range given {
				if row, ok := row.(map[string]any); ok {
					withPassword(rows, row, password)
				}
			}
			values[key] = given
			set = true
		case typ == "step" && wizard["flatten"] != false:
			set = withPassword(def["options"], values, password) || set
		case typ == "step" || def["items"] != nil && typ != "list":
			inner := def["items"]
			if typ == "step" {
				inner = def["options"]
			}
			group, ok := values[key].(map[string]any)
			if !ok {
				group = map[string]any{}
			}
			if withPassword(inner, group, password) {
				values[key] = group
				set = true
			}
		}
	}
	return set
}

// holdsSentinel reports whether v's JSON holds the sentinel.
func holdsSentinel(t *testing.T, v any) bool {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Contains(string(data), sentinel)
}

// A diagnostic never quotes a password's value, or a value made from one:
// each param that would show it is masked, wherever the password is.
func TestDiagnosticsHidePasswords(t *testing.T) {
	for _, tc := range []struct {
		name      string
		defs      []string
		submitted map[string]any
		kind      *problems.Kind
		masked    []string
	}{
		{
			"a password that isn't text",
			[]string{"pw: {type: password}"},
			map[string]any{"pw": []any{sentinel}},
			problems.InputNotText,
			[]string{"value"},
		},
		{
			"a row's password that isn't text",
			[]string{"creds: {type: list, template: {pass: {type: password}}}"},
			map[string]any{"creds": []any{map[string]any{"pass": map[string]any{"k": sentinel}}}},
			problems.InputNotText,
			[]string{"value"},
		},
		{
			"a repeated step's password that isn't text",
			[]string{"hosts: {type: step, title: Host, multi: true, options: {pass: {type: password}}}"},
			map[string]any{"hosts": []any{map[string]any{"pass": []any{sentinel}}}},
			problems.InputNotText,
			[]string{"value"},
		},
		{
			"a default that fails",
			[]string{"pw: {type: password}", "d: {type: string, default: '${{ fromJSON(inputs.pw) }}'}"},
			map[string]any{"pw": sentinel + "{"},
			problems.FromJSONInvalid,
			[]string{"text"},
		},
		{
			"a default that reads a row's password",
			[]string{"creds: {type: list, template: {pass: {type: password}}}", "n: {type: number, default: '${{ inputs.creds[0].pass }}'}"},
			map[string]any{"creds": []any{map[string]any{"pass": sentinel}}},
			problems.InputNotNumber,
			[]string{"value"},
		},
		{
			"a default that reads a default that read a password",
			[]string{"pw: {type: password}", "a: {type: string, default: '${{ inputs.pw }}'}", "b: {type: dropdown, options: [x, y], default: '${{ inputs.a }}'}"},
			map[string]any{"pw": sentinel},
			problems.InputNotOption,
			[]string{"value"},
		},
		{
			"a condition that fails",
			[]string{"pw: {type: password}", "h: {type: string, optional: true, hidden: '${{ map(inputs.pw, x => x) }}'}"},
			map[string]any{"pw": sentinel},
			problems.NotListOrMapping,
			[]string{"value"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, diags := compile.Compile("workflow.yaml", []byte(inputs(tc.defs...)), tc.submitted, compile.Options{})
			i := slices.IndexFunc(diags, func(d problems.Diagnostic) bool { return d.Code == tc.kind.Code() })
			if i < 0 {
				t.Fatalf("diagnostics %v, want %s", diags, tc.kind.Code())
			}
			for _, name := range tc.masked {
				if diags[i].Params[name] != problems.Masked {
					t.Errorf("%s = %v, want it masked", name, diags[i].Params[name])
				}
			}
			if holdsSentinel(t, diags) {
				t.Errorf("%v shows the password", diags)
			}
		})
	}
}
