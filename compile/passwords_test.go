package compile_test

import (
	"encoding/json/v2"
	"reflect"
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
// text, and as a list, which a password doesn't take. Given as text, it is
// compiled too with what a launch form submits, which evaluates each
// default it can, those that read a password too, and sends what it gives.
func TestPasswordsNeverInPlans(t *testing.T) {
	given, kept, forms := 0, 0, 0
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
			opts := compile.Options{Contexts: contexts(c)}
			if !c.NoPlatform {
				opts.Inputs = passThrough{}
			}
			var submissions []map[string]any
			for _, password := range []any{sentinel, []any{sentinel}} {
				submitted := jsonvalue.CloneMap(c.Inputs)
				if !withPassword(defs, submitted, password) {
					break
				}
				submissions = append(submissions, submitted)
				if password == sentinel {
					// forms counts those that send what a default made from
					// the password, beside the password.
					form := compile.FormValues(doc, submitted, opts)
					if sentinels(t, form) > sentinels(t, submitted) {
						forms++
					}
					submissions = append(submissions, form)
				}
			}
			for _, submitted := range submissions {
				given++
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
	if given == 0 || kept == 0 || forms == 0 {
		t.Fatalf("given a password %d times, %d of them by a form that read it, and %d kept among the secrets", given, forms, kept)
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
	return sentinels(t, v) > 0
}

// sentinels counts the sentinels in v's JSON.
func sentinels(t *testing.T, v any) int {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Count(string(data), sentinel)
}

// A value given for a list's or a repeated step's rows with a password
// among their inputs, that isn't the shape the inputs say on the way to the
// password, is an error that doesn't show it, and is dropped: neither the
// plan nor the values hold it.
func TestPasswordsInTheWrongShape(t *testing.T) {
	nested := "creds: {type: list, template: {inner: {type: list, template: {pass: {type: password}}}}}"
	for _, tc := range []struct {
		name, def string
		value     any
		kind      *problems.Kind
		path      string
		input     string
	}{
		{
			"a row that isn't a mapping", "creds: {type: list, template: {pass: {type: password}}}",
			[]any{sentinel},
			problems.InputGroupNotMapping, "/on/execute/inputs/creds/template", "creds.0",
		},
		{
			"a repeated step's row that isn't a mapping", "creds: {type: step, title: Creds, multi: true, options: {pass: {type: password}}}",
			[]any{sentinel},
			problems.InputGroupNotMapping, "/on/execute/inputs/creds/options", "creds.0",
		},
		{
			"a group in a row that isn't a mapping", "creds: {type: list, template: {g: {type: group, items: {pass: {type: password}}}}}",
			[]any{map[string]any{"g": sentinel}},
			problems.InputGroupNotMapping, "/on/execute/inputs/creds/template/g", "creds.0.g",
		},
		{
			"a list in a row that isn't a list", nested,
			[]any{map[string]any{"inner": sentinel}},
			problems.InputNotList, "/on/execute/inputs/creds/template/inner", "creds.0.inner",
		},
		{
			"a row of a list in a row that isn't a mapping", nested,
			[]any{map[string]any{"inner": []any{sentinel}}},
			problems.InputGroupNotMapping, "/on/execute/inputs/creds/template/inner/template", "creds.0.inner.0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := inputs(tc.def)
			submitted := map[string]any{"creds": tc.value}
			p, diags := compile.Compile("workflow.yaml", []byte(src), submitted, compile.Options{})
			if p != nil || len(diags) != 1 || diags[0].Code != tc.kind.Code() || diags[0].Path() != tc.path ||
				diags[0].Params["input"] != tc.input || diags[0].Params["value"] != problems.Masked {
				t.Fatalf("plan %v, diagnostics %v, want %s at %s for %s, masked", p != nil, diags, tc.kind.Code(), tc.path, tc.input)
			}
			inputs, _ := compile.ResolveInputs(load(t, src), submitted, compile.Options{})
			if holdsSentinel(t, inputs.Values) || holdsSentinel(t, inputs.Deferred) || holdsSentinel(t, diags) {
				t.Errorf("values %v, deferred %v or diagnostics hold the password", inputs.Values, inputs.Deferred)
			}
		})
	}
}

// A form can't evaluate defaults that read each other, so values submitted
// for them are compared with what their defaults give in no order: each is
// data. One whose default reads them, and a password, is compared once
// they are, and means its default.
func TestSubmittedDefaultsThatReadEachOther(t *testing.T) {
	src := inputs(
		"pw: {type: password}",
		"a: {type: string, default: '${{ inputs.b }}'}",
		"b: {type: string, default: '${{ inputs.a }}'}",
		"c: {type: string, default: '${{ inputs.pw }}-${{ inputs.a }}'}",
	)
	submitted := map[string]any{"pw": sentinel, "a": "x", "b": "x", "c": sentinel + "-x"}
	inputs, diags := compile.ResolveInputs(load(t, src), submitted, compile.Options{})
	if len(diags) != 0 {
		t.Fatal(diags)
	}
	if want := map[string]any{"a": "x", "b": "x"}; !reflect.DeepEqual(inputs.Values, want) {
		t.Errorf("values %v, want %v", inputs.Values, want)
	}
	if _, ok := inputs.Deferred["c"]; !ok || len(inputs.Deferred) != 1 {
		t.Errorf("deferred %v, want c", inputs.Deferred)
	}
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
