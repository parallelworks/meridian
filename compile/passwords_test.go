package compile_test

import (
	"encoding/json/v2"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/expr"
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
// default it can, those that read a password too, and sends what it gives;
// and with that sent as ACTIVATE's form can send it otherwise (formShapes),
// once the password has changed, or a list's rows have.
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
					submissions = append(submissions, formShapes(form)...)
					// The password changed after the form filled in what it
					// read.
					stale := jsonvalue.CloneMap(form)
					withPassword(defs, stale, "changed")
					submissions = append(submissions, stale)
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

// formShapes returns what a form submits, form, and as it can submit it
// once a person has changed a list's rows the form filled in: with the
// first of each list's rows deleted, and with a row added before them.
func formShapes(form map[string]any) []map[string]any {
	deleted, added := jsonvalue.CloneMap(form), jsonvalue.CloneMap(form)
	changed := false
	for k, v := range form {
		rows, ok := v.([]any)
		if !ok || len(rows) == 0 {
			continue
		}
		if _, ok := rows[0].(map[string]any); !ok {
			continue
		}
		changed = true
		deleted[k] = jsonvalue.Clone(rows[1:])
		added[k] = append([]any{map[string]any{}}, jsonvalue.Clone(rows).([]any)...)
	}
	if !changed {
		return []map[string]any{form}
	}
	return []map[string]any{form, deleted, added}
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
// password, can hold the password where the compiler can't find it. So it
// is kept as a secret, as it was given, by its path, and the plan doesn't
// hold it: a list's item is null in its place.
func TestPasswordsInTheWrongShape(t *testing.T) {
	nested := "creds: {type: list, template: {inner: {type: list, template: {pass: {type: password}}}}}"
	for _, tc := range []struct {
		name, def string
		value     any
		secret    string
		inputs    string
	}{
		{
			"a row that isn't a mapping", "creds: {type: list, template: {pass: {type: password}}}",
			[]any{sentinel, map[string]any{"pass": sentinel}},
			"creds.0", "[null,{}]",
		},
		{
			"a repeated step's row that isn't a mapping", "creds: {type: step, title: Creds, multi: true, options: {pass: {type: password}}}",
			[]any{sentinel},
			"creds.0", "[null]",
		},
		{
			"a group in a row that isn't a mapping", "creds: {type: list, template: {g: {type: group, items: {pass: {type: password}}}}}",
			[]any{map[string]any{"g": sentinel}},
			"creds.0.g", "[{}]",
		},
		{
			"a list in a row that isn't a list", nested,
			[]any{map[string]any{"inner": sentinel}},
			"creds.0.inner", "[{}]",
		},
		{
			"a row of a list in a row that isn't a mapping", nested,
			[]any{map[string]any{"inner": []any{sentinel}}},
			"creds.0.inner.0", `[{"inner":[null]}]`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := inputs(tc.def)
			submitted := map[string]any{"creds": tc.value}
			p, diags := compile.Compile("workflow.yaml", []byte(src), submitted, compile.Options{})
			if p == nil || len(diags) != 0 {
				t.Fatalf("plan %v, diagnostics %v, want a plan and none", p != nil, diags)
			}
			if !slices.Contains(p.Run.Secrets, "inputs."+tc.secret) {
				t.Errorf("the run's secrets %v, want inputs.%s", p.Run.Secrets, tc.secret)
			}
			inputs, _ := compile.ResolveInputs(load(t, src), submitted, compile.Options{})
			if holdsSentinel(t, inputs.Values) || holdsSentinel(t, inputs.Deferred) {
				t.Errorf("values %v or deferred %v hold the password", inputs.Values, inputs.Deferred)
			}
			if got, err := json.Marshal(inputs.Values["creds"]); err != nil || string(got) != tc.inputs {
				t.Errorf("creds is %s, want %s", got, tc.inputs)
			}
			if !holdsSentinel(t, inputs.Secrets[tc.secret]) {
				t.Errorf("secrets %v, want %s holding the value given", inputs.Secrets, tc.secret)
			}
		})
	}
}

// What ACTIVATE's launch form sends can differ from what the compiler
// evaluates: it evaluates a list's default rows once, as it loads, and a
// row's defaults again only when the list's length changes; it doesn't
// follow a dependency written inputs['x']; a person can delete, add and
// move rows, and type over what it filled in; and its toJSON keeps a
// mapping's keys in the order they were added. No plan, and no diagnostic,
// holds a password or a variable's value it sends that way.
func TestPasswordsNeverInPlansFromActivateForms(t *testing.T) {
	const variable = "VARIABLE-SENTINEL"
	envRows := func(rows ...string) string {
		return "env: {type: list, template: {name: {type: string}, value: {type: string}}, default: [" + strings.Join(rows, ", ") + "]}"
	}
	row := func(name, value string) map[string]any { return map[string]any{"name": name, "value": value} }
	for _, tc := range []struct {
		name      string
		defs      []string
		submitted map[string]any
	}{
		{
			"a row's default the form filled in before the password changed",
			[]string{"creds: {type: list, template: {user: {type: string}, pass: {type: password}, hint: {type: string, default: '${{ inputs.creds.[index].pass }}'}}}"},
			map[string]any{"creds": []any{map[string]any{"user": "a", "pass": "changed", "hint": sentinel}, map[string]any{"user": "b", "pass": "p2"}}},
		},
		{
			"a password's default, copied into a list's default row",
			[]string{"db_pass: {type: password, default: '${{ org.DB_PASS }}'}", envRows("{name: DB_PASS, value: '${{ inputs.db_pass }}'}")},
			map[string]any{"db_pass": variable, "env": []any{row("DB_PASS", variable)}},
		},
		{
			"a list's default rows, the first deleted",
			[]string{"pw: {type: password}", envRows("{name: DB_HOST, value: db}", "{name: DB_PASS, value: '${{ inputs.pw }}'}")},
			map[string]any{"pw": sentinel, "env": []any{row("DB_PASS", sentinel)}},
		},
		{
			"a list's default rows, a row added before them",
			[]string{"pw: {type: password}", envRows("{name: DB_PASS, value: '${{ inputs.pw }}'}")},
			map[string]any{"pw": sentinel, "env": []any{row("X", "y"), row("DB_PASS", sentinel)}},
		},
		{
			"a variable in a list's default rows, the first deleted",
			[]string{envRows("{name: A, value: a}", "{name: REGION, value: '${{ org.REGION }}'}")},
			map[string]any{"env": []any{row("REGION", variable)}},
		},
		{
			"a default whose dependency the form doesn't follow",
			[]string{"db-pass: {type: password}", "dsn: {type: string, default: 'postgres://u:${{ inputs[''db-pass''] }}@h'}"},
			map[string]any{"db-pass": "changed", "dsn": "postgres://u:" + sentinel + "@h"},
		},
		{
			"toJSON of a group, its keys in the order the form added them",
			[]string{"db: {type: group, items: {pass: {type: password}, host: {type: string}}}", "cfg: {type: string, default: '${{ toJSON(inputs.db) }}'}"},
			map[string]any{"db": map[string]any{"host": "h", "pass": sentinel}, "cfg": "{\n  \"pass\": \"" + sentinel + "\",\n  \"host\": \"h\"\n}"},
		},
		{
			"what the form filled in, edited",
			[]string{"pw: {type: password}", "dsn: {type: string, default: 'postgres://u:${{ inputs.pw }}@h/db'}"},
			map[string]any{"pw": sentinel, "dsn": "postgres://u:" + sentinel + "@h/db?sslmode=require"},
		},
		{
			"what the form filled in, of the wrong type",
			[]string{"pw: {type: password}", "n: {type: number, default: '${{ inputs.pw }}'}", "d: {type: dropdown, options: [a, b], default: '${{ inputs.pw }}'}"},
			map[string]any{"pw": sentinel, "n": sentinel + "-old", "d": sentinel + "-old"},
		},
		{
			"a flattened wizard step's password sent under the step's name",
			[]string{"creds: {type: step, title: Creds, options: {token: {type: password}}}", "n: {type: number, default: '${{ inputs.creds.token }}'}"},
			map[string]any{"creds": map[string]any{"token": sentinel}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := compile.Options{Contexts: map[string]any{"org": map[string]any{"DB_PASS": variable, "REGION": variable}}}
			p, diags := compile.Compile("workflow.yaml", []byte(inputs(tc.defs...)), tc.submitted, opts)
			for _, d := range diags {
				if s := fmt.Sprint(d.Params) + d.Message(); strings.Contains(s, sentinel) || strings.Contains(s, variable) {
					t.Errorf("%s shows it", d)
				}
			}
			if p != nil {
				encoded, err := plan.Encode(p)
				if err != nil {
					t.Fatal(err)
				}
				if s := string(encoded); strings.Contains(s, sentinel) || strings.Contains(s, variable) {
					t.Errorf("the plan holds it: %s", encoded)
				}
			}
		})
	}
}

// What a default can read a password through is found to its end, from
// the definitions alone: a default that reads one that reads one, in any
// order; an option made from one; a read of each of a group's inputs; a
// list's default row, read at another index, as a person can add a row
// before it; and what a relative reference reaches, by any key that reads
// a list's item or a slice of it, or in what a string gives. A form's
// value in such a default's place holds none in the plan.
func TestTaintFollowsWhatDefaultsRead(t *testing.T) {
	type taintCase struct {
		name      string
		defs      []string
		submitted map[string]any
	}
	cases := []taintCase{
		{
			"a default that reads a default that reads a password, before them",
			[]string{
				"pw: {type: password}",
				"a: {type: string, default: '${{ inputs.b }}'}",
				"b: {type: string, default: '${{ inputs.c }}'}",
				"c: {type: string, default: '${{ inputs.pw }}'}",
			},
			map[string]any{"pw": sentinel, "a": sentinel, "b": sentinel, "c": sentinel},
		},
		{
			"a default that reads an option made from a password",
			[]string{"pw: {type: password}", "choice: {type: dropdown, options: ['${{ inputs.pw }}', none]}", "copy: {type: string, default: 'x${{ inputs.choice }}'}"},
			map[string]any{"pw": sentinel, "choice": sentinel, "copy": "x" + sentinel},
		},
		{
			"a default that reads each of a group's inputs",
			[]string{"g: {type: group, items: {gp: {type: password}}}", `copy: {type: string, default: '${{ join(inputs.g.*, ",") }}'}`},
			map[string]any{"g": map[string]any{"gp": sentinel}, "copy": sentinel},
		},
		{
			"a default that reads a list's default row at another index",
			[]string{"pw: {type: password}", "l: {type: list, template: {v: {type: string}}, default: [{v: '${{ inputs.pw }}'}]}", "copy: {type: string, default: '${{ inputs.l[1].v }}'}"},
			map[string]any{"pw": sentinel, "l": []any{map[string]any{"v": "added"}, map[string]any{"v": sentinel}}, "copy": sentinel},
		},
		{
			"a default that reads a field of each input, one that reads a password",
			[]string{
				"g: {type: group, items: {gp: {type: password}}}",
				"h: {type: group, items: {x: {type: string, default: '${{ inputs.g.gp }}'}}}",
				`copy: {type: string, default: '${{ join(inputs.*.x, ",") }}'}`,
			},
			map[string]any{"g": map[string]any{"gp": sentinel}, "h": map[string]any{"x": sentinel}, "copy": sentinel},
		},
		{
			"a default that reads a list's default row by a relative reference",
			[]string{"pw: {type: password}", "l: {type: list, template: {v: {type: string}}, default: [{v: '${{ inputs.pw }}'}]}", "copy: {type: string, default: '${{ ..l.default[0].v }}'}"},
			map[string]any{"pw": sentinel, "copy": sentinel},
		},
		{
			"a default that reads, by a relative reference, a key of what a string gives",
			[]string{
				"g: {type: group, items: {gp: {type: password}}}",
				"cfg: {type: string, default: '${{ toJSON(inputs.g) }}'}",
				"copy: {type: string, default: '${{ ..cfg.default.gp }}'}",
			},
			map[string]any{"g": map[string]any{"gp": sentinel}, "copy": sentinel},
		},
		{
			"a default that reads a slice of a list's default rows by a relative reference",
			[]string{"pw: {type: password}", "ld: {type: list, template: {v: {type: string}}, default: [{v: '${{ inputs.pw }}'}]}", "copy: {type: string, default: '${{ toJSON(..ld.default[0:1]) }}'}"},
			map[string]any{"pw": sentinel, "copy": `[{"v":"` + sentinel + `"}]`},
		},
		{
			"a default that reads an item of a slice of a list's default rows by a relative reference",
			[]string{"pw: {type: password}", "ld: {type: list, template: {v: {type: string}}, default: [{v: '${{ inputs.pw }}'}]}", "copy: {type: string, default: '${{ ..ld.default[0:1][0].v }}'}"},
			map[string]any{"pw": sentinel, "copy": sentinel},
		},
		{
			"a row's default that reads, by a relative reference, one that reads its row's password",
			[]string{"l: {type: list, template: {p: {type: password}, g: {type: string, default: '${{ inputs.l[index].p }}'}, h: {type: string, default: '${{ ..g.default }}'}}}"},
			map[string]any{"l": []any{map[string]any{"p": sentinel, "g": sentinel, "h": sentinel}}},
		},
	}
	// A relative reference reads a list's item, or a slice of it, by any
	// key that reads one.
	for _, key := range []string{"0:1", ":1", "-2:-1", "00", "+0"} {
		cases = append(cases, taintCase{
			"a default that reads an option made from a password by a relative reference, at " + key,
			[]string{"pw: {type: password}", "choice: {type: dropdown, options: ['${{ inputs.pw }}', none]}", "copy: {type: string, default: '${{ toJSON(..choice.options[" + key + "]) }}'}"},
			map[string]any{"pw": sentinel, "choice": "none", "copy": `["` + sentinel + `"]`},
		})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, diags := compile.Compile("workflow.yaml", []byte(inputs(tc.defs...)), tc.submitted, compile.Options{})
			if p == nil {
				t.Fatal(diags)
			}
			encoded, err := plan.Encode(p)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), sentinel) {
				t.Errorf("the plan holds the password: %s", encoded)
			}
		})
	}
}

// A value the plan doesn't hold, in which a form's text for an expression
// that reads a secret is, is held but for that text, which stays the
// expression for the run's start: the run's start evaluates it, and puts
// the rest in its place.
func TestHeldValuesKeepTheirExpressions(t *testing.T) {
	src := inputs("l: {type: list, default: '${{ fromJSON(org.ROWS) }}', template: {x: {type: string, default: '${{ vars.SECRET }}'}, y: {type: string}}}")
	opts := compile.Options{Contexts: map[string]any{
		"org":  map[string]any{"ROWS": `[{"x": "a", "y": "b"}]`},
		"vars": map[string]any{"SECRET": expr.Unknown{Name: "vars"}},
	}}
	inputs, diags := compile.ResolveInputs(load(t, src), map[string]any{"l": []any{map[string]any{"x": "${{ vars.SECRET }}", "y": "typed"}}}, opts)
	if len(diags) != 0 {
		t.Fatal(diags)
	}
	if got := fmt.Sprint(deferredText(t, inputs.Deferred)["l"]); got != "[map[x:${{ vars.SECRET }}]]" {
		t.Errorf("l is %s as the run starts, want x's expression", got)
	}
	if len(inputs.Private) != 1 || inputs.Private["l.0.y"] != "typed" {
		t.Errorf("private %v, want l.0.y alone", inputs.Private)
	}
}

// A part already held for the run's start as one expression, a list's
// default row's, isn't kept as a secret because it isn't what another, its
// row's default, gives.
func TestPartsHeldForTheStartAreNotSecrets(t *testing.T) {
	src := inputs(
		"pw: {type: password}",
		"other: {type: string, default: o}",
		"l: {type: list, template: {h: {type: string, default: '${{ inputs.other }}${{ inputs.pw }}'}}, default: [{h: '${{ inputs.pw }}'}]}",
	)
	inputs, diags := compile.ResolveInputs(load(t, src), map[string]any{"pw": sentinel, "l": []any{map[string]any{"h": sentinel}}}, compile.Options{})
	if len(diags) != 0 || len(inputs.Secrets) != 1 {
		t.Errorf("secrets %v, %v; want pw's alone", inputs.Secrets, diags)
	}
	if got := fmt.Sprint(deferredText(t, inputs.Deferred)["l"]); got != "[map[h:${{ inputs.pw }}]]" {
		t.Errorf("l is %s as the run starts", got)
	}
}

// A value the plan doesn't hold, in the place of what reads a password or a
// secret variable, is a secret, masked as what is made from one: the plan
// names it among Secrets and Derived. One in the place of what reads only
// an organization or user variable that isn't a secret is private, as the
// variable's value is, and not masked. A password is a secret whatever made
// it, masked as one.
func TestHeldValuesAreSecretOrPrivate(t *testing.T) {
	src := inputs(
		"pw: {type: password}",
		"pw2: {type: password, default: '${{ inputs.pw }}'}",
		"dsn: {type: string, default: 'u:${{ inputs.pw }}'}",
		"key: {type: string, default: '${{ vars.KEY }}'}",
		"owner: {type: string, default: '${{ org.OWNER }}'}",
	)
	opts := compile.Options{Contexts: map[string]any{
		"org":  map[string]any{"OWNER": "research"},
		"vars": map[string]any{"KEY": expr.Unknown{Name: "vars"}},
	}}
	submitted := map[string]any{"pw": "a", "pw2": "b", "dsn": "typed", "key": "typed", "owner": "typed"}
	p, diags := compile.Compile("workflow.yaml", []byte(src), submitted, opts)
	if p == nil {
		t.Fatal(diags)
	}
	if want := []string{"inputs.dsn", "inputs.key", "inputs.pw", "inputs.pw2"}; !slices.Equal(p.Run.Secrets, want) {
		t.Errorf("secrets %v, want %v", p.Run.Secrets, want)
	}
	if want := []string{"inputs.dsn", "inputs.key"}; !slices.Equal(p.Run.Derived, want) {
		t.Errorf("derived %v, want %v", p.Run.Derived, want)
	}
	if want := []string{"inputs.owner"}; !slices.Equal(p.Run.Private, want) {
		t.Errorf("private %v, want %v", p.Run.Private, want)
	}
	if len(p.Run.Inputs.Get().(map[string]any)) != 0 {
		t.Errorf("the plan holds %v", p.Run.Inputs.Get())
	}
}

// A relative reference reads what it reaches: a value in the place of a
// default that reads a definition that reads no variable is data, and one
// in the place of a default that reads a definition that reads a variable
// isn't held in the plan.
func TestRelativeReferencesReadWhatTheyReach(t *testing.T) {
	src := inputs(
		"plain: {type: string, default: x}",
		"region: {type: string, default: '${{ org.REGION }}'}",
		"a: {type: string, default: '${{ ..plain.default }}'}",
		"b: {type: string, default: '${{ ..region.default }}'}",
	)
	opts := compile.Options{Contexts: map[string]any{"org": map[string]any{"REGION": "us-east"}}}
	inputs, diags := compile.ResolveInputs(load(t, src), map[string]any{"a": "typed", "b": "typed"}, opts)
	if len(diags) != 0 || inputs.Values["a"] != "typed" || inputs.Private["b"] != "typed" {
		t.Errorf("values %v, private %v, %v; want a typed and b private", inputs.Values, inputs.Private, diags)
	}
}

// A value submitted for an input the workflow doesn't define, taken for a
// password, is read as one: a default that reads it is evaluated as the
// run starts, and so is a value a form sent that is what the default
// gives.
func TestUnknownPasswordsAreRead(t *testing.T) {
	src := inputs(
		"hosts: {type: step, title: Host, multi: true, options: {key: {type: password}}}",
		"copy: {type: string, default: '${{ inputs.key }}'}",
	)
	for _, submitted := range []map[string]any{{"key": sentinel}, {"key": sentinel, "copy": sentinel}} {
		inputs, diags := compile.ResolveInputs(load(t, src), submitted, compile.Options{})
		if diags.HasErrors() || holdsSentinel(t, inputs.Values) || inputs.Secrets["key"] != sentinel {
			t.Errorf("%v: values %v, secrets %v, %v", submitted, inputs.Values, inputs.Secrets, diags)
		}
		if _, ok := inputs.Deferred["copy"]; !ok {
			t.Errorf("%v: deferred %v, want copy", submitted, inputs.Deferred)
		}
	}
}

// A submitted value is compared with its default once the values are
// filled in, and what reads another input's part held for the run's start
// is held too: so a condition can't read a value that is what its default,
// which reads a password and a part of such an input, gives.
func TestComparedValueReadsHeldInput(t *testing.T) {
	src := inputs(
		"pw: {type: password}",
		"l: {type: list, template: {a: {type: string, default: '${{ vars.SECRET }}'}, b: {type: string}}}",
		"x: {type: string, default: '${{ inputs.l[0].b }}-${{ inputs.pw }}'}",
		"y: {type: string, optional: true, hidden: '${{ inputs.x == ''z'' }}'}",
	)
	submitted := map[string]any{"pw": sentinel, "l": []any{map[string]any{"a": "${{ vars.SECRET }}", "b": "bee"}}, "x": "bee-" + sentinel}
	opts := compile.Options{Contexts: map[string]any{"vars": map[string]any{"SECRET": expr.Unknown{Name: "vars"}}}}
	_, diags := compile.ResolveInputs(load(t, src), submitted, opts)
	if len(diags) != 1 || diags[0].Code != problems.InputConditionNotKnown.Code() || diags[0].Params["input"] != "y" {
		t.Errorf("diagnostics %v, want %s for y", diags, problems.InputConditionNotKnown.Code())
	}
}

// A multi-dropdown's item that is the text of an expression option is
// evaluated as the run starts, as that option, and one that is an option
// written in the workflow is that option: neither is kept as a secret.
func TestTextItemsAreNotCompared(t *testing.T) {
	src := inputs("pw: {type: password}", "m: {type: multi-dropdown, options: ['${{ inputs.pw }}', plain]}")
	inputs, diags := compile.ResolveInputs(load(t, src), map[string]any{"pw": sentinel, "m": []any{"${{ inputs.pw }}", "plain"}}, compile.Options{})
	if len(diags) != 0 || len(inputs.Secrets) != 1 {
		t.Errorf("secrets %v, %v; want pw's alone", inputs.Secrets, diags)
	}
	if got := deferredText(t, inputs.Deferred)["m"]; fmt.Sprint(got) != "[${{ inputs.pw }} plain]" {
		t.Errorf("m is %v as the run starts", got)
	}
}

// A value a form can have made from a password is read as one from the
// start, before it is compared with what made it: a default that reads it
// is evaluated as the run starts.
func TestFormValuesAreReadAsPasswords(t *testing.T) {
	src := inputs(
		"pw: {type: password}",
		"copy: {type: string, default: '${{ inputs.pw }}'}",
		"n: {type: number, default: '${{ length(inputs.copy) }}'}",
	)
	inputs, diags := compile.ResolveInputs(load(t, src), map[string]any{"pw": sentinel, "copy": sentinel}, compile.Options{})
	if len(diags) != 0 || len(inputs.Values) != 0 {
		t.Errorf("values %v, %v; want none", inputs.Values, diags)
	}
	if _, ok := inputs.Deferred["n"]; !ok {
		t.Errorf("deferred %v, want n", inputs.Deferred)
	}
}

// A choice of an option that is an expression which reads no variable and
// no password is data, as the option's value, though another option reads
// a password.
func TestChoicesOfPlainOptionsAreData(t *testing.T) {
	src := inputs("pw: {type: password}", "a: {type: string, default: x}", "d: {type: dropdown, options: ['${{ inputs.pw }}', '${{ inputs.a }}']}")
	inputs, diags := compile.ResolveInputs(load(t, src), map[string]any{"pw": sentinel, "d": "x"}, compile.Options{})
	if len(diags) != 0 || inputs.Values["d"] != "x" || len(inputs.Deferred) != 0 {
		t.Errorf("values %v, deferred %v, %v; want d x", inputs.Values, inputs.Deferred, diags)
	}
}

// A value submitted in place of a default that reads no variable and no
// password is data, as what a person types is: it is neither compared nor
// kept as a secret.
func TestTypedValuesStayData(t *testing.T) {
	src := inputs("b: {type: string, default: x}", "a: {type: string, default: '${{ inputs.b }}-a'}")
	inputs, diags := compile.ResolveInputs(load(t, src), map[string]any{"a": "typed"}, compile.Options{})
	if len(diags) != 0 || inputs.Values["a"] != "typed" || len(inputs.Secrets) != 0 {
		t.Errorf("values %v, secrets %v, %v", inputs.Values, inputs.Secrets, diags)
	}
}

// Defaults that read each other are reported, and only they, though an
// input that reads them has a submitted value that its default, which reads
// a password, is compared with.
func TestDefaultsReadEachOtherListsOnlyThem(t *testing.T) {
	src := inputs(
		"pw: {type: password}",
		"c: {type: string, default: '${{ inputs.e }}${{ inputs.pw }}'}",
		"d: {type: string, default: '${{ inputs.c }}'}",
		"e: {type: string, default: '${{ inputs.f }}'}",
		"f: {type: string, default: '${{ inputs.e }}'}",
	)
	_, diags := compile.ResolveInputs(load(t, src), map[string]any{"pw": sentinel, "c": "typed"}, compile.Options{})
	if len(diags) != 1 || diags[0].Code != problems.DefaultsReadEachOther.Code() || diags[0].Params["inputs"] != `"e", "f"` {
		t.Errorf("diagnostics %v, want %s for e and f", diags, problems.DefaultsReadEachOther.Code())
	}
}

// A list with a password among its rows' inputs whose default reads
// another default that reads it never has its rows found, so neither are
// the passwords among them: the values a run with that error returns hold
// none of its value.
func TestPasswordsInDefaultsThatReadEachOther(t *testing.T) {
	src := inputs(
		"b: {type: password, default: '${{ inputs.l }}'}",
		"l: {type: list, template: {p: {type: password}, q: {type: string}}, default: [{q: '${{ fromJSON(inputs.b) }}'}]}",
	)
	submitted := map[string]any{"l": []any{map[string]any{"p": sentinel, "q": "${{ fromJSON(inputs.b) }}"}}}
	inputs, diags := compile.ResolveInputs(load(t, src), submitted, compile.Options{})
	if !slices.ContainsFunc(diags, func(d problems.Diagnostic) bool { return d.Code == problems.DefaultsReadEachOther.Code() }) {
		t.Errorf("diagnostics %v, want %s", diags, problems.DefaultsReadEachOther.Code())
	}
	if holdsSentinel(t, inputs.Values) || holdsSentinel(t, inputs.Deferred) {
		t.Errorf("values %v or deferred %v hold the password", inputs.Values, inputs.Deferred)
	}
}

// Values submitted for defaults that read each other, and neither a
// variable nor a password, are data, as on their own. One whose default
// reads them, and a password, is compared with what its default gives once
// the values are filled in, and means its default.
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

// A password beside, or in a row of, a value the compiler holds as derived
// is masked as a secret's own text, however short, though the derived
// value holds it too: as when a list held as derived has rows with a
// password field.
func TestDerivedValuesMaskPasswords(t *testing.T) {
	opts := compile.Options{Contexts: map[string]any{
		"vars": map[string]any{"SECRET": expr.Unknown{Name: "vars"}, "ROWS": expr.Unknown{Name: "vars"}},
	}}
	for _, tc := range []struct {
		name      string
		defs      []string
		submitted map[string]any
	}{
		{
			"row field over a secret default, beside a password",
			[]string{"creds: {type: list, template: {host: {type: string, default: '${{ vars.SECRET }}'}, pass: {type: password}}}"},
			map[string]any{"creds": []any{map[string]any{"host": "h1", "pass": "pw1"}}},
		},
		{
			"a list whose default reads a secret, rows with a password",
			[]string{"creds: {type: list, default: '${{ fromJSON(vars.ROWS) }}', template: {user: {type: string}, pass: {type: password}}}"},
			map[string]any{"creds": []any{map[string]any{"user": "typed", "pass": "pw1"}}},
		},
		{
			"a group whose field reads a secret, beside a password",
			[]string{"db: {type: group, items: {host: {type: string, default: '${{ vars.SECRET }}'}, pass: {type: password}}}"},
			map[string]any{"db": map[string]any{"host": "typed", "pass": "pw1"}},
		},
		{
			"a string over a default that reads a password",
			[]string{"pw: {type: password}", "dsn: {type: string, default: 'u:${{ inputs.pw }}'}"},
			map[string]any{"pw": "pw1", "dsn": "typed"},
		},
		{
			"a list over a default made from a password",
			[]string{"pw: {type: password}", "l: {type: list, default: '${{ fromJSON(format(''[{{\"p\":\"{0}\"}}]'', inputs.pw)) }}', template: {p: {type: password}, q: {type: string}}}"},
			map[string]any{"pw": "pw1", "l": []any{map[string]any{"p": "pw2", "q": "typed"}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, diags := compile.Compile("workflow.yaml", []byte(inputs(tc.defs...)), tc.submitted, opts)
			if p == nil || len(p.Run.Derived) == 0 {
				t.Fatal(p, diags)
			}
			masked := plan.RunValues{Inputs: plan.DataOf(tc.submitted), Secrets: p.Run.Secrets, Derived: p.Run.Derived}.Masked()
			for _, pw := range []string{"pw1", "pw2"} {
				if strings.Contains(fmt.Sprint(tc.submitted), pw) && !slices.Contains(masked, pw) {
					t.Errorf("password %q isn't masked: %q", pw, masked)
				}
			}
			if slices.Contains(masked, "typed") {
				t.Errorf("a short derived value is masked: %q", masked)
			}
		})
	}
}
