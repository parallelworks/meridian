package compile_test

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// The plan holds what Start needs to resolve the run's inputs again as
// Create did: the input definitions as the workflow writes them; the
// decisions of the run's shape that read a variable or a password, and no
// others; a job's needs: that read one; remote, sessions: and the names a
// session provider gave them; and the limits.
func TestPlanHoldsWhatStartNeeds(t *testing.T) {
	src := `on:
  execute:
    inputs:
      region: {type: string, default: '${{ org.REGION }}', hidden: '${{ org.HIDE == "y" }}'}
      plain: {type: string, default: x, optional: '${{ inputs.region == "" }}', ignore: false}
      cluster: {type: group, items: {name: {type: string, default: 'c-${{ vars.TOKEN }}'}}}
      config:
        type: string
        default: '{"a": "${{ vars.TOKEN }}", "b": "${{ inputs.plain }}"}'
      hosts:
        type: list
        template:
          name: {type: string}
          zone: {type: string, default: '${{ org.REGION }}-${{ inputs.hosts[index].name }}'}
      choice:
        type: dropdown
        options: ['${{ vars.TOKEN }}', none]
sessions:
  desktop:
    useTLS: false
jobs:
  a:
    steps:
      - run: echo
  b:
    needs: ${{ fromJSON(org.NEEDS) }}
    steps:
      - run: echo
  c:
    needs: [a]
    steps:
      - run: echo
`
	submitted := map[string]any{
		"hosts":  []any{map[string]any{"name": "h0"}, map[string]any{"name": "h1"}},
		"choice": "${{ vars.TOKEN }}",
	}
	opts := compile.Options{
		Inputs: passThrough{},
		Contexts: map[string]any{
			"org":      map[string]any{"REGION": "east", "HIDE": "n", "NEEDS": `["a"]`},
			"vars":     map[string]any{"TOKEN": expr.Unknown{Name: "vars"}},
			"remote":   map[string]any{"repo": "https://example.com/r", "branch": "main"},
			"sessions": map[string]any{"desktop": "desktop-1"},
		},
		Limits: plan.Limits{MaxInstances: 9},
	}
	c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), submitted, opts)
	if err != nil || c == nil {
		t.Fatal(diags, err)
	}
	r := c.Plan.Run
	// optional reads the region, which reads a variable; ignore: false is
	// written out, and reads nothing. The plan names them, and the secret
	// record holds what they decided.
	if want := []string{"/on/execute/inputs/plain/optional", "/on/execute/inputs/region/hidden"}; !slices.Equal(slices.Sorted(slices.Values(r.Decisions)), want) {
		t.Errorf("decisions %v, want %v", r.Decisions, want)
	}
	if want := map[string]bool{"/on/execute/inputs/plain/optional": false, "/on/execute/inputs/region/hidden": false}; !maps.Equal(c.Record.Decisions, want) {
		t.Errorf("decided %v, want %v", c.Record.Decisions, want)
	}
	for _, j := range c.Plan.Jobs {
		if got, want := !j.NeedsExpr.IsZero(), j.Name == "b"; got != want {
			t.Errorf("job %s's needs: held %v", j.Name, got)
		}
	}
	if defs, _ := r.Definitions.Written().(map[string]any); len(defs) != 6 || defs["config"].(map[string]any)["default"] != `{"a": "${{ vars.TOKEN }}", "b": "${{ inputs.plain }}"}` {
		t.Errorf("definitions %v", defs)
	}
	if !maps.Equal(r.Remote, map[string]string{"repo": "https://example.com/r", "branch": "main"}) ||
		!maps.Equal(r.SessionNames, map[string]string{"desktop": "desktop-1"}) || r.Sessions.IsZero() || r.Limits.MaxInstances != 9 {
		t.Errorf("remote %v, sessions %v %v, limits %v", r.Remote, r.Sessions.Written(), r.SessionNames, r.Limits)
	}
	if want := []string{"org.HIDE", "org.NEEDS", "org.REGION"}; !slices.Equal(r.Variables, want) {
		t.Errorf("variables %v, want %v", r.Variables, want)
	}
	if want := []string{"vars.TOKEN"}; !slices.Equal(r.Secrets, want) {
		t.Errorf("secrets %v, want %v", r.Secrets, want)
	}
}

// A password input's default, a password field's of a list's rows and a
// group's among them, and the default of a list whose rows have a password
// input, can hold a password, which a plan never holds: the plan names
// where each is, and the secret record holds it.
func TestPlanWithholdsPasswordDefaults(t *testing.T) {
	const sentinel = "chang3me-SENTINEL"
	for defs, want := range map[string][]string{
		"pw: {type: password, default: chang3me-SENTINEL}": {"/on/execute/inputs/pw/default"},
		"l: {type: list, template: {p: {type: password, default: chang3me-SENTINEL}, u: {type: string}}, default: [{u: a}]}": {
			"/on/execute/inputs/l/default", "/on/execute/inputs/l/template/p/default",
		},
		"l: {type: list, template: {p: {type: password}, u: {type: string}}, default: [{u: a, p: chang3me-SENTINEL}]}": {"/on/execute/inputs/l/default"},
		"g: {type: group, items: {p: {type: password, default: chang3me-SENTINEL}, u: {type: string, default: kept}}}": {"/on/execute/inputs/g/items/p/default"},
	} {
		c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(inputs(defs)), nil, compile.Options{})
		if err != nil || c == nil {
			t.Fatalf("%s: %v, %v", defs, diags, err)
		}
		data, err := plan.Encode(c.Plan)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), sentinel) {
			t.Errorf("%s: the plan holds the password's default: %s", defs, data)
		}
		if !slices.Equal(c.Plan.Run.Withheld, want) || !slices.Equal(slices.Sorted(maps.Keys(c.Record.Withheld)), want) {
			t.Errorf("%s: withheld %v, record %v, want %v", defs, c.Plan.Run.Withheld, c.Record.Withheld, want)
		}
		if !strings.Contains(fmt.Sprint(c.Record.Withheld), sentinel) {
			t.Errorf("%s: the record holds %v", defs, c.Record.Withheld)
		}
	}
	// What can hold no password stays.
	c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(inputs("u: {type: string, default: kept}")), nil, compile.Options{})
	if err != nil || c == nil || len(c.Plan.Run.Withheld) != 0 || c.Record.Withheld != nil {
		t.Errorf("%v, %v: withheld %v", diags, err, c.Plan.Run.Withheld)
	}
}

// What a condition of the run's shape that read a password decided can say
// something of the password, a bit at a time, so the plan names the
// condition and the secret record holds what it decided: the plans of two
// runs whose passwords differ are the same.
func TestPlanHoldsNoDecisionOfAPassword(t *testing.T) {
	var defs strings.Builder
	defs.WriteString("pw: {type: password}\n")
	for i, c := range "abcdefghijklmnopqrstuvwxyz0123456789" {
		fmt.Fprintf(&defs, "h%02d: {type: string, optional: true, hidden: '${{ contains(inputs.pw, ''%c'') }}'}\n", i, c)
	}
	src := workflow(strings.TrimSuffix(defs.String(), "\n"))
	var encoded []string
	var records []plan.Record
	for _, pw := range []string{"hunter2", "zzzzzzz"} {
		c := createRun(t, src, map[string]any{"pw": pw}, nil)
		if len(c.Plan.Run.Decisions) != 36 {
			t.Fatalf("decisions %v", c.Plan.Run.Decisions)
		}
		data, err := plan.Encode(c.Plan)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, string(data))
		records = append(records, c.Record)
	}
	if encoded[0] != encoded[1] {
		t.Errorf("the plans differ:\n%s\n%s", encoded[0], encoded[1])
	}
	if maps.Equal(records[0].Decisions, records[1].Decisions) {
		t.Errorf("the records' decisions are the same: %v", records[0].Decisions)
	}
}

// A plan is the same, byte for byte, whatever a password or a variable that
// isn't a secret is, in each shape what decides the run's shape or what is
// held can take: an ignore, a group's ignore, an optional, a wizard step's
// input, a list's or a nested list's passwords, a default typed over, a
// value read from an input a password ignored, the default rows the plan
// withholds, and a number a password hides. The secret record round-trips
// through JSON, and Start gives the same with it, as it decides what the
// plan leaves to it: a run whose optional input a password made required
// fails to start without it.
func TestPlansDontSayWhatSecretsAre(t *testing.T) {
	given := func(key string) func(string) map[string]any {
		return func(s string) map[string]any { return map[string]any{key: s} }
	}
	for _, tc := range []struct {
		name      string
		defs      string
		submitted func(secret string) map[string]any
		vars      func(secret string) testVariables
		required  string
	}{
		{"an ignore with a default", "pw: {type: password}\nc: {type: string, default: kept, ignore: '${{ inputs.pw == ''hunter2'' }}'}", given("pw"), nil, ""},
		{"an optional with no value", "pw: {type: password}\nc: {type: string, optional: '${{ inputs.pw == ''hunter2'' }}'}", given("pw"), nil, "zzz"},
		{"an ignore with no value", "pw: {type: password}\nc: {type: string, ignore: '${{ inputs.pw == ''hunter2'' }}'}", given("pw"), nil, "zzz"},
		{"a group's ignore", "pw: {type: password}\ng: {type: group, ignore: '${{ inputs.pw == ''hunter2'' }}', items: {x: {type: string, default: v}, y: {type: string, optional: true, hidden: '${{ inputs.g.x == ''v'' }}'}}}", given("pw"), nil, ""},
		{
			"a list with password rows", "l: {type: list, template: {p: {type: password}, u: {type: string}}, default: [{u: a, p: d}]}",
			func(s string) map[string]any { return map[string]any{"l": []any{map[string]any{"u": "a", "p": s}}} }, nil, "",
		},
		{
			"a nested list with passwords", "l: {type: list, template: {inner: {type: list, template: {p: {type: password}}}, u: {type: string}}}",
			func(s string) map[string]any {
				return map[string]any{"l": []any{map[string]any{"u": "a", "inner": []any{map[string]any{"p": s}}}}}
			}, nil, "",
		},
		{
			"a default that reads a password, typed over", "pw: {type: password}\nd: {type: string, default: 'u:${{ inputs.pw }}'}",
			func(s string) map[string]any { return map[string]any{"pw": s, "d": "typed-" + s} }, nil, "",
		},
		{
			"a wizard step's input", "s: {type: step, title: S, options: {sp: {type: password}, sx: {type: string, default: kept, ignore: '${{ inputs.s.sp == ''hunter2'' }}'}, sd: {type: string, default: 'd${{ inputs.s.sp }}'}}}",
			given("sp"), nil, "",
		},
		{"a value read from an input a password ignored", "pw: {type: password}\nc: {type: string, default: kept, ignore: '${{ inputs.pw == ''hunter2'' }}'}\nd: {type: string, default: '${{ inputs.c }}-x'}", given("pw"), nil, ""},
		{"default rows withheld", "l: {type: list, template: {p: {type: password}, u: {type: string}}, default: [{u: a, p: hunter2}]}", nil, nil, ""},
		{
			"an ignore that reads a variable", "c: {type: string, default: kept, ignore: '${{ org.FLAG == ''on'' }}'}", nil,
			func(s string) testVariables {
				if s == "hunter2" {
					return testVariables{"org.FLAG": {Value: "on"}}
				}
				return testVariables{"org.FLAG": {Value: "off"}}
			}, "",
		},
		{
			"a default from a variable", "c: {type: string, default: '${{ org.FLAG }}'}", nil,
			func(s string) testVariables { return testVariables{"org.FLAG": {Value: s}} }, "",
		},
		{"a number a password hides", "pw: {type: password}\nn: {type: number, default: 5, max: 10, hidden: '${{ length(inputs.pw) > 5 }}'}", given("pw"), nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var plans []string
			for _, secret := range []string{"hunter2", "zzz"} {
				var submitted map[string]any
				if tc.submitted != nil {
					submitted = tc.submitted(secret)
				}
				var vars testVariables
				if tc.vars != nil {
					vars = tc.vars(secret)
				}
				created := createRun(t, workflow(tc.defs), submitted, vars)
				data, err := plan.Encode(created.Plan)
				if err != nil {
					t.Fatal(err)
				}
				plans = append(plans, string(data))
				record, err := plan.EncodeRecord(&created.Record)
				if err != nil {
					t.Fatal(err)
				}
				back := *created
				decoded, err := plan.DecodeRecord(record)
				if err != nil {
					t.Fatal(err)
				}
				back.Record = *decoded
				res, diags := startRun(t, created, vars, compile.StartInput{RunID: "run"})
				again, diags2 := startRun(t, &back, vars, compile.StartInput{RunID: "run"})
				if fmt.Sprint(diags) != fmt.Sprint(diags2) || (res == nil) != (again == nil) || res != nil && canonical(t, res.Run) != canonical(t, again.Run) {
					t.Errorf("%s: Start differs with the record through JSON: %v, %v", secret, diags, diags2)
				}
				required := slices.ContainsFunc(diags, func(d problems.Diagnostic) bool { return d.Code == problems.InputRequired.Code() })
				if required != (secret == tc.required) {
					t.Errorf("%s: Start %v", secret, diags)
				}
			}
			if plans[0] != plans[1] {
				t.Errorf("the plans differ:\n%s\n%s", plans[0], plans[1])
			}
		})
	}
}
