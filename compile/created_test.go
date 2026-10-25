package compile_test

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
)

// The plan holds what Start needs to complete the run's values as Create
// would have: the input definitions as the workflow writes them; where
// each part of an input held for Start is in them, with the rows it is
// for; the decisions of the run's shape that read a variable or a
// password, and no others; a job's needs: that read one; remote, sessions:
// and the names a session provider gave them; and the limits.
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
	parts := map[string]plan.DeferredPart{}
	for _, part := range r.DeferredParts {
		parts[part.Name] = part
	}
	want := map[string]plan.DeferredPart{
		"inputs.region":       {Name: "inputs.region", Location: "/on/execute/inputs/region/default", Default: true},
		"inputs.cluster.name": {Name: "inputs.cluster.name", Location: "/on/execute/inputs/cluster/items/name/default", Default: true},
		"inputs.config":       {Name: "inputs.config", Location: "/on/execute/inputs/config/default", Default: true},
		"inputs.hosts.0.zone": {Name: "inputs.hosts.0.zone", Location: "/on/execute/inputs/hosts/template/zone/default", Index: []int{0}, Default: true},
		"inputs.hosts.1.zone": {Name: "inputs.hosts.1.zone", Location: "/on/execute/inputs/hosts/template/zone/default", Index: []int{1}, Default: true},
		// The form sent the option's text, which what it reads of the
		// value reads.
		"inputs.choice": {Name: "inputs.choice", Location: "/on/execute/inputs/choice/options/0", Text: true},
	}
	if !reflect.DeepEqual(parts, want) {
		t.Errorf("parts %v, want %v", parts, want)
	}
	if got := slices.Sorted(maps.Keys(r.DeferredInputs)); !slices.Equal(got, []string{"choice", "cluster.name", "config", "hosts", "region"}) {
		t.Errorf("deferred inputs %v", got)
	}
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
	var records []compile.Record
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
