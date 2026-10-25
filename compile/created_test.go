package compile_test

import (
	"maps"
	"reflect"
	"slices"
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
		"inputs.choice":       {Name: "inputs.choice", Location: "/on/execute/inputs/choice/options/0"},
	}
	if !reflect.DeepEqual(parts, want) {
		t.Errorf("parts %v, want %v", parts, want)
	}
	if got := slices.Sorted(maps.Keys(r.DeferredInputs)); !slices.Equal(got, []string{"choice", "cluster.name", "config", "hosts", "region"}) {
		t.Errorf("deferred inputs %v", got)
	}
	// optional reads the region, which reads a variable; ignore: false is
	// written out, and reads nothing.
	if want := []plan.Decision{
		{Path: "/on/execute/inputs/plain/optional", Value: false},
		{Path: "/on/execute/inputs/region/hidden", Value: false},
	}; !slices.EqualFunc(sortedDecisions(r.Decisions), want, func(a, b plan.Decision) bool { return a == b }) {
		t.Errorf("decisions %v, want %v", r.Decisions, want)
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

func sortedDecisions(ds []plan.Decision) []plan.Decision {
	out := slices.Clone(ds)
	slices.SortFunc(out, func(a, b plan.Decision) int {
		if a.Path < b.Path {
			return -1
		}
		if a.Path > b.Path {
			return 1
		}
		return 0
	})
	return out
}
