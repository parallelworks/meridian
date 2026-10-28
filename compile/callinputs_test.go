package compile

import (
	"maps"
	"slices"
	"testing"

	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/problems"
)

// calledInputs is a workflow a step calls, with inputs of each kind that
// is required, or isn't.
const calledInputs = `on:
  execute:
    inputs:
      name: {type: string, label: Name}
      count: {type: number}
      chosen: {type: dropdown, options: ["", a]}
      picked: {type: dropdown, options: [a, b]}
      defaulted: {type: string, default: x}
      blank: {type: string, default: ""}
      flag: {type: boolean}
      maybe: {type: string, optional: true}
      shown: {type: string, hidden: true}
      dropped: {type: string, ignore: true}
      head: {type: header, text: Head}
      group:
        type: group
        items:
          inner: {type: string}
          spare: {type: string, optional: true}
      flat:
        type: group
        flatten: true
        items:
          beside: {type: string}
      covered:
        type: group
        hidden: true
        items:
          deep: {type: string}
      gone:
        type: group
        ignore: true
        items:
          under: {type: string}
      rows:
        type: list
        template:
          cell: {type: string}
      first:
        type: step
        title: First
        options:
          stepped: {type: string}
jobs:
  main:
    steps:
      - run: echo
`

// TestRequiredLeftOutAgreesWithCreate checks that, for a with: that holds
// no expression, of inputs whose conditions are written out, the inputs
// requiredLeftOut finds left out are those the child's Create, given the
// with:, finds no value for (input_required): each that is required, in a
// group, beside a group that flattens or in a wizard step, given no value,
// or "" without an option that is "", or under a group's value that isn't
// a mapping; and none that has a default, is a boolean, or that it or a
// group around it makes optional, hidden or ignored.
func TestRequiredLeftOutAgreesWithCreate(t *testing.T) {
	doc, diags := format.Load("w.yaml", []byte(calledInputs))
	if diags.HasErrors() {
		t.Fatal(diags)
	}
	defs, _ := jsonptr.Lookup(doc.Value, definitionsPath)
	all := map[string]any{
		"name": "n", "count": 1, "chosen": "a", "picked": "a", "group": map[string]any{"inner": "i"},
		"beside": "b", "rows": []any{}, "stepped": "s",
	}
	with := func(changes map[string]any) map[string]any {
		out := maps.Clone(all)
		maps.Copy(out, changes)
		return out
	}
	for _, tc := range []struct {
		name string
		with map[string]any
		want []string
	}{
		{name: "none", with: map[string]any{}, want: []string{"beside", "chosen", "count", "group.inner", "name (Name)", "picked", "rows", "stepped"}},
		{name: "all", with: all},
		{name: "empty", with: with(map[string]any{"name": "", "chosen": "", "picked": "", "beside": ""}), want: []string{"beside", "name (Name)", "picked"}},
		{name: "null", with: with(map[string]any{"name": nil, "group": map[string]any{"inner": "i", "spare": ""}})},
		{name: "group not a mapping", with: with(map[string]any{"group": "text"}), want: []string{"group.inner"}},
		{name: "group null", with: with(map[string]any{"group": nil}), want: []string{"group.inner"}},
		{name: "group empty", with: with(map[string]any{"group": map[string]any{}}), want: []string{"group.inner"}},
		{name: "others", with: with(map[string]any{"defaulted": "", "blank": "", "maybe": "", "shown": "", "dropped": "", "spare": ""})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := requiredLeftOut(defs.(map[string]any), tc.with)
			slices.Sort(got)
			if !slices.Equal(got, tc.want) {
				t.Errorf("requiredLeftOut = %q, want %q", got, tc.want)
			}
			_, diags := Compile("w.yaml", []byte(calledInputs), tc.with, Options{})
			var required []string
			for _, d := range diags.Errors() {
				if d.Code == problems.InputRequired.Code() {
					required = append(required, d.Params["input"].(string))
				}
			}
			slices.Sort(required)
			if !slices.Equal(got, required) {
				t.Errorf("requiredLeftOut = %q, but Create finds %q left out: %v", got, required, diags)
			}
		})
	}
}

// TestRequiredLeftOutLeavesTheRunToCheck checks that what is known only as
// the step runs isn't left out: a with: that is an expression, a value an
// expression gives, there or around it, and an input whose hidden:,
// ignore: or optional:, or its group's, is an expression.
func TestRequiredLeftOutLeavesTheRunToCheck(t *testing.T) {
	defs := map[string]any{
		"name":   map[string]any{"type": "string"},
		"hidden": map[string]any{"type": "string", "hidden": "${{ inputs.name == 'a' }}"},
		"ignore": map[string]any{"type": "string", "ignore": "${{ inputs.name == 'a' }}"},
		"maybe":  map[string]any{"type": "string", "optional": "${{ inputs.name == 'a' }}"},
		"group": map[string]any{"type": "group", "hidden": "${{ inputs.name == 'a' }}", "items": map[string]any{
			"inner": map[string]any{"type": "string"},
		}},
		"other": map[string]any{"type": "group", "items": map[string]any{"part": map[string]any{"type": "string"}}},
		"pick":  map[string]any{"type": "dropdown", "options": []any{"a", map[string]any{"value": "${{ inputs.name }}"}}},
	}
	for name, with := range map[string]any{
		"with: an expression": "${{ inputs.all }}",
		"values expressions": map[string]any{
			"name": "${{ inputs.name }}", "other": map[string]any{"part": "${{ needs.a.outputs.b }}"}, "pick": "${{ inputs.pick }}",
		},
		"a group's value":         map[string]any{"name": "n", "other": "${{ inputs.other }}", "pick": "a"},
		"an option's expressions": map[string]any{"name": "n", "other": map[string]any{"part": "p"}, "pick": ""},
	} {
		if got := requiredLeftOut(defs, with); len(got) > 0 {
			t.Errorf("%s: requiredLeftOut = %q", name, got)
		}
	}
	if got := requiredLeftOut(defs, nil); !slices.Equal(got, []string{"name", "other.part", "pick"}) {
		t.Errorf("requiredLeftOut of no with: = %q", got)
	}
}
