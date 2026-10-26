package inputpath

import (
	"reflect"
	"slices"
	"testing"
)

// defs are input definitions with a plain input, a header that has items,
// a group under its key and one that flattens, a list with rows, a wizard
// step, a repeated one, one with no options, and what isn't a definition.
func defs() map[string]any {
	return map[string]any{
		"$meta": map[string]any{"wizard": map[string]any{"flatten": true}},
		"name":  map[string]any{"type": "string"},
		"head":  map[string]any{"type": "header", "items": map[string]any{"h": map[string]any{"type": "string"}}},
		"group": map[string]any{"type": "group", "items": map[string]any{"a": map[string]any{"type": "string"}}},
		"flat": map[string]any{"type": "group", "flatten": true, "items": map[string]any{
			"b":     map[string]any{"type": "string"},
			"inner": map[string]any{"type": "group", "items": map[string]any{"c": map[string]any{"type": "string"}}},
		}},
		"rows":   map[string]any{"type": "list", "items": map[string]any{"x": map[string]any{}}, "template": map[string]any{"r": map[string]any{}}},
		"step":   map[string]any{"type": "step", "options": map[string]any{"s": map[string]any{"type": "string"}}},
		"again":  map[string]any{"type": "step", "multi": true, "options": map[string]any{"t": map[string]any{}}},
		"bare":   map[string]any{"type": "step"},
		"text":   "not a definition",
		"$other": map[string]any{"type": "string"},
	}
}

// Inputs gives each definition at its place, in the order of their keys,
// and says which hold others, and which flatten them.
func TestInputs(t *testing.T) {
	type got struct {
		key, typ           string
		loc, path          []string
		container, flatten bool
	}
	var out []got
	for _, in := range Inputs(defs(), []string{"inputs"}, []string{"v"}) {
		out = append(out, got{in.Key, in.Type, in.Loc, in.Path, in.Container, in.Flatten})
	}
	want := []got{
		{"again", "step", []string{"inputs", "again"}, []string{"v", "again"}, false, false},
		{"bare", "step", []string{"inputs", "bare"}, []string{"v", "bare"}, false, false},
		{"flat", "group", []string{"inputs", "flat"}, []string{"v", "flat"}, true, true},
		{"group", "group", []string{"inputs", "group"}, []string{"v", "group"}, true, false},
		{"head", "header", []string{"inputs", "head"}, []string{"v", "head"}, false, false},
		{"name", "string", []string{"inputs", "name"}, []string{"v", "name"}, false, false},
		{"rows", "list", []string{"inputs", "rows"}, []string{"v", "rows"}, false, false},
		{"step", "step", []string{"inputs", "step"}, []string{"v", "step"}, true, true},
	}
	if !reflect.DeepEqual(out, want) {
		t.Errorf("Inputs =\n%v\nwant\n%v", out, want)
	}
}

// A container's inputs are under its key, or beside it when it flattens
// them, and each place is a slice of its own.
func TestInner(t *testing.T) {
	ins := Inputs(defs(), []string{"inputs"}, []string{"v"})
	byKey := map[string]Input{}
	for _, in := range ins {
		byKey[in.Key] = in
	}
	for key, want := range map[string]struct {
		loc, path []string
		keys      []string
	}{
		"group": {[]string{"inputs", "group", "items"}, []string{"v", "group"}, []string{"a"}},
		"flat":  {[]string{"inputs", "flat", "items"}, []string{"v"}, []string{"b", "inner"}},
		"step":  {[]string{"inputs", "step", "options"}, []string{"v"}, []string{"s"}},
	} {
		inner, loc, path := byKey[key].Inner()
		var keys []string
		for _, in := range Inputs(inner, loc, path) {
			keys = append(keys, in.Key)
		}
		if !slices.Equal(loc, want.loc) || !slices.Equal(path, want.path) || !slices.Equal(keys, want.keys) {
			t.Errorf("%s holds %v at %v, values at %v; want %v at %v, values at %v", key, keys, loc, path, want.keys, want.loc, want.path)
		}
	}
	for _, key := range []string{"name", "head", "rows", "again", "bare"} {
		if inner, loc, path := byKey[key].Inner(); inner != nil || loc != nil || path != nil {
			t.Errorf("%s holds %v at %v", key, inner, loc)
		}
	}
	// Changing one place changes no other.
	group := byKey["group"]
	_, loc, path := group.Inner()
	loc[0], path[1] = "changed", "changed"
	if group.Loc[0] != "inputs" || group.Path[1] != "group" {
		t.Error("an input's inner places share its slices")
	}
	group.Path[0] = "changed"
	if byKey["flat"].Loc[0] != "inputs" || byKey["flat"].Path[0] != "v" || path[0] != "v" {
		t.Error("places share their slices")
	}
}

// A wizard step flattens its inputs unless $meta.wizard.flatten is false.
func TestWizardFlattens(t *testing.T) {
	for meta, want := range map[string]bool{"": true, "true": true, "false": false} {
		d := map[string]any{"step": map[string]any{"type": "step", "options": map[string]any{"s": map[string]any{}}}}
		switch meta {
		case "true":
			d["$meta"] = map[string]any{"wizard": map[string]any{"flatten": true}}
		case "false":
			d["$meta"] = map[string]any{"wizard": map[string]any{"flatten": false}}
		}
		if got := WizardFlattens(d); got != want {
			t.Errorf("$meta %q flattens %v", meta, got)
		}
		in := Inputs(d, nil, []string{"v"})[0]
		_, _, path := in.Inner()
		if in.Flatten != want || slices.Equal(path, []string{"v", "step"}) == want {
			t.Errorf("$meta %q: the step flattens %v, its inputs at %v", meta, in.Flatten, path)
		}
	}
}

// Find gives the definition whose value is at the longest prefix of a
// path, the first of two at one path, and none for a path no value is at.
func TestFind(t *testing.T) {
	d := defs()
	// A plain input beside the flattened group's, at one path.
	d["b"] = map[string]any{"type": "number"}
	for _, tc := range []struct {
		path []string
		loc  []string
		n    int
	}{
		{[]string{"name"}, []string{"name"}, 1},
		{[]string{"name", "deeper"}, []string{"name"}, 1},
		{[]string{"group"}, []string{"group"}, 1},
		{[]string{"group", "a"}, []string{"group", "items", "a"}, 2},
		{[]string{"flat"}, []string{"flat"}, 1},
		{[]string{"b"}, []string{"b"}, 1},
		{[]string{"inner", "c"}, []string{"flat", "items", "inner", "items", "c"}, 2},
		{[]string{"s"}, []string{"step", "options", "s"}, 1},
		{[]string{"rows", "0", "r"}, []string{"rows"}, 1},
		{[]string{"head", "h"}, []string{"head"}, 1},
		{[]string{"again", "0", "t"}, []string{"again"}, 1},
	} {
		in, n, ok := Find(d, tc.path)
		if !ok || n != tc.n || !slices.Equal(in.Loc, tc.loc) {
			t.Errorf("Find(%v) = %v, %d, %v; want %v, %d", tc.path, in.Loc, n, ok, tc.loc, tc.n)
		}
	}
	if in, n, ok := Find(d, []string{"missing"}); ok || n != 0 || in.Def != nil {
		t.Errorf("Find(missing) = %v, %d, %v", in.Loc, n, ok)
	}
}

// A list's rows are its template's inputs, and a repeated wizard step's
// its options.
func TestRowsKey(t *testing.T) {
	d := defs()
	for key, want := range map[string]string{"rows": "template", "again": "options", "step": "", "group": "", "name": ""} {
		def := d[key].(map[string]any)
		got, ok := RowsKey(def)
		if got != want || ok != (want != "") {
			t.Errorf("RowsKey(%s) = %q, %v; want %q", key, got, ok, want)
		}
		if Repeats(def) != (key == "again") {
			t.Errorf("Repeats(%s) = %v", key, Repeats(def))
		}
	}
}
