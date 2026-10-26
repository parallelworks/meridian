// Package inputpath holds how format 1 places the values of a workflow's
// inputs: where among the inputs' values each input definition's value
// is. A group's and a wizard step's inputs have their values under its
// key, unless it flattens them beside it: a group with flatten: true, and
// a wizard step unless $meta.wizard.flatten, beside the steps, is false. A
// list's value, and a repeated wizard step's, is a list of rows, each a
// group of the inputs its template, or its options, define.
//
// The compiler reads the inputs with it (compile), and so does the plan
// where it follows a reference to an input to its definition
// (plan.Plan.CancelPath), so the two place every value alike.
package inputpath

import (
	"slices"
	"strings"

	"github.com/parallelworks/meridian/internal/jsonvalue"
)

// An Input is one input definition, and where it and its value are.
type Input struct {
	// Key is its key among the definitions around it, and Def its
	// definition. Type is Def's type:, or "" for none.
	Key  string
	Def  map[string]any
	Type string
	// Loc is the definition's path, and Path its value's path among the
	// inputs' values. Each is its own slice.
	Loc, Path []string
	// Container is set for a group or a wizard step that holds other inputs
	// rather than a value of its own, and Flatten for one whose inputs'
	// values are beside its own key rather than under it.
	Container, Flatten bool
}

// Inputs returns the input definitions among defs, which are at loc, with
// their values at path, in the order of their keys: each that is a
// mapping, but those whose keys start with $, such as $meta. It doesn't
// return those they hold (Input.Inner).
func Inputs(defs map[string]any, loc, path []string) []Input {
	var out []Input
	flattenSteps := WizardFlattens(defs)
	for key, v := range jsonvalue.Sorted(defs) {
		def, ok := v.(map[string]any)
		if !ok || strings.HasPrefix(key, "$") {
			continue
		}
		typ, _ := def["type"].(string)
		in := Input{Key: key, Def: def, Type: typ, Loc: slices.Concat(loc, []string{key}), Path: slices.Concat(path, []string{key})}
		switch _, hasItems := def["items"].(map[string]any); {
		case typ == "header":
		case typ == "step":
			if _, ok := def["options"].(map[string]any); ok && def["multi"] != true {
				in.Container, in.Flatten = true, flattenSteps
			}
		case hasItems && typ != "list":
			in.Container, in.Flatten = true, def["flatten"] == true
		}
		out = append(out, in)
	}
	return out
}

// Inner returns the input definitions that a container holds, a group's
// items or a wizard step's options, where they are, and where their values
// are: under the container's own value's path, or beside it if it
// flattens them. defs is nil for any input other than a container. Each
// place is its own slice.
func (in Input) Inner() (defs map[string]any, loc, path []string) {
	if !in.Container {
		return nil, nil, nil
	}
	key := "items"
	if in.Type == "step" {
		key = "options"
	}
	defs, _ = in.Def[key].(map[string]any)
	path = in.Path
	if in.Flatten {
		path = path[:len(path)-1]
	}
	return defs, slices.Concat(in.Loc, []string{key}), slices.Clone(path)
}

// Find returns the input definition among defs, and those they hold,
// whose value is at the longest prefix of path that one's is, and that
// prefix's length; of two at one path, the first, in the order of their
// keys and with each container before the inputs it holds. ok is false if
// none is.
func Find(defs map[string]any, path []string) (found Input, n int, ok bool) {
	var visit func(defs map[string]any, loc, at []string)
	visit = func(defs map[string]any, loc, at []string) {
		for _, in := range Inputs(defs, loc, at) {
			if len(in.Path) > n && len(in.Path) <= len(path) && slices.Equal(in.Path, path[:len(in.Path)]) {
				found, n, ok = in, len(in.Path), true
			}
			if inner, innerLoc, innerPath := in.Inner(); inner != nil {
				visit(inner, innerLoc, innerPath)
			}
		}
	}
	visit(defs, nil, nil)
	return found, n, ok
}

// RowsKey returns the key of the definitions of the inputs that each row
// of def's value is a group of: template for a list, and options for a
// repeated wizard step, one with multi true. ok is false for any other
// input.
func RowsKey(def map[string]any) (key string, ok bool) {
	switch {
	case def["type"] == "list":
		return "template", true
	case Repeats(def):
		return "options", true
	}
	return "", false
}

// Repeats reports whether def is a wizard step with multi true, which the
// person running the workflow adds as many times as they like: its value is
// a list of rows, one for each time, as a list's is.
func Repeats(def map[string]any) bool {
	return def["type"] == "step" && def["multi"] == true
}

// WizardFlattens reports whether the wizard steps among defs have their
// inputs' values beside their own: unless $meta.wizard.flatten is false.
func WizardFlattens(defs map[string]any) bool {
	meta, _ := defs["$meta"].(map[string]any)
	wizard, _ := meta["wizard"].(map[string]any)
	return wizard["flatten"] != false
}
