package compile

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/internal/reach"
	"github.com/parallelworks/meridian/problems"
)

// A field is an input definition.
type field struct {
	key string
	def map[string]any
	// loc is the definition's path in the workflow, and path its value's
	// path in the values.
	loc, path []string
	typ       string
	group     *field
	// container is set for a group or a wizard step, which holds other
	// inputs rather than a value of its own; flatten for one whose inputs'
	// values are beside its own key rather than under it: a step, unless
	// $meta.wizard.flatten is false, or a group with flatten true. A value
	// given at a flattened group's key is kept (ownsKey).
	container, flatten bool
	// children are a container's inputs.
	children []*field
	// header is set for a header, which has no value.
	header bool

	hasDefault bool
	hidden     bool
	ignored    bool
	optional   bool
	// shaped is set for an input whose ignore read a variable or a
	// password, or one in a group whose did: the plan holds it the same
	// whatever that decided, out of its values, and Start drops it if it
	// is ignored (applyConditions). ignoredLater is set for one ignored by
	// such a decision, which only Start applies.
	shaped, ignoredLater bool
	// hiddenDecides, ignoreDecides and optionalDecides are set for each of
	// its conditions that is a decision of the run's shape, which Start
	// makes again (condition).
	hiddenDecides, ignoreDecides, optionalDecides bool
	// deferred is set for an input whose value is evaluated once the run is
	// underway, because its default, or a part of its value, reads something
	// the compiler doesn't know. variable is set for one whose value reads
	// an organization or user variable, or a password input, which the
	// compiler knows but a plan never holds: what the compiler evaluates
	// reads its value, and Start resolves it again.
	deferred, variable bool
	// secretWhole is set for a list, a group or a wizard step with a
	// password among its inputs, whose value, or one of whose rows, is
	// evaluated as the run starts: the plan can't say which of its values
	// are passwords, so the run's secrets name all of it.
	secretWhole bool

	// index holds the indexes of the list rows that a field of a list's
	// template is for, outermost first, and in is the input whose value
	// holds the rows. Both are nil for an input.
	index []int
	in    *field
	// rows holds the fields of a list's or a repeated step's rows, once its
	// value is known, by the rows' indexes: none for a row that isn't a
	// mapping.
	rows [][]*field
	// seq is the field's place among the inputs' fields and then the rows'
	// fields, the order evalValues fills in their values.
	seq int
	// resolved is set for an input of a platform's type, or a field of one
	// in a list's rows, once the Inputs resolver was asked for its value,
	// as soon as the value was known (resolvePlatform), and given holds the
	// value it was asked for, which compare reads; failed is set if it
	// couldn't give one, which was reported then. resolveLater is set for
	// one whose value compare makes, as the run starts, before the
	// resolver is asked for it (meansLater).
	resolved, failed, resolveLater bool
	given                          any
	// options is what checking the value reads of the field's options,
	// once it is read, which the fields of a list's rows share with its
	// template's.
	options *options
	// texts are the strings in the value that are a form's text for an
	// expression, and reads the expressions that filling in the value
	// reads: its default's, or its texts'.
	texts []text
	reads []*expr.Expr
	// candidates are the parts of a submitted value that a form can have
	// made from a variable or a password, as it evaluates each default and
	// option it can and sends what it gives (compare).
	candidates []candidate
	// later holds the parts of an input's value that are evaluated once the
	// run is underway, until they are in dflt, and laterAt their indexes by
	// their paths, so that the parts at a row, however deep, are found
	// without going through every row's.
	later   []laterPart
	laterAt *reach.Trie[int]
}

// A template is the fields of a list's template, or a repeated step's
// inputs, with their paths in a row, and the index in fields of each one's
// group, or -1.
type template struct {
	fields []*field
	groups []int
}

// options is what checking a field's value reads of its options, found
// once for the field.
type options struct {
	found bool
	// values are its options' values (optionValues), and exprs those that
	// hold expressions (expressionOptions). template is set if any holds
	// one, and empty if one is "".
	values          []any
	exprs           []exprOption
	template, empty bool
	// keys holds each value's looseKey, once a value is checked, and
	// literals those of the values that hold no expression, once a choice
	// is compared (compare).
	keys, literals map[string]bool
}

// literal reports whether v is one of the options' values that hold no
// expression, by its looseKey, as checkOption matches them.
func (o *options) literal(v any) bool {
	if o.literals == nil {
		o.literals = map[string]bool{}
		for _, value := range o.values {
			if !hasTemplate(value) {
				o.literals[looseKey(value)] = true
			}
		}
	}
	return o.literals[looseKey(v)]
}

// collect returns the fields that the definitions in defs, at loc in the
// workflow, define, and those in them, whose values are at path.
func (r *inputResolver) collect(defs map[string]any, loc, path []string, group *field) []*field {
	var out []*field
	flattenSteps := wizardFlattens(defs)
	for key, v := range jsonvalue.Sorted(defs) {
		def, ok := v.(map[string]any)
		if !ok || strings.HasPrefix(key, "$") {
			continue
		}
		typ, _ := def["type"].(string)
		f := &field{key: key, def: def, loc: appendPath(loc, key), path: appendPath(path, key), typ: typ, group: group}
		_, f.hasDefault = def["default"]
		out = append(out, f)
		if group != nil {
			group.children = append(group.children, f)
		}
		switch items, hasItems := def["items"].(map[string]any); {
		case typ == "header":
			f.header = true
		case typ == "step":
			options, ok := def["options"].(map[string]any)
			if !ok || def["multi"] == true {
				continue
			}
			f.container, f.flatten = true, flattenSteps
			out = append(out, r.collect(options, appendPath(f.loc, "options"), f.childPath(), f)...)
		case hasItems && typ != "list":
			f.container, f.flatten = true, def["flatten"] == true
			out = append(out, r.collect(items, appendPath(f.loc, "items"), f.childPath(), f)...)
		}
	}
	return out
}

// checkShared reports each two input definitions, of defs at loc in the
// workflow and those in them, whose values have one key in one mapping,
// where a group that flattens puts one beside the other
// (flattened_name_taken): ACTIVATE fills such a value from both, in the
// order the file writes them, which a map doesn't keep. A group that
// flattens counts at its own key too, which a value given there, and
// ignoring the group, reach. Wizard steps whose inputs share a key are
// left as they were.
func (r *inputResolver) checkShared(defs map[string]any, loc []string) {
	type definition struct {
		loc     []string
		brought bool // by a group that flattens
	}
	byKey := map[string][]definition{}
	var gather func(defs map[string]any, loc []string, brought bool)
	gather = func(defs map[string]any, loc []string, brought bool) {
		for key, v := range jsonvalue.Sorted(defs) {
			def, ok := v.(map[string]any)
			if !ok || strings.HasPrefix(key, "$") {
				continue
			}
			at := appendPath(loc, key)
			byKey[key] = append(byKey[key], definition{at, brought})
			typ, _ := def["type"].(string)
			items, hasItems := def["items"].(map[string]any)
			options, hasOptions := def["options"].(map[string]any)
			switch {
			case typ == "step" && hasOptions && def["multi"] != true && wizardFlattens(defs):
				gather(options, appendPath(at, "options"), brought)
			case typ == "step" && hasOptions:
				r.checkShared(options, appendPath(at, "options"))
			case typ == "list":
				if template, ok := def["template"].(map[string]any); ok {
					r.checkShared(template, appendPath(at, "template"))
				}
			case hasItems && def["flatten"] == true:
				gather(items, appendPath(at, "items"), true)
			case hasItems:
				r.checkShared(items, appendPath(at, "items"))
			}
		}
	}
	gather(defs, loc, false)
	for key, found := range jsonvalue.Sorted(byKey) {
		for _, d := range found[1:] {
			if d.brought || found[0].brought {
				r.reportAt(problems.FlattenedNameTaken, d.loc, problems.Params{
					"input": key, "first": definitionName(found[0].loc), "second": definitionName(d.loc),
				})
			}
		}
	}
}

// wizardFlattens reports whether the wizard steps among defs have their
// inputs' values beside their own: unless $meta.wizard.flatten is false.
func wizardFlattens(defs map[string]any) bool {
	meta, _ := defs["$meta"].(map[string]any)
	wizard, _ := meta["wizard"].(map[string]any)
	return wizard["flatten"] != false
}

// definitionName names the input definition at loc in the workflow by its
// keys in the input definitions, as in group.name, or list.name for a
// field of a list's rows.
func definitionName(loc []string) string {
	var names []string
	for i := len(definitionsPath); i < len(loc); i += 2 {
		names = append(names, loc[i])
	}
	return strings.Join(names, ".")
}

// holder returns the input whose value holds f's: f, or the input whose
// rows f is in.
func (f *field) holder() *field {
	if f.in != nil {
		return f.in
	}
	return f
}

// repeats reports whether f is a wizard step with multi true, which the
// person running the workflow adds as many times as they like: its value is
// a list of rows, one for each time, as a list's is.
func (f *field) repeats() bool {
	return f.typ == "step" && f.def["multi"] == true
}

// rowInputs returns the definitions of the inputs that each row of f's
// value is a group of, and where they are in the workflow: a list's
// template, or a repeated step's inputs. It returns nil for any other
// input.
func (f *field) rowInputs() (map[string]any, []string) {
	key := "template"
	switch {
	case f.repeats():
		key = "options"
	case f.typ != "list":
		return nil, nil
	}
	defs, ok := f.def[key].(map[string]any)
	if !ok {
		return nil, nil
	}
	return defs, appendPath(f.loc, key)
}

// holdsPassword reports whether def, an input's definition, is a password
// input's, or defines one among its inputs: a group's, a wizard step's, or
// those of a list's rows.
func holdsPassword(def map[string]any) bool {
	if def["type"] == "password" {
		return true
	}
	for _, key := range []string{"items", "options", "template"} {
		defs, _ := def[key].(map[string]any)
		for name, v := range defs {
			if d, ok := v.(map[string]any); ok && !strings.HasPrefix(name, "$") && holdsPassword(d) {
				return true
			}
		}
	}
	return false
}

// ownsKey reports whether the value at f's key goes with f: whether
// ignoring f drops it, and an ignore that read a variable or a password
// holds it. It does for every input but a wizard step whose inputs'
// values are beside its key, which takes no ignore. A group that flattens
// has none of its inputs' values there, but a value given there is dropped
// with them, as ACTIVATE drops it.
func (f *field) ownsKey() bool {
	return !f.container || !f.flatten || f.typ != "step"
}

// childPath is where a container's inputs' values are.
func (f *field) childPath() []string {
	if f.flatten {
		return f.path[:len(f.path)-1]
	}
	return f.path
}

// label names an input in messages: its path, and its label if it has one.
func (f *field) label() string {
	name := strings.Join(f.path, ".")
	if l, ok := f.def["label"].(string); ok && l != "" && l != name {
		return fmt.Sprintf("%s (%s)", name, l)
	}
	return name
}

// checkSyntax reports the syntax errors in the expressions in v, at loc in
// the input definitions: in a default, an option, a label or anywhere else,
// whether or not resolving the inputs evaluates it. An evaluation that
// reads one reports the same diagnostic, which sorting the diagnostics
// drops, so each is reported once. It appends to loc in place, so that the
// walk reuses one path: the caller's loc must own the room after it.
func (r *inputResolver) checkSyntax(loc []string, v any) {
	switch v := v.(type) {
	case string:
		if !strings.Contains(v, "${{") {
			return
		}
		if _, err := r.workflow.Template(v); err != nil {
			r.reportError(loc, 0, err)
		}
	case []any:
		for i, item := range v {
			r.checkSyntax(append(loc, strconv.Itoa(i)), item)
		}
	case map[string]any:
		// The diagnostics are sorted, so the keys' order doesn't matter.
		for k, item := range v {
			r.checkSyntax(append(loc, k), item)
		}
	}
}

// template returns the fields that defs, at loc in the workflow, define
// for each row of a list or a repeated step, collected once.
func (r *inputResolver) template(defs map[string]any, loc []string) *template {
	key := jsonptr.Format(loc)
	if t, ok := r.templates[key]; ok {
		return t
	}
	t := &template{fields: r.collect(defs, loc, nil, nil)}
	at := make(map[*field]int, len(t.fields))
	for i, f := range t.fields {
		f.options = &options{} // which each row's field shares
		at[f] = i
		g, ok := at[f.group]
		if !ok {
			g = -1
		}
		t.groups = append(t.groups, g)
	}
	if r.templates == nil {
		r.templates = map[string]*template{}
	}
	r.templates[key] = t
	return t
}

// row returns the fields of the row at path: a copy of each of the
// template's, with its path in the row's value, in the row's own groups.
func (t *template) row(path []string) []*field {
	structs := make([]field, len(t.fields))
	out := make([]*field, len(t.fields))
	for i, tf := range t.fields {
		f := &structs[i]
		*f = *tf
		f.path = slices.Concat(path, tf.path)
		f.group, f.children = nil, nil
		if g := t.groups[i]; g >= 0 {
			f.group = out[g]
			f.group.children = append(f.group.children, f)
		}
		out[i] = f
	}
	return out
}

// An exprOption is one of an input's options whose value holds an
// expression, and where the value is in the workflow.
type exprOption struct {
	value string
	loc   []string
}

// expressionOptions returns the options, at loc in the workflow, whose
// values hold expressions: options as a list, or a list in each of their
// groups.
func expressionOptions(options any, loc []string) []exprOption {
	var out []exprOption
	switch o := options.(type) {
	case []any:
		for i, item := range o {
			at := appendPath(loc, strconv.Itoa(i))
			if m, ok := item.(map[string]any); ok {
				item, at = m["value"], appendPath(at, "value")
			}
			if s, ok := item.(string); ok && strings.Contains(s, "${{") {
				out = append(out, exprOption{s, at})
			}
		}
	case map[string]any:
		for k, group := range jsonvalue.Sorted(o) {
			if _, ok := group.([]any); ok {
				out = append(out, expressionOptions(group, appendPath(loc, k))...)
			}
		}
	}
	return out
}

// findOptions returns what checking f's value reads of its options,
// finding it the first time.
func (f *field) findOptions() *options {
	if f.options == nil {
		f.options = &options{}
	}
	o := f.options
	if o.found {
		return o
	}
	o.found = true
	raw := f.def["options"]
	o.values = optionValues(raw)
	o.exprs = expressionOptions(raw, appendPath(f.loc, "options"))
	o.template = hasTemplate(raw)
	o.empty = slices.ContainsFunc(o.values, func(v any) bool { return v == "" })
	return o
}

// optionValues returns a dropdown's option values: its options' values, or
// for options grouped by key, every group's.
func optionValues(options any) []any {
	var out []any
	switch o := options.(type) {
	case []any:
		for _, item := range o {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m["value"])
			} else {
				out = append(out, item)
			}
		}
	case map[string]any:
		for _, group := range jsonvalue.Sorted(o) {
			out = append(out, optionValues(group)...)
		}
	}
	return out
}

func hasTemplate(v any) bool {
	switch v := v.(type) {
	case string:
		return strings.Contains(v, "${{")
	case []any:
		return slices.ContainsFunc(v, hasTemplate)
	case map[string]any:
		for _, item := range v {
			if hasTemplate(item) {
				return true
			}
		}
	}
	return false
}
