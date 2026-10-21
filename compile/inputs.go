package compile

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/hint"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// ErrInputType is what an InputResolver returns for a type it doesn't
// provide.
var ErrInputType = errors.New("compile: no such input type")

// An InputResolver provides the input types that name a platform's
// resources, such as clusters and Slurm partitions.
type InputResolver interface {
	// ResolveInput checks the value given for an input of a platform type
	// and returns what the run sees for it, such as a cluster's details for
	// its name. It returns ErrInputType for a type it doesn't provide.
	ResolveInput(typ string, definition map[string]any, value any) (any, error)
}

// platformTypes are the input types whose values name a platform's
// resources.
var platformTypes = map[string]bool{
	"compute-clusters": true, "compute-resources": true, "compute-target": true, "execution-groups": true,
	"kubernetes-clusters": true, "kubernetes-namespaces": true, "kubernetes-pods": true, "kubernetes-pvc": true,
	"kubernetes-secrets": true, "kubernetes-configmaps": true, "kubernetes-statefulsets": true,
	"kubernetes-deployments": true, "kubernetes-services": true, "kubernetes-workloads": true,
	"region": true, "zone": true, "instance-type": true, "bucket": true, "storage": true,
	"organization-groups": true, "slurm-accounts": true, "slurm-partitions": true, "slurm-qos": true,
	"pbs-queues": true, "ai-model": true, "allocations": true,
}

// Inputs are a run's input values.
type Inputs struct {
	// Values are the input values, with defaults applied and ignored inputs
	// left out.
	Values map[string]any
	// Deferred are the defaults that read something the compiler doesn't
	// know, such as a secret variable, or an organization or user variable
	// or a password input, whose values a plan never holds, by their
	// input's path, as in group.name. They are evaluated once the run is
	// underway, and their inputs aren't in Values or Secrets. A password's
	// value isn't in one: it is among Secrets.
	Deferred map[string]plan.Value
	// Secrets are the values of password inputs, by their path in the
	// values, such as k8s.token, or creds.0.pass for a password field of a
	// list's first row. The plan doesn't hold them.
	Secrets map[string]any
}

// Options are what the compiler reads besides the workflow.
type Options struct {
	// Source names the workflow. Without a Revision, the revision is the
	// SHA-256 digest of the workflow's text.
	Source plan.Source
	// Inputs provides the input types that name a platform's resources.
	// Nil means there is no platform.
	Inputs InputResolver
	// Contexts are what expressions read when the run is created, besides
	// inputs: org, vars, remote and sessions. The value of a secret
	// variable is an expr.Unknown, so that an expression that reads it is
	// evaluated once the run is underway. The plan holds no organization
	// or user variable's value, secret or not: the compiler reads them to
	// decide the run's shape, such as which inputs are ignored and which
	// jobs a job needs, and keeps what else reads them an expression.
	Contexts map[string]any
}

// definitionsPath is where a workflow defines its inputs.
var definitionsPath = []string{"on", "execute", "inputs"}

// ResolveInputs checks a run's submitted input values against the workflow's
// input definitions, applies defaults, and drops ignored inputs, as
// docs/design.md describes. It reports every problem it finds.
func ResolveInputs(doc *format.Document, submitted map[string]any, opts Options) (*Inputs, problems.Diagnostics) {
	r := resolveInputs(doc, submitted, opts)
	return &Inputs{Values: r.kept(), Deferred: r.deferred(), Secrets: r.secrets}, r.diags
}

// resolveInputs resolves the inputs, keeping their definitions for the
// compiler's checks of what reads them.
func resolveInputs(doc *format.Document, submitted map[string]any, opts Options) *inputResolver {
	r := &inputResolver{
		doc:          doc,
		opts:         opts,
		workflow:     &expr.Workflow{Root: doc.Value, Definitions: definitionsPath},
		budget:       expr.Scope{}.Budgeted(),
		values:       jsonvalue.CloneMap(submitted),
		secrets:      map[string]any{},
		secretsLater: map[string]bool{},
		marks:        [][]string{{"org"}, {"vars"}},
	}
	defs, _ := lookup(doc.Value, definitionsPath).(map[string]any)
	r.fields = r.collect(defs, definitionsPath, nil, nil)
	r.checkSyntax(append(make([]string, 0, 16), definitionsPath...), defs)
	for _, f := range r.fields {
		r.markPassword(f)
	}
	r.checkJSON(r.values)
	r.checkUnknown(r.values, nil)
	for _, f := range r.fields {
		if f.header {
			r.remove(f.path)
		}
	}
	r.fillDefaults()
	r.applyConditions()
	r.checkValues()
	r.diags.Sort()
	return r
}

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
	// inputs rather than a value of its own; flatten for a step whose
	// inputs' values are beside its own key rather than under it.
	container, flatten bool
	// children are a container's inputs.
	children []*field
	// header is set for a header, which has no value.
	header bool

	hasDefault bool
	hidden     bool
	ignored    bool
	optional   bool
	// deferred is set for an input whose value is evaluated once the run is
	// underway, because its default, or a part of its value, reads something
	// the compiler doesn't know. variable is set for one whose value reads
	// an organization or user variable, or a password input, which the
	// compiler knows but a plan never holds: what the compiler evaluates
	// reads its value, and the plan holds what is evaluated as the run
	// starts in its place. dflt holds what is evaluated then.
	deferred, variable bool
	dflt               plan.Value
	// secretRows is set for a list whose rows are evaluated as the run
	// starts, with a password field in them: the plan can't say which of
	// its values are passwords, so the run's secrets name all of it.
	secretRows bool

	// index holds the indexes of the list rows that a field of a list's
	// template is for, outermost first, and in is the input whose value
	// holds the rows. Both are nil for an input.
	index []int
	in    *field
	// texts are the strings in the value that are a form's text for an
	// expression, and reads the expressions that filling in the value
	// reads: its default's, or its texts'.
	texts []text
	reads []*expr.Expr
	// later holds the parts of an input's value that are evaluated once the
	// run is underway, until they are in dflt.
	later []laterPart
}

// A text is a string in a value, at path in it, that is the text of the
// expression at loc in the workflow: the one in the same place in the
// input's default, or one of its options' values. A form sends that text
// for an expression it can't evaluate itself.
type text struct{ path, loc []string }

// A laterPart is a part of an input's value, at path in it, that is
// evaluated once the run is underway: v, a workflow's JSON values with its
// templates parsed. variable is set for a part that reads a variable or a
// password, whose value the compiler knows, and keeps in the values, until
// the plan holds its expressions in its place.
type laterPart struct {
	path     []string
	v        any
	variable bool
}

type inputResolver struct {
	reporter
	opts     Options
	workflow *expr.Workflow
	// budget holds the one budget that every evaluation of the inputs'
	// expressions shares: they are one group, for Meridian's limit on what
	// a group builds. Each template has the language's limit as well.
	budget expr.Scope
	values map[string]any
	// secrets are the values of password inputs, by their paths, and
	// secretsLater the paths of those whose values are evaluated as the run
	// starts, and of lists whose rows are, with one in them: the run's
	// secrets name both.
	secrets      map[string]any
	secretsLater map[string]bool
	// marks are the paths in the contexts of what reads a variable or a
	// password: org, vars, password inputs, password fields of lists' rows,
	// and the parts of the values that read them.
	marks  [][]string
	fields []*field
	// rows are the fields of lists' rows: each field of a list's template,
	// once for each row.
	rows []*field
	// evaluators evaluate expressions with the values as they are, one for
	// the rows at each index, and contexts are what they read. Both are
	// made again once the values change.
	evaluators map[string]*expr.Evaluator
	contexts   map[string]any
}

// collect returns the fields that the definitions in defs, at loc in the
// workflow, define, and those in them, whose values are at path.
func (r *inputResolver) collect(defs map[string]any, loc, path []string, group *field) []*field {
	var out []*field
	flattenSteps := true
	if meta, ok := defs["$meta"].(map[string]any); ok {
		if wizard, ok := meta["wizard"].(map[string]any); ok && wizard["flatten"] == false {
			flattenSteps = false
		}
	}
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
			f.container = true
			out = append(out, r.collect(items, appendPath(f.loc, "items"), f.childPath(), f)...)
		}
	}
	return out
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

// evaluatedLater reports whether f's value is evaluated as the run starts,
// as a part of its input's value is, or is in one; and if so, whether the
// compiler knows it all the same, as it does a part that reads only
// variables and passwords.
func (f *field) evaluatedLater() (later, known bool) {
	in := f.holder()
	at := f.path[len(in.path):]
	known = true
	for _, p := range in.later {
		if len(p.path) <= len(at) && slices.Equal(p.path, at[:len(p.path)]) {
			later, known = true, known && p.variable
		}
	}
	return later, later && known
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

// markPassword marks f's value if f is a password input, or a password
// field of a list's row, as a variable's is: what is made from it is
// evaluated as the run starts, and what is wrong with it isn't shown.
func (r *inputResolver) markPassword(f *field) {
	if f.typ == "password" {
		r.marks = append(r.marks, slices.Concat([]string{"inputs"}, f.path))
	}
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

// checkJSON reports, and drops, submitted values that aren't JSON values,
// which a plan can't hold.
func (r *inputResolver) checkJSON(values map[string]any) {
	for key, v := range jsonvalue.Sorted(values) {
		if what, bad, _ := notJSON(v); what != "" {
			r.reportAt(problems.InputNotJSON, definitionsPath, problems.Params{"input": key, "what": what, "value": bad})
			delete(values, key)
		}
	}
}

// checkUnknown reports submitted values for inputs the workflow doesn't
// define, at path in the values.
func (r *inputResolver) checkUnknown(values map[string]any, path []string) {
	known := map[string]*field{}
	for _, f := range r.fields {
		if !f.container || !f.flatten {
			if slices.Equal(f.path[:len(f.path)-1], path) {
				known[f.key] = f
			}
		}
	}
	var names []string
	for key, v := range jsonvalue.Sorted(values) {
		f, ok := known[key]
		if !ok {
			if names == nil {
				names = slices.Sorted(maps.Keys(known))
			}
			s, ok := hint.Suggest(key, names)
			params := problems.Params{"input": strings.Join(appendPath(path, key), ".")}
			r.reportAt(problems.UnknownInput, definitionsPath, problems.WithSuggestion(params, strings.Join(appendPath(path, s), "."), ok))
			continue
		}
		if !f.container || f.flatten || v == nil {
			continue
		}
		child, ok := v.(map[string]any)
		if !ok {
			params := problems.Params{"input": f.label(), "value": problems.Show(v)}
			if holdsPassword(f.def) {
				params = mask(params)
			}
			r.reportAt(problems.InputGroupNotMapping, f.loc, params)
			delete(values, key)
			continue
		}
		r.checkUnknown(child, f.path)
	}
}

// fillDefaults fills in the values. Each input without a value gets its
// default, and so does each field of a list's rows, from the list's
// template, once the list's rows are known. A string a form sent as the
// text of an expression gets the expression's value. Expressions are
// evaluated with the other inputs' values, defaults included, in the order
// they need.
func (r *inputResolver) fillDefaults() {
	var pending pendingFields
	var start func(fields []*field)
	start = func(fields []*field) {
		for _, f := range fields {
			if r.prepare(f) {
				pending.add(f)
			} else {
				start(r.collectRows(f))
			}
		}
	}
	start(r.fields)
	for p := pending.next(); p != nil; p = pending.next() {
		r.evalPending(p.f)
		start(r.collectRows(p.f))
		pending.filled(p)
	}
	if left := pending.waiting(); len(left) > 0 {
		names := make([]string, len(left))
		for i, f := range left {
			names[i] = strings.Join(f.path, ".")
		}
		r.reportAt(problems.DefaultsReadEachOther, left[0].loc, problems.Params{"inputs": problems.QuoteAll(names)})
	}
	r.holdLater()
}

// prepare fills in what it can of f's value without evaluating anything: a
// default that has no expressions. It reports whether f waits for its
// default's expressions, or its texts', to be evaluated.
func (r *inputResolver) prepare(f *field) bool {
	if f.container || f.header {
		return false
	}
	if !r.missing(f) {
		f.texts, f.reads = r.findTexts(f)
		return f.texts != nil
	}
	if !f.hasDefault {
		return false
	}
	// The workflow keeps what it parses, for the evaluation.
	reads, err := r.workflow.Exprs(appendPath(f.loc, "default"))
	if err != nil {
		return false // checkSyntax reported it.
	}
	if reads == nil {
		r.setDefault(f, f.def["default"])
		return false
	}
	if rows, ok := f.def["default"].([]any); ok && f.typ == "list" {
		// The rows are filled in as a form's starting rows are, each
		// expression where it is, so that what is known of them is, and
		// their fields' defaults can be filled in too.
		r.set(f.path, jsonvalue.Clone(rows))
		f.texts, f.reads = r.findTexts(f)
		return true
	}
	f.reads = reads
	return true
}

// evalPending evaluates f's texts, or else its default, and fills in its
// value with them. What reads something the compiler doesn't know is
// evaluated once the run is underway, and so is what reads a variable or a
// password, whose value the plan doesn't hold, though the compiler fills it
// in.
func (r *inputResolver) evalPending(f *field) {
	if f.texts == nil {
		loc := appendPath(f.loc, "default")
		switch v, marked, err := r.evalAt(loc, f.index); {
		case expr.IsUnknown(err):
			r.deferPart(f, nil, f.def["default"], false)
		case err != nil:
			r.reportError(loc, 0, maskedIf(marked, err))
		default:
			r.setDefault(f, v)
			if marked {
				r.deferPart(f, nil, f.def["default"], true)
			}
		}
		return
	}
	value := lookup(r.values, f.path)
	for _, t := range f.texts {
		text := lookup(value, t.path)
		switch v, marked, err := r.evalAt(t.loc, f.index); {
		case expr.IsUnknown(err):
			r.deferPart(f, t.path, text, false)
		case err != nil:
			r.reportError(t.loc, 0, maskedIf(marked, err))
		default:
			r.set(slices.Concat(f.path, t.path), v)
			if marked {
				r.deferPart(f, t.path, text, true)
			}
		}
	}
}

// setDefault sets f's value to its default, unless the default is "", which
// gives no value. The value is a copy, so that filling in a list's rows
// doesn't change the workflow.
func (r *inputResolver) setDefault(f *field, v any) {
	if v != "" && v != nil {
		r.set(f.path, jsonvalue.Clone(v))
	}
}

// collectRows returns the fields of f's rows, if f is a list or a repeated
// step whose value is known: each of its rows' inputs, for each row that is
// a mapping.
func (r *inputResolver) collectRows(f *field) []*field {
	defs, loc := f.rowInputs()
	if defs == nil {
		return nil
	}
	rows, _ := lookup(r.values, f.path).([]any)
	var out []*field
	for i, row := range rows {
		if _, ok := row.(map[string]any); !ok {
			continue
		}
		index := append(slices.Clip(f.index), i)
		for _, c := range r.collect(defs, loc, appendPath(f.path, strconv.Itoa(i)), nil) {
			c.index, c.in = index, f.holder()
			r.markPassword(c)
			out = append(out, c)
		}
	}
	if len(out) > 0 {
		r.rows = append(r.rows, out...)
		r.changed()
	}
	return out
}

// findTexts finds the strings in f's value that are a form's text for an
// expression: the one in the same place in its default, or one of its
// options' values. It returns them, and the expressions they hold. Every
// other string is data.
func (r *inputResolver) findTexts(f *field) ([]text, []*expr.Expr) {
	value := lookup(r.values, f.path)
	var texts []text
	for _, p := range defaultTexts(f.def["default"], value, nil) {
		texts = append(texts, text{p, slices.Concat(f.loc, []string{"default"}, p)})
	}
	for _, o := range expressionOptions(f.def["options"], appendPath(f.loc, "options")) {
		if value == o.value {
			texts = append(texts, text{nil, o.loc})
		}
		list, _ := value.([]any)
		for i, item := range list {
			p := []string{strconv.Itoa(i)}
			if item == o.value && !slices.ContainsFunc(texts, func(t text) bool { return slices.Equal(t.path, p) }) {
				texts = append(texts, text{p, o.loc})
			}
		}
	}
	var reads []*expr.Expr
	for _, t := range texts {
		exprs, err := r.workflow.Exprs(t.loc)
		if err != nil {
			continue // checkSyntax reported it.
		}
		reads = append(reads, exprs...)
	}
	return texts, reads
}

// defaultTexts returns the paths, from path, of the strings in v that are
// the text of an expression in the same place in def.
func defaultTexts(def, v any, path []string) [][]string {
	var out [][]string
	switch def := def.(type) {
	case string:
		if s, ok := v.(string); ok && s == def && strings.Contains(s, "${{") {
			out = append(out, path)
		}
	case []any:
		list, _ := v.([]any)
		for i := range min(len(def), len(list)) {
			out = append(out, defaultTexts(def[i], list[i], appendPath(path, strconv.Itoa(i)))...)
		}
	case map[string]any:
		m, _ := v.(map[string]any)
		for k, d := range jsonvalue.Sorted(def) {
			if item, ok := m[k]; ok {
				out = append(out, defaultTexts(d, item, appendPath(path, k))...)
			}
		}
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

// deferPart records that the part of f's value at path, raw in the
// workflow, is evaluated once the run is underway. The value of f's input
// is then evaluated as the run starts, all of it, with raw's expressions
// in that part, parsed for f's rows. A part that reads a variable keeps
// its value until then, and what reads it reads a variable too.
func (r *inputResolver) deferPart(f *field, path []string, raw any, variable bool) {
	v, err := parseFor(raw, f.index)
	if err != nil {
		return // checkSyntax reported it.
	}
	in := f.holder()
	in.later = append(in.later, laterPart{slices.Concat(f.path[len(in.path):], path), v, variable})
	if variable {
		r.marks = append(r.marks, slices.Concat([]string{"inputs"}, f.path, path))
	}
	r.changed()
}

// holdLater makes what the plan holds of each input with parts evaluated
// once the run is underway: its value, with those parts' expressions where
// they are, and without the values of the password fields in its rows,
// which the run's start puts in their places from the secrets. An input
// with a part the compiler doesn't know is deferred; one whose parts read
// only variables and passwords is held for the run's start too, but what
// the compiler evaluates reads its value. A list whose rows a part gives,
// with a password field in them, is a secret as a whole.
func (r *inputResolver) holdLater() {
	if !slices.ContainsFunc(r.fields, func(f *field) bool { return f.later != nil }) {
		return
	}
	// The password fields of each input's rows, and the lists with password
	// fields in their rows, by their paths.
	passwords := map[*field][]*field{}
	lists := map[string]*field{}
	for _, fields := range [][]*field{r.fields, r.rows} {
		for _, f := range fields {
			if f.typ == "password" && f.in != nil {
				passwords[f.in] = append(passwords[f.in], f)
			}
			if defs, _ := f.rowInputs(); defs != nil && holdsPassword(f.def) {
				lists[jsonptr.Format(f.path)] = f
			}
		}
	}
	for _, f := range r.fields {
		if f.later == nil {
			continue
		}
		held := jsonvalue.Clone(lookup(r.values, f.path))
		for _, c := range passwords[f] {
			deletePath(held, c.path[len(f.path):])
		}
		for _, p := range f.later {
			// A part that is a list's value, or one of its rows, gives rows
			// whose passwords the plan can't name.
			at := slices.Concat(f.path, p.path)
			l := lists[jsonptr.Format(at)]
			if n := len(p.path); l == nil && n > 0 && isIndex(p.path[n-1]) {
				l = lists[jsonptr.Format(at[:len(at)-1])]
			}
			if l != nil {
				l.secretRows = true
			}
			if len(p.path) == 0 {
				held = p.v
			} else {
				setIn(held, p.path, p.v)
			}
		}
		f.dflt = plan.Literal(held)
		if slices.ContainsFunc(f.later, func(p laterPart) bool { return !p.variable }) {
			f.deferred = true
		} else {
			f.variable = true
		}
	}
}

// isIndex reports whether a key in a path is a list's index.
func isIndex(key string) bool {
	_, err := strconv.Atoi(key)
	return err == nil
}

// indexPath returns path with each [index] in it the index it reads.
func indexPath(path []string, index []int) []string {
	out := slices.Clone(path)
	k := 0
	for i, name := range out {
		if name == "[index]" && k < len(index) {
			out[i] = strconv.Itoa(index[k])
			k++
		}
	}
	return out
}

// missing reports whether f has no value: none at all, "" when "" isn't one
// of its options, or the text of its default when that is an expression,
// which a form sends when it can't evaluate the default itself.
func (r *inputResolver) missing(f *field) bool {
	v, ok := jsonptr.Lookup(r.values, f.path)
	if !ok || v == nil {
		return true
	}
	if s, isString := v.(string); isString && s != "" && s == f.def["default"] && strings.Contains(s, "${{") {
		return true
	}
	return v == "" && !hasEmptyOption(f.def["options"])
}

func hasEmptyOption(options any) bool {
	for _, o := range optionValues(options) {
		if o == "" {
			return true
		}
	}
	return false
}

// applyConditions evaluates each input's hidden, ignore and optional, drops
// the values of ignored inputs, and reports required inputs with no value.
func (r *inputResolver) applyConditions() {
	for _, f := range r.fields {
		if f.group != nil && f.group.ignored {
			f.ignored = true
			r.changed()
			continue
		}
		f.hidden = r.condition(f, "hidden") || f.group != nil && f.group.hidden
		if f.ignored = r.condition(f, "ignore"); f.ignored {
			// What expressions read of f changes, before its value is
			// dropped.
			r.changed()
		}
		f.optional = r.condition(f, "optional")
		if f.ignored {
			r.drop(f)
		}
	}
	for _, f := range r.fields {
		// A repeated step with no value has no rows.
		if f.container || f.header || f.ignored || f.hasDefault || f.deferred || f.repeats() || !r.missing(f) {
			continue
		}
		switch {
		case f.optional || f.hidden:
		case f.typ == "boolean":
			r.set(f.path, false)
		default:
			r.reportAt(problems.InputRequired, f.loc, problems.Params{"input": f.label()})
		}
	}
}

// condition evaluates one of f's hidden, ignore and optional, which is true
// if it is true or the text "true".
func (r *inputResolver) condition(f *field, key string) bool {
	raw, ok := f.def[key]
	if !ok {
		return false
	}
	v := raw
	if _, isString := raw.(string); isString {
		var err error
		var marked bool
		if v, marked, err = r.evalAt(appendPath(f.loc, key), f.index); err != nil {
			if u, ok := errors.AsType[*expr.UnknownError](err); ok {
				r.reportAt(problems.InputConditionNotKnown, appendPath(f.loc, key),
					problems.Params{"key": key, "input": f.label(), "name": u.Name})
			} else {
				r.reportError(appendPath(f.loc, key), 0, maskedIf(marked, err))
			}
			return false
		}
	}
	switch v := v.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true")
	}
	return false
}

// drop removes the values of an ignored input, and of the inputs it holds.
func (r *inputResolver) drop(f *field) {
	if !f.container || !f.flatten {
		r.remove(f.path)
		return
	}
	for _, c := range f.children {
		r.drop(c)
	}
}

// evalAt evaluates what is at loc in the workflow, where it is, for the list
// rows at index, with the values so far: the expressions in a string, and in
// each string of a list or mapping. marked is set if it read a variable,
// or a value that did.
func (r *inputResolver) evalAt(loc []string, index []int) (v any, marked bool, err error) {
	key := fmt.Sprint(index)
	ev, ok := r.evaluators[key]
	if !ok {
		if r.contexts == nil {
			r.contexts = compileTimeContexts(r.opts, r.evalValues())
		}
		scope := r.budget
		scope.Contexts = r.contexts
		scope.Index = index
		scope.Marked = r.marks
		ev = r.workflow.Evaluator(scope)
		if r.evaluators == nil {
			r.evaluators = map[string]*expr.Evaluator{}
		}
		r.evaluators[key] = ev
	}
	return ev.EvalMarked(loc)
}

// set sets the value at path, and remove removes it.
func (r *inputResolver) set(path []string, v any) {
	setIn(r.values, path, v)
	r.changed()
}

func (r *inputResolver) remove(path []string) {
	deletePath(r.values, path)
	r.changed()
}

// changed notes that what expressions read of the values changed: the
// values, or what is known of them, or which read a variable. The
// evaluations that follow read them anew.
func (r *inputResolver) changed() {
	r.evaluators, r.contexts = nil, nil
}

// deferred returns the defaults that are evaluated once the run is
// underway, by their inputs' paths.
func (r *inputResolver) deferred() map[string]plan.Value {
	var out map[string]plan.Value
	for _, f := range r.fields {
		if (f.deferred || f.variable) && !f.ignored {
			if out == nil {
				out = map[string]plan.Value{}
			}
			out[strings.Join(f.path, ".")] = f.dflt
		}
	}
	return out
}

// evalValues returns the values for expressions to read: each input with no
// value null, or false for a boolean, which is the value it gets, rather than
// its definition, and so is each field of a row; and what is evaluated once
// the run is underway unknown.
func (r *inputResolver) evalValues() any {
	values := jsonvalue.CloneMap(r.values)
	for _, fields := range [][]*field{r.fields, r.rows} {
		for _, f := range fields {
			if f.container || f.header || f.deferred && !f.ignored || f.in != nil && f.in.deferred {
				continue
			}
			if _, ok := jsonptr.Lookup(values, f.path); ok {
				continue
			}
			var v any
			if f.typ == "boolean" && !f.hasDefault {
				v = false
			}
			setIn(values, f.path, v)
		}
	}
	for _, f := range r.fields {
		switch {
		case f.ignored:
		case f.deferred:
			setIn(values, f.path, expr.Unknown{Name: "inputs"})
		default:
			for _, p := range f.later {
				if !p.variable {
					setIn(values, slices.Concat(f.path, p.path), expr.Unknown{Name: "inputs"})
				}
			}
		}
	}
	return values
}

// kept returns the values a plan keeps: all but those of the inputs
// evaluated as the run starts, which it holds as what is evaluated then.
func (r *inputResolver) kept() map[string]any {
	out, copied := r.values, false
	for _, f := range r.fields {
		if f.deferred || f.variable {
			if !copied {
				out, copied = jsonvalue.CloneMap(r.values), true
			}
			deletePath(out, f.path)
		}
	}
	return out
}

// compileTimeContexts returns the contexts known when the run is created:
// inputs, and the caller's org, vars, remote and sessions. org, vars and
// remote default to empty, which reads every name as "".
func compileTimeContexts(opts Options, inputs any) map[string]any {
	contexts := map[string]any{"org": map[string]any{}, "vars": map[string]any{}, "remote": map[string]any{}}
	maps.Copy(contexts, opts.Contexts)
	contexts["inputs"] = inputs
	return contexts
}

// checkValues checks each value against its input's type, converts numbers
// and booleans given as text, resolves platform types, and moves password
// values to the secrets: a password input's, and a password field's in a
// list's or a repeated step's rows, which is checked as an input's is,
// though the rows' other fields aren't. A value that isn't JSON, which a
// plan can't hold, such as a default that gives NaN, is reported and
// dropped, as a submitted one is. The value of an input that reads a
// variable or a password is checked too, and kept for what the compiler
// evaluates, but what is wrong with it is reported without it, as is what
// is wrong with a password's. A password whose value is evaluated as the
// run starts isn't among the secrets, but the run's secrets name it.
func (r *inputResolver) checkValues() {
	for _, fields := range [][]*field{r.fields, r.rows} {
		for _, f := range fields {
			if f.header || f.container || f.holder().ignored {
				continue
			}
			if f.secretRows {
				r.secretsLater[strings.Join(f.path, ".")] = true
			}
			if f.in != nil && f.typ != "password" {
				continue // A row's other fields aren't checked.
			}
			later, known := f.deferred || f.variable, f.variable
			if f.in != nil {
				later, known = f.evaluatedLater()
			}
			if later && f.typ == "password" {
				r.secretsLater[strings.Join(f.path, ".")] = true
			}
			if !later || known {
				r.checkField(f, later)
			}
		}
	}
}

// checkField checks f's value, and moves it to the secrets if f is a
// password, unless later is set: the value is evaluated again as the run
// starts. A value given for a password is never kept, even one of the
// wrong type.
func (r *inputResolver) checkField(f *field, later bool) {
	v, ok := jsonptr.Lookup(r.values, f.path)
	if !ok || v == nil {
		return
	}
	password := holdsPassword(f.def)
	out, bad := r.checkValue(f, v)
	at := f
	if bad.kind == nil {
		at, bad = r.checkJSONValue(f, out)
	}
	switch {
	case bad.kind != nil:
		bad.params["input"] = at.label()
		if f.variable || password {
			bad.params = mask(bad.params)
		}
		r.reportAt(bad.kind, at.loc, bad.params)
		if !password && bad.kind != problems.InputNotJSON {
			return
		}
	case f.typ != "password":
		r.set(f.path, out)
		return
	case !later:
		r.secrets[strings.Join(f.path, ".")] = out
	}
	r.remove(f.path)
}

// checkValue checks v, the value of f, and returns it as the run sees it, or
// what is wrong with it: a finding whose params the caller adds the input
// to.
func (r *inputResolver) checkValue(f *field, v any) (any, finding) {
	wrong := func(k *problems.Kind, params problems.Params) (any, finding) {
		if params == nil {
			params = problems.Params{}
		}
		return nil, finding{k, params}
	}
	switch {
	case platformTypes[f.typ]:
		if r.opts.Inputs == nil {
			return wrong(problems.PlatformInput, problems.Params{"type": f.typ})
		}
		out, err := r.opts.Inputs.ResolveInput(f.typ, f.def, v)
		switch {
		case errors.Is(err, ErrInputType):
			return wrong(problems.PlatformInput, problems.Params{"type": f.typ})
		case err != nil:
			return wrong(problems.InputRejected, problems.Params{"reason": err.Error()})
		}
		if what, bad, _ := notJSON(out); what != "" {
			return wrong(problems.PlatformValueNotJSON, problems.Params{"what": what, "value": bad})
		}
		return out, finding{}
	case f.typ == "number":
		n, ok := toNumber(v)
		if !ok {
			return wrong(problems.InputNotNumber, problems.Params{"value": problems.Show(v)})
		}
		if lo, ok := toNumber(f.def["min"]); ok && n < lo {
			return wrong(problems.InputBelowMin, problems.Params{"value": formatNumber(n), "min": formatNumber(lo)})
		}
		if hi, ok := toNumber(f.def["max"]); ok && n > hi {
			return wrong(problems.InputAboveMax, problems.Params{"value": formatNumber(n), "max": formatNumber(hi)})
		}
		return number(n), finding{}
	case f.typ == "boolean":
		switch v := v.(type) {
		case bool:
			return v, finding{}
		case string:
			switch strings.ToLower(strings.TrimSpace(v)) {
			case "true":
				return true, finding{}
			case "false":
				return false, finding{}
			}
		}
		return wrong(problems.InputNotBoolean, problems.Params{"value": problems.Show(v)})
	case f.typ == "list" || f.repeats():
		if _, ok := v.([]any); !ok {
			return wrong(problems.InputNotList, problems.Params{"value": problems.Show(v)})
		}
		return v, finding{}
	case f.typ == "multi-dropdown":
		list, ok := v.([]any)
		if !ok {
			return wrong(problems.InputNotOptionList, problems.Params{"value": problems.Show(v)})
		}
		for _, item := range list {
			if bad := checkOption(f, item); bad.kind != nil {
				return nil, bad
			}
		}
		return v, finding{}
	case f.typ == "dropdown":
		if bad := checkOption(f, v); bad.kind != nil {
			return nil, bad
		}
		return v, finding{}
	}
	// Text: string, password, editor, color-picker and the like.
	switch v := v.(type) {
	case string:
		return v, finding{}
	case bool:
		return strconv.FormatBool(v), finding{}
	case map[string]any, []any:
		return wrong(problems.InputNotText, problems.Params{"value": problems.Show(v)})
	}
	if n, ok := toNumber(v); ok {
		return formatNumber(n), finding{}
	}
	return v, finding{}
}

// checkJSONValue checks that v, the value of f as the run sees it, is JSON:
// an expression can give NaN or an infinity, as 0/0 and 1/0 do, which a
// run's values can't hold. It returns what is wrong, and the field it is
// at: the innermost field of f's rows that holds the part that isn't JSON,
// or else f.
func (r *inputResolver) checkJSONValue(f *field, v any) (*field, finding) {
	what, value, at := notJSON(v)
	if what == "" {
		return f, finding{}
	}
	in, path := f, slices.Concat(f.path, at)
	for _, row := range r.rows {
		if len(row.path) > len(in.path) && len(row.path) <= len(path) && slices.Equal(row.path, path[:len(row.path)]) {
			in = row
		}
	}
	return in, finding{problems.InputNotJSON, problems.Params{"what": what, "value": value}}
}

// checkOption checks that v is one of a dropdown's options, when they are
// written in the workflow rather than computed.
func checkOption(f *field, v any) finding {
	options := optionValues(f.def["options"])
	if len(options) == 0 || hasTemplate(f.def["options"]) {
		return finding{}
	}
	for _, o := range options {
		if looseEqual(o, v) {
			return finding{}
		}
	}
	shown := make([]string, len(options))
	for i, o := range options {
		shown[i] = problems.Show(o)
	}
	return finding{problems.InputNotOption, problems.Params{"value": problems.Show(v), "options": problems.List(shown)}}
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
