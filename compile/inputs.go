package compile

import (
	"cmp"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/hint"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/internal/reach"
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
	// its name. It returns ErrInputType for a type it doesn't provide. An
	// error that is, or wraps, a *problem.Problem of one of the problems
	// package's kinds of error, foundation's shared rules among them, but
	// not the command's, is reported as that kind, with its params and the
	// input's name as input; any other error, a problem of a warning's kind
	// or of the command's among them, is input_rejected, with the error's
	// text as its reason, so that an error never becomes a warning that
	// lets the value through.
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
	// Values are the input values, with defaults applied, and ignored inputs
	// and passwords left out.
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
	// list's first row; and of the other submitted values that are kept as
	// passwords are, as they can hold one: one for an input the workflow
	// doesn't define, named as a password field, and one in the place of a
	// default that reads a password or a secret variable that isn't what the
	// default gives. The plan doesn't hold them.
	Secrets map[string]any
	// Private are the values, by their paths, that the plan doesn't hold,
	// though they aren't secrets: each in the place of a default that reads
	// an organization or user variable that isn't a secret, and no
	// password, that isn't what the default gives. They are kept as a
	// variable's value is, and aren't masked.
	Private map[string]any
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
	return &Inputs{Values: r.kept(), Deferred: r.deferred(), Secrets: r.secrets, Private: r.private}, listed(r.diags)
}

// resolveInputs resolves the inputs, keeping their definitions for the
// compiler's checks of what reads them.
func resolveInputs(doc *format.Document, submitted map[string]any, opts Options) *inputResolver {
	r := newInputResolver(doc, submitted, opts)
	r.resolve()
	return r
}

func newInputResolver(doc *format.Document, submitted map[string]any, opts Options) *inputResolver {
	return &inputResolver{
		doc:          doc,
		opts:         opts,
		workflow:     &expr.Workflow{Root: doc.Value, Definitions: definitionsPath},
		budget:       expr.Scope{}.Budgeted(),
		values:       jsonvalue.CloneMap(submitted),
		secrets:      map[string]any{},
		private:      map[string]any{},
		secretsLater: map[string]bool{},
		marks:        expr.NewMarks([]string{"org"}, []string{"vars"}),
	}
}

func (r *inputResolver) resolve() {
	r.fill()
	r.applyConditions()
	r.checkValues()
	r.keepHeld()
	r.diags.Sort()
}

// fill finds the inputs' fields, checks the syntax of their definitions
// and the submitted values' names, and fills in the values.
func (r *inputResolver) fill() {
	defs, _ := lookup(r.doc.Value, definitionsPath).(map[string]any)
	r.fields = r.collect(defs, definitionsPath, nil, nil)
	for i, f := range r.fields {
		f.seq = i
	}
	r.checkSyntax(append(make([]string, 0, 16), definitionsPath...), defs)
	// The format drops the keys it doesn't define, but every expression in
	// the input definitions must parse, theirs too.
	for path, v := range r.doc.Dropped() {
		if len(path) > len(definitionsPath) && slices.Equal(path[:len(definitionsPath)], definitionsPath) {
			r.checkSyntax(path, v)
		}
	}
	for _, f := range r.fields {
		r.markPassword(f)
	}
	r.checkJSON(r.values)
	r.checkUnknownInputs()
	for _, f := range r.fields {
		if f.header {
			r.remove(f.path)
		}
	}
	r.fillDefaults()
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

// A text is a string in a value, at path in it, that is the text of the
// expression at loc in the workflow: the one in the same place in the
// input's default, or one of its options' values. A form sends that text
// for an expression it can't evaluate itself.
type text struct{ path, loc []string }

// A candidate is a part of a submitted value, at path in it, that can be
// what one of the strings at locs in the workflow gave, one of which reads
// a variable or a password: the string in the same place in the input's
// default, or with a list's default rows, at the same key in any of them,
// as a form evaluates them once and the person can delete and add rows; or
// the input's options' values, for option set. With each set, each item
// of the list at path can be.
type candidate struct {
	path         []string
	locs         [][]string
	option, each bool
}

// A heldPart is a part of the values, at path, in the value of the input
// in, that the plan doesn't hold (holdPath), and how it is kept.
type heldPart struct {
	in   *field
	path []string
	kind holding
}

// A holding is how a value the plan doesn't hold is kept.
type holding int

const (
	// heldSecret is a value kept as a secret, as a password's is: one
	// that can hold a password where the compiler can't find it.
	heldSecret holding = iota
	// heldDerived is a value kept as a secret in the place of what is made
	// from a password or a secret variable, that isn't what makes it: it
	// can be made from one, or not, and is masked as what is made from a
	// secret is.
	heldDerived
	// heldPrivate is a value in the place of what is made from an
	// organization or user variable that isn't a secret, and no password,
	// that isn't what makes it: it is private, as such a variable's value
	// is, and isn't masked.
	heldPrivate
)

// An evaluation is a part of a field's value, at path in it, that the
// compiler evaluated from raw, the workflow's value, or that a form
// evaluated from it, which a default, a form's text or a comparison gave.
// done is set once it is evaluated as the run starts instead (holdWhole).
type evaluation struct {
	f    *field
	path []string
	raw  any
	done bool
}

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
	// expressions shares, and the jobs' needs: what decides the run's shape
	// is one group, for Meridian's limits on what a group does. Each
	// template has the language's limits as well.
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
	// and the parts of the values that read them; and markedInputs their
	// paths in the inputs' values.
	marks        expr.Marks
	markedInputs reach.Trie[struct{}]
	// passwords are the password inputs, and the password fields of lists'
	// rows; unknownSecrets the paths of values submitted for inputs the
	// workflow doesn't define that are taken for passwords; and held the
	// other parts of the values kept out of the plan (holdPath), which are
	// among the secrets or private (keepHeld). derived names those among
	// the secrets that can be made from a secret, and private holds the
	// values of the private ones, by their paths.
	passwords      []*field
	unknownSecrets [][]string
	held           []heldPart
	derived        map[string]bool
	private        map[string]any
	// taint holds the paths in the values, as patterns with * for a row's
	// index, whose values can read a variable or a password, and
	// secretTaint those whose values can read a password or a secret
	// variable (taintInputs). taintNodes holds the strings in the workflow
	// that taintInputs looked at, by their locations, and taintOrder them
	// in the order it found them.
	taint, secretTaint patterns
	taintNodes         map[string]*taintNode
	taintOrder         []*taintNode
	// compareScope holds the budget that comparing what a form sent with
	// what made it spends (compare): a group of its own, so that what it
	// spends doesn't decide the run's shape. comparisons holds what each
	// string compared gave, by its location and the row index it read.
	compareScope *expr.Scope
	comparisons  map[string]comparison
	// compareSlow is set once a comparison runs out of the group's time,
	// which is reported once.
	compareSlow bool
	// evaluated holds the parts of the values that evaluations gave, by the
	// patterns (reach.Patterns) of the paths of the values they read.
	evaluated reach.Trie[*evaluation]
	fields    []*field
	// rows are the fields of lists' rows: each field of a list's template,
	// once for each row.
	rows []*field
	// templates holds the fields of each list's template, and each repeated
	// step's inputs, collected once with their paths in a row, by where they
	// are in the workflow.
	templates map[string]*template
	// byPath holds the inputs' fields by their paths in the values, once
	// the view changes, to find those whose values a change reaches
	// (fieldsAt). Rows are found from their lists.
	byPath *reach.Trie[*field]
	// view is what expressions read of the values (evalValues): made when
	// they are first read, and then changed where the values, or what is
	// known of them, change (refresh). contexts are what evaluations read,
	// the view among them.
	view     map[string]any
	contexts map[string]any
	// evaluators evaluate expressions with the values as they are, one for
	// the rows at each index. They are made again once the values change.
	evaluators map[string]*expr.Evaluator
	// unknown counts the submitted values for inputs the workflow doesn't
	// define, and passwordKeys holds the names of the password fields the
	// workflow defines, anywhere, once one is.
	unknown      int
	passwordKeys map[string]bool
	// checkView, which tests set, is called each time the view changes, and
	// each time an evaluation is to read it.
	checkView func()
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
	for _, p := range in.partsAround(at) {
		if len(p.path) <= len(at) {
			later, known = true, known && p.variable
		}
	}
	return later, later && known
}

// addLater adds p to the parts of f's value that are evaluated once the run
// is underway.
func (f *field) addLater(p laterPart) {
	if f.laterAt == nil {
		f.laterAt = &reach.Trie[int]{}
	}
	f.laterAt.Add(p.path, len(f.later))
	f.later = append(f.later, p)
}

// partsAround returns the parts of f's value evaluated once the run is
// underway that are at at, a path in the value, on the way to it, or under
// it, in the order they were added.
func (f *field) partsAround(at []string) iter.Seq2[int, laterPart] {
	return func(yield func(int, laterPart) bool) {
		if len(at) == 0 {
			for i, p := range f.later {
				if !yield(i, p) {
					return
				}
			}
			return
		}
		if f.laterAt == nil {
			return
		}
		// Reached reads a * in at as any key, so it can find more than
		// these, which related leaves out.
		found := slices.Sorted(f.laterAt.Reached(at))
		for _, i := range slices.Compact(found) {
			if p := f.later[i]; related(p.path, at) && !yield(i, p) {
				return
			}
		}
	}
}

// related reports whether one of two paths leads to the other, or they are
// the same.
func related(a, b []string) bool {
	n := min(len(a), len(b))
	return slices.Equal(a[:n], b[:n])
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
		r.passwords = append(r.passwords, f)
		r.mark(f.path)
	}
}

// mark marks the value at path in the inputs' values: what reads it reads a
// variable or a password. The evaluations that follow read the marks anew.
func (r *inputResolver) mark(path []string) {
	r.marks.Add(slices.Concat([]string{"inputs"}, path))
	r.markedInputs.Add(path, struct{}{})
	r.evaluators = nil
}

// marked reports whether the value at path in the inputs' values is
// marked, holds a marked value, or is in one: as the value of another input
// with the same path, as a wizard step's input can have, can be.
func (r *inputResolver) marked(path []string) bool {
	for range r.markedInputs.Reached(path) {
		return true
	}
	return false
}

// taintInputs finds the paths in the values whose values can read a
// variable or a password, and those whose values can read a password or a
// secret variable, from the input definitions alone, before any is
// evaluated: password fields', values submitted for inputs the workflow
// doesn't define that are taken for passwords, and those of the strings in
// defaults, and of options, that can read one (reads), until there are no
// more. A row's index is * in them.
//
// Each string is looked at once, with the strings its relative references
// reach (taintNode). A string can read one if its expressions read a path
// that can, or a string it reaches can; and then so can the places in the
// values that hold what it gives (propagateTaint). A string is looked at
// again only when a path is added that its expressions can read, so the
// order in which defaults read each other doesn't make it looked at again
// and again.
func (r *inputResolver) taintInputs() {
	r.taintNodes = map[string]*taintNode{}
	var walk func(fields []*field, prefix []string)
	walk = func(fields []*field, prefix []string) {
		for _, f := range fields {
			path := slices.Concat(prefix, f.path)
			if f.typ == "password" {
				r.taint.add(path)
				r.secretTaint.add(path)
			}
			rows := f.typ == "list"
			exprStrings(f.def["default"], appendPath(f.loc, "default"), nil, func(loc, at []string) {
				if rows && len(at) > 0 {
					at = slices.Concat([]string{"*"}, at[1:])
				}
				r.taintPlace(slices.Concat(path, at), loc)
			})
			for _, o := range f.findOptions().exprs {
				r.taintPlace(path, o.loc)
			}
			if defs, loc := f.rowInputs(); defs != nil {
				walk(r.template(defs, loc).fields, appendPath(path, "*"))
			}
		}
	}
	walk(r.fields, nil)
	for _, path := range r.unknownSecrets {
		r.taint.add(path)
		r.secretTaint.add(path)
	}
	// Each string's relative references, as it is found, which can find
	// more.
	for i := 0; i < len(r.taintOrder); i++ {
		r.linkTaintNode(r.taintOrder[i])
	}
	r.propagateTaint(&r.taint, anyRead)
	r.propagateTaint(&r.secretTaint, secretRead)
}

// A taintNode is a string in the workflow that holds an expression, as
// taintInputs sees it: what its expressions read, the strings their
// relative references reach, and what it can read, as reads says.
type taintNode struct {
	loc  []string
	refs []expr.Ref
	// rels are the strings its relative references reach, and readers the
	// strings whose relative references reach it.
	rels, readers []*taintNode
	// places are the places in the values of what it gives.
	places []*taintPlace
	// reads is whether it can read a variable or a password, for any of
	// the list rows it is evaluated for, by what is read (a readKind).
	reads [2]bool
}

// A taintPlace is a place in the values, as a pattern with * for a row's
// index, that holds what one or more strings give; tainted is whether
// they can read a variable or a password, by what is read.
type taintPlace struct {
	path    []string
	tainted [2]bool
}

// A readKind is what reads looks for: a variable or a password, or a
// password or a secret variable.
type readKind int

const (
	anyRead readKind = iota
	secretRead
)

// taintNode returns the node of the string at loc, a new one if there
// isn't one yet.
func (r *inputResolver) taintNode(loc []string) *taintNode {
	key := jsonptr.Format(loc)
	if n := r.taintNodes[key]; n != nil {
		return n
	}
	n := &taintNode{loc: loc}
	r.taintNodes[key] = n
	r.taintOrder = append(r.taintOrder, n)
	return n
}

// taintPlace notes that path, a place in the values, holds what the
// strings at loc give.
func (r *inputResolver) taintPlace(path, loc []string) {
	p := &taintPlace{path: path}
	exprStrings(lookup(r.doc.Value, loc), loc, nil, func(at, _ []string) {
		n := r.taintNode(at)
		n.places = append(n.places, p)
	})
}

// linkTaintNode notes what n's expressions read, and the strings their
// relative references reach (relativeLocs), which are evaluated in turn.
func (r *inputResolver) linkTaintNode(n *taintNode) {
	exprs, err := r.workflow.Exprs(n.loc)
	if err != nil {
		return // checkSyntax reports it.
	}
	for _, e := range exprs {
		n.refs = append(n.refs, e.Refs()...)
		for _, rel := range e.Relatives() {
			for _, loc := range r.relativeLocs(n.loc, rel) {
				exprStrings(lookup(r.doc.Value, loc), loc, nil, func(at, _ []string) {
					m := r.taintNode(at)
					n.rels = append(n.rels, m)
					m.readers = append(m.readers, n)
				})
			}
		}
	}
}

// propagateTaint finds, as reads says for kind, the strings that can read
// a variable or a password, for any row, and the places in the values,
// added to taint, that hold what they give, until there are no more. A
// string is looked at again only when a place is added whose input its
// expressions read.
func (r *inputResolver) propagateTaint(taint *patterns, kind readKind) {
	secret := kind == secretRead
	// byInput holds the strings that read an input, by its name, and
	// whole those that read a path that can be any input's.
	byInput := map[string][]*taintNode{}
	var whole []*taintNode
	for _, n := range r.taintOrder {
		for _, ref := range n.refs {
			if ref.Context != "inputs" {
				continue
			}
			for p := range reach.Patterns(anyRow(ref.Path)) {
				if len(p) == 0 || p[0] == "*" {
					whole = append(whole, n)
				} else {
					byInput[p[0]] = append(byInput[p[0]], n)
				}
			}
		}
	}
	var queue []*taintNode
	reads := func(n *taintNode) {
		if !n.reads[kind] {
			n.reads[kind] = true
			queue = append(queue, n)
		}
	}
	check := func(n *taintNode) {
		if !n.reads[kind] && r.readsRefs(n.refs, secret) {
			reads(n)
		}
	}
	// recheck checks nodes again, and returns those that still can't read
	// one.
	recheck := func(nodes []*taintNode) []*taintNode {
		for _, n := range nodes {
			check(n)
		}
		return slices.DeleteFunc(nodes, func(n *taintNode) bool { return n.reads[kind] })
	}
	for _, n := range r.taintOrder {
		check(n)
	}
	for len(queue) > 0 {
		n := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		for _, m := range n.readers {
			reads(m)
		}
		for _, p := range n.places {
			if !p.tainted[kind] {
				p.tainted[kind] = true
				taint.add(p.path)
				byInput[p.path[0]] = recheck(byInput[p.path[0]])
				whole = recheck(whole)
			}
		}
	}
}

// anyRow returns path, a path an expression reads, with * in place of
// each [index]: the path it reads for any row.
func anyRow(path []string) []string {
	if !slices.Contains(path, "[index]") {
		return path
	}
	path = slices.Clone(path)
	for i, k := range path {
		if k == "[index]" {
			path[i] = "*"
		}
	}
	return path
}

// exprStrings calls yield with the location in the workflow, and the path
// in v, of each string in v, at loc, that holds an expression.
func exprStrings(v any, loc, at []string, yield func(loc, at []string)) {
	switch v := v.(type) {
	case string:
		if strings.Contains(v, "${{") {
			yield(loc, at)
		}
	case []any:
		for i, item := range v {
			k := strconv.Itoa(i)
			exprStrings(item, appendPath(loc, k), appendPath(at, k), yield)
		}
	case map[string]any:
		for k, item := range jsonvalue.Sorted(v) {
			exprStrings(item, appendPath(loc, k), appendPath(at, k), yield)
		}
	}
}

// reads reports whether what the workflow holds at loc, evaluated for any
// list row, can read a variable or a password, or with secret set, a
// password or a secret variable: the expressions in each of its strings,
// and in the definitions their relative references reach (relativeLocs),
// which are evaluated in turn. An expression can read one if it reads org
// or vars, or a secret variable, or a path of the inputs' values whose
// value can (taintInputs). Those paths have * for each row's index, so
// what can read one for a row can for each.
//
// loc is one of the places taintInputs found, whose strings it decided.
func (r *inputResolver) reads(loc []string, secret bool) bool {
	kind := anyRead
	if secret {
		kind = secretRead
	}
	found := false
	exprStrings(lookup(r.doc.Value, loc), loc, nil, func(at, _ []string) {
		n := r.taintNodes[jsonptr.Format(at)]
		// One taintInputs didn't find, which it can't decide, can.
		found = found || n == nil || n.reads[kind]
	})
	return found
}

// readsRefs reports whether refs, for any list row, can read a variable or
// a password, or with secret set, a password or a secret variable.
func (r *inputResolver) readsRefs(refs []expr.Ref, secret bool) bool {
	taint := &r.taint
	if secret {
		taint = &r.secretTaint
	}
	for _, ref := range refs {
		switch ref.Context {
		case "org", "vars":
			if !secret || r.secretVariable(ref) {
				return true
			}
		case "inputs":
			for p := range reach.Patterns(anyRow(ref.Path)) {
				if taint.overlaps(p) {
					return true
				}
			}
		}
	}
	return false
}

// secretVariable reports whether ref, a reference to org or vars, can read
// a secret: a variable whose value the caller's contexts don't give, one
// it names, or any of the context's, for one that names none.
func (r *inputResolver) secretVariable(ref expr.Ref) bool {
	vars, ok := r.opts.Contexts[ref.Context].(map[string]any)
	if !ok {
		return r.opts.Contexts[ref.Context] != nil
	}
	secret := func(v any) bool {
		_, ok := v.(expr.Unknown)
		return ok
	}
	if unnamed(ref.Path) {
		for _, v := range vars {
			if secret(v) {
				return true
			}
		}
		return false
	}
	return secret(vars[ref.Path[0]])
}

// relativeLocs returns the locations in the workflow of what rel, a
// relative reference in the string at loc, reaches: where it leads, and a
// string on the way, which is evaluated and read from. A * or an [index]
// in its path reaches each key there, and a slice of a list each item in
// it, all of which can be read.
func (r *inputResolver) relativeLocs(loc []string, rel expr.RelativeRef) [][]string {
	if rel.Up > len(loc) {
		return nil
	}
	at := [][]string{slices.Clip(loc[:len(loc)-rel.Up])}
	var out [][]string
	for _, k := range rel.Path {
		var next [][]string
		each := k == "*" || k == "[index]"
		for _, l := range at {
			switch v := lookup(r.doc.Value, l).(type) {
			case string:
				out = append(out, l)
			case map[string]any:
				if !each {
					if _, ok := v[k]; ok {
						next = append(next, appendPath(l, k))
					}
					continue
				}
				for key := range jsonvalue.Sorted(v) {
					next = append(next, appendPath(l, key))
				}
			case []any:
				if !each {
					// A key reads a list as property does: a slice, all of
					// each of whose items it reads, as what comes after it
					// reads the slice; or an index.
					if start, end, ok := reach.Slice(k, len(v)); ok {
						for i := start; i < end; i++ {
							out = append(out, appendPath(l, strconv.Itoa(i)))
						}
					} else if i, ok := reach.Index(k); ok && i < len(v) {
						next = append(next, appendPath(l, strconv.Itoa(i)))
					}
					continue
				}
				for i := range v {
					next = append(next, appendPath(l, strconv.Itoa(i)))
				}
			}
		}
		at = next
	}
	return append(out, at...)
}

// patterns holds patterns of paths, with * for any key, to find those a
// pattern overlaps without comparing it with each. The zero patterns hold
// none.
type patterns struct {
	end      bool
	children map[string]*patterns
}

// add adds p.
func (t *patterns) add(p []string) {
	for _, k := range p {
		c := t.children[k]
		if c == nil {
			if t.children == nil {
				t.children = map[string]*patterns{}
			}
			c = &patterns{}
			t.children[k] = c
		}
		t = c
	}
	t.end = true
}

// overlaps reports whether p, or one of the patterns t holds, leads to the
// other, or they are the same, a * in either being any key.
func (t *patterns) overlaps(p []string) bool {
	if t.end {
		return true
	}
	if len(p) == 0 {
		return len(t.children) > 0
	}
	if p[0] == "*" {
		for _, c := range t.children {
			if c.overlaps(p[1:]) {
				return true
			}
		}
		return false
	}
	if c := t.children[p[0]]; c != nil && c.overlaps(p[1:]) {
		return true
	}
	c := t.children["*"]
	return c != nil && c.overlaps(p[1:])
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

// checkUnknownInputs reports the submitted values for inputs the workflow
// doesn't define, and for the inputs of a list's or a repeated step's
// rows that its rows don't have: the first maxListed one by one, which
// suggest the name each probably meant, and then how many more there are.
// The values are kept.
func (r *inputResolver) checkUnknownInputs() {
	r.checkUnknown(r.values, nil, nil, knownInputsOf(r.fields), false)
	if more := r.unknown - maxListed; more > 0 {
		r.reportAt(problems.ProblemsNotListed, definitionsPath, problems.Params{
			"code": string(problems.UnknownInput.Code()), "count": more, "limit": maxListed,
		})
	}
}

// knownInputsOf returns fields, inputs, by the paths of the mappings
// their values are in.
func knownInputsOf(fields []*field) map[string]*knownInputs {
	known := map[string]*knownInputs{}
	for _, f := range fields {
		if !f.container || !f.flatten {
			at := jsonptr.Format(f.path[:len(f.path)-1])
			k := known[at]
			if k == nil {
				k = &knownInputs{byKey: map[string]*field{}}
				known[at] = k
			}
			k.byKey[f.key] = f
			if defs, _ := f.rowInputs(); defs != nil {
				if k.rows == nil {
					k.rows = map[string][]*field{}
				}
				k.rows[f.key] = append(k.rows[f.key], f)
			}
		}
	}
	return known
}

// knownInputs are the inputs whose values are in a mapping, by their keys,
// and their names, sorted, and those of the password inputs and of the
// inputs with a password among theirs, once a value there isn't one of
// them. rows holds, by their keys, the lists and repeated steps there, more
// than one where inputs share a path, as a wizard step's can, and
// rowInputs the inputs of their rows, by key, once a row is checked.
type knownInputs struct {
	byKey                     map[string]*field
	rows                      map[string][]*field
	rowInputs                 map[string]map[string]*knownInputs
	names, passwords, holders []string
}

// rowsOf returns the inputs of the rows of the lists and repeated steps at
// key, those of each one's rows, by the paths in a row of the mappings
// their values are in.
func (k *knownInputs) rowsOf(r *inputResolver, key string) map[string]*knownInputs {
	if known, ok := k.rowInputs[key]; ok {
		return known
	}
	var fields []*field
	for _, f := range k.rows[key] {
		defs, loc := f.rowInputs()
		fields = append(fields, r.template(defs, loc).fields...)
	}
	known := knownInputsOf(fields)
	if k.rowInputs == nil {
		k.rowInputs = map[string]map[string]*knownInputs{}
	}
	k.rowInputs[key] = known
	return known
}

// holdsOnly reports whether values holds only values of k's inputs that
// hold no others' values for checkUnknown to check, as most rows do: no
// group's or step's mapping, and no list's or repeated step's rows.
func (k *knownInputs) holdsOnly(values map[string]any) bool {
	if k == nil {
		return len(values) == 0
	}
	for key, v := range values {
		f, ok := k.byKey[key]
		if !ok || v != nil && (f.container && !f.flatten || len(k.rows[key]) > 0) {
			return false
		}
	}
	return true
}

// checkUnknown reports submitted values for inputs the workflow doesn't
// define, in values, which are at path in the values and, in a row of a
// list or a repeated step, at rel in the row, or else at path; known holds
// the inputs there, the workflow's or a row's, by the paths of the
// mappings they are in. A value that names a password (namesPassword) is
// taken for one: it is kept among the secrets, and not in the plan. A
// group's value that isn't a mapping is an error, but in a row, where it
// is taken as it was given, and kept as a secret if it can hold a
// password (collectRows).
func (r *inputResolver) checkUnknown(values map[string]any, path, rel []string, known map[string]*knownInputs, row bool) {
	inputs := known[jsonptr.Format(rel)]
	if inputs.holdsOnly(values) {
		return
	}
	if inputs == nil {
		inputs = &knownInputs{}
	}
	for key, v := range jsonvalue.Sorted(values) {
		f, ok := inputs.byKey[key]
		if !ok {
			at := appendPath(path, key)
			if r.namesPassword(key, v, inputs) {
				r.secrets[strings.Join(at, ".")] = v
				r.unknownSecrets = append(r.unknownSecrets, at)
				r.mark(at)
			}
			if r.unknown++; r.unknown > maxListed {
				continue
			}
			if inputs.names == nil {
				inputs.names = slices.Sorted(maps.Keys(inputs.byKey))
			}
			s, ok := hint.Suggest(key, inputs.names)
			params := problems.Params{"input": strings.Join(at, ".")}
			r.reportAt(problems.UnknownInput, definitionsPath, problems.WithSuggestion(params, strings.Join(appendPath(path, s), "."), ok))
			continue
		}
		if rows, ok := v.([]any); ok && len(inputs.rows[key]) > 0 {
			rowInputs := inputs.rowsOf(r, key)
			for i, item := range rows {
				if m, ok := item.(map[string]any); ok && !rowInputs[""].holdsOnly(m) {
					r.checkUnknown(m, appendPath(path, key, strconv.Itoa(i)), nil, rowInputs, true)
				}
			}
		}
		if !f.container || f.flatten || v == nil {
			continue
		}
		child, ok := v.(map[string]any)
		switch {
		case ok:
			r.checkUnknown(child, appendPath(path, key), appendPath(rel, key), known, row)
		case !row:
			params := problems.Params{"input": f.label(), "value": problems.Show(v)}
			if holdsPassword(f.def) {
				params = mask(params)
			}
			r.reportAt(problems.InputGroupNotMapping, f.loc, params)
			delete(values, key)
		}
	}
}

// namesPassword reports whether v, a value submitted for an input the
// workflow doesn't define, named key, among the inputs, is a password's: if
// key is a password field's name, wherever the workflow defines one, as a
// repeated step's password sent beside the step is; or can be a secret's
// (namesSecret), as one sent for a password field that was renamed can;
// or is close to the name of a password input among them, as a
// misspelling is; or v is a mapping or a list, and key is close to the
// name of one with a password among its inputs, or v holds a key that is a
// password field's name or can be a secret's, as a flattened wizard step's
// inputs sent under its name do.
func (r *inputResolver) namesPassword(key string, v any, inputs *knownInputs) bool {
	if r.passwordKeys == nil {
		r.passwordKeys = map[string]bool{}
		defs, _ := lookup(r.doc.Value, definitionsPath).(map[string]any)
		passwordKeys(defs, r.passwordKeys)
	}
	if r.passwordKeys[key] || namesSecret(key) || r.holdsPasswordKey(v) {
		return true
	}
	if inputs.passwords == nil {
		inputs.passwords, inputs.holders = []string{}, []string{}
		for name, f := range jsonvalue.Sorted(inputs.byKey) {
			switch {
			case f.typ == "password":
				inputs.passwords = append(inputs.passwords, name)
			case holdsPassword(f.def):
				inputs.holders = append(inputs.holders, name)
			}
		}
	}
	if _, ok := hint.Suggest(key, inputs.passwords); ok {
		return true
	}
	switch v.(type) {
	case map[string]any, []any:
		_, ok := hint.Suggest(key, inputs.holders)
		return ok
	}
	return false
}

// holdsPasswordKey reports whether v holds, at any depth, a key that is a
// password field's name, wherever the workflow defines one, or that can be
// a secret's (namesSecret).
func (r *inputResolver) holdsPasswordKey(v any) bool {
	switch v := v.(type) {
	case map[string]any:
		for k, item := range v {
			if r.passwordKeys[k] || namesSecret(k) || r.holdsPasswordKey(item) {
				return true
			}
		}
	case []any:
		return slices.ContainsFunc(v, r.holdsPasswordKey)
	}
	return false
}

// passwordKeys adds to out the names of the password inputs that defs,
// input definitions, define, and those among their inputs.
func passwordKeys(defs map[string]any, out map[string]bool) {
	for name, v := range defs {
		def, ok := v.(map[string]any)
		if !ok || strings.HasPrefix(name, "$") {
			continue
		}
		if def["type"] == "password" {
			out[name] = true
		}
		for _, key := range []string{"items", "options", "template"} {
			if inner, ok := def[key].(map[string]any); ok {
				passwordKeys(inner, out)
			}
		}
	}
}

// fillDefaults fills in the values. Each input without a value gets its
// default, and so does each field of a list's rows, from the list's
// template, once the list's rows are known. A string a form sent as the
// text of an expression gets the expression's value. Expressions are
// evaluated with the other inputs' values, defaults included, in the order
// they need. A submitted value that a form can have made from a variable or
// a password is read as one of them from the start, and once the values
// are filled in, is evaluated as the run starts if it is what made it, and
// is kept as a secret if not (compare).
func (r *inputResolver) fillDefaults() {
	r.taintInputs()
	var pending pendingFields
	var compared []*field
	var start func(fields []*field)
	start = func(fields []*field) {
		for _, f := range fields {
			waits := r.prepare(f)
			if waits {
				pending.add(f)
			}
			if f.candidates != nil {
				compared = append(compared, f)
				for _, c := range f.candidates {
					for _, path := range r.parts(f, c) {
						r.mark(slices.Concat(f.path, path))
					}
				}
			}
			if !waits {
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
			// Its rows aren't known, and so neither are the passwords among
			// them: all of its value is kept as a secret.
			if f.typ != "password" && holdsPassword(f.def) {
				r.holdPath(f, f.path, heldSecret)
			}
		}
		r.reportAt(problems.DefaultsReadEachOther, left[0].loc, problems.Params{"inputs": problems.QuoteAll(names)})
	}
	for _, f := range compared {
		r.compare(f)
	}
	r.holdWhole()
	r.heldParts()
	r.holdLater()
}

// prepare fills in what it can of f's value without evaluating anything: a
// default that has no expressions. It reports whether f waits for its
// default's expressions, or its texts', to be evaluated; and finds the
// candidates of a submitted value, which are compared once the values are
// filled in.
func (r *inputResolver) prepare(f *field) bool {
	if f.container || f.header {
		return false
	}
	if !r.missing(f) {
		r.findTexts(f)
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
		r.findTexts(f)
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
			r.noteEvaluation(f, nil, loc, f.def["default"])
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
			// A copy, as the value can be part of what the evaluation read.
			r.set(slices.Concat(f.path, t.path), jsonvalue.Clone(v))
			r.noteEvaluation(f, t.path, t.loc, text)
			if marked {
				r.deferPart(f, t.path, text, true)
			}
		}
	}
}

// noteEvaluation notes that the part of f's value at path is what the
// expressions in raw, at loc in the workflow, gave, by the paths of the
// values they read: if one of them is evaluated as the run starts, so is
// the part (holdWhole).
func (r *inputResolver) noteEvaluation(f *field, path, loc []string, raw any) {
	exprs, err := r.workflow.Exprs(loc)
	if err != nil {
		return
	}
	e := &evaluation{f: f, path: path, raw: raw}
	for _, p := range readPatterns(exprs, f.index) {
		r.evaluated.Add(p, e)
	}
}

// compare decides what each of f's candidates is: a part of f's submitted
// value that a form can have made from a variable or a password. A form
// evaluates each default and option it can and sends what it gives, but it
// can send what it gave before the person changed what it read, or gave a
// row they have since moved, and evaluates some expressions otherwise than
// Meridian does. So a part that is what one of its expressions gives now
// means that expression, as a form's text for it does: it is evaluated as
// the run starts, as the expression, if the expression can read a variable
// or a password, and is data if not. A choice of an option written in the
// workflow is that option. Any other part, what a form gave before, or what
// the person typed in its place, the plan doesn't hold, but the run has
// (holdPath): it is a secret, masked as what is made from one is, if one of
// its expressions can read a password or a secret variable, and private,
// as a variable's value is, if not. A part already evaluated as the run
// starts, or in one, is left as it is. The expressions are evaluated in a
// group of their own (evalCompare). Comparing changes no value, so what an
// expression gives is the same for each candidate it is compared with, at
// each row index it reads.
func (r *inputResolver) compare(f *field) {
	value := lookup(r.values, f.path)
	for _, c := range f.candidates {
		var gave []comparison
		for _, path := range r.parts(f, c) {
			if f.isText(path) {
				continue
			}
			if gave == nil {
				gave = make([]comparison, len(c.locs))
				for i, loc := range c.locs {
					gave[i] = r.evalCompare(loc, f.index)
				}
			}
			part := lookup(value, path)
			i := slices.IndexFunc(gave, func(res comparison) bool { return res.ok && sameValue(part, res.v) })
			switch {
			case i >= 0:
				if loc := c.locs[i]; r.reads(loc, false) {
					raw := lookup(r.doc.Value, loc)
					r.noteEvaluation(f, path, loc, raw)
					r.deferPart(f, path, raw, true)
				}
			case c.option && f.findOptions().literal(part):
			case f.typ == "password" && len(path) == 0:
				// A password is a secret, whatever made it.
			case slices.ContainsFunc(c.locs, func(loc []string) bool { return r.reads(loc, true) }):
				r.holdPath(f.holder(), slices.Concat(f.path, path), heldDerived)
			default:
				r.holdPath(f.holder(), slices.Concat(f.path, path), heldPrivate)
			}
		}
	}
}

// A comparison is what a string in the workflow gave, if it gave one,
// when compare evaluated it.
type comparison struct {
	v  any
	ok bool
}

// evalCompare evaluates what is at loc in the workflow, as evalAt does, for
// the list rows at index, in the group that comparisons share, once for
// each row index that what it reads depends on. The group is one of its
// own (compareScope), so that comparing spends nothing of what decides the
// run's shape. A comparison its budget stops gives nothing, as one that
// fails does, and so what is compared is held, the same each time for the
// same workflow and values; but running out of the group's time, which
// depends on how fast the machine is, is an error.
func (r *inputResolver) evalCompare(loc []string, index []int) comparison {
	exprs, err := r.workflow.Exprs(loc)
	if err != nil {
		return comparison{}
	}
	key := jsonptr.Format(loc)
	if slices.ContainsFunc(exprs, readsIndex) {
		key += "|" + indexKey(index)
	}
	if res, ok := r.comparisons[key]; ok {
		return res
	}
	if r.compareScope == nil {
		s := expr.Scope{}.Budgeted()
		r.compareScope = &s
	}
	base := r.scope()
	scope := *r.compareScope
	scope.Contexts, scope.Marked, scope.Index = base.Contexts, base.Marked, index
	v, err := r.workflow.Evaluator(scope).Eval(loc)
	if be, ok := errors.AsType[*expr.BudgetError](err); ok && be.Kind == problems.ExpressionsTooSlow && !r.compareSlow {
		r.compareSlow = true
		r.reportError(loc, 0, err)
	}
	// A copy, as the value can be part of what the evaluation read.
	res := comparison{jsonvalue.Clone(v), err == nil}
	if r.comparisons == nil {
		r.comparisons = map[string]comparison{}
	}
	r.comparisons[key] = res
	return res
}

// readsIndex reports whether what e gives can depend on the row index it
// is evaluated for: whether it reads [index], or another definition.
func readsIndex(e *expr.Expr) bool {
	return e.Relative() || slices.ContainsFunc(e.Refs(), func(ref expr.Ref) bool {
		return slices.Contains(ref.Path, "[index]")
	})
}

// holdPath keeps the value at path in the inputs' values, in the value of
// the input in, out of the plan, as kind says, and what reads it reads a
// variable or a password. Once every part evaluated as the run starts is
// known, a value with such parts in it is held but for them (heldParts).
// The plan holds null in place of a list's item, and the run's start puts
// the value in its place.
func (r *inputResolver) holdPath(in *field, path []string, kind holding) {
	r.mark(path)
	r.held = append(r.held, heldPart{in, path, kind})
}

// heldParts makes the values held the parts of them that aren't evaluated
// as the run starts: a value in a part that is is left to it, and of a
// value with such parts in it, each of its keys or items that isn't one is
// held instead.
func (r *inputResolver) heldParts() {
	var out []heldPart
	var split func(p heldPart, at []string)
	split = func(p heldPart, at []string) {
		rel := at[len(p.in.path):]
		in := false
		for _, part := range p.in.partsAround(rel) {
			if len(part.path) <= len(rel) {
				return
			}
			in = true
		}
		if !in {
			out = append(out, heldPart{p.in, at, p.kind})
			return
		}
		switch v := lookup(r.values, at).(type) {
		case map[string]any:
			for k := range jsonvalue.Sorted(v) {
				split(p, appendPath(at, k))
			}
		case []any:
			for i := range v {
				split(p, appendPath(at, strconv.Itoa(i)))
			}
		}
	}
	for _, p := range r.held {
		split(p, p.path)
	}
	r.held = out
}

// keepHeld puts the values kept out of the plan (holdPath) among the
// secrets, or among the private values, once they are checked: those of
// the inputs that have them. derived names the secrets that are what can be
// made from one.
func (r *inputResolver) keepHeld() {
	for _, p := range r.held {
		v, ok := jsonptr.Lookup(r.values, p.path)
		if !ok || v == nil {
			continue
		}
		key := strings.Join(p.path, ".")
		switch p.kind {
		case heldPrivate:
			r.private[key] = v
		case heldDerived:
			if r.derived == nil {
				r.derived = map[string]bool{}
			}
			r.derived[key] = true
			fallthrough
		default:
			r.secrets[key] = v
		}
	}
}

// parts returns the paths in f's value of the parts that c, one of its
// candidates, is: its own, or with each set, each item there that isn't a
// form's text.
func (r *inputResolver) parts(f *field, c candidate) [][]string {
	if !c.each {
		return [][]string{c.path}
	}
	var out [][]string
	list, _ := lookup(r.values, slices.Concat(f.path, c.path)).([]any)
	for i := range list {
		if path := appendPath(c.path, strconv.Itoa(i)); !f.isText(path) {
			out = append(out, path)
		}
	}
	return out
}

// isText reports whether the part of f's value at path is a form's text
// for an expression, or is evaluated as the run starts already, or is in
// such a part.
func (f *field) isText(path []string) bool {
	for _, t := range f.texts {
		if related(t.path, path) && len(t.path) <= len(path) {
			return true
		}
	}
	return f.startsLater(slices.Concat(f.path, path), true)
}

// startsLater reports whether the value at path, f's or a part of it, is
// in a part of its input's value that is evaluated as the run starts, or is
// one: any such part, or with known unset, one the compiler doesn't know
// now, whose value is the text of what is evaluated then.
func (f *field) startsLater(path []string, known bool) bool {
	in := f.holder()
	at := path[len(in.path):]
	for _, p := range in.partsAround(at) {
		if len(p.path) <= len(at) && (known || !p.variable) {
			return true
		}
	}
	return false
}

// sameValue reports whether a submitted value, a, is b, what an expression
// gave, as a form sends it: a list or a mapping holds the same values, and
// any other value has the same text (looseKey), as the number 8 and the
// text "8" do.
func sameValue(a, b any) bool {
	switch a := a.(type) {
	case nil:
		return b == nil
	case []any:
		l, ok := b.([]any)
		if !ok || len(a) != len(l) {
			return false
		}
		for i := range a {
			if !sameValue(a[i], l[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		m, ok := b.(map[string]any)
		if !ok || len(a) != len(m) {
			return false
		}
		for k, v := range a {
			w, ok := m[k]
			if !ok || !sameValue(v, w) {
				return false
			}
		}
		return true
	}
	switch b.(type) {
	case nil, []any, map[string]any:
		return false
	}
	// A string is its own text, which needn't be made again.
	as, aString := a.(string)
	bs, bString := b.(string)
	switch {
	case aString && bString:
		return as == bs
	case aString:
		return as == looseKey(b)
	case bString:
		return looseKey(a) == bs
	}
	return looseKey(a) == looseKey(b)
}

// setDefault sets f's value to its default, unless the default is "", which
// gives no value. The value is a copy, so that filling in a list's rows
// doesn't change the workflow, nor what the evaluation read.
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
	value := lookup(r.values, f.path)
	rows, isList := value.([]any)
	// A password among the rows' inputs is kept out of the plan wherever it
	// is in the value, so a row, or a group or list of a row on the way to
	// one, that isn't the shape the rows' inputs say it is, which can hold a
	// password where the compiler can't find it, is kept as a secret, as it
	// was given.
	secret := holdsPassword(f.def) && !f.startsLater(f.path, false)
	if secret && f.in != nil && value != nil && !isList {
		r.holdPath(f.holder(), f.path, heldSecret)
		return nil
	}
	t := r.template(defs, loc)
	var out []*field
	for i, row := range rows {
		rowPath := appendPath(f.path, strconv.Itoa(i))
		if _, ok := row.(map[string]any); !ok {
			if secret && row != nil && !f.startsLater(rowPath, false) {
				r.holdPath(f.holder(), rowPath, heldSecret)
			}
			continue
		}
		if len(t.fields) == 0 {
			continue
		}
		index := append(slices.Clip(f.index), i)
		fields := t.row(rowPath)
		for _, c := range fields {
			c.index, c.in = index, f.holder()
			c.seq = len(r.fields) + len(r.rows) + len(out)
			r.markPassword(c)
			out = append(out, c)
			if v := lookup(r.values, c.path); c.container && !c.flatten && v != nil && holdsPassword(c.def) && !c.startsLater(c.path, false) {
				if _, ok := v.(map[string]any); !ok {
					r.holdPath(c.holder(), c.path, heldSecret)
				}
			}
		}
		if f.rows == nil {
			f.rows = make([][]*field, len(rows))
		}
		f.rows[i] = fields
	}
	if len(out) > 0 {
		r.rows = append(r.rows, out...)
		r.refresh(f.path)
	}
	return out
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

// findTexts finds the strings in f's value that are a form's text for an
// expression: the one in the same place in its default, or one of its
// options' values, or with a list's default rows, the one at the same key
// in any of them. Every other string is data. It sets f's texts, and the
// expressions they hold; and its candidates, the other parts of the value
// in the place of one of those expressions, or that the options can have
// given, where one of them can read a variable or a password.
func (r *inputResolver) findTexts(f *field) {
	value := lookup(r.values, f.path)
	var texts []text
	var candidates []candidate
	// items holds the keys of the paths of one key that have a text, so
	// that each item of a list has one.
	items := map[string]bool{}
	dflt, loc := f.def["default"], appendPath(f.loc, "default")
	if rows, ok := dflt.([]any); ok && f.typ == "list" {
		texts, candidates = rowTexts(rows, value, loc)
	} else {
		textPaths, candidatePaths := defaultTexts(dflt, value, nil, nil, nil)
		for _, p := range textPaths {
			texts = append(texts, text{p, slices.Concat(loc, p)})
			if len(p) == 1 {
				items[p[0]] = true
			}
		}
		for _, p := range candidatePaths {
			candidates = append(candidates, candidate{path: p, locs: [][]string{slices.Concat(loc, p)}})
		}
	}
	list, isList := value.([]any)
	var options [][]string
	for _, o := range f.findOptions().exprs {
		options = append(options, o.loc)
		if value == o.value {
			texts = append(texts, text{nil, o.loc})
			options = nil
		}
		for i, item := range list {
			if item != o.value {
				continue
			}
			if k := strconv.Itoa(i); !items[k] {
				items[k] = true
				texts = append(texts, text{[]string{k}, o.loc})
			}
		}
	}
	if options != nil && value != nil && !slices.ContainsFunc(texts, func(t text) bool { return t.path == nil }) {
		candidates = append(candidates, candidate{locs: options, option: true, each: isList})
	}
	f.texts, f.reads = texts, nil
	for _, t := range texts {
		exprs, err := r.workflow.Exprs(t.loc)
		if err != nil {
			continue // checkSyntax reported it.
		}
		f.reads = append(f.reads, exprs...)
	}
	f.candidates = slices.DeleteFunc(candidates, func(c candidate) bool {
		return !slices.ContainsFunc(c.locs, func(loc []string) bool { return r.reads(loc, false) })
	})
}

// rowTexts finds, in value, a list's submitted rows, the strings that are
// the text of an expression in the same place in rows, the list's default
// rows, at loc in the workflow; and the candidates in the place of an
// expression at the same key in any default row, since a person can
// delete, add and move the rows a form filled in from the default.
func rowTexts(rows []any, value any, loc []string) ([]text, []candidate) {
	// byKey holds the places of the expressions in a row, by their paths in
	// it, and the default rows they are in.
	type place struct {
		at   []string
		rows []int
	}
	var places []*place
	byKey := map[string]*place{}
	for j, row := range rows {
		exprStrings(row, nil, nil, func(_, at []string) {
			key := jsonptr.Format(at)
			if byKey[key] == nil {
				byKey[key] = &place{at: at}
				places = append(places, byKey[key])
			}
			byKey[key].rows = append(byKey[key].rows, j)
		})
	}
	var texts []text
	var candidates []candidate
	list, _ := value.([]any)
	for i, item := range list {
		for _, p := range places {
			v := lookup(item, p.at)
			if v == nil {
				continue
			}
			path := slices.Concat([]string{strconv.Itoa(i)}, p.at)
			if i < len(rows) && v == lookup(rows[i], p.at) {
				texts = append(texts, text{path, slices.Concat(loc, path)})
				continue
			}
			c := candidate{path: path}
			for _, j := range p.rows {
				c.locs = append(c.locs, slices.Concat(loc, []string{strconv.Itoa(j)}, p.at))
			}
			candidates = append(candidates, c)
		}
	}
	return texts, candidates
}

// defaultTexts appends to texts the paths, from path, of the strings in v
// that are the text of an expression in the same place in def, and to
// candidates those of the other values in v in the place of a string in
// def that holds one. It appends to path in place, so that the walk reuses
// one path, and copies the paths it keeps.
func defaultTexts(def, v any, path []string, texts, candidates [][]string) ([][]string, [][]string) {
	switch def := def.(type) {
	case string:
		if !strings.Contains(def, "${{") || v == nil {
			break
		}
		if s, ok := v.(string); ok && s == def {
			texts = append(texts, slices.Clone(path))
		} else {
			candidates = append(candidates, slices.Clone(path))
		}
	case []any:
		list, _ := v.([]any)
		for i := range min(len(def), len(list)) {
			texts, candidates = defaultTexts(def[i], list[i], append(path, strconv.Itoa(i)), texts, candidates)
		}
	case map[string]any:
		m, _ := v.(map[string]any)
		for k, d := range jsonvalue.Sorted(def) {
			if item, ok := m[k]; ok {
				texts, candidates = defaultTexts(d, item, append(path, k), texts, candidates)
			}
		}
	}
	return texts, candidates
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
// its value until then, and what reads it reads a variable too. A part
// that is evaluated as the run starts already stays one, and isn't known
// now if either says it isn't.
func (r *inputResolver) deferPart(f *field, path []string, raw any, variable bool) {
	v, err := parseFor(raw, f.index)
	if err != nil {
		return // checkSyntax reported it.
	}
	in := f.holder()
	at := slices.Concat(f.path[len(in.path):], path)
	defer r.refresh(slices.Concat(f.path, path))
	for i, p := range in.partsAround(at) {
		if slices.Equal(p.path, at) {
			in.later[i].variable = p.variable && variable
			return
		}
	}
	in.addLater(laterPart{at, v, variable})
	if variable {
		r.mark(slices.Concat(f.path, path))
	}
}

// holdLater makes what the plan holds of each input with parts evaluated
// once the run is underway: its value, with those parts' expressions where
// they are, and without the values of the password fields in its rows,
// which the run's start puts in their places from the secrets. An input
// with a part the compiler doesn't know is deferred; one whose parts read
// only variables and passwords is held for the run's start too, but what
// the compiler evaluates reads its value. A part that gives a list's rows,
// or the value of a list, a group or a wizard step, with a password field
// among its inputs, is a secret as a whole.
func (r *inputResolver) holdLater() {
	if !slices.ContainsFunc(r.fields, func(f *field) bool { return f.later != nil }) {
		return
	}
	// The password fields of each input's rows, and the lists, groups and
	// wizard steps with password fields among their inputs, by their paths.
	passwords := map[*field][]*field{}
	holders := map[string]*field{}
	for _, fields := range [][]*field{r.fields, r.rows} {
		for _, f := range fields {
			if f.typ == "password" && f.in != nil {
				passwords[f.in] = append(passwords[f.in], f)
			}
			if f.typ != "password" && holdsPassword(f.def) {
				holders[jsonptr.Format(f.path)] = f
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
			// A part that is the value of a list, a group or a step with a
			// password among its inputs, or one of a list's rows, holds
			// passwords the plan can't name.
			at := slices.Concat(f.path, p.path)
			h := holders[jsonptr.Format(at)]
			if n := len(p.path); h == nil && n > 0 && isIndex(p.path[n-1]) {
				h = holders[jsonptr.Format(at[:len(at)-1])]
			}
			if h != nil {
				h.secretWhole = true
			}
			if len(p.path) > 0 {
				setIn(held, p.path, p.v)
			}
		}
		// A part that is the whole value is all of it, whatever the other
		// parts are.
		if i := slices.IndexFunc(f.later, func(p laterPart) bool { return len(p.path) == 0 }); i >= 0 {
			held = f.later[i].v
		}
		// What is kept out of the plan (holdPath) isn't held either, and
		// the run's start puts it in its place.
		for _, p := range r.held {
			if p.in == f {
				clearPath(held, p.path[len(f.path):])
			}
		}
		// Nor are values for inputs its rows or groups don't have that are
		// taken for passwords, which the run's start puts in their places.
		for _, at := range r.unknownSecrets {
			if len(at) > len(f.path) && slices.Equal(at[:len(f.path)], f.path) {
				deletePath(held, at[len(f.path):])
			}
		}
		f.dflt = plan.Literal(held)
		if slices.ContainsFunc(f.later, func(p laterPart) bool { return !p.variable }) {
			// A part of its value isn't known until the run starts, so
			// what expressions read of it is unknown, all of it, from
			// here on.
			f.deferred = true
			r.refresh(f.path)
		} else {
			f.variable = true
		}
	}
}

// holdWhole evaluates as the run starts what an evaluation made from an
// input that is evaluated as the run starts because a part of it reads what
// isn't known yet: an input's value stays whole, so what is made from any
// part of it, the parts the compiler knows too, isn't known yet either, as
// a condition that reads it isn't. The value of an input whose evaluation
// read such an input is then evaluated as the run starts as well, and so
// on. Evaluations made from the input's own parts, as a row's default that
// reads another field of its row, read the parts as they are.
func (r *inputResolver) holdWhole() {
	var held []*field
	seen := map[*field]bool{}
	for _, f := range r.fields {
		if slices.ContainsFunc(f.later, func(p laterPart) bool { return !p.variable }) {
			held = append(held, f)
			seen[f] = true
		}
	}
	for len(held) > 0 {
		h := held[0]
		held = held[1:]
		var found []*evaluation
		for e := range r.evaluated.Reaching(h.path) {
			if !e.done && e.f.holder() != h {
				e.done = true
				found = append(found, e)
			}
		}
		// The trie finds them in no particular order.
		slices.SortFunc(found, func(a, b *evaluation) int {
			return cmp.Or(cmp.Compare(a.f.seq, b.f.seq), slices.Compare(a.path, b.path))
		})
		for _, e := range found {
			r.deferPart(e.f, e.path, e.raw, false)
			if in := e.f.holder(); !seen[in] {
				seen[in] = true
				held = append(held, in)
			}
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
	return v == "" && !f.findOptions().empty
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

// applyConditions evaluates each input's hidden, ignore and optional, drops
// the values of ignored inputs, and reports required inputs with no value.
func (r *inputResolver) applyConditions() {
	for _, f := range r.fields {
		if f.group != nil && f.group.ignored {
			f.ignored = true
			r.refresh(f.path)
			continue
		}
		f.hidden = r.condition(f, "hidden") || f.group != nil && f.group.hidden
		if f.ignored = r.condition(f, "ignore"); f.ignored {
			// What expressions read of f changes, before its value is
			// dropped.
			r.refresh(f.path)
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
	key := indexKey(index)
	ev, ok := r.evaluators[key]
	if !ok {
		scope := r.scope()
		scope.Index = index
		ev = r.workflow.Evaluator(scope)
		if r.evaluators == nil {
			r.evaluators = map[string]*expr.Evaluator{}
		}
		r.evaluators[key] = ev
	}
	return ev.EvalMarked(loc)
}

// indexKey returns the key of the evaluator for the list rows at index.
func indexKey(index []int) string {
	b := make([]byte, 0, 4*len(index))
	for _, i := range index {
		b = strconv.AppendInt(b, int64(i), 10)
		b = append(b, ',')
	}
	return string(b)
}

// scope returns the scope that what the compiler evaluates with the
// inputs' values reads: the values as they are (evalValues), and the marks,
// in the inputs' group, which shares one budget.
func (r *inputResolver) scope() expr.Scope {
	if r.view == nil {
		r.view = r.evalValues()
		r.contexts = compileTimeContexts(r.opts, r.view)
	}
	if r.checkView != nil {
		r.checkView()
	}
	scope := r.budget
	scope.Contexts = r.contexts
	scope.Marked = r.marks
	return scope
}

// set sets the value at path, and remove removes it.
func (r *inputResolver) set(path []string, v any) {
	setIn(r.values, path, v)
	r.refresh(path)
}

func (r *inputResolver) remove(path []string) {
	deletePath(r.values, path)
	r.refresh(path)
}

// refresh notes that what expressions read of the values changed at path,
// or under it: the values, or what is known of them, or which read a
// variable. The evaluations that follow read them anew, and the view is
// made at path what evalValues would make it now: the values there, each
// field's value, where it has none, as evalValues fills it in, and what is
// unknown, there, on the way to path and under it. Nothing else in the view
// changes.
func (r *inputResolver) refresh(path []string) {
	r.evaluators = nil
	if r.view == nil {
		return
	}
	if v, ok := jsonptr.Lookup(r.values, path); ok {
		setIn(r.view, path, jsonvalue.Clone(v))
	} else {
		deletePath(r.view, path)
	}
	// fill holds the fields whose values are at path or under it, which
	// are filled in in the order evalValues fills them in, as of two with
	// the same path the first's value is kept; and holders the inputs on
	// the way to path, at it or under it, with parts evaluated once the run
	// is underway.
	var fill, holders []*field
	for f := range r.fieldsAt().Reached(path) {
		if !related(f.path, path) {
			continue
		}
		if f.later != nil {
			holders = append(holders, f)
		}
		fill = appendUnder(fill, f, path)
	}
	slices.SortFunc(fill, func(a, b *field) int { return cmp.Compare(a.seq, b.seq) })
	for _, f := range fill {
		if v, ok := f.filledWith(); ok {
			if _, has := jsonptr.Lookup(r.view, f.path); !has {
				setIn(r.view, f.path, v)
			}
		}
	}
	slices.SortFunc(holders, func(a, b *field) int { return cmp.Compare(a.seq, b.seq) })
	for _, f := range holders {
		f.markUnknown(r.view, path)
	}
	if r.checkView != nil {
		r.checkView()
	}
}

// fieldsAt returns the inputs' fields by their paths in the values, putting
// them there the first time.
func (r *inputResolver) fieldsAt() *reach.Trie[*field] {
	if r.byPath == nil {
		r.byPath = &reach.Trie[*field]{}
		for _, f := range r.fields {
			r.byPath.Add(f.path, f)
		}
	}
	return r.byPath
}

// appendUnder appends to out f, if its value is at path or under it, and
// the fields of its rows whose values are.
func appendUnder(out []*field, f *field, path []string) []*field {
	if len(f.path) >= len(path) {
		out = append(out, f)
		for _, row := range f.rows {
			for _, c := range row {
				out = appendUnder(out, c, path)
			}
		}
		return out
	}
	// path is in f's value: in one of its rows, if any.
	if i, ok := rowIndex(path[len(f.path)], len(f.rows)); ok {
		for _, c := range f.rows[i] {
			if related(c.path, path) {
				out = appendUnder(out, c, path)
			}
		}
	}
	return out
}

// rowIndex returns the index of the row that key, a key of a path, names
// among n rows, as a row's fields' paths write it.
func rowIndex(key string, n int) (int, bool) {
	i, err := strconv.Atoi(key)
	return i, err == nil && i >= 0 && i < n && strconv.Itoa(i) == key
}

// filledWith returns the value evalValues gives f where it has none, and
// whether it gives one: null, or false for a boolean without a default,
// which is the value it gets, rather than its definition. A container, a
// header and an input evaluated once the run is underway, and the fields of
// its rows, get none; nor do the fields of an ignored input's rows, which
// it doesn't have.
func (f *field) filledWith() (any, bool) {
	if f.container || f.header || f.deferred && !f.ignored || f.in != nil && (f.in.deferred || f.in.ignored) {
		return nil, false
	}
	if f.typ == "boolean" && !f.hasDefault {
		return false, true
	}
	return nil, true
}

// markUnknown marks in values, as evalValues does, what is evaluated of
// f's value once the run is underway, at path, on the way to it or under
// it, or everywhere if path is nil: all of it, if f is deferred, or else
// the parts that don't read only variables and passwords, whose values the
// compiler knows.
func (f *field) markUnknown(values map[string]any, path []string) {
	unknown := expr.Unknown{Name: "inputs"}
	switch {
	case f.ignored:
	case f.deferred:
		setIn(values, f.path, unknown)
	default:
		var at []string
		if len(path) > len(f.path) {
			at = path[len(f.path):]
		}
		for _, p := range f.partsAround(at) {
			if !p.variable {
				setIn(values, slices.Concat(f.path, p.path), unknown)
			}
		}
	}
}

// deferred returns the defaults that are evaluated once the run is
// underway, by their inputs' paths.
func (r *inputResolver) deferred() map[string]plan.Value {
	var out map[string]plan.Value
	for _, f := range r.fields {
		// One whose value can't be held, as it isn't JSON, has none.
		if (f.deferred || f.variable) && !f.ignored && !f.dflt.IsZero() {
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
// the run is underway unknown. The view is made with it, and kept as it
// would make it (refresh).
func (r *inputResolver) evalValues() map[string]any {
	values := jsonvalue.CloneMap(r.values)
	for _, fields := range [][]*field{r.fields, r.rows} {
		for _, f := range fields {
			v, ok := f.filledWith()
			if !ok {
				continue
			}
			if _, has := jsonptr.Lookup(values, f.path); !has {
				setIn(values, f.path, v)
			}
		}
	}
	for _, f := range r.fields {
		f.markUnknown(values, nil)
	}
	return values
}

// kept returns the values a plan keeps: all but those of the inputs
// evaluated as the run starts, which it holds as what is evaluated then,
// and the passwords', which are among the secrets.
func (r *inputResolver) kept() map[string]any {
	var left [][]string
	for _, f := range r.fields {
		if f.deferred || f.variable {
			left = append(left, f.path)
		}
	}
	for _, f := range r.passwords {
		left = append(left, f.path)
	}
	left = append(left, r.unknownSecrets...)
	for _, p := range r.held {
		left = append(left, p.path)
	}
	if len(left) == 0 {
		return r.values
	}
	out := jsonvalue.CloneMap(r.values)
	for _, path := range left {
		clearPath(out, path)
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
// and booleans given as text, resolves platform types, and puts password
// values among the secrets: a password input's, and a password field's in
// a list's or a repeated step's rows, which is checked as an input's is,
// though the rows' other fields aren't. A value that isn't JSON, which a
// plan can't hold, such as a default that gives NaN, is reported and
// dropped, as a submitted one is. The value of an input that reads a
// variable or a password is checked too, and kept for what the compiler
// evaluates, but what is wrong with it is reported without it, as is what
// is wrong with a password's. A password whose value is evaluated as the
// run starts isn't among the secrets, but the run's secrets name it. Of an
// input whose value is evaluated as the run starts because a part of it
// reads what isn't known yet, what the compiler knows is checked to be
// JSON (checkKnown).
func (r *inputResolver) checkValues() {
	// passwords holds the password fields of each input's rows, once an
	// input is checked with checkKnown.
	var passwords map[*field][]*field
	for _, fields := range [][]*field{r.fields, r.rows} {
		for _, f := range fields {
			if f.header || f.holder().ignored {
				continue
			}
			if f.secretWhole {
				r.secretsLater[strings.Join(f.path, ".")] = true
			}
			if f.container || f.in != nil && f.typ != "password" {
				continue // A row's other fields aren't checked.
			}
			later, known := f.deferred || f.variable, f.variable
			if f.in != nil {
				later, known = f.evaluatedLater()
			}
			if later && f.typ == "password" {
				r.secretsLater[strings.Join(f.path, ".")] = true
			}
			switch {
			case !later || known:
				r.checkField(f, later)
			case f.in == nil:
				if passwords == nil {
					passwords = map[*field][]*field{}
					for _, c := range r.passwords {
						if c.in != nil {
							passwords[c.in] = append(passwords[c.in], c)
						}
					}
				}
				r.checkKnown(f, passwords[f])
			}
		}
	}
}

// checkKnown checks what the compiler knows of the value of f, an input
// whose value is evaluated as the run starts because a part of it reads
// what isn't known yet: the plan holds the rest of the value as it is, so
// it must be JSON, as a value that is known must. A part that isn't is
// reported at the innermost field of the input's rows that holds it, and
// the input has no value to hold. What the parts that read a variable or a
// password gave isn't shown, and the input's passwords, the password
// fields of its rows, aren't checked here.
func (r *inputResolver) checkKnown(f *field, passwords []*field) {
	known := jsonvalue.Clone(lookup(r.values, f.path))
	for _, p := range f.later {
		if !p.variable {
			if len(p.path) == 0 {
				return
			}
			setIn(known, p.path, nil)
		}
	}
	for _, c := range passwords {
		setIn(known, c.path[len(f.path):], nil)
	}
	what, value, at := notJSON(known)
	if what == "" {
		return
	}
	params := problems.Params{"what": what, "value": value}
	variable := false
	for _, p := range f.partsAround(at) {
		variable = variable || p.variable && len(p.path) <= len(at)
	}
	if variable || holdsPassword(f.def) || r.marked(slices.Concat(f.path, at)) {
		params = mask(params)
	}
	in := r.innermostRow(f, slices.Concat(f.path, at))
	params["input"] = in.label()
	r.reportAt(problems.InputNotJSON, in.loc, params)
	f.dflt = plan.Value{}
}

// checkField checks f's value, and moves it to the secrets if f is a
// password, unless later is set: the value is evaluated again as the run
// starts. A value given for a password is never kept, even one of the
// wrong type. A password's value that is checked stays among the values,
// for what the compiler evaluates, which reads it as it reads a variable,
// but isn't among those the plan keeps (kept).
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
		if f.variable || password || r.marked(f.path) {
			bad.params = mask(bad.params)
		}
		r.reportAt(bad.kind, at.loc, bad.params)
		if !password && bad.kind != problems.InputNotJSON {
			return
		}
		if bad.kind == problems.InputNotJSON && f.in == nil {
			// Nor can the plan hold what is evaluated as the run starts.
			f.dflt = plan.Value{}
		}
		r.remove(f.path)
		return
	case f.typ == "password" && !later:
		r.secrets[strings.Join(f.path, ".")] = out
	}
	r.set(f.path, out)
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
			// A problem of one of the kinds of error a workflow can have,
			// foundation's shared rules among them, is reported as itself, so
			// that each reader sees it in their own language. Any other error,
			// a warning's kind among them, which would let the value through,
			// or the command's own, is the platform's reason, as it gives it.
			if p, ok := errors.AsType[*problem.Problem](err); ok {
				_, command := problems.CommandRegistry.Lookup(p.Code)
				if k, ok := problems.Lookup(p.Code); ok && !command && k.Severity() == problems.Error {
					return wrong(k, maps.Clone(p.Params))
				}
			}
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
	return r.innermostRow(f, slices.Concat(f.path, at)), finding{problems.InputNotJSON, problems.Params{"what": what, "value": value}}
}

// innermostRow returns the innermost field of lists' rows whose value holds
// the value at path, a path in f's value, or f if none does. Of fields with
// the same path, it returns the first.
func (r *inputResolver) innermostRow(f *field, path []string) *field {
	in := f
	var walk func(g *field)
	walk = func(g *field) {
		if len(g.path) >= len(path) {
			return
		}
		i, ok := rowIndex(path[len(g.path)], len(g.rows))
		if !ok {
			return
		}
		for _, c := range g.rows[i] {
			if len(c.path) > len(path) || !slices.Equal(c.path, path[:len(c.path)]) {
				continue
			}
			if len(c.path) > len(in.path) || in != f && len(c.path) == len(in.path) && c.seq < in.seq {
				in = c
			}
			walk(c)
		}
	}
	for g := range r.fieldsAt().Reached(path) {
		if len(g.path) <= len(path) && related(g.path, path) {
			walk(g)
		}
	}
	return in
}

// checkOption checks that v is one of a dropdown's options, when they are
// written in the workflow rather than computed: that its looseKey is one of
// theirs.
func checkOption(f *field, v any) finding {
	o := f.findOptions()
	if len(o.values) == 0 || o.template {
		return finding{}
	}
	if o.keys == nil {
		o.keys = make(map[string]bool, len(o.values))
		for _, value := range o.values {
			o.keys[looseKey(value)] = true
		}
	}
	if o.keys[looseKey(v)] {
		return finding{}
	}
	shown := make([]string, len(o.values))
	for i, value := range o.values {
		shown[i] = problems.Show(value)
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
