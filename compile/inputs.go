package compile

import (
	"errors"
	"slices"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
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

// Inputs are a run's input values. Deferred, Secrets and Private name each
// value by its path in the values, as plan.JoinPath names it: keys joined
// with dots, a key's own . and ~ written ~1 and ~0, as in db~1pass for a
// submitted value's key db.pass. The plan names the same value with
// inputs. before it.
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
	// inputs: org, vars, remote and sessions, each a mapping of strings by
	// name, as plan.NewScope gives them. The value of a secret variable is
	// an expr.Unknown, so that an expression that reads it is evaluated
	// once the run is underway, and a whole context's value can be one too.
	// A context that isn't given is empty, but for sessions, which aren't
	// known without a session provider. The plan holds no organization
	// or user variable's value, secret or not: the compiler reads them to
	// decide the run's shape, such as which inputs are ignored and which
	// jobs a job needs, and keeps what else reads them an expression.
	Contexts map[string]any
	// Limits are Meridian's limits on the run's size that the phases after
	// Create apply, such as a matrix's instances, which the plan records
	// (plan.Run.Limits). A zero field is the default.
	Limits plan.Limits
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

// An inputResolver resolves a run's input values (ResolveInputs). What it
// keeps for each part of the work is in a struct of its own, declared with
// that part's methods: secretState (secrets.go), taintState (taint.go),
// compareState (compare.go) and viewState (view.go).
type inputResolver struct {
	reporter
	opts     Options
	workflow *expr.Workflow
	// budget is Create's scope (viewState.created) with the one budget that
	// every evaluation of the inputs' expressions shares, and the jobs'
	// needs, made with the view: what decides the run's shape is one group,
	// for Meridian's limits on what a group does. Each template has the
	// language's limits as well.
	budget expr.Scope
	values map[string]any
	secretState
	taintState
	compareState
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
	viewState
	// unknown counts the submitted values for inputs the workflow doesn't
	// define, and passwordKeys holds the names of the password fields the
	// workflow defines, anywhere, once one is.
	unknown      int
	passwordKeys map[string]bool
	// decisions are the JSON Pointers of the conditions that read a
	// variable or a password, in order, which the plan names for Start,
	// and decided what each decided, which the secret record keeps;
	// ignored the names of the inputs the conditions ignored, as in
	// inputs.region, and ignoredValues their values, as they were given,
	// for those that had one; and unconverted
	// the values of the inputs whose values checking them converted, as
	// they were before, by their paths' names, which what was evaluated
	// before read.
	decisions     []string
	decided       map[string]bool
	ignored       []string
	ignoredValues map[string]any
	unconverted   map[string]any
	// start is set when the resolver completes a run's values as it
	// starts (Start), and nil when it creates the run.
	start *startState
}

// privateExprs returns the expressions of the definitions of the inputs
// that hold a private value: one the run was given in the place of what
// reads a variable that isn't a secret.
func (r *inputResolver) privateExprs() []*expr.Expr {
	var out []*expr.Expr
	seen := map[*field]bool{}
	for name := range jsonvalue.Sorted(r.private) {
		path := plan.SplitPath(name)
		for n := len(path); n > 0; n-- {
			if f := r.fieldAt(path[:n]); f != nil {
				if !seen[f] {
					seen[f] = true
					exprs, _ := r.workflow.Exprs(f.loc)
					out = append(out, exprs...)
				}
				break
			}
		}
	}
	return out
}

// withheldParts returns where, in the workflow, the parts of the input
// definitions are that can hold a password, which a plan doesn't hold:
// the default of each input that holds a password, a password input's, a
// password field's of a list's rows, and a list's or a repeated step's
// whose rows have a password input, each once, in order.
func (r *inputResolver) withheldParts() [][]string {
	var out [][]string
	seen := map[string]bool{}
	var walk func(fields []*field)
	walk = func(fields []*field) {
		for _, f := range fields {
			if _, ok := f.def["default"]; ok && holdsPassword(f.def) {
				loc := appendPath(f.loc, "default")
				if pointer := jsonptr.Format(loc); !seen[pointer] {
					seen[pointer] = true
					out = append(out, loc)
				}
			}
			if defs, loc := f.rowInputs(); defs != nil {
				walk(r.collect(defs, loc, appendPath(f.path, "[index]"), nil))
			}
		}
	}
	walk(r.fields)
	return out
}

// deferred returns the defaults that are evaluated once the run is
// underway, by their inputs' paths.
func (r *inputResolver) deferred() map[string]plan.Value {
	var out map[string]plan.Value
	for _, f := range r.deferredFields() {
		if out == nil {
			out = map[string]plan.Value{}
		}
		out[plan.JoinPath(f.path...)] = f.dflt
	}
	return out
}

// deferredFields returns the inputs whose values are evaluated once the run
// is underway, in order. One whose value can't be held, as it isn't JSON,
// has none.
func (r *inputResolver) deferredFields() []*field {
	var out []*field
	for _, f := range r.fields {
		if (f.deferred || f.variable) && !f.ignored && !f.dflt.IsZero() {
			out = append(out, f)
		}
	}
	return out
}

// unconvertedData returns the values of the inputs the plan keeps, kept,
// that checking them converted, as they were given (plan.Run.Unconverted):
// none the plan doesn't keep, as a private one.
func (r *inputResolver) unconvertedData(kept map[string]any) map[string]plan.Data {
	var out map[string]plan.Data
	for name, v := range r.unconverted {
		if _, ok := jsonptr.Lookup(kept, plan.SplitPath(name)); !ok {
			continue
		}
		if out == nil {
			out = map[string]plan.Data{}
		}
		out[name] = plan.DataOf(v)
	}
	return out
}

// deferredParts returns where the parts of the deferred values that hold
// expressions are in the workflow, in order: each input's, in the order
// they were held, or the one that is all of its value.
func (r *inputResolver) deferredParts() []plan.DeferredPart {
	var out []plan.DeferredPart
	for _, f := range r.deferredFields() {
		parts := f.later
		if i := slices.IndexFunc(parts, func(p laterPart) bool { return len(p.path) == 0 }); i >= 0 {
			parts = parts[i : i+1]
		}
		for _, p := range parts {
			out = append(out, plan.DeferredPart{
				Name:     plan.JoinPath(slices.Concat([]string{"inputs"}, f.path, p.path)...),
				Location: jsonptr.Format(p.loc),
				Index:    slices.Clone(p.index),
				Default:  p.dflt,
			})
		}
	}
	return out
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
