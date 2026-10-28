package compile

import (
	"context"
	"errors"
	"fmt"
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
	// its name. It is asked as soon as the value is known, and what reads
	// the input, the other inputs' definitions and the workflow's env: among
	// them, reads what it returns; a field of a list's rows is asked for
	// too, and so is an input that turns out to be ignored, whose error
	// fails the run all the same. A resolver that runs format 1 as ACTIVATE
	// does gives a compute-clusters or a bucket input's details, and every
	// other type's value as it is. It returns ErrInputType for a type it
	// doesn't provide. It refuses a value with an error that is, or wraps, a
	// *problem.Problem: one of the problems package's kinds of error,
	// foundation's shared rules among them, but not the command's, is
	// reported as that kind, with its params and the input's name as input,
	// which the resolver leaves out (problems.Kind.InputProblem), and any
	// other, as one of a warning's kind, is input_rejected with the
	// problem's text as its reason, as is input_rejected with a reason, for
	// a refusal that has no kind of its own. What a refusal shows is masked
	// where the value can be private. Any other error says it couldn't
	// answer, as when ctx is done or the platform can't be reached: the run
	// isn't created, or doesn't start, and Create or Start returns an error
	// that wraps ErrUnanswered, so that it can be tried again. That error
	// names the input, and has the resolver's error's text only where the
	// value can't be private; all the same, an error that says it couldn't
	// answer never quotes the value, as a URL it couldn't reach can.
	ResolveInput(ctx context.Context, typ string, definition map[string]any, value any) (any, error)
}

// ErrUnanswered is what the error Create, Start and ResolveInputs return
// wraps when the Inputs resolver couldn't answer for an input's value
// (InputResolver): the run can be created, or started, again.
var ErrUnanswered = errors.New("compile: the Inputs resolver couldn't answer")

// An unansweredError is the error of the Inputs resolver that couldn't
// answer for input's value. It has reason, the resolver's error's text,
// only where the value can't be private, as the value of a password, or
// one a form made from a variable or a password, or that reads one, can
// be: the resolver's error can quote it. It wraps ErrUnanswered and, where
// ctx was done, ctx's error, and nothing else of the resolver's error, so
// that one that happens to wrap ErrMismatch, plan.ErrMalformed or
// plan.ErrVersion isn't taken for a run that can't be started again.
type unansweredError struct {
	input, reason string
	ctx           error
}

func (e *unansweredError) Error() string {
	msg := fmt.Sprintf("%v for the input %s", ErrUnanswered, e.input)
	if e.reason != "" {
		msg += ": " + e.reason
	}
	return msg
}

func (e *unansweredError) Unwrap() []error {
	if e.ctx != nil {
		return []error{ErrUnanswered, e.ctx}
	}
	return []error{ErrUnanswered}
}

// unansweredFor returns the error of the Inputs resolver that couldn't answer
// for f's value with err (unansweredError).
func (r *inputResolver) unansweredFor(f *field, err error) error {
	e := &unansweredError{input: f.label()}
	if !r.mayBePrivate(f) {
		e.reason = err.Error()
	}
	e.ctx = contextEnd(err)
	return e
}

// contextEnd returns context.Canceled or context.DeadlineExceeded, if err
// is or wraps it, and nil if not: what an error that says nothing else of
// err, as one that can quote a private value, wraps of it, so that a
// caller can tell a context's end apart.
func contextEnd(err error) error {
	for _, done := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, done) {
			return done
		}
	}
	return nil
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

// Inputs are a run's input values. Secrets and Private name each value by
// its path in the values, as plan.JoinPath names it: keys joined with
// dots, a key's own . and ~ written ~1 and ~0, as in db~1pass for a
// submitted value's key db.pass. The plan names the same value with
// inputs. before it.
type Inputs struct {
	// Values are the input values, with defaults applied, and ignored
	// inputs, passwords and the inputs whose values read something the
	// compiler doesn't know, or a variable or a password, left out: Start
	// resolves them.
	Values map[string]any
	// Deferred are the inputs whose values Start resolves, as they read
	// what the compiler doesn't know, such as a secret variable, or a
	// variable or a password, by their input's path, as in group.name, each
	// with its default as the definitions write it.
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
	// Variables reads the organization and user variables the workflow
	// reads as Create reads them: each plain one's value, and which are
	// secrets, whose values it doesn't read. When it is set, the variables
	// are its, and not Contexts' org and vars, which are for a caller
	// without one, such as an editor or a test.
	Variables Variables
	// Groups provides the execution groups and the environments a job's
	// runs-on: names. Nil means there are none: a runs-on: that names one,
	// written out, fails to create a run (resolver_missing).
	Groups Groups
	// Sources provides the workflows a step's uses: names, which Create
	// pins, and those they call in turn, down the tree. Nil means there
	// are none: a uses: that names a workflow fails to create a run
	// (resolver_missing).
	Sources Sources
	// Calls are the limits on that tree, which Create checks.
	Calls CallLimits
	// Sessions checks the workflow's sessions: and gives the sessions
	// context. Nil means there is no session provider: a workflow with
	// sessions fails to create a run (resolver_missing), unless Contexts
	// gives the sessions context, as a caller that knows it does.
	Sessions Sessions
	// Parent is the step that calls the workflow, for a child run's
	// Create, or nil for a run no step calls. What the run is submitted is
	// the step's with:, but for its $yaml, $host and $thumbnail.
	Parent *Parent
}

// definitionsPath is where a workflow defines its inputs.
var definitionsPath = []string{"on", "execute", "inputs"}

// ResolveInputs checks a run's submitted input values against the workflow's
// input definitions, applies defaults, and drops ignored inputs, as
// docs/design.md describes. It reports every problem it finds. It returns
// an error, and nothing else, when opts' Inputs resolver couldn't answer,
// which wraps ErrUnanswered, or ctx is done, with ctx.Err().
func ResolveInputs(ctx context.Context, doc *format.Document, submitted map[string]any, opts Options) (*Inputs, problems.Diagnostics, error) {
	r := resolveInputs(ctx, doc, submitted, opts)
	if err := r.unansweredErr(); err != nil {
		return nil, nil, err
	}
	return &Inputs{Values: r.kept(), Deferred: r.deferred(), Secrets: r.secrets, Private: r.private}, listed(r.diags), nil
}

// deferred returns the inputs whose values Start resolves, each with its
// default as the definitions write it (Inputs.Deferred).
func (r *inputResolver) deferred() map[string]plan.Value {
	var out map[string]plan.Value
	for _, f := range r.fields {
		if (f.deferred || f.variable) && !f.ignored {
			if out == nil {
				out = map[string]plan.Value{}
			}
			v, _ := r.parsed(f.def["default"])
			out[plan.JoinPath(f.path...)] = plan.Literal(v)
		}
	}
	return out
}

// resolveInputs resolves the inputs, keeping their definitions for the
// compiler's checks of what reads them. With a nil ctx, as Compile has, it
// asks no Inputs resolver, which takes one: the value of an input of a
// platform's type is as it was given, and isn't checked, but where opts has
// no Inputs resolver at all (platform_input).
func resolveInputs(ctx context.Context, doc *format.Document, submitted map[string]any, opts Options) *inputResolver {
	r := newInputResolver(doc, submitted, opts)
	r.ctx = ctx
	r.resolve()
	return r
}

// unansweredErr returns the error of the Inputs resolver that couldn't
// answer, or ctx's if it is done, or nil.
func (r *inputResolver) unansweredErr() error {
	if r.ctx != nil {
		if err := r.ctx.Err(); err != nil {
			return err
		}
	}
	return r.unanswered
}

func newInputResolver(doc *format.Document, submitted map[string]any, opts Options) *inputResolver {
	if submitted != nil {
		submitted = jsonvalue.CloneMap(submitted)
		floatNumbers(submitted)
	}
	return &inputResolver{
		doc:          doc,
		opts:         opts,
		workflow:     &expr.Workflow{Root: doc.Value, Definitions: definitionsPath},
		submitted:    submitted,
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
	r.fillGroups()
	r.checkValues()
	r.keepHeld()
	r.diags.Sort()
}

// fillGroups gives each group, and each wizard step whose inputs' values
// are under its own key, that has no value, or null, an empty mapping, as
// ACTIVATE fills in a run's values: a group's value is a mapping, though
// none of its inputs has a value. One that is ignored, or in one that is,
// gets none, and so does one in a list's rows, as ACTIVATE fills in a
// row's group only when one of its inputs has a value. The expressions
// Create and Start evaluate read the inputs' view, which this doesn't
// change: what reads the run's values, the jobs, reads the mapping, as
// ACTIVATE's do.
func (r *inputResolver) fillGroups() {
	for _, f := range r.fields {
		if !f.container || f.flatten || f.repeats() || f.in != nil || f.ignored {
			continue
		}
		if v, ok := jsonptr.Lookup(r.values, f.path); ok && v != nil {
			continue
		}
		// One in a group that has no mapping, as one that is ignored, has
		// none either. The fields are in order, a group's before its own.
		if parent := f.path[:len(f.path)-1]; len(parent) > 0 {
			if _, ok := lookup(r.values, parent).(map[string]any); !ok {
				continue
			}
		}
		r.set(f.path, map[string]any{})
	}
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
	r.checkShared(defs, definitionsPath)
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
	r.holdGiven()
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
	opts Options
	// ctx is what the Inputs resolver is asked with, or nil, for Compile,
	// which asks none; unanswered is the error of one that couldn't answer,
	// once one couldn't, after which none is asked (askPlatform).
	ctx        context.Context
	unanswered error
	workflow   *expr.Workflow
	// budget is Create's scope (viewState.created) with the one budget that
	// every evaluation of the inputs' expressions shares, and the jobs'
	// needs, made with the view: what decides the run's shape is one group,
	// for Meridian's limits on what a group does. Each template has the
	// language's limits as well.
	budget expr.Scope
	// values are the inputs' values, and submitted those the run was given,
	// as they were given, but for a number, read as ACTIVATE reads it
	// (floatNumbers).
	values, submitted map[string]any
	secretState
	taintState
	compareState
	// evaluated holds the parts of the values that evaluations gave, by the
	// patterns (reach.Patterns) of the paths of the values they read.
	evaluated reach.Trie[*evaluation]
	// evaluations are the same, in order, and gave those of them up to
	// gaveTo by the paths of the parts they gave, made as a comparison
	// first needs them (readsPart).
	evaluations []*evaluation
	gave        reach.Trie[*evaluation]
	gaveTo      int
	fields      []*field
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
	// and decided what each decided, which the secret record keeps.
	decisions []string
	decided   map[string]bool
	// ignoredLater holds the inputs that an ignore which read a variable
	// or a password ignored, whose values the compiler holds out of the
	// plan whatever it decided, and Start drops (applyConditions); and
	// shapeView the inputs' values without them, as the jobs' needs: read
	// them (shapeScope).
	ignoredLater []*field
	shapeView    map[string]any
	// start is set when the resolver completes a run's values as it
	// starts (Start), and nil when it creates the run.
	start *startState
	// final is set as the run starts: the resolver resolves the inputs
	// from what was submitted with everything known, so it decides what a
	// value a form can have made from a variable or a password means,
	// applies every condition, and checks every input.
	final bool
	// means names each part of a submitted value that means the expression
	// a form made it from, one that reads a variable or a password, as the
	// plan names a value, with its context, as in inputs.region
	// (plan.JoinPath), with the expression's JSON Pointer: as the run is
	// created, what the compiler decided (compare), which the secret record
	// keeps (plan.Record.Means); as it starts, what Start reads of it, each
	// a text of the expression in the part's place (meanTexts). remeant
	// holds those whose expressions gave another value as the run started
	// than was submitted, as a variable changed, by their paths in the
	// values, as in region, with what they gave: what reads one through a
	// relative reference doesn't wait for it, and can have read what was
	// submitted (starter.resolve).
	means   map[string]string
	remeant map[string]any
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
