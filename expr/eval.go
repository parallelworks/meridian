package expr

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/problems"
)

// maxBuilt is the language's limit on what one template builds: a ${{ }}
// alone, or text with ${{ }} in it. Each operation that makes a string, list
// or mapping counts the size of what it makes, as docs/expressions.md says
// under Limits, so that an expression can't make the compiler or coordinator
// use unbounded memory. A template doesn't count what another template that
// it reads builds, such as an input's default: that one has its own count.
const maxBuilt = 8 << 20

// maxGroupBuilt is Meridian's own limit on what the templates of a group,
// which share a budget (Scope.Budgeted), build together, such as all of a
// run's input defaults and conditions. It isn't part of the format: it is a
// safety net that bounds what a group builds however many templates it has.
// It is never narrower than the language's limit, so a template on its own
// that keeps to the language's limits never reaches it.
const maxGroupBuilt = 64 << 20

// maxWork is the language's limit on the work one template does, so that
// evaluating it takes bounded time. Work is counted in bytes, as builds are:
// 8 for each step, such as an operand, an operator, a call, a segment of a
// path or a run of a lambda; the size of each value that an operation reads,
// such as both sides of ==; and what the template builds. docs/expressions.md
// says how each operation counts, under Limits. A template doesn't count the
// work of a template it reads: that one has its own count.
const maxWork = 32 << 20

// step is what each step of an evaluation counts toward its work: the size
// of a scalar.
const step = 8

// maxGroupWork is Meridian's own limit on the work that the templates of a
// group do together: eight templates at the language's limit. Like
// maxGroupBuilt, it isn't part of the format.
const maxGroupWork = 8 * maxWork

// A group's limits are at least one template's, and a template can build
// all that it may within its work: this doesn't compile if they aren't.
const (
	_ uint = maxGroupBuilt - maxBuilt
	_ uint = maxGroupWork - maxWork
	_ uint = maxWork - maxBuilt
)

// maxGroupTime is Meridian's own limit on how long the templates of a group
// spend evaluating, together. It is a backstop that work within the limits
// never reaches: the work a template does is counted by what its steps and
// reads cost, so a group at its limit takes a few seconds. It isn't part of
// the format, since how long evaluating takes depends on the machine.
const maxGroupTime = 10 * time.Second

// clockEvery is how many steps a group's templates take between reads of
// the clock, so that most steps don't read it: each node they evaluate, and
// each run of a lambda's body.
const clockEvery = 256

// A Scope is what an expression can read.
type Scope struct {
	// Contexts maps context names (inputs, needs, steps, matrix, env, job,
	// org, vars, remote, sessions) to their values. An expression that reads a
	// context the scope doesn't have can't be evaluated yet: Eval returns
	// an *UnknownError. The values of env, org, vars, remote and sessions
	// are strings, and so are outputs in needs and steps.
	Contexts map[string]any
	// Status is how the job or step got here, for the status functions. Nil
	// means it isn't known yet.
	Status *Status
	// Resolver reads the workflow around the expression, for relative
	// references and input definitions. Nil means there is none.
	Resolver Resolver
	// Index holds the indexes of the list items the expression is for,
	// outermost first: the first [index] in a reference is Index[0], the
	// second Index[1].
	Index []int

	// budget counts what the templates that share it have done together,
	// for Meridian's limits on a group. Nil means a template is a group of
	// its own.
	budget *budget
}

// A budget counts what the templates of a group have built, the work they
// have done, and how long they have spent evaluating. The group's
// evaluations happen one at a time, though one can be nested in another, as
// a template that reads another is.
type budget struct {
	built, work int
	// steps counts the steps the group's templates have taken, so that
	// the clock is read every clockEvery of them.
	steps int
	// elapsed is how long the group's templates have spent evaluating, up
	// to last, when the clock was last read.
	elapsed time.Duration
	last    time.Time
	// active counts the evaluations under way, so that the time between
	// two of the group's evaluations doesn't count.
	active int
	// clock reads the time: time.Now, or a test's clock.
	clock func() time.Time
}

// Budgeted returns scope with one budget for what templates do, which every
// evaluation with the returned scope, or a copy of it, shares: the
// templates are a group, such as all of a run's input defaults and
// conditions, or every template in one value. Each template still has the
// language's limits, and the group has Meridian's wider ones, which aren't
// part of the format: its templates build at most 64 MiB together, do at
// most 256 MiB of work, and spend at most 10 seconds evaluating.
// A scope that has a budget keeps it.
func (s Scope) Budgeted() Scope {
	if s.budget == nil {
		s.budget = &budget{clock: time.Now}
	}
	return s
}

// start notes that one of the group's evaluations starts, and the time
// counts from then if no other is under way.
func (b *budget) start() {
	if b.active == 0 {
		b.last = b.clock()
	}
	b.active++
}

// stop notes that an evaluation ends, and counts the time since the clock
// was last read. Its time is counted even if it passes the group's limit:
// what the evaluation did is done, and the next check fails.
func (b *budget) stop() {
	b.tick()
	b.active--
}

// tick counts the time since the clock was last read, and reports whether
// the group has spent longer evaluating than Meridian allows.
func (b *budget) tick() bool {
	now := b.clock()
	b.elapsed += now.Sub(b.last)
	b.last = now
	return b.elapsed > maxGroupTime
}

// Status is how a job or step got to where an expression is evaluated: for
// a step, how the earlier steps of its job ended, and for a job, how the
// jobs it needs ended.
type Status struct {
	Failed    bool
	Cancelled bool
}

// A Resolver reads the workflow around an expression. Each method follows
// a path through the values of the workflow's expressions as walk does,
// with charge, which counts the work of what it reads; an error from
// charge stops it and is returned.
type Resolver interface {
	// Relative returns the value of the relative reference that goes up
	// levels from the key the expression is in and then follows path, with
	// any expressions in it evaluated. ok is false if there is nothing
	// there.
	Relative(up int, path []string, charge func(n int) error) (v any, ok bool, err error)
	// Definition returns what the input definitions hold at path, with any
	// expressions in it evaluated, or else the default of the input at
	// path. ok is false if there is neither.
	Definition(path []string, charge func(n int) error) (v any, ok bool, err error)
}

// Unknown is a value in a context that exists but isn't known yet, such as a
// secret's when the compiler evaluates an expression. An expression that
// reads it, or reads something that holds it, returns an *UnknownError with
// its Name.
type Unknown struct {
	Name string
}

// An EvalError is an expression that can't be evaluated with the values it
// was given, such as fromJSON of text that isn't JSON.
type EvalError struct {
	// Offset is the byte offset of the problem: in the expression, in the
	// template evaluated, or in the string at Location.
	Offset int
	// Location is the JSON Pointer of the string in a workflow the problem
	// is in, when it is in another expression than the one evaluated: one
	// that it reads, as a relative reference or an input's definition
	// does. It is "" for the expression itself.
	Location string
	// Kind is the kind of problem, and Params the values its message shows.
	Kind   *problems.Kind
	Params problems.Params
}

func (e *EvalError) Error() string { return e.Kind.Message(e.Params) }

// An UnknownError means an expression reads something its scope doesn't
// have yet, such as needs before the jobs it needs have finished. It is
// evaluated later, when the scope has it.
type UnknownError struct {
	// Offset is the byte offset in the expression of what is unknown.
	Offset int
	// Name is the context, or "status", "index" or "workflow".
	Name string
}

func (e *UnknownError) Error() string { return fmt.Sprintf("%s isn't known yet", e.Name) }

// A BudgetError means the templates of a group, which share a budget
// (Scope.Budgeted), built more together, did more work, or spent longer
// evaluating than Meridian allows a group. It isn't a problem with any one
// expression, which can each be within the language's limits: the budget is
// Meridian's own, not part of the format.
type BudgetError struct {
	// Kind is the limit the group passed: problems.ExpressionsBuildTooMuch,
	// problems.ExpressionsTooMuchWork or problems.ExpressionsTooSlow.
	Kind *problems.Kind
	// Limit is the group's limit: in MiB for what it builds and the work it
	// does, and in seconds for its time.
	Limit int
}

func (e *BudgetError) Error() string {
	return e.Kind.Message(problems.Params{"limit": e.Limit})
}

// Eval evaluates the expression in scope.
func (e *Expr) Eval(scope Scope) (any, error) {
	ev := newEvaluator(scope)
	defer ev.stop()
	return ev.eval(e.root)
}

// An evaluator evaluates one template: a ${{ }} alone, or the expressions
// in text.
type evaluator struct {
	scope Scope
	// spent counts what the template has built, and worked the work it has
	// done, for the language's limits.
	spent, worked int
	// frame holds the parameters of the lambda whose body is being
	// evaluated, if any, and those of the lambdas around it.
	frame *frame
	// unknowns holds what unknown finds in each list and mapping.
	unknowns map[identity]found
}

// A frame holds the values of a lambda's parameters while its body is
// evaluated for an item, and the frame of the lambda around it, if any.
type frame struct {
	lambda *lambda
	// values are the item, and its index in a list or its key in a
	// mapping.
	values [2]any
	outer  *frame
}

// lookup returns the value of the parameter name of the innermost lambda
// that has one, which hides any of the same name around it.
func (f *frame) lookup(name string) any {
	for ; f != nil; f = f.outer {
		if i := slices.Index(f.lambda.params, name); i >= 0 {
			return f.values[i]
		}
	}
	return nil
}

// newEvaluator returns an evaluator for a template in scope, whose time
// counts toward its group's until stop.
func newEvaluator(scope Scope) *evaluator {
	e := &evaluator{scope: scope.Budgeted()}
	e.scope.budget.start()
	return e
}

// stop ends the evaluation, and counts its time.
func (e *evaluator) stop() { e.scope.budget.stop() }

// stepped counts a step toward the group's reads of the clock, and reads it
// every clockEvery steps.
func (e *evaluator) stepped() error {
	if e.scope.budget.steps++; e.scope.budget.steps%clockEvery == 0 {
		return e.timed()
	}
	return nil
}

// timed reads the clock, and fails if the group has spent longer evaluating
// than Meridian allows.
func (e *evaluator) timed() error {
	if e.scope.budget.tick() {
		return &BudgetError{Kind: problems.ExpressionsTooSlow, Limit: int(maxGroupTime / time.Second)}
	}
	return nil
}

func (e *evaluator) errorAt(at int, k *problems.Kind, params problems.Params) error {
	return &EvalError{Offset: at, Kind: k, Params: params}
}

// spend counts n bytes built, which count as work too, and fails if they
// take the template past the language's limits, or its group past
// Meridian's. A count that fails isn't kept, since what it counts isn't
// built.
func (e *evaluator) spend(at, n int) error {
	if e.spent+n > maxBuilt {
		return e.errorAt(at, problems.ExpressionTooLarge, problems.Params{"limit": maxBuilt >> 20})
	}
	if e.scope.budget.built+n > maxGroupBuilt {
		return &BudgetError{Kind: problems.ExpressionsBuildTooMuch, Limit: maxGroupBuilt >> 20}
	}
	if err := e.work(at, n); err != nil {
		return err
	}
	e.spent += n
	e.scope.budget.built += n
	return nil
}

// work counts n bytes of work, and fails if they take the template past the
// language's limit, or its group past Meridian's. A count that fails isn't
// kept, since the work it counts isn't done.
func (e *evaluator) work(at, n int) error {
	if e.worked+n > maxWork {
		return e.errorAt(at, problems.ExpressionTooMuchWork, problems.Params{"limit": maxWork >> 20})
	}
	if e.scope.budget.work+n > maxGroupWork {
		return &BudgetError{Kind: problems.ExpressionsTooMuchWork, Limit: maxGroupWork >> 20}
	}
	e.worked += n
	e.scope.budget.work += n
	return nil
}

// read counts the work of an operation that reads values: their sizes.
func (e *evaluator) read(at int, vs ...any) error {
	n := 0
	for _, v := range vs {
		n += size(v)
	}
	return e.work(at, n)
}

// readText counts the work of an operation that reads those of vs that are
// strings, such as to convert them to numbers: their sizes.
func (e *evaluator) readText(at int, vs ...any) error {
	n := 0
	for _, v := range vs {
		if s, ok := v.(string); ok {
			n += len(s)
		}
	}
	return e.work(at, n)
}

// run counts a run of a lambda's body: a step.
func (e *evaluator) run(at int) error {
	if err := e.work(at, step); err != nil {
		return err
	}
	return e.stepped()
}

// built counts v as built and returns it.
func (e *evaluator) built(at int, v any) (any, error) {
	if err := e.spend(at, size(v)); err != nil {
		return nil, err
	}
	return v, nil
}

func (e *evaluator) eval(n node) (any, error) {
	if err := e.stepped(); err != nil {
		return nil, err
	}
	// Each node is a step: an operand, an operator or a call. A group
	// isn't a node, and a lambda isn't evaluated as one: each of its runs
	// is a step (each).
	if err := e.work(n.offset(), step); err != nil {
		return nil, err
	}
	switch n := n.(type) {
	case *literal:
		// A list or mapping written as JSON is read each time it is
		// evaluated.
		switch n.value.(type) {
		case []any, map[string]any:
			if err := e.read(n.at, n.value); err != nil {
				return nil, err
			}
		}
		// A copy, so that a caller that changes the result doesn't change
		// the expression.
		return jsonvalue.Clone(n.value), nil
	case *word:
		return n.text, nil
	case *ref:
		return e.ref(n)
	case *param:
		path, err := e.names(n.at, n.path)
		if err != nil {
			return nil, err
		}
		// The parameter's name counts its bytes, beyond the step.
		if err := e.work(n.at, nameWork(n.name)-step); err != nil {
			return nil, err
		}
		if err := e.pathWork(n.at, path); err != nil {
			return nil, err
		}
		v, ok, err := walk(e.frame.lookup(n.name), path, func(w int) error { return e.work(n.at, w) })
		if err != nil || !ok {
			return nil, err
		}
		return v, nil
	case *relative:
		return e.relative(n)
	case *unary:
		v, err := e.eval(n.x)
		if err != nil {
			return nil, err
		}
		return e.prefix(n.at, n.op, v)
	case *binary:
		return e.binary(n)
	case *ternary:
		c, err := e.eval(n.cond)
		if err != nil {
			return nil, err
		}
		if truthy(c) {
			return e.eval(n.then)
		}
		return e.eval(n.els)
	case *member:
		v, err := e.eval(n.x)
		if err != nil {
			return nil, err
		}
		path, err := e.names(n.at, n.path)
		if err != nil {
			return nil, err
		}
		if err := e.pathWork(n.at, path); err != nil {
			return nil, err
		}
		// A computed value holds nothing unknown, since a reference that
		// reads something unknown fails.
		v, ok, err := walk(v, path, func(w int) error { return e.work(n.at, w) })
		if err != nil || !ok {
			return nil, err
		}
		return v, nil
	case *call:
		return e.call(n)
	case *slice:
		start, err := e.eval(n.start)
		if err != nil {
			return nil, err
		}
		end := ""
		if n.end != nil {
			v, err := e.eval(n.end)
			if err != nil {
				return nil, err
			}
			end = stringOf(v)
		}
		return e.built(n.at, stringOf(start)+":"+end)
	case *concat:
		v, err := e.eval(n.x)
		if err != nil {
			return nil, err
		}
		return e.built(n.at, n.prefix+stringOf(v))
	}
	return nil, fmt.Errorf("expr: unknown node %T", n)
}

func (e *evaluator) binary(n *binary) (any, error) {
	a, err := e.eval(n.x)
	if err != nil {
		return nil, err
	}
	switch n.op {
	case "||":
		if truthy(a) {
			return a, nil
		}
		return e.eval(n.y)
	case "&&":
		if !truthy(a) {
			return a, nil
		}
		return e.eval(n.y)
	case "??":
		if a != nil {
			return a, nil
		}
		return e.eval(n.y)
	}
	b, err := e.eval(n.y)
	if err != nil {
		return nil, err
	}
	switch n.op {
	case "==", "!=", "===", "!==", "<", ">", "<=", ">=", "in", "includes", "startsWith", "endsWith", "split", "join":
		// These read both operands, which cost their sizes whether or not
		// they are read to the end.
		if err := e.read(n.at, a, b); err != nil {
			return nil, err
		}
	case "range":
		// range reads text it converts to a number.
		if err := e.readText(n.at, a, b); err != nil {
			return nil, err
		}
	}
	switch n.op {
	case "==":
		return looseEqual(a, b), nil
	case "!=":
		return !looseEqual(a, b), nil
	case "===":
		return strictEqual(a, b), nil
	case "!==":
		return !strictEqual(a, b), nil
	case "<", ">", "<=", ">=":
		c, ok := compare(a, b)
		if !ok {
			return false, nil
		}
		switch n.op {
		case "<":
			return c < 0, nil
		case ">":
			return c > 0, nil
		case "<=":
			return c <= 0, nil
		}
		return c >= 0, nil
	case "in":
		return contains(b, a) != n.negated, nil
	case "includes":
		return contains(a, b) != n.negated, nil
	case "startsWith":
		return strings.HasPrefix(stringOf(a), stringOf(b)) != n.negated, nil
	case "endsWith":
		return strings.HasSuffix(stringOf(a), stringOf(b)) != n.negated, nil
	case "get":
		k, ok := key(b)
		if !ok {
			return nil, nil
		}
		// get reads a key that is text, which it looks up or reads as an
		// index, and a string it reads into, as a path's name does.
		if err := e.readText(n.at, b); err != nil {
			return nil, err
		}
		v, _, err := lookup(a, k, func(w int) error { return e.work(n.at, w) })
		return v, err
	case "range":
		return e.rangeOf(n.at, a, b)
	case "split":
		s, sep := stringOf(a), stringOf(b)
		// Count the list before making it, as the string's bytes and 8 for
		// each part that strings.Split makes: an empty separator makes a
		// part of every character, and none of an empty string.
		count := strings.Count(s, sep) + 1
		if sep == "" {
			count = utf8.RuneCountInString(s)
		}
		if err := e.spend(n.at, len(s)+8*count); err != nil {
			return nil, err
		}
		parts := strings.Split(s, sep)
		out := make([]any, len(parts))
		for i, p := range parts {
			out[i] = p
		}
		return out, nil
	case "join":
		return e.join(n.at, a, stringOf(b))
	case "+":
		_, aStr := a.(string)
		_, bStr := b.(string)
		if aStr || bStr {
			return e.built(n.at, stringOf(a)+stringOf(b))
		}
	}
	// Arithmetic reads text it converts to a number.
	if err := e.readText(n.at, a, b); err != nil {
		return nil, err
	}
	x, y := toNumber(a), toNumber(b)
	switch n.op {
	case "+":
		return x + y, nil
	case "-":
		return x - y, nil
	case "*":
		return x * y, nil
	case "/":
		return x / y, nil
	case "//":
		return math.Floor(x / y), nil
	case "%", "mod":
		return math.Mod(x, y), nil
	case "**", "^", "exp":
		return math.Pow(x, y), nil
	}
	return nil, e.errorAt(n.at, problems.UnknownOperator, problems.Params{"op": n.op})
}

func (e *evaluator) call(n *call) (any, error) {
	if check, ok := statusFunctions[n.name]; ok {
		if e.scope.Status == nil {
			return nil, &UnknownError{Offset: n.at, Name: "status"}
		}
		return check(e.scope.Status), nil
	}
	if n.name == "case" {
		// case(pred, value, ..., default) returns the value of the first
		// true predicate, and evaluates nothing after it.
		for i := 0; i+1 < len(n.args); i += 2 {
			pred, err := e.eval(n.args[i])
			if err != nil {
				return nil, err
			}
			if truthy(pred) {
				return e.eval(n.args[i+1])
			}
		}
		return e.eval(n.args[len(n.args)-1])
	}
	fn := functions[n.name]
	if fn.each != nil {
		return e.each(n, fn)
	}
	args := make([]any, len(n.args))
	for i, a := range n.args {
		v, err := e.eval(a)
		if err != nil {
			return nil, err
		}
		args[i] = v
	}
	if fn.reads {
		if err := e.read(n.at, args...); err != nil {
			return nil, err
		}
	}
	return fn.eval(e, n.at, args)
}

// each evaluates n, a call to map() or filter(): it runs the lambda that is
// the last argument for each item of the first, and makes a list of what
// fn.each gives for each. Each run is a step, and it counts 8 for each item
// of the list it makes and the item's size.
func (e *evaluator) each(n *call, fn function) (any, error) {
	v, err := e.eval(n.args[0])
	if err != nil {
		return nil, err
	}
	c := collection(v)
	if err := e.work(n.at, traversal(v, c)); err != nil {
		return nil, err
	}
	keys, items, ok := entries(c)
	if !ok {
		return nil, e.errorAt(n.at, problems.NotListOrMapping, problems.Params{"name": n.name, "value": problems.Show(v)})
	}
	f := &frame{lambda: n.args[len(n.args)-1].(*lambda), outer: e.frame}
	e.frame = f
	defer func() { e.frame = f.outer }()
	out := []any{}
	for i, item := range items {
		if err := e.run(n.at); err != nil {
			return nil, err
		}
		f.values = [2]any{item, keys[i]}
		result, err := e.eval(f.lambda.body)
		if err != nil {
			return nil, err
		}
		if kept, keep := fn.each(item, result); keep {
			if err := e.spend(n.at, 8+size(kept)); err != nil {
				return nil, err
			}
			out = append(out, kept)
		}
	}
	return out, nil
}

func (e *evaluator) ref(r *ref) (any, error) {
	root, ok := e.scope.Contexts[r.context]
	if !ok {
		return nil, &UnknownError{Offset: r.at, Name: r.context}
	}
	path, err := e.names(r.at, r.path)
	if err != nil {
		return nil, err
	}
	if err := e.pathWork(r.at, path); err != nil {
		return nil, err
	}
	if r.context == "inputs" {
		v, err := e.inputs(r, root, path)
		if err != nil {
			return nil, err
		}
		return e.known(r.at, root, path, v)
	}
	v, ok, err := walk(root, path, func(w int) error { return e.work(r.at, w) })
	if err != nil {
		return nil, err
	}
	if !ok && contexts[r.context].strings {
		return "", nil
	}
	return e.known(r.at, root, path, v)
}

// known returns v, which a reference at offset at read by following path
// from root, or an *UnknownError if v is or holds an Unknown. A list that a
// * or a slice makes is new, and holds nothing unknown if what it is made
// from doesn't, so that is what it looks for first: it found that before if
// the reference read it already.
func (e *evaluator) known(at int, root any, path []string, v any) (any, error) {
	if u, ok := v.(Unknown); ok {
		return nil, &UnknownError{Offset: at, Name: u.Name}
	}
	if i := slices.IndexFunc(path, makesList); i >= 0 {
		from, ok := source(root, path[:i])
		if !ok {
			return v, nil
		}
		if _, ok := e.unknown(from); !ok {
			return v, nil
		}
		if slices.Contains(path[i:], "*") {
			// The list a * makes is new each time: look through its items,
			// which aren't.
			items, _ := v.([]any)
			for _, item := range items {
				if u, ok := e.unknown(item); ok {
					return nil, &UnknownError{Offset: at, Name: u.Name}
				}
			}
			return v, nil
		}
	}
	if u, ok := e.unknown(v); ok {
		return nil, &UnknownError{Offset: at, Name: u.Name}
	}
	return v, nil
}

// makesList reports whether a path's segment makes a new list when it reads
// one: a *, or a slice such as 1:3.
func makesList(name string) bool { return name == "*" || strings.Contains(name, ":") }

// source returns what path leads to from root through lists and mappings
// alone, which a list a reference makes is made from. ok is false if it
// leads through anything else, such as text, where nothing unknown is, or
// nowhere.
func source(root any, path []string) (any, bool) {
	v := root
	for _, name := range path {
		switch v.(type) {
		case []any, map[string]any:
		default:
			return nil, false
		}
		next, ok := property(v, name)
		if !ok {
			return nil, false
		}
		v = next
	}
	return v, true
}

// An identity tells a list or a mapping from any other while it is held:
// where its items are, and how many a list has.
type identity struct {
	at  uintptr
	len int
}

// found is what unknown found in a list or a mapping.
type found struct {
	unknown Unknown
	ok      bool
}

// unknown returns the Unknown that v is or holds, if any. It remembers what
// it finds in each list and mapping, which keeps its identity while the
// evaluation holds it, so that a reference that reads one again doesn't go
// through it again. A list that a * makes is new each time, and isn't
// passed here.
func (e *evaluator) unknown(v any) (Unknown, bool) {
	var id identity
	switch c := v.(type) {
	case Unknown:
		return c, true
	case []any:
		id = identity{reflect.ValueOf(c).Pointer(), len(c)}
	case map[string]any:
		id = identity{at: reflect.ValueOf(c).Pointer()}
	default:
		return Unknown{}, false
	}
	if f, ok := e.unknowns[id]; ok {
		return f.unknown, f.ok
	}
	u, ok := holdsUnknown(v)
	if e.unknowns == nil {
		e.unknowns = map[identity]found{}
	}
	e.unknowns[id] = found{u, ok}
	return u, ok
}

// holdsUnknown returns the Unknown that v is or holds, if any.
func holdsUnknown(v any) (Unknown, bool) {
	switch v := v.(type) {
	case Unknown:
		return v, true
	case []any:
		for _, item := range v {
			if u, ok := holdsUnknown(item); ok {
				return u, true
			}
		}
	case map[string]any:
		for _, item := range v {
			if u, ok := holdsUnknown(item); ok {
				return u, true
			}
		}
	}
	return Unknown{}, false
}

// inputs reads an input's value, or what its definition holds: inputs.x.type
// is the type of input x, as its value has no type. A last segment in
// brackets without quotes, as in inputs.x[hidden], reads the definition
// first.
func (e *evaluator) inputs(r *ref, values any, path []string) (any, error) {
	charge := func(w int) error { return e.work(r.at, w) }
	definitionFirst := len(r.path) > 0 && r.path[len(r.path)-1].kind == bareKey
	if !definitionFirst {
		if v, ok, err := walk(values, path, charge); err != nil || ok {
			return v, err
		}
	}
	if e.scope.Resolver != nil && len(path) > 0 {
		v, ok, err := e.scope.Resolver.Definition(path, func(w int) error { return e.work(r.at, w) })
		if err != nil {
			return nil, e.resolverError(r.at, err)
		}
		if ok {
			return v, nil
		}
	}
	if definitionFirst {
		if v, ok, err := walk(values, path, charge); err != nil || ok {
			return v, err
		}
	}
	return nil, nil
}

func (e *evaluator) relative(r *relative) (any, error) {
	if e.scope.Resolver == nil {
		return nil, &UnknownError{Offset: r.at, Name: "workflow"}
	}
	path, err := e.names(r.at, r.path)
	if err != nil {
		return nil, err
	}
	if err := e.pathWork(r.at, path); err != nil {
		return nil, err
	}
	v, ok, err := e.scope.Resolver.Relative(r.up, path, func(w int) error { return e.work(r.at, w) })
	if err != nil || !ok {
		return nil, e.resolverError(r.at, err)
	}
	return v, nil
}

// resolverError reports an error from the resolver at the reference that
// led to it: a problem in another expression of the workflow is a problem
// evaluating this one. The group's budget running out, which the two
// expressions share, isn't a problem at either, and is returned as it is.
// A problem in another expression has its Location; one without is this
// evaluation's own, from counting the work of what the reference's path
// reads, and is returned as it is too.
func (e *evaluator) resolverError(at int, err error) error {
	if err == nil {
		return nil
	}
	if u, ok := errors.AsType[*UnknownError](err); ok {
		return &UnknownError{Offset: at, Name: u.Name}
	}
	if be, ok := errors.AsType[*BudgetError](err); ok {
		return be
	}
	if ee, ok := errors.AsType[*EvalError](err); ok {
		return ee
	}
	if se, ok := errors.AsType[*SyntaxError](err); ok && se.Location != "" {
		return se
	}
	return &EvalError{Offset: at, Kind: problems.ExpressionFailed, Params: problems.Params{"reason": err.Error()}}
}

// names returns a path's names, with each [index] replaced by its index.
func (e *evaluator) names(at int, path []segment) ([]string, error) {
	names := make([]string, len(path))
	k := 0
	for i, s := range path {
		if s.kind != index {
			names[i] = s.name
			continue
		}
		if k >= len(e.scope.Index) {
			return nil, &UnknownError{Offset: at, Name: "index"}
		}
		names[i] = strconv.Itoa(e.scope.Index[k])
		k++
	}
	return names, nil
}

// pathWork counts the steps of following path, which the node at offset at
// reads, from where it starts: the segments up to its first *, and that *.
// walk counts the rest as it follows the path.
func (e *evaluator) pathWork(at int, path []string) error {
	return e.work(at, namesWork(path))
}

// nameWork returns what reading a segment of a path counts: a name or an
// index, which it looks up, the bytes written, or 8 if that is more, and a *
// a step.
func nameWork(name string) int {
	if name == "*" {
		return step
	}
	return max(step, len(name))
}

// namesWork returns what the segments of path that are followed from where
// it starts count: those up to its first *, and that *.
func namesWork(path []string) int {
	n := 0
	for _, name := range path {
		n += nameWork(name)
		if name == "*" {
			break
		}
	}
	return n
}

// heldWork returns what reading v as c, the list or mapping it holds as
// JSON (collection), reads besides v itself: the value, if v is a string
// that holds one.
func heldWork(v, c any) int {
	if _, isString := v.(string); !isString {
		return 0
	}
	if _, isString := c.(string); isString {
		return 0
	}
	return size(c)
}

// lookup returns the property name of v, as property does, and counts with
// charge what it reads of a string: the string, and the value it holds as
// JSON, if it holds any, which the name is read from in turn. A string that
// doesn't hold JSON is read for a character of it.
func lookup(v any, name string, charge func(n int) error) (any, bool, error) {
	for {
		s, isString := v.(string)
		if !isString {
			next, ok := property(v, name)
			return next, ok, nil
		}
		if err := charge(len(s)); err != nil {
			return nil, false, err
		}
		parsed, ok := jsonValue(s)
		if !ok {
			next, ok := character(s, name)
			return next, ok, nil
		}
		if err := charge(size(parsed)); err != nil {
			return nil, false, err
		}
		v = parsed
	}
}

// traversal returns what going through v's items reads besides them, given
// c, what v holds (collection): a string, and the list or mapping it holds as
// JSON, if it holds one, and a mapping's keys, which they are gone through in
// the order of, 8 for each and its bytes. A list's items are gone through as
// they are.
func traversal(v, c any) int {
	switch v := v.(type) {
	case string:
		return len(v) + heldWork(v, c)
	case map[string]any:
		n := 8 * len(v)
		for k := range v {
			n += len(k)
		}
		return n
	}
	return 0
}

// walk follows a path from v. A * reads the rest of the path from each item
// of a list or mapping, and returns the results that have it as a list. ok is
// false if the path leads nowhere. charge, if it isn't nil, counts the work
// of what the path reads that its segments don't (pathWork): a string that a
// name reads into (lookup), and for each *, its traversal, and for each item
// a step and the segments of the rest of the path followed from it, up to
// its next *. An error from charge stops the walk.
func walk(v any, path []string, charge func(n int) error) (any, bool, error) {
	for i, name := range path {
		if _, ok := v.(Unknown); ok {
			return v, true, nil
		}
		if name != "*" {
			var next any
			ok := false
			if charge == nil {
				next, ok = property(v, name)
			} else {
				var err error
				if next, ok, err = lookup(v, name, charge); err != nil {
					return nil, false, err
				}
			}
			if !ok {
				return nil, false, nil
			}
			v = next
			continue
		}
		rest, c := path[i+1:], collection(v)
		if charge != nil {
			if err := charge(traversal(v, c)); err != nil {
				return nil, false, err
			}
		}
		items := elements(c)
		if charge != nil {
			if err := charge(len(items) * (step + namesWork(rest))); err != nil {
				return nil, false, err
			}
		}
		out := []any{}
		for _, item := range items {
			w, ok, err := walk(item, rest, charge)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				continue
			}
			if list, isList := w.([]any); isList && slices.Contains(rest, "*") {
				out = append(out, list...)
			} else {
				out = append(out, w)
			}
		}
		return out, true, nil
	}
	return v, true, nil
}

// IsUnknown reports whether err means an expression reads something that
// isn't known yet.
func IsUnknown(err error) bool {
	_, ok := errors.AsType[*UnknownError](err)
	return ok
}
