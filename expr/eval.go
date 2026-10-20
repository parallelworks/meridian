package expr

import (
	"errors"
	"fmt"
	"math"
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

// maxLambdaRuns is the language's limit on how many times one template runs
// the lambdas of map() and filter(), so that lambdas in lambdas can't make
// evaluating it take unbounded time.
const maxLambdaRuns = 100000

// maxGroupLambdaRuns is Meridian's own limit on how many times the
// templates of a group run lambdas together: ten templates at the
// language's limit. Like maxGroupBuilt, it isn't part of the format.
const maxGroupLambdaRuns = 10 * maxLambdaRuns

// A group's limits are at least one template's: this doesn't compile if
// they aren't.
const (
	_ uint = maxGroupBuilt - maxBuilt
	_ uint = maxGroupLambdaRuns - maxLambdaRuns
)

// maxGroupTime is Meridian's own limit on how long the templates of a group
// spend evaluating, together. Counting what expressions build and run can't
// bound the cost of a step that counts nothing, such as contains on a large
// value, which a lambda can repeat 100,000 times, so this is a last resort
// for workflows that do far more work than any real one. It isn't part of
// the format, since how long evaluating takes depends on the machine.
const maxGroupTime = 10 * time.Second

// clockEvery is how many nodes a group's templates evaluate between reads
// of the clock, besides the read at each run of a lambda's body, so that
// most nodes don't read it.
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

// A budget counts what the templates of a group have built, how many times
// they have run lambdas, and how long they have spent evaluating. The
// group's evaluations happen one at a time, though one can be nested in
// another, as a template that reads another is.
type budget struct {
	built, runs int
	// nodes counts the nodes the group's templates have evaluated, so that
	// the clock is read every clockEvery of them.
	nodes int
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
// part of the format: its templates build at most 64 MiB together, run
// lambdas at most 1,000,000 times, and spend at most 10 seconds evaluating.
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

// A Resolver reads the workflow around an expression.
type Resolver interface {
	// Relative returns the value of the relative reference that goes up
	// levels from the key the expression is in and then follows path, with
	// any expressions in it evaluated. ok is false if there is nothing
	// there.
	Relative(up int, path []string) (v any, ok bool, err error)
	// Definition returns what the input definitions hold at path, with any
	// expressions in it evaluated, or else the default of the input at
	// path. ok is false if there is neither.
	Definition(path []string) (v any, ok bool, err error)
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
// (Scope.Budgeted), built more together, ran lambdas more often, or spent
// longer evaluating than Meridian allows a group. It isn't a problem with
// any one expression, which can each be within the language's limits: the
// budget is Meridian's own, not part of the format.
type BudgetError struct {
	// Kind is the limit the group passed: problems.ExpressionsBuildTooMuch,
	// problems.ExpressionsRunTooManyLambdas or problems.ExpressionsTooSlow.
	Kind *problems.Kind
	// Limit is the group's limit: in MiB for what it builds, in runs for
	// its lambdas, and in seconds for its time.
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
	// spent counts what the template has built, and runs how many times it
	// has run lambdas, for the language's limits.
	spent, runs int
	// frame holds the parameters of the lambda whose body is being
	// evaluated, if any, and those of the lambdas around it.
	frame *frame
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

// spend counts n bytes built, and fails if they take the template past the
// language's limit, or its group past Meridian's. A count that fails isn't
// kept, since what it counts isn't built.
func (e *evaluator) spend(at, n int) error {
	if e.spent+n > maxBuilt {
		return e.errorAt(at, problems.ExpressionTooLarge, problems.Params{"limit": maxBuilt >> 20})
	}
	if e.scope.budget.built+n > maxGroupBuilt {
		return &BudgetError{Kind: problems.ExpressionsBuildTooMuch, Limit: maxGroupBuilt >> 20}
	}
	e.spent += n
	e.scope.budget.built += n
	return nil
}

// run counts a run of a lambda's body, and fails if it takes the template
// past the language's limit, or its group past Meridian's. It reads the
// clock too, since a body can do work that counts nothing.
func (e *evaluator) run(at int) error {
	if e.runs >= maxLambdaRuns {
		return e.errorAt(at, problems.ExpressionRunsTooManyLambdas, problems.Params{"limit": maxLambdaRuns})
	}
	if e.scope.budget.runs >= maxGroupLambdaRuns {
		return &BudgetError{Kind: problems.ExpressionsRunTooManyLambdas, Limit: maxGroupLambdaRuns}
	}
	e.runs++
	e.scope.budget.runs++
	return e.timed()
}

// built counts v as built and returns it.
func (e *evaluator) built(at int, v any) (any, error) {
	if err := e.spend(at, size(v)); err != nil {
		return nil, err
	}
	return v, nil
}

func (e *evaluator) eval(n node) (any, error) {
	if e.scope.budget.nodes++; e.scope.budget.nodes%clockEvery == 0 {
		if err := e.timed(); err != nil {
			return nil, err
		}
	}
	switch n := n.(type) {
	case *literal:
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
		v, ok := walk(e.frame.lookup(n.name), path)
		if !ok {
			return nil, nil
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
		v, ok := walk(v, path)
		if !ok {
			return nil, nil
		}
		return known(n.at, v)
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
		return stringOf(start) + ":" + end, nil
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
		v, _ := property(a, k)
		return v, nil
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
	if fn := functions[n.name]; fn.each != nil {
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
	return functions[n.name].eval(e, n.at, args)
}

// each evaluates n, a call to map() or filter(): it runs the lambda that is
// the last argument for each item of the first, and makes a list of what
// fn.each gives for each. It counts each run, and 8 for each item of the
// list and the item's size.
func (e *evaluator) each(n *call, fn function) (any, error) {
	v, err := e.eval(n.args[0])
	if err != nil {
		return nil, err
	}
	keys, items, ok := entries(v)
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
	if r.context == "inputs" {
		v, err := e.inputs(r, root, path)
		if err != nil {
			return nil, err
		}
		return known(r.at, v)
	}
	v, ok := walk(root, path)
	if !ok && contexts[r.context].strings {
		return "", nil
	}
	return known(r.at, v)
}

// known returns v, or an *UnknownError if v is or holds an Unknown.
func known(at int, v any) (any, error) {
	switch v := v.(type) {
	case Unknown:
		return nil, &UnknownError{Offset: at, Name: v.Name}
	case []any:
		for _, item := range v {
			if _, err := known(at, item); err != nil {
				return nil, err
			}
		}
	case map[string]any:
		for _, item := range v {
			if _, err := known(at, item); err != nil {
				return nil, err
			}
		}
	}
	return v, nil
}

// inputs reads an input's value, or what its definition holds: inputs.x.type
// is the type of input x, as its value has no type. A last segment in
// brackets without quotes, as in inputs.x[hidden], reads the definition
// first.
func (e *evaluator) inputs(r *ref, values any, path []string) (any, error) {
	definitionFirst := len(r.path) > 0 && r.path[len(r.path)-1].kind == bareKey
	if !definitionFirst {
		if v, ok := walk(values, path); ok {
			return v, nil
		}
	}
	if e.scope.Resolver != nil && len(path) > 0 {
		v, ok, err := e.scope.Resolver.Definition(path)
		if err != nil {
			return nil, e.resolverError(r.at, err)
		}
		if ok {
			return v, nil
		}
	}
	if definitionFirst {
		if v, ok := walk(values, path); ok {
			return v, nil
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
	v, ok, err := e.scope.Resolver.Relative(r.up, path)
	if err != nil || !ok {
		return nil, e.resolverError(r.at, err)
	}
	return v, nil
}

// resolverError reports an error from the resolver at the reference that
// led to it: a problem in another expression of the workflow is a problem
// evaluating this one. The group's budget running out, which the two
// expressions share, isn't a problem at either, and is returned as it is.
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
	if ee, ok := errors.AsType[*EvalError](err); ok && ee.Location != "" {
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

// walk follows a path from v. A * reads the rest of the path from each item
// of a list or mapping, and returns the results that have it as a list. ok is
// false if the path leads nowhere.
func walk(v any, path []string) (any, bool) {
	for i, name := range path {
		if _, ok := v.(Unknown); ok {
			return v, true
		}
		if name != "*" {
			next, ok := property(v, name)
			if !ok {
				return nil, false
			}
			v = next
			continue
		}
		rest := path[i+1:]
		out := []any{}
		for _, item := range elements(v) {
			w, ok := walk(item, rest)
			if !ok {
				continue
			}
			if list, isList := w.([]any); isList && slices.Contains(rest, "*") {
				out = append(out, list...)
			} else {
				out = append(out, w)
			}
		}
		return out, true
	}
	return v, true
}

// IsUnknown reports whether err means an expression reads something that
// isn't known yet.
func IsUnknown(err error) bool {
	_, ok := errors.AsType[*UnknownError](err)
	return ok
}
