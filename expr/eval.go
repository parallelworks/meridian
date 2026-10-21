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
	// Marked holds the paths of the values in Contexts that are marked,
	// such as the secrets a runner masks: each is a context's name, vars
	// rather than var, then the names that lead to the value from it, a
	// list's items by their indexes, as in [vars token]. An evaluation that
	// reads a marked value, a list or mapping that holds one, or a part of
	// one, is marked, whatever it makes of it: see Template.EvalMarked. A
	// reference reads a marked value however it spells the keys that lead
	// to it, as a slice or a * does a list's items. Nil marks nothing.
	//
	// The evaluations that share a budget (Budgeted) index the paths, so
	// Marked's paths must not change while they are in use, though more can
	// be appended to it.
	Marked [][]string

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
	// clock reads the time: now, or a test's clock.
	clock func() time.Time
	// marked is set once what is being evaluated reads a value its scope
	// marks (Scope.Marked). An evaluation that another reads, such as an
	// input's default, notes what it read on its own, and adds that to its
	// reader's.
	marked bool
	// marks is what the group knows of the paths the scope marks.
	marks markIndex
}

// noting starts noting whether what is evaluated next reads a marked value,
// and returns whether what was evaluated before it did, for done.
func (b *budget) noting() (outer bool) {
	outer, b.marked = b.marked, false
	return outer
}

// done stops noting, and reports whether what was evaluated since noting
// read a marked value. What it read was read by what is around it too,
// which outer says had read one before.
func (b *budget) done(outer bool) bool {
	marked := b.marked
	b.marked = outer || marked
	return marked
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
		s.budget = &budget{clock: now}
	}
	return s
}

// start notes that one of the group's evaluations starts, and reads the
// clock: the time counts from then if no other is under way, and up to then
// if one is, as a template that another reads starts while its reader is
// under way. It fails, and the evaluation doesn't start, if the group has
// no time left, can't take a step more, or has built more than it may.
func (b *budget) start() error {
	now := b.clock()
	if b.active > 0 {
		b.elapsed += now.Sub(b.last)
	}
	b.last = now
	switch {
	case b.elapsed > maxGroupTime:
		return &BudgetError{Kind: problems.ExpressionsTooSlow, Limit: int(maxGroupTime / time.Second)}
	case b.work+step > maxGroupWork:
		return &BudgetError{Kind: problems.ExpressionsTooMuchWork, Limit: maxGroupWork >> 20}
	case b.built > maxGroupBuilt:
		return &BudgetError{Kind: problems.ExpressionsBuildTooMuch, Limit: maxGroupBuilt >> 20}
	}
	b.active++
	return nil
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

// A cost is what evaluating counts toward a group's budget: what it built,
// the work it did, and the steps it took.
type cost struct{ built, work, steps int }

func (c cost) plus(d cost) cost { return cost{c.built + d.built, c.work + d.work, c.steps + d.steps} }

func (c cost) minus(d cost) cost { return cost{c.built - d.built, c.work - d.work, c.steps - d.steps} }

// counted returns what the group's templates have counted so far.
func (b *budget) counted() cost { return cost{b.built, b.work, b.steps} }

// charge counts c, what evaluating something again would count, and
// reports true, if the group stays within its limits. Otherwise it counts
// nothing and reports false, so that evaluating it again fails where it
// would: past what the group may build or do, or once it has spent its
// time.
func (b *budget) charge(c cost) bool {
	if b.built+c.built > maxGroupBuilt || b.work+c.work > maxGroupWork || b.elapsed > maxGroupTime {
		return false
	}
	b.built, b.work, b.steps = b.built+c.built, b.work+c.work, b.steps+c.steps
	return true
}

// epoch is when the process started, which now reads the time from.
var epoch = time.Now()

// now reads the time from the monotonic clock alone, which is cheaper to read
// than time.Now, which reads the wall clock too. A budget only measures how
// long evaluations take.
func now() time.Time { return epoch.Add(time.Since(epoch)) }

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
	ev, err := newEvaluator(scope)
	if err != nil {
		return nil, err
	}
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
	// facts holds what the evaluation has found out about the lists and
	// mappings in its scope that references read.
	facts map[identity]*facts
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
// counts toward its group's until stop. It fails if the group has spent
// what it may (budget.start), so that a group's templates stop at its
// limits however little each does before it fails.
func newEvaluator(scope Scope) (*evaluator, error) {
	e := &evaluator{scope: scope.Budgeted()}
	if err := e.scope.budget.start(); err != nil {
		return nil, err
	}
	return e, nil
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

// spend counts n bytes that are about to be built, which count as work too,
// and fails if they take the template past the language's limits, or its
// group past Meridian's. A count that fails isn't kept, since what it
// counts isn't built.
func (e *evaluator) spend(at, n int) error { return e.count(at, n, n, false) }

// work counts n bytes of work that is about to be done, and fails if they
// take the template past the language's limit, or its group past
// Meridian's. A count that fails isn't kept, since the work it counts isn't
// done.
func (e *evaluator) work(at, n int) error { return e.count(at, 0, n, false) }

// did counts n bytes of work that is done already, such as a read, and
// fails as work does. The group keeps a count that fails all the same.
func (e *evaluator) did(at, n int) error { return e.count(at, 0, n, true) }

// count counts built bytes built and w bytes of work, and fails if they
// take the template past the language's limits, or its group past
// Meridian's. A count that passes is kept. One that fails isn't kept by the
// template, which fails, and its group keeps it if done is set: what it
// counts is built or done already, and the group's limits bound what its
// templates do, even those that fail their own. A group past its limits
// fails the next count, and every template that starts after it.
func (e *evaluator) count(at, built, w int, done bool) error {
	b := e.scope.budget
	var err error
	switch {
	case e.spent+built > maxBuilt:
		err = e.errorAt(at, problems.ExpressionTooLarge, problems.Params{"limit": maxBuilt >> 20})
	case b.built+built > maxGroupBuilt:
		err = &BudgetError{Kind: problems.ExpressionsBuildTooMuch, Limit: maxGroupBuilt >> 20}
	case e.worked+w > maxWork || b.work+w > maxGroupWork:
		err = e.tooMuchWork(at, w)
	default:
		e.spent, e.worked = e.spent+built, e.worked+w
	}
	if err == nil || done {
		b.built, b.work = b.built+built, b.work+w
	}
	return err
}

// tooMuchWork returns the error for n bytes of work that take the template
// past the language's limit, or its group past Meridian's.
func (e *evaluator) tooMuchWork(at, n int) error {
	if e.worked+n > maxWork {
		return e.errorAt(at, problems.ExpressionTooMuchWork, problems.Params{"limit": maxWork >> 20})
	}
	return &BudgetError{Kind: problems.ExpressionsTooMuchWork, Limit: maxGroupWork >> 20}
}

// read counts the work of an operation that read vs, their sizes, and
// returns result, what it made of them. A read is counted after it, since
// it holds nothing: an operation that reads too much fails all the same,
// and its group counts the read (did).
func (e *evaluator) read(at int, result any, vs ...any) (any, error) {
	n := 0
	for _, v := range vs {
		n += e.sizeOf(v)
	}
	if err := e.did(at, n); err != nil {
		return nil, err
	}
	return result, nil
}

// readBoth is read for an operation that read a and b.
func (e *evaluator) readBoth(at int, result, a, b any) (any, error) {
	if err := e.did(at, e.sizeOf(a)+e.sizeOf(b)); err != nil {
		return nil, err
	}
	return result, nil
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
	if n == 0 {
		return nil
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

// built counts v, which is built already, and returns it. If v takes the
// template past its limits, its group counts it all the same.
func (e *evaluator) built(at int, v any) (any, error) {
	n := size(v)
	if err := e.count(at, n, n, true); err != nil {
		return nil, err
	}
	return v, nil
}

func (e *evaluator) eval(n node) (any, error) {
	b := e.scope.budget
	if b.steps++; b.steps%clockEvery == 0 {
		if err := e.timed(); err != nil {
			return nil, err
		}
	}
	// Each node is a step: an operand, an operator or a call. A group
	// isn't a node, and a lambda isn't evaluated as one: each of its runs
	// is a step (each). Its offset is read only for an error.
	if e.worked+step > maxWork || b.work+step > maxGroupWork {
		return nil, e.tooMuchWork(n.offset(), step)
	}
	e.worked += step
	b.work += step
	switch n := n.(type) {
	case *literal:
		// A list or mapping written as JSON is read each time it is
		// evaluated.
		switch n.value.(type) {
		case []any, map[string]any:
			if _, err := e.read(n.at, nil, n.value); err != nil {
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
		f := follower{e: e, at: n.at}
		v, ok, err := f.walk(e.frame.lookup(n.name), path)
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
		f := follower{e: e, at: n.at}
		v, ok, err := f.walk(v, path)
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
	// The operators that compare and search read both operands, which cost
	// their sizes whether or not they are read to the end: they count them
	// after reading them (read).
	switch n.op {
	case "==":
		return e.readBoth(n.at, looseEqual(a, b), a, b)
	case "!=":
		return e.readBoth(n.at, !looseEqual(a, b), a, b)
	case "===":
		return e.readBoth(n.at, strictEqual(a, b), a, b)
	case "!==":
		return e.readBoth(n.at, !strictEqual(a, b), a, b)
	case "<", ">", "<=", ">=":
		c, ok := compare(a, b)
		holds := ok && (n.op == "<" && c < 0 || n.op == ">" && c > 0 || n.op == "<=" && c <= 0 || n.op == ">=" && c >= 0)
		return e.readBoth(n.at, holds, a, b)
	case "in":
		return e.readBoth(n.at, contains(b, a) != n.negated, a, b)
	case "includes":
		return e.readBoth(n.at, contains(a, b) != n.negated, a, b)
	case "startsWith":
		return e.readBoth(n.at, strings.HasPrefix(stringOf(a), stringOf(b)) != n.negated, a, b)
	case "endsWith":
		return e.readBoth(n.at, strings.HasSuffix(stringOf(a), stringOf(b)) != n.negated, a, b)
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
		f := follower{e: e, at: n.at}
		v, _, err := f.lookup(a, k)
		return v, err
	case "range":
		// range reads text it converts to a number.
		if err := e.readText(n.at, a, b); err != nil {
			return nil, err
		}
		return e.rangeOf(n.at, a, b)
	case "split":
		if _, err := e.readBoth(n.at, nil, a, b); err != nil {
			return nil, err
		}
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
		if _, err := e.readBoth(n.at, nil, a, b); err != nil {
			return nil, err
		}
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
		var err error
		if len(args) == 2 {
			_, err = e.readBoth(n.at, nil, args[0], args[1])
		} else {
			_, err = e.read(n.at, nil, args...)
		}
		if err != nil {
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
	var keys, items []any
	c, err := e.collection(n.at, v)
	if err != nil {
		return nil, err
	}
	switch c := c.(type) {
	case []any:
		keys, items = make([]any, len(c)), c
		for i := range c {
			keys[i] = float64(i)
		}
	case map[string]any:
		sorted := e.sortedKeys(c)
		if _, isString := v.(string); !isString {
			if err := e.did(n.at, keysWork(sorted)); err != nil {
				return nil, err
			}
		}
		keys, items = make([]any, len(sorted)), make([]any, len(sorted))
		for i, k := range sorted {
			keys[i], items[i] = k, c[k]
		}
	default:
		if c != nil {
			return nil, e.errorAt(n.at, problems.NotListOrMapping, problems.Params{"name": n.name, "value": problems.Show(v)})
		}
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
	if e.scope.Marked != nil && e.marks(r.context, path) {
		e.scope.budget.marked = true
	}
	f := follower{e: e, at: r.at}
	var v any
	if r.context == "inputs" {
		if v, err = e.inputs(r, &f, root, path); err != nil {
			return nil, err
		}
	} else {
		if v, ok, err = f.walk(root, path); err != nil {
			return nil, err
		}
		if !ok && contexts[r.context].strings {
			return "", nil
		}
	}
	if f.throughText {
		// What a path reads from text is new, and holds nothing unknown.
		return v, nil
	}
	return e.known(r.at, root, path, v)
}

// known returns v, which the reference at offset at read by following path
// from root, in the scope, or an *UnknownError if v is or holds an Unknown.
// It goes through a list or a mapping once in an evaluation (examine). A
// list that a * makes is new each time, so it looks through its items,
// unless the list or mapping it is made from was read before: then it goes
// through that once instead.
func (e *evaluator) known(at int, root any, path []string, v any) (any, error) {
	switch u := v.(type) {
	case Unknown:
		return nil, &UnknownError{Offset: at, Name: u.Name}
	case []any, map[string]any:
	default:
		return v, nil
	}
	if i := slices.Index(path, "*"); i >= 0 {
		if from, ok := source(root, path[:i]); ok && e.seen(from) {
			if f := e.examine(from); !f.holds {
				return v, nil
			}
		}
		items, _ := v.([]any)
		for _, item := range items {
			if _, isString := item.(string); isString {
				continue
			}
			if _, u, holds := examine(item); holds {
				return nil, &UnknownError{Offset: at, Name: u.Name}
			}
		}
		return v, nil
	}
	if f := e.examine(v); f != nil && f.holds {
		return nil, &UnknownError{Offset: at, Name: f.unknown.Name}
	}
	return v, nil
}

// source returns what path leads to from root through lists and mappings
// alone, which a list a reference makes with a * is made from. ok is false
// if it leads through anything else, such as text, where nothing unknown
// is, or nowhere.
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

// An identity tells a list or a mapping from any other while it is alive:
// where its items are, and how many a list has.
type identity struct {
	at  uintptr
	len int
}

// identityOf returns the identity of v, if it is a list or a mapping.
func identityOf(v any) (identity, bool) {
	switch c := v.(type) {
	case []any:
		return identity{reflect.ValueOf(v).Pointer(), len(c)}, true
	case map[string]any:
		return identity{reflect.ValueOf(v).Pointer(), -1}, true
	}
	return identity{}, false
}

// facts are what an evaluation has found out about a list or a mapping in
// its scope: its size, the Unknown it is or holds, if any, and, once asked
// for, a mapping's keys in order. examined is false while all that is known
// is that a reference read it with a *. held keeps it alive, so that no
// other takes its place, and its identity, while the facts are remembered.
type facts struct {
	examined bool
	size     int
	unknown  Unknown
	holds    bool
	keys     []string
	held     any
}

// factsOf returns what the evaluation has found out about v, or nil.
func (e *evaluator) factsOf(v any) *facts {
	if len(e.facts) == 0 {
		return nil
	}
	id, ok := identityOf(v)
	if !ok {
		return nil
	}
	return e.facts[id]
}

// seen reports whether the evaluation has met v, a list or mapping in its
// scope, before, and remembers it if not.
func (e *evaluator) seen(v any) bool {
	id, ok := identityOf(v)
	if !ok {
		return false
	}
	if _, ok := e.facts[id]; ok {
		return true
	}
	if e.facts == nil {
		e.facts = map[identity]*facts{}
	}
	e.facts[id] = &facts{held: v}
	return false
}

// examine returns what v, a list or mapping in the evaluation's scope, is
// and holds: it goes through it the first time, and remembers that. It
// returns nil for anything else.
func (e *evaluator) examine(v any) *facts {
	id, ok := identityOf(v)
	if !ok {
		return nil
	}
	f := e.facts[id]
	if f == nil {
		f = &facts{held: v}
		if e.facts == nil {
			e.facts = map[identity]*facts{}
		}
		e.facts[id] = f
	}
	if !f.examined {
		f.size, f.unknown, f.holds = examine(v)
		f.examined = true
	}
	return f
}

// examine returns v's size, and the Unknown that v is or holds, if any,
// going through it once. It stops at an Unknown, without the size.
func examine(v any) (n int, u Unknown, holds bool) {
	switch v := v.(type) {
	case string:
		return len(v), Unknown{}, false
	case Unknown:
		return 0, v, true
	case []any:
		n = step * len(v)
		for _, item := range v {
			if s, ok := item.(string); ok {
				n += len(s)
				continue
			}
			m, u, holds := examine(item)
			if holds {
				return 0, u, true
			}
			n += m
		}
		return n, Unknown{}, false
	case map[string]any:
		n = step * len(v)
		for k, item := range v {
			n += len(k)
			if s, ok := item.(string); ok {
				n += len(s)
				continue
			}
			m, u, holds := examine(item)
			if holds {
				return 0, u, true
			}
			n += m
		}
		return n, Unknown{}, false
	}
	return step, Unknown{}, false
}

// sizeOf returns v's size: as examine found it, if v is a list or mapping
// in the scope that a reference read.
func (e *evaluator) sizeOf(v any) int {
	if s, ok := v.(string); ok {
		return len(s)
	}
	return e.sizeOfValue(v)
}

// sizeOfValue is sizeOf for what isn't a string.
func (e *evaluator) sizeOfValue(v any) int {
	switch v.(type) {
	case []any, map[string]any:
		if f := e.factsOf(v); f != nil && f.examined && !f.holds {
			return f.size
		}
		return size(v)
	}
	return step
}

// sortedKeys returns a mapping's keys in order. It remembers them for a
// mapping in the scope that a reference read, so that going through it again
// doesn't sort them again.
func (e *evaluator) sortedKeys(m map[string]any) []string {
	f := e.factsOf(m)
	if f != nil && f.keys != nil {
		return f.keys
	}
	keys := sortedKeys(m)
	if f != nil {
		f.keys = keys
	}
	return keys
}

// sortedKeys returns a mapping's keys in order.
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// inputs reads an input's value, or what its definition holds: inputs.x.type
// is the type of input x, as its value has no type. A last segment in
// brackets without quotes, as in inputs.x[hidden], reads the definition
// first. f follows the path through the values.
func (e *evaluator) inputs(r *ref, f *follower, values any, path []string) (any, error) {
	definitionFirst := len(r.path) > 0 && r.path[len(r.path)-1].kind == bareKey
	if !definitionFirst {
		if v, ok, err := f.walk(values, path); err != nil || ok {
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
		if v, ok, err := f.walk(values, path); err != nil || ok {
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

// collection returns the list or mapping that v holds as JSON, or v
// itself, as collection does, and counts the work of reading a string as
// one, for the node at offset at (follower.collection).
func (e *evaluator) collection(at int, v any) (any, error) {
	f := follower{e: e, at: at}
	return f.collection(v)
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

// A follower follows paths, as walk does, and counts the work of what they
// read beyond their segments (pathWork): the evaluator's, for the node at
// offset at, or else charge's, if it isn't nil.
type follower struct {
	e      *evaluator
	at     int
	charge func(n int) error
	// throughText is set once a path reads into a string: what it reads
	// from there is new. A * sets it if each item's path does.
	throughText bool
}

// count counts n bytes of the follower's work.
func (f *follower) count(n int) error {
	switch {
	case f.e != nil:
		return f.e.work(f.at, n)
	case f.charge != nil:
		return f.charge(n)
	}
	return nil
}

// did counts n bytes of the follower's work that is done already, as count
// does, and the evaluator's group keeps them if they fail (evaluator.did).
func (f *follower) did(n int) error {
	if f.e != nil {
		return f.e.did(f.at, n)
	}
	return f.count(n)
}

// sortedKeys returns a mapping's keys in order, as the evaluator remembers
// them, if there is one.
func (f *follower) sortedKeys(m map[string]any) []string {
	if f.e != nil {
		return f.e.sortedKeys(m)
	}
	return sortedKeys(m)
}

// lookup returns the property name of v, as property does, and counts what
// it reads of a string: the string, and the value it holds as JSON, if it
// holds any, which the name is read from in turn. A string that doesn't
// hold JSON is read for a character of it.
func (f *follower) lookup(v any, name string) (any, bool, error) {
	for {
		s, isString := v.(string)
		if !isString {
			next, ok := property(v, name)
			return next, ok, nil
		}
		f.throughText = true
		if err := f.count(len(s)); err != nil {
			return nil, false, err
		}
		parsed, ok := jsonValue(s)
		if !ok {
			next, ok := character(s, name)
			return next, ok, nil
		}
		if err := f.did(size(parsed)); err != nil {
			return nil, false, err
		}
		v = parsed
	}
}

// collection returns the list or mapping that v holds as JSON, or v
// itself, as collection does, and counts the work of reading a string as
// one: its bytes, before it is parsed, so that a string too large to read
// isn't, and the value it holds, if it holds one (heldWork). It counts
// nothing for anything else, which isn't read as JSON.
func (f *follower) collection(v any) (any, error) {
	s, isString := v.(string)
	if !isString {
		return v, nil
	}
	if err := f.count(len(s)); err != nil {
		return nil, err
	}
	c := collection(s)
	if err := f.did(heldWork(s, c)); err != nil {
		return nil, err
	}
	return c, nil
}

// keysWork returns what going through a mapping's keys, in order, reads
// besides its values: 8 for each key and its bytes. Going through a list
// reads nothing besides its items, and through a string, what reading it as
// JSON does (follower.collection).
func keysWork(keys []string) int {
	n := step * len(keys)
	for _, k := range keys {
		n += len(k)
	}
	return n
}

// walk follows a path from v. A * reads the rest of the path from each item
// of a list or mapping, and returns the results that have it as a list. ok is
// false if the path leads nowhere. charge, if it isn't nil, counts the work
// of what the path reads beyond its segments, as a follower does.
func walk(v any, path []string, charge func(n int) error) (any, bool, error) {
	f := follower{charge: charge}
	return f.walk(v, path)
}

// walk follows a path from v, as walk does, and counts the work of what it
// reads beyond the segments (pathWork): a string that a name reads into
// (lookup), and for each *, what it goes through: a string and the JSON it
// holds (collection), or a mapping's keys (keysWork), and for each item a
// step and the segments of the rest of the path followed from it, up to its
// next *.
func (f *follower) walk(v any, path []string) (any, bool, error) {
	for i, name := range path {
		if _, ok := v.(Unknown); ok {
			return v, true, nil
		}
		if name != "*" {
			var next any
			ok := false
			if _, isString := v.(string); isString {
				var err error
				if next, ok, err = f.lookup(v, name); err != nil {
					return nil, false, err
				}
			} else {
				next, ok = property(v, name)
			}
			if !ok {
				return nil, false, nil
			}
			v = next
			continue
		}
		rest := path[i+1:]
		_, isString := v.(string)
		if isString {
			f.throughText = true
		}
		c, err := f.collection(v)
		if err != nil {
			return nil, false, err
		}
		var items []any
		switch c := c.(type) {
		case []any:
			items = c
		case map[string]any:
			keys := f.sortedKeys(c)
			if !isString {
				if err := f.did(keysWork(keys)); err != nil {
					return nil, false, err
				}
			}
			items = make([]any, len(keys))
			for i, k := range keys {
				items[i] = c[k]
			}
		}
		if err := f.count(len(items) * (step + namesWork(rest))); err != nil {
			return nil, false, err
		}
		// What the * makes is new if what each item's path reads is: if it
		// read into a string, or the items are read from one. An item that
		// isn't can be or hold an Unknown, as another that reads into a
		// string can't.
		out, flatten := make([]any, 0, len(items)), slices.Contains(rest, "*")
		fromText, allText := f.throughText, true
		for _, item := range items {
			f.throughText = fromText
			w, ok, err := f.walk(item, rest)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				continue
			}
			allText = allText && f.throughText
			if list, isList := w.([]any); isList && flatten {
				out = append(out, list...)
			} else {
				out = append(out, w)
			}
		}
		f.throughText = allText
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
