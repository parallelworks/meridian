package expr

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/internal/jsonvalue"
)

// maxBuilt bounds the bytes of strings and lists that one evaluation can
// build, so that an expression can't make the compiler or coordinator use
// unbounded memory.
const maxBuilt = 8 << 20

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

	// spent counts what the evaluations that share it have built. Nil means
	// an evaluation counts on its own.
	spent *int
}

// Budgeted returns scope with one budget for what evaluations build, which
// every evaluation with the returned scope, or a copy of it, shares: together
// they build no more than one evaluation may. A scope that has a budget keeps
// it.
func (s Scope) Budgeted() Scope {
	if s.spent == nil {
		s.spent = new(int)
	}
	return s
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
	// Offset is the byte offset in the expression of the problem.
	Offset int
	Msg    string
}

func (e *EvalError) Error() string { return e.Msg }

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

// Eval evaluates the expression in scope.
func (e *Expr) Eval(scope Scope) (any, error) {
	return newEvaluator(scope).eval(e.root)
}

type evaluator struct {
	scope Scope
}

func newEvaluator(scope Scope) *evaluator {
	return &evaluator{scope: scope.Budgeted()}
}

func (e *evaluator) errorAt(at int, format string, args ...any) error {
	return &EvalError{Offset: at, Msg: fmt.Sprintf(format, args...)}
}

// spend counts n bytes built, and fails if the evaluation has built too
// much.
func (e *evaluator) spend(at, n int) error {
	*e.scope.spent += n
	if *e.scope.spent > maxBuilt {
		return e.errorAt(at, "the expression builds more than %d MiB", maxBuilt>>20)
	}
	return nil
}

// built counts v as built and returns it.
func (e *evaluator) built(at int, v any) (any, error) {
	if err := e.spend(at, size(v)); err != nil {
		return nil, err
	}
	return v, nil
}

func (e *evaluator) eval(n node) (any, error) {
	switch n := n.(type) {
	case *literal:
		// A copy, so that a caller that changes the result doesn't change
		// the expression.
		return jsonvalue.Clone(n.value), nil
	case *word:
		return n.text, nil
	case *ref:
		return e.ref(n)
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
			end = text(v)
		}
		return text(start) + ":" + end, nil
	case *concat:
		v, err := e.eval(n.x)
		if err != nil {
			return nil, err
		}
		return e.built(n.at, n.prefix+text(v))
	}
	panic(fmt.Sprintf("expr: unknown node %T", n))
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
		// Count the list before making it: an empty separator makes a
		// string of every character.
		if err := e.spend(n.at, len(s)+8*(strings.Count(s, sep)+1)); err != nil {
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
	return nil, e.errorAt(n.at, "unknown operator %s", n.op)
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
// evaluating this one.
func (e *evaluator) resolverError(at int, err error) error {
	if err == nil {
		return nil
	}
	if u, ok := errors.AsType[*UnknownError](err); ok {
		return &UnknownError{Offset: at, Name: u.Name}
	}
	return &EvalError{Offset: at, Msg: err.Error()}
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
