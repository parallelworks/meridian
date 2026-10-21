package expr

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/problems"
)

// A Workflow is a workflow that the expressions in it read: through
// relative references, which read keys near the expression, and through
// inputs, whose definitions they can read as well as their values. It
// keeps the template of each string it parses, so that each is parsed once
// however many evaluations read it.
type Workflow struct {
	// Root is the workflow, as JSON values.
	Root map[string]any
	// Definitions is the path in Root of the input definitions.
	Definitions []string

	// mu guards templates, which holds the strings parsed so far, by their
	// text.
	mu        sync.Mutex
	templates map[string]parsed
}

// parsed is a string parsed as a template, or the error parsing it.
type parsed struct {
	t   *Template
	err error
}

// Template returns the template of s, a string in the workflow, as
// ParseTemplate does. The workflow parses each string once.
func (w *Workflow) Template(s string) (*Template, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if p, ok := w.templates[s]; ok {
		return p.t, p.err
	}
	t, err := ParseTemplate(s)
	if w.templates == nil {
		w.templates = map[string]parsed{}
	}
	w.templates[s] = parsed{t, err}
	return t, err
}

// Exprs returns the expressions in what the workflow holds at location:
// those in a string, or in each string of a list or mapping, in the order
// of a mapping's keys. It fails if a string's expressions don't parse.
func (w *Workflow) Exprs(location []string) ([]*Expr, error) {
	v, _, _ := walk(w.Root, location, nil)
	var out []*Expr
	var add func(v any) error
	add = func(v any) error {
		switch v := v.(type) {
		case string:
			if !strings.Contains(v, "${{") {
				return nil
			}
			t, err := w.Template(v)
			if err != nil {
				return err
			}
			for _, e := range t.Exprs() {
				out = append(out, e)
			}
		case []any:
			for _, item := range v {
				if err := add(item); err != nil {
					return err
				}
			}
		case map[string]any:
			for _, item := range jsonvalue.Sorted(v) {
				if err := add(item); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := add(v); err != nil {
		return nil, err
	}
	return out, nil
}

// Scope returns scope with a Resolver for expressions at location in the
// workflow: the path of keys and list indexes that leads to them. The
// templates the expressions read through it are in their group: each has
// its own count for the language's limit, and they share the group's
// budget, which scope keeps if it has one.
func (w *Workflow) Scope(location []string, scope Scope) Scope {
	scope = scope.Budgeted()
	scope.Resolver = &workflowResolver{eval: newWorkflowEval(w, scope), location: location}
	return scope
}

// Eval evaluates what the workflow holds at location, in scope, as an
// expression there that read it would: the expressions in a string, and in
// each string of a list or mapping. It returns nil if there is nothing at
// location.
func (w *Workflow) Eval(location []string, scope Scope) (any, error) {
	return w.Evaluator(scope).Eval(location)
}

// An Evaluator evaluates what a workflow holds, as Workflow.Eval does, in
// one scope, whose contexts must not change while it is in use. It keeps
// the value of each string it evaluates, and of each list and mapping it
// resolves, so that evaluating one again, or another that reads it,
// doesn't make it again.
//
// Each evaluation counts toward the scope's group as evaluating everything
// it reads anew would: a value it keeps counts what making it counted, once
// in each evaluation that reads it, and is made again if that would take
// the group past its limits, so that the group's limit fails where it
// would. An Evaluator is for one goroutine at a time.
type Evaluator struct {
	eval *workflowEval
}

// Evaluator returns an Evaluator of the workflow's expressions in scope.
// Its evaluations are a group that shares scope's budget, if it has one.
func (w *Workflow) Evaluator(scope Scope) *Evaluator {
	return &Evaluator{newWorkflowEval(w, scope.Budgeted())}
}

// Eval evaluates what the workflow holds at location, as Workflow.Eval
// does.
func (e *Evaluator) Eval(location []string) (any, error) {
	e.eval.evaluation++
	r := &workflowResolver{eval: e.eval, location: location}
	v, ok := r.raw(location)
	if !ok {
		return nil, nil
	}
	return r.resolve(v, location)
}

// A workflowEval evaluates the expressions in a workflow that its
// evaluations reach, each at its own location. active holds the locations
// being evaluated, so that expressions that read each other fail instead
// of recursing forever, and done the values of those evaluated, and of the
// lists and mappings resolved, so that reading one again doesn't make it
// again.
type workflowEval struct {
	workflow *Workflow
	scope    Scope
	active   map[string]bool
	done     map[string]*made
	// evaluation counts the evaluations, so that a value made in one
	// counts toward the group in each later one that reads it; making is
	// the value being made, which notes the values it reads.
	evaluation int
	making     *made
	// walks counts the walks of what values read, to mark the values each
	// walk reaches.
	walks int
}

func newWorkflowEval(w *Workflow, scope Scope) *workflowEval {
	return &workflowEval{workflow: w, scope: scope, active: map[string]bool{}, done: map[string]*made{}}
}

// A made is a value that evaluating a string, or resolving a list or
// mapping, made, with what making it counted toward the group: its cost,
// beyond that of the values it read, which have their own.
type made struct {
	v     any
	cost  cost
	reads []*made
	// read counts what the values it read counted while it was made, and
	// readBy is the value that last read it, so that each value notes
	// another once.
	read   cost
	readBy *made
	// counted is the evaluation that last counted it, and walked the last
	// walk that reached it.
	counted, walked int
}

// begin starts making a value, which notes the values it reads, and
// returns it, the value being made before, and what the group has counted
// so far.
func (ev *workflowEval) begin() (m, outer *made, before cost) {
	m, outer = &made{counted: ev.evaluation}, ev.making
	ev.making = m
	return m, outer, ev.scope.budget.counted()
}

// keep keeps v, which m made at k, once making it is done, and notes that
// outer, the value being made around it, read it. A value whose making
// failed isn't kept, nor is what reads it, which fails too.
func (ev *workflowEval) keep(k string, m, outer *made, before cost, v any) {
	ev.making = outer
	spent := ev.scope.budget.counted().minus(before)
	m.v, m.cost = v, spent.minus(m.read)
	ev.done[k] = m
	ev.noteRead(m, spent)
}

// noteRead notes that the value being made read m, which counted c.
func (ev *workflowEval) noteRead(m *made, c cost) {
	outer := ev.making
	if outer == nil {
		return
	}
	outer.read = outer.read.plus(c)
	if m.readBy != outer {
		m.readBy = outer
		outer.reads = append(outer.reads, m)
	}
}

// count counts m toward the group, with the values it read that this
// evaluation hasn't counted, as making them again would. It counts nothing,
// and reports false, if that would take the group past its limits: m is
// then made again, so that the limit fails where it would.
func (ev *workflowEval) count(m *made) bool {
	if m.counted == ev.evaluation {
		ev.noteRead(m, cost{})
		return true
	}
	ev.walks++
	var c cost
	reached := []*made{m}
	m.walked = ev.walks
	for i := 0; i < len(reached); i++ {
		x := reached[i]
		c = c.plus(x.cost)
		for _, y := range x.reads {
			if y.counted != ev.evaluation && y.walked != ev.walks {
				y.walked = ev.walks
				reached = append(reached, y)
			}
		}
	}
	if !ev.scope.budget.charge(c) {
		return false
	}
	for _, x := range reached {
		x.counted = ev.evaluation
	}
	ev.noteRead(m, c)
	return true
}

// A workflowResolver resolves for an expression at location.
type workflowResolver struct {
	eval     *workflowEval
	location []string
}

// evalAt evaluates t, the template of the string at location, whose key is
// k.
func (r *workflowResolver) evalAt(t *Template, location []string, k string) (any, error) {
	ev := r.eval
	if ev.active[k] {
		return nil, &EvalError{Location: jsonptr.Format(location), Kind: problems.ExpressionsReadEachOther}
	}
	ev.active[k] = true
	defer delete(ev.active, k)
	m, outer, before := ev.begin()
	scope := ev.scope
	scope.Resolver = &workflowResolver{eval: ev, location: location}
	v, err := t.Eval(scope)
	if err != nil {
		ev.making = outer
		if IsUnknown(err) {
			return nil, err
		}
		return nil, located(err, location)
	}
	ev.keep(k, m, outer, before, v)
	return v, nil
}

// resolve evaluates what the workflow holds at location: a string's
// expressions, and those in each string of a list or mapping.
func (r *workflowResolver) resolve(v any, location []string) (any, error) {
	ev := r.eval
	k := strings.Join(location, "\x00")
	if m, ok := ev.done[k]; ok && ev.count(m) {
		return m.v, nil
	}
	switch v := v.(type) {
	case string:
		t, err := ev.workflow.Template(v)
		if err != nil {
			return nil, located(err, location)
		}
		if t.IsText() {
			m, outer, before := ev.begin()
			ev.keep(k, m, outer, before, v)
			return v, nil
		}
		return r.evalAt(t, location, k)
	case []any:
		m, outer, before := ev.begin()
		out := make([]any, len(v))
		for i, item := range v {
			var err error
			if out[i], err = r.resolve(item, appendPath(location, strconv.Itoa(i))); err != nil {
				ev.making = outer
				return nil, err
			}
		}
		ev.keep(k, m, outer, before, out)
		return out, nil
	case map[string]any:
		m, outer, before := ev.begin()
		out := make(map[string]any, len(v))
		for key, item := range jsonvalue.Sorted(v) {
			var err error
			if out[key], err = r.resolve(item, appendPath(location, key)); err != nil {
				ev.making = outer
				return nil, err
			}
		}
		ev.keep(k, m, outer, before, out)
		return out, nil
	}
	return v, nil
}

// raw returns what the workflow holds at location, unevaluated.
func (r *workflowResolver) raw(location []string) (any, bool) {
	v, ok, _ := walk(r.eval.workflow.Root, location, nil)
	return v, ok
}

// follow walks path from location in the workflow and returns the value it
// reaches, evaluated. A string on the way is evaluated, and the rest of the
// path read from its value, with charge, as walk does.
func (r *workflowResolver) follow(location, path []string, charge func(n int) error) (any, bool, error) {
	v, ok := r.raw(location)
	if !ok {
		return nil, false, nil
	}
	for i, k := range path {
		if s, isString := v.(string); isString {
			resolved, err := r.resolve(s, location)
			if err != nil {
				return nil, false, err
			}
			return walk(resolved, path[i:], charge)
		}
		if v, ok = property(v, k); !ok {
			return nil, false, nil
		}
		location = appendPath(location, k)
	}
	v, err := r.resolve(v, location)
	return v, err == nil, err
}

// Relative resolves a relative reference: .x is a key beside the one the
// expression is in, and each further level goes up one more.
func (r *workflowResolver) Relative(up int, path []string, charge func(n int) error) (any, bool, error) {
	if up > len(r.location) {
		return nil, false, nil
	}
	return r.follow(r.location[:len(r.location)-up], path, charge)
}

// Definition reads the input definitions at path, or else the default of
// the input at path. An input in a group is in the group's items.
func (r *workflowResolver) Definition(path []string, charge func(n int) error) (any, bool, error) {
	if v, ok, err := r.follow(r.eval.workflow.Definitions, path, charge); err != nil || ok {
		return v, ok, err
	}
	location := r.eval.workflow.Definitions
	definitions, _ := r.raw(location)
	for i, k := range path {
		field, ok := property(definitions, k)
		next := appendPath(location, k)
		if !ok {
			items, isGroup := property(definitions, "items")
			if !isGroup {
				return nil, false, nil
			}
			if field, ok = property(items, k); !ok {
				return nil, false, nil
			}
			next = appendPath(location, "items", k)
		}
		if i == len(path)-1 {
			if _, ok := property(field, "default"); !ok {
				return nil, false, nil
			}
			return r.follow(next, []string{"default"}, nil)
		}
		definitions, location = field, next
		if items, ok := property(field, "items"); ok {
			definitions, location = items, appendPath(next, "items")
		}
	}
	return nil, false, nil
}

// located returns err, a problem in the string at location, with that
// location, unless it has one already: a problem in another string that
// string reads.
func located(err error, location []string) error {
	if ee, ok := errors.AsType[*EvalError](err); ok && ee.Location == "" {
		c := *ee
		c.Location = jsonptr.Format(location)
		return &c
	}
	if se, ok := errors.AsType[*SyntaxError](err); ok && se.Location == "" {
		c := *se
		c.Location = jsonptr.Format(location)
		return &c
	}
	return err
}

// appendPath returns a new path: path followed by keys.
func appendPath(path []string, keys ...string) []string {
	return slices.Concat(path, keys)
}
