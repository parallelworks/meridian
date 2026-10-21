package expr

import (
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/problems"
)

// A Workflow is a workflow that the expressions in it read: through
// relative references, which read keys near the expression, and through
// inputs, whose definitions they can read as well as their values.
type Workflow struct {
	// Root is the workflow, as JSON values.
	Root map[string]any
	// Definitions is the path in Root of the input definitions.
	Definitions []string
}

// Scope returns scope with a Resolver for expressions at location in the
// workflow: the path of keys and list indexes that leads to them. The
// templates the expressions read through it are in their group: each has
// its own count for the language's limit, and they share the group's
// budget, which scope keeps if it has one.
func (w *Workflow) Scope(location []string, scope Scope) Scope {
	scope = scope.Budgeted()
	ev := &workflowEval{workflow: w, scope: scope, active: map[string]bool{}, done: map[string]any{}}
	scope.Resolver = &workflowResolver{eval: ev, location: location}
	return scope
}

// Eval evaluates what the workflow holds at location, in scope, as an
// expression there that read it would: the expressions in a string, and in
// each string of a list or mapping. It returns nil if there is nothing at
// location.
func (w *Workflow) Eval(location []string, scope Scope) (any, error) {
	r := w.Scope(location, scope).Resolver.(*workflowResolver)
	v, ok := r.raw(location)
	if !ok {
		return nil, nil
	}
	return r.resolve(v, location)
}

// A workflowEval evaluates the expressions in a workflow that one
// evaluation reaches, each at its own location and once. active holds the
// locations being evaluated, so that expressions that read each other fail
// instead of recursing forever, and done the values of those evaluated, and
// of the lists and mappings resolved, so that reading one again doesn't
// make it again.
type workflowEval struct {
	workflow *Workflow
	scope    Scope
	active   map[string]bool
	done     map[string]any
}

// A workflowResolver resolves for an expression at location.
type workflowResolver struct {
	eval     *workflowEval
	location []string
}

func (r *workflowResolver) evalAt(t *Template, location []string) (any, error) {
	ev := r.eval
	k := strings.Join(location, "\x00")
	if v, ok := ev.done[k]; ok {
		return v, nil
	}
	if ev.active[k] {
		return nil, &EvalError{Location: jsonptr.Format(location), Kind: problems.ExpressionsReadEachOther}
	}
	ev.active[k] = true
	defer delete(ev.active, k)
	scope := ev.scope
	scope.Resolver = &workflowResolver{eval: ev, location: location}
	v, err := t.Eval(scope)
	if err != nil {
		if IsUnknown(err) {
			return nil, err
		}
		return nil, located(err, location)
	}
	ev.done[k] = v
	return v, nil
}

// resolve evaluates what the workflow holds at location: a string's
// expressions, and those in each string of a list or mapping.
func (r *workflowResolver) resolve(v any, location []string) (any, error) {
	k := strings.Join(location, "\x00")
	if v, ok := r.eval.done[k]; ok {
		return v, nil
	}
	switch v := v.(type) {
	case string:
		t, err := ParseTemplate(v)
		if err != nil {
			return nil, located(err, location)
		}
		if t.IsText() {
			r.eval.done[k] = v
			return v, nil
		}
		return r.evalAt(t, location)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			var err error
			if out[i], err = r.resolve(item, appendPath(location, strconv.Itoa(i))); err != nil {
				return nil, err
			}
		}
		r.eval.done[k] = out
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range jsonvalue.Sorted(v) {
			var err error
			if out[key], err = r.resolve(item, appendPath(location, key)); err != nil {
				return nil, err
			}
		}
		r.eval.done[k] = out
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
