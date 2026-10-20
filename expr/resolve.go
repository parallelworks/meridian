package expr

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// A Document is a workflow that the expressions in it read: through
// relative references, which read keys near the expression, and through
// inputs, whose definitions they can read as well as their values.
type Document struct {
	// Root is the workflow, as JSON values.
	Root map[string]any
	// Definitions is the path in Root of the input definitions.
	Definitions []string
}

// Scope returns scope with a Resolver for expressions at location in the
// document: the path of keys and list indexes that leads to them.
func (d *Document) Scope(location []string, scope Scope) Scope {
	scope = scope.Budgeted()
	ev := &documentEval{doc: d, scope: scope, active: map[string]bool{}, done: map[string]any{}}
	scope.Resolver = &documentResolver{eval: ev, location: location}
	return scope
}

// Eval evaluates what the document holds at location, in scope, as an
// expression there that read it would: the expressions in a string, and in
// each string of a list or mapping. It returns nil if there is nothing at
// location.
func (d *Document) Eval(location []string, scope Scope) (any, error) {
	r := d.Scope(location, scope).Resolver.(*documentResolver)
	v, ok := r.raw(location)
	if !ok {
		return nil, nil
	}
	return r.resolve(v, location)
}

// A documentEval evaluates the expressions in a document that one
// evaluation reaches, each at its own location and once. active holds the
// locations being evaluated, so that expressions that read each other fail
// instead of recursing forever, and done the values of those evaluated.
type documentEval struct {
	doc    *Document
	scope  Scope
	active map[string]bool
	done   map[string]any
}

// A documentResolver resolves for an expression at location.
type documentResolver struct {
	eval     *documentEval
	location []string
}

func (r *documentResolver) evalAt(t *Template, location []string) (any, error) {
	ev := r.eval
	k := strings.Join(location, "\x00")
	if v, ok := ev.done[k]; ok {
		return v, nil
	}
	if ev.active[k] {
		return nil, fmt.Errorf("the expressions at %s read each other", pointer(location))
	}
	ev.active[k] = true
	defer delete(ev.active, k)
	scope := ev.scope
	scope.Resolver = &documentResolver{eval: ev, location: location}
	v, err := t.Eval(scope)
	if err != nil {
		if IsUnknown(err) {
			return nil, err
		}
		return nil, fmt.Errorf("in %s: %w", pointer(location), err)
	}
	ev.done[k] = v
	return v, nil
}

// resolve evaluates what the document holds at location: a string's
// expressions, and those in each string of a list or mapping.
func (r *documentResolver) resolve(v any, location []string) (any, error) {
	switch v := v.(type) {
	case string:
		t, err := ParseTemplate(v)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", pointer(location), err)
		}
		if t.IsText() {
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
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for _, k := range slices.Sorted(maps.Keys(v)) {
			var err error
			if out[k], err = r.resolve(v[k], appendPath(location, k)); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	return v, nil
}

// raw returns what the document holds at location, unevaluated.
func (r *documentResolver) raw(location []string) (any, bool) {
	return walk(r.eval.doc.Root, location)
}

// follow walks path from location in the document and returns the value it
// reaches, evaluated. A string on the way is evaluated, and the rest of the
// path read from its value.
func (r *documentResolver) follow(location, path []string) (any, bool, error) {
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
			v, ok := walk(resolved, path[i:])
			return v, ok, nil
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
func (r *documentResolver) Relative(up int, path []string) (any, bool, error) {
	if up > len(r.location) {
		return nil, false, nil
	}
	return r.follow(r.location[:len(r.location)-up], path)
}

// Definition reads the input definitions at path, or else the default of
// the input at path. An input in a group is in the group's items.
func (r *documentResolver) Definition(path []string) (any, bool, error) {
	if v, ok, err := r.follow(r.eval.doc.Definitions, path); err != nil || ok {
		return v, ok, err
	}
	location := r.eval.doc.Definitions
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
			return r.follow(next, []string{"default"})
		}
		definitions, location = field, next
		if items, ok := property(field, "items"); ok {
			definitions, location = items, appendPath(next, "items")
		}
	}
	return nil, false, nil
}

func appendPath(path []string, keys ...string) []string {
	out := make([]string, 0, len(path)+len(keys))
	return append(append(out, path...), keys...)
}

// pointer formats a location as a JSON Pointer.
func pointer(location []string) string {
	var b strings.Builder
	escape := strings.NewReplacer("~", "~0", "/", "~1")
	for _, k := range location {
		b.WriteString("/")
		b.WriteString(escape.Replace(k))
	}
	return b.String()
}
