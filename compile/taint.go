package compile

import (
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/internal/reach"
)

// taintState is what the inputResolver keeps of what taintInputs found.
type taintState struct {
	// taint holds the paths in the values, as patterns with * for a row's
	// index, whose values can read a variable or a password, and
	// secretTaint those whose values can read a password or a secret
	// variable (taintInputs). taintNodes holds the strings in the workflow
	// that taintInputs looked at, by their locations, and taintOrder them
	// in the order it found them.
	taint, secretTaint patterns
	taintNodes         map[string]*taintNode
	taintOrder         []*taintNode
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
	// What is held already: what the step that calls the run gave it that
	// is private (holdGiven).
	for _, p := range r.held {
		r.taint.add(p.path)
		if p.kind != heldPrivate {
			r.secretTaint.add(p.path)
		}
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
