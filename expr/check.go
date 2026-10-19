package expr

import (
	"fmt"
	"strings"
)

// A Ref is a context an expression reads, and the path it reads in it.
type Ref struct {
	// Offset is the byte offset in the expression of the reference.
	Offset int
	// Context is the context's name. var is vars.
	Context string
	// Path is the reference's path in the context. An [index] is "[index]",
	// and a .* is "*".
	Path []string
}

// Refs returns the contexts an expression reads, in the order they appear.
func (e *Expr) Refs() []Ref {
	var refs []Ref
	visit(e.root, func(n node) {
		if r, ok := n.(*ref); ok {
			path := make([]string, len(r.path))
			for i, s := range r.path {
				path[i] = s.name
				if s.kind == index {
					path[i] = "[index]"
				}
			}
			refs = append(refs, Ref{Offset: r.at, Context: r.context, Path: path})
		}
	})
	return refs
}

// Relative reports whether the expression has a relative reference, such
// as .hidden.
func (e *Expr) Relative() bool {
	found := false
	visit(e.root, func(n node) {
		if _, ok := n.(*relative); ok {
			found = true
		}
	})
	return found
}

// StatusChecks reports whether an expression calls a status function, such
// as failure(), and whether one it calls can be true once its job is
// cancelled: always(), cancelled(), or a negated success() or failure().
// A job or step whose if: can be true after a cancel keeps running through
// it.
func (e *Expr) StatusChecks() (used, afterCancel bool) {
	var walk func(n node, negated bool)
	walk = func(n node, negated bool) {
		switch n := n.(type) {
		case *call:
			if _, ok := statusFunctions[n.name]; ok {
				used = true
				switch n.name {
				case "always", "cancelled", "canceled":
					afterCancel = afterCancel || !negated
				default:
					afterCancel = afterCancel || negated
				}
				return
			}
			for _, a := range n.args {
				walk(a, negated)
			}
		case *unary:
			walk(n.x, negated != (n.op == "!"))
		default:
			for _, c := range children(n) {
				walk(c, negated)
			}
		}
	}
	walk(e.root, false)
	return used, afterCancel
}

// A Problem is something in an expression that is probably a mistake,
// though the expression means something.
type Problem struct {
	// Offset is the byte offset in the expression of the problem.
	Offset int
	Msg    string
}

// Check returns the expression's probable mistakes: words that look like
// references but are text, because they don't start with a context.
func (e *Expr) Check() []Problem {
	var problems []Problem
	visit(e.root, func(n node) {
		w, ok := n.(*word)
		if !ok {
			return
		}
		i := strings.IndexAny(w.text, ".[")
		if i <= 0 || !identifier(w.text[:i]) {
			return
		}
		problems = append(problems, Problem{
			Offset: w.at,
			Msg: fmt.Sprintf("%s is the text %q, because %s isn't a context; quote it if text is what you mean",
				w.text, w.text, w.text[:i]),
		})
	})
	return problems
}

// CheckCondition returns the probable mistakes of an expression that is a
// condition, such as a job's if:, as well as those Check returns: a word
// that decides the condition is never false, so ${{ !completed }} is always
// false. always, the one if: keyword, is not a mistake.
func (e *Expr) CheckCondition() []Problem {
	problems := e.Check()
	var walk func(n node, top bool)
	walk = func(n node, top bool) {
		switch n := n.(type) {
		case *word:
			if top && n.text == "always" {
				return
			}
			problems = append(problems, Problem{
				Offset: n.at,
				Msg: fmt.Sprintf("%s is the text %q, which is always true; a condition needs a comparison, or a status function such as failure()",
					n.text, n.text),
			})
		case *unary:
			if n.op == "!" {
				walk(n.x, false)
			}
		case *binary:
			if n.op == "&&" || n.op == "||" {
				walk(n.x, false)
				walk(n.y, top)
			}
		case *ternary:
			walk(n.cond, false)
			walk(n.then, top)
			walk(n.els, top)
		}
	}
	walk(e.root, true)
	return problems
}

// identifier reports whether s is a name such as inputs or my_var: a letter
// or underscore, then letters, digits and underscores.
func identifier(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !isLetter(c) && c != '_' && (i == 0 || !isDigit(c)) {
			return false
		}
	}
	return s != ""
}

// visit calls f for n and everything under it.
func visit(n node, f func(node)) {
	f(n)
	for _, c := range children(n) {
		visit(c, f)
	}
}

func children(n node) []node {
	switch n := n.(type) {
	case *unary:
		return []node{n.x}
	case *binary:
		return []node{n.x, n.y}
	case *ternary:
		return []node{n.cond, n.then, n.els}
	case *member:
		return []node{n.x}
	case *call:
		return n.args
	case *slice:
		if n.end == nil {
			return []node{n.start}
		}
		return []node{n.start, n.end}
	case *concat:
		return []node{n.x}
	}
	return nil
}
