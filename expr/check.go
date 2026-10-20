package expr

import (
	"strings"

	"github.com/parallelworks/meridian/problems"
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

// Refs returns the contexts an expression reads, in the order they appear,
// in the bodies of its lambdas too.
func (e *Expr) Refs() []Ref {
	var refs []Ref
	visit(e.root, func(n node) {
		if r, ok := n.(*ref); ok {
			refs = append(refs, Ref{Offset: r.at, Context: r.context, Path: refPath(r.path)})
		}
	})
	return refs
}

// refPath returns a reference's path as a Ref holds it.
func refPath(path []segment) []string {
	out := make([]string, len(path))
	for i, s := range path {
		out[i] = s.name
		if s.kind == index {
			out[i] = "[index]"
		}
	}
	return out
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
	// Kind is the kind of problem, and Params the values its message shows.
	Kind   *problems.Kind
	Params problems.Params
	// Meant, if set, is what the expression probably meant to read, up to
	// the name the mistake split: inputs with the path pw-user for
	// inputs.pw-user. Whoever knows the workflow can say whether it has one,
	// which the params defines, defined and job say: see
	// problems.HyphenatedName.
	Meant *Ref
}

// Message returns the problem's message, in English.
func (p Problem) Message() string { return p.Kind.Message(p.Params) }

// Check returns the expression's probable mistakes: words that look like
// references but are text, because they don't start with a context or a
// lambda's parameter, and a reference or a parameter's path that subtracts
// a word, such as inputs.pw-user, which is NaN and probably meant a name
// with a hyphen in it. The bodies of lambdas are checked as the rest is.
func (e *Expr) Check() []Problem {
	var found []Problem
	visit(e.root, func(n node) {
		switch n := n.(type) {
		case *binary:
			if p, ok := e.hyphenated(n); ok {
				found = append(found, p)
			}
		case *word:
			i := strings.IndexAny(n.text, ".[")
			if i <= 0 || !identifier(n.text[:i]) {
				return
			}
			found = append(found, Problem{
				Offset: n.at,
				Kind:   problems.WordNotReference,
				Params: problems.Params{"word": n.text, "text": problems.Show(n.text), "head": n.text[:i]},
			})
		}
	})
	return found
}

// hyphenated returns the problem with n if it subtracts a word from a
// reference with nothing between them, as inputs.pw-user does: the text
// "user" from inputs.pw, which is NaN. A name with a hyphen in it is read in
// brackets, inputs['pw-user'], which the problem suggests. A lambda's
// parameter with a path, as in h.pw-user, has the same problem, without a
// context it meant. A decoded tree has no offsets to tell whether the - had
// spaces, so it has no such problem.
func (e *Expr) hyphenated(n *binary) (Problem, bool) {
	var at int
	var path []segment
	var r *ref
	switch x := n.x.(type) {
	case *ref:
		at, path, r = x.at, x.path, x
	case *param:
		at, path = x.at, x.path
	}
	w, isWord := n.y.(*word)
	if n.op != "-" || len(path) == 0 || !isWord || n.at == 0 || n.at >= len(e.src) || e.src[n.at] != '-' ||
		at >= n.at || w.at != n.at+1 {
		return Problem{}, false
	}
	written := e.src[at:n.at]
	last := path[len(path)-1]
	before, isName := strings.CutSuffix(written, "."+last.name)
	if last.kind != field || !isName {
		return Problem{}, false
	}
	head, rest := w.text, ""
	if i := strings.IndexAny(w.text, ".["); i > 0 {
		head, rest = w.text[:i], w.text[i:]
	}
	name := last.name + "-" + head
	p := Problem{
		Offset: at,
		Kind:   problems.HyphenatedName,
		Params: problems.Params{
			"written": written, "word": w.text, "text": problems.Show(w.text), "name": name,
			"fix": before + "['" + name + "']" + rest, "defines": "none", "defined": "", "job": "",
		},
	}
	if r != nil {
		meant := refPath(r.path)
		meant[len(meant)-1] = name
		p.Meant = &Ref{Offset: at, Context: r.context, Path: meant}
	}
	return p, true
}

// replacedKeywords are the if: keywords ACTIVATE once had, and what replaced
// each. They are words now, so they decide nothing.
var replacedKeywords = map[string]string{
	"completed": "success()", "success": "success()", "error": "failure()", "failure": "failure()", "failed": "failure()",
	"canceled": "cancelled()", "cancelled": "cancelled()", "never": "false",
}

// CheckCondition returns the probable mistakes of an expression that is a
// condition, such as a job's if:, as well as those Check returns: a word
// that decides the condition is never false, so ${{ !completed }} is always
// false. The word always, whose value as a condition means always(), is the
// older spelling of always().
func (e *Expr) CheckCondition() []Problem {
	found := e.Check()
	var walk func(n node, top bool)
	walk = func(n node, top bool) {
		switch n := n.(type) {
		case *word:
			p := Problem{
				Offset: n.at,
				Kind:   problems.ConditionWord,
				Params: problems.Params{"word": n.text, "text": problems.Show(n.text)},
			}
			switch replacement, replaced := replacedKeywords[n.text]; {
			case top && n.text == "always":
				p.Kind, p.Params = problems.OlderAlways, nil
			case replaced:
				p.Kind, p.Params["replacement"] = problems.ReplacedKeyword, replacement
			}
			found = append(found, p)
		case *unary:
			if n.op == "!" {
				walk(n.x, false)
			}
		case *binary:
			// A word is never false, so the left of || is the value, and the
			// left of && decides it.
			switch n.op {
			case "&&":
				walk(n.x, false)
				walk(n.y, top)
			case "||":
				walk(n.x, top)
				walk(n.y, top)
			}
		case *ternary:
			walk(n.cond, false)
			walk(n.then, top)
			walk(n.els, top)
		}
	}
	walk(e.root, true)
	return found
}

// identifier reports whether s is a name such as inputs or my_var: a letter
// or underscore, then letters, digits and underscores.
func identifier(s string) bool {
	for i := 0; i < len(s); i++ {
		if !isNameByte(s[i]) || i == 0 && isDigit(s[i]) {
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
	case *lambda:
		return []node{n.body}
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
