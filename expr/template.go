package expr

import (
	"errors"
	"iter"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/problems"
)

// A Template is a string in a workflow, with the expressions in it parsed.
type Template struct {
	src   string
	parts []part
}

// A part of a template is text, or an expression.
type part struct {
	text string
	expr *Expr
	// at is the byte offset in the template of the expression's source,
	// just after its ${{.
	at int
}

// ParseTemplate parses the expressions in a string: each ${{ }}. A }} in
// quotes or brackets doesn't close an expression, so ${{ ['}}'] }} is one
// expression. A ${{ that isn't closed is a syntax error; a template writes
// ${{ as text with ${{ '${{' }}.
func ParseTemplate(s string) (*Template, error) {
	t := &Template{src: s}
	for rest, offset := s, 0; ; {
		start := strings.Index(rest, "${{")
		if start < 0 {
			t.addText(rest)
			return t, nil
		}
		end := closing(rest[start+3:])
		if end < 0 {
			return nil, &SyntaxError{Offset: offset + start, Kind: problems.UnclosedTemplate}
		}
		t.addText(rest[:start])
		src := rest[start+3 : start+3+end]
		at := offset + start + 3
		e, err := Parse(src)
		if err != nil {
			if se, ok := errors.AsType[*SyntaxError](err); ok {
				se.Offset += at
			}
			return nil, err
		}
		t.parts = append(t.parts, part{expr: e, at: at})
		consumed := start + 3 + end + 2
		rest, offset = rest[consumed:], offset+consumed
	}
}

// ParseTemplateFor parses s as ParseTemplate does, for the list items at
// index, outermost first: each [index] in a path reads one of them, as it
// would with index as a Scope's Index. The template keeps them where no
// scope gives them, such as in a plan.
func ParseTemplateFor(s string, index []int) (*Template, error) {
	t, err := ParseTemplate(s)
	if err != nil || len(index) == 0 {
		return t, err
	}
	for _, p := range t.parts {
		if p.expr != nil {
			visit(p.expr.root, func(n node) { bindIndex(n, index) })
		}
	}
	return t, nil
}

// bindIndex replaces each [index] in n's path with the index it reads. An
// [index] past the end of indexes stays one.
func bindIndex(n node, indexes []int) {
	var path []segment
	switch n := n.(type) {
	case *ref:
		path = n.path
	case *relative:
		path = n.path
	case *member:
		path = n.path
	case *param:
		path = n.path
	}
	k := 0
	for i, s := range path {
		if s.kind != index || k >= len(indexes) {
			continue
		}
		path[i] = segment{name: strconv.Itoa(indexes[k]), kind: field}
		k++
	}
}

func (t *Template) addText(s string) {
	if s != "" {
		t.parts = append(t.parts, part{text: s})
	}
}

// closing returns the index of the }} that closes an expression in s, or -1
// if there is none. A }} in quotes or brackets doesn't count.
func closing(s string) int {
	var quote, open byte
	depth := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"' || c == '`':
			quote = c
		case depth == 0:
			switch c {
			case '(', '[', '{':
				open, depth = c, 1
			case '}':
				if i+1 < len(s) && s[i+1] == '}' {
					return i
				}
			}
		default:
			// Only the kind of bracket that opened is counted.
			switch c {
			case open:
				depth++
			case closer(open):
				depth--
			}
		}
	}
	return -1
}

func closer(open byte) byte {
	switch open {
	case '(':
		return ')'
	case '[':
		return ']'
	}
	return '}'
}

// String returns the template's source.
func (t *Template) String() string { return t.src }

// Exprs returns an iterator over the template's expressions, each with the
// byte offset in the template of its source.
func (t *Template) Exprs() iter.Seq2[int, *Expr] {
	return func(yield func(int, *Expr) bool) {
		for _, p := range t.parts {
			if p.expr != nil && !yield(p.at, p.expr) {
				return
			}
		}
	}
}

// Expr returns the template's expression if the template is one expression
// and nothing else, such as ${{ inputs.x }}, whose value is the
// expression's, of whatever type.
func (t *Template) Expr() (*Expr, bool) {
	if len(t.parts) == 1 && t.parts[0].expr != nil {
		return t.parts[0].expr, true
	}
	return nil, false
}

// IsText reports whether the template has no expressions.
func (t *Template) IsText() bool {
	for _, p := range t.parts {
		if p.expr != nil {
			return false
		}
	}
	return true
}

// Eval evaluates the template. A template that is one expression and
// nothing else has the expression's value, of whatever type. Otherwise its
// value is a string, with each expression replaced by its value's text: a
// string as it is, null as nothing, and anything else as JSON.
//
// The template's expressions share the language's limits on what a
// template builds and the work it does, which count each value's text here
// but not the text around it. The limits are the template's own: a template
// it reads, such as an input's default, has its own.
func (t *Template) Eval(scope Scope) (any, error) {
	return t.eval(scope, nil)
}

// eval evaluates the template, and calls part, if it isn't nil, with the
// value of each of its expressions and what it read of the values scope
// marks, which scope's budget notes.
func (t *Template) eval(scope Scope, part func(v any, read Read)) (any, error) {
	if part != nil {
		scope = scope.Budgeted()
	}
	if len(t.parts) == 1 && t.parts[0].expr != nil {
		var outer Read
		if part != nil {
			outer = scope.budget.noting()
		}
		v, err := t.parts[0].expr.Eval(scope)
		if part != nil {
			if read := scope.budget.done(outer); err == nil {
				part(v, read)
			}
		}
		return v, offsetError(err, t.parts[0].at)
	}
	if t.IsText() {
		// Text evaluates nothing, and is its value.
		return t.src, nil
	}
	ev, err := newEvaluator(scope)
	if err != nil {
		return nil, err
	}
	defer ev.stop()
	var b strings.Builder
	for _, p := range t.parts {
		if p.expr == nil {
			b.WriteString(p.text)
			continue
		}
		var outer Read
		if part != nil {
			outer = ev.scope.budget.noting()
		}
		v, err := ev.eval(p.expr.root)
		if part != nil {
			if read := ev.scope.budget.done(outer); err == nil {
				part(v, read)
			}
		}
		if err != nil {
			return nil, offsetError(err, p.at)
		}
		s := stringOf(v)
		if err := ev.spend(p.at, len(s)); err != nil {
			return nil, offsetError(err, p.at)
		}
		b.WriteString(s)
	}
	return b.String(), nil
}

// EvalMarked evaluates the template as Eval does, and reports whether the
// evaluation read a value that scope marks (Scope.Marked), or that a
// template it read, such as an input's default, read. Whatever it gives is
// then made from the marked value, as ${{ vars.token * 2 }} is, or decided
// by it, as a condition is, so a runner that masks the marked value masks
// what it gives too. A failed evaluation is marked as well, so that what
// reports the error can leave out the values it shows.
func (t *Template) EvalMarked(scope Scope) (v any, marked bool, err error) {
	v, read, err := t.EvalRead(scope)
	return v, read.Marked, err
}

// offsetError moves an error's offset from its expression to the template.
func offsetError(err error, at int) error {
	if ee, ok := errors.AsType[*EvalError](err); ok {
		if ee.Location != "" {
			return ee // in another string
		}
		c := *ee
		c.Offset += at
		return &c
	}
	if ue, ok := errors.AsType[*UnknownError](err); ok {
		return &UnknownError{Offset: ue.Offset + at, Name: ue.Name}
	}
	return err
}
