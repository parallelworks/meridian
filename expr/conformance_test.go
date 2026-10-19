package expr_test

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/expr"
)

func TestConformance(t *testing.T) {
	files, err := conformance.ExpressionFiles()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		t.Run(strings.TrimSuffix(strings.TrimPrefix(f.Name, "expressions/"), ".yaml"), func(t *testing.T) {
			t.Parallel()
			for i, c := range f.Cases {
				name := cmpOr(c.Expr, c.Template, c.At)
				t.Run(fmt.Sprintf("%d %s", i+1, name), func(t *testing.T) {
					runCase(t, &f, &c)
				})
			}
		})
	}
}

func runCase(t *testing.T, f *conformance.ExpressionFile, c *conformance.ExpressionCase) {
	s := f.ScopeFor(c)
	scope := expr.Scope{Contexts: s.Contexts, Index: s.Index}
	if s.Status != nil {
		scope.Status = &expr.Status{Failed: s.Status.Failed, Cancelled: s.Status.Cancelled}
	}

	src, location := c.Template, []string(nil)
	if c.At != "" {
		location = pointerPath(c.At)
		v, ok := lookup(f.Workflow, location)
		s, isString := v.(string)
		if !ok || !isString {
			t.Fatalf("%s isn't a string in the workflow", c.At)
		}
		src = s
	}
	if f.Workflow != nil {
		doc := &expr.Document{Root: f.Workflow, Definitions: []string{"on", "execute", "inputs"}}
		scope = doc.Scope(location, scope)
	}

	var got any
	var err error
	if c.Expr != "" {
		var e *expr.Expr
		if e, err = expr.Parse(c.Expr); err == nil {
			checkWarnings(t, c, e)
			got, err = e.Eval(scope)
		}
	} else {
		var tmpl *expr.Template
		if tmpl, err = expr.ParseTemplate(src); err == nil {
			got, err = tmpl.Eval(scope)
		}
	}

	var syntaxErr *expr.SyntaxError
	var evalErr *expr.EvalError
	var unknownErr *expr.UnknownError
	switch {
	case c.Error == "syntax":
		if !errors.As(err, &syntaxErr) {
			t.Fatalf("got %v, %v; want a syntax error", show(got), err)
		}
	case c.Error == "eval":
		if !errors.As(err, &evalErr) {
			t.Fatalf("got %v, %v; want an evaluation error", show(got), err)
		}
	case c.Error != "":
		t.Fatalf("unknown error kind %q", c.Error)
	case c.Unknown != "":
		if !errors.As(err, &unknownErr) || unknownErr.Name != c.Unknown {
			t.Fatalf("got %v, %v; want %s to be unknown", show(got), err, c.Unknown)
		}
	case err != nil:
		t.Fatalf("got error %v; want %s", err, show(c.Value))
	case !equal(got, c.Value):
		t.Fatalf("got %s, want %s", show(got), show(c.Value))
	}
}

// checkWarnings checks the probable mistakes the case's expression has, if
// the case lists them.
func checkWarnings(t *testing.T, c *conformance.ExpressionCase, e *expr.Expr) {
	t.Helper()
	if c.Warnings == nil {
		return
	}
	problems := e.Check()
	if c.Condition {
		problems = e.CheckCondition()
	}
	var offsets []int
	for _, p := range problems {
		offsets = append(offsets, p.Offset)
	}
	slices.Sort(offsets)
	if !slices.Equal(offsets, c.Warnings) {
		t.Errorf("warnings at %v (%v), want at %v", offsets, problems, c.Warnings)
	}
}

func cmpOr(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

// equal compares values as JSON does, with numbers compared by value, and
// NaN equal to NaN.
func equal(a, b any) bool {
	an, aNum := toFloat(a)
	bn, bNum := toFloat(b)
	if aNum || bNum {
		return aNum && bNum && (an == bn || math.IsNaN(an) && math.IsNaN(bn))
	}
	switch a := a.(type) {
	case []any:
		b, ok := b.([]any)
		return ok && slices.EqualFunc(a, b, equal)
	case map[string]any:
		b, ok := b.(map[string]any)
		if !ok || len(a) != len(b) {
			return false
		}
		for k, v := range a {
			w, ok := b[k]
			if !ok || !equal(v, w) {
				return false
			}
		}
		return true
	}
	return a == b
}

func toFloat(v any) (float64, bool) {
	switch v := v.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	}
	return 0, false
}

func show(v any) string {
	if s, ok := v.(string); ok {
		return strconv.Quote(s)
	}
	return fmt.Sprintf("%#v", v)
}

func pointerPath(p string) []string {
	if p == "" {
		return nil
	}
	unescape := strings.NewReplacer("~1", "/", "~0", "~")
	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	for i, part := range parts {
		parts[i] = unescape.Replace(part)
	}
	return parts
}

func lookup(v any, path []string) (any, bool) {
	for _, k := range path {
		switch c := v.(type) {
		case map[string]any:
			var ok bool
			if v, ok = c[k]; !ok {
				return nil, false
			}
		case []any:
			i, err := strconv.Atoi(k)
			if err != nil || i < 0 || i >= len(c) {
				return nil, false
			}
			v = c[i]
		default:
			return nil, false
		}
	}
	return v, true
}
