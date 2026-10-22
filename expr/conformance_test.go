package expr_test

import (
	"cmp"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"iter"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/problems"
)

func TestConformance(t *testing.T) {
	files, err := conformance.ExpressionFiles()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		t.Run(strings.TrimSuffix(strings.TrimPrefix(f.Name, "expressions/"), ".yaml"), func(t *testing.T) {
			t.Parallel()
			scope, workflow := canonical(t, f.Scope), canonical(t, f.Workflow)
			for i, c := range f.Cases {
				name := cmp.Or(c.Expr, c.Template, c.At)
				t.Run(fmt.Sprintf("%d %s", i+1, name), func(t *testing.T) {
					runCase(t, &f, &c)
				})
			}
			// The cases share the file's scope and workflow, so a case that
			// changed them would change what the cases after it see.
			if canonical(t, f.Scope) != scope || canonical(t, f.Workflow) != workflow {
				t.Error("evaluating the cases changed the file's scope or workflow")
			}
		})
	}
}

// canonical encodes v as JSON, with mappings in the order of their keys.
func canonical(t *testing.T, v any) string {
	t.Helper()
	data, err := jsonv2.Marshal(v, jsonv2.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// stopped is a clock that never moves, so that an evaluation passes
// Meridian's limit on a group's time only if it passes the others, however
// slow the machine running the tests is. No case evaluates for anywhere
// near the 10 seconds a group may take.
func stopped() time.Time { return time.Time{} }

func runCase(t *testing.T, f *conformance.ExpressionFile, c *conformance.ExpressionCase) {
	s := f.ScopeFor(c)
	src, location := c.Template, []string(nil)
	if c.At != "" {
		var err error
		if location, err = jsonptr.Split(c.At); err != nil {
			t.Fatal(err)
		}
		v, _ := jsonptr.Lookup(f.Workflow, location)
		s, isString := v.(string)
		if !isString {
			t.Fatalf("%s isn't a string in the workflow", c.At)
		}
		src = s
	}
	// newScope returns the case's scope, whose evaluations are a group of
	// their own, with a stopped clock.
	newScope := func() expr.Scope {
		scope := expr.WithClock(expr.Scope{Contexts: s.Contexts, Index: s.Index}, stopped)
		if s.Status != nil {
			scope.Status = &expr.Status{Failed: s.Status.Failed, Cancelled: s.Status.Cancelled}
		}
		if f.Workflow != nil {
			w := &expr.Workflow{Root: f.Workflow, Definitions: []string{"on", "execute", "inputs"}}
			scope = w.Scope(location, scope)
		}
		return scope
	}
	scope := newScope()

	var got any
	var err error
	if c.Expr != "" {
		var e *expr.Expr
		if e, err = expr.Parse(c.Expr); err == nil {
			checkWarnings(t, c, func(yield func(int, *expr.Expr) bool) { yield(0, e) })
			got, err = e.Eval(scope)
		}
	} else {
		var tmpl *expr.Template
		if tmpl, err = expr.ParseTemplate(src); err == nil {
			checkWarnings(t, c, tmpl.Exprs())
			got, err = tmpl.Eval(scope)
		}
	}

	switch {
	case c.Error == "syntax":
		e, ok := errors.AsType[*expr.SyntaxError](err)
		if !ok {
			t.Fatalf("got %v, %v; want a syntax error", show(got), err)
		}
		checkCode(t, c, e.Kind)
	case c.Error == "eval":
		e, ok := errors.AsType[*expr.EvalError](err)
		if !ok {
			t.Fatalf("got %v, %v; want an evaluation error", show(got), err)
		}
		checkCode(t, c, e.Kind)
	case c.Error != "":
		t.Fatalf("unknown error kind %q", c.Error)
	case c.Unknown != "":
		if u, ok := errors.AsType[*expr.UnknownError](err); !ok || u.Name != c.Unknown {
			t.Fatalf("got %v, %v; want %s to be unknown", show(got), err, c.Unknown)
		}
	case err != nil:
		t.Fatalf("got error %v; want %s", err, show(c.Value))
	case !equal(got, c.Value):
		t.Fatalf("got %s, want %s", show(got), show(c.Value))
	case len(scope.Index) > 0:
		if c.Expr != "" {
			src = "${{" + c.Expr + "}}"
		}
		// Evaluated again, in a group of its own, without the Index, though
		// what reads the workflow still has it.
		again := newScope()
		again.Index = nil
		checkParsedFor(t, src, scope.Index, again, c.Value)
	}
}

// checkParsedFor checks that src, parsed for the list items index, has the
// value want after a trip through JSON, in scope, which has no Index.
func checkParsedFor(t *testing.T, src string, index []int, scope expr.Scope, want any) {
	t.Helper()
	tmpl, err := expr.ParseTemplateFor(src, index)
	if err != nil {
		t.Fatal(err)
	}
	data, err := tmpl.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var stored expr.Template
	if err := stored.UnmarshalJSON(data); err != nil {
		t.Fatal(err)
	}
	if got, err := stored.Eval(scope); err != nil || !equal(got, want) {
		t.Errorf("parsed for %v: got %s, %v; want %s", index, show(got), err, show(want))
	}
}

// checkWarnings checks the probable mistakes of the case's expressions, each
// with its byte offset in the expression or template, if the case lists
// them.
func checkWarnings(t *testing.T, c *conformance.ExpressionCase, exprs iter.Seq2[int, *expr.Expr]) {
	t.Helper()
	if c.Warnings == nil {
		return
	}
	var found []expr.Problem
	var offsets []int
	for at, e := range exprs {
		ps := e.Check()
		if c.Condition {
			ps = e.CheckCondition()
		}
		for _, p := range ps {
			found = append(found, p)
			offsets = append(offsets, at+p.Offset)
		}
	}
	slices.Sort(offsets)
	if !slices.Equal(offsets, c.Warnings) {
		t.Errorf("warnings at %v (%v), want at %v", offsets, found, c.Warnings)
	}
}

// checkCode checks the code of an error's kind, if the case gives it.
func checkCode(t *testing.T, c *conformance.ExpressionCase, k *problems.Kind) {
	t.Helper()
	if c.Code != "" && string(k.Code()) != c.Code {
		t.Errorf("got %s, want %s", k.Code(), c.Code)
	}
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
		return ok && maps.EqualFunc(a, b, equal)
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
