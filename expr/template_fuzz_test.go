package expr_test

import (
	"errors"
	"fmt"
	"testing"
	"unicode/utf8"

	"github.com/parallelworks/meridian/expr"
)

// FuzzTemplate checks that every offset an expression or a template reports,
// of an error, a probable mistake or a reference, is in its source; and that
// a template has the same value, or the same error, after a trip through
// JSON, as does one parsed for a list's items, whose value is the
// template's with those items as the scope's Index. FuzzExpression checks
// that the JSON itself survives the trip.
func FuzzTemplate(f *testing.F) {
	for _, seed := range []string{
		"a ${{ inputs.a }} b", "${{ inputs.x[1:3] join ',' }}", "${{ 'x' }}${{ 1 + 2 }}", "${{ inputs.pw-user }}",
		"${{ always }}", "${{ inputs.l[index].n }}-${{ inputs.l.[index].n }}", "${{ fromJSON('{') }}", "${{ inputs.a ? }}",
		"x ${{ inputs.l.*.n }} ${{ format('{0}', inputs.b) }}", "${{ 'a' - 1 }}", "${{ map(inputs.x, v => v * 2) }}", "${{",
	} {
		f.Add(seed)
	}
	scope := expr.Scope{
		Contexts: map[string]any{"inputs": map[string]any{
			"a": "x", "b": false, "x": []any{1, 2, 3}, "l": []any{map[string]any{"n": "first"}, map[string]any{"n": "second"}},
		}},
		Status: &expr.Status{},
		Index:  []int{1},
	}
	f.Fuzz(func(t *testing.T, src string) {
		if e, err := expr.Parse(src); err == nil {
			for _, p := range append(e.Check(), e.CheckCondition()...) {
				inSource(t, src, "a probable mistake", p.Offset)
			}
			for _, r := range e.Refs() {
				inSource(t, src, "a reference", r.Offset)
			}
		} else {
			inSource(t, src, "an error", errorOffset(t, err))
		}

		tmpl, err := expr.ParseTemplate(src)
		if err != nil {
			inSource(t, src, "an error", errorOffset(t, err))
			return
		}
		for at, e := range tmpl.Exprs() {
			inSource(t, src, "an expression", at)
			for _, p := range append(e.Check(), e.CheckCondition()...) {
				inSource(t, src, "a probable mistake", at+p.Offset)
			}
			for _, r := range e.Refs() {
				inSource(t, src, "a reference", at+r.Offset)
			}
		}
		want, wantErr := tmpl.Eval(scope)
		if wantErr != nil && !expr.IsUnknown(wantErr) {
			inSource(t, src, "an error", errorOffset(t, wantErr))
		}
		// JSON holds only UTF-8, and writes each byte of invalid UTF-8 as
		// U+FFFD, which can change the value.
		if !utf8.ValidString(src) {
			return
		}
		sameAfterJSON(t, src, tmpl, scope, want, wantErr)
		if forItems, err := expr.ParseTemplateFor(src, scope.Index); err == nil {
			noIndex := scope
			noIndex.Index = nil
			sameAfterJSON(t, src, forItems, noIndex, want, wantErr)
		}
	})
}

// sameAfterJSON checks that tmpl, encoded as JSON and decoded, has the value
// want in scope, or fails with wantErr.
func sameAfterJSON(t *testing.T, src string, tmpl *expr.Template, scope expr.Scope, want any, wantErr error) {
	t.Helper()
	data, err := tmpl.MarshalJSON()
	if err != nil {
		t.Fatalf("%q: %v", src, err)
	}
	var back expr.Template
	if err := back.UnmarshalJSON(data); err != nil {
		t.Fatalf("%q: %s: %v", src, data, err)
	}
	got, err := back.Eval(scope)
	// %#v shows NaN as NaN, and a mapping's keys in order.
	if fmt.Sprint(err) != fmt.Sprint(wantErr) || fmt.Sprintf("%#v", got) != fmt.Sprintf("%#v", want) {
		t.Fatalf("%q: after JSON, %#v, %v; want %#v, %v", src, got, err, want, wantErr)
	}
}

// errorOffset returns the offset of a syntax or evaluation error, and fails
// the test for any other error.
func errorOffset(t *testing.T, err error) int {
	t.Helper()
	if e, ok := errors.AsType[*expr.SyntaxError](err); ok {
		return e.Offset
	}
	if e, ok := errors.AsType[*expr.EvalError](err); ok {
		return e.Offset
	}
	if _, ok := errors.AsType[*expr.BudgetError](err); ok {
		return 0
	}
	t.Fatalf("an error that is neither a syntax nor an evaluation error: %v", err)
	return 0
}

// inSource checks that offset is a byte offset in src: one of its bytes, or
// its end.
func inSource(t *testing.T, src, what string, offset int) {
	t.Helper()
	if offset < 0 || offset > len(src) {
		t.Fatalf("%q: %s at %d, outside its %d bytes", src, what, offset, len(src))
	}
}
