// Package conformancetest holds what Meridian's tests share for running the
// conformance suite.
package conformancetest

import (
	jsonv2 "encoding/json/v2"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/problems"
)

// CheckDiagnostics checks that got has each diagnostic a case expects, and
// no others. One expected without a line matches on any line, one without a
// column in any column, and one matches whatever values the params it
// doesn't list have.
func CheckDiagnostics(t testing.TB, want []conformance.Diagnostic, got problems.Diagnostics) {
	t.Helper()
	unmatched := slices.Clone(got)
	for _, w := range want {
		i := slices.IndexFunc(unmatched, func(d problems.Diagnostic) bool {
			pos := d.Position()
			return d.Severity().String() == w.Severity && string(d.Code) == w.Code && d.Path() == w.Path &&
				(w.Line == 0 || pos.Line == w.Line) && (w.Column == 0 || pos.Column == w.Column) &&
				hasParams(t, d, w.Params)
		})
		if i < 0 {
			t.Errorf("missing %s %s at %q, line %d, column %d, params %s; got:\n%s",
				w.Severity, w.Code, w.Path, w.Line, w.Column, Canonical(t, w.Params), describe(t, got))
			continue
		}
		unmatched = slices.Delete(unmatched, i, i+1)
	}
	for _, d := range unmatched {
		t.Errorf("unexpected %s (path %q, params %s)", d, d.Path(), Canonical(t, d.Params))
	}
}

// hasParams reports whether d's params have the values want gives them.
func hasParams(t testing.TB, d problems.Diagnostic, want map[string]any) bool {
	for name, w := range want {
		g, ok := d.Params[name]
		if !ok || Canonical(t, g) != Canonical(t, w) {
			return false
		}
	}
	return true
}

// describe lists diagnostics one per line, each with its path and params.
func describe(t testing.TB, ds problems.Diagnostics) string {
	var b strings.Builder
	for _, d := range ds {
		fmt.Fprintf(&b, "\t%s (path %q, params %s)\n", d, d.Path(), Canonical(t, d.Params))
	}
	return b.String()
}

// Lookup returns the value at a JSON Pointer in v. ok is false if there is
// nothing there, and the test fails if pointer isn't a JSON Pointer.
func Lookup(t testing.TB, v any, pointer string) (any, bool) {
	t.Helper()
	tokens, err := jsonptr.Split(pointer)
	if err != nil {
		t.Fatalf("%q: %v", pointer, err)
	}
	return jsonptr.Lookup(v, tokens)
}

// Canonical encodes v as JSON, so that values decoded by different means
// compare equal. Mappings are in the order of their keys, and a nil list or
// mapping is null.
func Canonical(t testing.TB, v any) string {
	t.Helper()
	b, err := jsonv2.Marshal(v, jsonv2.Deterministic(true), jsonv2.FormatNilMapAsNull(true), jsonv2.FormatNilSliceAsNull(true))
	if err != nil {
		t.Fatalf("%#v: %v", v, err)
	}
	return string(b)
}
