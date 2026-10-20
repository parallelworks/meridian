// Package conformancetest holds what Meridian's tests share for running the
// conformance suite.
package conformancetest

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/problems"
)

// CheckDiagnostics checks that got has each diagnostic a case expects, and
// no others. One expected without a line matches at any position.
func CheckDiagnostics(t testing.TB, want []conformance.Diagnostic, got problems.Diagnostics) {
	t.Helper()
	unmatched := slices.Clone(got)
	for _, w := range want {
		i := slices.IndexFunc(unmatched, func(d problems.Diagnostic) bool {
			return d.Severity().String() == w.Severity && string(d.Code) == w.Code && d.Path() == w.Path &&
				(w.Line == 0 || d.Position() == problems.Position{Line: w.Line, Column: w.Column})
		})
		if i < 0 {
			t.Errorf("missing %s %s at %q, line %d, column %d; got %v", w.Severity, w.Code, w.Path, w.Line, w.Column, got)
			continue
		}
		unmatched = slices.Delete(unmatched, i, i+1)
	}
	for _, d := range unmatched {
		t.Errorf("unexpected %s (path %q)", d, d.Path())
	}
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
// compare equal.
func Canonical(t testing.TB, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("%#v: %v", v, err)
	}
	return string(b)
}
