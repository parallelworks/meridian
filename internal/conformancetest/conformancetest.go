// Package conformancetest holds what Meridian's tests share for running the
// conformance suite.
package conformancetest

import (
	jsonv2 "encoding/json/v2"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/problems"
)

// CheckDiagnostics checks that got has each diagnostic a case expects, and
// no others. One expected without a line matches on any line, one without a
// column in any column, and one matches whatever values the params it
// doesn't list have. Each expected diagnostic is paired with one of got,
// and the pairing is the largest there is, so that one that says less
// never takes the diagnostic another needs.
func CheckDiagnostics(t testing.TB, want []conformance.Diagnostic, got problems.Diagnostics) {
	t.Helper()
	fits := make([][]bool, len(want))
	for i, w := range want {
		fits[i] = make([]bool, len(got))
		for j, d := range got {
			fits[i][j] = matches(t, w, d)
		}
	}
	pairedWith := pair(fits, len(got))
	paired := make([]bool, len(got))
	for _, j := range pairedWith {
		if j >= 0 {
			paired[j] = true
		}
	}
	for i, w := range want {
		if pairedWith[i] < 0 {
			t.Errorf("missing %s %s at %q, line %s, column %s, params %s; got:\n%s",
				w.Severity, w.Code, w.Path, optional(w.Line), optional(w.Column), Canonical(t, w.Params), describe(t, got))
		}
	}
	for j, d := range got {
		if !paired[j] {
			t.Errorf("unexpected %s (path %q, params %s)", d, d.Path(), Canonical(t, d.Params))
		}
	}
}

// matches reports whether d is a diagnostic w expects.
func matches(t testing.TB, w conformance.Diagnostic, d problems.Diagnostic) bool {
	pos := d.Position()
	return d.Severity().String() == w.Severity && string(d.Code) == w.Code && d.Path() == w.Path &&
		(w.Line == nil || pos.Line == *w.Line) && (w.Column == nil || pos.Column == *w.Column) &&
		hasParams(t, d, w.Params)
}

// pair pairs each of the expected diagnostics with one of n that fits it,
// fits[i][j] saying whether the jth fits the ith, as many as can be. It
// returns the index each is paired with, or -1. It finds the largest
// pairing by augmenting paths, which for a case's few diagnostics is
// quick.
func pair(fits [][]bool, n int) []int {
	pairedWith := make([]int, len(fits))
	owner := make([]int, n)
	for j := range owner {
		owner[j] = -1
	}
	var augment func(i int, seen []bool) bool
	augment = func(i int, seen []bool) bool {
		for j, ok := range fits[i] {
			if !ok || seen[j] {
				continue
			}
			seen[j] = true
			if owner[j] < 0 || augment(owner[j], seen) {
				owner[j] = i
				return true
			}
		}
		return false
	}
	for i := range fits {
		augment(i, make([]bool, n))
	}
	for i := range pairedWith {
		pairedWith[i] = -1
	}
	for j, i := range owner {
		if i >= 0 {
			pairedWith[i] = j
		}
	}
	return pairedWith
}

// optional shows a line or column a case may leave out.
func optional(n *int) string {
	if n == nil {
		return "any"
	}
	return strconv.Itoa(*n)
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
