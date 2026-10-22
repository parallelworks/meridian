package conformancetest

import (
	"fmt"
	"testing"

	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/problems"
)

// recorder records the errors a check reports, rather than failing.
type recorder struct {
	testing.TB
	errors []string
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

func TestCheckDiagnostics(t *testing.T) {
	unknown := func(key string, line, column int) problems.Diagnostic {
		return problems.UnknownKey.At("workflow.yaml", problems.Position{Line: line, Column: column}, "/a",
			problems.WithSuggestion(problems.Params{"key": key}, "", false))
	}
	want := func(line, column *int, params map[string]any) conformance.Diagnostic {
		return conformance.Diagnostic{Severity: "warning", Code: "unknown_key", Path: "/a", Line: line, Column: column, Params: params}
	}
	for _, tc := range []struct {
		name   string
		want   []conformance.Diagnostic
		got    problems.Diagnostics
		errors int
	}{
		{"none", nil, nil, 0},
		{"any line", []conformance.Diagnostic{want(nil, nil, nil)}, problems.Diagnostics{unknown("x", 5, 3)}, 0},
		{"a line, any column", []conformance.Diagnostic{want(new(5), nil, nil)}, problems.Diagnostics{unknown("x", 5, 3)}, 0},
		{"another line", []conformance.Diagnostic{want(new(4), nil, nil)}, problems.Diagnostics{unknown("x", 5, 3)}, 2},
		{"no column", []conformance.Diagnostic{want(new(5), new(0), nil)}, problems.Diagnostics{unknown("x", 5, 0)}, 0},
		{"a column where there is none", []conformance.Diagnostic{want(new(5), new(1), nil)}, problems.Diagnostics{unknown("x", 5, 0)}, 2},
		{"a column, where none is expected", []conformance.Diagnostic{want(new(5), new(0), nil)}, problems.Diagnostics{unknown("x", 5, 3)}, 2},
		{"params", []conformance.Diagnostic{want(nil, nil, map[string]any{"key": "x", "suggested": "false"})}, problems.Diagnostics{unknown("x", 5, 3)}, 0},
		{"other params", []conformance.Diagnostic{want(nil, nil, map[string]any{"key": "y"})}, problems.Diagnostics{unknown("x", 5, 3)}, 2},
		{"missing", []conformance.Diagnostic{want(nil, nil, nil), want(nil, nil, nil)}, problems.Diagnostics{unknown("x", 5, 3)}, 1},
		{"unexpected", nil, problems.Diagnostics{unknown("x", 5, 3)}, 1},
		// The first expected one fits both diagnostics, and the second only
		// the first: they pair the other way around.
		{
			"pairs that say as much",
			[]conformance.Diagnostic{want(new(5), nil, nil), want(nil, nil, map[string]any{"key": "x"})},
			problems.Diagnostics{unknown("x", 5, 3), unknown("y", 5, 7)},
			0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &recorder{TB: t}
			CheckDiagnostics(r, tc.want, tc.got)
			if len(r.errors) != tc.errors {
				t.Errorf("%d errors, want %d: %q", len(r.errors), tc.errors, r.errors)
			}
		})
	}
}
