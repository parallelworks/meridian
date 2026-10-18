package format_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/format"
)

func TestConformance(t *testing.T) {
	cases, err := conformance.Cases("format")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(strings.TrimPrefix(c.Name, "format/"), func(t *testing.T) {
			t.Parallel()
			data, err := c.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			doc, diags := format.Load("workflow.yaml", data)

			// Each expected diagnostic matches one that Load reported. One
			// without a line matches at any position.
			unmatched := slices.Clone(diags)
			for _, want := range c.Expect.Diagnostics {
				i := slices.IndexFunc(unmatched, func(d format.Diagnostic) bool {
					return d.Severity.String() == want.Severity && string(d.Code) == want.Code && d.Path == want.Path &&
						(want.Line == 0 || d.Position == format.Position{Line: want.Line, Column: want.Column})
				})
				if i < 0 {
					t.Errorf("missing %s %s at %q, line %d, column %d", want.Severity, want.Code, want.Path, want.Line, want.Column)
					continue
				}
				unmatched = slices.Delete(unmatched, i, i+1)
			}
			for _, d := range unmatched {
				t.Errorf("unexpected %s (path %q)", d, d.Path)
			}
			if (doc == nil) != diags.HasErrors() {
				t.Errorf("document is %v with errors %v", doc, diags.HasErrors())
			}

			for pointer, want := range c.Expect.Values {
				if doc == nil {
					t.Errorf("%s: no document", pointer)
					continue
				}
				got, ok := valueAt(doc.Value, pointer)
				if !ok {
					t.Errorf("%s: no value", pointer)
					continue
				}
				if g, w := canonical(t, got), canonical(t, want); g != w {
					t.Errorf("%s = %s, want %s", pointer, g, w)
				}
			}
		})
	}
}

// valueAt returns the value at a JSON Pointer.
func valueAt(v any, pointer string) (any, bool) {
	if pointer == "" {
		return v, true
	}
	unescape := strings.NewReplacer("~1", "/", "~0", "~")
	for tok := range strings.SplitSeq(strings.TrimPrefix(pointer, "/"), "/") {
		tok = unescape.Replace(tok)
		switch c := v.(type) {
		case map[string]any:
			var ok bool
			if v, ok = c[tok]; !ok {
				return nil, false
			}
		case []any:
			var i int
			if _, err := fmt.Sscan(tok, &i); err != nil || i < 0 || i >= len(c) {
				return nil, false
			}
			v = c[i]
		default:
			return nil, false
		}
	}
	return v, true
}

// canonical encodes a value as JSON, so that values decoded by different
// means compare equal.
func canonical(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
