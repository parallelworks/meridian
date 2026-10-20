package format_test

import (
	"strings"
	"testing"

	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/conformancetest"
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
			conformancetest.CheckDiagnostics(t, c.Expect.Diagnostics, diags)
			if (doc == nil) != diags.HasErrors() {
				t.Errorf("document is %v with errors %v", doc, diags.HasErrors())
			}

			for pointer, want := range c.Expect.Values {
				if doc == nil {
					t.Errorf("%s: no document", pointer)
					continue
				}
				got, ok := conformancetest.Lookup(t, doc.Value, pointer)
				if !ok {
					t.Errorf("%s: no value", pointer)
					continue
				}
				if g, w := conformancetest.Canonical(t, got), conformancetest.Canonical(t, want); g != w {
					t.Errorf("%s = %s, want %s", pointer, g, w)
				}
			}
		})
	}
}
