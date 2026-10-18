package format_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/format"
)

const hello = `jobs:
  main:
    steps:
      - run: echo hello
`

func TestLoadSizeLimit(t *testing.T) {
	big := hello + "# " + strings.Repeat("x", format.DefaultMaxSize) + "\n"
	for _, tc := range []struct {
		name   string
		loader format.Loader
		data   string
		ok     bool
	}{
		{"default limit", format.Loader{}, big, false},
		{"under a lower limit", format.Loader{MaxSize: len(hello)}, hello, true},
		{"over a lower limit", format.Loader{MaxSize: len(hello) - 1}, hello, false},
		{"under a higher limit", format.Loader{MaxSize: 2 * format.DefaultMaxSize}, big, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc, diags := tc.loader.Load("workflow.yaml", []byte(tc.data))
			if (doc != nil) != tc.ok {
				t.Fatalf("loaded %v, want %v: %v", doc != nil, tc.ok, diags)
			}
			if !tc.ok && (len(diags) != 1 || diags[0].Code != format.CodeTooLarge) {
				t.Errorf("diagnostics = %v, want one %s", diags, format.CodeTooLarge)
			}
		})
	}
}

func TestUnknownKeysAreDropped(t *testing.T) {
	doc, diags := format.Load("workflow.yaml", []byte(`jobs:
  main:
    early-cancel: any-job-failed
    steps:
      - run: echo hello
`))
	if doc == nil || len(diags) != 1 {
		t.Fatalf("Load = %v, %v; want a document and one warning", doc, diags)
	}
	job := doc.Value["jobs"].(map[string]any)["main"].(map[string]any)
	if _, ok := job["early-cancel"]; ok {
		t.Errorf("the job still has early-cancel: %v", job)
	}
}

func TestDocumentPosition(t *testing.T) {
	doc, diags := format.Load("workflow.yaml", []byte(hello))
	if doc == nil {
		t.Fatal(diags)
	}
	if got, want := doc.Format, 1; got != want {
		t.Errorf("Format = %d, want %d", got, want)
	}
	for pointer, want := range map[string]format.Position{
		"":                       {Line: 1, Column: 1},
		"/jobs/main":             {Line: 3, Column: 5},
		"/jobs/main/steps/0/run": {Line: 4, Column: 14},
		"/jobs/other":            {},
	} {
		if got := doc.Position(pointer); got != want {
			t.Errorf("Position(%q) = %v, want %v", pointer, got, want)
		}
	}
}

func TestDiagnostics(t *testing.T) {
	warning := format.Diagnostic{
		File: "workflow.yaml", Position: format.Position{Line: 3, Column: 5},
		Severity: format.Warning, Code: format.CodeUnknownKey, Message: `unknown key "x" is ignored`,
	}
	syntax := format.Diagnostic{
		File: "workflow.yaml", Position: format.Position{Line: 7},
		Severity: format.Error, Code: format.CodeSyntax, Message: "invalid YAML: mapping values are not allowed here",
	}
	tooLarge := format.Diagnostic{Severity: format.Error, Code: format.CodeTooLarge, Message: "the file is too large"}

	for d, want := range map[*format.Diagnostic]string{
		&warning:  `workflow.yaml:3:5: warning: unknown key "x" is ignored (unknown-key)`,
		&syntax:   "workflow.yaml:7: error: invalid YAML: mapping values are not allowed here (syntax)",
		&tooLarge: "error: the file is too large (too-large)",
	} {
		if got := d.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	}

	if err := (format.Diagnostics{warning}).Err(); err != nil {
		t.Errorf("Err() with only a warning = %v, want nil", err)
	}
	err := (format.Diagnostics{warning, syntax}).Err()
	var errs format.Diagnostics
	if !errors.As(err, &errs) || len(errs) != 1 || errs[0] != syntax {
		t.Errorf("Err() = %v, want the syntax error alone", err)
	}
}

// FuzzLoad checks that Load never panics, and returns a document exactly
// when it reports no errors.
func FuzzLoad(f *testing.F) {
	cases, err := conformance.Cases("format")
	if err != nil {
		f.Fatal(err)
	}
	for _, c := range cases {
		data, err := c.Workflow()
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		doc, diags := format.Load("workflow.yaml", data)
		if (doc == nil) != diags.HasErrors() {
			t.Fatalf("document %v with diagnostics %v", doc, diags)
		}
	})
}

func BenchmarkLoad(b *testing.B) {
	data, err := (&conformance.Case{Name: "format/public-spack-builder"}).Workflow()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if doc, diags := format.Load("workflow.yaml", data); doc == nil {
			b.Fatal(diags)
		}
	}
}
