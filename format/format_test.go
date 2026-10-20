package format_test

import (
	"strings"
	"testing"

	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/problems"
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
			if !tc.ok && (len(diags) != 1 || diags[0].Code != problems.FileTooLarge.Code()) {
				t.Errorf("diagnostics = %v, want one %s", diags, problems.FileTooLarge.Code())
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
	for pointer, want := range map[string]problems.Position{
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
	_, diags := format.Load("workflow.yaml", []byte("jobs:\n  main:\n    x: 1\n    steps: [}\n"))
	if len(diags) != 1 {
		t.Fatalf("diagnostics = %v, want one", diags)
	}
	want := "workflow.yaml:3: error: The file isn't valid YAML: did not find expected node content. (yaml_syntax)"
	if got := diags[0].String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
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
