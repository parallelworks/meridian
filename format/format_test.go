package format_test

import (
	"fmt"
	"math"
	"reflect"
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
	var dropped []string
	for path, v := range doc.Dropped() {
		dropped = append(dropped, fmt.Sprintf("%s=%v", strings.Join(path, "."), v))
	}
	if want := []string{"jobs.main.early-cancel=any-job-failed"}; !reflect.DeepEqual(dropped, want) {
		t.Errorf("Dropped = %v, want %v", dropped, want)
	}
}

// A whole number is an int64, and one too large for an int64 is a float64,
// as JSON's numbers are: still a number, not text.
func TestLoadWholeNumbers(t *testing.T) {
	doc, diags := format.Load("workflow.yaml", []byte(`configurations:
  numbers:
    inputs:
      small: 9223372036854775807
      large: 18446744073709551615
      negative: -9223372036854775808
`+hello))
	if diags.HasErrors() {
		t.Fatal(diags)
	}
	inputs := doc.Value["configurations"].(map[string]any)["numbers"].(map[string]any)["inputs"].(map[string]any)
	for key, want := range map[string]any{
		"small":    int64(math.MaxInt64),
		"large":    float64(math.MaxUint64),
		"negative": int64(math.MinInt64),
	} {
		if got := inputs[key]; got != want {
			t.Errorf("%s = %#v, want %#v", key, got, want)
		}
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

// A value of the wrong type is told the types it can have in one order,
// whether the schema gives them in one type keyword, as it does a job's
// if:, or as forms, as it does a job's needs:, a list or an expression,
// and the workflow's description:, text or a file.
func TestWrongTypeOrder(t *testing.T) {
	for _, c := range []struct{ workflow, path, want string }{
		{
			"jobs:\n  main:\n    if: [1]\n    steps: [{run: echo}]\n", "/jobs/main/if",
			"Must be empty, true or false or a string, not a list.",
		},
		{
			"jobs:\n  main:\n    needs: 5\n    steps: [{run: echo}]\n", "/jobs/main/needs",
			"Must be a string or a list, not a number.",
		},
		{
			"description: 5\njobs:\n  main:\n    steps: [{run: echo}]\n", "/description",
			"Must be a string or a mapping, not a number.",
		},
	} {
		_, diags := format.Load("workflow.yaml", []byte(c.workflow))
		if len(diags) != 1 || diags[0].Code != problems.WrongType.Code() || diags[0].Path() != c.path || diags[0].Message() != c.want {
			t.Errorf("%s: diagnostics = %v, want %q at %s", c.path, diags, c.want, c.path)
		}
	}
}

// FuzzLoad checks that Load never panics; that it returns a document
// exactly when it reports no errors, and the same document and diagnostics
// each time; that each diagnostic is of a kind Meridian has, at a JSON
// Pointer, with a message; and that the document's value holds only JSON's
// values, as Document.Value says.
func FuzzLoad(f *testing.F) {
	for _, area := range []string{"format", "inputs", "compile"} {
		cases, err := conformance.Cases(area)
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
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		doc, diags := format.Load("workflow.yaml", data)
		if (doc == nil) != diags.HasErrors() {
			t.Fatalf("document %v with diagnostics %v", doc, diags)
		}
		again, againDiags := format.Load("workflow.yaml", data)
		if !reflect.DeepEqual(againDiags, diags) || (again == nil) != (doc == nil) || doc != nil && !reflect.DeepEqual(again.Value, doc.Value) {
			t.Fatalf("loading again gave\n%v\nnot\n%v", againDiags, diags)
		}
		for _, d := range diags {
			if _, ok := d.Kind(); !ok || d.Message() == string(d.Code) || d.Path() != "" && !strings.HasPrefix(d.Path(), "/") {
				t.Fatalf("diagnostic %s at %q", d, d.Path())
			}
		}
		if doc != nil && !isJSON(doc.Value) {
			t.Fatalf("the value %#v isn't JSON's", doc.Value)
		}
	})
}

// isJSON reports whether v is made of the values Document.Value holds: maps
// of strings to values, lists, strings, booleans, int64s, finite float64s
// and nil.
func isJSON(v any) bool {
	switch v := v.(type) {
	case nil, bool, string, int64:
		return true
	case float64:
		return !math.IsNaN(v) && !math.IsInf(v, 0)
	case []any:
		for _, item := range v {
			if !isJSON(item) {
				return false
			}
		}
		return true
	case map[string]any:
		for _, item := range v {
			if !isJSON(item) {
				return false
			}
		}
		return true
	}
	return false
}

// Each input matches one of the forms of an input, which are many.
func BenchmarkLoadInputs(b *testing.B) {
	var w strings.Builder
	w.WriteString("on:\n  execute:\n    inputs:\n")
	for i := range 50 {
		fmt.Fprintf(&w, "      s%02d: {type: string, default: text}\n", i)
		fmt.Fprintf(&w, "      n%02d: {type: number, default: 3, min: 1}\n", i)
		fmt.Fprintf(&w, "      b%02d: {type: boolean, default: true}\n", i)
		fmt.Fprintf(&w, "      d%02d: {type: dropdown, options: [a, b, c], default: a}\n", i)
	}
	w.WriteString("jobs:\n  main:\n    steps:\n      - run: echo\n")
	data := []byte(w.String())
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if doc, diags := format.Load("workflow.yaml", data); doc == nil {
			b.Fatal(diags)
		}
	}
}

// Each of 2,000 inputs has a key the schema doesn't define, which is
// explained against each of an input's forms.
func BenchmarkLoadUnknownKeys(b *testing.B) {
	var w strings.Builder
	w.WriteString("on:\n  execute:\n    inputs:\n")
	for i := range 2000 {
		fmt.Fprintf(&w, "      i%d: {type: string, zz: 1}\n", i)
	}
	w.WriteString("jobs:\n  main:\n    steps:\n      - run: echo\n")
	data := []byte(w.String())
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if doc, diags := format.Load("workflow.yaml", data); doc == nil {
			b.Fatal(diags)
		}
	}
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

// TestUpdateSessionKeysFolded checks that a key of update-session's with:,
// or of its targetInfo, but for case, isn't unknown, as format 1 reads it
// as that key, decoding the object as encoding/json does: it is kept, and
// counts as a key the object requires. A key that isn't one but for case,
// as local_port, is still unknown, and so is a key but for case of another
// action's with:, which format 1 doesn't read so.
func TestUpdateSessionKeysFolded(t *testing.T) {
	doc, diags := format.Load("workflow.yaml", []byte(`jobs:
  main:
    steps:
      - uses: parallelworks/update-session
        with:
          Name: desktop
          REMOTEPORT: 8080
          local_port: 1
      - uses: parallelworks/update-session
        with:
          name: dashboard
          targetInfo:
            NAME: k8s
            resourcetype: pods
      - uses: parallelworks/checkout
        with:
          repo: https://example.com/app.git
          branch: main
          Path: app
`))
	if doc == nil || diags.HasErrors() {
		t.Fatalf("Load = %v", diags)
	}
	var unknown []string
	for _, d := range diags {
		if d.Code == problems.UnknownKey.Code() {
			unknown = append(unknown, d.Path())
		}
	}
	if want := []string{"/jobs/main/steps/0/with/local_port", "/jobs/main/steps/2/with/Path"}; !reflect.DeepEqual(unknown, want) || len(diags) != len(want) {
		t.Errorf("diagnostics %v, want unknown_key at %v", diags, want)
	}
	steps := doc.Value["jobs"].(map[string]any)["main"].(map[string]any)["steps"].([]any)
	with := steps[0].(map[string]any)["with"].(map[string]any)
	info := steps[1].(map[string]any)["with"].(map[string]any)["targetInfo"].(map[string]any)
	if with["Name"] != "desktop" || with["REMOTEPORT"] != int64(8080) || info["NAME"] != "k8s" || info["resourcetype"] != "pods" {
		t.Errorf("the with: kept is %v, and its targetInfo %v", with, info)
	}
}
