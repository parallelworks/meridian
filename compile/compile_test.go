package compile_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

const sample = `on:
  execute:
    inputs:
      n:
        type: number
        default: 2
jobs:
  build:
    steps:
      - id: make
        run: make -j${{ inputs.n }}
  test:
    needs: [build]
    if: ${{ needs.build.result == 'success' }}
    steps:
      - run: make test
`

// The same workflow and inputs compile to the same bytes, which decode to
// the same plan.
func TestCompileIsDeterministic(t *testing.T) {
	first, diags := compile.Compile("workflow.yaml", []byte(sample), map[string]any{"n": 4}, compile.Options{})
	if diags.HasErrors() {
		t.Fatal(diags)
	}
	a, err := plan.Encode(first)
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		again, _ := compile.Compile("workflow.yaml", []byte(sample), map[string]any{"n": 4}, compile.Options{})
		if b, _ := plan.Encode(again); string(b) != string(a) {
			t.Fatalf("compiled differently:\n%s\n%s", a, b)
		}
	}
	decoded, err := plan.Decode(a)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := plan.Encode(decoded); string(b) != string(a) {
		t.Errorf("decoding changed the plan")
	}
}

func TestSource(t *testing.T) {
	p, _ := compile.Compile("workflow.yaml", []byte(sample), nil, compile.Options{})
	sum := sha256.Sum256([]byte(sample))
	if want := "sha256:" + hex.EncodeToString(sum[:]); p.Run.Source != (plan.Source{Location: "workflow.yaml", Revision: want}) {
		t.Errorf("source = %+v, want the file and the SHA-256 of its text", p.Run.Source)
	}
	p, _ = compile.Compile("workflow.yaml", []byte(sample), nil, compile.Options{
		Source: plan.Source{Location: "https://example.com/repo.git#workflow.yaml", Revision: "0123abc"},
	})
	if p.Run.Source.Revision != "0123abc" || !strings.HasPrefix(p.Run.Source.Location, "https://") {
		t.Errorf("source = %+v, want the caller's", p.Run.Source)
	}
}

func TestNoPlanWithErrors(t *testing.T) {
	p, diags := compile.Compile("workflow.yaml", []byte(strings.Replace(sample, "needs: [build]", "needs: [buld]", 1)), nil, compile.Options{})
	if p != nil || !diags.HasErrors() {
		t.Errorf("got a plan %v with %v", p, diags)
	}
	p, diags = compile.Compile("workflow.yaml", []byte("jobs: ["), nil, compile.Options{})
	if p != nil || !diags.HasErrors() {
		t.Errorf("a file that doesn't load: plan %v, %v", p, diags)
	}
}

// What a run is given stays data in its plan, whatever it looks like: a
// value shaped like a stored template is never evaluated.
func TestInputsStayData(t *testing.T) {
	stored := map[string]any{"${{": []any{map[string]any{
		"src": " vars.KEY ", "tree": map[string]any{"ref": "vars", "path": []any{"KEY"}},
	}}}
	p, diags := compile.Compile("workflow.yaml", []byte(sample), map[string]any{"n": 4, "cfg": stored}, compile.Options{})
	if diags.HasErrors() {
		t.Fatal(diags)
	}
	data, err := plan.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	back, err := plan.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	cfg := back.Run.Inputs.Get().(map[string]any)["cfg"].(map[string]any)
	if _, ok := cfg["${{"].([]any); !ok || len(cfg) != 1 {
		t.Errorf("cfg decoded as %#v, not the data it was given", cfg)
	}
}

func TestStepValues(t *testing.T) {
	src := `jobs:
  build:
    working-directory: /work
    steps:
      - name: build ${{ inputs.target }}
        working-directory: null
        run: make
`
	p, diags := compile.Compile("workflow.yaml", []byte(src), map[string]any{"target": "all"}, compile.Options{})
	if diags.HasErrors() {
		t.Fatal(diags)
	}
	step := p.Jobs[0].Steps[0]
	if got, err := step.Name.Eval(expr.Scope{Contexts: map[string]any{"inputs": map[string]any{"target": "all"}}}); err != nil || got != "build all" {
		t.Errorf("name = %v, %v; want its template evaluated", got, err)
	}
	if step.WorkingDirectory.IsZero() {
		t.Error("working-directory: null was lost")
	}
	data, err := plan.Encode(p)
	if err != nil || !strings.Contains(string(data), `"workingDirectory":null`) {
		t.Errorf("the plan is %s, %v", data, err)
	}
}

// A reference that subtracts a word warns, and says when the workflow has
// the name with the hyphen that it probably meant.
func TestHyphenatedNames(t *testing.T) {
	src := `on:
  execute:
    inputs:
      pw-user:
        type: string
        default: me
jobs:
  prep-model:
    steps:
      - run: echo prep
  build:
    needs: [prep-model]
    strategy:
      matrix:
        python-version: ['3.11']
    steps:
      - id: setup-go
        run: echo go
      - run: |
          echo ${{ inputs.pw-user }}
          echo ${{ needs.prep-model.outputs.path }}
          echo ${{ steps.setup-go.outputs.version }}
          echo ${{ matrix.python-version }}
          echo ${{ inputs.no-such }}
`
	_, diags := compile.Compile("workflow.yaml", []byte(src), nil, compile.Options{})
	var words []string
	for _, d := range diags {
		if d.Code == problems.HyphenatedName.Code() {
			words = append(words, d.Message())
		}
	}
	want := []string{
		`. The workflow has an input "pw-user".`,
		`. The workflow has a job "prep-model".`,
		`. The job "build" has a step with the id "setup-go".`,
		`. The matrix of the job "build" has a key "python-version".`,
		"To read no-such, write inputs['no-such'].",
	}
	if len(words) != len(want) {
		t.Fatalf("warnings %q, want %d", words, len(want))
	}
	for i, w := range want {
		if !strings.HasSuffix(words[i], w) {
			t.Errorf("warning %d is %q, want it to end %q", i, words[i], w)
		}
	}
}
