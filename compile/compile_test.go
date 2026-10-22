package compile_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"maps"
	"slices"
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

// A plan keeps each warning's code, params and place, but not its message
// in English, so that each reader sees it in their own language: decoded,
// it says what the compiler's warning says.
func TestPlanWarningsKeepNoMessage(t *testing.T) {
	src := strings.Replace(sample, "make test", "make test ${{ inputs.mode }}", 1)
	p, diags := compile.Compile("workflow.yaml", []byte(src), nil, compile.Options{})
	if diags.HasErrors() || len(diags) != 1 || len(p.Warnings) != 1 {
		t.Fatalf("diagnostics %v, plan warnings %v; want the one warning in both", diags, p.Warnings)
	}
	data, err := plan.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	var raw struct {
		Warnings []map[string]any `json:"warnings"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if len(raw.Warnings) != 1 || raw.Warnings[0]["detail"] != nil || raw.Warnings[0]["code"] != string(problems.UndefinedInput.Code()) {
		t.Errorf("the plan's warnings are %v, want the code and no detail", raw.Warnings)
	}
	back, err := plan.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := back.Warnings[0], diags[0]; got.String() != want.String() || got.Localize("ja") != want.Localize("ja") || got.Path() != want.Path() {
		t.Errorf("the plan's warning is %s, at %q; want %s, at %q", got, got.Path(), want, want.Path())
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

// Compile lists the first 100 problems of each kind, and one more, where the
// first it leaves out is, counts the rest: a workflow with 130 references to
// inputs that aren't there has 100 warnings of them, and one that counts
// 30, which the plan keeps too. Errors are listed the same way.
func TestProblemsListedPerKind(t *testing.T) {
	var run strings.Builder
	for i := range 130 {
		fmt.Fprintf(&run, "${{ inputs.no%03d }} ", i)
	}
	data := "jobs:\n  main:\n    steps:\n      - run: " + run.String() + "${{ matrix.x }}\n"
	p, diags := compile.Compile("workflow.yaml", []byte(data), nil, compile.Options{})
	if p == nil {
		t.Fatal(diags)
	}
	counts := map[string]int{}
	for _, d := range diags {
		counts[string(d.Code)]++
	}
	want := map[string]int{"undefined_input": 100, "no_matrix": 1, "problems_not_listed": 1}
	if !maps.Equal(counts, want) {
		t.Errorf("diagnostics by code %v, want %v", counts, want)
	}
	i := slices.IndexFunc(diags, func(d problems.Diagnostic) bool { return d.Code == problems.ProblemsNotListed.Code() })
	// The 101st reference is after "      - run: " and 100 of them, 20
	// bytes each, and its "${{ ".
	column := 14 + 100*20 + 4
	if d := diags[i]; d.Params["code"] != "undefined_input" || d.Params["count"] != 30 || d.Params["limit"] != 100 ||
		d.Position() != (problems.Position{Line: 4, Column: column}) {
		t.Errorf("the rest: %v at %v, want 30 of undefined_input at 4:%d", d.Params, d.Position(), column)
	}
	if len(p.Warnings) != len(diags) {
		t.Fatalf("the plan has %d warnings, and Compile returns %d", len(p.Warnings), len(diags))
	}
	for i, w := range p.Warnings {
		if w.Code != diags[i].Code || !maps.Equal(w.Params, diags[i].Params) || w.Detail != "" {
			t.Errorf("the plan's warning %d is %v, want %v without its detail", i, w, diags[i])
		}
	}

	var jobs strings.Builder
	jobs.WriteString("jobs:\n")
	for i := range 105 {
		fmt.Fprintf(&jobs, "  j%03d:\n    needs: [nope]\n    steps: [{run: echo}]\n", i)
	}
	p, diags = compile.Compile("workflow.yaml", []byte(jobs.String()), nil, compile.Options{})
	counts = map[string]int{}
	for _, d := range diags {
		counts[string(d.Code)]++
	}
	want = map[string]int{"needs_unknown_job": 100, "problems_not_listed": 1}
	if p != nil || !maps.Equal(counts, want) {
		t.Errorf("105 jobs that need a job that isn't there: diagnostics by code %v, want %v", counts, want)
	}

	// A workflow that doesn't load: 300 unknown keys, and an error.
	var keys strings.Builder
	keys.WriteString("env: 5\njobs:\n  main:\n    steps:\n      - run: echo\n")
	for i := range 300 {
		fmt.Fprintf(&keys, "    k%03d: x\n", i)
	}
	p, diags = compile.Compile("workflow.yaml", []byte(keys.String()), nil, compile.Options{})
	counts = map[string]int{}
	for _, d := range diags {
		counts[string(d.Code)]++
	}
	want = map[string]int{"unknown_key": 100, "wrong_type": 1, "problems_not_listed": 1}
	if p != nil || !maps.Equal(counts, want) {
		t.Errorf("a workflow that doesn't load: diagnostics by code %v, want %v", counts, want)
	}
}

// A job's needs is evaluated in one group with the inputs' defaults and
// conditions, and the other jobs' needs, which share one budget: once eight
// defaults have done all the 256 MiB of work the group may, a needs can do
// none; and eight needs that each do almost 32 MiB of it leave none for a
// ninth, though each is within the language's limit on its own.
func TestNeedsShareTheInputsBudget(t *testing.T) {
	notListed := func(diags problems.Diagnostics) []string {
		var out []string
		for _, d := range diags.Errors() {
			out = append(out, string(d.Code)+" "+d.Path())
		}
		return out
	}
	var b strings.Builder
	b.WriteString("on:\n  execute:\n    inputs:\n      half:\n        type: string\n")
	for i := range 8 {
		fmt.Fprintf(&b, "      d%d:\n        type: boolean\n        default: ${{ inputs.half == inputs.half }}\n", i)
	}
	b.WriteString("jobs:\n  main:\n    steps: [{run: echo}]\n  next:\n    needs: ${{ 'main' }}\n    steps: [{run: echo}]\n")
	_, diags := compile.Compile("workflow.yaml", []byte(b.String()), map[string]any{"half": strings.Repeat("x", 16<<20-20)}, compile.Options{})
	if got, want := notListed(diags), []string{"expressions_too_much_work /jobs/next/needs"}; !slices.Equal(got, want) {
		t.Errorf("a needs after defaults that spend the budget: %v, want %v", got, want)
	}

	b.Reset()
	b.WriteString("on:\n  execute:\n    inputs:\n      half:\n        type: string\njobs:\n  main:\n    steps: [{run: echo}]\n")
	for i := range 9 {
		fmt.Fprintf(&b, "  next%d:\n    needs: ${{ inputs.half == inputs.half && 'main' }}\n    steps: [{run: echo}]\n", i)
	}
	_, diags = compile.Compile("workflow.yaml", []byte(b.String()), map[string]any{"half": strings.Repeat("x", 16<<20-64)}, compile.Options{})
	if got, want := notListed(diags), []string{"expressions_too_much_work /jobs/next8/needs"}; !slices.Equal(got, want) {
		t.Errorf("nine needs that each do almost 32 MiB of work: %v, want %v", got, want)
	}
}

// suggestions returns, for each diagnostic of kind k, its params' name, or
// else id, and what it suggests.
func suggestions(diags problems.Diagnostics, k *problems.Kind) []string {
	var out []string
	for _, d := range diags {
		if d.Code == k.Code() {
			name, _ := d.Params["name"].(string)
			if name == "" {
				name, _ = d.Params["id"].(string)
			}
			out = append(out, name+" "+d.Params["suggestion"].(string))
		}
	}
	return out
}

// Each misspelled name suggests what it probably meant: an input of the
// workflow's, and a step before the one that reads it, each step's own.
func TestEachMisspellingSuggests(t *testing.T) {
	src := `on:
  execute:
    inputs:
      name: {type: string, default: x}
      value: {type: string, default: y}
jobs:
  main:
    steps:
      - id: alpha
        run: echo ${{ inputs.namee }} ${{ inputs.valu }} ${{ inputs.namee }}
      - id: beta
        run: echo ${{ steps.alphaa.outputs.x }}
      - run: echo ${{ steps.betaa.outputs.x }} ${{ steps.alphaa.outputs.x }}
`
	_, diags := compile.Compile("workflow.yaml", []byte(src), nil, compile.Options{})
	if got, want := suggestions(diags, problems.UndefinedInput), []string{"namee name", "valu value", "namee name"}; !slices.Equal(got, want) {
		t.Errorf("inputs: %q, want %q", got, want)
	}
	if got, want := suggestions(diags, problems.UndefinedStep), []string{"alphaa alpha", "betaa beta", "alphaa alpha"}; !slices.Equal(got, want) {
		t.Errorf("steps: %q, want %q", got, want)
	}
}

// inputs.pw-user names a group's input by its path: an input pw-user in a
// group isn't one at the top.
func TestHyphenatedNamesInGroups(t *testing.T) {
	src := `on:
  execute:
    inputs:
      g:
        type: group
        items:
          pw-user: {type: string, default: me}
jobs:
  main:
    steps:
      - run: |
          echo ${{ inputs.g.pw-user }}
          echo ${{ inputs.pw-user }}
`
	_, diags := compile.Compile("workflow.yaml", []byte(src), nil, compile.Options{})
	var defined []string
	for _, d := range diags {
		if d.Code == problems.HyphenatedName.Code() {
			s, _ := d.Params["defined"].(string)
			defined = append(defined, s)
		}
	}
	if want := []string{"g.pw-user", ""}; !slices.Equal(defined, want) {
		t.Errorf("hyphenated names define %q, want %q", defined, want)
	}
}
