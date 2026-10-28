package compile_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/internal/schematest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// treeSources gives each workflow it has by its uses:, at a location of
// the uses: and its $yaml, pinned to a commit, and records what it was
// asked for.
type treeSources struct {
	texts map[string]string
	asked []plan.SourceRef
	// pinned are the repositories and revisions it was asked to pin, as
	// repo@revision, each pinned to a commit of its own.
	pinned []string
}

func (s *treeSources) Pin(_ context.Context, repo, revision string) (string, error) {
	s.pinned = append(s.pinned, repo+"@"+revision)
	return "89abcdef0123456789abcdef0123456789abcdef", nil
}

func (s *treeSources) Resolve(_ context.Context, ref plan.SourceRef) (*compile.Workflow, error) {
	s.asked = append(s.asked, ref)
	text, ok := s.texts[ref.Uses]
	if !ok {
		return nil, compile.ErrNotFound
	}
	return &compile.Workflow{
		Source: plan.Source{Location: ref.Uses + ":" + ref.Path, Revision: "0123456789abcdef0123456789abcdef01234567"},
		Text:   []byte(text),
	}, nil
}

func (s *treeSources) Read(context.Context, plan.Source) (*compile.Workflow, error) {
	return nil, errors.New("not read as the run is created")
}

// calling is a workflow whose job's steps call the workflows uses names,
// each a github/t/<name>@main, in order, and then run a command.
func calling(names ...string) string {
	var b strings.Builder
	b.WriteString("jobs:\n  j:\n    steps:\n")
	for _, n := range names {
		fmt.Fprintf(&b, "      - uses: github/t/%s@main\n        with: {$yaml: w.yaml}\n", n)
	}
	b.WriteString("      - run: echo\n")
	return b.String()
}

// tree is a treeSources of the workflows calls names, each calling the
// workflows it lists.
func tree(calls map[string][]string) *treeSources {
	s := &treeSources{texts: map[string]string{}}
	for name, callees := range calls {
		s.texts["github/t/"+name+"@main"] = calling(callees...)
	}
	return s
}

// digestOf is the digest of a workflow's text, as a plan's Source has it.
func digestOf(s *treeSources, name string) string {
	sum := sha256.Sum256([]byte(s.texts["github/t/"+name+"@main"]))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Create pins the whole tree of workflows the run calls: each reference
// once, whatever path reaches it, the run's own steps' in their steps, and
// the others in the plan's calls, by their caller's digest and the step's
// place, which makes the plan, and its record, of version 2; it gives the
// texts of them all, for the coordinator to keep; and a plan without calls
// is of version 1.
func TestCreatePinsTheTree(t *testing.T) {
	s := tree(map[string][]string{"a": {"b", "c"}, "b": {"c"}, "c": nil})
	c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(calling("a", "c")), nil, compile.Options{Sources: s})
	if err != nil || c == nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	var asked []string
	for _, r := range s.asked {
		asked = append(asked, r.Uses)
	}
	if want := []string{"github/t/a@main", "github/t/c@main", "github/t/b@main"}; !slices.Equal(asked, want) {
		t.Errorf("asked for %q, want each once, breadth-first: %q", asked, want)
	}
	steps := c.Plan.Jobs[0].Steps
	if steps[0].Source == nil || steps[0].Source.Location != "github/t/a@main:w.yaml" || steps[1].Source == nil || steps[1].Source.Location != "github/t/c@main:w.yaml" {
		t.Errorf("the run's own steps are pinned to %+v and %+v", steps[0].Source, steps[1].Source)
	}
	a, b := digestOf(s, "a"), digestOf(s, "b")
	type call struct{ caller, step, location string }
	want := []call{
		{a, "/jobs/j/steps/0", "github/t/b@main:w.yaml"},
		{a, "/jobs/j/steps/1", "github/t/c@main:w.yaml"},
		{b, "/jobs/j/steps/0", "github/t/c@main:w.yaml"},
	}
	slices.SortFunc(want, func(x, y call) int { return strings.Compare(x.caller+x.step, y.caller+y.step) })
	var got []call
	for _, k := range c.Plan.Calls {
		got = append(got, call{k.Caller, k.Step, k.Source.Location})
		if k.Source.Ref.Uses == "" || k.Source.Digest == "" || k.Source.Revision == "" {
			t.Errorf("the call %+v isn't pinned", k)
		}
	}
	if !slices.Equal(got, want) {
		t.Errorf("the plan's calls are %+v, want %+v", got, want)
	}
	if c.Plan.Version != 2 || c.Record.Version != 2 {
		t.Errorf("the plan is of version %d, and its record %d, want 2", c.Plan.Version, c.Record.Version)
	}
	data, err := plan.Encode(c.Plan)
	if err != nil {
		t.Fatal(err)
	}
	schematest.Validate(t, schematest.Schema(t, "plan/schema/2/plan.schema.json"), "the plan", data)
	if back, err := plan.Decode(data); err != nil || len(back.Calls) != 3 {
		t.Errorf("the plan decodes as %v, %v", back, err)
	}
	if source, ok := c.Plan.FindCall(b, "/jobs/j/steps/0"); !ok || source.Location != "github/t/c@main:w.yaml" {
		t.Errorf("FindCall of b's step = %+v, %v", source, ok)
	}
	// The texts of every workflow the tree pins, the run's own steps'
	// among them, each once, by their digests, for the coordinator to
	// keep.
	byDigest := map[string]string{}
	for _, name := range []string{"a", "b", "c"} {
		byDigest[digestOf(s, name)] = s.texts["github/t/"+name+"@main"]
	}
	var digests []string
	for _, text := range c.Texts {
		digests = append(digests, text.Digest)
		if want, ok := byDigest[text.Digest]; !ok || string(text.Text) != want {
			t.Errorf("the text by %s is %q", text.Digest, text.Text)
		}
	}
	if want := slices.Sorted(maps.Keys(byDigest)); !slices.Equal(digests, want) {
		t.Errorf("the texts are by %q, want each of a's, b's and c's once, in order: %q", digests, want)
	}

	// A run whose called workflows call none has no calls, and is of
	// version 1.
	c, diags, err = compile.Create(t.Context(), "workflow.yaml", []byte(calling("c")), nil, compile.Options{Sources: s})
	if err != nil || c == nil || diags.HasErrors() || c.Plan.Calls != nil || c.Plan.Version != 1 || c.Record.Version != 1 {
		t.Errorf("a run whose child calls nothing: %+v, %v, %v", c, diags, err)
	}
}

// Create refuses a tree it can't run, as format 1 does: a workflow more
// than 4 levels below the run, or already on its own path of calls, which
// either path finds; more calls, counted on every path, or more distinct
// workflows, or more text, than the limits allow, each at its boundary,
// whether the run's own steps cross it or a called workflow's do. Each is
// reported at the uses: of the run's own step the path to it starts at, in
// the run's own workflow, with the chain of calls from the run for a cycle
// and a depth.
func TestCreateRefusesTrees(t *testing.T) {
	one := len(calling())
	for _, tc := range []struct {
		name   string
		calls  map[string][]string
		root   []string
		limits compile.CallLimits
		code   problem.Code
		chain  string
		// at is the run's own step whose uses: the problem is at.
		at int
	}{
		{name: "4 levels", calls: map[string][]string{"a": {"b"}, "b": {"c"}, "c": {"d"}, "d": nil}, root: []string{"a"}},
		{
			name: "5 levels", calls: map[string][]string{"a": {"b"}, "b": {"c"}, "c": {"d"}, "d": {"e"}, "e": nil}, root: []string{"a"},
			code:  problems.WorkflowNestedTooDeep.Code(),
			chain: `"workflow.yaml", "github/t/a@main:w.yaml", "github/t/b@main:w.yaml", "github/t/c@main:w.yaml", "github/t/d@main:w.yaml", "github/t/e@main"`,
		},
		{
			name: "2 levels at most", calls: map[string][]string{"x": nil, "a": {"b"}, "b": {"c"}, "c": nil}, root: []string{"x", "a"}, limits: compile.CallLimits{MaxDepth: 2},
			code: problems.WorkflowNestedTooDeep.Code(), at: 1,
			chain: `"workflow.yaml", "github/t/a@main:w.yaml", "github/t/b@main:w.yaml", "github/t/c@main"`,
		},
		{
			name: "itself", calls: map[string][]string{"a": {"a"}}, root: []string{"a"},
			code: problems.WorkflowCallsItself.Code(), chain: `"workflow.yaml", "github/t/a@main:w.yaml", "github/t/a@main:w.yaml"`,
		},
		{
			name: "each other", calls: map[string][]string{"a": {"b"}, "b": {"a"}}, root: []string{"a"},
			code:  problems.WorkflowCallsItself.Code(),
			chain: `"workflow.yaml", "github/t/a@main:w.yaml", "github/t/b@main:w.yaml", "github/t/a@main:w.yaml"`,
		},
		// Each reached from the run first: the cycle is found on the paths
		// through the other.
		{name: "each other, from the run", calls: map[string][]string{"a": {"b"}, "b": {"a"}}, root: []string{"a", "b"}, code: problems.WorkflowCallsItself.Code()},
		{name: "calls on every path", calls: map[string][]string{"a": {"b"}, "b": nil}, root: []string{"a", "a"}, limits: compile.CallLimits{MaxCalls: 3}, code: problems.WorkflowsTooMany.Code(), at: 1},
		{name: "calls on every path, within", calls: map[string][]string{"a": {"b"}, "b": nil}, root: []string{"a", "a"}, limits: compile.CallLimits{MaxCalls: 4}},
		{name: "workflows", calls: map[string][]string{"a": nil, "b": nil, "c": nil}, root: []string{"a", "b", "c"}, limits: compile.CallLimits{MaxWorkflows: 2}, code: problems.WorkflowsTooMany.Code(), at: 2},
		{name: "workflows, within", calls: map[string][]string{"a": nil, "b": nil, "c": nil}, root: []string{"a", "b", "c"}, limits: compile.CallLimits{MaxWorkflows: 3}},
		{name: "text", calls: map[string][]string{"a": nil, "b": nil}, root: []string{"a", "b"}, limits: compile.CallLimits{MaxBytes: 2*one - 1}, code: problems.WorkflowsTooLarge.Code(), at: 1},
		// The run's own text doesn't count, as in format 1.
		{name: "text, within", calls: map[string][]string{"a": nil, "b": nil}, root: []string{"a", "b"}, limits: compile.CallLimits{MaxBytes: 2 * one}},
		// Each limit crossed below the run, at a called workflow's step: it
		// is reported at the run's own step the path starts at.
		{name: "workflows, below the run", calls: map[string][]string{"x": nil, "y": nil, "a": {"b"}, "b": nil}, root: []string{"x", "y", "a"}, limits: compile.CallLimits{MaxWorkflows: 3}, code: problems.WorkflowsTooMany.Code(), at: 2},
		{name: "workflows, below the run, within", calls: map[string][]string{"x": nil, "y": nil, "a": {"b"}, "b": nil}, root: []string{"x", "y", "a"}, limits: compile.CallLimits{MaxWorkflows: 4}},
		{name: "text, below the run", calls: map[string][]string{"x": nil, "a": {"b"}, "b": nil}, root: []string{"x", "a"}, limits: compile.CallLimits{MaxBytes: 2*one + len(calling("b")) - 1}, code: problems.WorkflowsTooLarge.Code(), at: 1},
		{name: "text, below the run, within", calls: map[string][]string{"x": nil, "a": {"b"}, "b": nil}, root: []string{"x", "a"}, limits: compile.CallLimits{MaxBytes: 2*one + len(calling("b"))}},
	} {
		s := tree(tc.calls)
		c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(calling(tc.root...)), nil, compile.Options{Sources: s, Calls: tc.limits})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if tc.code == "" {
			if c == nil || diags.HasErrors() {
				t.Errorf("%s: %v", tc.name, diags)
			}
			continue
		}
		errs := diags.Errors()
		if c != nil || len(errs) == 0 || errs[0].Code != tc.code {
			t.Errorf("%s: %v, want %s", tc.name, diags, tc.code)
			continue
		}
		d := errs[0]
		if at := fmt.Sprintf("/jobs/j/steps/%d/uses", tc.at); d.File() != "workflow.yaml" || d.Path() != at {
			t.Errorf("%s: reported in %s at %s, want the run's own workflow at %s", tc.name, d.File(), d.Path(), at)
		}
		if tc.chain != "" && d.Params["chain"] != tc.chain {
			t.Errorf("%s: the chain is %v, want %s", tc.name, d.Params["chain"], tc.chain)
		}
	}
}

// A cycle that several paths reach, from whichever of its workflows each
// enters it at, is reported once.
func TestCreateReportsACycleOnce(t *testing.T) {
	s := tree(map[string][]string{"a": {"c"}, "b": {"c"}, "c": {"a"}})
	_, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(calling("a", "b")), nil, compile.Options{Sources: s})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, d := range diags.Errors() {
		if d.Code == problems.WorkflowCallsItself.Code() {
			n++
		}
	}
	if n != 1 {
		t.Errorf("the cycle was reported %d times: %v", n, diags)
	}
}

// What is wrong with a called workflow's step is reported in its file, at
// the step: a reference that isn't written out, one that names no
// workflow, and a file that doesn't load.
func TestCreateReportsCalledWorkflows(t *testing.T) {
	s := tree(map[string][]string{"a": nil})
	s.texts["github/t/a@main"] = "jobs:\n  j:\n    steps:\n      - uses: github/t/x@main\n        with: {$yaml: '${{ inputs.f }}'}\n      - uses: github/t/none@main\n      - uses: github/t/bad@main\n"
	s.texts["github/t/bad@main"] = "jobs: [\n"
	_, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(calling("a")), nil, compile.Options{Sources: s})
	if err != nil {
		t.Fatal(err)
	}
	type at struct {
		code problem.Code
		file string
	}
	var got []at
	for _, d := range diags.Errors() {
		got = append(got, at{d.Code, d.File()})
	}
	for _, want := range []at{
		{problems.SourceNotKnown.Code(), "github/t/a@main:w.yaml"},
		{problems.SourceNotFound.Code(), "github/t/a@main:w.yaml"},
		{"yaml_syntax", "github/t/bad@main:"},
	} {
		if !slices.Contains(got, want) {
			t.Errorf("the problems are %+v, without %+v", got, want)
		}
	}
}

// remoteSources is a treeSources whose workflows have a remote context of
// their own, as a repository's have.
type remoteSources struct{ *treeSources }

func (s remoteSources) Resolve(ctx context.Context, ref plan.SourceRef) (*compile.Workflow, error) {
	w, err := s.treeSources.Resolve(ctx, ref)
	if err == nil {
		w.Remote = map[string]string{"repo": "https://example.com/" + ref.Uses}
	}
	return w, err
}

// Create keeps, in the plan, the variables the workflows the run calls
// read, down the tree, which the run doesn't, as its Start reads them for
// the tree: in an expression, read whole, and in an if: written without
// ${{ }}; and the remote context each pin has, as Sources gave it, which
// makes the plan of version 2.
func TestCreateKeepsTreeVariablesAndRemotes(t *testing.T) {
	s := tree(map[string][]string{"a": {"b"}, "b": nil})
	s.texts["github/t/a@main"] = "jobs:\n  j:\n    if: vars.GATE == 'open'\n    steps:\n      - run: echo ${{ vars.A }} ${{ org.B }}\n      - uses: github/t/b@main\n        with: {$yaml: w.yaml}\n"
	s.texts["github/t/b@main"] = "jobs:\n  j:\n    steps:\n      - run: echo ${{ vars.* }}\n"
	c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(calling("a")), nil, compile.Options{Sources: remoteSources{s}})
	if err != nil || c == nil {
		t.Fatal(diags, err)
	}
	if want := []string{"org.B", "vars", "vars.A", "vars.GATE"}; !slices.Equal(c.Plan.Run.TreeVariables, want) {
		t.Errorf("the tree's variables are %q, want %q", c.Plan.Run.TreeVariables, want)
	}
	if src := c.Plan.Jobs[0].Steps[0].Source; src == nil || src.Remote["repo"] != "https://example.com/github/t/a@main" {
		t.Errorf("the step's pin is %+v", src)
	}
	if len(c.Plan.Calls) != 1 || c.Plan.Calls[0].Source.Remote["repo"] != "https://example.com/github/t/b@main" {
		t.Errorf("the plan's calls are %+v", c.Plan.Calls)
	}
	if c.Plan.Version != 2 {
		t.Errorf("the plan is of version %d", c.Plan.Version)
	}
	// A tree whose workflows read no variable has none.
	c, _, err = compile.Create(t.Context(), "workflow.yaml", []byte(calling("b")), nil, compile.Options{Sources: tree(map[string][]string{"b": nil})})
	if err != nil || c == nil || c.Plan.Run.TreeVariables != nil {
		t.Errorf("a tree that reads no variable: %+v, %v", c, err)
	}
}

// Create refuses a run any of whose calls, down its tree, leaves out of its
// with: an input the workflow it calls requires, as format 1 does,
// whatever the step's if:: at the uses: of the run's own step the path of
// calls starts at, with the chain of calls from the run, and a call that
// two paths from that step reach, once. What an expression in a with:
// gives, the child's Create checks as the step runs.
func TestCreateChecksCalledInputs(t *testing.T) {
	needs := "on:\n  execute:\n    inputs:\n      name: {type: string, label: Name}\n      other: {type: string, default: x}\njobs:\n  j:\n    steps:\n      - run: echo\n"
	callsNeeds := func(withs ...string) string {
		var b strings.Builder
		b.WriteString("jobs:\n  j:\n    steps:\n")
		for _, w := range withs {
			fmt.Fprintf(&b, "      - uses: github/t/needs@main\n        if: ${{ false }}\n        with: {$yaml: w.yaml%s}\n", w)
		}
		return b.String()
	}
	s := tree(map[string][]string{"a": {"b", "c"}, "b": {"d"}, "c": {"d"}})
	s.texts["github/t/needs@main"] = needs
	s.texts["github/t/mid@main"] = callsNeeds("", ", name: '${{ inputs.x }}'", ", name: n")
	s.texts["github/t/d@main"] = callsNeeds("")
	root := callsNeeds("", ", name: n", ", name: '${{ inputs.x }}'") +
		"      - uses: github/t/mid@main\n        with: {$yaml: w.yaml}\n" +
		"      - uses: github/t/mid@main\n        with: {$yaml: w.yaml}\n" +
		"      - uses: github/t/a@main\n        with: {$yaml: w.yaml}\n"
	_, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(root), nil, compile.Options{Sources: s})
	if err != nil {
		t.Fatal(err)
	}
	type missing struct{ at, input, chain string }
	var got []missing
	for _, d := range diags.Errors() {
		if d.Code != problems.ChildInputMissing.Code() {
			t.Errorf("unexpected %v", d)
			continue
		}
		got = append(got, missing{d.Path(), d.Params["input"].(string), d.Params["chain"].(string)})
	}
	want := []missing{
		{"/jobs/j/steps/0/uses", "name (Name)", `"workflow.yaml", "github/t/needs@main:w.yaml"`},
		{"/jobs/j/steps/3/uses", "name (Name)", `"workflow.yaml", "github/t/mid@main:w.yaml", "github/t/needs@main:w.yaml"`},
		{"/jobs/j/steps/4/uses", "name (Name)", `"workflow.yaml", "github/t/mid@main:w.yaml", "github/t/needs@main:w.yaml"`},
		{"/jobs/j/steps/5/uses", "name (Name)", `"workflow.yaml", "github/t/a@main:w.yaml", "github/t/b@main:w.yaml", "github/t/d@main:w.yaml", "github/t/needs@main:w.yaml"`},
	}
	if !slices.Equal(got, want) {
		t.Errorf("the inputs left out are %q, want %q", got, want)
	}
}
