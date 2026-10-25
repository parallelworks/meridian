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

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// testGroups has the execution groups, environments and compute targets
// it names, and gives err for the rest, ErrNotFound by default.
type testGroups struct {
	groups       []string
	environments []string
	targets      []string
	err          error
	asked        []string
}

func (g *testGroups) notFound() error {
	if g.err != nil {
		return g.err
	}
	return compile.ErrNotFound
}

func (g *testGroups) Group(_ context.Context, name string) (*backend.Group, error) {
	g.asked = append(g.asked, "group "+name)
	if slices.Contains(g.groups, name) {
		return &backend.Group{Name: name}, nil
	}
	return nil, g.notFound()
}

func (g *testGroups) Environment(_ context.Context, e plan.Environment) (*backend.Group, error) {
	g.asked = append(g.asked, fmt.Sprintf("environment %s/%s %v", e.Cluster, e.Name, e.Params.Get()))
	if slices.Contains(g.environments, e.Cluster+"/"+e.Name) {
		return &backend.Group{Name: e.Name}, nil
	}
	return nil, g.notFound()
}

func (g *testGroups) ComputeTarget(_ context.Context, c plan.ComputeTarget) (*backend.Group, error) {
	g.asked = append(g.asked, "compute target "+c.Mode+" "+c.TargetID)
	if slices.Contains(g.targets, c.Mode+" "+c.TargetID) {
		return &backend.Group{Name: c.TargetID}, nil
	}
	return nil, g.notFound()
}

// testSources gives the workflows it has by their uses:, pinned to a
// commit of their own, and err for the rest, ErrNotFound by default.
type testSources struct {
	texts map[string]string
	err   error
	asked []plan.SourceRef
}

func (s *testSources) Resolve(_ context.Context, ref plan.SourceRef) (*compile.Workflow, error) {
	s.asked = append(s.asked, ref)
	text, ok := s.texts[ref.Uses]
	if !ok {
		if s.err != nil {
			return nil, s.err
		}
		return nil, compile.ErrNotFound
	}
	return &compile.Workflow{
		Source: plan.Source{Location: ref.Uses + "/" + ref.Path, Revision: "0123456789abcdef0123456789abcdef01234567"},
		Text:   []byte(text),
	}, nil
}

func (s *testSources) Read(context.Context, plan.Source) (*compile.Workflow, error) {
	return nil, errors.New("not read as the run is created")
}

// anyGroups has every execution group and environment.
type anyGroups struct{}

func (anyGroups) Group(_ context.Context, name string) (*backend.Group, error) {
	return &backend.Group{Name: name}, nil
}

func (anyGroups) Environment(_ context.Context, e plan.Environment) (*backend.Group, error) {
	return &backend.Group{Name: e.Name}, nil
}

func (anyGroups) ComputeTarget(_ context.Context, c plan.ComputeTarget) (*backend.Group, error) {
	return &backend.Group{Name: c.TargetID}, nil
}

// anySources has a workflow of one job for every reference.
type anySources struct{}

func (anySources) Resolve(_ context.Context, ref plan.SourceRef) (*compile.Workflow, error) {
	return &compile.Workflow{
		Source: plan.Source{Location: ref.Uses, Revision: "0123456789abcdef0123456789abcdef01234567"},
		Text:   []byte("jobs:\n  x:\n    steps:\n      - run: echo\n"),
	}, nil
}

func (anySources) Read(ctx context.Context, source plan.Source) (*compile.Workflow, error) {
	return anySources{}.Resolve(ctx, source.Ref)
}

// Create reads the variables the workflow reads through Variables, by
// their names as they are, the plain ones' values and not the secrets':
// what reads a plain one is evaluated as the run is created, and what reads
// a secret one waits for Start. Contexts' org and vars, which a caller
// without the interface gives, are left aside.
func TestCreateReadsVariables(t *testing.T) {
	src := workflow(`region: {type: string, default: '${{ org.REGION }}', hidden: '${{ org.REGION == "west" }}'}
token: {type: string, default: '${{ vars["my.token"] }}'}`, `jobs:
  main:
    if: vars.API_TOKEN != ''
    steps:
      - run: echo ${{ toJSON(org.*) }}
      - run: echo
        if: always
      - run: echo
        if: vars.STAGE == 'test'
`)
	vars := &countingVariables{testVariables: testVariables{
		"org.REGION": {Value: "west"}, "org.OTHER": {Value: "x"}, "vars.my.token": {Value: "t", Secret: true}, "vars.UNREAD": {Value: "u"},
		"vars.API_TOKEN": {Value: "s", Secret: true}, "vars.STAGE": {Value: "test"},
	}}
	opts := compile.Options{Variables: vars, Contexts: map[string]any{"org": map[string]any{"REGION": "east"}}}
	c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, opts)
	if err != nil || c == nil {
		t.Fatal(diags, err)
	}
	// An if: written without ${{ }} is an expression, and what it reads is
	// asked for too.
	if want := [][]string{{"org", "vars.API_TOKEN", "vars.STAGE", "vars.my.token"}}; !slices.EqualFunc(vars.asked, want, slices.Equal) {
		t.Errorf("asked %q, want %q", vars.asked, want)
	}
	for _, d := range diags {
		if d.Code == problems.VariableLooksSecret.Code() {
			t.Errorf("a secret variable is taken for a plain one: %s", d)
		}
	}
	if d := c.Record.Decisions; len(d) != 1 || !d["/on/execute/inputs/region/hidden"] {
		t.Errorf("the hidden: read %v, not the interface's value", d)
	}
	if want := []string{"vars.API_TOKEN", "vars.my~1token"}; !slices.Equal(c.Plan.Run.Secrets, want) {
		t.Errorf("secrets %v, want %v", c.Plan.Run.Secrets, want)
	}
	if _, ok := c.Plan.Run.DeferredInputs["token"]; !ok {
		t.Errorf("the default that reads a secret isn't held for Start: %v", c.Plan.Run.DeferredInputs)
	}
	// Compile reaches none of the resolvers that take a context.
	p, diags := compile.Compile("workflow.yaml", []byte(src), nil, compile.Options{Variables: failingVariables{errors.New("asked")}})
	if p == nil || len(vars.asked) != 1 {
		t.Errorf("Compile: %v", diags)
	}
}

// Create with the Variables resolver gives the plan and the diagnostics
// Create gives with the same variables in Contexts, for each conformance
// case that reads variables.
func TestCreateVariablesAsContexts(t *testing.T) {
	compared := 0
	for _, area := range []string{"inputs", "compile", "start"} {
		cases, err := conformance.Cases(area)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			vars := caseVariables(c, func(name, v string) string {
				if v == "" {
					return "value-of-" + name
				}
				return v
			})
			if len(vars) == 0 {
				continue
			}
			data, err := c.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			opts := compile.Options{Inputs: passThrough{}, Sessions: &testSessions{}, Sources: anySources{}, Groups: anyGroups{}}
			byContexts, byVariables := opts, opts
			byContexts.Contexts, byVariables.Variables = vars.contexts(), vars
			viaContexts, d1, err1 := compile.Create(t.Context(), "workflow.yaml", data, c.Inputs, byContexts)
			viaVariables, d2, err2 := compile.Create(t.Context(), "workflow.yaml", data, c.Inputs, byVariables)
			if err1 != nil || err2 != nil {
				t.Errorf("%s: %v, %v", c.Name, err1, err2)
				continue
			}
			compared++
			if fmt.Sprint(d1) != fmt.Sprint(d2) {
				t.Errorf("%s: diagnostics differ:\n contexts:  %v\n variables: %v", c.Name, d1, d2)
			}
			if (viaContexts == nil) != (viaVariables == nil) {
				t.Errorf("%s: one gave a plan and the other didn't", c.Name)
				continue
			}
			if viaContexts == nil {
				continue
			}
			if a, b := canonical(t, viaContexts.Plan), canonical(t, viaVariables.Plan); a != b {
				t.Errorf("%s: plans differ:\n contexts:  %s\n variables: %s", c.Name, a, b)
			}
		}
	}
	if compared < 20 {
		t.Errorf("compared %d cases", compared)
	}
}

// Create checks the workflow's sessions: with the session provider, which
// gives the sessions context, and a workflow with sessions and no provider
// is resolver_missing, unless Contexts gives the context.
func TestCreateChecksSessions(t *testing.T) {
	src := workflow(`name: {type: string, default: '${{ sessions.desktop }}'}`, "sessions:\n  desktop:\n    redirect: ${{ inputs.name != '' }}\n")
	s := &testSessions{}
	c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, compile.Options{Sessions: s})
	if err != nil || c == nil {
		t.Fatal(diags, err)
	}
	if got := c.Plan.Run.Inputs.Get().(map[string]any)["name"]; got != "desktop-checked" || !maps.Equal(c.Plan.Run.SessionNames, map[string]string{"desktop": "desktop-checked"}) {
		t.Errorf("name %v, session names %v", got, c.Plan.Run.SessionNames)
	}
	for name, opts := range map[string]compile.Options{
		"no provider": {},
		"a problem":   {Sessions: &testSessions{checkErr: problems.InputRejected.Problem(problems.Params{"input": "desktop", "reason": "no"})}},
	} {
		_, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, opts)
		if err != nil || len(diags.Errors()) != 1 || diags.Errors()[0].Path() != "/sessions" {
			t.Errorf("%s: %v, %v", name, diags, err)
		}
	}
	down := errors.New("down")
	if _, _, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, compile.Options{Sessions: &testSessions{checkErr: down}}); !errors.Is(err, down) {
		t.Errorf("a provider that couldn't answer: %v", err)
	}
	given := compile.Options{Contexts: map[string]any{"sessions": map[string]any{"desktop": "given"}}}
	if c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, given); err != nil || c == nil {
		t.Errorf("with the sessions context given: %v, %v", diags, err)
	}
}

// Create checks where a job's runs-on: runs it when the workflow writes it
// out whole: an execution group, a format 1 environment or a compute
// target, each asked for once. One an expression
// gives, in whole or in part, is known only once the job is ready.
func TestCreateChecksGroups(t *testing.T) {
	src := `on:
  execute:
    inputs:
      group: {type: string, default: hpc}
jobs:
  a:
    runs-on: {execution-group: gpus}
    steps:
      - run: echo
  b:
    runs-on:
      environment: {cluster: me/c1, name: batch}
      params: {partition: p}
    steps:
      - run: echo
  c:
    runs-on: {execution-group: '${{ inputs.group }}'}
    steps:
      - run: echo
  d:
    runs-on: {execution-group: gpus}
    steps:
      - run: echo
  f:
    runs-on: {mode: login, targetId: c1}
    steps:
      - run: echo
  g:
    runs-on:
      environment: {cluster: me/c1, name: batch}
      params: {partition: '${{ inputs.group }}'}
    steps:
      - run: echo
`
	ok := &testGroups{groups: []string{"gpus"}, environments: []string{"me/c1/batch"}, targets: []string{"login c1"}}
	if c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, compile.Options{Groups: ok}); err != nil || c == nil {
		t.Fatalf("%v, %v", diags, err)
	}
	if want := []string{"group gpus", "environment me/c1/batch map[partition:p]", "compute target login c1"}; !slices.Equal(ok.asked, want) {
		t.Errorf("asked %q, want %q", ok.asked, want)
	}
	pointers := []string{
		"/jobs/a/runs-on/execution-group", "/jobs/b/runs-on/environment", "/jobs/d/runs-on/execution-group", "/jobs/f/runs-on",
	}
	codes := func(codes ...string) map[string]string {
		m := map[string]string{}
		for i, p := range pointers {
			m[p] = codes[min(i, len(codes)-1)]
		}
		return m
	}
	for name, tc := range map[string]struct {
		groups compile.Groups
		codes  map[string]string
	}{
		"none":        {&testGroups{}, codes("group_unknown", "environment_unknown", "group_unknown", "compute_target_unknown")},
		"no resolver": {nil, codes("resolver_missing")},
		"a problem":   {&testGroups{err: problems.InputRejected.Problem(problems.Params{"input": "x", "reason": "no"})}, codes("input_rejected")},
	} {
		_, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, compile.Options{Groups: tc.groups})
		codes := map[string]string{}
		for _, d := range diags.Errors() {
			codes[d.Path()] = string(d.Code)
		}
		if err != nil || !maps.Equal(codes, tc.codes) {
			t.Errorf("%s: %v, %v, want %v", name, codes, err, tc.codes)
		}
	}
	down := errors.New("down")
	if _, _, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, compile.Options{Groups: &testGroups{err: down}}); !errors.Is(err, down) {
		t.Errorf("a resolver that couldn't answer: %v", err)
	}
}

// Create resolves each workflow a step's uses: names, with the file and the
// server its with: $yaml and $host name, pins it in the step, once for
// each reference, and loads it, whose errors are the run's. A built-in
// action names none.
func TestCreatePinsWorkflows(t *testing.T) {
	src := `jobs:
  a:
    steps:
      - uses: github/acme/flows@main
        with: {$yaml: sub/flow.yaml, n: 1}
      - uses: github/acme/flows@main
        with: {$yaml: sub/flow.yaml, n: 2}
      - uses: gitlab/team/flows
        with: {$yaml: w.yaml, $host: git.example.com}
      - uses: parallelworks/checkout
        with: {repo: https://example.com/r.git, branch: main}
`
	s := &testSources{texts: map[string]string{
		"github/acme/flows@main": "jobs:\n  x:\n    steps:\n      - run: echo\n",
		"gitlab/team/flows":      "jobs:\n  x:\n    steps:\n      - run: echo\n",
	}}
	c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, compile.Options{Sources: s})
	if err != nil || c == nil {
		t.Fatal(diags, err)
	}
	if want := []plan.SourceRef{{Uses: "github/acme/flows@main", Path: "sub/flow.yaml"}, {Uses: "gitlab/team/flows", Path: "w.yaml", Host: "git.example.com"}}; !slices.Equal(s.asked, want) {
		t.Errorf("asked %+v, want %+v", s.asked, want)
	}
	steps := c.Plan.Jobs[0].Steps
	for i, want := range []*plan.SourceRef{&s.asked[0], &s.asked[0], &s.asked[1], nil} {
		got := steps[i].Source
		if (got == nil) != (want == nil) || got != nil && (got.Ref != *want || got.Digest == "" || got.Revision == "") {
			t.Errorf("step %d's source %+v, want ref %+v", i, got, want)
		}
	}
	if steps[0].Source == steps[1].Source {
		t.Error("two steps share a source")
	}

	for name, tc := range map[string]struct {
		src     string
		sources compile.Sources
		code    string
		pointer string
	}{
		"not found": {"jobs:\n  a:\n    steps:\n      - uses: github/acme/none\n", &testSources{}, "source_not_found", "/jobs/a/steps/0/uses"},
		"a problem": {
			"jobs:\n  a:\n    steps:\n      - uses: github/acme/none\n",
			&testSources{err: problems.SourceRefInvalid.Problem(problems.Params{"ref": "github/acme/none"})}, "source_ref_invalid", "/jobs/a/steps/0/uses",
		},
		"no resolver": {"jobs:\n  a:\n    steps:\n      - uses: workflow/mine\n", nil, "resolver_missing", "/jobs/a/steps/0/uses"},
		"not written out": {
			"on:\n  execute:\n    inputs:\n      f: {type: string, default: w.yaml}\njobs:\n  a:\n    steps:\n      - uses: github/acme/flows\n        with: {$yaml: '${{ inputs.f }}'}\n",
			&testSources{}, "source_not_known", "/jobs/a/steps/0/with/$yaml",
		},
		"pinned to other text": {
			"jobs:\n  a:\n    steps:\n      - uses: github/acme/x\n",
			pinnedSources{plan.Source{Location: "github/acme/x", Revision: "0123456789abcdef0123456789abcdef01234567", Digest: "sha256:00"}},
			"source_changed", "/jobs/a/steps/0/uses",
		},
		"a workflow that doesn't load": {
			"jobs:\n  a:\n    steps:\n      - uses: github/acme/bad\n",
			&testSources{texts: map[string]string{"github/acme/bad": "jobs: [\n"}}, "yaml_syntax", "",
		},
	} {
		_, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(tc.src), nil, compile.Options{Sources: tc.sources})
		errs := diags.Errors()
		if err != nil || len(errs) != 1 || string(errs[0].Code) != tc.code || errs[0].Path() != tc.pointer {
			t.Errorf("%s: %v, %v, want %s at %q", name, diags, err, tc.code, tc.pointer)
			continue
		}
		if tc.code == "yaml_syntax" && !strings.HasPrefix(errs[0].File(), "github/acme/bad") {
			t.Errorf("%s: the error is in %q, not the workflow's file", name, errs[0].File())
		}
	}
	uses := []byte("jobs:\n  a:\n    steps:\n      - uses: github/acme/x\n")
	down := errors.New("down")
	if _, _, err := compile.Create(t.Context(), "workflow.yaml", uses, nil, compile.Options{Sources: &testSources{err: down}}); !errors.Is(err, down) {
		t.Errorf("a resolver that couldn't answer: %v", err)
	}
	// A resolver's digest is the text's, and without a revision, the
	// digest names the text, as it does the run's own.
	text := "jobs:\n  x:\n    steps:\n      - run: echo\n"
	sum := sha256.Sum256([]byte(text))
	digest := "sha256:" + hex.EncodeToString(sum[:])
	for _, given := range []plan.Source{{Location: "github/acme/x", Digest: digest}, {Location: "github/acme/x"}} {
		c, diags, err := compile.Create(t.Context(), "workflow.yaml", uses, nil, compile.Options{Sources: pinnedSources{given}})
		if err != nil || c == nil {
			t.Fatalf("%+v: %v, %v", given, diags, err)
		}
		if got := c.Plan.Jobs[0].Steps[0].Source; got.Digest != digest || got.Revision != digest {
			t.Errorf("%+v: pinned %+v", given, got)
		}
	}
	if _, _, err := compile.Create(t.Context(), "workflow.yaml", uses, nil, compile.Options{Sources: pinnedSources{nil}}); err == nil {
		t.Error("a resolver that gave nothing")
	}
	// An empty $yaml means workflow.yaml, as ACTIVATE reads it; the schema
	// refuses one that isn't text.
	for _, yaml := range []string{"''", `""`} {
		s := &testSources{texts: map[string]string{"github/acme/flows@main": text}}
		src := "jobs:\n  a:\n    steps:\n      - uses: github/acme/flows@main\n        with: {$yaml: " + yaml + "}\n"
		c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, compile.Options{Sources: s})
		if err != nil || c == nil || !slices.Equal(s.asked, []plan.SourceRef{{Uses: "github/acme/flows@main"}}) {
			t.Errorf("$yaml %s: asked %v, %v, %v", yaml, s.asked, diags, err)
		}
	}
}

// pinnedSources gives source, with a workflow of one job, for every
// reference, or nothing, without an error, when source is nil.
type pinnedSources struct{ source any }

func (s pinnedSources) Resolve(context.Context, plan.SourceRef) (*compile.Workflow, error) {
	source, ok := s.source.(plan.Source)
	if !ok {
		return nil, nil
	}
	return &compile.Workflow{Source: source, Text: []byte("jobs:\n  x:\n    steps:\n      - run: echo\n")}, nil
}

func (pinnedSources) Read(context.Context, plan.Source) (*compile.Workflow, error) {
	return nil, errors.New("not read as the run is created")
}
