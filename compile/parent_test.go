package compile_test

import (
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/internal/schematest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// childWorkflow is a workflow a step calls: inputs its with: gives, one
// whose default reads one of them, and an env: that reads the calling
// step's environment, which it is laid over, and lays its own STAGE over
// it, which one of its values reads.
const childWorkflow = `on:
  execute:
    inputs:
      region: {type: string}
      label: {type: string, default: '${{ inputs.region }}-x'}
      plain: {type: string}
      creds: {type: group, items: {user: {type: string}, key: {type: string}}}
      pw: {type: password, optional: true}
env:
  STAGE: own
  OWN_STAGE: ${{ env.STAGE }}-x
  FROM_TOKEN: ${{ env.TOKEN }}
  FROM_REGION: ${{ env.REGION }}
  FROM_PLAIN: ${{ env.CALLER }}
jobs:
  main:
    steps:
      - run: echo ${{ env.CALLER }} ${{ env.PW_MATRIX_INDEX }}
`

// childParent is what the step that calls childWorkflow gives it: its
// try, its environment and what of it, and of its with:, is private.
func childParent() *compile.Parent {
	return &compile.Parent{
		Run: "7", Job: "deploy#1", Step: 2, Try: 1, Root: "3", Depth: 2,
		Env: map[string]string{
			"CALLER": "caller", "STAGE": "prod", "PW_MATRIX_INDEX": "1",
			"REGION": "eu-west-private", "TOKEN": "t0k3n-0123456789",
		},
		Private: []string{"env.REGION", "inputs.region", "inputs.extra"},
		Secrets: []string{"env.TOKEN", "inputs.creds.key", "inputs.pw"},
		Masked:  []string{"t0k3n-0123456789", "hunter2"},
	}
}

// childWith is the calling step's with:, as Step gave it.
func childWith() map[string]any {
	return map[string]any{
		"$yaml": "w.yaml", "$host": "github.com", "$thumbnail": "t.png",
		"region": "us-east-private", "plain": "p",
		"creds": map[string]any{"user": "u", "key": "k3y-0123456789"},
		"pw":    "hunter2", "extra": "unknown-private",
	}
}

// A child run's Create takes its inputs from its step's with:, but for
// $yaml, $host and $thumbnail, and keeps out of its plan what of them the
// step made from a variable or a password, in its secret record, as a
// password is, and what reads one, as an input's default does, for Start;
// with what the step made from a secret, which is masked as what is made
// from one is. The plan holds the link to its step, of version 2, and the
// step's environment but for what of it is private, which the record
// holds, with the strings the step's worker masks. The step's environment
// counts as defined, and nothing private is anywhere in the plan.
func TestCreateChild(t *testing.T) {
	c, diags, err := compile.Create(t.Context(), "w.yaml", []byte(childWorkflow), childWith(), compile.Options{Parent: childParent()})
	if err != nil || c == nil {
		t.Fatal(diags, err)
	}
	for _, d := range diags {
		if d.Code != problems.UnknownInput.Code() || d.Params["input"] != "extra" {
			t.Errorf("a problem: %v", d)
		}
	}
	p := c.Plan
	want := plan.ParentRun{
		Run: "7", Job: "deploy#1", Step: 2, Try: 1, Root: "3", Depth: 2,
		Env:     map[string]string{"CALLER": "caller", "STAGE": "prod", "PW_MATRIX_INDEX": "1"},
		Private: []string{"env.REGION", "inputs.extra", "inputs.region"},
		Secrets: []string{"env.TOKEN", "inputs.creds.key", "inputs.pw"},
	}
	if got := p.Run.Parent; got == nil || canonical(t, *got) != canonical(t, want) {
		t.Errorf("the plan's parent is %+v, want %+v", got, want)
	}
	if p.Version != 2 || c.Record.Version != 2 {
		t.Errorf("a child run's plan is of version %d, and its record %d", p.Version, c.Record.Version)
	}
	if got := canonical(t, p.Run.Inputs); got != `{"creds":{"user":"u"},"plain":"p"}` {
		t.Errorf("the plan holds the inputs %s", got)
	}
	for name, got := range map[string][]string{"secrets": p.Run.Secrets, "derived": p.Run.Derived, "private": p.Run.Private} {
		if want := map[string][]string{
			"secrets": {"inputs.creds.key", "inputs.pw"},
			"derived": {"inputs.creds.key"},
			"private": {"inputs.extra", "inputs.region"},
		}[name]; !slices.Equal(got, want) {
			t.Errorf("the plan's %s are %q, want %q", name, got, want)
		}
	}
	submitted, _ := c.Record.Submitted.Get().(map[string]any)
	if _, ok := submitted["$yaml"]; ok || submitted["region"] != "us-east-private" || submitted["extra"] != "unknown-private" {
		t.Errorf("the record holds what was submitted as %v", submitted)
	}
	if in := c.Record.Inherited; in == nil || canonical(t, in.Env) != `{"REGION":"eu-west-private","TOKEN":"t0k3n-0123456789"}` || !slices.Equal(in.Masked, []string{"t0k3n-0123456789", "hunter2"}) {
		t.Errorf("the record holds of the step %+v", in)
	}
	data, err := plan.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"us-east-private", "eu-west-private", "t0k3n", "k3y-", "hunter2", "unknown-private"} {
		if strings.Contains(string(data), private) {
			t.Errorf("the plan holds %s:\n%s", private, data)
		}
	}
	schematest.Validate(t, schematest.Schema(t, "plan/schema/2/plan.schema.json"), "the plan", data)
	if back, err := plan.Decode(data); err != nil || back.Run.Parent == nil {
		t.Errorf("the plan decodes as %v, %v", back, err)
	}

	// A run of the same workflow that no step calls has no environment
	// under its env:, which reads no env, and its step reads CALLER as
	// undefined.
	with := map[string]any{"region": "r", "plain": "p", "creds": map[string]any{"user": "u", "key": "k"}}
	_, diags, err = compile.Create(t.Context(), "w.yaml", []byte(childWorkflow), with, compile.Options{})
	if err != nil {
		t.Fatal(err)
	}
	codes := map[string][]string{}
	for _, d := range diags {
		codes[string(d.Code)] = append(codes[string(d.Code)], d.Path())
	}
	if want := map[string][]string{
		string(problems.ContextNotKnown.Code()): {"/env/OWN_STAGE", "/env/FROM_TOKEN", "/env/FROM_REGION", "/env/FROM_PLAIN"},
		string(problems.UndefinedEnv.Code()):    {"/jobs/main/steps/0/run"},
	}; canonical(t, codes) != canonical(t, want) {
		t.Errorf("a run of its own: %v, want %v", codes, want)
	}
}

// A value the step gives that is what an input's default makes of a part
// it gave that is private is held too, as one a form sends that a default
// made from a variable is: the default reads what the step made private,
// so what it gives is private too, though the step gave it as it is.
func TestCreateChildHoldsWhatAGivenPartMakes(t *testing.T) {
	with := childWith()
	with["label"] = "us-east-private-x"
	c, diags, err := compile.Create(t.Context(), "w.yaml", []byte(childWorkflow), with, compile.Options{Parent: childParent()})
	if err != nil || c == nil {
		t.Fatal(diags, err)
	}
	data, err := plan.Encode(c.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "us-east-private") {
		t.Errorf("the plan holds what the default made of a private part:\n%s", data)
	}
	if !slices.Contains(c.Plan.Run.Private, "inputs.label") {
		t.Errorf("the plan's private values are %q, without inputs.label", c.Plan.Run.Private)
	}
}

// What the step names as private must be a part of its with: or a variable
// of its environment, and once: anything else is its caller's mistake.
func TestCreateChildRefusesParents(t *testing.T) {
	for name, change := range map[string]func(p *compile.Parent){
		"a variable it doesn't have": func(p *compile.Parent) { p.Private = append(p.Private, "env.NONE") },
		"another context":            func(p *compile.Parent) { p.Secrets = append(p.Secrets, "vars.TOKEN") },
		"the whole with:":            func(p *compile.Parent) { p.Private = append(p.Private, "inputs") },
		// A part of a value that holds nothing: region is text.
		"a part of a text":       func(p *compile.Parent) { p.Private = []string{"env.REGION", "inputs.region.x"} },
		"a part it doesn't have": func(p *compile.Parent) { p.Secrets = append(p.Secrets, "inputs.missing") },
		"a part of where it is":  func(p *compile.Parent) { p.Secrets = append(p.Secrets, "inputs.$yaml") },
		"twice":                  func(p *compile.Parent) { p.Secrets = append(p.Secrets, "inputs.region") },
	} {
		p := childParent()
		change(p)
		if c, _, err := compile.Create(t.Context(), "w.yaml", []byte(childWorkflow), childWith(), compile.Options{Parent: p}); err == nil || c != nil {
			t.Errorf("a parent that names %s: %v", name, err)
		}
	}
}

// A child run's Start lays the workflow's env: over the calling step's
// environment, whose values its expressions read, the private ones from
// the secret record; and what the step gave that is private is so in the
// run's values, those made from a secret secrets, derived, as is what is
// made from them, and kept out of run state.
func TestStartChild(t *testing.T) {
	c := createWith(t, childWorkflow, childWith(), compile.Options{Parent: childParent()})
	res, diags := startRun(t, c, nil, compile.StartInput{RunID: "run"})
	if res == nil {
		t.Fatal(diags)
	}
	run := res.Run
	want := map[string]string{
		"CALLER": "caller", "PW_MATRIX_INDEX": "1", "REGION": "eu-west-private", "TOKEN": "t0k3n-0123456789",
		"STAGE": "own", "OWN_STAGE": "own-x", "FROM_TOKEN": "t0k3n-0123456789", "FROM_REGION": "eu-west-private", "FROM_PLAIN": "caller",
	}
	if !maps.Equal(run.Env, want) {
		t.Errorf("the run's env is %v, want %v", run.Env, want)
	}
	inputs, _ := run.Inputs.Get().(map[string]any)
	if inputs["region"] != "us-east-private" || inputs["label"] != "us-east-private-x" || inputs["extra"] != "unknown-private" || inputs["pw"] != "hunter2" {
		t.Errorf("the run's inputs are %v", inputs)
	}
	for name, got := range map[string][]string{"secrets": run.Secrets, "derived": run.Derived, "private": run.Private} {
		if want := map[string][]string{
			"secrets": {"env.FROM_TOKEN", "env.TOKEN", "inputs.creds.key", "inputs.pw"},
			"derived": {"env.FROM_TOKEN", "env.TOKEN", "inputs.creds.key"},
			"private": {"env.FROM_REGION", "env.REGION", "inputs.extra", "inputs.label", "inputs.region"},
		}[name]; !slices.Equal(got, want) {
			t.Errorf("the run's %s are %q, want %q", name, got, want)
		}
	}
	_, state := run.Split()
	data := canonical(t, state)
	for _, private := range []string{"us-east-private", "eu-west-private", "t0k3n", "k3y-", "hunter2", "unknown-private"} {
		if strings.Contains(data, private) {
			t.Errorf("run state holds %s: %s", private, data)
		}
	}

	// A record that doesn't hold what the plan names of the step's
	// environment isn't the run's.
	for name, inherited := range map[string]*plan.Inherited{
		"none":    nil,
		"one":     {Env: map[string]string{"REGION": "eu-west-private"}},
		"another": {Env: map[string]string{"REGION": "x", "TOKEN": "y", "OTHER": "z"}},
	} {
		in := c.Record
		in.Inherited = inherited
		if _, _, err := compile.Start(t.Context(), c.Plan, compile.StartInput{RunID: "run", Record: in, Inputs: passThrough{}}); !errors.Is(err, compile.ErrMismatch) {
			t.Errorf("a record that holds %s of the step's environment: %v", name, err)
		}
	}
	// Nor is a record of a run no step calls that holds a step's
	// environment.
	own := createWith(t, workflow("a: {type: string, optional: true}"), nil, compile.Options{})
	in := own.Record
	in.Inherited = &plan.Inherited{Env: map[string]string{"TOKEN": "y"}}
	if _, _, err := compile.Start(t.Context(), own.Plan, compile.StartInput{RunID: "run", Record: in, Inputs: passThrough{}}); !errors.Is(err, compile.ErrMismatch) {
		t.Errorf("a record of a run of its own that holds a step's environment: %v", err)
	}

	// A value of a child's env: that reads its own name, which format 1
	// reads from the step's environment under it, isn't built: the run
	// fails as one whose values read each other does.
	parent := &compile.Parent{Run: "7", Job: "j", Root: "3", Depth: 1, Env: map[string]string{"STAGE": "prod"}}
	selfRead := createWith(t, "env:\n  STAGE: ${{ env.STAGE }}-x\njobs:\n  main:\n    steps:\n      - run: echo\n", nil, compile.Options{Parent: parent})
	if _, diags := startRun(t, selfRead, nil, compile.StartInput{RunID: "run"}); len(diags.Errors()) != 1 || diags.Errors()[0].Code != problems.ValuesReadEachOther.Code() {
		t.Errorf("a child's env: value that reads its own name: %v", diags)
	}
}
