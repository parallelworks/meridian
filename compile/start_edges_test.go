package compile_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
)

// The rows of a list that a part Start evaluates gives, all of the list's
// or one of its rows, a list's in a row among them, take their fields'
// defaults, as when the run is created they do: the plan holds the part's
// expression, and not what Create filled in of it.
func TestStartFillsRowsOfWhatItEvaluates(t *testing.T) {
	for name, tc := range map[string]struct {
		defs string
		vars testVariables
		want string
	}{
		"a list in a row, from a plain variable": {
			defs: `l: {type: list, default: [{}], template: {inner: {type: list, default: '${{ fromJSON(org.INNER) }}', template: {n: {type: string, default: dflt}, m: {type: string}}}}}`,
			vars: testVariables{"org.INNER": {Value: `[{"m":"a"}]`}},
			want: `{"l":[{"inner":[{"m":"a","n":"dflt"}]}]}`,
		},
		"a list in a row, from a secret": {
			defs: `l: {type: list, default: [{}], template: {inner: {type: list, default: '${{ fromJSON(vars.INNER) }}', template: {n: {type: string, default: dflt}, m: {type: string}}}}}`,
			vars: testVariables{"vars.INNER": {Value: `[{"m":"a"}]`, Secret: true}},
			want: `{"l":[{"inner":[{"m":"a","n":"dflt"}]}]}`,
		},
		"a row an expression gives": {
			defs: `hosts: {type: list, default: ['${{ fromJSON(vars.ROW) }}'], template: {name: {type: string}, site: {type: string, default: east}}}`,
			vars: testVariables{"vars.ROW": {Value: `{"name":"a"}`, Secret: true}},
			want: `{"hosts":[{"name":"a","site":"east"}]}`,
		},
		"a row's field named default": {
			defs: `l: {type: list, default: [{default: '${{ vars.X }}'}], template: {default: {type: string}}}`,
			vars: testVariables{"vars.X": {Value: "", Secret: true}},
			want: `{"l":[{"default":""}]}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, res, diags := started(t, workflow(tc.defs), nil, tc.vars)
			if res == nil {
				t.Fatal(diags)
			}
			if got := canonical(t, res.Run.Inputs.Get()); got != tc.want {
				t.Errorf("inputs %s, want %s", got, tc.want)
			}
		})
	}
}

// A list's default rows can mix a row an expression gives with rows
// written out: a row that is a mapping, in the place of the expression, is
// compared with what it gives, not taken for a form's text, which is a
// string. Creating such a run panicked.
func TestDefaultRowAnExpressionGives(t *testing.T) {
	src := workflow(`hosts: {type: list, default: ['${{ fromJSON(org.FIRST) }}', {name: b}], template: {name: {type: string}}}`)
	opts := compile.Options{Contexts: map[string]any{"org": map[string]any{"FIRST": `{"name":"a"}`}}}
	for _, submitted := range []map[string]any{nil, {"hosts": []any{map[string]any{"name": "a"}, map[string]any{"name": "b"}}}} {
		if p, diags := compile.Compile("workflow.yaml", []byte(src), submitted, opts); p == nil {
			t.Errorf("%v: %v", submitted, diags)
		}
	}
}

// What Start evaluates reads the inputs as Create's evaluations read
// them, before Create converted them to their types, as a form sends a
// number's text; what the run reads after it is converted.
func TestStartReadsInputsAsCreateDid(t *testing.T) {
	src := workflow(`cpus: {type: number}
flag: {type: boolean}
label: {type: string, default: '${{ org.PREFIX }}-${{ inputs.cpus }}-${{ inputs.flag }}'}
x: {type: string, optional: true, hidden: '${{ org.PREFIX == "p" && inputs.flag == "false" }}'}`, `env:
  CPUS: ${{ inputs.cpus }}
jobs:
  main:
    needs: ${{ fromJSON(org.NEEDS) }}
    steps:
      - run: echo
  other:
    steps:
      - run: echo
`)
	vars := testVariables{"org.PREFIX": {Value: "p"}, "org.NEEDS": {Value: `["other"]`}}
	c, res, diags := started(t, src, map[string]any{"cpus": "2.50", "flag": "false"}, vars)
	if res == nil {
		t.Fatal(diags)
	}
	if want := `{"cpus":2.5,"flag":false,"label":"p-2.50-false"}`; canonical(t, res.Run.Inputs.Get()) != want {
		t.Errorf("inputs %s, want %s", canonical(t, res.Run.Inputs.Get()), want)
	}
	if got := res.Run.Env["CPUS"]; got != "2.50" {
		t.Errorf("env.CPUS = %q, want the text it was given", got)
	}
	if got := canonical(t, c.Plan.Run.Unconverted); got != `{"cpus":"2.50","flag":"false"}` {
		t.Errorf("unconverted %s", got)
	}
}

// Start evaluates what reads what in the order they read each other, each
// once, as Create does, so that a run Create accepted doesn't run out of
// the group's budget as it starts, however its inputs are ordered.
func TestStartEvaluatesEachOnce(t *testing.T) {
	const n = 100
	var defs strings.Builder
	fmt.Fprintf(&defs, "big: {type: string, default: '%s'}\n", strings.Repeat("x", 20000))
	for i := range n - 1 {
		fmt.Fprintf(&defs, "i%04d: {type: string, default: '${{ length(toJSON(inputs.big)) }}${{ inputs.i%04d }}'}\n", i, i+1)
	}
	fmt.Fprintf(&defs, "i%04d: {type: string, default: '${{ vars.X }}'}\n", n-1)
	vars := testVariables{"vars.X": {Value: "x", Secret: true}}
	c := createRun(t, workflow(defs.String()), nil, vars)
	began := time.Now()
	res, diags := startRun(t, c, vars, compile.StartInput{})
	if res == nil {
		t.Fatalf("%d problems, the first %v", len(diags), diags[0])
	}
	if got := res.Run.Inputs.Get().(map[string]any)["i0000"]; got != strings.Repeat("20002", n-1)+"x" {
		t.Errorf("i0000 = %.40v", got)
	}
	t.Logf("started in %v", time.Since(began))
}

// A plan that isn't one the compiler made is an error, not a panic or the
// run's problem.
func TestStartRefusesMalformedPlans(t *testing.T) {
	src := workflow(`l: {type: list, template: {pass: {type: password}}}
a: {type: string, default: '${{ vars.A }}', hidden: '${{ vars.A == "" }}'}`)
	vars := testVariables{"vars.A": {Value: "a"}}
	c := createRun(t, src, map[string]any{"l": []any{map[string]any{"pass": "p"}}}, vars)
	for name, change := range map[string]func(p *plan.Plan, in *compile.StartInput){
		"a held input at a huge index": func(p *plan.Plan, _ *compile.StartInput) {
			p.Run.DeferredInputs["l.9223372036854775807"] = p.Run.DeferredInputs["a"]
		},
		"a password at a huge index": func(p *plan.Plan, in *compile.StartInput) {
			p.Run.Secrets = append(p.Run.Secrets, "inputs.l.100000000000.pass")
			in.Passwords = map[string]any{"inputs.l.100000000000.pass": "p"}
		},
		"a decision of no condition": func(p *plan.Plan, _ *compile.StartInput) {
			p.Run.Decisions = append(p.Run.Decisions, plan.Decision{Path: "/jobs/main"})
		},
		"a part outside the definitions": func(p *plan.Plan, _ *compile.StartInput) {
			p.Run.DeferredParts[0].Location = "/jobs/main/steps/0/run"
		},
	} {
		t.Run(name, func(t *testing.T) {
			again := createRun(t, src, map[string]any{"l": []any{map[string]any{"pass": "p"}}}, vars)
			in := compile.StartInput{Variables: vars, Passwords: c.Passwords, Inputs: passThrough{}}
			change(again.Plan, &in)
			if res, diags, err := compile.Start(t.Context(), again.Plan, in); err == nil {
				t.Errorf("no error: %v, %v", res, diags)
			}
		})
	}
}

// A private value the run was given that holds a password, as a list
// typed over a default that reads a plain variable does, and the password
// it holds, are each put in their place, whichever is given first.
func TestStartRestoresPrivateAroundPasswords(t *testing.T) {
	src := workflow(`creds:
  type: list
  template: {user: {type: string}, pass: {type: password}}
  default: ${{ fromJSON(org.CREDS) }}`)
	vars := testVariables{"org.CREDS": {Value: `[{"user":"b","pass":"x"}]`}}
	c, res, diags := started(t, src, map[string]any{"creds": []any{map[string]any{"user": "a", "pass": "HUNTER2"}}}, vars)
	if res == nil {
		t.Fatal(diags)
	}
	if _, ok := c.Private["inputs.creds"]; !ok {
		t.Fatalf("private %v", c.Private)
	}
	if got := canonical(t, res.Run.Inputs.Get()); got != `{"creds":[{"pass":"HUNTER2","user":"a"}]}` {
		t.Errorf("inputs %s", got)
	}
	_, state := res.Run.Split()
	if strings.Contains(canonical(t, state), "HUNTER2") {
		t.Errorf("the run state holds the password: %s", canonical(t, state))
	}
}
