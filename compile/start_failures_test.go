package compile_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/problems"
)

// What fails to evaluate as the run starts fails the run, with the
// problem at what failed, and shows nothing a variable or a password made:
// the workflow's env:, a job's needs:, an input's condition and a value
// held for the start, each made to fail by a variable that changed since
// the run was created, or that is a secret, which Create doesn't read; a
// status function, which no input reads before a job runs; and an input
// with no value whose ignore read a variable, whether it is required Start
// decides.
func TestStartFailures(t *testing.T) {
	plain := func(v string) testVariables { return testVariables{"vars.V": {Value: v}} }
	secret := testVariables{"vars.S": {Value: "s3cr3t{", Secret: true}}
	for name, tc := range map[string]struct {
		defs, rest    string
		submitted     map[string]any
		create, start testVariables
		code, pointer string
		hidden        string
	}{
		"an env: value": {
			"a: {type: string, optional: true}", "env:\n  E: ${{ fromJSON(vars.S) }}\n", nil, secret, secret,
			"from_json_invalid", "/env/E", "s3cr3t",
		},
		"a job's needs:": {
			"a: {type: string, optional: true}", "jobs:\n  a:\n    steps:\n      - run: echo\n  b:\n    needs: ${{ fromJSON(vars.V) }}\n    steps:\n      - run: echo\n",
			nil, plain(`["a"]`), plain(`[a{`), "from_json_invalid", "/jobs/b/needs", "a{",
		},
		"an input's hidden": {
			"x: {type: string, optional: true, hidden: '${{ fromJSON(vars.V) }}'}", "", nil, plain("true"), plain("tru{"),
			"from_json_invalid", "/on/execute/inputs/x/hidden", "tru{",
		},
		"an input's ignore": {
			"x: {type: string, optional: true, ignore: '${{ fromJSON(vars.V).off }}'}", "", nil, plain(`{"off":false}`), plain("of{"),
			"from_json_invalid", "/on/execute/inputs/x/ignore", "of{",
		},
		"a default held for the start": {
			"x: {type: string, default: '${{ fromJSON(vars.S) }}'}", "", nil, secret, secret,
			"from_json_invalid", "/on/execute/inputs/x/default", "s3cr3t",
		},
		"a status function": {
			"x: {type: string, default: '${{ vars.S }}${{ success() }}'}", "", nil, secret, secret,
			"status_not_known", "/on/execute/inputs/x/default", "s3cr3t",
		},
		"a required input whose ignore read a variable": {
			"x: {type: string, ignore: '${{ vars.V == \"skip\" }}'}", "", nil, plain("keep"), plain("keep"),
			"input_required", "/on/execute/inputs/x", "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			src := workflow(tc.defs, tc.rest)
			c := createRun(t, src, tc.submitted, tc.create)
			res, diags, err := compile.Start(t.Context(), c.Plan, compile.StartInput{RunID: "r", Variables: tc.start, Record: c.Record, Inputs: passThrough{}})
			if err != nil || res != nil {
				t.Fatalf("Start: %v, %v, want the run's problem", res, err)
			}
			errs := diags.Errors()
			if len(errs) != 1 || string(errs[0].Code) != tc.code || errs[0].Path() != tc.pointer {
				t.Fatalf("diagnostics %v, want %s at %s", diags, tc.code, tc.pointer)
			}
			if tc.hidden != "" && strings.Contains(fmt.Sprint(diags), tc.hidden) {
				t.Errorf("the problem shows %q: %v", tc.hidden, diags)
			}
		})
	}

	// A boolean whose ignore read a variable, which Start decides, is
	// false with no value, as one with none is.
	src := workflow("b: {type: boolean, ignore: '${{ vars.V == \"skip\" }}'}")
	c := createRun(t, src, nil, plain("keep"))
	res, diags := startRun(t, c, plain("keep"), compile.StartInput{})
	if res == nil {
		t.Fatal(diags)
	}
	if got := canonical(t, res.Run.Inputs.Get()); got != `{"b":false}` {
		t.Errorf("the run's inputs are %s, want b false", got)
	}
}

// A session provider that couldn't make the sessions is Start's error,
// which the coordinator starts the run again after, and one that refuses
// them with a problem of a workflow's kind of error is the run's problem.
func TestStartSessionsFail(t *testing.T) {
	src := workflow("a: {type: string, optional: true}", "sessions:\n  desktop:\n    useTLS: false\n")
	down := errors.New("the session provider is down")
	c := createWith(t, src, nil, compile.Options{Inputs: passThrough{}, Sessions: &testSessions{}})
	if _, _, err := compile.Start(t.Context(), c.Plan, compile.StartInput{RunID: "r", Record: c.Record, Variables: testVariables{}, Inputs: passThrough{}, Sessions: &testSessions{err: down}}); !errors.Is(err, down) || errors.Is(err, compile.ErrMismatch) {
		t.Errorf("a provider that couldn't answer: %v", err)
	}
	refused := problems.ResolverMissing.Problem(problems.Params{"key": "sessions", "interface": "Sessions"})
	res, diags, err := compile.Start(t.Context(), c.Plan, compile.StartInput{RunID: "r", Record: c.Record, Variables: testVariables{}, Inputs: passThrough{}, Sessions: &testSessions{err: refused}})
	if err != nil || res != nil || len(diags.Errors()) != 1 || diags.Errors()[0].Path() != "/sessions" {
		t.Errorf("a provider that refused: %v, %v, %v", res, diags, err)
	}
}

// Create with a Variables resolver that refuses a name with a problem
// reports it, as the run's; one that couldn't answer is Create's error.
func TestCreateVariablesFail(t *testing.T) {
	src := workflow("x: {type: string, default: '${{ vars.V }}'}")
	down := errors.New("the variables store is down")
	if c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, compile.Options{Inputs: passThrough{}, Variables: failingVariables{down}}); !errors.Is(err, down) || c != nil || diags != nil {
		t.Errorf("a resolver that couldn't answer: %v, %v, %v", c, diags, err)
	}
	refused := problems.ResolverMissing.Problem(problems.Params{"key": "vars", "interface": "Variables"})
	c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, compile.Options{Inputs: passThrough{}, Variables: failingVariables{refused}})
	if err != nil || c != nil || len(diags.Errors()) != 1 || string(diags.Errors()[0].Code) != "resolver_missing" {
		t.Errorf("a resolver that refused: %v, %v, %v", c, diags, err)
	}
}
