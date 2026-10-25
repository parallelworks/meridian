package compile_test

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// testVariables are variables a test gives Create and Start, by their names
// as the Variables interface reads them: a context, a dot and the name as
// it is, as in vars.TOKEN or vars.my.token.
type testVariables map[string]compile.Variable

// Read gives the variables names name, as the interface says, and counts
// in reads each time it is asked, and what it is asked for.
func (v testVariables) Read(_ context.Context, names []string, secrets bool) (map[string]compile.Variable, error) {
	out := map[string]compile.Variable{}
	for _, name := range names {
		_, _, one := strings.Cut(name, ".")
		for k, x := range v {
			if one && k != name || !one && !strings.HasPrefix(k, name+".") {
				continue
			}
			if x.Secret && !secrets {
				x.Value = ""
			}
			out[k] = x
		}
	}
	return out, nil
}

// contexts gives the variables as Options.Contexts takes them when the run
// is created: each plain one's value, and each secret one unknown.
func (v testVariables) contexts() map[string]any {
	out := map[string]any{"org": map[string]any{}, "vars": map[string]any{}}
	for k, x := range v {
		context, name, _ := strings.Cut(k, ".")
		var value any = x.Value
		if x.Secret {
			value = expr.Unknown{Name: context}
		}
		out[context].(map[string]any)[name] = value
	}
	return out
}

// caseVariables gives a conformance case's variables, its contexts' org
// and vars, with each secret one secret, and each value as value gives it.
func caseVariables(c conformance.Case, value func(name, v string) string) testVariables {
	out := testVariables{}
	for _, context := range []string{"org", "vars"} {
		vars, _ := c.Contexts[context].(map[string]any)
		for name, v := range vars {
			s, _ := v.(string)
			out[context+"."+name] = compile.Variable{Value: value(name, s)}
		}
	}
	for _, name := range c.SecretVariables {
		v := out["vars."+name]
		v.Secret = true
		if v.Value == "" {
			v.Value = value(name, "")
		}
		out["vars."+name] = v
	}
	return out
}

// countingVariables counts what the Variables interface is asked.
type countingVariables struct {
	testVariables
	asked [][]string
}

func (v *countingVariables) Read(ctx context.Context, names []string, secrets bool) (map[string]compile.Variable, error) {
	v.asked = append(v.asked, slices.Clone(names))
	return v.testVariables.Read(ctx, names, secrets)
}

// createRun creates a run of src with submitted, and the variables vars
// gives when the run is created, and returns what Create gave, with its
// plan as a coordinator stores it and reads it back.
func createRun(t testing.TB, src string, submitted map[string]any, vars testVariables) *compile.Created {
	t.Helper()
	return createWith(t, src, submitted, compile.Options{Inputs: passThrough{}, Contexts: vars.contexts()})
}

// createWith creates a run of src with submitted and opts, as createRun
// does.
func createWith(t testing.TB, src string, submitted map[string]any, opts compile.Options) *compile.Created {
	t.Helper()
	c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), submitted, opts)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatalf("Create: %v", diags)
	}
	data, err := plan.Encode(c.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if c.Plan, err = plan.Decode(data); err != nil {
		t.Fatal(err)
	}
	return c
}

// startRun starts the run c, as a coordinator does: with the secret record
// Create gave, the Variables interface vars, and in's other resolvers, the
// platform a passThrough unless in gives one.
func startRun(t testing.TB, c *compile.Created, vars compile.Variables, in compile.StartInput) (*compile.StartResult, problems.Diagnostics) {
	t.Helper()
	in.Variables = vars
	in.Record = c.Record
	if in.Inputs == nil {
		in.Inputs = passThrough{}
	}
	res, diags, err := compile.Start(t.Context(), c.Plan, in)
	if err != nil {
		t.Fatal(err)
	}
	if (res == nil) != diags.HasErrors() {
		t.Fatalf("result %v with errors %v", res != nil, diags)
	}
	return res, diags
}

// started creates a run of src and starts it, with the variables vars
// gives, both times.
func started(t *testing.T, src string, submitted map[string]any, vars testVariables) (*compile.Created, *compile.StartResult, problems.Diagnostics) {
	t.Helper()
	c := createRun(t, src, submitted, vars)
	res, diags := startRun(t, c, vars, compile.StartInput{RunID: "run"})
	return c, res, diags
}

// canonical returns v's JSON.
func canonical(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// workflow returns a workflow with the input definitions defs, in YAML,
// and rest, its other keys, with one job if rest has none.
func workflow(defs string, rest ...string) string {
	var b strings.Builder
	b.WriteString("on:\n  execute:\n    inputs:\n")
	for line := range strings.SplitSeq(strings.TrimRight(defs, "\n"), "\n") {
		b.WriteString("      " + line + "\n")
	}
	jobs := false
	for _, r := range rest {
		b.WriteString(r)
		jobs = jobs || strings.HasPrefix(r, "jobs:") || strings.Contains(r, "\njobs:")
	}
	if !jobs {
		b.WriteString("jobs:\n  main:\n    steps:\n      - run: echo\n")
	}
	return b.String()
}

// Start completes the run's values: the inputs the plan holds for it,
// which read variables, secret ones too, and the workflow's env, in the
// order they read each other, the workflow's env: and timeout:, and the
// matrices whose values are known. What it made from a secret is a
// secret, and from a variable or a password private, by name.
func TestStartCompletesTheRunsValues(t *testing.T) {
	src := workflow(`region: {type: string, default: '${{ org.REGION }}'}
token: {type: string, default: '${{ vars.TOKEN }}-x'}
dir: {type: string, default: '${{ env.BASE }}/run'}
size: {type: number, default: 2}
plain: {type: string, default: '${{ inputs.size }}0'}`, `env:
  BASE: /data/${{ inputs.size }}
  TOK: ${{ inputs.token }}
  AT: ${{ inputs.dir }}
timeout: ${{ inputs.size * 60 }}
jobs:
  main:
    strategy:
      matrix:
        r: ['${{ inputs.region }}', b]
    steps:
      - run: echo
  later:
    needs: [main]
    strategy:
      matrix:
        o: ['${{ needs.main.outputs.o }}']
    steps:
      - run: echo
`)
	vars := testVariables{"org.REGION": {Value: "east"}, "vars.TOKEN": {Value: "s3cr3t", Secret: true}}
	_, res, diags := started(t, src, nil, vars)
	if res == nil {
		t.Fatal(diags)
	}
	want := map[string]any{"region": "east", "token": "s3cr3t-x", "dir": "/data/2/run", "size": int64(2), "plain": "20"}
	if g, w := canonical(t, res.Run.Inputs.Get()), canonical(t, want); g != w {
		t.Errorf("inputs %s, want %s", g, w)
	}
	if want := map[string]string{"BASE": "/data/2", "TOK": "s3cr3t-x", "AT": "/data/2/run"}; !maps.Equal(res.Run.Env, want) {
		t.Errorf("env %v, want %v", res.Run.Env, want)
	}
	if want := []string{"env.TOK", "inputs.token", "vars.TOKEN"}; !slices.Equal(res.Run.Secrets, want) {
		t.Errorf("secrets %v, want %v", res.Run.Secrets, want)
	}
	if want := []string{"inputs.region"}; !slices.Equal(res.Run.Private, want) {
		t.Errorf("private %v, want %v", res.Run.Private, want)
	}
	if want := map[string]string{"REGION": "east"}; !maps.Equal(res.Run.Org, want) {
		t.Errorf("org %v, want %v", res.Run.Org, want)
	}
	if res.TimeoutSeconds != 120 || res.TimeoutPrivate {
		t.Errorf("timeout %d, private %v", res.TimeoutSeconds, res.TimeoutPrivate)
	}
	x := res.Expansions["main"]
	if x == nil || len(x.Instances) != 2 || !x.Instances[0].Private || x.Instances[0].Secret ||
		canonical(t, x.Instances[0].Matrix.Get()) != `{"r":"east"}` {
		t.Errorf("main's expansion %s", canonical(t, x))
	}
	if _, ok := res.Expansions["later"]; ok {
		t.Error("Start expanded a matrix that reads needs")
	}
}

// Start puts each password and private value the run was given at the
// path its name leads to, whatever its keys hold, in a list at its index,
// after the nulls the plan holds in place of the items it doesn't hold; and
// keeps the names the plan gives, a derived one over a password's among
// them. What is private is in the secret record's part of the values, and
// none of it in the run state's.
func TestStartRestoresHeldValues(t *testing.T) {
	for name, tc := range map[string]struct {
		defs      string
		submitted map[string]any
		vars      testVariables
		inputs    string
		secrets   []string
		derived   []string
		private   []string
	}{
		"keys with dots and tildes": {
			defs: `g: {type: group, items: {pass: {type: password, optional: true}}}
c: {type: string, default: '${{ vars["my.token"] }}'}`,
			submitted: map[string]any{"db.password": "S1", "g": map[string]any{"x~y.token": "S2", "pass": "S3"}},
			vars:      testVariables{"vars.my.token": {Value: "tok", Secret: true}},
			inputs:    `{"c":"tok","db.password":"S1","g":{"pass":"S3","x~y.token":"S2"}}`,
			secrets:   []string{"inputs.c", "inputs.db~1password", "inputs.g.pass", "inputs.g.x~0y~1token", "vars.my~1token"},
			derived:   []string{"inputs.c"},
		},
		"nested lists": {
			defs: `l:
  type: list
  template:
    pass: {type: password, optional: true}
    inner: {type: list, template: {pw: {type: password}, n: {type: string}}}`,
			submitted: map[string]any{"l": []any{
				map[string]any{"pass": "S3", "inner": []any{map[string]any{"n": "a"}, map[string]any{"pw": "S4", "n": "b"}}},
				map[string]any{"inner": []any{}},
			}},
			inputs:  `{"l":[{"inner":[{"n":"a"},{"n":"b","pw":"S4"}],"pass":"S3"},{"inner":[]}]}`,
			secrets: []string{"inputs.l.0.inner.1.pw", "inputs.l.0.pass"},
		},
		"a row that isn't the shape its inputs say": {
			defs:      `l: {type: list, template: {pass: {type: password}}}`,
			submitted: map[string]any{"l": []any{map[string]any{"pass": "S1"}, "S2", map[string]any{"pass": "S3"}}},
			inputs:    `{"l":[{"pass":"S1"},"S2",{"pass":"S3"}]}`,
			secrets:   []string{"inputs.l.0.pass", "inputs.l.1", "inputs.l.2.pass"},
		},
		"derived over a password": {
			defs: `creds:
  type: list
  template: {user: {type: string}, pass: {type: password}}
  default: ${{ fromJSON(vars.CREDS) }}`,
			submitted: map[string]any{"creds": []any{map[string]any{"user": "a", "pass": "HUNTER2"}}},
			vars:      testVariables{"vars.CREDS": {Value: `[{"user":"b","pass":"x"}]`, Secret: true}},
			inputs:    `{"creds":[{"pass":"HUNTER2","user":"a"}]}`,
			secrets:   []string{"inputs.creds", "inputs.creds.0.pass"},
			derived:   []string{"inputs.creds"},
		},
		"typed over a variable": {
			defs:      `owner: {type: string, default: '${{ org.OWNER }}'}`,
			submitted: map[string]any{"owner": "typed"},
			vars:      testVariables{"org.OWNER": {Value: "research"}},
			inputs:    `{"owner":"typed"}`,
			private:   []string{"inputs.owner"},
		},
		"a list held for Start without its passwords": {
			defs: `hosts:
  type: list
  template:
    name: {type: string}
    key: {type: password}
    site: {type: string, default: '${{ vars.SITE }}'}`,
			submitted: map[string]any{"hosts": []any{map[string]any{"name": "a", "key": "K1"}, map[string]any{"name": "b", "key": "K2"}}},
			vars:      testVariables{"vars.SITE": {Value: "east", Secret: true}},
			inputs:    `{"hosts":[{"key":"K1","name":"a","site":"east"},{"key":"K2","name":"b","site":"east"}]}`,
			secrets:   []string{"inputs.hosts.0.key", "inputs.hosts.0.site", "inputs.hosts.1.key", "inputs.hosts.1.site", "vars.SITE"},
			derived:   []string{"inputs.hosts.0.site", "inputs.hosts.1.site"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, res, diags := started(t, workflow(tc.defs), tc.submitted, tc.vars)
			if res == nil {
				t.Fatal(diags)
			}
			if g := canonical(t, res.Run.Inputs.Get()); g != tc.inputs {
				t.Errorf("inputs %s, want %s", g, tc.inputs)
			}
			for what, names := range map[string][2][]string{
				"secrets": {res.Run.Secrets, tc.secrets}, "derived": {res.Run.Derived, tc.derived}, "private": {res.Run.Private, tc.private},
			} {
				if !slices.Equal(names[0], names[1]) {
					t.Errorf("%s %q, want %q", what, names[0], names[1])
				}
			}
			record, state := res.Run.Split()
			for _, name := range slices.Concat(res.Run.Secrets, res.Run.Private) {
				path := plan.SplitPath(name)
				if _, ok := jsonptr.Lookup(stateTree(t, state), path); ok && path[0] != "vars" && path[0] != "org" {
					if v, _ := jsonptr.Lookup(stateTree(t, state), path); v != nil {
						t.Errorf("the run state holds %s: %v", name, v)
					}
				}
			}
			if g, w := canonical(t, plan.JoinRunValues(record, state)), canonical(t, res.Run); g != w {
				t.Errorf("joined %s, want %s", g, w)
			}
		})
	}
}

// stateTree returns the run state's values as one mapping, by context.
func stateTree(t *testing.T, r plan.RunValues) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(canonical(t, r)), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// The rows of a list whose value Start evaluates, all of it, are known
// only then: each row's fields take their defaults, as a row's do when the
// run is created, those that read the row, a variable and what Start
// evaluated among them.
func TestStartFillsRowsKnownOnlyAtStart(t *testing.T) {
	src := workflow(`keys:
  type: list
  template:
    key: {type: password}
    site: {type: string, default: '${{ org.SITE }}-${{ inputs.keys[index].key }}-${{ env.ZONE }}'}
    n: {type: number, default: 3}
  default: ${{ fromJSON(vars.KEYS) }}`, "env:\n  ZONE: z\n")
	vars := testVariables{"vars.KEYS": {Value: `[{"key":"k1"},{"key":"k2","n":5}]`, Secret: true}, "org.SITE": {Value: "east"}}
	_, res, diags := started(t, src, nil, vars)
	if res == nil {
		t.Fatal(diags)
	}
	want := `{"keys":[{"key":"k1","n":3,"site":"east-k1-z"},{"key":"k2","n":5,"site":"east-k2-z"}]}`
	if g := canonical(t, res.Run.Inputs.Get()); g != want {
		t.Errorf("inputs %s, want %s", g, want)
	}
	if !slices.Contains(res.Run.Secrets, "inputs.keys") {
		t.Errorf("secrets %v", res.Run.Secrets)
	}
}

// A decision of the run's shape that read a variable or a password when
// the run was created is decided again as it starts, and one that decides
// otherwise fails the run, rather than run it with values from two
// readings: an input's hidden, ignore and optional, and a job's needs.
func TestStartRunShapeChanged(t *testing.T) {
	src := workflow(`a: {type: string, default: x, hidden: '${{ org.HIDE == "yes" }}'}
b: {type: string, optional: true, ignore: '${{ org.SKIP }}'}
c: {type: string, optional: '${{ org.OPT == "yes" }}'}
grouped:
  type: group
  ignore: ${{ .hidden }}
  hidden: ${{ org.HIDE_GROUP == "yes" }}
  items:
    d: {type: string, default: y}`, `jobs:
  build:
    steps:
      - run: echo
  test:
    steps:
      - run: echo
  deploy:
    needs: ${{ fromJSON(org.NEEDS) }}
    steps:
      - run: echo
`)
	created := testVariables{
		"org.HIDE": {Value: "no"}, "org.SKIP": {Value: "true"}, "org.OPT": {Value: "yes"},
		"org.HIDE_GROUP": {Value: "no"}, "org.NEEDS": {Value: `["build", "test:any"]`},
	}
	c := createRun(t, src, nil, created)
	if res, diags := startRun(t, c, created, compile.StartInput{}); res == nil {
		t.Fatalf("started with the variables it was created with: %v", diags)
	}
	// The same jobs, in another order, and written twice, are the same
	// shape.
	same := maps.Clone(created)
	same["org.NEEDS"] = compile.Variable{Value: `["test:any", "build", "build"]`}
	if res, diags := startRun(t, c, same, compile.StartInput{}); res == nil {
		t.Errorf("the same needs in another order: %v", diags)
	}
	for name, tc := range map[string]struct {
		variable, value string
		pointer, key    string
		shown           string
	}{
		"hidden":         {"org.HIDE", "yes", "/on/execute/inputs/a/hidden", "hidden", "a"},
		"ignore":         {"org.SKIP", "false", "/on/execute/inputs/b/ignore", "ignore", "b"},
		"optional":       {"org.OPT", "no", "/on/execute/inputs/c/optional", "optional", "c"},
		"a relative one": {"org.HIDE_GROUP", "yes", "/on/execute/inputs/grouped/ignore", "ignore", "grouped"},
		"needs":          {"org.NEEDS", `["build"]`, "/jobs/deploy/needs", "needs", "deploy"},
		"needs any":      {"org.NEEDS", `["build", "test"]`, "/jobs/deploy/needs", "needs", "deploy"},
		"needs garbage":  {"org.NEEDS", `[1]`, "/jobs/deploy/needs", "needs", "deploy"},
	} {
		t.Run(name, func(t *testing.T) {
			changed := maps.Clone(created)
			changed[tc.variable] = compile.Variable{Value: tc.value}
			res, diags := startRun(t, c, changed, compile.StartInput{})
			if res != nil {
				t.Fatal("the run started")
			}
			found := false
			for _, d := range diags.Errors() {
				if d.Code == problems.RunShapeChanged.Code() && d.Path() == tc.pointer {
					found = d.Params["key"] == tc.key && d.Params["name"] == tc.shown
				}
			}
			if !found {
				t.Errorf("diagnostics %v, want run_shape_changed of %s %s at %s", diags, tc.key, tc.shown, tc.pointer)
			}
		})
	}
}

// Start reads the variables the plan lists once, secret ones included, by
// their names as they are, and each start reads them again, as a rerun
// does: what reads one has its value as it is then. A variable that is a
// secret as the run is created, or as it starts, is one.
func TestStartReadsVariablesAgain(t *testing.T) {
	src := workflow(`a: {type: string, default: '${{ vars.A }}'}
b: {type: string, default: '${{ vars["my.b"] }}'}
c: {type: string, default: '${{ vars.C }}'}`, `jobs:
  main:
    steps:
      - run: echo ${{ org.REGION }} ${{ toJSON(vars.*) }}
`)
	created := testVariables{
		"vars.A": {Value: "1"}, "vars.my.b": {Value: "2", Secret: true}, "vars.C": {Value: "3"}, "org.REGION": {Value: "east"},
	}
	c := createRun(t, src, nil, created)
	later := &countingVariables{testVariables: testVariables{
		"vars.A": {Value: "10", Secret: true}, "vars.my.b": {Value: "20"}, "vars.C": {Value: "30"}, "org.REGION": {Value: "west"},
	}}
	res, diags := startRun(t, c, later, compile.StartInput{})
	if res == nil {
		t.Fatal(diags)
	}
	if want := [][]string{{"org.REGION", "vars"}}; !slices.EqualFunc(later.asked, want, slices.Equal) {
		t.Errorf("asked %q, want %q", later.asked, want)
	}
	if g, w := canonical(t, res.Run.Inputs.Get()), `{"a":"10","b":"20","c":"30"}`; g != w {
		t.Errorf("inputs %s, want %s", g, w)
	}
	if want := []string{"inputs.a", "inputs.b", "vars.A", "vars.my~1b"}; !slices.Equal(res.Run.Secrets, want) {
		t.Errorf("secrets %v, want %v", res.Run.Secrets, want)
	}
	if want := []string{"inputs.c"}; !slices.Equal(res.Run.Private, want) {
		t.Errorf("private %v, want %v", res.Run.Private, want)
	}
	if want := map[string]string{"A": "10", "my.b": "20", "C": "30"}; !maps.Equal(res.Run.Vars, want) {
		t.Errorf("vars %v, want %v", res.Run.Vars, want)
	}
}

// A run that reads a variable can't start without a Variables interface; a
// resolver's problem of a workflow's kind is the run's, and any other
// error is one the coordinator starts the run again for.
func TestStartVariablesResolver(t *testing.T) {
	src := workflow(`a: {type: string, default: '${{ vars.A }}'}`)
	vars := testVariables{"vars.A": {Value: "1"}}
	c := createRun(t, src, nil, vars)
	res, diags := startRun(t, c, nil, compile.StartInput{})
	if res != nil || len(diags.Errors()) != 1 || diags.Errors()[0].Code != problems.ResolverMissing.Code() {
		t.Errorf("without Variables: %v", diags)
	}
	down := errors.New("the store is down")
	in := compile.StartInput{Variables: failingVariables{down}, Record: c.Record}
	if _, _, err := compile.Start(t.Context(), c.Plan, in); !errors.Is(err, down) {
		t.Errorf("error %v, want %v", err, down)
	}
	in.Variables = failingVariables{problems.InputRejected.Problem(problems.Params{"input": "a", "reason": "no"})}
	res, diags, err := compile.Start(t.Context(), c.Plan, in)
	if err != nil || res != nil || len(diags.Errors()) != 1 || diags.Errors()[0].Code != problems.InputRejected.Code() {
		t.Errorf("a problem: %v, %v", diags, err)
	}
}

type failingVariables struct{ err error }

func (v failingVariables) Read(context.Context, []string, bool) (map[string]compile.Variable, error) {
	return nil, v.err
}

// Start evaluates the workflow's env: and the inputs held for it in the
// order they read each other; values that read each other are an error,
// listed by name.
func TestStartValuesReadEachOther(t *testing.T) {
	src := workflow(`a: {type: string, default: '${{ env.B }}'}
c: {type: string, default: '${{ vars.C }}${{ env.D }}'}`, "env:\n  B: ${{ inputs.a }}\n  D: d\n")
	_, res, diags := started(t, src, nil, testVariables{"vars.C": {Value: "c", Secret: true}})
	if res != nil {
		t.Fatal("the run started")
	}
	errs := diags.Errors()
	if len(errs) != 1 || errs[0].Code != problems.ValuesReadEachOther.Code() || errs[0].Params["names"] != `"inputs.a", "env.B"` {
		t.Errorf("diagnostics %v", diags)
	}
}

// Start checks what it evaluated as the compiler checks an input's value,
// without showing a value made from a variable or a password; and reports
// reading what isn't known as the run starts.
func TestStartChecksWhatItEvaluated(t *testing.T) {
	for name, tc := range map[string]struct {
		defs    string
		vars    testVariables
		in      compile.StartInput
		code    string
		pointer string
		shown   string
	}{
		"a number": {
			defs: `n: {type: number, default: '${{ vars.N }}'}`, vars: testVariables{"vars.N": {Value: "many", Secret: true}},
			code: "input_not_number", pointer: "/on/execute/inputs/n", shown: problems.Masked,
		},
		"a number from env": {
			defs: `n: {type: number, default: '${{ env.N }}'}`,
			code: "input_not_number", pointer: "/on/execute/inputs/n", shown: `"x"`,
		},
		"above its max": {
			defs: `n: {type: number, max: 3, default: '${{ vars.N }}'}`, vars: testVariables{"vars.N": {Value: "4", Secret: true}},
			code: "input_above_max", pointer: "/on/execute/inputs/n", shown: problems.Masked,
		},
		"an option": {
			defs: `o: {type: dropdown, options: [a, b], default: '${{ vars.O }}'}`, vars: testVariables{"vars.O": {Value: "c", Secret: true}},
			code: "input_not_option", pointer: "/on/execute/inputs/o", shown: problems.Masked,
		},
		"a platform's": {
			defs: `cl: {type: compute-clusters, default: '${{ vars.CL }}'}`, vars: testVariables{"vars.CL": {Value: "c", Secret: true}},
			in: compile.StartInput{Inputs: rejecting{}}, code: "input_rejected", pointer: "/on/execute/inputs/cl",
		},
		"needs": {
			defs: `a: {type: string, default: '${{ vars.A }}${{ needs.x.outputs.y }}'}`, vars: testVariables{"vars.A": {Value: "a", Secret: true}},
			code: "context_not_known", pointer: "/on/execute/inputs/a/default",
		},
		"fromJSON": {
			defs: `a: {type: string, default: '${{ fromJSON(vars.A) }}'}`, vars: testVariables{"vars.A": {Value: "{", Secret: true}},
			code: "from_json_invalid", pointer: "/on/execute/inputs/a/default",
		},
	} {
		t.Run(name, func(t *testing.T) {
			src := workflow(tc.defs, "env:\n  N: x\n")
			c := createRun(t, src, nil, tc.vars)
			res, diags := startRun(t, c, tc.vars, tc.in)
			if res != nil {
				t.Fatal("the run started")
			}
			errs := diags.Errors()
			if len(errs) != 1 || string(errs[0].Code) != tc.code || errs[0].Path() != tc.pointer {
				t.Fatalf("diagnostics %v, want %s at %s", diags, tc.code, tc.pointer)
			}
			if tc.shown != "" && errs[0].Params["value"] != tc.shown {
				t.Errorf("shows %v, want %v", errs[0].Params["value"], tc.shown)
			}
			for _, v := range tc.vars {
				if strings.Contains(errs[0].Message(), v.Value) && len(v.Value) > 1 {
					t.Errorf("%s shows a variable's value", errs[0])
				}
			}
		})
	}
}

// rejecting is a platform that rejects every value.
type rejecting struct{}

func (rejecting) ResolveInput(string, map[string]any, any) (any, error) {
	return nil, errors.New("no such cluster")
}

// Start makes the run's sessions only once everything else has passed,
// and a run with sessions can't start without a session provider.
func TestStartSessions(t *testing.T) {
	src := workflow(`n: {type: number, default: '${{ vars.N }}'}`, "sessions:\n  desktop:\n    useTLS: false\n    redirect: ${{ inputs.n == 2 }}\n")
	vars := testVariables{"vars.N": {Value: "2"}}
	s := &testSessions{}
	c := createWith(t, src, nil, compile.Options{Inputs: passThrough{}, Contexts: vars.contexts(), Sessions: s})
	if !maps.Equal(c.Plan.Run.SessionNames, map[string]string{"desktop": "desktop-checked"}) {
		t.Errorf("the session provider's names %v", c.Plan.Run.SessionNames)
	}
	res, diags := startRun(t, c, vars, compile.StartInput{})
	if res != nil || len(diags.Errors()) != 1 || diags.Errors()[0].Code != problems.ResolverMissing.Code() || diags.Errors()[0].Path() != "/sessions" {
		t.Errorf("without a session provider: %v", diags)
	}
	if res, diags := startRun(t, c, vars, compile.StartInput{RunID: "r1", Sessions: s}); res == nil {
		t.Fatal(diags)
	}
	if len(s.made) != 1 || s.made[0] != `r1 {"desktop":{"redirect":"${{ inputs.n == 2 }}","useTLS":false}}` {
		t.Errorf("made %q", s.made)
	}
	bad := testVariables{"vars.N": {Value: "two"}}
	if res, _ := startRun(t, c, bad, compile.StartInput{RunID: "r2", Sessions: s}); res != nil || len(s.made) != 1 {
		t.Errorf("a run that fails to start made sessions: %q", s.made)
	}
	s.err = problems.InputRejected.Problem(problems.Params{"input": "desktop", "reason": "busy"})
	if res, diags := startRun(t, c, vars, compile.StartInput{RunID: "r3", Sessions: s}); res != nil || len(diags.Errors()) != 1 || diags.Errors()[0].Code != problems.InputRejected.Code() {
		t.Errorf("a provider's problem: %v", diags)
	}
}

// testSessions is a session provider: it names each session as its key
// and -checked, and records each run it makes the sessions of, unless it
// gives checkErr or err.
type testSessions struct {
	made     []string
	err      error
	checkErr error
}

func (s *testSessions) Check(_ context.Context, sessions map[string]any) (map[string]string, error) {
	if s.checkErr != nil {
		return nil, s.checkErr
	}
	names := map[string]string{}
	for name := range sessions {
		names[name] = name + "-checked"
	}
	return names, nil
}

func (s *testSessions) Make(_ context.Context, runID string, sessions map[string]any, _ map[string]string) error {
	if s.err != nil {
		return s.err
	}
	data, err := json.Marshal(sessions, json.Deterministic(true))
	if err != nil {
		return err
	}
	s.made = append(s.made, runID+" "+string(data))
	return nil
}

// Start expands a matrix whose values are known, and fails the run when it
// can't be expanded; it evaluates the workflow's timeout:, without showing
// a value made from a variable.
func TestStartMatricesAndTimeout(t *testing.T) {
	vars := testVariables{"vars.SIZES": {Value: "[]", Secret: true}, "org.T": {Value: "soon"}}
	src := workflow(`sizes: {type: string, default: '${{ vars.SIZES }}'}`, `timeout: ${{ org.T }}
jobs:
  main:
    strategy:
      matrix:
        s: ${{ fromJSON(inputs.sizes) }}
    steps:
      - run: echo
`)
	_, res, diags := started(t, src, nil, vars)
	if res != nil {
		t.Fatal("the run started")
	}
	codes := map[string]string{}
	for _, d := range diags.Errors() {
		codes[d.Path()] = string(d.Code)
		if d.Code == problems.DurationInvalid.Code() && d.Params["value"] != problems.Masked {
			t.Errorf("%s shows the timeout", d)
		}
	}
	if want := map[string]string{"/jobs/main/strategy/matrix": "matrix_empty", "/timeout": "duration_invalid"}; !maps.Equal(codes, want) {
		t.Errorf("errors %v, want %v", codes, want)
	}
	vars["vars.SIZES"] = compile.Variable{Value: "[1,2]", Secret: true}
	vars["org.T"] = compile.Variable{Value: "1h"}
	_, res, diags = started(t, src, nil, vars)
	if res == nil {
		t.Fatal(diags)
	}
	x := res.Expansions["main"]
	if res.TimeoutSeconds != 3600 || !res.TimeoutPrivate || x == nil || len(x.Instances) != 2 || !x.Instances[1].Secret {
		t.Errorf("timeout %d, private %v, expansion %s", res.TimeoutSeconds, res.TimeoutPrivate, canonical(t, x))
	}
}

// Given the same plan, input and resolver answers, Start gives the same
// result, byte for byte.
func TestStartIsDeterministic(t *testing.T) {
	src := workflow(`a: {type: string, default: '${{ vars.A }}'}
l:
  type: list
  template: {k: {type: password}, s: {type: string, default: '${{ org.S }}'}}
  default: ${{ fromJSON(vars.L) }}
m: {type: string, default: '${{ toJSON(inputs.l) }}'}`, "env:\n  X: ${{ inputs.m }}\n  Y: ${{ inputs.a }}\n")
	vars := testVariables{"vars.A": {Value: "a", Secret: true}, "vars.L": {Value: `[{"k":"1"},{"k":"2"},{"k":"3"}]`}, "org.S": {Value: "s"}}
	c := createRun(t, src, nil, vars)
	var first string
	for range 20 {
		res, diags := startRun(t, c, vars, compile.StartInput{})
		if res == nil {
			t.Fatal(diags)
		}
		if got := canonical(t, res); first == "" {
			first = got
		} else if got != first {
			t.Fatalf("%s, then %s", first, got)
		}
	}
}

// What doesn't fit the plan is a caller's mistake, which Start returns as
// an error, as it does the context's end.
func TestStartCallerMistakes(t *testing.T) {
	src := workflow(`pw: {type: password}`)
	c := createRun(t, src, map[string]any{"pw": "x"}, nil)
	for name, in := range map[string]compile.StartInput{
		"a password the plan doesn't name": {Record: compile.Record{Passwords: map[string]any{"inputs.other": "x"}}},
		"a private value it doesn't name":  {Record: compile.Record{Private: map[string]any{"inputs.pw": "x"}}},
		"a name of no input":               {Record: compile.Record{Passwords: map[string]any{"pw": "x"}}},
		// The plan names the password, which Start doesn't evaluate.
		"no password the plan names": {},
	} {
		if _, _, err := compile.Start(t.Context(), c.Plan, in); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, _, err := compile.Start(t.Context(), nil, compile.StartInput{}); err == nil {
		t.Error("no plan: no error")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := compile.Start(ctx, c.Plan, compile.StartInput{}); !errors.Is(err, context.Canceled) {
		t.Errorf("a context that is done: %v", err)
	}
}

// No run state that Start gives, and no diagnostic, holds a password's
// value: each conformance workflow is created and started with its case's
// inputs, the password given for each password input and field, as text,
// and what a launch form submits for them (TestPasswordsNeverInPlans), and
// the case's variables. The secret record holds each password.
func TestPasswordsNeverInRunState(t *testing.T) {
	startedRuns, kept := 0, 0
	for _, area := range []string{"format", "inputs", "compile", "start"} {
		cases, err := conformance.Cases(area)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			data, err := c.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			doc, diags := format.Load("workflow.yaml", data)
			if diags.HasErrors() {
				continue
			}
			defs, _ := jsonptr.Lookup(doc.Value, []string{"on", "execute", "inputs"})
			opts := compile.Options{Contexts: contexts(c), Inputs: passThrough{}, Sessions: &testSessions{}, Sources: anySources{}, Groups: anyGroups{}}
			submitted := jsonvalue.CloneMap(c.Inputs)
			if !withPassword(defs, submitted, sentinel) {
				continue
			}
			vars := caseVariables(c, func(_, v string) string { return v })
			for _, submitted := range append([]map[string]any{submitted}, formShapes(compile.FormValues(doc, submitted, opts))...) {
				created, _, err := compile.Create(t.Context(), "workflow.yaml", data, submitted, opts)
				if err != nil {
					t.Fatal(err)
				}
				if created == nil {
					continue
				}
				res, diags, err := compile.Start(t.Context(), created.Plan, compile.StartInput{
					Variables: vars, Record: created.Record, Inputs: passThrough{}, Sessions: &testSessions{},
				})
				if err != nil {
					t.Fatalf("%s: %v", c.Name, err)
				}
				for _, d := range diags {
					if holdsSentinel(t, d) {
						t.Errorf("%s: %s shows the password", c.Name, d)
					}
				}
				if res == nil {
					continue
				}
				startedRuns++
				record, state := res.Run.Split()
				if strings.Contains(canonical(t, state), sentinel) {
					t.Errorf("%s: the run state holds the password: %s", c.Name, canonical(t, state))
				}
				if strings.Contains(canonical(t, record), sentinel) {
					kept++
				}
			}
		}
	}
	t.Logf("started %d runs, %d of whose secret records hold the password", startedRuns, kept)
	if startedRuns == 0 || kept == 0 {
		t.Fatalf("started %d runs, %d of whose secret records hold the password", startedRuns, kept)
	}
}

// No run state that Start gives, and no diagnostic, holds a variable's
// value, plain or secret: each conformance workflow whose case gives
// variables is created and started with each one's value its own text,
// with its case's inputs and with what a form submits for them. The
// secret record holds the variables, and what is made from them.
func TestVariablesNeverInRunState(t *testing.T) {
	startedRuns, made := 0, 0
	for _, area := range []string{"inputs", "compile", "start"} {
		cases, err := conformance.Cases(area)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			vars := caseVariables(c, func(name, _ string) string { return variableSentinel + name })
			if len(vars) == 0 {
				continue
			}
			data, err := c.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			doc, diags := format.Load("workflow.yaml", data)
			if diags.HasErrors() {
				continue
			}
			opts := compile.Options{Contexts: vars.contexts(), Inputs: passThrough{}, Sessions: &testSessions{}, Sources: anySources{}, Groups: anyGroups{}}
			for _, submitted := range append([]map[string]any{c.Inputs}, formShapes(compile.FormValues(doc, c.Inputs, opts))...) {
				created, _, err := compile.Create(t.Context(), "workflow.yaml", data, submitted, opts)
				if err != nil {
					t.Fatal(err)
				}
				if created == nil {
					continue
				}
				res, diags, err := compile.Start(t.Context(), created.Plan, compile.StartInput{
					Variables: vars, Record: created.Record, Inputs: passThrough{}, Sessions: &testSessions{},
				})
				if err != nil {
					t.Fatalf("%s: %v", c.Name, err)
				}
				for _, d := range diags {
					if strings.Contains(fmt.Sprint(d.Params), variableSentinel) || strings.Contains(d.Message(), variableSentinel) {
						t.Errorf("%s: %s shows a variable's value", c.Name, d)
					}
				}
				if res == nil {
					continue
				}
				startedRuns++
				record, state := res.Run.Split()
				if strings.Contains(canonical(t, state), variableSentinel) {
					t.Errorf("%s: the run state holds a variable's value: %s", c.Name, canonical(t, state))
				}
				for name, x := range res.Expansions {
					for _, i := range x.Instances {
						if bytes.Contains([]byte(canonical(t, i.Matrix.Get())), []byte(variableSentinel)) && !i.Private {
							t.Errorf("%s: %s's instance %d holds a variable's value, and isn't private", c.Name, name, i.Index)
						}
					}
				}
				if res.TimeoutSeconds != 0 && !res.TimeoutPrivate && strings.Contains(fmt.Sprint(created.Plan.Run.Timeout.Exprs()), "vars") {
					t.Errorf("%s: the timeout read a variable, and isn't private", c.Name)
				}
				if strings.Count(canonical(t, record.Inputs.Get()), variableSentinel) > 0 {
					made++
				}
			}
		}
	}
	t.Logf("started %d runs, %d with values made from a variable", startedRuns, made)
	if startedRuns == 0 || made == 0 {
		t.Fatalf("started %d runs, %d with values made from a variable", startedRuns, made)
	}
}

// The parts of the input definitions the plan withholds, which can hold a
// password, the secret record gives back to Start: the default rows of a
// list whose rows have a password input, and the password field's default
// that fills in the rows Start evaluates. A record without them doesn't fit
// the plan.
func TestStartRestoresWithheldDefinitions(t *testing.T) {
	const sentinel = "chang3me-SENTINEL"
	src := workflow(`hosts:
  type: list
  default: ${{ fromJSON(vars.HOSTS) }}
  template: {name: {type: string}, key: {type: password, default: chang3me-SENTINEL}}`)
	vars := testVariables{"vars.HOSTS": {Value: `[{"name":"a"},{"name":"b"}]`, Secret: true}}
	c, res, diags := started(t, src, nil, vars)
	if res == nil {
		t.Fatal(diags)
	}
	if got := canonical(t, res.Run.Inputs.Get()); got != `{"hosts":[{"key":"chang3me-SENTINEL","name":"a"},{"key":"chang3me-SENTINEL","name":"b"}]}` {
		t.Errorf("inputs %s", got)
	}
	if !slices.Contains(res.Run.Masked(), sentinel) {
		t.Errorf("masked %q", res.Run.Masked())
	}
	data, err := plan.Encode(c.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), sentinel) {
		t.Errorf("the plan holds the password's default: %s", data)
	}
	without := c.Record
	without.Withheld = nil
	if _, _, err := compile.Start(t.Context(), c.Plan, compile.StartInput{Variables: vars, Record: without, Inputs: passThrough{}}); err == nil {
		t.Error("no error without the withheld definitions")
	}
}

// What Start makes from a secret is masked as what is made from one, not as
// a secret's own text: an env: value that says a secret is set, true, or
// yes, masks no true and no yes in the run's logs. A value Start made that
// is a secret's own text is masked whole however short, as the secret's.
func TestStartMadeFromSecretIsDerived(t *testing.T) {
	src := workflow(`flag: {type: string, default: '${{ vars.TOKEN != '''' }}'}`, `env:
  HAS_TOKEN: ${{ vars.TOKEN != '' }}
  MODE: ${{ vars.TOKEN != '' && 'yes' || 'no' }}
  TOKEN: ${{ vars.TOKEN }}
jobs:
  main:
    steps:
      - run: echo
`)
	vars := testVariables{"vars.TOKEN": {Value: "s3c", Secret: true}}
	_, res, diags := started(t, src, nil, vars)
	if res == nil {
		t.Fatal(diags)
	}
	masked := res.Run.Masked()
	for _, s := range []string{"true", "yes"} {
		if slices.Contains(masked, s) {
			t.Errorf("masked %q: every %q in the run's logs is masked", masked, s)
		}
	}
	if !slices.Contains(masked, "s3c") {
		t.Errorf("masked %q, want the secret's own text", masked)
	}
	for _, name := range []string{"env.HAS_TOKEN", "env.MODE", "env.TOKEN", "inputs.flag"} {
		if !slices.Contains(res.Run.Secrets, name) || !slices.Contains(res.Run.Derived, name) {
			t.Errorf("%s: secrets %v, derived %v", name, res.Run.Secrets, res.Run.Derived)
		}
	}
}

// A variable that wasn't a secret as the run was created and is one as it
// starts: what the run was given in the place of what is made from a
// variable, which the plan names as private, is masked from then on as
// what is made from a secret.
func TestStartVariableThatBecameSecret(t *testing.T) {
	src := workflow(`owner: {type: string, default: '${{ org.OWNER }}'}`)
	submitted := map[string]any{"owner": "typed-owner"}
	c := createRun(t, src, submitted, testVariables{"org.OWNER": {Value: "research"}})
	for _, tc := range []struct {
		secret                    bool
		secrets, derived, private []string
	}{
		{false, nil, nil, []string{"inputs.owner"}},
		{true, []string{"inputs.owner", "org.OWNER"}, []string{"inputs.owner"}, nil},
	} {
		res, diags := startRun(t, c, testVariables{"org.OWNER": {Value: "research", Secret: tc.secret}}, compile.StartInput{})
		if res == nil {
			t.Fatal(diags)
		}
		if !slices.Equal(res.Run.Secrets, tc.secrets) || !slices.Equal(res.Run.Derived, tc.derived) || !slices.Equal(res.Run.Private, tc.private) {
			t.Errorf("secret %v: secrets %v, derived %v, private %v", tc.secret, res.Run.Secrets, res.Run.Derived, res.Run.Private)
		}
		if got := slices.Contains(res.Run.Masked(), "typed-owner"); got != tc.secret {
			t.Errorf("secret %v: masked %q", tc.secret, res.Run.Masked())
		}
	}
}

// A condition Start decides again reads what it read as the run was
// created: an input another condition ignores reads its value until the
// conditions reach it, in the inputs' order, and null after, as the
// compiler's did. One whose ignore read a variable or a password the plan
// holds whatever it decided, out of its values, in the secret record, and
// Start drops it when it decides so again; one the compiler ignored, as
// its ignore read neither, the record holds as ignored, for the conditions
// that read it. What else Start evaluates reads it as null, as the phases
// after it do, not as its default.
func TestStartConditionsReadIgnoredInputs(t *testing.T) {
	// The inputs' order is their names'.
	hidden := `%s: {type: string, optional: true, hidden: '${{ %s != '''' && inputs.%s == ''y'' }}'}`
	ignore := `%s: {type: string, default: x, ignore: '${{ %s }}'}`
	env := "env:\n  E: ${{ inputs.%s }}-${{ %s }}\n"
	for name, tc := range map[string]struct {
		defs   []string
		env    string
		vars   testVariables
		given  map[string]any
		read   bool
		record func(compile.Record) map[string]any
	}{
		"ignored by a variable, read before": {
			[]string{fmt.Sprintf(hidden, "a", "org.Z", "z"), fmt.Sprintf(ignore, "z", "org.IGN == ''yes''")},
			fmt.Sprintf(env, "z", "org.Z"),
			testVariables{"org.IGN": {Value: "yes"}, "org.Z": {Value: "z"}},
			map[string]any{"z": "y"},
			true,
			func(r compile.Record) map[string]any { return r.Private },
		},
		"ignored by a variable, read after": {
			[]string{fmt.Sprintf(ignore, "a", "org.IGN == ''yes''"), fmt.Sprintf(hidden, "z", "org.Z", "a")},
			fmt.Sprintf(env, "a", "org.Z"),
			testVariables{"org.IGN": {Value: "yes"}, "org.Z": {Value: "z"}},
			map[string]any{"a": "y"},
			false,
			func(r compile.Record) map[string]any { return r.Private },
		},
		"ignored by a password, read before": {
			[]string{"pw: {type: password}", fmt.Sprintf(hidden, "a", "inputs.pw", "z"), fmt.Sprintf(ignore, "z", "inputs.pw == ''p''")},
			fmt.Sprintf(env, "z", "inputs.pw"),
			nil,
			map[string]any{"pw": "p", "z": "y"},
			true,
			func(r compile.Record) map[string]any { return r.Passwords },
		},
		"ignored by a password, read after": {
			[]string{"pw: {type: password}", fmt.Sprintf(ignore, "a", "inputs.pw == ''p''"), fmt.Sprintf(hidden, "z", "inputs.pw", "a")},
			fmt.Sprintf(env, "a", "inputs.pw"),
			nil,
			map[string]any{"pw": "p", "a": "y"},
			false,
			func(r compile.Record) map[string]any { return r.Passwords },
		},
		"ignored by an input, read before": {
			[]string{"f: {type: string}", fmt.Sprintf(hidden, "a", "org.Z", "z"), fmt.Sprintf(ignore, "z", "inputs.f == ''off''")},
			fmt.Sprintf(env, "z", "org.Z"),
			testVariables{"org.Z": {Value: "z"}},
			map[string]any{"f": "off", "z": "y"},
			true,
			func(r compile.Record) map[string]any { return r.Ignored },
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := createRun(t, workflow(strings.Join(tc.defs, "\n"), tc.env), tc.given, tc.vars)
			held := tc.record(c.Record)
			if !slices.Contains(slices.Collect(maps.Values(held)), any("y")) {
				t.Fatalf("the record holds %+v", c.Record)
			}
			if _, ok := c.Plan.Run.Inputs.Get().(map[string]any)["z"]; ok {
				t.Errorf("the plan holds the input: %v", c.Plan.Run.Inputs.Get())
			}
			res, diags := startRun(t, c, tc.vars, compile.StartInput{})
			if res == nil || len(diags) != 0 {
				t.Fatalf("Start: %v", diags)
			}
			if e := res.Run.Env["E"]; !strings.HasPrefix(e, "-") {
				t.Errorf("env E %q reads the ignored input", e)
			}
			v := res.Run.Inputs.Get().(map[string]any)
			for _, k := range []string{"a", "z"} {
				if v[k] == "y" {
					t.Errorf("inputs %v hold the ignored input", v)
				}
			}
			if !tc.read {
				return
			}
			// Without the ignored input's value in the record, the run
			// doesn't start as it was created.
			without := c.Record
			without.Ignored, without.Private, without.Passwords = nil, nil, nil
			if !strings.Contains(name, "password") {
				without.Passwords = c.Record.Passwords
			}
			res, diags, err := compile.Start(t.Context(), c.Plan, compile.StartInput{Variables: tc.vars, Record: without, Inputs: passThrough{}})
			if res != nil && !slices.ContainsFunc(diags, func(d problems.Diagnostic) bool { return d.Code == problems.RunShapeChanged.Code() }) {
				t.Errorf("without the ignored input: %v, %v, %v", res != nil, diags, err)
			}
		})
	}
}

// A job's needs: and its matrix that read an input which an ignore that
// read a password or a variable ignored read it as ignored, at Create as at
// Start, so that the run starts either way: the jobs the run waits for and
// its matrix's instances are its shape, which the plan and the run show,
// as they show what a needs: that read a password itself decided. The
// input's value stays out of the plan, and the instances' values are
// private.
func TestShapeReadsIgnoredInputsAsDecided(t *testing.T) {
	jobs := map[string]string{
		"needs":  "jobs:\n  a:\n    steps:\n      - run: echo\n  main:\n    needs: '${{ inputs.c == ''kept'' && fromJSON(''[\"a\"]'') || fromJSON(''[]'') }}'\n    steps:\n      - run: echo\n",
		"matrix": "jobs:\n  main:\n    strategy:\n      matrix:\n        x: '${{ inputs.c == ''kept'' && fromJSON(''[1,2]'') || fromJSON(''[1]'') }}'\n    steps:\n      - run: echo ${{ matrix.x }}\n",
	}
	ignores := map[string]string{
		"a password": "pw: {type: password}\nc: {type: string, default: kept, ignore: '${{ inputs.pw == ''hunter2'' }}'}",
		"a variable": "c: {type: string, default: kept, ignore: '${{ org.FLAG == ''on'' }}'}",
	}
	for what, defs := range ignores {
		for shape, rest := range jobs {
			t.Run(what+" "+shape, func(t *testing.T) {
				var plans []string
				for _, ignored := range []bool{true, false} {
					submitted := map[string]any{"pw": "zzz"}
					vars := testVariables{"org.FLAG": {Value: "off"}}
					if ignored {
						submitted["pw"] = "hunter2"
						vars["org.FLAG"] = compile.Variable{Value: "on"}
					}
					if what == "a variable" {
						submitted = nil
					}
					c := createRun(t, workflow(defs, rest), submitted, vars)
					res, diags := startRun(t, c, vars, compile.StartInput{})
					if res == nil {
						t.Fatalf("ignored %v: %v", ignored, diags)
					}
					main := &c.Plan.Jobs[slices.IndexFunc(c.Plan.Jobs, func(j plan.Job) bool { return j.Name == "main" })]
					switch shape {
					case "needs":
						if want := !ignored; (len(main.Needs) == 1) != want || main.NeedsExpr.IsZero() {
							t.Errorf("ignored %v: main needs %v, expression held %v", ignored, main.Needs, !main.NeedsExpr.IsZero())
						}
						// The jobs it waits for are all that differs.
						main.Needs = nil
					case "matrix":
						x := res.Expansions["main"]
						want := 2
						if ignored {
							want = 1
						}
						if x == nil || len(x.Instances) != want || slices.ContainsFunc(x.Instances, func(in plan.Instance) bool { return !in.Private }) {
							t.Errorf("ignored %v: expansion %+v, want %d private instances", ignored, x, want)
						}
					}
					data, err := plan.Encode(c.Plan)
					if err != nil {
						t.Fatal(err)
					}
					plans = append(plans, string(data))
				}
				if plans[0] != plans[1] {
					t.Errorf("the plans differ but for the jobs main waits for:\n%s\n%s", plans[0], plans[1])
				}
			})
		}
	}
}
