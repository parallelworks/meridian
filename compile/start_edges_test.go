package compile_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
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

// Format 1 reads each input's value as it was submitted, as ACTIVATE does:
// text given for a number or a boolean stays text, in what Create
// evaluates, in what Start does and in the run's values, and the plan
// warns of each (input_read_as_submitted), without its value.
func TestInputsReadAsSubmitted(t *testing.T) {
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
	if want := `{"cpus":"2.50","flag":"false","label":"p-2.50-false"}`; canonical(t, res.Run.Inputs.Get()) != want {
		t.Errorf("inputs %s, want %s", canonical(t, res.Run.Inputs.Get()), want)
	}
	if got := res.Run.Env["CPUS"]; got != "2.50" {
		t.Errorf("env.CPUS = %q, want the text it was given", got)
	}
	var warned []string
	for _, w := range c.Plan.Warnings {
		if w.Code == problems.InputReadAsSubmitted.Code() {
			warned = append(warned, fmt.Sprintf("%v %v %v", w.Params["input"], w.Params["type"], w.Params["given"]))
			if strings.Contains(fmt.Sprint(w.Params), "2.50") {
				t.Errorf("the warning shows the value: %v", w.Params)
			}
		}
	}
	slices.Sort(warned)
	if want := []string{"cpus number text", "flag boolean text"}; !slices.Equal(warned, want) {
		t.Errorf("warned of %v, want %v", warned, want)
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
			in.Record.Passwords = map[string]any{"inputs.l.100000000000.pass": "p"}
		},
		"a decision of no condition": func(p *plan.Plan, in *compile.StartInput) {
			p.Run.Decisions = append(p.Run.Decisions, "/jobs/main")
			in.Record.Decisions = map[string]bool{"/jobs/main": true}
		},
		"a part outside the definitions": func(p *plan.Plan, _ *compile.StartInput) {
			p.Run.DeferredParts[0].Location = "/jobs/main/steps/0/run"
		},
	} {
		t.Run(name, func(t *testing.T) {
			again := createRun(t, src, map[string]any{"l": []any{map[string]any{"pass": "p"}}}, vars)
			in := compile.StartInput{Variables: vars, Record: compile.Record{Passwords: c.Record.Passwords}, Inputs: passThrough{}}
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
	if _, ok := c.Record.Private["inputs.creds"]; !ok {
		t.Fatalf("private %v", c.Record.Private)
	}
	if got := canonical(t, res.Run.Inputs.Get()); got != `{"creds":[{"pass":"HUNTER2","user":"a"}]}` {
		t.Errorf("inputs %s", got)
	}
	_, state := res.Run.Split()
	if strings.Contains(canonical(t, state), "HUNTER2") {
		t.Errorf("the run state holds the password: %s", canonical(t, state))
	}
}

// What Start evaluates reads a list's rows as Create's evaluations read
// them, a row's field that has no value as null, so an expression gives
// the same whether Create evaluates it, as it reads nothing held, or Start
// does, as it reads a secret too; a row's default held for Start reads
// them so too; and a condition that reads such a value decides as it did.
func TestStartReadsRowsAsCreateDid(t *testing.T) {
	submitted := map[string]any{"l": []any{map[string]any{"u": "a", "p": "x"}, map[string]any{"u": "b"}}}
	vars := testVariables{"vars.S": {Value: "", Secret: true}, "vars.V": {Value: ""}}
	src := workflow(`l: {type: list, template: {u: {type: string}, p: {type: string, optional: true}, h: {type: string, optional: true, default: '${{ join(inputs.l.*.p, ",") }}'}, hs: {type: string, optional: true, default: '${{ join(inputs.l.*.p, ",") }}${{ vars.S }}'}}}
c: {type: string, default: '${{ join(inputs.l.*.p, ",") }}'}
cs: {type: string, default: '${{ join(inputs.l.*.p, ",") }}${{ vars.S }}'}
j: {type: string, default: '${{ toJSON(inputs.l) }}'}
js: {type: string, default: '${{ toJSON(inputs.l) }}${{ vars.S }}'}`)
	_, res, diags := started(t, src, submitted, vars)
	if res == nil {
		t.Fatal(diags)
	}
	v := res.Run.Inputs.Get().(map[string]any)
	if v["c"] != "x," || v["cs"] != v["c"] || v["js"] != v["j"] {
		t.Errorf("Create gave %q and %q, Start %q and %q", v["c"], v["j"], v["cs"], v["js"])
	}
	for _, row := range v["l"].([]any) {
		if r := row.(map[string]any); r["h"] != "x," || r["hs"] != r["h"] {
			t.Errorf("row %v", r)
		}
	}
	src = workflow(`l: {type: list, template: {u: {type: string}, p: {type: string, optional: true}}}
c: {type: string, hidden: true, default: '${{ join(inputs.l.*.p, ",") }}${{ vars.V }}'}
x: {type: string, optional: true, ignore: '${{ inputs.c == "x," }}'}`)
	if _, res, diags := started(t, src, submitted, vars); res == nil {
		t.Errorf("the run didn't start: %v", diags)
	}
}

// A default that reads the value it gives reads it as Create read it, with
// none, rather than waiting for itself, whether what else it reads is a
// secret or not; and a form's text for it reads the text, as Create did.
func TestStartSelfReadingDefault(t *testing.T) {
	src := workflow(`a: {type: string, default: '${{ inputs.a }}-${{ vars.S }}'}`)
	for _, secret := range []bool{true, false} {
		_, res, diags := started(t, src, nil, testVariables{"vars.S": {Value: "s", Secret: secret}})
		if res == nil || res.Run.Inputs.Get().(map[string]any)["a"] != "-s" {
			t.Errorf("secret %v: %v, %v", secret, res, diags)
		}
	}
	src = workflow(`pw: {type: password, optional: true}
l: {type: list, template: {u: {type: string}, h: {type: string, optional: true, default: '${{ toJSON(inputs.l[index]) }}${{ inputs.pw }}'}}}`)
	text := "${{ toJSON(inputs.l[index]) }}${{ inputs.pw }}"
	_, res, diags := started(t, src, map[string]any{"pw": "p", "l": []any{map[string]any{"u": "a", "h": text}}}, nil)
	if res == nil {
		t.Fatal(diags)
	}
	h, _ := res.Run.Inputs.Get().(map[string]any)["l"].([]any)[0].(map[string]any)["h"].(string)
	if !strings.Contains(h, `"h": "${{ toJSON`) {
		t.Errorf("h is %q, which doesn't read the form's text", h)
	}
}

// A submitted value that a default which reads it gives back, as one that
// reads another input whose default reads the first does, isn't what the
// default made, whatever it gives: it is kept, and the run starts with it,
// rather than with two defaults that wait for each other.
func TestStartKeepsWhatADefaultReadsOfItself(t *testing.T) {
	src := workflow(`pw: {type: password, optional: true}
a: {type: number, optional: true, default: '${{ inputs.k }}'}
k: {type: string, hidden: true, default: '${{ inputs.pw == ''zz'' && ''y'' || inputs.a }}'}`)
	_, res, diags := started(t, src, map[string]any{"pw": "p", "a": "7"}, nil)
	if res == nil {
		t.Fatal(diags)
	}
	if v := res.Run.Inputs.Get().(map[string]any); fmt.Sprint(v["a"]) != "7" || v["k"] != "7" {
		t.Errorf("inputs %v", v)
	}
}

// A default that gives no value leaves what was there, as when the run was
// created: null a list's default row gave, and no mapping that the plan
// held only to hold it, a row's group or a wizard step.
func TestStartLeavesWhatADefaultDoesnt(t *testing.T) {
	for name, tc := range map[string]struct {
		defs      string
		submitted map[string]any
		want      string
	}{
		"null from a default row": {
			defs: `pw: {type: password, optional: true}
l: {type: list, optional: true, default: [{u: plain, h: '${{ inputs.pw }}'}], template: {u: {type: string, optional: true}, p: {type: password, optional: true}, h: {type: string, optional: true, default: '${{ inputs.l.[index].p }}'}}}`,
			want: `{"l":[{"h":null,"u":"plain"}]}`,
		},
		"a row's group": {
			defs:      `l: {type: list, template: {u: {type: string}, grp: {type: group, items: {gq: {type: password, optional: true}, gh: {type: string, optional: true, default: '${{ inputs.l[index].grp.gq }}'}}}}}`,
			submitted: map[string]any{"l": []any{map[string]any{"u": "a"}}},
			want:      `{"l":[{"u":"a"}]}`,
		},
		"a wizard step": {
			defs: `s: {type: step, title: S, options: {sp: {type: password, optional: true}, sx: {type: string, optional: true, default: '${{ inputs.s.sp }}'}}}`,
			want: `{}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, res, diags := started(t, workflow(tc.defs), tc.submitted, nil)
			if res == nil {
				t.Fatal(diags)
			}
			if got := canonical(t, res.Run.Inputs.Get()); got != tc.want {
				t.Errorf("inputs %s, want %s", got, tc.want)
			}
		})
	}
}

// Parts that read each other only through a definition, a relative
// reference to a default that reads the next part, are evaluated in the
// order they read each other, each once, so each reading a large input
// spends no more of the run's budget than Create did.
func TestStartRelativeChain(t *testing.T) {
	const n = 100
	var b strings.Builder
	b.WriteString("big: {type: string, optional: true}\n")
	for i := range n {
		fmt.Fprintf(&b, "x%04d: {type: string, default: '${{ length(toJSON(inputs.big)) }}${{ ..h%04d.default }}${{ vars.S }}'}\n", i, i)
		if i < n-1 {
			fmt.Fprintf(&b, "h%04d: {type: string, optional: true, hidden: true, default: '${{ inputs.x%04d }}'}\n", i, i+1)
		}
	}
	fmt.Fprintf(&b, "h%04d: {type: string, optional: true, hidden: true, default: '${{ vars.S }}'}", n-1)
	vars := testVariables{"vars.S": {Value: "s", Secret: true}}
	_, res, diags := started(t, workflow(b.String()), map[string]any{"big": strings.Repeat("x", 20000)}, vars)
	if res == nil {
		t.Fatal(diags)
	}
	if x := res.Run.Inputs.Get().(map[string]any)["x0000"].(string); !strings.HasPrefix(x, "20002") || !strings.HasSuffix(x, "s") {
		t.Errorf("x0000 is %.40q", x)
	}
}

// cancellingVariables cancels its context once it has read the variables,
// and countingInputs counts what the Inputs resolver is asked.
type cancellingVariables struct {
	testVariables
	cancel context.CancelFunc
}

func (v cancellingVariables) Read(ctx context.Context, names []string, secrets bool) (map[string]compile.Variable, error) {
	defer v.cancel()
	return v.testVariables.Read(ctx, names, secrets)
}

type countingInputs struct{ asked *int }

func (c countingInputs) ResolveInput(_ string, _ map[string]any, v any) (any, error) {
	*c.asked++
	return v, nil
}

// Start stops evaluating once its context is done, and returns the
// context's error: it asks nothing more of a resolver.
func TestStartStopsWhenCancelled(t *testing.T) {
	src := workflow(`c: {type: compute-clusters, default: '${{ vars.S }}'}`)
	vars := testVariables{"vars.S": {Value: "s", Secret: true}}
	c := createRun(t, src, nil, vars)
	ctx, cancel := context.WithCancel(t.Context())
	asked := 0
	in := compile.StartInput{Variables: cancellingVariables{vars, cancel}, Record: c.Record, Inputs: countingInputs{&asked}}
	if res, _, err := compile.Start(ctx, c.Plan, in); !errors.Is(err, context.Canceled) || res != nil || asked != 0 {
		t.Errorf("error %v, result %v, the Inputs resolver asked %d times", err, res, asked)
	}
}

// A value typed over a default that reads a password, which the secret
// record holds, is read as it was submitted, as a value the plan holds is:
// a dropdown whose option reads it takes it the same either way.
func TestStartReadsHeldValuesAsSubmitted(t *testing.T) {
	var ds []any
	for _, def := range []string{"'${{ inputs.pw }}'", "7"} {
		src := workflow(`pw: {type: password}
a: {type: number, default: ` + def + `}
d: {type: dropdown, options: ['${{ inputs.a }}', none]}`)
		submitted := map[string]any{"pw": "p", "a": "7", "d": "7"}
		_, res, diags := started(t, src, submitted, testVariables{})
		if res == nil {
			t.Fatal(diags)
		}
		ds = append(ds, res.Run.Inputs.Get().(map[string]any)["d"])
	}
	if ds[0] != ds[1] {
		t.Errorf("d is %#v when a's default reads a password, %#v when it doesn't", ds[0], ds[1])
	}
}

// A condition decided again at Start reads a value the secret record holds
// as it was submitted, as Create's did, so the run's shape doesn't change.
func TestStartShapeUnchangedOverHeldValues(t *testing.T) {
	src := workflow(`pw: {type: password}
a: {type: number, default: '${{ inputs.pw }}'}
x: {type: string, optional: true, ignore: '${{ toJSON(inputs.a) == ''"7"'' }}'}`)
	if _, res, diags := started(t, src, map[string]any{"pw": "p", "a": "7"}, testVariables{}); res == nil {
		t.Errorf("the run didn't start: %v", diags)
	}
}

// clusterInputs resolves a compute-clusters input's name to the cluster's
// details, as a platform's Inputs resolver does.
type clusterInputs struct{}

func (clusterInputs) ResolveInput(typ string, _ map[string]any, v any) (any, error) {
	if s, ok := v.(string); ok && typ == "compute-clusters" {
		return map[string]any{"name": s, "id": "id-" + s}, nil
	}
	return v, nil
}

// What is evaluated before the inputs are checked reads an input of a
// platform's type as it was submitted, at Create as at Start, where the
// Inputs resolver gave another value for it (plan.Run.Unresolved), and
// what reads it after, the run's values among them, as the resolver gave
// it: a condition decided again doesn't change the run's shape, and a
// default reads the same whether it reads a secret too.
func TestStartReadsResolvedInputsAsSubmitted(t *testing.T) {
	vars := testVariables{"vars.S": {Value: "SECRET", Secret: true}, "vars.V": {Value: "v"}}
	for name, tc := range map[string]struct {
		defs, rest string
		want       string
	}{
		"a default that reads a secret":  {"c: {type: compute-clusters}\nd: {type: string, default: '${{ toJSON(inputs.c) }}-${{ vars.S }}'}", "", `"c1"-SECRET`},
		"a default that reads no secret": {"c: {type: compute-clusters}\nd: {type: string, default: '${{ toJSON(inputs.c) }}'}", "", `"c1"`},
		"a hidden a variable decides":    {"c: {type: compute-clusters}\nd: {type: string, hidden: '${{ inputs.c == \"c1\" && vars.V != \"\" }}', default: dd}", "", "dd"},
		"an ignore a variable decides":   {"c: {type: compute-clusters}\nd: {type: string, optional: true, ignore: '${{ inputs.c == \"c1\" && vars.V != \"\" }}', default: dd}", "", ""},
		"the env":                        {"c: {type: compute-clusters}", "env:\n  E: ${{ toJSON(inputs.c) }}-${{ vars.S }}\n", ""},
	} {
		t.Run(name, func(t *testing.T) {
			src := workflow(tc.defs, tc.rest)
			c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), map[string]any{"c": "c1"}, compile.Options{Inputs: clusterInputs{}, Contexts: vars.contexts()})
			if err != nil || c == nil {
				t.Fatal(diags, err)
			}
			res, sd := startRun(t, c, vars, compile.StartInput{Inputs: clusterInputs{}})
			if res == nil {
				t.Fatal(sd)
			}
			inputs := res.Run.Inputs.Get().(map[string]any)
			if got := canonical(t, inputs["c"]); got != `{"id":"id-c1","name":"c1"}` {
				t.Errorf("c = %s, want the cluster's details", got)
			}
			if got, _ := inputs["d"].(string); got != tc.want {
				t.Errorf("d = %q, want %q", got, tc.want)
			}
			if e, ok := res.Run.Env["E"]; ok && e != `"c1"-SECRET` {
				t.Errorf("env.E = %q, want what reads the name", e)
			}
		})
	}
}

// A password submitted as a number or a boolean, which format 1 reads as it
// was submitted, is masked as its text, as an expression writes it: in
// what a step prints of it, and of toJSON(inputs), whatever its kind, a
// row's password among them, and the plan never holds it.
func TestPasswordOfEachKindMasked(t *testing.T) {
	for name, tc := range map[string]struct {
		defs      string
		submitted map[string]any
		want      string
	}{
		"a whole number":  {"pw: {type: password}", map[string]any{"pw": 98765432}, "98765432"},
		"a boolean":       {"pw: {type: password}", map[string]any{"pw": true}, "true"},
		"a fraction":      {"pw: {type: password}", map[string]any{"pw": 3.14159265}, "3.14159265"},
		"a whole float":   {"pw: {type: password}", map[string]any{"pw": 86420.0}, "86420"},
		"a row's number":  {"l: {type: list, template: {p: {type: password}}}", map[string]any{"l": []any{map[string]any{"p": 55667788}}}, "55667788"},
		"text":            {"pw: {type: password}", map[string]any{"pw": "98765432"}, "98765432"},
		"a group's value": {"g: {type: group, items: {pw: {type: password}}}", map[string]any{"g": map[string]any{"pw": 24681357}}, "24681357"},
	} {
		t.Run(name, func(t *testing.T) {
			c, res, diags := started(t, workflow(tc.defs), tc.submitted, testVariables{})
			if res == nil {
				t.Fatal(diags)
			}
			data, err := plan.Encode(c.Plan)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), tc.want) {
				t.Errorf("the plan holds the password: %s", data)
			}
			masker := plan.NewMasker(res.Run.Masked()...)
			for _, printed := range []string{expr.Text(res.Run.Inputs.Get()), "pw=" + tc.want} {
				if got := masker.Mask(printed); strings.Contains(got, tc.want) {
					t.Errorf("%q is masked as %q", printed, got)
				}
			}
		})
	}
}
