package compile_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
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
	for name, change := range map[string]func(p *plan.Plan, in *compile.StartInput){
		"a decision of no condition": func(p *plan.Plan, in *compile.StartInput) {
			p.Run.Decisions = append(p.Run.Decisions, "/jobs/main")
			in.Record.Decisions = map[string]bool{"/jobs/main": true}
		},
		"a part withheld outside the definitions": func(p *plan.Plan, in *compile.StartInput) {
			p.Run.Withheld = append(p.Run.Withheld, "/jobs/main/steps/0/run")
			in.Record.Withheld = map[string]plan.Data{"/jobs/main/steps/0/run": plan.DataOf("x")}
		},
	} {
		t.Run(name, func(t *testing.T) {
			again := createRun(t, src, map[string]any{"l": []any{map[string]any{"pass": "p"}}}, vars)
			in := compile.StartInput{Variables: vars, Record: again.Record, Inputs: passThrough{}}
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
	_, res, diags := started(t, src, map[string]any{"creds": []any{map[string]any{"user": "a", "pass": "HUNTER2"}}}, vars)
	if res == nil {
		t.Fatal(diags)
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
// Inputs resolver gave another value for it, and so does the workflow's
// env:; what reads it after, the run's values among them, reads it as the
// resolver gave it: a condition decided again doesn't change the run's
// shape, and a default reads the same whether it reads a secret too.
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

// detailInputs gives a platform input's mapping with an id, and an id
// made from each of its parts, as a platform's Inputs resolver gives a
// resource's details for what names it.
type detailInputs struct{}

func (detailInputs) ResolveInput(_ string, _ map[string]any, v any) (any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return v, nil
	}
	out := map[string]any{"id": "rid"}
	maps.Copy(out, m)
	for k, x := range m {
		out[k+"_id"] = fmt.Sprint("id-", x)
	}
	return out, nil
}

// A part of a platform input's value typed over a part of its default that
// reads a variable is private, or a secret if the variable is one; and as
// what the Inputs resolver gives can't be split by the parts that made it,
// as an id made from the part, the whole value is: the plan's Inputs have
// none of it, and Start reads it as the resolver gives it.
func TestResolvedHoldsNoPrivatePart(t *testing.T) {
	vars := testVariables{"vars.S": {Value: "SECRET", Secret: true}, "vars.V": {Value: "plain"}}
	for _, typ := range []string{
		"kubernetes-clusters", "storage", "bucket", "organization-groups",
		"region, csp: aws", "slurm-partitions, resource: '${{ inputs.c }}'", "kubernetes-namespaces, clusterName: c",
	} {
		for _, variable := range []string{"vars.V", "vars.S"} {
			t.Run(typ+" "+variable, func(t *testing.T) {
				src := workflow("k: {type: " + typ + ", optional: true, default: {name: k1, token: '${{ " + variable + " }}'}}")
				submitted := map[string]any{"k": map[string]any{"name": "k1", "token": "TYPED"}}
				c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), submitted, compile.Options{Inputs: detailInputs{}, Contexts: vars.contexts()})
				if err != nil || c == nil {
					t.Fatal(diags, err)
				}
				data, err := plan.Encode(c.Plan)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(data), "TYPED") || strings.Contains(string(data), "rid") {
					t.Errorf("the plan holds the value, or what the resolver made of it: %s", data)
				}
				names := c.Plan.Run.Private
				if variable == "vars.S" {
					names = c.Plan.Run.Secrets
				}
				if !slices.Contains(names, "inputs.k") {
					t.Errorf("the plan names %v as private and %v as secrets, want inputs.k among the %s", c.Plan.Run.Private, c.Plan.Run.Secrets, variable)
				}
				res, sd := startRun(t, c, vars, compile.StartInput{Inputs: detailInputs{}})
				if res == nil {
					t.Fatal(sd)
				}
				want := `{"k":{"id":"rid","name":"k1","name_id":"id-k1","token":"TYPED","token_id":"id-TYPED"}}`
				if got := canonical(t, res.Run.Inputs.Get()); got != want {
					t.Errorf("the run's inputs are %s, want %s", got, want)
				}
			})
		}
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
		// Read as a float64, as ACTIVATE reads it, every rendering of it
		// is the same number.
		"a whole number past 2^53": {"pw: {type: password}", map[string]any{"pw": int64(9007199254740993)}, "9007199254740992"},
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

// firstStep starts a run of src, readies its job main and returns its first
// step's command, and the command masked as a worker masks it.
func firstStep(t *testing.T, src string, submitted map[string]any) (raw, masked string) {
	t.Helper()
	c, res, diags := started(t, src, submitted, testVariables{})
	if res == nil {
		t.Fatal(diags)
	}
	j := &c.Plan.Jobs[slices.IndexFunc(c.Plan.Jobs, func(j plan.Job) bool { return j.Name == "main" })]
	ready, _, err := j.Ready(t.Context(), plan.ReadyInput{Run: res.Run})
	if err != nil || ready.Job == nil {
		t.Fatal(err)
	}
	state := plan.JobState{Run: res.Run, Masked: res.Run.Masked()}
	env, d, err := ready.Job.JobEnv(t.Context(), state)
	if err != nil || d.HasErrors() {
		t.Fatal(d, err)
	}
	state.JobEnv = env
	state.Masked = append(slices.Clone(state.Masked), env.Masked...)
	s, d, err := ready.Job.Step(t.Context(), 0, state)
	if err != nil || d.HasErrors() {
		t.Fatal(d, err)
	}
	return s.Run, plan.NewMasker(slices.Concat(state.Masked, s.Masked)...).Mask(s.Run)
}

// A submitted whole number past 2^53 is read as ACTIVATE reads a JSON
// number, as the float64 nearest it, everywhere: so a password's every
// rendering, the value a round trip through JSON gives among them, is the
// same text, which the run masks.
func TestWholeNumberPast2To53ReadAsFloat(t *testing.T) {
	src := workflow("pw: {type: password}\nn: {type: number}",
		"jobs:\n  main:\n    steps:\n      - run: echo ${{ inputs.pw }} ${{ fromJSON(toJSON(inputs.pw)) }} ${{ toJSON(inputs) }} n=${{ inputs.n }}\n")
	raw, masked := firstStep(t, src, map[string]any{"pw": int64(9007199254740995), "n": int64(9007199254740993)})
	if !strings.Contains(raw, "n=9007199254740992") || strings.Contains(raw, "9007199254740993") {
		t.Errorf("n reads otherwise than 9007199254740992: %s", raw)
	}
	for _, text := range []string{"9007199254740995", "9007199254740996"} {
		if strings.Contains(masked, text) {
			t.Errorf("the password shows as %s: %s", text, masked)
		}
	}
}

// A null submitted for an input is its value, which the run keeps where
// the plan doesn't hold it, as the secret record does for a password and
// for an input whose ignore a variable or a password decides: the run
// starts with the null, as it does with a value the plan holds, and a
// required input given null isn't missing at Start.
func TestStartKeepsHeldNulls(t *testing.T) {
	vars := testVariables{"vars.V": {Value: "v"}}
	for name, tc := range map[string]struct {
		defs      string
		submitted map[string]any
		want      string
	}{
		"a password": {"pw: {type: password}", map[string]any{"pw": nil}, `{"pw":null}`},
		"an ignore a variable decides": {
			"a: {type: string, ignore: '${{ vars.V == \"x\" }}'}",
			map[string]any{"a": nil},
			`{"a":null}`,
		},
		"an ignore a password decides": {
			"pw: {type: password}\na: {type: string, ignore: '${{ inputs.pw == \"x\" }}'}",
			map[string]any{"pw": "y", "a": nil},
			`{"a":null,"pw":"y"}`,
		},
		"a group's": {
			"g: {type: group, items: {pw: {type: password}, a: {type: string, ignore: '${{ vars.V == \"x\" }}'}}}",
			map[string]any{"g": map[string]any{"pw": nil, "a": nil}},
			`{"g":{"a":null,"pw":null}}`,
		},
		"a group's whose ignore a variable decides": {
			"g: {type: group, ignore: '${{ vars.V == \"x\" }}', items: {a: {type: string}}}",
			map[string]any{"g": map[string]any{"a": nil}},
			`{"g":{"a":null}}`,
		},
		"a row's password": {
			"l: {type: list, template: {p: {type: password, optional: true}}}",
			map[string]any{"l": []any{map[string]any{"p": nil}}},
			`{"l":[{"p":null}]}`,
		},
		"a row's whose ignore a variable decides": {
			"l: {type: list, template: {u: {type: string, optional: true, ignore: '${{ vars.V == \"x\" }}'}}}",
			map[string]any{"l": []any{map[string]any{"u": nil}}},
			`{"l":[{"u":null}]}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, res, diags := started(t, workflow(tc.defs), tc.submitted, vars)
			if res == nil {
				t.Fatal(diags)
			}
			if got := canonical(t, res.Run.Inputs.Get()); got != tc.want {
				t.Errorf("the run's inputs are %s, want %s", got, tc.want)
			}
		})
	}
}

// Keep splits all of what Start gives: run state holds no password, no
// variable's value and nothing made from one, a private matrix's instances
// by key and index alone, and no private timeout; the secret record holds
// those, and nothing that isn't.
func TestKeepSplitsWhatStartGives(t *testing.T) {
	src := workflow("pw: {type: password}\nv: {type: string, default: plain}", "timeout: ${{ org.T }}\njobs:\n  a:\n    strategy:\n      matrix:\n        x: ['${{ inputs.pw }}', '${{ org.V }}']\n    steps:\n      - run: echo\n  b:\n    strategy:\n      matrix:\n        y: [one, two]\n    steps:\n      - run: echo\n")
	vars := testVariables{"org.V": {Value: "VARSENT-1"}, "org.T": {Value: "1h"}}
	_, res, diags := started(t, src, map[string]any{"pw": "PWSENT-1"}, vars)
	if res == nil {
		t.Fatal(diags)
	}
	var record plan.Record
	state := res.Keep(&record)
	shown := canonical(t, state)
	for _, s := range []string{"PWSENT", "VARSENT", "3600"} {
		if strings.Contains(shown, s) {
			t.Errorf("run state holds %s: %s", s, shown)
		}
	}
	if !state.TimeoutPrivate || state.TimeoutSeconds != 0 || record.TimeoutSeconds == nil || *record.TimeoutSeconds != 3600 {
		t.Errorf("the timeout: state %d (private %v), record %v", state.TimeoutSeconds, state.TimeoutPrivate, record.TimeoutSeconds)
	}
	a, b := state.Expansions["a"], state.Expansions["b"]
	if a == nil || len(a.Instances) != 2 || a.Instances[1].Key != "a#1" || !a.Instances[1].Matrix.IsZero() {
		t.Errorf("run state's private matrix: %s", canonical(t, a))
	}
	if b == nil || canonical(t, b.Instances[1].Matrix.Get()) != `{"y":"two"}` || record.Expansions["b"] != nil {
		t.Errorf("a matrix that isn't private: state %s, record %s", canonical(t, b), canonical(t, record.Expansions["b"]))
	}
	if x := record.Expansions["a"]; x == nil || !strings.Contains(canonical(t, x), "PWSENT-1") || !strings.Contains(canonical(t, x), "VARSENT-1") {
		t.Errorf("the record's private matrix: %s", canonical(t, x))
	}
	if record.Run == nil || !strings.Contains(canonical(t, record.Run), "PWSENT-1") {
		t.Errorf("the record's values: %s", canonical(t, record.Run))
	}
	// Neither shares what res holds.
	record.Expansions["a"].Instances[0].Matrix = plan.DataOf(map[string]any{"x": "changed"})
	if strings.Contains(canonical(t, res.Expansions["a"]), "changed") {
		t.Error("the record shares the result's expansion")
	}
}

// An input whose ignore read a variable or a password is held out of the
// plan whatever it decided: as private when the ignore can read only
// variables that aren't secrets, directly or through an input made from
// one, as a variable's value is; and as derived, masked as what is made
// from a secret, when it can read a password, or an input held so because
// its own ignore can. Create and Start name it alike.
func TestIgnoredInputsHeldByWhatTheIgnoreReads(t *testing.T) {
	for name, tc := range map[string]struct {
		defs    string
		private bool
	}{
		"a variable": {
			defs: `e: {type: string, ignore: '${{ org.V == ''zzz'' }}'}`, private: true,
		},
		"an input made from a variable": {
			defs:    "c: {type: string, default: '${{ org.V }}'}\ne: {type: string, ignore: '${{ inputs.c == ''zzz'' }}'}",
			private: true,
		},
		"a password": {
			defs: "pw: {type: password}\ne: {type: string, ignore: '${{ inputs.pw == ''zzz'' }}'}",
		},
		"an input held as derived": {
			defs: "pw: {type: password}\nd: {type: string, ignore: '${{ inputs.pw == ''zzz'' }}'}\ne: {type: string, ignore: '${{ inputs.d == ''zzz'' }}'}",
		},
	} {
		t.Run(name, func(t *testing.T) {
			vars := testVariables{"org.V": {Value: "v"}}
			c, res, diags := started(t, workflow(tc.defs), map[string]any{"pw": "p", "d": "dd", "e": "kept"}, vars)
			if res == nil {
				t.Fatal(diags)
			}
			for what, names := range map[string][3][]string{
				"plan": {c.Plan.Run.Private, c.Plan.Run.Secrets, c.Plan.Run.Derived},
				"run":  {res.Run.Private, res.Run.Secrets, res.Run.Derived},
			} {
				private, secret, derived := slices.Contains(names[0], "inputs.e"), slices.Contains(names[1], "inputs.e"), slices.Contains(names[2], "inputs.e")
				if private != tc.private || secret == tc.private || derived == tc.private {
					t.Errorf("%s: inputs.e private %v, secret %v, derived %v; want private %v", what, private, secret, derived, tc.private)
				}
			}
		})
	}
}

// Start refuses a secret record that isn't the one Create gave with the
// plan (ErrMismatch), and says so without a value of it: one of another
// plan, by its digest, and one whose values the plan's digest doesn't
// cover, as what was submitted is, that give other values than the plan
// holds, a value for an input the workflow doesn't define, or a password
// the plan doesn't name. A record that lacks a password the plan names is
// the run's problem, as it is when the run is created without it. What the
// plan doesn't hold, as a password's value, or a value given in place of a
// default that reads a variable, a record of another run of the same plan
// can give otherwise: the digest binds a record to its plan, not its run.
func TestStartRefusesAnotherRecord(t *testing.T) {
	vars := testVariables{"vars.V": {Value: "v"}}
	src := workflow("pw: {type: password, optional: true}\nc: {type: string, optional: true}\nn: {type: number, default: 1}\nd: {type: string, default: '${{ vars.V }}'}")
	created := createRun(t, src, map[string]any{"pw": "SENT-pw", "c": "planned"}, vars)
	start := func(p *plan.Plan, record plan.Record) (*compile.StartResult, problems.Diagnostics, error) {
		return compile.Start(t.Context(), p, compile.StartInput{RunID: "run", Variables: vars, Record: record, Inputs: passThrough{}})
	}
	// The record, with what was submitted changed.
	changed := func(change func(m map[string]any)) plan.Record {
		record := created.Record
		m := maps.Clone(record.Submitted.Get().(map[string]any))
		change(m)
		record.Submitted = plan.DataOf(m)
		return record
	}
	refused := func(name string, p *plan.Plan, record plan.Record) {
		t.Helper()
		res, diags, err := start(p, record)
		if !errors.Is(err, compile.ErrMismatch) {
			t.Errorf("%s: %v, %v, %v; want ErrMismatch", name, res != nil, diags, err)
		}
		if err != nil && strings.Contains(err.Error(), "SENT") {
			t.Errorf("%s: the error quotes a value: %v", name, err)
		}
	}
	for name, change := range map[string]func(m map[string]any){
		"a plain value changed":                func(m map[string]any) { m["c"] = "SENT-other" },
		"a value given over a default":         func(m map[string]any) { m["n"] = 99 },
		"an input the workflow doesn't define": func(m map[string]any) { m["zzz"] = "SENT-unknown" },
		"a password under another name":        func(m map[string]any) { m["pW"] = m["pw"]; delete(m, "pw") },
	} {
		refused(name, created.Plan, changed(change))
	}
	refused("no record", created.Plan, plan.Record{})
	refused("submitted values that aren't a mapping", created.Plan, func() plan.Record {
		record := created.Record
		record.Submitted = plan.DataOf([]any{"SENT-list"})
		return record
	}())
	// Records of other runs, of other plans.
	for name, sub := range map[string]map[string]any{
		"another plain value":    {"pw": "SENT-pw", "c": "SENT-other"},
		"no password":            {"c": "planned"},
		"a value over a default": {"pw": "SENT-pw", "c": "planned", "n": 2},
	} {
		refused(name+", another plan", created.Plan, createRun(t, src, sub, vars).Record)
	}
	other := workflow("pw: {type: password, optional: true}\nc: {type: string, optional: true}")
	refused("another workflow's", created.Plan, createRun(t, other, map[string]any{"pw": "SENT-pw", "c": "planned"}, vars).Record)
	// A password the plan doesn't name, given in a record that names the
	// plan.
	without := createRun(t, src, map[string]any{"c": "planned"}, vars)
	record := without.Record
	record.Submitted = plan.DataOf(map[string]any{"c": "planned", "pw": "SENT-pw"})
	refused("a password the plan doesn't name", without.Plan, record)

	required := createRun(t, workflow("pw: {type: password}"), map[string]any{"pw": "SENT-pw"}, vars)
	record = required.Record
	record.Submitted = plan.DataOf(map[string]any{})
	res, diags, err := start(required.Plan, record)
	if err != nil || res != nil || !slices.ContainsFunc(diags, func(d problems.Diagnostic) bool { return d.Code == problems.InputRequired.Code() }) {
		t.Errorf("a password the record lacks: %v, %v, %v; want input_required", res != nil, diags, err)
	}
	if res, diags, err := start(created.Plan, created.Record); res == nil || err != nil {
		t.Errorf("the record Create gave: %v, %v", diags, err)
	}
}

// Values a form made from expressions that read each other and a variable
// are each what its expression gives as the run starts: with the variable
// as it was, what was submitted, as Create found, and once it changed,
// what the expressions give then. As they can't be evaluated in the order
// they read each other, each reads the others as they were submitted, the
// first first, and what reads them reads what they give.
func TestStartFormValuesThatReadEachOther(t *testing.T) {
	src := workflow("a: {type: string, default: '${{ vars.V == ''v'' && inputs.b || ''x'' }}'}\nb: {type: string, default: '${{ vars.V == ''v'' && inputs.a || ''y'' }}'}\nc: {type: string, default: '${{ inputs.a }}-c'}")
	c := createRun(t, src, map[string]any{"a": "1", "b": "1", "c": "1-c"}, testVariables{"vars.V": {Value: "v"}})
	if len(c.Record.Means) != 3 {
		t.Fatalf("means %v, want a, b and c", c.Record.Means)
	}
	for v, want := range map[string]string{"v": `{"a":"1","b":"1","c":"1-c"}`, "w": `{"a":"x","b":"y","c":"x-c"}`} {
		res, diags := startRun(t, c, testVariables{"vars.V": {Value: v}}, compile.StartInput{RunID: "run"})
		if res == nil {
			t.Fatalf("V=%s: %v", v, diags)
		}
		if got := canonical(t, res.Run.Inputs.Get()); got != want {
			t.Errorf("V=%s: inputs %s, want %s", v, got, want)
		}
	}
}

// Parts of a list's rows that mean their expressions, which read what
// reads the list in turn, are read as Create read them: the list's rows
// with their fields filled in, a field without a value as null, and the
// parts as they were submitted until they are evaluated, by what reads
// them first.
func TestStartReadsRowsThatMeanExpressionsAsCreateDid(t *testing.T) {
	vars := testVariables{"vars.V": {Value: "v"}, "vars.S": {Value: "s", Secret: true}}
	for name, c := range map[string]struct {
		src       string
		submitted map[string]any
		want      string
	}{
		"a default that reads a field the rows don't have, which a row reads": {
			src: workflow(`a: {type: string, optional: true, default: '${{ join(inputs.l.*.p, '','') }}'}
k: {type: string, default: '${{ vars.S }}'}
l: {type: list, optional: true, template: {u: {type: string, optional: true}, p: {type: password, optional: true}}, default: [{u: '${{ vars.V }}'}, {u: '${{ inputs.a }}'}]}`),
			submitted: map[string]any{"a": ",", "l": []any{map[string]any{"u": "v"}, map[string]any{"u": ","}}},
			want:      `{"a":",","k":"s","l":[{"u":"v"},{"u":","}]}`,
		},
		"a default that reads a row's field, whose default reads a part the row means": {
			src: workflow(`a: {type: string, optional: true, default: '${{ inputs.l[0].h }}'}
pw: {type: password, optional: true}
l: {type: list, optional: true, template: {u: {type: string, optional: true}, h: {type: string, optional: true, default: '${{ inputs.l[index].u }}'}}, default: [{u: '${{ inputs.a }}', h: lit}, {u: '${{ vars.V }}', h: '${{ inputs.pw }}'}]}`),
			submitted: map[string]any{"l": []any{map[string]any{"h": "alice", "u": "alice"}}},
			want:      `{"a":"alice","l":[{"h":"alice","u":"alice"}]}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, res, diags := started(t, c.src, c.submitted, vars)
			if res == nil {
				t.Fatal(diags)
			}
			if got := canonical(t, res.Run.Inputs.Get()); got != c.want {
				t.Errorf("inputs %s, want %s", got, c.want)
			}
		})
	}
}

// A default that reads, through a relative reference, a value a form made
// from a variable that changed since reads what the value's expression
// gives as the run starts: a relative reference doesn't make it wait for
// the value, so Start resolves the inputs again with what the expression
// gave in the value's place.
func TestStartFormValueReadThroughARelativeReference(t *testing.T) {
	src := workflow("a: {type: string, optional: true, default: '${{ inputs.c }}'}\nb: {type: string, optional: true, default: '${{ ..a.default }}'}\nc: {type: string, optional: true, default: 'x-${{ vars.V }}'}")
	c := createRun(t, src, map[string]any{"a": "typed", "c": "x-v"}, testVariables{"vars.V": {Value: "v"}})
	res, diags := startRun(t, c, testVariables{"vars.V": {Value: "w"}}, compile.StartInput{RunID: "run"})
	if res == nil {
		t.Fatal(diags)
	}
	if got, want := canonical(t, res.Run.Inputs.Get()), `{"a":"typed","b":"x-w","c":"x-w"}`; got != want {
		t.Errorf("inputs %s, want %s", got, want)
	}
}
