package compile_test

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

func load(t *testing.T, src string) *format.Document {
	t.Helper()
	doc, diags := format.Load("workflow.yaml", []byte(src))
	if diags.HasErrors() {
		t.Fatal(diags)
	}
	return doc
}

const clusterWorkflow = `on:
  execute:
    inputs:
      cluster:
        type: compute-clusters
jobs:
  main:
    steps:
      - run: echo ok
`

// platform resolves clusters by name, and provides no other type.
type platform struct{}

func (platform) ResolveInput(typ string, _ map[string]any, v any) (any, error) {
	if typ != "compute-clusters" {
		return nil, compile.ErrInputType
	}
	if v == "missing" {
		return nil, errors.New("there is no cluster named missing")
	}
	return map[string]any{"name": v, "ip": "10.0.0.1"}, nil
}

func TestPlatformInputs(t *testing.T) {
	doc := load(t, clusterWorkflow)
	inputs, diags := compile.ResolveInputs(doc, map[string]any{"cluster": "a"}, compile.Options{Inputs: platform{}})
	if len(diags) != 0 {
		t.Fatal(diags)
	}
	if got := inputs.Values["cluster"].(map[string]any)["ip"]; got != "10.0.0.1" {
		t.Errorf("cluster = %v, want the resolver's details", inputs.Values)
	}

	_, diags = compile.ResolveInputs(doc, map[string]any{"cluster": "missing"}, compile.Options{Inputs: platform{}})
	if len(diags) != 1 || diags[0].Code != problems.InputRejected.Code() || !strings.Contains(diags[0].Message(), "no cluster named missing") {
		t.Errorf("a cluster the platform rejects: %v", diags)
	}

	doc = load(t, strings.Replace(clusterWorkflow, "compute-clusters", "slurm-partitions\n        resource: ${{ inputs.other }}", 1))
	_, diags = compile.ResolveInputs(doc, map[string]any{"cluster": "debug"}, compile.Options{Inputs: platform{}})
	if len(diags) != 1 || diags[0].Code != problems.PlatformInput.Code() {
		t.Errorf("a type the platform doesn't provide: %v", diags)
	}
}

// rejects is a platform that rejects every value with err.
type rejects struct{ err error }

func (r rejects) ResolveInput(string, map[string]any, any) (any, error) { return nil, r.err }

// TestPlatformInputProblem checks that a resolver's problem of one of
// Meridian's kinds is reported as that kind, with the input's name, and
// that one of a kind Meridian doesn't have is the platform's reason.
func TestPlatformInputProblem(t *testing.T) {
	doc := load(t, clusterWorkflow)
	p := problems.ExecutionGroupUnknown.Problem(problems.Params{"value": problems.Show("gpu"), "groups": problems.QuoteAll([]string{"local"})})
	_, diags := compile.ResolveInputs(doc, map[string]any{"cluster": "gpu"}, compile.Options{Inputs: rejects{fmt.Errorf("resolving: %w", p)}})
	if len(diags) != 1 || diags[0].Code != problems.ExecutionGroupUnknown.Code() {
		t.Fatalf("a resolver's problem: %v", diags)
	}
	d := diags[0]
	if d.Params["input"] != "cluster" || d.Params["value"] != `"gpu"` || d.Params["groups"] != `"local"` || d.Path() != "/on/execute/inputs/cluster" {
		t.Errorf("the problem is %s, with %v at %s", d, d.Params, d.Path())
	}
	if want := `The input "cluster" names the execution group "gpu", which isn't one of the groups: "local".`; d.Message() != want {
		t.Errorf("Message = %q, want %q", d.Message(), want)
	}

	// A problem of a kind that isn't one of the workflow's errors, which
	// would turn a rejection into a warning or a problem of the command's,
	// is the platform's reason.
	for name, p := range map[string]*problem.Problem{
		"a platform's own":   {Code: "quota_exceeded", Detail: "the quota is spent"},
		"a warning's kind":   problems.InputLooksSecret.Problem(problems.Params{"input": "cluster"}),
		"the command's kind": problems.FileNotOwned.Problem(problems.Params{"path": "/etc/passwd"}),
	} {
		// An error stops the run, and its plan with the value, being made.
		_, diags := compile.ResolveInputs(doc, map[string]any{"cluster": "gpu"}, compile.Options{Inputs: rejects{p}})
		if len(diags) != 1 || diags[0].Code != problems.InputRejected.Code() || diags[0].Params["reason"] != p.Detail || !diags.HasErrors() {
			t.Errorf("%s problem: %v", name, diags)
		}
	}
}

// returns is a platform whose value for every input is v.
type returns struct{ v any }

func (r returns) ResolveInput(string, map[string]any, any) (any, error) { return r.v, nil }

// A platform's value for an input is checked as a submitted one is: a plan
// holds only JSON, so a value that isn't is an error, which says what of it
// isn't.
func TestPlatformValueNotJSON(t *testing.T) {
	doc := load(t, clusterWorkflow)
	for _, tc := range []struct {
		v           any
		what, value string
	}{
		{map[string]any{"cores": math.Inf(1)}, "number", "+Inf"},
		{[]any{"a", math.NaN()}, "number", "NaN"},
		{map[string]any{"name": "\xff"}, "text", ""},
		{map[string]any{"\xff": 1}, "key", ""},
		{map[string]any{"since": struct{}{}}, "go_type", "struct {}"},
	} {
		_, diags := compile.ResolveInputs(doc, map[string]any{"cluster": "a"}, compile.Options{Inputs: returns{tc.v}})
		if len(diags) != 1 || diags[0].Code != problems.PlatformValueNotJSON.Code() {
			t.Errorf("%#v: %v, want %s", tc.v, diags, problems.PlatformValueNotJSON.Code())
			continue
		}
		d := diags[0]
		if d.Path() != "/on/execute/inputs/cluster" || d.Params["input"] != "cluster" || d.Params["what"] != tc.what || d.Params["value"] != tc.value {
			t.Errorf("%#v: %s at %q with %v, want what %q and value %q", tc.v, d, d.Path(), d.Params, tc.what, tc.value)
		}
	}
}

func TestDeferredDefaultsEncode(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      dir:
        type: string
        default: ${{ env.HOME }}/work
jobs:
  main:
    steps:
      - run: echo ok
`)
	inputs, diags := compile.ResolveInputs(doc, nil, compile.Options{})
	if len(diags) != 0 {
		t.Fatal(diags)
	}
	if _, ok := inputs.Values["dir"]; ok {
		t.Error("a default that reads env has a value")
	}
	data, err := json.Marshal(inputs.Deferred["dir"])
	if err != nil || !strings.HasPrefix(string(data), `{"${{":[{"src":" env.HOME "`) {
		t.Errorf("the deferred default encodes as %s, %v", data, err)
	}
}

// A submitted row with the text of part of its default that reads a secret
// is deferred whole, with only that part an expression: the same text
// elsewhere in the row is data.
func TestDefaultTextKnownLater(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      hosts:
        type: list
        template:
          token:
            type: string
          note:
            type: string
        default:
          - token: ${{ vars.TOKEN }}
jobs:
  main:
    steps:
      - run: echo ok
`)
	row := map[string]any{"token": "${{ vars.TOKEN }}", "note": "${{ vars.TOKEN }}"}
	submitted := map[string]any{"hosts": []any{row}}
	opts := compile.Options{Contexts: map[string]any{"vars": map[string]any{"TOKEN": expr.Unknown{Name: "vars"}}}}
	inputs, diags := compile.ResolveInputs(doc, submitted, opts)
	if len(diags) != 0 {
		t.Fatal(diags)
	}
	if _, ok := inputs.Values["hosts"]; ok {
		t.Error("a value deferred until the run is underway has a value now")
	}
	data, err := json.Marshal(inputs.Deferred["hosts"])
	if err != nil || !strings.HasPrefix(string(data), `[{"note":"${{ vars.TOKEN }}","token":{"${{":[{"src":" vars.TOKEN "`) {
		t.Errorf("the deferred value encodes as %s, %v", data, err)
	}
	if row["token"] != "${{ vars.TOKEN }}" {
		t.Errorf("submitted values changed to %v", submitted)
	}
}

// A row's default that reads a secret is evaluated once the run is
// underway, with the row's index in it, since no scope gives it then; and
// the list's value with it, all of it.
func TestRowDefaultKnownLater(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      names:
        type: list
        template:
          name:
            type: string
      hosts:
        type: list
        template:
          name:
            type: string
          token:
            type: string
            default: ${{ vars.TOKEN }}-${{ inputs.names.[index].name }}
jobs:
  main:
    steps:
      - run: echo ok
`)
	submitted := map[string]any{
		"names": []any{map[string]any{"name": "a"}, map[string]any{"name": "b"}},
		"hosts": []any{map[string]any{"name": "x"}, map[string]any{"name": "y", "token": "given"}},
	}
	opts := compile.Options{Contexts: map[string]any{"vars": map[string]any{"TOKEN": expr.Unknown{Name: "vars"}}}}
	inputs, diags := compile.ResolveInputs(doc, submitted, opts)
	if len(diags) != 0 {
		t.Fatal(diags)
	}
	if _, ok := inputs.Values["hosts"]; ok {
		t.Error("a value deferred until the run is underway has a value now")
	}
	data, err := json.Marshal(inputs.Deferred["hosts"])
	if err != nil {
		t.Fatal(err)
	}
	var stored plan.Value
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatal(err)
	}
	got, err := stored.Eval(expr.Scope{Contexts: map[string]any{
		"inputs": map[string]any{"names": submitted["names"]},
		"vars":   map[string]any{"TOKEN": "s"},
	}})
	want := []any{map[string]any{"name": "x", "token": "s-a"}, map[string]any{"name": "y", "token": "given"}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("the deferred value is %v, %v; want %v", got, err, want)
	}
}

// Filling in a list's default rows doesn't change the workflow's default.
func TestDefaultRowsAreCopied(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      hosts:
        type: list
        template:
          gpus:
            type: number
            default: 1
        default:
          - {}
jobs:
  main:
    steps:
      - run: echo ok
`)
	for range 2 {
		inputs, diags := compile.ResolveInputs(doc, nil, compile.Options{})
		if len(diags) != 0 {
			t.Fatal(diags)
		}
		if got := fmt.Sprint(inputs.Values["hosts"]); got != "[map[gpus:1]]" {
			t.Errorf("hosts = %s", got)
		}
	}
	dflt, _ := jsonptr.Lookup(doc.Value, []string{"on", "execute", "inputs", "hosts", "default"})
	if got := fmt.Sprint(dflt); got != "[map[]]" {
		t.Errorf("the workflow's default changed to %s", got)
	}
}

// A run's submitted values aren't changed by resolving them.
func TestSubmittedValuesUnchanged(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      group:
        type: group
        items:
          n:
            type: number
          extra:
            type: string
            default: x
jobs:
  main:
    steps:
      - run: echo ok
`)
	submitted := map[string]any{"group": map[string]any{"n": "3"}}
	if _, diags := compile.ResolveInputs(doc, submitted, compile.Options{}); len(diags) != 0 {
		t.Fatal(diags)
	}
	if got := submitted["group"].(map[string]any); got["n"] != "3" || len(got) != 1 {
		t.Errorf("submitted values changed to %v", submitted)
	}
}

// A value of the wrong type in a group drops only that value.
func TestNestedValueError(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      g:
        type: string
      s:
        type: group
        items:
          g:
            type: group
            items:
              x:
                type: string
                optional: true
jobs:
  main:
    steps:
      - run: echo ok
`)
	inputs, diags := compile.ResolveInputs(doc, map[string]any{"g": "mine", "s": map[string]any{"g": "notamap"}}, compile.Options{})
	if len(diags) != 1 || diags[0].Code != problems.InputGroupNotMapping.Code() || diags[0].Path() != "/on/execute/inputs/s/items/g" {
		t.Fatalf("diagnostics = %v", diags)
	}
	if inputs.Values["g"] != "mine" {
		t.Errorf("g = %#v, want the value given for it", inputs.Values["g"])
	}
	if s, _ := inputs.Values["s"].(map[string]any); s == nil || s["g"] != nil {
		t.Errorf("s = %#v, want s.g dropped", inputs.Values["s"])
	}
}

// Submitted values that aren't JSON, which a plan can't hold, are errors.
func TestValuesMustBeJSON(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      s:
        type: string
        optional: true
jobs:
  main:
    steps:
      - run: echo ok
`)
	for name, v := range map[string]any{
		"NaN":         math.NaN(),
		"infinity":    []any{math.Inf(1)},
		"a channel":   make(chan int),
		"bad UTF-8":   map[string]any{"k": "\xff"},
		"a uint":      uint(1),
		"a bad key":   map[string]any{"\xff": 1},
		"a Go struct": struct{}{},
	} {
		for _, key := range []string{"s", "unknown"} {
			inputs, diags := compile.ResolveInputs(doc, map[string]any{key: v}, compile.Options{})
			if !diags.HasErrors() || diags[0].Code != problems.InputNotJSON.Code() {
				t.Errorf("%s in %s: %v", name, key, diags)
			}
			if _, ok := inputs.Values[key]; ok {
				t.Errorf("%s in %s: the value was kept", name, key)
			}
		}
	}
}

// A default that gives a value that isn't JSON, as 0/0 gives NaN, is an
// error at its input, or at the field of a list's rows that holds it, and
// the value is dropped, as a submitted one is, so that the values are JSON.
func TestDefaultsMustBeJSON(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      s:
        type: string
        default: ${{ 0/0 }}
      rows:
        type: list
        template:
          n:
            type: string
            default: ${{ 1/0 }}
        default: [{}]
      fine:
        type: string
        default: ${{ 1/2 }}
jobs:
  main:
    steps:
      - run: echo ok
`)
	inputs, diags := compile.ResolveInputs(doc, nil, compile.Options{})
	var got []string
	for _, d := range diags {
		got = append(got, fmt.Sprintf("%s at %s: %v %v", d.Code, d.Path(), d.Params["input"], d.Params["value"]))
	}
	want := []string{
		"input_not_json at /on/execute/inputs/s: s NaN",
		"input_not_json at /on/execute/inputs/rows/template/n: rows.0.n +Inf",
	}
	if !slices.Equal(got, want) {
		t.Errorf("diagnostics\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if _, err := json.Marshal(inputs.Values); err != nil || !reflect.DeepEqual(inputs.Values, map[string]any{"fine": "0.5"}) {
		t.Errorf("values = %v (%v), want fine's alone", inputs.Values, err)
	}
}

// The expressions of all the inputs' defaults are one group, which builds
// at most 64 MiB, though each default can build 8 MiB.
func TestDefaultsShareABudget(t *testing.T) {
	var b strings.Builder
	b.WriteString("on:\n  execute:\n    inputs:\n      half:\n        type: string\n")
	for i := range 9 {
		fmt.Fprintf(&b, "      d%d:\n        type: string\n        default: ${{ inputs.half + inputs.half }}\n", i)
	}
	b.WriteString("jobs:\n  main:\n    steps:\n      - run: echo ok\n")
	doc := load(t, b.String())
	_, diags := compile.ResolveInputs(doc, map[string]any{"half": strings.Repeat("x", 4<<20)}, compile.Options{})
	if len(diags) != 1 || diags[0].Code != problems.ExpressionsBuildTooMuch.Code() {
		t.Errorf("nine defaults that each build 8 MiB: %v, want %s", diags, problems.ExpressionsBuildTooMuch.Code())
	}
}

// The group of all the inputs' defaults does at most 256 MiB of work,
// though each default can do 32 MiB: eight can, and nine can't.
func TestDefaultsShareAWorkBudget(t *testing.T) {
	for _, tc := range []struct {
		defaults int
		want     problems.Diagnostics
	}{
		{8, nil},
		{9, problems.Diagnostics{problems.ExpressionsTooMuchWork.At("", problems.Position{}, "", problems.Params{"limit": 256})}},
	} {
		var b strings.Builder
		b.WriteString("on:\n  execute:\n    inputs:\n      half:\n        type: string\n")
		for i := range tc.defaults {
			// 8, two references of 16, and half read twice: 32 MiB.
			fmt.Fprintf(&b, "      d%d:\n        type: boolean\n        default: ${{ inputs.half == inputs.half }}\n", i)
		}
		b.WriteString("jobs:\n  main:\n    steps:\n      - run: echo ok\n")
		submitted := map[string]any{"half": strings.Repeat("x", 16<<20-20)}
		_, diags := compile.ResolveInputs(load(t, b.String()), submitted, compile.Options{})
		if len(diags) != len(tc.want) || len(diags) > 0 && (diags[0].Code != tc.want[0].Code || diags[0].Message() != tc.want[0].Message()) {
			t.Errorf("%d defaults that each do 32 MiB of work: %v, want %v", tc.defaults, diags, tc.want)
		}
	}
}

// A default doesn't count what the default of an input it reads built.
func TestDefaultsCountOnTheirOwn(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      half:
        type: string
      a:
        type: string
        default: ${{ inputs.half + inputs.half }}
      b:
        type: string
        default: ${{ inputs.a + inputs.half }}
jobs:
  main:
    steps:
      - run: echo ok
`)
	inputs, diags := compile.ResolveInputs(doc, map[string]any{"half": strings.Repeat("x", 5<<19)}, compile.Options{})
	if b, _ := inputs.Values["b"].(string); len(diags) != 0 || len(b) != 15<<19 {
		t.Errorf("b, 7.5 MiB, reads a, 5 MiB: %d bytes, %v", len(b), diags)
	}
}

// Defaults are filled in the order they need, whichever way they read each
// other, through a list's rows too. Those that read each other, and those
// that read them, are an error, which names them in order.
func TestDefaultsFillInOrder(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      a: {type: string, default: '${{ inputs.b }}-a'}
      b: {type: string, default: '${{ inputs.hosts[1].name }}-b'}
      c: {type: string, default: '${{ inputs.z }}-c'}
      hosts:
        type: list
        template:
          name: {type: string, default: '${{ inputs.base }}-row'}
        default: [{}, {}]
      base: {type: string, default: '${{ inputs.c }}-base'}
      x: {type: string, default: '${{ inputs.y }}'}
      y: {type: string, default: '${{ inputs.w }}'}
      w: {type: string, default: '${{ inputs.x }}'}
      reader: {type: string, default: '${{ inputs.y }}', optional: true}
      z: {type: string, default: z}
jobs:
  main:
    steps:
      - run: echo ok
`)
	inputs, diags := compile.ResolveInputs(doc, nil, compile.Options{})
	if len(diags) != 1 || diags[0].Code != problems.DefaultsReadEachOther.Code() ||
		diags[0].Params["inputs"] != `"reader", "w", "x", "y"` {
		t.Fatalf("diagnostics %v, want %s naming reader, w, x and y", diags, problems.DefaultsReadEachOther.Code())
	}
	if got, want := inputs.Values["a"], "z-c-base-row-b-a"; got != want {
		t.Errorf("a = %v, want %v", got, want)
	}
}

// An input's ignore that is true changes what its optional reads of it: an
// ignored input that is evaluated once the run is underway has no value,
// rather than one that isn't known yet.
func TestConditionsReadIgnored(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      token:
        type: string
        default: ${{ vars.TOKEN }}
        hidden: ${{ false }}
        ignore: true
        optional: ${{ inputs.token == null }}
jobs:
  main:
    steps:
      - run: echo ok
`)
	opts := compile.Options{Contexts: map[string]any{"vars": map[string]any{"TOKEN": expr.Unknown{Name: "vars"}}}}
	inputs, diags := compile.ResolveInputs(doc, nil, opts)
	if len(diags) != 0 || len(inputs.Deferred) != 0 {
		t.Errorf("diagnostics %v and deferred %v, want none", diags, inputs.Deferred)
	}
}

// Defaults are filled in passes over those that wait, in order: each pass
// fills in each one that reads none that still waits. A relative reference
// to another input's default reads the values as they are then, so e,
// filled in the first pass after c, reads c's value.
func TestDefaultsFillInPasses(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      a: {type: string, default: '${{ inputs.b }}'}
      b: {type: string, default: "${{ 'x' }}"}
      c: {type: string, default: '${{ inputs.b }}-c'}
      e: {type: string, default: '${{ ..f.default }}'}
      f: {type: string, default: '${{ inputs.c }}'}
jobs:
  main:
    steps:
      - run: echo ok
`)
	inputs, diags := compile.ResolveInputs(doc, nil, compile.Options{})
	want := map[string]any{"a": "x", "b": "x", "c": "x-c", "e": "x-c", "f": "x-c"}
	if len(diags) != 0 || !reflect.DeepEqual(inputs.Values, want) {
		t.Errorf("values %v, %v; want %v", inputs.Values, diags, want)
	}
}

// The submitted values for inputs the workflow doesn't define are each a
// warning that suggests the name probably meant, the first 100 of them, in
// the order of their names, a group's in its place; and one more warning,
// where the rest would be, counts them. The values are kept.
func TestUnknownInputsListed(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      name: {type: string, optional: true}
      group: {type: group, items: {host: {type: string, optional: true}}}
jobs:
  main:
    steps:
      - run: echo ok
`)
	submitted := map[string]any{"group": map[string]any{"hots": "x"}}
	for i := range 250 {
		submitted[fmt.Sprintf("n%03d", i)] = i
	}
	inputs, diags := compile.ResolveInputs(doc, submitted, compile.Options{})
	var listed []string
	var more problems.Diagnostics
	for _, d := range diags {
		switch d.Code {
		case problems.UnknownInput.Code():
			listed = append(listed, d.Params["input"].(string))
		case problems.ProblemsNotListed.Code():
			more = append(more, d)
		default:
			t.Errorf("unexpected %s", d)
		}
	}
	want := []string{"group.hots"}
	for i := range 99 {
		want = append(want, fmt.Sprintf("n%03d", i))
	}
	slices.Sort(listed)
	slices.Sort(want)
	if !slices.Equal(listed, want) {
		t.Errorf("listed %v, want %v", listed, want)
	}
	if len(more) != 1 || more[0].Params["code"] != "unknown_input" || more[0].Params["count"] != 151 ||
		more[0].Params["limit"] != 100 || more[0].Path() != "/on/execute/inputs" {
		t.Errorf("the rest: %v, want one that counts 151 at /on/execute/inputs", more)
	}
	if len(inputs.Values) != 251 {
		t.Errorf("%d values, want 251", len(inputs.Values))
	}
}

// A condition that reads an input whose value isn't known until the run
// starts, because a part of it reads a secret, isn't known when the run is
// created either, any part of the input, whichever default was evaluated
// last: here z's, which gives "" and so sets no value, after the field x of
// l's row, which reads the secret.
func TestConditionReadsInputHeldForTheRun(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      c: {type: string, optional: true, hidden: '${{ inputs.l[0].y == "b" }}'}
      l:
        type: list
        template:
          x: {type: string, default: '${{ vars.S }}'}
          y: {type: string, default: b}
      z: {type: string, optional: true, default: "${{ '' }}"}
jobs:
  main:
    steps:
      - run: echo ok
`)
	opts := compile.Options{Contexts: map[string]any{"vars": map[string]any{"S": expr.Unknown{Name: "vars"}}}}
	inputs, diags := compile.ResolveInputs(doc, map[string]any{"l": []any{map[string]any{}}}, opts)
	if len(diags) != 1 || diags[0].Code != problems.InputConditionNotKnown.Code() || diags[0].Params["input"] != "c" {
		t.Errorf("diagnostics %v, want %s for c", diags, problems.InputConditionNotKnown.Code())
	}
	if _, ok := inputs.Deferred["l"]; !ok {
		t.Errorf("deferred %v, want l", inputs.Deferred)
	}
}

// A dropdown's and a multi-dropdown's options match a value by their text,
// so that the option 8 matches the value "8".
func TestOptionsMatchByText(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      one: {type: dropdown, optional: true, options: [8, "9", true, 1.5, {value: 2, label: Two}, x]}
      many: {type: multi-dropdown, optional: true, options: [8, "9", true, 1.5, {value: 2, label: Two}, x]}
jobs:
  main:
    steps:
      - run: echo ok
`)
	for _, tc := range []struct {
		value any
		ok    bool
	}{
		{8, true},
		{"8", true},
		{8.0, true},
		{"8.0", false},
		{9, true},
		{"9", true},
		{"true", true},
		{true, true},
		{1.5, true},
		{"1.5", true},
		{2, true},
		{"2", true},
		{"Two", false},
		{"x", true},
		{"y", false},
		{[]any{8}, false},
	} {
		for _, input := range []string{"one", "many"} {
			v := tc.value
			if input == "many" {
				v = []any{"x", tc.value}
			}
			_, diags := compile.ResolveInputs(doc, map[string]any{input: v}, compile.Options{})
			if got := !diags.HasErrors(); got != tc.ok {
				t.Errorf("%s given %#v: %v, want ok %v", input, v, diags, tc.ok)
			}
		}
	}
}

// A value that isn't JSON in a list's rows is reported at the innermost
// field of the rows that holds it. Here the input a and the wizard step's
// input a share a path, and so do their rows' fields z: the step's rows are
// collected first, since a waits for its form's text, so the step's z,
// whose default gives NaN, is the one reported.
func TestNotJSONAtTheFirstRowField(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      a:
        type: list
        template:
          x: {type: string}
          z: {type: number, label: Outer}
        default: [{x: "${{ 'q' }}"}]
      s:
        type: step
        title: Step
        options:
          a:
            type: list
            template:
              z: {type: number, label: Inner, default: '${{ 0/0 }}'}
jobs:
  main:
    steps:
      - run: echo ok
`)
	_, diags := compile.ResolveInputs(doc, map[string]any{"a": []any{map[string]any{"x": "${{ 'q' }}"}}}, compile.Options{})
	if len(diags) != 1 || diags[0].Code != problems.InputNotJSON.Code() ||
		diags[0].Path() != "/on/execute/inputs/s/options/a/template/z" || diags[0].Params["input"] != "a.0.z (Inner)" {
		t.Errorf("diagnostics %v, want %s at the step's z", diags, problems.InputNotJSON.Code())
	}
}

// A multi-dropdown's item that is the text of more than one of its options'
// values means the first: here .label reads each option's label.
func TestItemTextIsTheFirstOptions(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      m:
        type: multi-dropdown
        options:
          - {label: A, value: "${{ .label }}"}
          - {label: B, value: "${{ .label }}"}
jobs:
  main:
    steps:
      - run: echo ok
`)
	inputs, diags := compile.ResolveInputs(doc, map[string]any{"m": []any{"${{ .label }}"}}, compile.Options{})
	if want := []any{"A"}; len(diags) != 0 || !reflect.DeepEqual(inputs.Values["m"], want) {
		t.Errorf("m = %v, %v; want %v", inputs.Values["m"], diags, want)
	}
}

// A field that waits for a default reads the values once every field at
// the path it reads is filled in, and no longer waits for one filled in
// before it was added: l's row is added once l's default is evaluated,
// after the input a's and before the step's a's, which share a path.
func TestRowWaitsForWhatIsLeft(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      a: {type: string, default: "${{ 'x' }}"}
      l:
        type: list
        default: "${{ fromJSON('[{}]') }}"
        template:
          f: {type: string, default: '${{ inputs.a }}'}
      s:
        type: step
        title: Step
        options:
          a: {type: string, default: "${{ 'y' }}"}
jobs:
  main:
    steps:
      - run: echo ok
`)
	inputs, diags := compile.ResolveInputs(doc, nil, compile.Options{})
	if want := []any{map[string]any{"f": "y"}}; len(diags) != 0 || !reflect.DeepEqual(inputs.Values["l"], want) {
		t.Errorf("l = %v, %v; want %v", inputs.Values["l"], diags, want)
	}
}
