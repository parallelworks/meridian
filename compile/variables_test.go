package compile_test

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// A diagnostic never quotes a value that came from a variable, or was made
// from one: each param that would show it is masked, and a suggestion near
// it is left out. The same problem with a value that didn't come from a
// variable shows it.
func TestDiagnosticsHideVariables(t *testing.T) {
	for _, tc := range []struct {
		name, workflow string
		value          string
		kind           *problems.Kind
		masked         []string
	}{
		{
			"needs naming no job", "jobs:\n  build: {steps: [{run: make}]}\n  test:\n    needs: ${{ vars.X }}\n    steps: [{run: make}]\n",
			"s3cr3t", problems.NeedsUnknownJob,
			[]string{"needed"},
		},
		{
			"needs near a job", "jobs:\n  build: {steps: [{run: make}]}\n  test:\n    needs: ${{ vars.X }}\n    steps: [{run: make}]\n",
			"buildd", problems.NeedsUnknownJob,
			[]string{"needed"},
		},
		{
			"needs listing what isn't a name", "jobs:\n  build: {steps: [{run: make}]}\n  test:\n    needs: ${{ fromJSON(vars.X) }}\n    steps: [{run: make}]\n",
			`[{"k": "s3cr3t"}]`, problems.NeedsItemNotName,
			[]string{"value"},
		},
		{
			"needs that isn't a name", "jobs:\n  build: {steps: [{run: make}]}\n  test:\n    needs: ${{ fromJSON(vars.X) }}\n    steps: [{run: make}]\n",
			`{"k": "s3cr3t"}`, problems.NeedsNotName,
			[]string{"value"},
		},
		{
			"a default that isn't a number", inputs("n: {type: number, default: '${{ vars.X }}'}"),
			"s3cr3t", problems.InputNotNumber,
			[]string{"value"},
		},
		{
			"a default below its minimum", inputs("n: {type: number, min: 1, default: '${{ vars.X }}'}"),
			"-7", problems.InputBelowMin,
			[]string{"value"},
		},
		{
			"a default that isn't an option", inputs("d: {type: dropdown, options: [a, b], default: '${{ vars.X }}'}"),
			"s3cr3t", problems.InputNotOption,
			[]string{"value"},
		},
		{
			"a default a platform rejects", inputs("c: {type: compute-clusters, default: '${{ vars.X }}'}"),
			"missing", problems.InputRejected,
			[]string{"reason"},
		},
		{
			"a default that fails", inputs("d: {type: string, default: '${{ fromJSON(vars.X) }}'}"),
			"s3cr3t{", problems.FromJSONInvalid,
			[]string{"text"},
		},
		{
			"a default that reads a default that read a variable", inputs("a: {type: string, default: '${{ vars.X }}'}", "b: {type: string, default: '${{ fromJSON(inputs.a) }}'}"),
			"s3cr3t{", problems.FromJSONInvalid,
			[]string{"text"},
		},
		{
			"a default that reads a row that read a variable through a slice",
			inputs("rows: {type: list, template: {x: {type: string, default: '${{ vars.X }}'}}, default: [{}]}",
				"z: {type: string, default: '${{ fromJSON(inputs.rows[0:1][0].x) }}'}"),
			"s3cr3t{", problems.FromJSONInvalid,
			[]string{"text"},
		},
		{
			"a default that reads a row that read a variable by an index written 00",
			inputs("rows: {type: list, template: {x: {type: string, default: '${{ vars.X }}'}}, default: [{}]}",
				"z: {type: string, default: '${{ fromJSON(inputs.rows[00].x) }}'}"),
			"s3cr3t{", problems.FromJSONInvalid,
			[]string{"text"},
		},
		{
			"a condition that fails", inputs("h: {type: string, optional: true, hidden: '${{ map(vars.X, x => x) }}'}"),
			"s3cr3t", problems.NotListOrMapping,
			[]string{"value"},
		},
		{
			"a default that isn't JSON", inputs("s: {type: string, default: '${{ vars.X / 0 }}'}"),
			"31337", problems.InputNotJSON,
			[]string{"value"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := compile.Options{Inputs: platform{}, Contexts: map[string]any{"vars": map[string]any{"X": tc.value}}}
			_, diags := compile.Compile("workflow.yaml", []byte(tc.workflow), nil, opts)
			i := slices.IndexFunc(diags, func(d problems.Diagnostic) bool { return d.Code == tc.kind.Code() })
			if i < 0 {
				t.Fatalf("diagnostics %v, want %s", diags, tc.kind.Code())
			}
			d := diags[i]
			for _, name := range tc.masked {
				if d.Params[name] != problems.Masked {
					t.Errorf("%s = %v, want it masked", name, d.Params[name])
				}
			}
			if s, ok := d.Params["suggested"]; ok && (s != "false" || d.Params["suggestion"] != "") {
				t.Errorf("suggests %v", d.Params["suggestion"])
			}
			if strings.Contains(fmt.Sprint(d.Params), tc.value) || strings.Contains(d.Message(), tc.value) {
				t.Errorf("%s shows the variable's value", d)
			}

			// The same value from an input, rather than a variable, is shown.
			workflow := strings.ReplaceAll(tc.workflow, "vars.X", "inputs.x")
			if strings.HasPrefix(workflow, "jobs:") {
				workflow = inputs("x: {type: string}") + strings.TrimPrefix(workflow, "jobs:\n")
			} else {
				workflow = strings.Replace(workflow, "    inputs:\n", "    inputs:\n      x: {type: string}\n", 1)
			}
			_, diags = compile.Compile("workflow.yaml", []byte(workflow), map[string]any{"x": tc.value}, compile.Options{Inputs: platform{}})
			i = slices.IndexFunc(diags, func(d problems.Diagnostic) bool { return d.Code == tc.kind.Code() })
			if i < 0 || slices.ContainsFunc(tc.masked, func(name string) bool { return diags[i].Params[name] == problems.Masked }) {
				t.Errorf("from an input: %v, want %s showing the value", diags, tc.kind.Code())
			}
		})
	}
}

// Two inputs can have the same path, as an input and a wizard step's input
// beside it do. A problem with the value there doesn't show it when a
// variable gave it, whichever input's problem it is.
func TestDiagnosticsHideVariablesAtSharedPaths(t *testing.T) {
	src := inputs(
		"n: {type: number, optional: true}",
		"s: {type: step, title: Step, options: {n: {type: string, default: '${{ vars.X }}'}}}",
	)
	opts := compile.Options{Contexts: map[string]any{"vars": map[string]any{"X": "s3cr3t"}}}
	_, diags := compile.Compile("workflow.yaml", []byte(src), nil, opts)
	i := slices.IndexFunc(diags, func(d problems.Diagnostic) bool { return d.Code == problems.InputNotNumber.Code() })
	if i < 0 || diags[i].Params["value"] != problems.Masked || strings.Contains(fmt.Sprint(diags), "s3cr3t") {
		t.Errorf("diagnostics %v, want %s with the value masked", diags, problems.InputNotNumber.Code())
	}
}

// A value given in the place of what reads a variable or a password can be
// what it would have made, as a list given as text whose rows' defaults
// read one can, so a problem with it doesn't show it. One in the place of
// what reads neither is shown.
func TestDiagnosticsHideValuesOverPrivateDefaults(t *testing.T) {
	const typed = "TYPED-1234"
	opts := compile.Options{Contexts: map[string]any{"org": map[string]any{"X": "v"}}}
	for name, tc := range map[string]struct {
		defs   []string
		masked bool
	}{
		"a row's default reads a variable":         {[]string{"l: {type: list, template: {x: {type: string, default: '${{ org.X }}'}}}"}, true},
		"a row's default reads a password":         {[]string{"pw: {type: password, optional: true}", "l: {type: list, template: {x: {type: string, default: '${{ inputs.pw }}'}}}"}, true},
		"a row's default reads a variable's input": {[]string{"v: {type: string, default: '${{ org.X }}'}", "l: {type: list, template: {x: {type: string, default: '${{ inputs.v }}'}}}"}, true},
		"a row's default reads neither":            {[]string{"l: {type: list, template: {x: {type: string, default: plain}}}"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			_, diags := compile.Compile("workflow.yaml", []byte(inputs(tc.defs...)), map[string]any{"l": `[{"x":"` + typed + `"}]`}, opts)
			i := slices.IndexFunc(diags, func(d problems.Diagnostic) bool { return d.Code == problems.InputNotList.Code() })
			if i < 0 {
				t.Fatalf("diagnostics %v, want %s", diags, problems.InputNotList.Code())
			}
			if shown := strings.Contains(fmt.Sprint(diags[i].Params), typed); shown == tc.masked {
				t.Errorf("%s: shown %v, want masked %v", diags[i], shown, tc.masked)
			}
		})
	}
}

// So does a problem with the shape of such a value: a group given as text
// whose items' defaults read a variable or a password, and a value that
// isn't JSON.
func TestDiagnosticsHideShapesOverPrivateDefaults(t *testing.T) {
	const typed = "TYPED-1234"
	opts := compile.Options{Contexts: map[string]any{"org": map[string]any{"X": "v"}}}
	for name, tc := range map[string]struct {
		defs   []string
		masked bool
	}{
		"an item's default reads a variable": {[]string{"g: {type: group, items: {x: {type: string, default: '${{ org.X }}'}}}"}, true},
		"an item's default reads a password": {[]string{"pw: {type: password, optional: true}", "g: {type: group, items: {x: {type: string, default: '${{ inputs.pw }}'}}}"}, true},
		"an item's default reads neither":    {[]string{"g: {type: group, items: {x: {type: string, default: plain}}}"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			_, diags := compile.Compile("workflow.yaml", []byte(inputs(tc.defs...)), map[string]any{"g": `{"x":"` + typed + `"}`}, opts)
			i := slices.IndexFunc(diags, func(d problems.Diagnostic) bool { return d.Code == problems.InputGroupNotMapping.Code() })
			if i < 0 {
				t.Fatalf("diagnostics %v, want %s", diags, problems.InputGroupNotMapping.Code())
			}
			if shown := strings.Contains(fmt.Sprint(diags[i].Params), typed); shown == tc.masked {
				t.Errorf("%s: shown %v, want masked %v", diags[i], shown, tc.masked)
			}
		})
	}
}

// inputs returns a workflow with the input definitions defs, each a line of
// YAML, and a job.
func inputs(defs ...string) string {
	var b strings.Builder
	b.WriteString("on:\n  execute:\n    inputs:\n")
	for _, d := range defs {
		fmt.Fprintf(&b, "      %s\n", d)
	}
	b.WriteString("jobs:\n  main:\n    steps:\n      - run: echo ok\n")
	return b.String()
}

// An input whose default reads a variable has no value in the plan, but its
// default is evaluated as the run starts. What the compiler decides reads
// its value: whether another input is ignored, and which jobs a job needs.
// A password's value isn't one of the secrets the compiler returns, since
// the plan names it among the run's secrets, which it reads as it starts.
func TestDefaultsThatReadVariables(t *testing.T) {
	src := `on:
  execute:
    inputs:
      after:
        type: string
        default: ${{ org.AFTER }}
      key:
        type: password
        default: ${{ vars.KEY }}
      n:
        type: number
        default: ${{ vars.N }}
      skipped:
        type: string
        ignore: ${{ inputs.after == 'build' }}
jobs:
  build:
    steps:
      - run: make
  test:
    needs: ${{ inputs.after }}
    steps:
      - run: make test ${{ inputs.key }}
`
	opts := compile.Options{Contexts: map[string]any{
		"org":  map[string]any{"AFTER": "build"},
		"vars": map[string]any{"KEY": "s3cr3t", "N": "5"},
	}}
	inputs, diags := compile.ResolveInputs(load(t, src), map[string]any{"skipped": "x"}, opts)
	if len(diags) != 0 {
		t.Fatal(diags)
	}
	// skipped's ignore read after, which reads a variable that isn't a
	// secret, so its value is held, out of the plan, whatever the ignore
	// decided, as private, as the variable's value is.
	if len(inputs.Values) != 0 || len(inputs.Secrets) != 0 || len(inputs.Private) != 1 || inputs.Private["skipped"] != "x" {
		t.Errorf("values %v, secrets %v and private %v, want skipped's alone, private", inputs.Values, inputs.Secrets, inputs.Private)
	}
	if got := slices.Sorted(maps.Keys(inputs.Deferred)); !slices.Equal(got, []string{"after", "key", "n"}) {
		t.Errorf("deferred %v, want after, key and n", got)
	}

	p, diags := compile.Compile("workflow.yaml", []byte(src), nil, opts)
	if diags.HasErrors() {
		t.Fatal(diags)
	}
	if got := p.Jobs[1].Needs; len(got) != 1 || got[0].Job != "build" {
		t.Errorf("needs %v, want build", got)
	}
	if !slices.Equal(p.Run.Secrets, []string{"inputs.key"}) || !slices.Equal(p.Run.Variables, []string{"org.AFTER", "vars.KEY", "vars.N"}) {
		t.Errorf("secrets %v and variables %v", p.Run.Secrets, p.Run.Variables)
	}
}

// A plan holds no variable's value that a default reads from a row whose
// field read the variable, however its path reaches the field: the default
// is evaluated as the run starts.
func TestPlanHoldsNoVariableFromRows(t *testing.T) {
	for _, read := range []string{
		"toJSON(inputs.rows)", "toJSON(inputs.rows[0:1])", "toJSON(inputs.rows[-1:])", "toJSON(inputs.rows.*.x)",
		"inputs.rows[00].x", "inputs.rows['+0'].x", "inputs.rows[-0].x", "inputs.rows[:][0].x",
	} {
		src := inputs("rows: {type: list, template: {x: {type: string, default: '${{ vars.REGION }}'}}, default: [{}]}",
			`summary: {type: string, default: "${{ `+read+` }}"}`)
		p, diags := compile.Compile("workflow.yaml", []byte(src), nil, compile.Options{
			Contexts: map[string]any{"vars": map[string]any{"REGION": "s3cr3t-region"}},
		})
		if diags.HasErrors() {
			t.Fatalf("%s: %v", read, diags)
		}
		encoded, err := plan.Encode(p)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(encoded, []byte("s3cr3t-region")) {
			t.Errorf("%s: the plan holds the variable's value: %s", read, encoded)
		}
	}
}

// variableSentinel starts each value of a variable that no plan or
// diagnostic may hold.
const variableSentinel = "VARIABLE-SENTINEL-"

// No plan, and no diagnostic, holds an organization or user variable's
// value, nor what a launch form made from one. Each inputs and compile
// conformance workflow whose case gives variables is compiled with each
// one's value its own text, unless it is a secret: with the case's inputs,
// and with what a form submits for them, which evaluates each default it
// can, those that read a variable too, and sends what it gives; and with
// that sent as ACTIVATE's form can send it otherwise (formShapes), and
// once the variables have changed.
func TestVariablesNeverInPlans(t *testing.T) {
	given, forms := 0, 0
	for _, area := range []string{"inputs", "compile"} {
		cases, err := conformance.Cases(area)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			opts := compile.Options{Contexts: contexts(c)}
			if !c.NoPlatform {
				opts.Inputs = passThrough{}
			}
			// later holds the variables as they are once they have changed.
			later := compile.Options{Inputs: opts.Inputs, Contexts: maps.Clone(opts.Contexts)}
			changed := false
			for _, name := range []string{"org", "vars"} {
				vars, _ := opts.Contexts[name].(map[string]any)
				vars, changedVars := maps.Clone(vars), maps.Clone(vars)
				for k, v := range vars {
					if _, ok := v.(string); ok {
						vars[k], changedVars[k], changed = variableSentinel+k, "changed", true
					}
				}
				if vars != nil {
					opts.Contexts[name], later.Contexts[name] = vars, changedVars
				}
			}
			data, err := c.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			doc, diags := format.Load("workflow.yaml", data)
			if !changed || diags.HasErrors() {
				continue
			}
			form := compile.FormValues(doc, c.Inputs, opts)
			forms += strings.Count(fmt.Sprint(form), variableSentinel)
			// A form sends what the person typed, which the compiler
			// compares with what its default gives.
			for k, v := range c.Inputs {
				if s, isText := v.(string); isText && !strings.Contains(s, "${{") && form[k] == nil {
					t.Errorf("%s: the form doesn't send %s", c.Name, k)
				}
			}
			type run struct {
				submitted map[string]any
				opts      compile.Options
			}
			runs := []run{{c.Inputs, opts}, {form, later}}
			for _, submitted := range formShapes(form) {
				runs = append(runs, run{submitted, opts})
			}
			for _, run := range runs {
				given++
				p, diags := compile.Compile("workflow.yaml", data, run.submitted, run.opts)
				for _, d := range diags {
					if strings.Contains(fmt.Sprint(d.Params), variableSentinel) || strings.Contains(d.Message(), variableSentinel) {
						t.Errorf("%s: %s shows a variable's value", c.Name, d)
					}
				}
				if p != nil {
					encoded, err := plan.Encode(p)
					if err != nil {
						t.Fatal(err)
					}
					if bytes.Contains(encoded, []byte(variableSentinel)) {
						t.Errorf("%s: the plan holds a variable's value: %s", c.Name, encoded)
					}
				}
			}
		}
	}
	if given == 0 || forms == 0 {
		t.Fatalf("compiled %d times, %d of them with a form that read a variable", given, forms)
	}
}

// A name looks like a secret's when one of its words, split at
// punctuation, spaces and changes of case, is a word such as token, or two
// in a row are such as api key. A word is whole.
func TestSecretNames(t *testing.T) {
	looks := []string{
		"token", "HF_TOKEN", "github-token", "token2", "authToken", "apiKey", "APIKey", "api_key", "api.key", "db password",
		"user_password", "passwd", "pwd", "passphrase", "secret", "client_secret", "privateKey", "SSH_PRIVATE_KEY",
		"AWS_SECRET_ACCESS_KEY", "accessKey", "apikey", "credentials", "gcp-credential",
	}
	doesnt := []string{
		"tokenizer", "max_tokens", "secretary", "key", "ssh_key", "gpu_resource_key", "keyboard", "passport",
		"compass", "author", "auth_method", "session_name", "license_user", "pw_context", "region",
	}
	var b strings.Builder
	b.WriteString("jobs:\n  main:\n    steps:\n")
	for _, name := range slices.Concat(looks, doesnt) {
		fmt.Fprintf(&b, "      - run: echo ${{ vars['%s'] }}\n", name)
	}
	_, diags := compile.Compile("workflow.yaml", []byte(b.String()), nil, compile.Options{})
	var warned []string
	for _, d := range diags {
		if d.Code == problems.VariableLooksSecret.Code() {
			warned = append(warned, d.Params["name"].(string))
		}
	}
	if !slices.Equal(warned, looks) {
		t.Errorf("warned about %q, want %q", warned, looks)
	}
	// A value submitted for an input the workflow doesn't define is taken
	// for a password by the same names, and by a word a letter from one of
	// the longer words, as a misspelling is, but not from token, whose
	// neighbours are common words.
	misspelled := []string{"pasword", "passwrod", "old_passowrd", "pasphrase", "secrte", "credentail", "passed"}
	for _, name := range slices.Concat(looks, misspelled) {
		if !compile.NamesSecret(name) {
			t.Errorf("%s isn't taken for a secret's name", name)
		}
	}
	for _, name := range slices.Concat(doesnt, []string{"taken", "tokens", "maxTokens", "toke", "pass", "credit"}) {
		if compile.NamesSecret(name) {
			t.Errorf("%s is taken for a secret's name", name)
		}
	}
}

// A key of a list's default rows is checked against the inputs a row of
// it has: those of every list at its path, where a list and a wizard
// step's list share one, so a key the other's rows have isn't reported.
// A list among a row's inputs is named by its path with [index] for the
// row's, as input_looks_secret names a row's input.
func TestDefaultRowKeys(t *testing.T) {
	src := inputs(
		"$meta: {wizard: {mode: wizard}}",
		"l: {type: list, template: {a: {type: string, optional: true}}, default: [{a: x, b: y, c: z}]}",
		"s: {type: step, title: S, options: {l: {type: list, template: {b: {type: string, optional: true}}}}}",
		"m: {type: list, template: {n: {type: list, optional: true, template: {q: {type: string}}, default: [{q: x, z: y}]}}}",
	)
	_, diags := compile.Compile("workflow.yaml", []byte(src), nil, compile.Options{})
	var got []string
	for _, d := range diags {
		if d.Code == problems.DefaultRowUnknownInput.Code() {
			got = append(got, fmt.Sprintf("%s %v %v", d.Path(), d.Params["input"], d.Params["key"]))
		}
	}
	want := []string{"/on/execute/inputs/l/default/0/c l c", "/on/execute/inputs/m/template/n/default/0/z m.[index].n z"}
	if !slices.Equal(got, want) {
		t.Errorf("default rows' keys %q, want %q", got, want)
	}
}
