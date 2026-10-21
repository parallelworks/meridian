package compile_test

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/expr"
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
	if len(inputs.Values) != 0 || len(inputs.Secrets) != 0 {
		t.Errorf("values %v and secrets %v, want none", inputs.Values, inputs.Secrets)
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
	got, err := p.Run.DeferredInputs["n"].Eval(expr.Scope{Contexts: map[string]any{"vars": map[string]any{"N": "6"}}})
	if err != nil || got != "6" {
		t.Errorf("n is %v, %v, as the run starts", got, err)
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
		if _, ok := p.Run.DeferredInputs["summary"]; !ok {
			t.Errorf("%s: summary isn't evaluated as the run starts", read)
		}
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
}
