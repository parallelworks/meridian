package main

import (
	"context"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"
	"go.yaml.in/yaml/v3"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/problems"
)

var update = flag.Bool("update", false, "rewrite the golden files of meridian validate's output")

// conformanceCase returns the conformance case name, such as
// compile/conditions.
func conformanceCase(t *testing.T, name string) conformance.Case {
	t.Helper()
	area, _, _ := strings.Cut(name, "/")
	cases, err := conformance.Cases(area)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("there is no conformance case %s", name)
	return conformance.Case{}
}

// caseEnv writes a config directory holding a variables file with the
// case's org and vars, its secret variables as secrets, and a file of its
// input values, and returns the environment validate runs in and the
// inputs file's path, or "" when it has no inputs.
func caseEnv(t *testing.T, c conformance.Case) (map[string]string, string) {
	t.Helper()
	dir := t.TempDir()
	config := filepath.Join(dir, "config")
	if err := os.Mkdir(config, 0o700); err != nil {
		t.Fatal(err)
	}
	vars := map[string]map[string]any{}
	for _, context := range []string{"org", "vars"} {
		m, _ := c.Contexts[context].(map[string]any)
		vars[context] = map[string]any{}
		for name, v := range m {
			vars[context][name] = fmt.Sprint(v)
		}
	}
	for _, name := range c.SecretVariables {
		vars["vars"][name] = map[string]any{"secret": true, "env": "SECRET_" + strings.ToUpper(name)}
	}
	data, err := yaml.Marshal(vars)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config, local.VariablesFile), data, 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"HOME": dir, local.EnvConfigDir: config}
	if len(c.Inputs) == 0 {
		return env, ""
	}
	inputs := filepath.Join(dir, "inputs.json")
	data, err = jsonv2.Marshal(c.Inputs, jsonv2.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(inputs, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return env, inputs
}

// TestValidateGolden checks what meridian validate writes for conformance
// cases, in each format, against the golden files in testdata/validate,
// which go test -update rewrites. The cases' contexts are in a variables
// file, and their inputs in a file -inputs reads.
func TestValidateGolden(t *testing.T) {
	parallel(t)
	for _, tc := range []struct {
		name   string
		format string
	}{
		{"compile/context-steps-in-job-if", "text"},
		{"compile/context-steps-in-job-if", "json"},
		{"compile/conditions", "text"},
		{"compile/conditions", "json"},
		{"compile/variable-looks-secret", "text"},
		{"compile/needs-any", "text"},
		{"compile/needs-any", "json"},
		{"inputs/number-invalid", "text"},
		{"inputs/number-invalid", "json"},
		{"format/yaml-alias", "text"},
	} {
		t.Run(tc.name+"/"+tc.format, func(t *testing.T) {
			c := conformanceCase(t, tc.name)
			env, inputs := caseEnv(t, c)
			args := []string{"validate", "--format", tc.format}
			if inputs != "" {
				args = append(args, "--inputs", inputs)
			}
			workflow := filepath.ToSlash(filepath.Join("..", "..", "conformance", tc.name, "workflow.yaml"))
			args = append(args, workflow)
			r := runCLI(t, env, "", args...)
			if r.stderr != "" {
				t.Fatalf("meridian validate wrote to standard error: %s", r.stderr)
			}
			shown := "--format " + tc.format
			if inputs != "" {
				shown += " --inputs inputs.json"
			}
			got := fmt.Sprintf("$ meridian validate %s %s\n%sexit status %d\n", shown, workflow, r.stdout, r.code)
			golden := filepath.Join("testdata", "validate", strings.ReplaceAll(tc.name, "/", "-")+"."+tc.format)
			if *update {
				if err := os.MkdirAll(filepath.Dir(golden), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(golden, []byte(got), 0o644); err != nil { //nolint:gosec // a golden file is the repository's
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatalf("%v; go test -update writes it", err)
			}
			if got != string(want) {
				t.Errorf("meridian validate wrote\n%s\nwant, as %s has it,\n%s", got, golden, want)
			}

			// What it says agrees with the case.
			hasErrors := false
			for _, d := range c.Expect.Diagnostics {
				hasErrors = hasErrors || d.Severity == "error"
				if !strings.Contains(r.stdout, d.Code) {
					t.Errorf("meridian validate doesn't report %s", d.Code)
				}
			}
			if wantCode := map[bool]int{false: exitOK, true: exitInvalid}[hasErrors]; r.code != wantCode {
				t.Errorf("exit status %d, want %d", r.code, wantCode)
			}
			if tc.format == "json" && !jsontext.Value(r.stdout).IsValid() {
				t.Errorf("meridian validate --format json wrote what isn't JSON: %s", r.stdout)
			}
		})
	}
}

// emptyEnv is an environment with an empty config directory.
func emptyEnv(t *testing.T) map[string]string {
	t.Helper()
	dir := t.TempDir()
	return map[string]string{"HOME": dir, local.EnvConfigDir: filepath.Join(dir, "config")}
}

func writeFile(t *testing.T, name, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const groupInputs = `on:
  execute:
    inputs:
      cluster:
        type: group
        items:
          size:
            type: number
            max: 4
          name:
            type: string
      count:
        type: number
jobs:
  main:
    steps:
      - run: echo ${{ inputs.cluster.size }} ${{ inputs.count }}
`

func TestValidateInputs(t *testing.T) {
	parallel(t)
	workflow := writeFile(t, "w.yaml", groupInputs)
	inputs := writeFile(t, "inputs.json", `{"cluster": {"size": 2, "name": "a"}, "count": 9007199254740993}`)
	for _, tc := range []struct {
		args []string
		code int
		want string
	}{
		{args: []string{"--inputs", inputs}, code: exitOK},
		{args: []string{"--input", "cluster.size=5"}, code: exitInvalid, want: "input_above_max"},
		// -input sets values over the file's.
		{args: []string{"--inputs", inputs, "--input", "cluster.size=8"}, code: exitInvalid, want: "input_above_max"},
		// The file's values are read as it writes them, text for a number
		// among them, which passes the check and is read as text.
		{args: []string{"--inputs", writeFile(t, "text.json", `{"count": "many"}`)}, code: exitInvalid, want: "input_not_number"},
		{args: []string{"--inputs", writeFile(t, "three.json", `{"cluster": {"size": 2, "name": "a"}, "count": "3"}`)}, code: exitOK, want: "input_read_as_submitted"},
		{args: []string{"--input", "cluster.size=3", "--input", "cluster.name=a", "--input", "count=1"}, code: exitOK},
	} {
		r := runCLI(t, emptyEnv(t), "", append(append([]string{"validate"}, tc.args...), workflow)...)
		if r.code != tc.code || !strings.Contains(r.stdout, tc.want) || r.stderr != "" {
			t.Errorf("meridian validate %q = %+v, want exit status %d and %s", tc.args, r, tc.code, tc.want)
		}
	}
}

func TestValidateInputsRefuses(t *testing.T) {
	parallel(t)
	workflow := writeFile(t, "w.yaml", groupInputs)
	for name, args := range map[string][]string{
		"no =":           {"--input", "count"},
		"empty name":     {"--input", "cluster..size=1"},
		"value then key": {"--input", "u=1", "--input", "u.size=2"},
		"not a number":   {"--input", "count=many"},
		"through a leaf": {"--input", "count.size=2"},
		"missing file":   {"--inputs", filepath.Join(t.TempDir(), "none.json")},
		"list":           {"--inputs", writeFile(t, "list.json", `[1]`)},
		"two values":     {"--inputs", writeFile(t, "two.json", `{} {}`)},
		"repeated key":   {"--inputs", writeFile(t, "repeated.json", `{"count": 1, "count": 2}`)},
		"not json":       {"--inputs", writeFile(t, "bad.json", `{count: 1}`)},
		"huge number":    {"--inputs", writeFile(t, "huge.json", `{"count": 1e999}`)},
	} {
		r := runCLI(t, emptyEnv(t), "", append(append([]string{"validate"}, args...), workflow)...)
		if r.code != exitError || r.stdout != "" || r.stderr == "" {
			t.Errorf("meridian validate with %s = %+v", name, r)
		}
	}
}

// typedInputs are inputs of each kind -input converts by.
const typedInputs = `on:
  execute:
    inputs:
      debug: {type: boolean}
      nodes: {type: number}
      cores: {type: dropdown, options: [2, 4]}
      choice: {type: dropdown, options: [true, "no"]}
      grouped: {type: dropdown, options: [{category: a, options: [{label: x, value: 8}]}]}
      region: {type: string}
      name: {type: string}
      pass: {type: password}
      files: {type: list, template: {path: {type: string}}}
      md: {type: multi-dropdown, options: [a, b]}
      res: {type: group, items: {count: {type: number}}}
      db: {type: group, flatten: true, items: {port: {type: number}}}
      head: {type: header, text: H}
      wizard: {type: step, title: W, options: {s: {type: number}}}
      rep: {type: step, title: R, multi: true, options: {r: {type: string}}}
      ct: {type: compute-target}
      cc: {type: compute-clusters, multi: true}
jobs:
  main:
    steps:
      - run: echo
`

// TestSetInputs checks that each -input is converted by its input's type,
// as ACTIVATE's pw workflows run --input converts one, and that what it
// refuses names the path, never the value.
func TestSetInputs(t *testing.T) {
	parallel(t)
	defs := inputDefinitions("w.yaml", []byte(typedInputs))
	if defs == nil {
		t.Fatal("the workflow doesn't load")
	}
	// A dropdown whose options an option-key picks, which the schema has
	// no form for, as ACTIVATE's run form reads one.
	defs["keyed"] = map[string]any{"type": "dropdown", "option-key": "${{ inputs.region }}", "options": map[string]any{
		"us": []any{int64(1), int64(2)}, "eu": []any{"3"},
	}}
	for _, tc := range []struct {
		sets []string
		want map[string]any
		code string
	}{
		{sets: []string{"debug=true", "nodes=8", "name=true", "pass=42"}, want: map[string]any{"debug": true, "nodes": int64(8), "name": "true", "pass": "42"}},
		{sets: []string{"debug=false", "nodes=-2.5"}, want: map[string]any{"debug": false, "nodes": -2.5}},
		{sets: []string{"nodes=1e3"}, want: map[string]any{"nodes": 1000.0}},
		{sets: []string{"debug=True"}, code: "input_flag_wrong_type"},
		{sets: []string{"debug=1"}, code: "input_flag_wrong_type"},
		{sets: []string{"nodes=007"}, code: "input_flag_wrong_type"},
		{sets: []string{"nodes=0x10"}, code: "input_flag_wrong_type"},
		{sets: []string{"nodes=+1"}, code: "input_flag_wrong_type"},
		{sets: []string{"nodes=.5"}, code: "input_flag_wrong_type"},
		{sets: []string{"nodes=1."}, code: "input_flag_wrong_type"},
		{sets: []string{"nodes=NaN"}, code: "input_flag_wrong_type"},
		{sets: []string{"nodes=1e400"}, code: "input_flag_wrong_type"},
		// A dropdown's text, unless an option is the number or the boolean
		// it reads as, grouped options among them.
		{sets: []string{"cores=4", "choice=true", "grouped=8"}, want: map[string]any{"cores": int64(4), "choice": true, "grouped": int64(8)}},
		{sets: []string{"cores=3", "choice=no", "grouped=08"}, want: map[string]any{"cores": "3", "choice": "no", "grouped": "08"}},
		// An option-key picks the list by the value sent, before or after,
		// and every list when none is.
		{sets: []string{"keyed=3", "region=eu"}, want: map[string]any{"keyed": "3", "region": "eu"}},
		{sets: []string{"region=us", "keyed=1"}, want: map[string]any{"keyed": int64(1), "region": "us"}},
		{sets: []string{"keyed=1"}, want: map[string]any{"keyed": int64(1)}},
		// A later -input that sets it wins, and reads the list the key's
		// value picks then.
		{sets: []string{"keyed=1", "region=eu", "keyed=2"}, want: map[string]any{"keyed": "2", "region": "eu"}},
		// Structures are JSON of their shape.
		{sets: []string{`files=[{"path":"a.txt"}]`, `md=["a"]`, `res={"count":1}`, `ct={"id":"c"}`, `cc=["c"]`, `rep=[{"r":"x"}]`}, want: map[string]any{
			"files": []any{map[string]any{"path": "a.txt"}}, "md": []any{"a"}, "res": map[string]any{"count": int64(1)},
			"ct": map[string]any{"id": "c"}, "cc": []any{"c"}, "rep": []any{map[string]any{"r": "x"}},
		}},
		{sets: []string{"files=a.txt"}, code: "input_flag_not_json"},
		{sets: []string{"files="}, code: "input_flag_not_json"},
		{sets: []string{"files=[] []"}, code: "input_flag_not_json"},
		{sets: []string{"files={}"}, code: "input_flag_wrong_shape"},
		{sets: []string{"res=[]"}, code: "input_flag_wrong_shape"},
		// A key given twice has its last value, as ACTIVATE's reads it.
		{sets: []string{`res={"count":1,"count":2}`}, want: map[string]any{"res": map[string]any{"count": int64(2)}}},
		{sets: []string{"head=x"}, code: "input_flag_wrong_type"},
		// Paths: a group's inputs by the group's name, a flattened group's
		// and a wizard step's by their own, a list set whole.
		{sets: []string{"res.count=2", "s=5", "port=22"}, want: map[string]any{"res": map[string]any{"count": int64(2)}, "s": int64(5), "port": int64(22)}},
		{sets: []string{"wizard.s=1"}, code: "input_flag_path_not_settable"},
		{sets: []string{"db.port=1"}, code: "input_flag_path_not_settable"},
		{sets: []string{`db={"port":1}`}, code: "input_flag_path_not_settable"},
		{sets: []string{"files.path=a"}, code: "input_flag_path_not_settable"},
		{sets: []string{"rep.r=a"}, code: "input_flag_path_not_settable"},
		{sets: []string{"name.x=1"}, code: "input_flag_path_not_settable"},
		{sets: []string{"u=5", "v.w=6"}, want: map[string]any{"u": "5", "v": map[string]any{"w": "6"}}},
		{sets: []string{"u=1", "u.v=2"}, code: "input_path_conflict"},
		{sets: []string{"debug"}, code: "input_flag_unsplit"},
		{sets: []string{"a..b=1"}, code: "input_path_invalid"},
	} {
		values := map[string]any{}
		sets, err := parseInputSets(tc.sets)
		if err == nil {
			err = setInputs(values, defs, sets)
		}
		if tc.code != "" {
			p, ok := errors.AsType[*problem.Problem](err)
			if !ok || string(p.Code) != tc.code {
				t.Errorf("%q: %v, want %s", tc.sets, err, tc.code)
				continue
			}
			// The problem names the path, never the value.
			for _, s := range tc.sets {
				if _, value, ok := strings.Cut(s, "="); ok && len(value) > 2 && strings.Contains(fmt.Sprint(p.Params), value) {
					t.Errorf("%q: the problem shows the value: %v", tc.sets, p.Params)
				}
			}
			continue
		}
		if err != nil || !reflect.DeepEqual(values, tc.want) {
			t.Errorf("%q: %v, %v, want %v", tc.sets, values, err, tc.want)
		}
	}
}

func TestValidateStdin(t *testing.T) {
	parallel(t)
	r := runCLI(t, emptyEnv(t), "jobs:\n  a:\n    if: ${{ steps.x }}\n    steps:\n      - run: echo\n", "validate", "-")
	if r.code != exitInvalid || !strings.HasPrefix(r.stdout, "<stdin>:3:13: error: ") {
		t.Errorf("meridian validate - = %+v", r)
	}
}

// TestValidateTooLarge checks that a file larger than the compiler reads
// is reported as the compiler would report it, without being kept.
func TestValidateTooLarge(t *testing.T) {
	parallel(t)
	text := "# " + strings.Repeat("x", format.DefaultMaxSize) + "\n"
	path := writeFile(t, "big.yaml", text)
	r := runCLI(t, emptyEnv(t), "", "validate", path)
	_, diags := compile.Compile(path, []byte(text), nil, compile.Options{})
	if want := diags.Error() + "\n"; r.code != exitInvalid || r.stdout != want {
		t.Errorf("meridian validate of a large file = %+v, want %q", r, want)
	}
	// Standard input's size isn't known, and isn't read past the limit.
	r = runCLI(t, emptyEnv(t), text, "validate", "-")
	if want := "<stdin>: error: The workflow is more than 102,400 bytes, the limit on its size. (workflow_too_large)\n"; r.code != exitInvalid || r.stdout != want {
		t.Errorf("meridian validate of a large standard input = %+v, want %q", r, want)
	}
}

func TestValidateRefuses(t *testing.T) {
	parallel(t)
	workflow := writeFile(t, "w.yaml", "jobs:\n  a:\n    steps:\n      - run: echo\n")
	badConfig := emptyEnv(t)
	if err := os.MkdirAll(badConfig[local.EnvConfigDir], 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badConfig[local.EnvConfigDir], local.VariablesFile), []byte("org: {a: {secret: true}}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		env  map[string]string
		args []string
	}{
		"no workflow":        {emptyEnv(t), []string{"validate"}},
		"two workflows":      {emptyEnv(t), []string{"validate", workflow, workflow}},
		"format":             {emptyEnv(t), []string{"validate", "--format", "xml", workflow}},
		"unknown flag":       {emptyEnv(t), []string{"validate", "--lang", "ja", workflow}},
		"missing workflow":   {emptyEnv(t), []string{"validate", filepath.Join(t.TempDir(), "none.yaml")}},
		"bad variables file": {badConfig, []string{"validate", workflow}},
		"no home":            {map[string]string{}, []string{"validate", workflow}},
	} {
		r := runCLI(t, tc.env, "", tc.args...)
		if r.code != exitError || r.stdout != "" || r.stderr == "" {
			t.Errorf("meridian %s (%s) = %+v", strings.Join(tc.args, " "), name, r)
		}
	}
}

// TestValidateGroups checks that an execution-groups input chooses among
// the config's groups.
// TestValidateStart checks that validate with -start starts the run too,
// with the user's variables, secret ones included, and reports what would
// fail it as it starts, where the workflow writes it, without a variable's
// value; and that a run with sessions can't start here, where nothing
// provides them.
func TestValidateStart(t *testing.T) {
	parallel(t)
	env := emptyEnv(t)
	config := env[local.EnvConfigDir]
	if err := os.MkdirAll(config, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config, local.VariablesFile), []byte("vars:\n  N: {secret: true, env: SECRET_N}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env["SECRET_N"] = "many"
	workflow := writeFile(t, "w.yaml", `on:
  execute:
    inputs:
      n: {type: number, default: '${{ vars.N }}'}
jobs:
  main:
    steps:
      - run: echo
`)
	if r := runCLI(t, env, "", "validate", workflow); r.code != exitOK || r.stdout != "" {
		t.Errorf("meridian validate = %+v", r)
	}
	r := runCLI(t, env, "", "validate", "--start", workflow)
	if r.code != exitInvalid || !strings.Contains(r.stdout, "w.yaml:4:7: error:") || !strings.Contains(r.stdout, "(input_not_number)") ||
		!strings.Contains(r.stdout, "***") || strings.Contains(r.stdout, "many") || r.stderr != "" {
		t.Errorf("meridian validate --start = %+v", r)
	}
	env["SECRET_N"] = "3"
	if r := runCLI(t, env, "", "validate", "--start", workflow); r.code != exitOK || r.stdout != "" {
		t.Errorf("meridian validate --start with a number = %+v", r)
	}
	delete(env, "SECRET_N")
	if r := runCLI(t, env, "", "validate", "--start", workflow); r.code != exitError || !strings.Contains(r.stderr, "variable_env_unset") {
		t.Errorf("meridian validate --start without the secret = %+v", r)
	}
	sessions := writeFile(t, "s.yaml", "sessions:\n  desktop:\n    useTLS: false\njobs:\n  main:\n    steps:\n      - run: echo\n")
	// Sessions this machine serves are checked and named as the run is
	// created, and made by none as validate --start starts it.
	if r := runCLI(t, env, "", "validate", sessions); r.code != exitOK || r.stdout != "" {
		t.Errorf("meridian validate with sessions = %+v", r)
	}
	if r := runCLI(t, env, "", "validate", "--start", sessions); r.code != exitOK || r.stdout != "" {
		t.Errorf("meridian validate --start with sessions = %+v", r)
	}
	// One meridian run can't serve, without --start too.
	detached := writeFile(t, "d.yaml", "sessions:\n  desktop:\n    detach: true\njobs:\n  main:\n    steps:\n      - run: echo\n")
	if r := runCLI(t, env, "", "validate", detached); r.code != exitInvalid || !strings.Contains(r.stdout, "(session_field_unsupported)") {
		t.Errorf("meridian validate with a detached session = %+v", r)
	}
}

// TestValidateChecksConfig checks that validate creates the run as it would
// run: with the execution groups in the config, the local group among
// them, and the workflows uses: names, which it fetches and pins.
func TestValidateChecksConfig(t *testing.T) {
	// And for a GitLab server's that sources.yaml registers, and one's
	// that it doesn't, which Git is never asked for.
	flowsRepo(t, map[string]string{"flows/run.yaml": "jobs:\n  x:\n    steps:\n      - run: echo\n"}, "git.example.com", "other.example.com")

	env := emptyEnv(t)
	env[local.EnvCacheDir] = filepath.Join(t.TempDir(), "cache")
	config := env[local.EnvConfigDir]
	if err := os.MkdirAll(config, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config, local.GroupsFile), []byte("gpu:\n  members:\n    - {name: box, backend: ssh}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config, local.SourcesFile), []byte("gitlab-servers: [git.example.com]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		group, uses, host string
		code              int
		want              string
	}{
		{"gpu", "github/acme/flows@main", "", exitOK, ""},
		{"local", "github/acme/flows@main", "", exitOK, ""},
		{"cpu", "github/acme/flows@main", "", exitInvalid, "w.yaml:3:32: error: There is no execution group \"cpu\" here. (group_unknown)"},
		{"gpu", "github/acme/none@main", "", exitInvalid, "(source_revision_not_found)"},
		{"gpu", "github/acme/flows@gone", "", exitInvalid, "(source_revision_not_found)"},
		{"gpu", "gitlab/acme/flows@main", "git.example.com", exitOK, ""},
		{"gpu", "gitlab/acme/flows@main", "other.example.com", exitInvalid, "(source_server_unknown)"},
	} {
		with := "$yaml: flows/run.yaml"
		if tc.host != "" {
			with += ", $host: " + tc.host
		}
		workflow := writeFile(t, "w.yaml", fmt.Sprintf(`jobs:
  main:
    runs-on: {execution-group: %s}
    steps:
      - uses: %s
        with: {%s}
`, tc.group, tc.uses, with))
		r := runCLI(t, env, "", "validate", workflow)
		if r.code != tc.code || !strings.Contains(r.stdout, tc.want) || r.stderr != "" {
			t.Errorf("meridian validate with %s, %s and %q = %+v, want exit status %d and %s", tc.group, tc.uses, tc.host, r, tc.code, tc.want)
		}
	}
}

// TestRunMappedWorkflow checks that meridian runs a step's call of a
// workflow a platform stores that sources.yaml maps to a workflow in Git:
// validate pins it, run runs it as a run of its own, under format 1's name
// for it, its slug, and status shows that run's workflow by both names;
// and that a workflow it doesn't map is refused as the run is created.
func TestRunMappedWorkflow(t *testing.T) {
	flowsRepo(t, map[string]string{"flows/tools.yaml": "jobs:\n  greet:\n    steps:\n      - run: echo \"hello from $PW_WORKFLOW_NAME\"\n"})
	env := runEnv(t)
	config := env[local.EnvConfigDir]
	if err := os.MkdirAll(config, 0o700); err != nil {
		t.Fatal(err)
	}
	mapping := "workflows:\n  marketplace/tools/v1.2: {uses: github/acme/flows@main, yaml: flows/tools.yaml}\n"
	if err := os.WriteFile(filepath.Join(config, local.SourcesFile), []byte(mapping), 0o600); err != nil {
		t.Fatal(err)
	}
	workflow := writeFile(t, "w.yaml", "jobs:\n  main:\n    steps:\n      - uses: marketplace/tools@v1.2\n")
	if r := runCLI(t, env, "", "validate", workflow); r.code != exitOK || r.stdout != "" {
		t.Errorf("meridian validate of a mapped call = %+v", r)
	}
	if r := runTest(t, env, nil, workflow); r.code != exitOK || !strings.Contains(r.stdout, "| hello from tools\n") {
		t.Errorf("meridian run of a mapped call = %+v", r)
	}
	if r := commandTest(t, env, "status", "2"); r.code != exitOK ||
		!strings.Contains(r.stdout, "workflow: marketplace/tools/v1.2 → github.com/acme/flows@main:flows/tools.yaml\n") {
		t.Errorf("meridian status of the mapped call's run = %+v", r)
	}
	unmapped := writeFile(t, "u.yaml", "jobs:\n  main:\n    steps:\n      - uses: marketplace/other/v1\n")
	if r := runCLI(t, env, "", "validate", unmapped); r.code != exitInvalid || !strings.Contains(r.stdout, "u.yaml:4:15: error:") || !strings.Contains(r.stdout, "(source_on_platform)") {
		t.Errorf("meridian validate of a call that isn't mapped = %+v", r)
	}
}

// TestStatusEscapesLocation checks that meridian status shows a run's
// workflow with what a terminal would act on, or show otherwise, escaped,
// in the list of runs, a run's own, and the runs its steps called: here a
// workflow file whose name holds an escape, and a mapped workflow's file
// whose name holds a bidirectional override, which a $yaml can.
func TestStatusEscapesLocation(t *testing.T) {
	flowsRepo(t, map[string]string{"flows/t\u202etools.yaml": "jobs:\n  greet:\n    steps:\n      - run: echo hello\n"})
	env := runEnv(t)
	config := env[local.EnvConfigDir]
	if err := os.MkdirAll(config, 0o700); err != nil {
		t.Fatal(err)
	}
	mapping := "workflows:\n  workflow/tools: {uses: github/acme/flows@main, yaml: \"flows/t\\u202etools.yaml\"}\n"
	if err := os.WriteFile(filepath.Join(config, local.SourcesFile), []byte(mapping), 0o600); err != nil {
		t.Fatal(err)
	}
	workflow := writeFile(t, "w\x1b[31m.yaml", "jobs:\n  main:\n    steps:\n      - uses: workflow/tools\n")
	if r := runTest(t, env, nil, workflow); r.code != exitOK {
		t.Fatalf("meridian run = %+v", r)
	}
	for _, args := range [][]string{{"status", "-all"}, {"status", "1"}, {"status", "2"}} {
		r := commandTest(t, env, args...)
		if r.code != exitOK || strings.ContainsAny(r.stdout, "\x1b\u202e") {
			t.Errorf("meridian %s = %+v, which holds what a terminal acts on", strings.Join(args, " "), r)
		}
	}
	if r := commandTest(t, env, "status", "1"); !strings.Contains(r.stdout, "workflow: w\\x1b[31m.yaml\n") ||
		!strings.Contains(r.stdout, "workflow/tools → github.com/acme/flows@main:flows/t\\u202etools.yaml\n") {
		t.Errorf("meridian status 1 shows the workflows otherwise than escaped: %+v", r)
	}
}

// flowsRepo makes a Git repository of files, by their paths in it, a
// script among them executable, on its branch main, which Git reaches for github.com's acme/flows, and for each
// of hosts' as acme/flows there: a workflow that uses: names, which
// meridian fetches and pins. The user's own configuration may allow the
// file transport, as this one does, which Meridian allows only https and
// ssh of otherwise. It sets the process's environment, so the test can't
// be parallel.
func flowsRepo(t *testing.T, files map[string]string, hosts ...string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git isn't installed")
	}
	origins := t.TempDir()
	repo := filepath.Join(origins, "acme", "flows.git")
	if err := os.MkdirAll(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=T", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=T", "GIT_COMMITTER_EMAIL=t@example.com")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	var config strings.Builder
	config.WriteString("[url \"file://" + filepath.ToSlash(origins) + "/\"]\n\tinsteadOf = https://github.com/\n")
	for _, host := range hosts {
		config.WriteString("\tinsteadOf = https://" + host + "/\n")
	}
	config.WriteString("[protocol \"file\"]\n\tallow = always\n")
	t.Setenv("GIT_CONFIG_GLOBAL", writeFile(t, "gitconfig", config.String()))
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	git("init", "--quiet", "--initial-branch=main")
	for name, text := range files {
		path := filepath.Join(repo, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		// A script, which starts #!, is executable, as Git keeps it.
		mode := os.FileMode(0o600)
		if strings.HasPrefix(text, "#!") {
			mode = 0o700
		}
		if err := os.WriteFile(path, []byte(text), mode); err != nil {
			t.Fatal(err)
		}
	}
	git("add", ".")
	git("commit", "--quiet", "-m", "flows")
}

func TestValidateGroups(t *testing.T) {
	parallel(t)
	env := emptyEnv(t)
	config := env[local.EnvConfigDir]
	if err := os.MkdirAll(config, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config, local.GroupsFile), []byte("gpu:\n  members:\n    - {name: box, backend: ssh}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	workflow := writeFile(t, "w.yaml", `on:
  execute:
    inputs:
      group:
        type: execution-groups
jobs:
  main:
    runs-on: {execution-group: "${{ inputs.group }}"}
    steps:
      - run: echo
`)
	for group, code := range map[string]int{"gpu": exitOK, "local": exitOK, "cpu": exitInvalid} {
		r := runCLI(t, env, "", "validate", "--input", "group="+group, workflow)
		if r.code != code {
			t.Errorf("meridian validate with group %s = %+v", group, r)
		}
	}
}

// TestValidateClusters checks that a compute-clusters input names one of
// the clusters of the config's clusters.yaml, and so does a
// compute-resources one, as format 1 gives a cluster either names the same
// details; and that without the file a name is refused, saying there are
// none.
func TestValidateClusters(t *testing.T) {
	parallel(t)
	for _, typ := range []string{"compute-clusters", "compute-resources"} {
		env := emptyEnv(t)
		workflow := writeFile(t, typ+".yaml", `on:
  execute:
    inputs:
      cluster:
        type: `+typ+`
jobs:
  main:
    ssh:
      remoteHost: ${{ inputs.cluster.ip }}
    steps:
      - run: echo
`)
		if r := runCLI(t, env, "", "validate", "--start", "--input", "cluster=hpc", workflow); r.code != exitInvalid || !strings.Contains(r.stdout, "(clusters_none)") {
			t.Errorf("%s: meridian validate without clusters.yaml = %+v", typ, r)
		}
		config := env[local.EnvConfigDir]
		if err := os.MkdirAll(config, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(config, local.ClustersFile), []byte("hpc:\n  host: hpc-login\n  scheduler: slurm\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if r := runCLI(t, env, "", "validate", "--start", "--input", "cluster=hpc", workflow); r.code != exitOK || r.stdout != "" {
			t.Errorf("%s: meridian validate with cluster hpc = %+v", typ, r)
		}
		r := runCLI(t, env, "", "validate", "--start", "--input", "cluster=cpu", workflow)
		if r.code != exitInvalid || !strings.Contains(r.stdout, "(cluster_unknown)") || !strings.Contains(r.stdout, `"hpc"`) {
			t.Errorf("%s: meridian validate with cluster cpu = %+v", typ, r)
		}
	}
}

// TestValidatePlatformNames checks that the inputs whose value is a name,
// a scheduler's, a Kubernetes cluster's or an organization group's, take
// the name given, as format 1 does: that an account and a QoS a workflow
// requires for an existing cluster, as one of clusters.yaml is, are
// required here too; that, given, the run starts; and that a value that
// isn't text is refused.
func TestValidatePlatformNames(t *testing.T) {
	parallel(t)
	env := emptyEnv(t)
	config := env[local.EnvConfigDir]
	if err := os.MkdirAll(config, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config, local.ClustersFile), []byte("hpc:\n  host: hpc-login\n  scheduler: slurm\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	workflow := writeFile(t, "w.yaml", `on:
  execute:
    inputs:
      cluster:
        type: compute-clusters
      slurm:
        type: group
        items:
          account:
            type: slurm-accounts
            resource: ${{ inputs.cluster }}
            ignore: ${{ 'existing' != inputs.cluster.provider }}
          qos:
            type: slurm-qos
            resource: ${{ inputs.cluster }}
            ignore: ${{ 'existing' != inputs.cluster.provider }}
          partition:
            type: slurm-partitions
            resource: ${{ inputs.cluster }}
            optional: true
      queue:
        type: pbs-queues
        resource: ${{ inputs.cluster }}
        optional: true
      k8s:
        type: group
        items:
          cluster:
            type: kubernetes-clusters
          namespace:
            type: kubernetes-namespaces
            clusterName: ${{ inputs.k8s.cluster }}
          pvc:
            type: kubernetes-pvc
            clusterName: ${{ inputs.k8s.cluster }}
            namespace: ${{ inputs.k8s.namespace }}
            optional: true
      allocation:
        type: organization-groups
jobs:
  main:
    ssh:
      remoteHost: ${{ inputs.cluster.ip }}
    steps:
      - run: echo "#SBATCH --account=${{ inputs.slurm.account }} --qos=${{ inputs.slurm.qos }} --partition=${{ inputs.slurm.partition }}" ${{ inputs.k8s.namespace }} ${{ inputs.allocation }}
`)
	named := []string{"--input", "cluster=hpc", "--input", "k8s.cluster=lab", "--input", "k8s.namespace=team", "--input", "allocation=research"}
	args := append([]string{"validate", "--start"}, named...)
	r := runCLI(t, env, "", append(args, workflow)...)
	if r.code != exitInvalid || strings.Count(r.stdout, "(input_required)") != 2 || !strings.Contains(r.stdout, "slurm.account") || !strings.Contains(r.stdout, "slurm.qos") {
		t.Errorf("meridian validate without the account and the QoS an existing cluster requires = %+v", r)
	}
	args = append(args, "--input", "slurm.account=physics", "--input", "slurm.qos=normal", "--input", "slurm.partition=debug", "--input", "queue=workq", "--input", "k8s.pvc=data")
	if r := runCLI(t, env, "", append(args, workflow)...); r.code != exitOK || r.stdout != "" {
		t.Errorf("meridian validate with every name given = %+v", r)
	}
	inputs := writeFile(t, "inputs.json", `{"cluster": "hpc", "slurm": {"account": 7, "qos": "normal"}, "k8s": {"cluster": "lab", "namespace": "team"}, "allocation": "research"}`)
	r = runCLI(t, env, "", "validate", "--start", "--inputs", inputs, workflow)
	if r.code != exitInvalid || !strings.Contains(r.stdout, "(input_not_text)") || !strings.Contains(r.stdout, "slurm.account") {
		t.Errorf("meridian validate with an account that isn't text = %+v", r)
	}
}

// TestValidateBucket checks that a bucket input takes a bucket's
// pw://user/name URI, whose uri a workflow reads, and refuses a name
// alone, which a run here has no platform user to read as theirs, and an
// input that asks for the bucket's credentials.
func TestValidateBucket(t *testing.T) {
	parallel(t)
	env := emptyEnv(t)
	workflow := writeFile(t, "w.yaml", `on:
  execute:
    inputs:
      data:
        type: bucket
jobs:
  main:
    steps:
      - run: pw buckets cp "${{ inputs.data.uri }}/input.tgz" .
`)
	if r := runCLI(t, env, "", "validate", "--start", "--input", "data=pw://alice/data", workflow); r.code != exitOK || r.stdout != "" {
		t.Errorf("meridian validate with a bucket's URI = %+v", r)
	}
	r := runCLI(t, env, "", "validate", "--start", "--input", "data=data", workflow)
	if r.code != exitInvalid || !strings.Contains(r.stdout, "(bucket_uri_invalid)") || !strings.Contains(r.stdout, "pw://user/bucket") {
		t.Errorf("meridian validate with a bucket's name alone = %+v", r)
	}
	credentials := writeFile(t, "c.yaml", `on:
  execute:
    inputs:
      data:
        type: bucket
        generateCredentials: true
jobs:
  main:
    steps:
      - run: echo ${{ inputs.data.uri }}
`)
	r = runCLI(t, env, "", "validate", "--start", "--input", "data=pw://alice/data", credentials)
	if r.code != exitInvalid || !strings.Contains(r.stdout, "(bucket_credentials_need_platform)") {
		t.Errorf("meridian validate of a bucket with generateCredentials = %+v", r)
	}
}

// TestValidateDevZero checks that a stream that never ends is read only to
// the limit.
func TestValidateDevZero(t *testing.T) {
	parallel(t)
	if _, err := os.Stat("/dev/zero"); err != nil {
		t.Skip("there is no /dev/zero")
	}
	r := runCLI(t, emptyEnv(t), "", "validate", "/dev/zero")
	if r.code != exitInvalid || !strings.Contains(r.stdout, "(workflow_too_large)") {
		t.Errorf("meridian validate /dev/zero = %+v", r)
	}
}

// TestValidateInterrupted checks that an interrupt, which cancels the
// command's context, stops it waiting on a pipe that never ends.
func TestValidateInterrupted(t *testing.T) {
	parallel(t)
	stdin, w := io.Pipe()
	defer w.Close()
	ctx, cancel := context.WithCancel(t.Context())
	var stdout, stderr strings.Builder
	env := emptyEnv(t)
	c := &cli{stdin: stdin, stdout: &stdout, stderr: &stderr, lookupEnv: func(name string) (string, bool) { v, ok := env[name]; return v, ok }}
	done := make(chan int, 1)
	go func() { done <- run(ctx, c, []string{"validate", "-"}) }()
	cancel()
	select {
	case code := <-done:
		if code != exitError || stderr.String() != "meridian: error: Meridian was interrupted. (interrupted)\n" {
			t.Errorf("an interrupted meridian validate: %d, %q", code, stderr.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("meridian validate kept waiting on its standard input after an interrupt")
	}
}

// TestValidateControlCharacters checks that a key's control characters
// reach the terminal escaped: they can't clear it, or start a line that
// reads as another diagnostic.
func TestValidateControlCharacters(t *testing.T) {
	parallel(t)
	workflow := writeFile(t, "w.yaml", "jobs:\n  a:\n    steps:\n      - run: echo\n        \"x\\e[2J\\nsrc/main.go:1:1: error: injected\": 1\n")
	r := runCLI(t, emptyEnv(t), "", "validate", workflow)
	if strings.ContainsAny(r.stdout, "\x1b\a") || strings.Count(r.stdout, "\n") != 1 || !strings.Contains(r.stdout, `x\x1b[2J\nsrc/main.go:1:1: error: injected`) {
		t.Errorf("meridian validate wrote %q", r.stdout)
	}
}

// TestValidateProblems checks the problems validate reports besides a
// workflow's: each a kind, at the file when one is to blame.
func TestValidateProblems(t *testing.T) {
	parallel(t)
	workflow := writeFile(t, "w.yaml", groupInputs)
	inputs := writeFile(t, "bad.json", `{count: 1}`)
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"validate", filepath.Join(t.TempDir(), "none.yaml")}, "(file_unreadable)"},
		{[]string{"validate", "--inputs", inputs, workflow}, inputs + ": error: The file isn't valid JSON: "},
		{[]string{"validate", "--inputs", writeFile(t, "l.json", "[1]"), workflow}, "(inputs_file_not_object)"},
		{[]string{"validate", "--inputs", writeFile(t, "two.json", "{} {}"), workflow}, "(inputs_file_extra)"},
		{[]string{"validate", "--inputs", writeFile(t, "huge.json", `{"count": 1e999}`), workflow}, "(inputs_number_too_large)"},
		{[]string{"validate", "--input", "cluster..size=1", workflow}, "(input_path_invalid)"},
		{[]string{"validate", "--input", "u=1", "--input", "u.size=2", workflow}, "(input_path_conflict)"},
		{[]string{"validate", "--input", "count.size=2", workflow}, "(input_flag_path_not_settable)"},
		{[]string{"validate", "--input", "count=many", workflow}, "(input_flag_wrong_type)"},
		{[]string{"validate", "--input", "count", workflow}, "(input_flag_unsplit)"},
	} {
		r := runCLI(t, emptyEnv(t), "", tc.args...)
		if r.code != exitError || !strings.Contains(r.stderr, tc.want) {
			t.Errorf("meridian %q = %+v, want %s", tc.args, r, tc.want)
		}
	}
}

// TestValidateProblemsAsJSON checks that with --format json the command's
// own problems are RFC 9457 problem details too, on standard error.
func TestValidateProblemsAsJSON(t *testing.T) {
	parallel(t)
	r := runCLI(t, emptyEnv(t), "", "validate", "--format", "json", filepath.Join(t.TempDir(), "none.yaml"))
	if r.code != exitError || r.stdout != "" {
		t.Fatalf("meridian validate --format json of a missing file = %+v", r)
	}
	var p problem.Problem
	if err := jsonv2.Unmarshal([]byte(r.stderr), &p); err != nil {
		t.Fatalf("standard error isn't a problem: %v: %q", err, r.stderr)
	}
	if p.Code != problems.FileUnreadable.Code() || p.Type != "/problems/command/file_unreadable" || p.Detail == "" {
		t.Errorf("the problem is %+v", p)
	}
}

// TestValidateStdinRegularFile checks that standard input that is a regular
// file larger than the limit is reported with its size, unread.
func TestValidateStdinRegularFile(t *testing.T) {
	parallel(t)
	text := "# " + strings.Repeat("x", format.DefaultMaxSize) + "\n"
	f, err := os.Open(writeFile(t, "big.yaml", text))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	env := emptyEnv(t)
	var stdout, stderr strings.Builder
	c := &cli{stdin: f, stdout: &stdout, stderr: &stderr, lookupEnv: func(name string) (string, bool) { v, ok := env[name]; return v, ok }}
	code := run(t.Context(), c, []string{"validate", "-"})
	_, diags := compile.Compile("<stdin>", []byte(text), nil, compile.Options{})
	if want := diags.Error() + "\n"; code != exitInvalid || stdout.String() != want {
		t.Errorf("meridian validate - of a large regular file = %d, %q, want %q", code, stdout.String(), want)
	}
}
