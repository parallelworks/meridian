package main

import (
	"context"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
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
		{args: []string{"--inputs", inputs, "--input", "count=many"}, code: exitInvalid, want: "input_not_number"},
		{args: []string{"--input", "cluster.size=3", "--input", "cluster.name=a", "--input", "count=1"}, code: exitOK},
	} {
		r := runCLI(t, emptyEnv(t), "", append(append([]string{"validate"}, tc.args...), workflow)...)
		if r.code != tc.code || !strings.Contains(r.stdout, tc.want) || r.stderr != "" {
			t.Errorf("meridian validate %q = %+v, want exit status %d and %s", tc.args, r, tc.code, tc.want)
		}
	}
}

func TestValidateInputsRefuses(t *testing.T) {
	workflow := writeFile(t, "w.yaml", groupInputs)
	for name, args := range map[string][]string{
		"no =":           {"--input", "count"},
		"empty name":     {"--input", "cluster..size=1"},
		"value then key": {"--input", "count=1", "--input", "count.size=2"},
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

func TestSetPath(t *testing.T) {
	values := map[string]any{"a": map[string]any{"x": "1"}}
	for _, s := range []string{"a.y=2", "b.c.d=3", "e=4", "e=5"} {
		path, value, _ := strings.Cut(s, "=")
		if err := setPath(values, path, value); err != nil {
			t.Fatalf("setPath(%s): %v", s, err)
		}
	}
	want := map[string]any{
		"a": map[string]any{"x": "1", "y": "2"},
		"b": map[string]any{"c": map[string]any{"d": "3"}},
		"e": "5",
	}
	if !reflect.DeepEqual(values, want) {
		t.Errorf("values = %v, want %v", values, want)
	}
	if err := setPath(values, "e.f", "6"); err == nil {
		t.Error("setPath made a group of a value")
	}
}

func TestValidateStdin(t *testing.T) {
	r := runCLI(t, emptyEnv(t), "jobs:\n  a:\n    if: ${{ steps.x }}\n    steps:\n      - run: echo\n", "validate", "-")
	if r.code != exitInvalid || !strings.HasPrefix(r.stdout, "<stdin>:3:13: error: ") {
		t.Errorf("meridian validate - = %+v", r)
	}
}

// TestValidateTooLarge checks that a file larger than the compiler reads
// is reported as the compiler would report it, without being kept.
func TestValidateTooLarge(t *testing.T) {
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
	// Creating the run refuses it already, without --start too.
	if r := runCLI(t, env, "", "validate", sessions); r.code != exitInvalid || !strings.Contains(r.stdout, "(resolver_missing)") {
		t.Errorf("meridian validate with sessions = %+v", r)
	}
	if r := runCLI(t, env, "", "validate", "--start", sessions); r.code != exitInvalid || !strings.Contains(r.stdout, "s.yaml:1:1: error:") || !strings.Contains(r.stdout, "(resolver_missing)") {
		t.Errorf("meridian validate --start with sessions = %+v", r)
	}
}

// TestValidateChecksConfig checks that validate creates the run as it would
// run: with the execution groups in the config, the local group among
// them, and the workflows uses: names, which it fetches and pins.
func TestValidateChecksConfig(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git isn't installed")
	}
	// The workflow uses: names is in a repository here, which Git reaches
	// for github.com's.
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
	// And for a GitLab server's that sources.yaml registers, and one's
	// that it doesn't, which Git is never asked for.
	gitConfig := writeFile(t, "gitconfig", "[url \"file://"+filepath.ToSlash(origins)+"/\"]\n\tinsteadOf = https://github.com/\n\tinsteadOf = https://git.example.com/\n\tinsteadOf = https://other.example.com/\n")
	t.Setenv("GIT_CONFIG_GLOBAL", gitConfig)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	git("init", "--quiet", "--initial-branch=main")
	if err := os.MkdirAll(filepath.Join(repo, "flows"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "flows", "run.yaml"), []byte("jobs:\n  x:\n    steps:\n      - run: echo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "--quiet", "-m", "flows")

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

func TestValidateGroups(t *testing.T) {
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

// TestValidateDevZero checks that a stream that never ends is read only to
// the limit.
func TestValidateDevZero(t *testing.T) {
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
	stdin, w := io.Pipe()
	defer w.Close()
	ctx, cancel := context.WithCancel(t.Context())
	var stdout, stderr strings.Builder
	env := emptyEnv(t)
	c := &cli{stdin: stdin, stdout: &stdout, stderr: &stderr, getenv: func(name string) string { return env[name] }}
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
	workflow := writeFile(t, "w.yaml", "jobs:\n  a:\n    steps:\n      - run: echo\n        \"x\\e[2J\\nsrc/main.go:1:1: error: injected\": 1\n")
	r := runCLI(t, emptyEnv(t), "", "validate", workflow)
	if strings.ContainsAny(r.stdout, "\x1b\a") || strings.Count(r.stdout, "\n") != 1 || !strings.Contains(r.stdout, `x\x1b[2J\nsrc/main.go:1:1: error: injected`) {
		t.Errorf("meridian validate wrote %q", r.stdout)
	}
}

// TestValidateProblems checks the problems validate reports besides a
// workflow's: each a kind, at the file when one is to blame.
func TestValidateProblems(t *testing.T) {
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
		{[]string{"validate", "--input", "count=1", "--input", "count.size=2", workflow}, "(input_path_conflict)"},
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
	text := "# " + strings.Repeat("x", format.DefaultMaxSize) + "\n"
	f, err := os.Open(writeFile(t, "big.yaml", text))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	env := emptyEnv(t)
	var stdout, stderr strings.Builder
	c := &cli{stdin: f, stdout: &stdout, stderr: &stderr, getenv: func(name string) string { return env[name] }}
	code := run(t.Context(), c, []string{"validate", "-"})
	_, diags := compile.Compile("<stdin>", []byte(text), nil, compile.Options{})
	if want := diags.Error() + "\n"; code != exitInvalid || stdout.String() != want {
		t.Errorf("meridian validate - of a large regular file = %d, %q, want %q", code, stdout.String(), want)
	}
}
