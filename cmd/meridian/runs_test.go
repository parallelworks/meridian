package main

import (
	jsonv2 "encoding/json/v2"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/internal/conformancetest"
)

// A runLine is one of the JSON lines meridian run --format json writes, as
// much of it as these tests read.
type runLine struct {
	Version int    `json:"version"`
	Kind    string `json:"kind"`
	Run     string `json:"run"`
	Job     string `json:"job"`
	Attempt int    `json:"attempt"`
	Part    string `json:"part"`
	Step    *int   `json:"step"`
	Try     int    `json:"try"`
	Outcome string `json:"outcome"`
	Text    string `json:"text"`
	Ended   bool   `json:"ended"`
	Cut     bool   `json:"cut"`
}

// runLines reads what meridian run --format json wrote: a JSON object a
// line, each of the version.
func runLines(t *testing.T, stdout string) []runLine {
	t.Helper()
	var out []runLine
	for l := range strings.SplitSeq(strings.TrimSuffix(stdout, "\n"), "\n") {
		var r runLine
		if err := jsonv2.Unmarshal([]byte(l), &r); err != nil {
			t.Fatalf("meridian run --format json wrote %q: %v", l, err)
		}
		if r.Version != local.ProgressVersion {
			t.Errorf("the line %s is of version %d", l, r.Version)
		}
		out = append(out, r)
	}
	return out
}

// TestRunsConformance runs each case of the conformance suite's runs area
// through meridian run, as a person runs a workflow: with its inputs in a
// file -inputs names, and its variables in the config directory's
// variables file, each secret one in an environment variable the file
// names. It checks how meridian run exits, and how the run and each of its
// jobs end, and the logs the case names, as its JSON lines say, and that
// it writes no secret anywhere.
func TestRunsConformance(t *testing.T) {
	cases, err := conformance.Cases("runs")
	if err != nil {
		t.Fatal(err)
	}
	for _, cs := range cases {
		t.Run(strings.TrimPrefix(cs.Name, "runs/"), func(t *testing.T) {
			t.Parallel()
			want := cs.Expect.Run
			if want == nil {
				t.Fatal("the case says nothing of how the run ends")
			}
			env := runEnv(t)
			secrets := writeVariables(t, cs, env)
			args := []string{"--format", "json"}
			if len(cs.Inputs) > 0 {
				data, err := jsonv2.Marshal(cs.Inputs)
				if err != nil {
					t.Fatal(err)
				}
				args = append(args, "--inputs", writeFile(t, "inputs.json", string(data)))
			}
			workflow, err := cs.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			// The workflows its steps call are the case's, as the suite
			// says, rather than those of a Git repository of the user's.
			var setUp func(*cli)
			if sources := conformancetest.Sources(cs); sources != nil {
				setUp = func(c *cli) { c.sources = sources }
			}
			r := runTest(t, env, setUp, append(args, writeFile(t, "workflow.yaml", string(workflow)))...)
			wantCode := exitOK
			if want.Outcome != "success" {
				wantCode = exitInvalid
			}
			if r.code != wantCode {
				t.Errorf("meridian run exited %d, want %d: %s", r.code, wantCode, r.stderr)
			}
			for _, s := range secrets {
				if strings.Contains(r.stdout, s) || strings.Contains(r.stderr, s) {
					t.Errorf("meridian run wrote the secret %q", s)
				}
			}
			lines := runLines(t, r.stdout)
			if last := lines[len(lines)-1]; last.Kind != "run.finished" || last.Outcome != want.Outcome {
				t.Errorf("the run ended %+v, want %s", last, want.Outcome)
			}
			jobs := map[string]string{}
			attempts := map[string]int{}
			logs := map[string]string{}
			ended := map[string]bool{}
			for _, l := range lines {
				switch l.Kind {
				case "job.finished":
					jobs[l.Job] = l.Outcome
				case "job.running":
					attempts[l.Job] = max(attempts[l.Job], l.Attempt)
				case "log":
					name := runLogName(l.Job, l.Attempt, l.Part, l.Step, l.Try)
					logs[name] += l.Text
					if l.Ended {
						ended[name] = true
					}
					if l.Cut {
						t.Errorf("the log %s was cut short", name)
					}
				}
			}
			if len(jobs) != len(want.Jobs) {
				t.Errorf("the run's jobs ended %v, want %d of them", jobs, len(want.Jobs))
			}
			for name, w := range want.Jobs {
				if jobs[name] != w.Outcome {
					t.Errorf("the job %s ended %q, want %s", name, jobs[name], w.Outcome)
				}
				for _, l := range w.Logs {
					part := "step"
					if l.Cleanup {
						part = "cleanup"
					}
					step := l.Step
					log := runLogName(name, attempts[name], part, &step, l.Try)
					if logs[log] != l.Text || !ended[log] {
						t.Errorf("the log %s is %q, ended %v; want %q: it has %v", log, logs[log], ended[log], l.Text, slices.Sorted(maps.Keys(logs)))
					}
				}
			}
		})
	}
}

// runLogName names a log for the tests: its job, attempt, which of the
// attempt's logs it is, and its step and try.
func runLogName(job string, attempt int, part string, step *int, try int) string {
	if step == nil {
		return fmt.Sprintf("%s/%d %s", job, attempt, part)
	}
	return fmt.Sprintf("%s/%d %s %d.%d", job, attempt, part, *step, try)
}

// writeVariables writes the case's variables, which its contexts org and
// vars hold, to the variables file of the config directory env names, as
// a person writes theirs: each secret one in an environment variable of
// env's, which the file names. It returns the secret ones' values.
func writeVariables(t *testing.T, cs conformance.Case, env map[string]string) []string {
	t.Helper()
	var b strings.Builder
	var secrets []string
	for _, context := range []string{"org", "vars"} {
		vars, _ := cs.Contexts[context].(map[string]any)
		if len(vars) == 0 {
			continue
		}
		fmt.Fprintf(&b, "%s:\n", context)
		for _, name := range slices.Sorted(maps.Keys(vars)) {
			value, ok := vars[name].(string)
			if !ok {
				t.Fatalf("the case's variable %s.%s isn't text", context, name)
			}
			if context == "vars" && slices.Contains(cs.SecretVariables, name) {
				envName := "MERIDIAN_TEST_SECRET_" + strings.ToUpper(name)
				env[envName] = value
				secrets = append(secrets, value)
				fmt.Fprintf(&b, "  %s: {secret: true, env: %s}\n", quoted(t, name), envName)
				continue
			}
			fmt.Fprintf(&b, "  %s: %s\n", quoted(t, name), quoted(t, value))
		}
	}
	if b.Len() == 0 {
		return nil
	}
	dir := env[local.EnvConfigDir]
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, local.VariablesFile), []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return secrets
}

// quoted is s as a YAML string: in JSON's quotes, which YAML reads alike.
func quoted(t *testing.T, s string) string {
	t.Helper()
	data, err := jsonv2.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
