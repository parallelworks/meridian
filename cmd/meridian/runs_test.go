package main

import (
	"archive/tar"
	"cmp"
	"context"
	jsonv2 "encoding/json/v2"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/internal/conformancetest"
)

// A runLine is one of the JSON lines meridian run --format json writes, as
// much of it as these tests read.
type runLine struct {
	Version int          `json:"version"`
	Kind    string       `json:"kind"`
	Run     string       `json:"run"`
	Callers []lineCaller `json:"callers"`
	Child   string       `json:"child"`
	Job     string       `json:"job"`
	Attempt int          `json:"attempt"`
	Part    string       `json:"part"`
	Step    *int         `json:"step"`
	Try     int          `json:"try"`
	Outcome string       `json:"outcome"`
	Text    string       `json:"text"`
	Ended   bool         `json:"ended"`
	Cut     bool         `json:"cut"`
}

// A lineCaller is the try of a step that called a run, in a runLine.
type lineCaller struct {
	Run     string `json:"run"`
	Job     string `json:"job"`
	Attempt int    `json:"attempt"`
	Step    int    `json:"step"`
	Try     int    `json:"try"`
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
	t.Parallel()
	cases, err := conformance.Cases("runs")
	if err != nil {
		t.Fatal(err)
	}
	for _, cs := range cases {
		t.Run(strings.TrimPrefix(cs.Name, "runs/"), func(t *testing.T) {
			parallel(t)
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
			// says, rather than those of a Git repository of the user's; and
			// its hosts are SSH servers of the test's, each at its name.
			var setUps []func(*cli)
			if sources := conformancetest.Sources(cs); sources != nil {
				trees := caseTrees{cs}
				setUps = append(setUps, func(c *cli) { c.sources, c.trees = sources, trees })
			}
			if len(cs.Hosts) > 0 {
				setUps = append(setUps, caseHosts(t, cs, env))
			}
			setUp := func(c *cli) {
				for _, f := range setUps {
					f(c)
				}
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
			if last := lines[len(lines)-1]; last.Kind != "run.finished" || last.Outcome != want.Outcome || len(last.Callers) > 0 {
				t.Errorf("the run ended %+v, want %s", last, want.Outcome)
			}
			if first := lines[0]; first.Kind != "run.created" {
				t.Fatalf("the first line is %+v", first)
			}
			checkRunLines(t, lines, lines[0].Run, nil, "the run", *want, stepsElsewhere(t, workflow))
		})
	}
}

// checkRunLines checks what lines, meridian run's, say of the run id, what,
// which the steps' tries callers called, from the run meridian run ran
// down: that it ended as want says, and its jobs, their logs, and the runs
// their steps called, in turn, each line of which says which steps' tries
// called it.
func checkRunLines(t *testing.T, lines []runLine, id string, callers []lineCaller, what string, want conformance.RunEnd, elsewhere map[string]bool) {
	t.Helper()
	var outcome string
	jobs := map[string]string{}
	attempts := map[string]int{}
	logs := map[string]string{}
	ended := map[string]bool{}
	called := map[string]runLine{}
	for _, l := range lines {
		if l.Run != id {
			continue
		}
		if !slices.Equal(l.Callers, callers) {
			t.Errorf("a line of %s says it was called by %+v, want %+v: %+v", what, l.Callers, callers, l)
		}
		switch l.Kind {
		case "run.finished":
			outcome = l.Outcome
		case "job.finished":
			jobs[l.Job] = l.Outcome
		case "job.running":
			attempts[l.Job] = max(attempts[l.Job], l.Attempt)
		case "child.created":
			// A step run, of a step on another host than its job's, is
			// Meridian's own, which the case doesn't name: the step's try's
			// own (docs/design.md, Steps on another host).
			name, _, _ := strings.Cut(l.Job, "#")
			if !elsewhere[fmt.Sprintf("%s %d", name, *l.Step)] {
				called[fmt.Sprintf("%s %d.%d", l.Job, *l.Step, l.Try)] = l
			}
		case "log":
			name := runLogName(l.Job, l.Attempt, l.Part, l.Step, l.Try)
			logs[name] += l.Text
			if l.Ended {
				ended[name] = true
			}
			if l.Cut {
				t.Errorf("%s's log %s was cut short", what, name)
			}
		}
	}
	if outcome != want.Outcome {
		t.Errorf("%s ended %q, want %s", what, outcome, want.Outcome)
	}
	if len(jobs) != len(want.Jobs) {
		t.Errorf("%s's jobs ended %v, want %d of them", what, jobs, len(want.Jobs))
	}
	calls := 0
	for name, w := range want.Jobs {
		if jobs[name] != w.Outcome {
			t.Errorf("%s's job %s ended %q, want %s", what, name, jobs[name], w.Outcome)
		}
		for _, l := range w.Logs {
			part := "step"
			if l.Cleanup {
				part = "cleanup"
			}
			step := l.Step
			log := runLogName(name, attempts[name], part, &step, l.Try)
			if logs[log] != l.Text || !ended[log] {
				t.Errorf("%s's log %s is %q, ended %v; want %q: it has %v", what, log, logs[log], ended[log], l.Text, slices.Sorted(maps.Keys(logs)))
			}
		}
		for _, call := range w.Calls {
			created, ok := called[fmt.Sprintf("%s %d.%d", name, call.Step, call.Try)]
			if !ok {
				t.Errorf("%s's job %s's step %d's try %d called no run: it called %v", what, name, call.Step, call.Try, slices.Sorted(maps.Keys(called)))
				continue
			}
			calls++
			if created.Attempt != attempts[name] {
				t.Errorf("%s's job %s's attempt %d called a run, not its last, %d", what, name, created.Attempt, attempts[name])
			}
			under := append(slices.Clip(callers), lineCaller{Run: id, Job: name, Attempt: created.Attempt, Step: call.Step, Try: call.Try})
			checkRunLines(t, lines, created.Child, under, fmt.Sprintf("the run %s's job %s's step %d called", what, name, call.Step), call.Run, nil)
		}
	}
	if calls != len(called) {
		t.Errorf("%s's steps called %d runs, of which the case names %d", what, len(called), calls)
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

// caseHosts starts the hosts the case names, each an SSH server of the
// test's at its name, as newSSHHosts does, those that share a home with
// another after it, and returns what has meridian reach them.
func caseHosts(t *testing.T, cs conformance.Case, env map[string]string) func(*cli) {
	t.Helper()
	shares := func(name string) int {
		if cs.Hosts[name].Home != "" {
			return 1
		}
		return 0
	}
	names := slices.SortedFunc(maps.Keys(cs.Hosts), func(a, b string) int {
		return cmp.Or(cmp.Compare(shares(a), shares(b)), cmp.Compare(a, b))
	})
	specs := make([]hostSpec, len(names))
	for i, name := range names {
		specs[i] = hostSpec{alias: name, sameHome: cs.Hosts[name].Home}
	}
	return newSSHHosts(t, env, specs...)[names[0]].setUp
}

// stepsElsewhere returns the steps of workflow, by their job's name and
// their index, as "main 1", that run on another host than their job's, as
// step runs: those with an ssh: of their own that run a command, or check
// out a repository.
func stepsElsewhere(t *testing.T, workflow []byte) map[string]bool {
	t.Helper()
	var w struct {
		Jobs map[string]struct {
			Steps []map[string]any `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(workflow, &w); err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for name, j := range w.Jobs {
		for i, s := range j.Steps {
			uses, _ := s["uses"].(string)
			if _, ok := s["ssh"]; ok && (uses == "" || strings.HasPrefix(uses, "parallelworks/checkout")) {
				out[fmt.Sprintf("%s %d", name, i)] = true
			}
		}
	}
	return out
}

// caseTrees are the trees of a case's repositories, which its checkout
// steps check out, at the commit its sources pin (conformancetest.Tree).
type caseTrees struct{ cs conformance.Case }

// files returns the files of the tree of repo at commit that paths name,
// or all of them, for none, in order, and the indexes of paths that name
// none.
func (c caseTrees) files(repo, commit string, paths []string) (map[string][]byte, []string, []int, error) {
	if !conformancetest.Holds(c.cs, repo) || commit != conformancetest.Commit(c.cs) {
		return nil, nil, nil, fmt.Errorf("no commit %s of %s", commit, repo)
	}
	files, err := conformancetest.Tree(c.cs)
	if err != nil {
		return nil, nil, nil, err
	}
	all := slices.Sorted(maps.Keys(files))
	if len(paths) == 0 {
		return files, all, nil, nil
	}
	var names []string
	var missing []int
	for i, p := range paths {
		p = strings.TrimSuffix(p, "/")
		n := len(names)
		for _, name := range all {
			if (name == p || strings.HasPrefix(name, p+"/")) && !slices.Contains(names, name) {
				names = append(names, name)
			}
		}
		if len(names) == n {
			missing = append(missing, i)
		}
	}
	slices.Sort(names)
	return files, names, missing, nil
}

func (c caseTrees) SurveyTree(_ context.Context, repo, commit string, paths []string) (coordinator.TreeSurvey, error) {
	files, names, missing, err := c.files(repo, commit, paths)
	if err != nil {
		return coordinator.TreeSurvey{}, err
	}
	s := coordinator.TreeSurvey{Entries: len(names), Missing: missing}
	for _, name := range names {
		s.Size += int64(len(files[name]))
	}
	return s, nil
}

func (c caseTrees) WriteTree(_ context.Context, repo, commit string, paths []string, w io.Writer) error {
	files, names, _, err := c.files(repo, commit, paths)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(w)
	for _, name := range names {
		if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: name, Mode: 0o644, Size: int64(len(files[name]))}); err != nil {
			return err
		}
		if _, err := tw.Write(files[name]); err != nil {
			return err
		}
	}
	return tw.Close()
}
