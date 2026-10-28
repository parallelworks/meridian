package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
)

// callsCase is the conformance suite's runs/calls case, whose step calls a
// workflow that succeeds, and another that fails.
func callsCase(t *testing.T) conformance.Case {
	t.Helper()
	cases, err := conformance.Cases("runs")
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(cases, func(c conformance.Case) bool { return c.Name == "runs/calls" })
	if i < 0 {
		t.Fatal("no case runs/calls")
	}
	return cases[i]
}

// TestCalls checks what the meridian command shows of a run whose steps
// call workflows, runs of their own: meridian run shows each one's events
// and logs under the step that called it, masked; meridian logs shows them
// so, of the run or of the job whose step called them, and a run a step
// called alone; and meridian status lists the runs no step called, with
// how many runs their trees have below them, or, with -all, every run, a
// run a step called with what called it; and shows the runs a run's steps
// called, and what called a run a step did.
func TestCalls(t *testing.T) {
	cs := callsCase(t)
	env := runEnv(t)
	secrets := writeVariables(t, cs, env)
	workflow, err := cs.Workflow()
	if err != nil {
		t.Fatal(err)
	}
	sources := conformancetest.Sources(cs)
	r := runTest(t, env, func(c *cli) { c.sources = sources }, writeFile(t, "workflow.yaml", string(workflow)))
	if r.code != exitInvalid {
		t.Fatalf("meridian run = %+v", r)
	}
	for _, s := range secrets {
		if strings.Contains(r.stdout, s) || strings.Contains(r.stderr, s) {
			t.Errorf("meridian run wrote the secret %q", s)
		}
	}
	// Run 1 is the root, and 2 and 3 the runs its steps 0 and 2 called.
	for _, want := range []string{
		`(?m)^\d\d:\d\d:\d\d main step 0 › run 2 created$`,
		`(?m)^\d\d:\d\d:\d\d main step 0 › greet attempt 1 running`,
		`(?m)^main step 0 › greet step 0\| greeting hello, token \*\*\*$`,
		`(?m)^\d\d:\d\d:\d\d main step 0 › run 2 finished: success$`,
		`(?m)^main step 1\| hello from the caller, as github\.conformance\.calls, where its caller looks$`,
		`(?m)^\d\d:\d\d:\d\d main step 2 › run 3 created$`,
		`(?m)^\d\d:\d\d:\d\d main step 2 › fails step 0 finished: failure \(exit status 3\)$`,
		`(?m)^\d\d:\d\d:\d\d main step 2 › run 3 finished: failure$`,
		`run 1 finished: failure\n$`,
	} {
		if !regexp.MustCompile(want).MatchString(r.stdout) {
			t.Errorf("meridian run wrote no line %s:\n%s", want, r.stdout)
		}
	}
	if i, j := strings.Index(r.stdout, "run 3 finished"), strings.Index(r.stdout, "run 1 finished"); i < 0 || j < i {
		t.Errorf("meridian run showed the root's end before its child's:\n%s", r.stdout)
	}

	greeting := "main step 0 › greet step 0| greeting hello, token ***\n"
	for _, args := range [][]string{{"logs", "1"}, {"logs", "1", "main"}} {
		if r := commandTest(t, env, args...); r.code != exitOK || !strings.Contains(r.stdout, greeting) ||
			!strings.Contains(r.stdout, "main step 1| hello from the caller") {
			t.Errorf("meridian %s = %+v", strings.Join(args, " "), r)
		}
	}
	if r := commandTest(t, env, "logs", "2"); r.code != exitOK || r.stdout != "greet step 0| greeting hello, token ***\n" {
		t.Errorf("meridian logs of a run a step called = %+v", r)
	}
	r = commandTest(t, env, "logs", "--format", "json", "1")
	var child bool
	for _, l := range jsonLines(t, r.stdout) {
		if l["run"] == "2" {
			child = true
			callers, _ := l["callers"].([]any)
			if len(callers) != 1 {
				t.Errorf("meridian logs --format json 1 wrote %v", l)
				continue
			}
			if c, _ := callers[0].(map[string]any); c["run"] != "1" || c["job"] != "main" || c["attempt"] != float64(1) || c["step"] != float64(0) {
				t.Errorf("meridian logs --format json 1 wrote %v", l)
			}
		}
	}
	if r.code != exitOK || !child {
		t.Errorf("meridian logs --format json 1 = %+v", r)
	}

	when := `\d{4}-\d\d-\d\d \d\d:\d\d:\d\d`
	if r := commandTest(t, env, "status"); r.code != exitOK || !regexp.MustCompile(`^RUN +STATE +OUTCOME +CREATED +WORKFLOW +TREE
1 +ended +failure +`+when+` +\S+ +2 runs below
$`).MatchString(r.stdout) {
		t.Errorf("meridian status = %+v", r)
	}
	if r := commandTest(t, env, "status", "-all"); r.code != exitOK || !regexp.MustCompile(`^RUN +STATE +OUTCOME +CREATED +WORKFLOW +TREE
3 +ended +failure +`+when+` +\S+ +called by run 1 main step 2
2 +ended +success +`+when+` +\S+ +called by run 1 main step 0
1 +ended +failure +`+when+` +\S+ +2 runs below
$`).MatchString(r.stdout) {
		t.Errorf("meridian status -all = %+v", r)
	}
	if r := commandTest(t, env, "status", "1"); r.code != exitOK ||
		!regexp.MustCompile(`(?m)^CALLED BY +RUN +STATE +OUTCOME +WORKFLOW\nmain step 0 +2 +ended +success +\S+\nmain step 2 +3 +ended +failure +\S+$`).MatchString(r.stdout) {
		t.Errorf("meridian status 1 = %+v", r)
	}
	if r := commandTest(t, env, "status", "2"); r.code != exitOK || !strings.Contains(r.stdout, "called by: run 1 main step 0, of the tree of run 1\n") {
		t.Errorf("meridian status 2 = %+v", r)
	}
	r = commandTest(t, env, "status", "--format", "json", "1")
	var kids []string
	for _, l := range jsonLines(t, r.stdout) {
		if l["kind"] == "child" {
			parent, _ := l["parent"].(map[string]any)
			kids = append(kids, fmt.Sprintf("%v %v %v %v", l["run"], parent["job"], parent["step"], l["root"]))
		}
	}
	if want := []string{"2 main 0 1", "3 main 2 1"}; r.code != exitOK || !slices.Equal(kids, want) {
		t.Errorf("meridian status --format json 1 wrote the children %v, want %v: %+v", kids, want, r)
	}
}

// waitSources gives the workflow a test's step calls, github/t/wait, as
// git would, but from memory: one whose step waits until a file is there.
type waitSources map[string]string

func (s waitSources) workflow(ref plan.SourceRef) (*compile.Workflow, error) {
	text, ok := s[ref.Uses]
	if !ok {
		return nil, compile.ErrNotFound
	}
	sum := sha256.Sum256([]byte(text))
	digest := "sha256:" + hex.EncodeToString(sum[:])
	return &compile.Workflow{
		Source: plan.Source{Location: ref.Uses, Revision: digest, Digest: digest, Ref: ref},
		Text:   []byte(text),
	}, nil
}

func (s waitSources) Resolve(_ context.Context, ref plan.SourceRef) (*compile.Workflow, error) {
	return s.workflow(ref)
}

func (s waitSources) Read(_ context.Context, source plan.Source) (*compile.Workflow, error) {
	return s.workflow(source.Ref)
}

func (waitSources) Pin(context.Context, string, string) (string, error) {
	return "", compile.ErrNotFound
}

// TestCancelTree checks that meridian cancel of a run stops the runs its
// steps called that run, as their steps stop: each ends cancelled, before
// the run does, as meridian run shows them.
func TestCancelTree(t *testing.T) {
	env := runEnv(t)
	file := writeFile(t, "never", "")
	sources := waitSources{"github/t/wait": fmt.Sprintf(`jobs:
  wait:
    steps:
      - run: |
          echo waiting
          while [ -e %s ]; do sleep 0.05; done
`, quoted(t, file))}
	workflow := writeFile(t, "w.yaml", "jobs:\n  main:\n    steps:\n      - uses: github/t/wait\n        with: {$yaml: w.yaml}\n")
	running := startCommand(t, env, func(c *cli) { c.sources = sources }, "run", workflow)
	waitUntil(t, "the child's step to start", func() bool {
		return strings.Contains(running.stdout.String(), "main step 0 › wait step 0 started")
	})
	cancel := meridianProcess(t, env, "cancel", "1")
	if err := cancel.cmd.Wait(); err != nil {
		t.Fatalf("meridian cancel 1: %v\n%s%s", err, cancel.stdout.String(), cancel.stderr.String())
	}
	r := <-running.done
	if r.code != exitInvalid || !strings.Contains(r.stdout, "main step 0 › run 2 finished: cancelled (cancelled)\n") ||
		!strings.HasSuffix(r.stdout, "run 1 finished: cancelled (cancelled)\n") {
		t.Errorf("meridian run of the run cancelled = %+v", r)
	}
	if r := commandTest(t, env, "status", "2"); r.code != exitOK || !strings.HasPrefix(r.stdout, "run 2: ended, cancelled (cancelled)\n") {
		t.Errorf("meridian status of the run the cancelled run's step called = %+v", r)
	}
}
