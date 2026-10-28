package main

import (
	"slices"
	"strings"
	"testing"
)

// twoJobs is a workflow of two jobs, a of two steps and b of one, each of
// which writes a line.
const twoJobs = `jobs:
  a:
    steps:
      - run: echo a1
      - run: echo a2
  b:
    steps:
      - run: echo b
`

// TestLogs checks what meridian logs shows of a run that has ended: its
// steps' logs, each line after its job and step, or one job's, or as JSON
// lines; and a run, or a job, the store hasn't.
func TestLogs(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	if r := commandTest(t, env, "logs", "1"); r.code != exitError || !strings.Contains(r.stderr, "(run_not_found)") || r.stdout != "" {
		t.Errorf("meridian logs without a store = %+v", r)
	}
	if r := runTest(t, env, nil, writeFile(t, "w.yaml", twoJobs)); r.code != exitOK {
		t.Fatalf("meridian run = %+v", r)
	}
	// The jobs run at once, so their lines can come in either order.
	r := commandTest(t, env, "logs", "1")
	lines := strings.Split(strings.TrimSuffix(r.stdout, "\n"), "\n")
	slices.Sort(lines)
	if r.code != exitOK || r.stderr != "" || !slices.Equal(lines, []string{"a step 0| a1", "a step 1| a2", "b step 0| b"}) {
		t.Errorf("meridian logs 1 = %+v", r)
	}
	if r := commandTest(t, env, "logs", "1", "a"); r.code != exitOK || r.stdout != "a step 0| a1\na step 1| a2\n" {
		t.Errorf("meridian logs 1 a = %+v", r)
	}
	r = commandTest(t, env, "logs", "--format", "json", "1", "b")
	var text strings.Builder
	for _, l := range jsonLines(t, r.stdout) {
		if l["kind"] != "log" || l["run"] != "1" || l["job"] != "b" {
			t.Errorf("meridian logs --format json 1 b wrote %v", l)
		}
		if l["part"] == "step" {
			// The log's end can have no text.
			s, _ := l["text"].(string)
			text.WriteString(s)
		}
	}
	if r.code != exitOK || text.String() != "b\n" {
		t.Errorf("meridian logs --format json 1 b = %+v", r)
	}
	if r := commandTest(t, env, "logs", "1", "c"); r.code != exitError || !strings.Contains(r.stderr, "(job_not_found)") || r.stdout != "" {
		t.Errorf("meridian logs of a job the run hasn't = %+v", r)
	}
	if r := commandTest(t, env, "logs", "9"); r.code != exitError || !strings.Contains(r.stderr, "(run_not_found)") {
		t.Errorf("meridian logs of a run the store hasn't = %+v", r)
	}
}

// TestLogsFollow checks meridian logs of a run that runs: its logs as far
// as they are stored, at once; and with -f, as they are stored, until the
// run ends.
func TestLogsFollow(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	workflow, release := waitingWorkflow(t)
	running := startCommand(t, env, nil, "run", workflow)
	waitUntil(t, "the step's first line", func() bool { return strings.Contains(running.stdout.String(), "main step 0| one\n") })
	if r := commandTest(t, env, "logs", "1"); r.code != exitOK || r.stdout != "main step 0| one\n" {
		t.Errorf("meridian logs of a run that runs = %+v", r)
	}
	followed := startCommand(t, env, nil, "logs", "-f", "1", "main")
	waitUntil(t, "meridian logs -f to show the first line", func() bool { return strings.Contains(followed.stdout.String(), "main step 0| one\n") })
	select {
	case r := <-followed.done:
		t.Fatalf("meridian logs -f returned before the run ended: %+v", r)
	default:
	}
	release()
	if r := <-followed.done; r.code != exitOK || r.stdout != "main step 0| one\nmain step 0| two\n" || r.stderr != "" {
		t.Errorf("meridian logs -f = %+v", r)
	}
	if r := <-running.done; r.code != exitOK {
		t.Errorf("meridian run = %+v", r)
	}
}
