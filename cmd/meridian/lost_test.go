//go:build linux || darwin || freebsd

package main

import (
	"bytes"
	jsonv2 "encoding/json/v2"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/internal/testtiming"
)

// killedStep is a step that says where it is, and runs until it is
// killed, but on the job's retry, when it ends at once.
const killedStep = `      - run: |
          if [ "$PW_WORKFLOW_JOB_CURRENT_RETRY" = 1 ]; then echo again; exit 0; fi
          echo $$ > "$PW_JOB_DIR/step.pid"
          exec sleep 120
`

// TestWorkerKilled checks a worker killed during a step, as its host can
// kill it, which the local backend's placement says which process it is:
// with the coordinator's timing scaled, as only a test's may be, its
// attempt is lost, with worker-lost, once its lease has lapsed. The job
// fails with it; or, when its retry: lists worker-lost, runs again, and
// succeeds. meridian run's JSON lines show the attempt lost.
//
// The coordinator's timing is scaled only in a process of its own, so the
// test runs itself again with testtiming.EnvScale.
func TestWorkerKilled(t *testing.T) {
	// It holds one place while its subtests run, two at once, in a process
	// of its own, which takes no place of this one's.
	parallel(t)
	if testtiming.Scale == 0 {
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(t.Context(), exe, "-test.run=^TestWorkerKilled$", "-test.v", "-test.count=1")
		cmd.Env = append(os.Environ(), testtiming.EnvScale+"=0.1")
		out, err := cmd.CombinedOutput()
		if err != nil || !bytes.Contains(out, []byte("--- PASS: TestWorkerKilled")) {
			t.Fatalf("the test with its timing scaled: %v\n%s", err, out)
		}
		return
	}
	for _, tc := range []struct {
		name, workflow   string
		code             int
		outcome, reason  string
		attempts, killed int
	}{
		{"lost", "jobs:\n  main:\n    steps:\n" + killedStep, exitInvalid, "failure", "worker-lost", 1, 1},
		{
			"retried", "jobs:\n  main:\n    retry:\n      reasons: [worker-lost]\n      max-retries: 1\n      interval: 1s\n    steps:\n" + killedStep,
			exitOK, "success", "", 2, 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := runEnv(t)
			var stdout, stderr lockedBuffer
			c := &cli{
				stdin: strings.NewReader(""), stdout: &stdout, stderr: &stderr,
				lookupEnv: func(name string) (string, bool) { v, ok := env[name]; return v, ok },
			}
			done := make(chan int, 1)
			workflow := writeFile(t, "w.yaml", tc.workflow)
			go func() { done <- run(t.Context(), c, []string{"run", "--format", "json", workflow}) }()

			// Once the step has started, its worker is killed, as its host
			// would kill it, and the step's process, which outlives it,
			// once the test ends.
			runDir := killWorker(t, env, &stdout)
			t.Cleanup(func() { killStep(t, runDir) })
			var code int
			select {
			case code = <-done:
			case <-time.After(2 * time.Minute):
				t.Fatalf("meridian run didn't end:\n%s\n%s", stdout.String(), stderr.String())
			}
			if code != tc.code {
				t.Errorf("meridian run exited %d, want %d: %s", code, tc.code, stderr.String())
			}
			var lost, attempts int
			var job string
			for _, l := range killedLines(t, stdout.String()) {
				switch l.Kind {
				case "attempt.ended":
					if l.End == "lost" && l.Reason == "worker-lost" && l.Attempt == 1 {
						lost++
					}
				case "job.running":
					attempts = max(attempts, l.Attempt)
				case "job.finished":
					job = l.Outcome + "/" + l.Reason
				}
			}
			if lost != tc.killed || attempts != tc.attempts || job != tc.outcome+"/"+tc.reason {
				t.Errorf("the run showed %d attempts lost, %d attempts, and the job %s; want %d, %d and %s/%s\n%s",
					lost, attempts, job, tc.killed, tc.attempts, tc.outcome, tc.reason, stdout.String())
			}
		})
	}
}

// A killedLine is one of meridian run's JSON lines, as much as
// TestWorkerKilled reads.
type killedLine struct {
	Kind    string `json:"kind"`
	RunDir  string `json:"runDir"`
	Attempt int    `json:"attempt"`
	Step    *int   `json:"step"`
	End     string `json:"end"`
	Outcome string `json:"outcome"`
	Reason  string `json:"reason"`
}

// killedLines reads meridian run's JSON lines, but for the last, which
// may not have ended yet.
func killedLines(t *testing.T, stdout string) []killedLine {
	t.Helper()
	var out []killedLine
	lines := strings.Split(stdout, "\n")
	for _, l := range lines[:len(lines)-1] {
		var k killedLine
		if err := jsonv2.Unmarshal([]byte(l), &k, jsonv2.RejectUnknownMembers(false)); err != nil {
			t.Fatalf("meridian run wrote %q: %v", l, err)
		}
		out = append(out, k)
	}
	return out
}

// killWorker waits for the first attempt's step to start, as meridian
// run's JSON lines say, then kills its worker, the process its placement
// in the local backend's state names, and returns the run's directory.
func killWorker(t *testing.T, env map[string]string, stdout *lockedBuffer) string {
	t.Helper()
	var runDir string
	deadline := time.Now().Add(time.Minute)
	for started := false; !started; {
		if time.Now().After(deadline) {
			t.Fatalf("the step never started:\n%s", stdout.String())
		}
		for _, l := range killedLines(t, stdout.String()) {
			switch {
			case l.Kind == "run.created":
				runDir = l.RunDir
			case l.Kind == "step.started" && l.Attempt == 1:
				started = true
			}
		}
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo // polls meridian run's output, which says nothing more
	}
	placements, err := filepath.Glob(filepath.Join(env[local.EnvStateDir], "placements", "*.json"))
	if err != nil || len(placements) != 1 {
		t.Fatalf("the placements are %v: %v", placements, err)
	}
	data, err := os.ReadFile(placements[0])
	if err != nil {
		t.Fatal(err)
	}
	var placement struct {
		PID int `json:"pid"`
	}
	if err := jsonv2.Unmarshal(data, &placement, jsonv2.RejectUnknownMembers(false)); err != nil || placement.PID <= 0 {
		t.Fatalf("the placement is %s: %v", data, err)
	}
	if err := syscall.Kill(placement.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	return runDir
}

// killStep kills the killed attempt's step, which its worker left
// running, as the step's file in the run's directory names it.
func killStep(t *testing.T, runDir string) {
	data, err := os.ReadFile(filepath.Join(runDir, "step.pid"))
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		t.Error(err)
		return
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		t.Errorf("the step's process is %q", data)
		return
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		t.Error(err)
	}
}
