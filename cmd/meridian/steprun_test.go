package main

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
)

// Steps on another host (docs/design.md, Steps on another host), end to
// end: meridian run of a workflow whose steps' ssh: name hosts that the
// test starts, each an SSH server of the test's, as newSSHHosts says, or
// CI's sshd for TestSSHStepRun's one.

// runsOn returns the directory under which meridian host makes the run
// directories of the host h.
func runsOn(h *sshHost) string {
	return filepath.Join(h.home, ".local", "state", "meridian", "runs")
}

// placementsOn returns the placements meridian host keeps on the host h:
// none once each of its workers' ends is recorded, and its placement
// forgotten.
func placementsOn(h *sshHost) []string {
	left, _ := filepath.Glob(filepath.Join(h.home, ".local", "state", "meridian", "hosts", "*", "placements", "*"))
	return left
}

// runSucceeds runs workflow with args, as meridian run does, and fails the
// test unless run 1 succeeds; and returns what meridian logs shows of it.
func runSucceeds(t *testing.T, env map[string]string, setUp func(*cli), workflow string, args ...string) string {
	t.Helper()
	r := endedWithin(t, startCommand(t, env, setUp, append(append([]string{"run"}, args...), writeFile(t, "w.yaml", workflow))...))
	if r.code != exitOK || !strings.HasSuffix(r.stdout, "run 1 finished: success\n") {
		t.Fatalf("meridian run = %+v", r)
	}
	logs := commandTest(t, env, "logs", "1")
	if logs.code != exitOK {
		t.Fatalf("meridian logs 1 = %+v", logs)
	}
	return logs.stdout
}

// hasLines fails the test unless text, what meridian logs shows, has each
// of lines.
func hasLines(t *testing.T, text string, lines ...string) {
	t.Helper()
	for _, l := range lines {
		if !strings.Contains(text, l+"\n") {
			t.Errorf("meridian logs has no line %q:\n%s", l, text)
		}
	}
}

// sshStepRun is a workflow whose job local runs where the run started,
// with a step on the cluster its input names, which appends to $OUTPUTS
// there what it reads of the host and of the job's earlier step; and whose
// job after reads those outputs through needs.
const sshStepRun = `on:
  execute:
    inputs:
      cluster:
        type: compute-clusters
jobs:
  local:
    steps:
      - run: echo "here=$PWD" >> $OUTPUTS
      - run: |
          echo "remote job dir $PW_JOB_DIR, in $PWD"
          echo "remote=${{ env.PW_JOB_DIR }}" >> $OUTPUTS
          echo "from=${{ needs.local.outputs.here }}" >> $OUTPUTS
        ssh:
          remoteHost: ${{ inputs.cluster.ip }}
  after:
    needs: [local]
    steps:
      - run: |
          echo "remote ${{ needs.local.outputs.remote }}"
          echo "from ${{ needs.local.outputs.from }}"
`

// TestSSHStepRun checks a step whose ssh: names a host, of a job that runs
// where the run started: it runs on the host, as a run of its own, in the
// host's copy of the run's directory, with the host's PW_JOB_DIR, reading
// what its job's earlier step wrote; what it appends to $OUTPUTS there is
// its job's, which a later job reads through needs; and its placement on
// the host is forgotten once its end is recorded. On CI's ssh job, the
// host is OpenSSH's sshd.
func TestSSHStepRun(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	h := newSSHHost(t, env)
	logs := runSucceeds(t, env, h.setUp, sshStepRun, "--input", "cluster=hpc")
	// The run's directory there, named for the run and the store.
	dir := filepath.Join(runsOn(h), "1-")
	if !strings.Contains(logs, "local step 1 › local step 1| remote job dir "+dir) || !strings.Contains(logs, "after step 0| remote "+dir) {
		t.Errorf("the step ran elsewhere than the host's copy of the run's directory, or its outputs weren't its job's:\n%s", logs)
	}
	if !strings.Contains(logs, "after step 0| from "+env["TMPDIR"]) {
		t.Errorf("the step on the host didn't read its job's earlier step's outputs:\n%s", logs)
	}
	waitUntil(t, "the placement to be forgotten", func() bool { return len(placementsOn(h)) == 0 })
}

// TestSSHStepRunCheckout checks a checkout on a host, a step of a job that
// runs where the run started, whose tree a later job on that host runs a
// script of, by its path in the run's directory there, as format 1's
// workflows do.
func TestSSHStepRunCheckout(t *testing.T) {
	flowsRepo(t, map[string]string{"scripts/x": "#!/bin/sh\necho \"ran in $PWD\"\n", "README": "app\n"})
	env := runEnv(t)
	h := newSSHHost(t, env)
	logs := runSucceeds(t, env, h.setUp, `on:
  execute:
    inputs:
      cluster:
        type: compute-clusters
jobs:
  fetch:
    steps:
      - uses: parallelworks/checkout
        ssh:
          remoteHost: ${{ inputs.cluster.ip }}
        with:
          repo: https://github.com/acme/flows.git
          branch: main
          path: app
  use:
    needs: [fetch]
    ssh:
      remoteHost: ${{ inputs.cluster.ip }}
    steps:
      - run: ./app/scripts/x
`, "--input", "cluster=hpc")
	if !strings.Contains(logs, "use step 0| ran in "+filepath.Join(runsOn(h), "1-")) {
		t.Errorf("the job on the host didn't run the checkout's script:\n%s", logs)
	}
	if _, err := os.Stat(filepath.Join(env["TMPDIR"], "app")); err == nil {
		t.Error("the checkout on the host wrote where the run started")
	}
}

// TestSSHStepRunHosts checks one job, where the run started, with steps on
// three hosts: two of their own, and one reached through another, as a jump
// host its ssh: names, which meridian dials from where the run started.
// Each runs on its host, with its home, and appends to its job's outputs.
func TestSSHStepRunHosts(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	hosts := newSSHHosts(t, env, hostSpec{alias: "east"}, hostSpec{alias: "west"}, hostSpec{alias: "login"}, hostSpec{alias: "node"})
	logs := runSucceeds(t, env, hosts["east"].setUp, `jobs:
  main:
    steps:
      - run: echo "east=$HOME" >> $OUTPUTS
        ssh: {remoteHost: east}
      - run: echo "west=$HOME" >> $OUTPUTS
        ssh: {remoteHost: west}
      - run: echo "node=$HOME" >> $OUTPUTS
        ssh: {remoteHost: node, jumpNodeHost: login}
  after:
    needs: [main]
    steps:
      - run: |
          echo "east ${{ needs.main.outputs.east }}"
          echo "west ${{ needs.main.outputs.west }}"
          echo "node ${{ needs.main.outputs.node }}"
`)
	hasLines(t, logs, "after step 0| east "+hosts["east"].home, "after step 0| west "+hosts["west"].home, "after step 0| node "+hosts["node"].home)
	if left, _ := filepath.Glob(filepath.Join(hosts["login"].home, ".local", "state", "meridian", "runs", "*")); len(left) > 0 {
		t.Errorf("a step ran on the jump host: %v", left)
	}
}

// TestSSHStepRunFailover checks a step whose ssh: names its host by its
// retry counter, whose first host refuses its connection: the first try
// fails, as format 1's does when it can't connect, with ssh_unreachable,
// which its log says, and its retry: tries the next host, where the step
// succeeds.
func TestSSHStepRunFailover(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	hosts := newSSHHosts(t, env, hostSpec{alias: "down", unreachable: true}, hostSpec{alias: "farm"})
	logs := runSucceeds(t, env, hosts["farm"].setUp, `jobs:
  main:
    steps:
      - run: echo "ran on $HOME"
        retry: {max-retries: 1, interval: 1s}
        ssh:
          remoteHost: "${{ env.PW_WORKFLOW_STEP_CURRENT_RETRY == '0' && 'down' || 'farm' }}"
`)
	if !strings.Contains(logs, "| ran on "+hosts["farm"].home+"\n") || !strings.Contains(logs, "main step 0| Meridian couldn't reach the host down over SSH") {
		t.Errorf("the step didn't fail over from the host that refused it:\n%s", logs)
	}
}

// sshWaiting is a workflow of one job, main, where the run started, whose
// step on the cluster its input names waits until a file in the host's
// home, released, is there; its step after says it ran.
const sshWaiting = `on:
  execute:
    inputs:
      cluster:
        type: compute-clusters
jobs:
  main:
    steps:
      - run: |
          echo waiting
          while [ ! -e "$HOME/released" ]; do sleep 0.05; done
        ssh:
          remoteHost: ${{ inputs.cluster.ip }}
        STOP
      - run: echo after
`

// TestSSHStepRunStopped checks that a stop of a step on a host stops its
// step run's worker there: its timeout:, which fails it, and the run's
// cancel, which cancels it; and that the placement there is forgotten.
func TestSSHStepRunStopped(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, stop string
		end        string
	}{
		// Long enough for the step run's worker to start on a loaded
		// runner, under the race detector, before the timeout stops it.
		{"its timeout", "timeout: 10s", "run 1 finished: failure\n"},
		{"the run's cancel", "", "run 1 finished: cancelled (cancelled)\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parallel(t)
			env := runEnv(t)
			h := newSSHHost(t, env)
			running := startCommand(t, env, h.setUp, "run", "--input", "cluster=hpc", writeFile(t, "w.yaml", strings.Replace(sshWaiting, "STOP", tc.stop, 1)))
			waitUntil(t, "the step to wait on the host", func() bool { return strings.Contains(running.stdout.String(), "| waiting") })
			if tc.stop == "" {
				if err := meridianProcess(t, env, "cancel", "1").cmd.Wait(); err != nil {
					t.Fatal(err)
				}
			}
			r := endedWithin(t, running)
			if !strings.HasSuffix(r.stdout, tc.end) || strings.Contains(r.stdout, "| after") {
				t.Fatalf("meridian run of a step on a host stopped = %+v", r)
			}
			waitUntil(t, "the placement to be forgotten", func() bool { return len(placementsOn(h)) == 0 })
		})
	}
}

// TestSSHStepRunEarlyCancel checks a step on a host whose early-cancel: is
// any-job-failed: once another job of the run fails, its step run is
// stopped, and the step ends cancelled, which cancels its job, as in
// format 1, whose next step is skipped.
func TestSSHStepRunEarlyCancel(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	h := newSSHHost(t, env)
	r := endedWithin(t, startCommand(t, env, h.setUp, "run", "--input", "cluster=hpc", writeFile(t, "w.yaml", `on:
  execute:
    inputs:
      cluster:
        type: compute-clusters
jobs:
  fails:
    steps:
      - run: |
          while [ ! -e "`+h.home+`/waiting" ]; do sleep 0.05; done
          exit 3
  main:
    steps:
      - run: |
          touch "$HOME/waiting"
          sleep 600
        early-cancel: any-job-failed
        ssh:
          remoteHost: ${{ inputs.cluster.ip }}
      - run: echo after
`)))
	if r.code == exitOK || !strings.Contains(r.stdout, "main step 0 › run 2 stopping (cancelled)\n") ||
		!strings.Contains(r.stdout, "main step 0 finished: cancelled\n") || !strings.Contains(r.stdout, "main step 1 finished: skipped\n") {
		t.Fatalf("meridian run of a step on a host that early-cancel: stops = %+v", r)
	}
	waitUntil(t, "the placement to be forgotten", func() bool { return len(placementsOn(h)) == 0 })
}

// TestSSHStepRunCleanup checks a step on a host whose cleanup: runs there
// too, as a step run of its own, once its job's steps have: after the
// run's cancel, which it outlives, as the job's cleanups do.
func TestSSHStepRunCleanup(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	h := newSSHHost(t, env)
	running := startCommand(t, env, h.setUp, "run", "--input", "cluster=hpc", writeFile(t, "w.yaml", `on:
  execute:
    inputs:
      cluster:
        type: compute-clusters
jobs:
  main:
    steps:
      - run: echo setting up
        cleanup: echo "tidied $PW_JOB_DIR" > "$HOME/tidied"
        ssh:
          remoteHost: ${{ inputs.cluster.ip }}
      - run: |
          echo waiting
          sleep 600
`))
	waitUntil(t, "the job's second step to start", func() bool { return strings.Contains(running.stdout.String(), "main step 1| waiting") })
	if err := meridianProcess(t, env, "cancel", "1").cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if r := endedWithin(t, running); !strings.HasSuffix(r.stdout, "run 1 finished: cancelled (cancelled)\n") {
		t.Fatalf("meridian run of a job cancelled = %+v", r)
	}
	data, err := os.ReadFile(filepath.Join(h.home, "tidied"))
	if err != nil || !strings.HasPrefix(string(data), "tidied "+filepath.Join(runsOn(h), "1-")) {
		t.Errorf("the cleanup on the host left %q, %v", data, err)
	}
}

// TestSSHStepRunPrivateHost checks a step whose ssh: names its host from a
// variable, which is private: the host is never in the clear, in what
// meridian shows, or in its state where the run started.
func TestSSHStepRunPrivateHost(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	const alias = "h1dd3n-h0st"
	hosts := newSSHHosts(t, env, hostSpec{alias: alias})
	if err := os.WriteFile(filepath.Join(env[local.EnvConfigDir], local.VariablesFile), []byte("vars:\n  HOST: "+alias+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	logs := runSucceeds(t, env, hosts[alias].setUp, `jobs:
  main:
    steps:
      - run: echo "on $HOME"
        ssh: {remoteHost: "${{ vars.HOST }}"}
`)
	if !strings.Contains(logs, "| on "+hosts[alias].home+"\n") {
		t.Errorf("the step didn't run on the host the variable names:\n%s", logs)
	}
	for _, args := range [][]string{{"logs", "1"}, {"status", "-all"}, {"status", "--format", "json", "1"}, {"status", "--format", "json", "2"}} {
		if r := commandTest(t, env, args...); strings.Contains(r.stdout+r.stderr, alias) {
			t.Errorf("meridian %s shows the host: %+v", strings.Join(args, " "), r)
		}
	}
	state := env[local.EnvStateDir]
	if err := filepath.WalkDir(state, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err == nil && bytes.Contains(data, []byte(alias)) {
			t.Errorf("%s holds the host in the clear", path)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// TestSSHStepRunCross checks two jobs, each on a host of its own, each with
// a step on the other's host: neither waits on the other, as neither host
// queues a job, and both succeed.
func TestSSHStepRunCross(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	hosts := newSSHHosts(t, env, hostSpec{alias: "east"}, hostSpec{alias: "west"})
	logs := runSucceeds(t, env, hosts["east"].setUp, `jobs:
  e:
    ssh: {remoteHost: east}
    steps:
      - run: echo "e on $HOME"
        ssh: {remoteHost: west}
  w:
    ssh: {remoteHost: west}
    steps:
      - run: echo "w on $HOME"
        ssh: {remoteHost: east}
`)
	if !strings.Contains(logs, "| e on "+hosts["west"].home+"\n") || !strings.Contains(logs, "| w on "+hosts["east"].home+"\n") {
		t.Errorf("a step didn't run on the other job's host:\n%s", logs)
	}
}

// TestSSHStepRunCallRefused checks a step that calls a workflow with an
// ssh: of its own that names a host: it fails, with step_ssh_not_supported,
// without calling the workflow, and its job's next step, whose if: reads
// failure(), runs. Format 1 runs the workflow where its caller runs, with
// the host's paths in its environment, which Meridian doesn't (docs/
// design.md, Workflows a step calls).
func TestSSHStepRunCallRefused(t *testing.T) {
	flowsRepo(t, map[string]string{"flows/called.yaml": "jobs:\n  greet:\n    steps:\n      - run: echo hello\n"})
	env := runEnv(t)
	h := newSSHHost(t, env)
	r := endedWithin(t, startCommand(t, env, h.setUp, "run", "--format", "json", writeFile(t, "w.yaml", `jobs:
  main:
    steps:
      - uses: github/acme/flows@main
        with:
          $yaml: flows/called.yaml
        ssh:
          remoteHost: farm
      - if: ${{ failure() }}
        run: echo the call was refused
`)))
	if r.code != exitInvalid {
		t.Fatalf("meridian run of a call with an ssh: of its own = %+v", r)
	}
	var refused, log, finished bool
	for _, l := range jsonLines(t, r.stdout) {
		switch step, _ := l["step"].(float64); {
		case l["kind"] == "child.created":
			t.Errorf("a run was created for the call: %v", l)
		case l["kind"] == "step.finished" && step == 0 && l["outcome"] == "failure":
			ds, _ := l["diagnostics"].([]any)
			for _, d := range ds {
				refused = refused || d.(map[string]any)["code"] == "step_ssh_not_supported"
			}
		case l["kind"] == "log" && step == 1:
			log = log || l["text"] == "the call was refused\n"
		case l["kind"] == "run.finished":
			finished = l["outcome"] == "failure"
		}
	}
	if !refused || !log || !finished {
		t.Errorf("the call's step failed with step_ssh_not_supported %v, the next step logged %v, the run failed %v:\n%s", refused, log, finished, r.stdout)
	}
}
