package plan_test

import (
	"reflect"
	"testing"

	"github.com/parallelworks/meridian/plan"
)

// TestStepElsewhere checks which steps the plan says can run as step runs,
// on another host than their job's, without evaluating their ssh:: a step
// with an ssh: of its own, null among them, in a job with or without one.
// One that calls a workflow, whose pin Create gives, can't
// (TestStepRunRefuses).
func TestStepElsewhere(t *testing.T) {
	p := compiled(t, `jobs:
  here:
    steps:
      - run: echo none
      - run: echo named
        ssh: {remoteHost: farm}
      - run: echo null
        ssh: null
      - run: echo read
        ssh: {remoteHost: "${{ inputs.host }}"}
      - uses: parallelworks/cancel-jobs
        ssh: {remoteHost: farm}
        with: {jobs: [hosted]}
      - uses: parallelworks/checkout
        ssh: {remoteHost: farm}
        with: {repo: https://example.com/app.git, branch: main}
  hosted:
    ssh: {remoteHost: farm}
    steps:
      - run: echo none
      - run: echo null
        ssh: null
      - run: echo named
        ssh: {remoteHost: other}
`, nil)
	for job, want := range map[string][]bool{
		"here":   {false, true, true, true, false, true},
		"hosted": {false, true, true},
	} {
		j := p.Job(job)
		for i, w := range want {
			if got := j.StepElsewhere(&j.Steps[i]); got != w {
				t.Errorf("%s's step %d: %v, want %v", job, i, got, w)
			}
		}
	}
}

// TestStepSSHReadsWorker checks what of a step's ssh: only its job's worker
// knows, which the coordinator can't evaluate it with: the job's steps,
// env but the retry counter, the job's own outputs through needs, a job of
// needs that only evaluating it names, and the job's status, which the
// status functions read.
func TestStepSSHReadsWorker(t *testing.T) {
	p := compiled(t, `on:
  execute:
    inputs:
      host: {type: string, default: farm}
      which: {type: string, default: prep}
jobs:
  prep:
    steps:
      - run: echo
  build:
    needs: [prep]
    steps:
      - run: echo inputs
        ssh: {remoteHost: "${{ inputs.host }}"}
      - run: echo needs
        ssh: {remoteHost: "${{ needs.prep.outputs.host }}"}
      - run: echo counter
        ssh: {remoteHost: "h${{ env.PW_WORKFLOW_STEP_CURRENT_RETRY }}"}
      - run: echo own
        ssh: {remoteHost: "${{ needs.build.outputs.host }}"}
      - run: echo index
        ssh: {remoteHost: "${{ join(needs.*.outputs.host, '') }}"}
      - run: echo env
        ssh: {remoteHost: "${{ env.HOST }}"}
      - run: echo literal
        ssh: {remoteHost: "${{ needs['prep'].outputs.host }}"}
      - run: echo status
        if: always()
        ssh: {remoteHost: "${{ failure() && 'b' || 'a' }}"}
`, nil)
	j := p.Job("build")
	for i, want := range []string{"", "", "", "needs.build", "needs[]", "env.HOST", "", "job.status"} {
		if got := j.StepSSHReadsWorker(&j.Steps[i]); got != want {
			t.Errorf("step %d's ssh: reads %q of the worker's, want %q", i, got, want)
		}
	}
}

// TestStepTarget checks the coordinator's evaluation of a step's ssh: for a
// try: the retry counter only for a step with a retry:, as its worker
// defines it; a host that a private matrix instance names is private; and
// one that names no host is none.
func TestStepTarget(t *testing.T) {
	p := compiled(t, `jobs:
  build:
    strategy:
      matrix: {host: [a]}
    steps:
      - run: echo retried
        retry: {max-retries: 2}
        ssh: {remoteHost: "h${{ env.PW_WORKFLOW_STEP_CURRENT_RETRY }}"}
      - run: echo once
        ssh: {remoteHost: "h${{ env.PW_WORKFLOW_STEP_CURRENT_RETRY }}"}
      - run: echo matrix
        ssh: {remoteHost: "${{ matrix.host }}"}
      - run: echo none
        ssh: {remoteHost: ""}
`, nil)
	in := plan.ReadyInput{Instance: &plan.Instance{Key: "build#0", Matrix: plan.DataOf(map[string]any{"host": "a"}), Private: true}}
	host := func(index, try int) (string, bool) {
		t.Helper()
		target, private, diags, err := p.StepTarget(t.Context(), "build", index, try, in)
		if err != nil || diags.HasErrors() {
			t.Fatalf("step %d: %v, %v", index, diags, err)
		}
		if target == nil {
			return "", private
		}
		return target.SSH.Get().(map[string]any)["remoteHost"].(string), private
	}
	if h, _ := host(0, 1); h != "h1" {
		t.Errorf("a step with a retry: is on %q at its second try, want h1", h)
	}
	if h, _ := host(1, 0); h != "h" {
		t.Errorf("a step without a retry: is on %q, want h", h)
	}
	if h, private := host(2, 0); h != "a" || !private {
		t.Errorf("a private instance's host is %q, private %v", h, private)
	}
	if target, _, _, err := p.StepTarget(t.Context(), "build", 3, 0, in); err != nil || target != nil {
		t.Errorf("a step whose ssh: names no host is on %+v, %v", target, err)
	}
}

// TestReadsSteps checks what of its job's steps' results a step run is
// given, its job being the step and the steps before it by their id: each
// outcome, and of the outputs only those it reads, a step's with an id by
// name, or every one's where a * names it, or through needs, and the
// job's own, those written without an id and by cleanups, through needs.
func TestReadsSteps(t *testing.T) {
	p := compiled(t, `jobs:
  build:
    steps:
      - id: a
        run: echo a
      - id: b
        run: echo b
      - run: echo c
      - run: echo ${{ steps.a.outputs.x }} ${{ steps.*.outputs.y }} ${{ needs.build.outputs.z }} ${{ needs.build.steps.b.outputs.w }}
        ssh: {remoteHost: farm}
`, nil)
	j := *p.Job("build")
	j.Steps = []plan.Step{{ID: "a"}, {ID: "b"}, {}, j.Steps[3]}
	all := map[string]string{"x": "1", "y": "2", "z": "3", "w": "4"}
	got := j.ReadsSteps([]plan.StepResult{
		{Outcome: plan.OutcomeSuccess, Outputs: all},
		{Outcome: plan.OutcomeFailure, Outputs: all, CleanupOutputs: all},
		{Outcome: plan.OutcomeSkipped, Outputs: all},
	})
	want := []plan.StepResult{
		{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": "1", "y": "2"}},
		{Outcome: plan.OutcomeFailure, Outputs: map[string]string{"y": "2", "w": "4"}, CleanupOutputs: map[string]string{"z": "3"}},
		{Outcome: plan.OutcomeSkipped, Outputs: map[string]string{"z": "3"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("a step run is given %+v, want %+v", got, want)
	}
}
