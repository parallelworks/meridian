package plan_test

import "testing"

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
		"here":   {false, true, true, true},
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
