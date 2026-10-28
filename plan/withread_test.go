package plan_test

import (
	"testing"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
)

// TestStepWithRead checks that Step says what evaluating a uses: step's
// with: read, which the coordinator passes on to what does the action, as
// update-session's session provider: nothing, for a with: written out; a
// variable, which makes what it gives private; and a secret, which makes
// it masked too.
func TestStepWithRead(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    steps:
      - uses: parallelworks/update-session
        with:
          name: plain
      - uses: parallelworks/update-session
        with:
          name: ${{ vars.region }}
      - uses: parallelworks/update-session
        with:
          name: plain
          apiKey: ${{ vars.token }}
`, map[string]any{"vars": map[string]any{"token": expr.Unknown{Name: "vars"}, "region": "eu"}})
	run := runValues(t, p, map[string]string{"token": "s3cret", "region": "eu"})
	ready, _, err := p.Ready(t.Context(), "main", plan.ReadyInput{Run: run})
	if err != nil || ready.Job == nil {
		t.Fatal(err)
	}
	state := plan.JobState{Run: job(t, p, "main").Reads(run), Job: plan.JobContext{Name: "main", Key: "main", Attempt: 1}}
	for i, want := range []expr.Read{{}, {Private: true}, {Private: true, Marked: true}} {
		s, diags, err := ready.Job.Step(t.Context(), i, state)
		if err != nil || len(diags) > 0 {
			t.Fatal(diags, err)
		}
		if s.WithRead != want {
			t.Errorf("step %d's with: read %+v, want %+v", i, s.WithRead, want)
		}
		state.Steps = append(state.Steps, plan.StepResult{Outcome: plan.OutcomeSuccess})
	}
}
