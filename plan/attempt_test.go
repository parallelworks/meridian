package plan_test

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// A phaseRun is what a phase of an attempt was given and gave.
type phaseRun struct {
	name   string
	state  plan.JobState
	result any
	diags  problems.Diagnostics
}

// runAttempt runs the phases of an attempt of rj as the worker does:
// JobEnv, each step, which writes outputs[i], the steps' cleanups, in
// reverse, each of which writes cleanups[i], and End. Each phase after
// JobEnv is given the job's env: as JobEnv gave it if shared is set, as
// the worker gives it, and otherwise evaluates it again itself.
func runAttempt(t *testing.T, rj *plan.ReadyJob, run plan.RunValues, outputs, cleanups []map[string]string, shared bool) []phaseRun {
	t.Helper()
	ctx := t.Context()
	state := plan.JobState{Run: run, Masked: run.Masked()}
	env, diags, err := rj.JobEnv(ctx, state)
	if err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	if shared {
		state.JobEnv = env
	}
	masked := append(slices.Clone(state.Masked), env.Masked...)
	var runs []phaseRun
	add := func(name string, state plan.JobState, result any, diags problems.Diagnostics, err error) {
		if err != nil {
			t.Fatal(name, err)
		}
		runs = append(runs, phaseRun{name: name, state: state, result: result, diags: diags})
	}
	results := make([]plan.StepResult, 0, len(rj.Steps))
	for i := range rj.Steps {
		state.Steps, state.Masked = slices.Clip(results), slices.Clip(masked)
		rs, diags, err := rj.Step(ctx, i, state)
		add(fmt.Sprintf("step %d", i), state, rs, diags, err)
		if rs != nil {
			masked = append(masked, rs.Masked...)
		}
		results = append(results, plan.StepResult{Outcome: plan.OutcomeSuccess, Outputs: outputs[i]})
	}
	for i := len(rj.Steps) - 1; i >= 0; i-- {
		if rj.Steps[i].Cleanup.IsZero() {
			continue
		}
		state.Steps, state.Masked = slices.Clone(results), slices.Clip(masked)
		rs, diags, err := rj.Cleanup(ctx, i, state)
		add(fmt.Sprintf("cleanup %d", i), state, rs, diags, err)
		if rs != nil {
			masked = append(masked, rs.Masked...)
		}
		results[i].CleanupOutcome, results[i].CleanupOutputs = plan.OutcomeSuccess, cleanups[i]
	}
	state.Steps, state.Masked = results, slices.Clip(masked)
	end, diags, err := rj.End(ctx, state)
	add("end", state, end, diags, err)
	return runs
}

// The phases of an attempt that share what JobEnv found, as the worker has
// them, give what each gives alone, when it evaluates the job's env:
// itself, byte for byte, whether they run one at a time or all at once: a
// step's output that holds a secret, or what a later step made from one, is
// told as it is told alone.
func TestAttemptSharedAsAlone(t *testing.T) {
	token := "tok_" + strings.Repeat("W", 36)
	rj, run := readyMain(t, `
jobs:
  main:
    env:
      TOKEN: ${{ vars.token }}
    steps:
      - id: a
        run: ./a.sh
        cleanup: ./undo-a.sh
      - run: ./b.sh
      - id: c
        run: echo ${{ vars.token == 'x' && 'never' || 'made-later-from-it' }}
      - id: d
        run: ./d.sh ${{ steps.a.outputs.out }} ${{ needs.main.outputs.out }}
        cleanup: ./undo-d.sh ${{ steps.a.outputs.out }}
      - run: ./e.sh --token ${{ env.TOKEN }}
      - id: f
        run: ./f.sh ${{ fromJSON(steps.d.outputs.json).k }} ${{ fromJSON(steps.a.outputs.out) }}
    outputs:
      a: ${{ steps.a.outputs.out }}
      job: ${{ needs.main.outputs.out }}
      undone: ${{ needs.main.outputs.undone }}
`, map[string]string{"token": token}, "token")
	outputs := []map[string]string{
		{"out": "a wrote made-later-from-it", "plain": "nothing"},
		{"out": "b wrote " + token},
		{"out": "c wrote ***"},
		{"out": "d wrote nothing secret", "json": `{"k": "v"}`},
		{"out": "e wrote nothing secret"},
		{"out": "f wrote " + token[:12]},
	}
	cleanups := []map[string]string{{"undone": "a undid " + token}, nil, nil, {"undone": "d undid it"}, nil, nil}
	alone := runAttempt(t, rj, run, outputs, cleanups, false)
	shared := runAttempt(t, rj, run, outputs, cleanups, true)
	if len(alone) != len(shared) {
		t.Fatalf("%d phases alone, %d shared", len(alone), len(shared))
	}
	for i := range alone {
		if !reflect.DeepEqual(alone[i].result, shared[i].result) || !reflect.DeepEqual(alone[i].diags, shared[i].diags) {
			t.Errorf("%s: alone %+v, %v; shared %+v, %v", alone[i].name, alone[i].result, alone[i].diags, shared[i].result, shared[i].diags)
		}
	}
	// a's output holds what c made from the secret, which f reads in a
	// problem, and a's cleanup wrote the secret.
	end, ok := shared[len(shared)-1].result.(*plan.EndResult)
	if !ok || fmt.Sprint(end.Secrets, end.SecretSteps) != "[a undone] map[a:[out]]" {
		t.Errorf("End: %+v", end)
	}
	if f := shared[len(rj.Steps)-1]; f.result != (*plan.ReadyStep)(nil) || len(f.diags) != 1 || strings.Contains(fmt.Sprint(f.diags[0].Params), "made-later") {
		t.Errorf("%s: %+v, %v", f.name, f.result, f.diags)
	}

	// All at once, with a JobEnv of their own to share.
	env, _, err := rj.JobEnv(t.Context(), plan.JobState{Run: run, Masked: run.Masked()})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	got := make([]any, len(shared))
	gotDiags := make([]problems.Diagnostics, len(shared))
	for range 4 {
		for i, p := range shared {
			state := p.state
			state.JobEnv = env
			wg.Go(func() {
				switch {
				case strings.HasPrefix(p.name, "step "):
					var index int
					_, _ = fmt.Sscanf(p.name, "step %d", &index)
					got[i], gotDiags[i], _ = rj.Step(t.Context(), index, state)
				case strings.HasPrefix(p.name, "cleanup "):
					var index int
					_, _ = fmt.Sscanf(p.name, "cleanup %d", &index)
					got[i], gotDiags[i], _ = rj.Cleanup(t.Context(), index, state)
				default:
					got[i], gotDiags[i], _ = rj.End(t.Context(), state)
				}
			})
		}
		wg.Wait()
		for i, p := range shared {
			if !reflect.DeepEqual(got[i], p.result) || !reflect.DeepEqual(gotDiags[i], p.diags) {
				t.Errorf("%s at once: %+v, %v; one at a time %+v, %v", p.name, got[i], gotDiags[i], p.result, p.diags)
			}
		}
	}
}
