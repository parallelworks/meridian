package compile_test

import (
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// The compiler checks what each expression reads with the plan's one table
// of what each phase knows (plan.Phase.Knows and KnowsStatus): reading a
// context, or calling a status function, in a value evaluated before it is
// known is an error that names the phase, and reading one that is known
// isn't. What it checks of the run as a whole is Create's, which reads no
// env.
func TestChecksReadWhatPhasesKnow(t *testing.T) {
	places := map[string]plan.Phase{
		"/env/X":                    plan.PhaseCreate,
		"/timeout":                  plan.PhaseCreate,
		"/jobs/b/needs":             plan.PhaseCreate,
		"/jobs/b/strategy/matrix/x": plan.PhaseStrategy,
		"/jobs/b/if":                plan.PhaseReady,
		"/jobs/b/timeout":           plan.PhaseReady,
		"/jobs/b/env/X":             plan.PhaseJobEnv,
		"/jobs/b/steps/0/run":       plan.PhaseStep,
		"/jobs/b/outputs/o":         plan.PhaseEnd,
	}
	workflow := func(e string) string {
		return strings.NewReplacer("E", "'"+e+"'").Replace(`env:
  X: E
timeout: E
jobs:
  a:
    steps:
      - run: echo
  b:
    needs: E
    strategy:
      matrix:
        x: E
    if: E
    timeout: E
    env:
      X: E
    steps:
      - id: s
        run: E
    outputs:
      o: E
`)
	}
	for context, e := range map[string]string{
		"inputs": "${{ inputs.x }}", "org": "${{ org.x }}", "vars": "${{ vars.x }}", "remote": "${{ remote.x }}",
		"sessions": "${{ sessions.x }}", "env": "${{ env.X }}", "needs": "${{ needs.a.outputs.o }}",
		"matrix": "${{ matrix.x }}", "job": "${{ job.name }}", "steps": "${{ steps.s.outputs.o }}",
		"status": "${{ success() }}",
	} {
		_, diags := compile.Compile("workflow.yaml", []byte(workflow(e)), nil, compile.Options{})
		reported := map[string]problems.Diagnostic{}
		for _, d := range diags {
			if d.Code == problems.ContextNotKnown.Code() || d.Code == problems.StatusNotKnown.Code() {
				reported[d.Path()] = d
			}
		}
		for path, phase := range places {
			knows := phase.Knows(context)
			if context == "status" {
				knows = phase.KnowsStatus()
			}
			d, ok := reported[path]
			switch {
			case knows && ok:
				t.Errorf("%s at %s, in %v (%d): %s", e, path, phase, phase, d)
			case !knows && !ok:
				t.Errorf("%s at %s, in %v (%d), isn't reported", e, path, phase, phase)
			case !knows && d.Params["phase"] != phase.String():
				t.Errorf("%s at %s: %s, want the phase %s", e, path, d, phase)
			}
		}
	}
}
