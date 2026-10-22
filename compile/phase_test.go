package compile

import (
	"testing"

	"github.com/parallelworks/meridian/plan"
)

// TestPhasesArePlans checks that each of the compiler's phases names
// itself in a problem's params as the plan's phase at the same point does,
// so that the compiler can check its expressions with plan.Phase and no
// message changes.
func TestPhasesArePlans(t *testing.T) {
	for p, want := range map[phase]plan.Phase{
		runCreated: plan.PhaseCreate,
		needsDone:  plan.PhaseStrategy,
		jobReady:   plan.PhaseReady,
		jobEnv:     plan.PhaseJobEnv,
		stepRuns:   plan.PhaseStep,
		jobEnds:    plan.PhaseEnd,
	} {
		if p.name() != want.String() {
			t.Errorf("phase %d is %q, and plan.Phase %d %q", p, p.name(), want, want.String())
		}
	}
}
