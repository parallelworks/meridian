package compile

import (
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// planned returns v, the value at loc in the workflow, as a plan Value, or
// reports that it nests too deeply for a plan to hold, and returns the zero
// Value.
func (c *compiler) planned(loc []string, v any) plan.Value {
	pv := plan.Literal(v)
	if planDepth(loc)+pv.Depth() > plan.MaxDepth {
		c.reportAt(problems.ValueTooDeep, loc, problems.Params{"max": plan.MaxDepth})
		return plan.Value{}
	}
	return pv
}

// planDepth returns how many levels deep a plan holds the value at loc in
// the workflow. A plan's jobs and steps hold their values where the
// workflow's do, as deep, and the run holds its own one level deeper than
// the workflow, in its run.
func planDepth(loc []string) int {
	if loc[0] == "jobs" {
		return len(loc)
	}
	return len(loc) + 1
}

// checkInputDepths reports the inputs whose values nest too deeply for a
// plan to hold. It holds each in its run, three levels deep.
func (c *compiler) checkInputDepths(r plan.Run) {
	const depth = 3
	values, _ := r.Inputs.Get().(map[string]any)
	for name, v := range jsonvalue.Sorted(values) {
		if depth+plan.DataOf(v).Depth() > plan.MaxDepth {
			c.reportAt(problems.InputTooDeep, definitionsPath, problems.Params{"input": name, "max": plan.MaxDepth})
		}
	}
}
