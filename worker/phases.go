package worker

import (
	"context"
	"io"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// phases are what the worker evaluates its job with: package plan's Step
// and End phases, the values a run's attempt masks, and the masker. Run
// uses plan's own (planPhases); the worker's tests give their own, so that
// they drive whole attempts whatever of plan is built.
type phases struct {
	step   func(*plan.ReadyJob, context.Context, int, plan.JobState) (*plan.ReadyStep, problems.Diagnostics, error)
	end    func(*plan.ReadyJob, context.Context, plan.JobState) (map[string]string, problems.Diagnostics, error)
	masked func(plan.RunValues) []string
	masker func(values ...string) masker
}

// A masker replaces secrets in what a job gives with problems.Masked, as a
// plan.Masker does: in its logs, as they are written, and in its outputs.
type masker interface {
	Add(values ...string)
	Mask(s string) string
	Writer(w io.Writer) io.WriteCloser
}

var planPhases = phases{
	step:   (*plan.ReadyJob).Step,
	end:    (*plan.ReadyJob).End,
	masked: plan.RunValues.Masked,
	masker: func(values ...string) masker { return plan.NewMasker(values...) },
}
