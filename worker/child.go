package worker

import (
	"maps"
	"slices"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// childRequest returns what the worker asks for the child run of the try
// try of the step rs, whose uses: names a workflow, with: its with: and
// what of it is private, as Step gave them; its environment, Step's
// (plan.ReadyStep.Env), with vars over it, the variables the worker
// defines for the step (variables), but only those format 1 defines
// (plan.IsFormatVariable), and never the step's process's own, which holds
// the platform's credential, PW_API_KEY, which is no such variable; and
// masked, the strings the worker masks for the step, which the child's
// jobs mask too. As in format 1, which hands a called workflow its calling
// step's whole environment, the step's own variables are among them, as
// OUTPUTS and its retry counters: the child's worker sets its own over
// them for each step of the child's, but the counters of a step without a
// retry:, which read its caller's, as format 1's do.
func childRequest(rs *plan.ReadyStep, try int, vars map[string]string, masked []string) *workerapi.ChildRequest {
	env := maps.Clone(rs.Env)
	for name, v := range vars {
		if plan.IsFormatVariable(name) {
			if env == nil {
				env = map[string]string{}
			}
			env[name] = v
		}
	}
	masked = slices.Compact(slices.Sorted(slices.Values(masked)))
	if len(masked) > 0 && masked[0] == "" {
		masked = masked[1:]
	}
	return &workerapi.ChildRequest{
		Step: rs.Index, Try: try, With: rs.With, Env: env,
		WithPrivate: rs.WithPrivate, WithSecrets: rs.WithSecrets, EnvPrivate: rs.EnvPrivate, EnvSecrets: rs.EnvSecrets,
		Masked: masked, WorkingDirectory: rs.WorkingDirectory,
	}
}
