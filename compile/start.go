package compile

import (
	"context"
	"errors"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// errNotImplemented is what Start returns until it is built.
var errNotImplemented = errors.New("compile: not implemented")

// StartInput is what Start reads besides the plan.
type StartInput struct {
	// RunID is the run's ID, which its sessions are made for.
	RunID string
	// Variables reads the variables the plan lists, plain and secret. Nil
	// means there are none: a run that reads one can't start.
	Variables Variables
	// Passwords are the run's passwords, from its secret record, by their
	// paths, as plan.Run.Secrets names them: inputs.creds.0.pass for the
	// field pass of the list creds's first row. (Inputs.Secrets gives them
	// by their paths in the inputs' values, creds.0.pass: whoever keeps
	// them in the secret record names them as the plan does.)
	Passwords map[string]any
	// Private are the run's private values, from its secret record, by
	// their paths, as plan.Run.Private names them: values the run was given
	// that the plan doesn't hold, though they aren't secrets
	// (Inputs.Private). Start puts each in the inputs' values at its path,
	// and names it among the private values it gives (plan.RunValues).
	Private map[string]any
	// Inputs provides the input types that name a platform's resources, as
	// Options.Inputs does when the run is created. Nil means there is no
	// platform.
	Inputs InputResolver
	// Sessions makes the run's sessions. Nil means there is no session
	// provider: a run with sessions can't start.
	Sessions Sessions
}

// StartResult is what Start gives: the run's values, complete.
type StartResult struct {
	// Run is what the phases after Start read of the run, whole: the plan's
	// input values with Start's evaluated and each password at its path,
	// the workflow's env:, every variable Start read, plain and secret, and
	// in Run.Private and Run.Secrets what of it is private, and secret. The
	// coordinator keeps the private part only in the run's secret record,
	// and the rest as run state too (plan.RunValues.Split).
	Run plan.RunValues
	// TimeoutSeconds is the run's timeout:, in seconds, or 0 for none.
	// TimeoutPrivate is set when evaluating it read a variable or a
	// password: the coordinator keeps it only in the secret record.
	TimeoutSeconds int64
	TimeoutPrivate bool
	// Expansions are the matrices Start expanded, those whose values are
	// known once the run starts, by job name. A job with a strategy: that
	// isn't here reads needs, and Ready expands it.
	Expansions map[string]*plan.Expansion
}

// Start is the Start phase: it completes what p leaves to the run's start,
// as docs/design.md describes under Starting a run. It reads the variables
// p lists, through in.Variables, once for the run; decides again each
// decision of the run's shape that read a variable, and reports
// run_shape_changed if one differs from what p records; evaluates the
// inputs p holds for Start, the workflow's env: and timeout:, and checks
// each input's value as Compile does; expands each matrix whose values are
// known by then, with plan.Expand and p's Run.Limits. Only once all of that
// has passed does it make the run's sessions, through in.Sessions, so a
// run that fails to start makes none. A rerun calls Start again, with the
// same plan, and reads every variable again.
//
// The coordinator runs Start as the run's first job, and stores what it
// gives in the transaction that ends that job. Given the same plan, input
// and resolver answers, it gives the same result, byte for byte, so a
// coordinator that dies during Start leaves another to start it again.
//
// Its diagnostics are the run's problems, which never show a value made
// from a variable or a password; an error among them fails the run, and
// then there is no result. It returns an error, and no result, when it
// couldn't finish for a reason that isn't the run's: a resolver that
// couldn't answer, or ctx done, with ctx.Err(). The coordinator starts the
// run again then.
//
// It isn't built yet, and returns an error that says so.
func Start(ctx context.Context, p *plan.Plan, in StartInput) (*StartResult, problems.Diagnostics, error) {
	return nil, nil, errNotImplemented
}
