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
	// Variables reads the variables the plan lists, plain and secret. Nil
	// means there are none: a run that reads one can't start.
	Variables Variables
	// Passwords are the run's passwords, from its secret record, by their
	// paths in the inputs' values, as Inputs.Secrets gives them: creds.0.pass
	// for the field pass of the list creds's first row.
	Passwords map[string]any
	// Inputs provides the input types that name a platform's resources, as
	// Options.Inputs does when the run is created. Nil means there is no
	// platform.
	Inputs InputResolver
}

// StartResult is what Start gives: the run's values, complete.
type StartResult struct {
	// Run is what the phases after Start read of the run, whole: the
	// plan's input values with Start's evaluated and each password at its
	// path, the workflow's env:, every variable Start read, plain and
	// secret, and in Run.Secrets each value that is a secret or was made
	// from one.
	Run plan.RunValues
	// TimeoutSeconds is the run's timeout:, in seconds, or 0 for none.
	TimeoutSeconds int64
	// Expansions are the matrices Start expanded, those whose values are
	// known once the run starts, by job name. A job with a strategy: that
	// isn't here reads needs, and Ready expands it.
	Expansions map[string]*plan.Expansion
	// Private names what Start gives that it made from a variable or a
	// password, which the coordinator keeps only in the run's secret
	// record. It keeps the rest as run state, for whoever can see the run.
	// A name is inputs.<path> for an input's value, by its path as
	// plan.Run.DeferredInputs names it; env.<name> for a value of the
	// workflow's env:; timeout for the run's timeout:; and
	// jobs.<name>.matrix for a job's matrix values, whose instances' keys
	// are run state all the same. Run's variables and passwords are kept
	// only in the secret record, without being named here.
	Private []string
}

// Start is the Start phase: it completes what p leaves to the run's start,
// as docs/design.md describes under Starting a run. It reads the variables
// p lists, through in.Variables, once for the run; decides again each
// decision of the run's shape that read a variable, and reports
// run_shape_changed if one differs from what p records; evaluates the
// inputs p holds for Start, the workflow's env: and timeout:, and checks
// each input's value as Compile does; and expands each matrix whose values
// are known by then, with plan.Expand.
//
// The coordinator runs Start as the run's first job, and stores what it
// gives in the transaction that ends that job. Given the same plan, input
// and resolver answers, it gives the same result, byte for byte.
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
