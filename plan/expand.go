package plan

import (
	"context"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/problems"
)

// DefaultMaxInstances is how many instances a matrix can have unless
// whoever runs the workflow sets another limit. The limit is Meridian's,
// not the format's.
const DefaultMaxInstances = 256

// An Expansion is a matrix job's instances, and how the coordinator runs
// them, from its strategy:.
type Expansion struct {
	// Instances are the job's instances, in the order of their indexes.
	Instances []Instance `json:"instances"`
	// FailFast is the strategy's fail-fast:: when an instance fails, the
	// coordinator cancels the ones that remain.
	FailFast bool `json:"failFast,omitzero"`
	// MaxParallel is the strategy's max-parallel:, how many instances the
	// coordinator places at a time, or 0 for no limit.
	MaxParallel int `json:"maxParallel,omitzero"`
}

// An Instance is one combination of a matrix's values.
type Instance struct {
	// Key is the job's name, # and Index, as in build#2.
	Key string `json:"key"`
	// Index is the instance's index, from 0.
	Index int `json:"index"`
	// Matrix holds its matrix values, by key.
	Matrix map[string]any `json:"matrix"`
	// Secret is set when evaluating the matrix read a secret: its values
	// are marked wherever its job reads them (Contexts.Secrets), and masked.
	Secret bool `json:"secret,omitzero"`
}

// An ExpandError is a matrix that can't be expanded once its values are
// known: one with no instance or more than the limit, or an include or
// exclude entry of the wrong shape. It is a problem with the job's
// strategy:, which the caller reports there.
type ExpandError struct {
	Kind   *problems.Kind
	Params problems.Params
}

func (e *ExpandError) Error() string { return e.Kind.Message(e.Params) }

// Expand evaluates job's strategy: in scope and expands its matrix into the
// job's instances, as docs/design.md describes under Matrices: its keys
// other than include and exclude, in the order of their names, give every
// combination of their values, the first key changing slowest; each
// exclude entry removes the combinations that have all its values; and
// each include entry then adds its values to every combination it matches
// without changing one, or else is a combination of its own. Each
// instance's key is the job's name, # and its index.
//
// It is the one expansion: the compiler checks a matrix with it where the
// matrix's values are known when the run is created, Start expands one
// whose values are known when the run starts, and Ready one that reads
// needs. It counts the instances before it makes them, so a matrix far
// over the limit costs no more than one at it.
//
// It returns an *ExpandError for a matrix that has no instance, more than
// maxInstances (DefaultMaxInstances when 0) or an entry of the wrong
// shape, and the error of evaluating the strategy otherwise: an
// *expr.UnknownError when it reads what scope doesn't have yet, such as
// needs when the run is created. When ctx is done before it finishes, it
// returns ctx.Err().
//
// It isn't built yet, and returns an error that says so.
func Expand(ctx context.Context, job *Job, scope expr.Scope, maxInstances int) (*Expansion, error) {
	return nil, errNotImplemented
}
