package compile

import (
	"context"
	"errors"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/plan"
)

// ErrNotFound is what a resolver returns for a name it doesn't have.
var ErrNotFound = errors.New("compile: not found")

// Variables provides organization and user variables, plain and secret, by
// name.
type Variables interface {
	// Read returns the variables that names name, each keyed by its name
	// as the plan writes one, org.<name> or vars.<name>. A name can also
	// be org or vars alone, for every variable of that context, each keyed
	// the same way. A variable that isn't set is left out. When secrets is
	// false, a secret variable is given without its value: the compiler
	// reads no secret's value when the run is created, and learns only
	// that it is a secret.
	Read(ctx context.Context, names []string, secrets bool) (map[string]Variable, error)
}

// A Variable is an organization or user variable.
type Variable struct {
	Value  string `json:"value"`
	Secret bool   `json:"secret,omitzero"`
}

// Groups provides execution groups.
type Groups interface {
	// Group returns the execution group named name, or ErrNotFound.
	Group(ctx context.Context, name string) (*backend.Group, error)
	// Environment returns the inline group with one member that a format 1
	// runs-on: environment: names: a platform's scheduler environment. Only
	// a platform knows its environments: elsewhere it returns ErrNotFound.
	Environment(ctx context.Context, environment plan.Environment) (*backend.Group, error)
	// ComputeTarget returns the inline group with one member that a format
	// 1 runs-on: gives from a compute-target input. Only a platform knows
	// its compute targets: elsewhere it returns ErrNotFound.
	ComputeTarget(ctx context.Context, target plan.ComputeTarget) (*backend.Group, error)
}

// Sources provides the workflows that a step's uses: names.
type Sources interface {
	// Resolve finds the workflow that ref, a uses: such as
	// github/org/repo/path@main, names now, and pins it to the exact text
	// it names: its Source's Revision is a commit, or the text's digest
	// when no commit has it, and its Digest the text's digest. It returns
	// ErrNotFound for a ref that names no workflow.
	Resolve(ctx context.Context, ref string) (*Workflow, error)
	// Read reads the workflow that Resolve pinned as source, so that a run
	// and its reruns run the same text however its ref has moved since. It
	// fails if the text it reads isn't the digest's.
	Read(ctx context.Context, source plan.Source) (*Workflow, error)
}

// A Workflow is a workflow file that Sources gives.
type Workflow struct {
	// Source is where it is, pinned by its Revision and its Digest.
	Source plan.Source
	// Text is the file's text.
	Text []byte
	// Remote is what its remote context holds, such as remote.repo and
	// remote.branch.
	Remote map[string]string
}

// Sessions provides the interactive sessions a workflow serves.
type Sessions interface {
	// Check checks the workflow's sessions:, given as it writes them, when
	// the run is created, and returns what an expression reads as each,
	// sessions.<name>. It makes none: the run may never start.
	Check(ctx context.Context, sessions map[string]any) (map[string]string, error)
	// Make makes the sessions of the run runID as it starts, from the
	// workflow's sessions: and what Check gave for them. Making them again
	// for the same run, as a coordinator that starts the run again after
	// another stopped does, makes nothing more.
	Make(ctx context.Context, runID string, sessions map[string]any, names map[string]string) error
}
