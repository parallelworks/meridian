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
	// Read returns the variables that names name, by their names as the
	// plan writes one: org.<name> or vars.<name>. A name can also be org
	// or vars alone, for every variable of that context. A variable that
	// isn't set is left out. When secrets is false, a secret variable is
	// given without its value: the compiler reads no secret's value when
	// the run is created, and learns only that it is a secret.
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
	// runs-on: environment: names, given as the workflow gives it,
	// evaluated: a platform's scheduler environment, by cluster and name,
	// with its scheduling parameters. Only a platform knows its
	// environments: elsewhere it returns ErrNotFound.
	Environment(ctx context.Context, environment map[string]any) (*backend.Group, error)
}

// Sources provides the workflows that a step's uses: names.
type Sources interface {
	// Resolve finds the workflow that ref, a uses: such as
	// github/org/repo/path@main, names now, pinned to the exact text it
	// names: a commit, or else the text's digest. It returns ErrNotFound
	// for a ref that names no workflow.
	Resolve(ctx context.Context, ref string) (*Workflow, error)
	// Read reads the workflow that Resolve pinned as source, so that a run
	// and its reruns run the same text however its ref has moved since.
	Read(ctx context.Context, source plan.Source) (*Workflow, error)
}

// A Workflow is a workflow file that Sources gives.
type Workflow struct {
	// Source is where it is, pinned: its revision is the commit or the
	// digest that Read reads it by.
	Source plan.Source
	// Text is the file's text.
	Text []byte
	// Remote is what its remote context holds, such as remote.repo and
	// remote.branch.
	Remote map[string]string
}

// Sessions provides the interactive sessions a workflow serves.
type Sessions interface {
	// Make makes the sessions a run serves, from the workflow's
	// sessions: as it writes them, by name, and returns what an expression
	// reads as each, sessions.<name>.
	Make(ctx context.Context, sessions map[string]any) (map[string]string, error)
}
