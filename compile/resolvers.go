package compile

import (
	"context"
	"errors"
	"maps"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// ErrNotFound is what a resolver returns for a name it doesn't have.
var ErrNotFound = errors.New("compile: not found")

// workflowProblem returns the kind and a copy of the params of err, an
// error from a resolver, when it is, or wraps, a problem of one of the
// problems package's kinds of error, foundation's shared rules among them,
// but not the command's or the worker API's: what is wrong with what the
// resolver was asked, which a run reports as it is, so that each reader
// sees it in their own language. ok is false for any other error, a
// warning's kind among them, which would let what is wrong through, or the
// command's or the worker API's own.
func workflowProblem(err error) (k *problems.Kind, params problems.Params, ok bool) {
	p, isProblem := errors.AsType[*problem.Problem](err)
	if !isProblem {
		return nil, nil, false
	}
	_, command := problems.CommandRegistry.Lookup(p.Code)
	_, worker := problems.WorkerRegistry.Lookup(p.Code)
	k, ok = problems.Lookup(p.Code)
	if !ok || command || worker || k.Severity() != problems.Error {
		return nil, nil, false
	}
	return k, maps.Clone(p.Params), true
}

// Variables provides organization and user variables, plain and secret, by
// name.
type Variables interface {
	// Read returns the variables that names name, each keyed by its name
	// as it names it: org.<name> or vars.<name>, the context, a dot and the
	// variable's name as it is, which can hold dots of its own, as in
	// vars.my.token, so the context is what is before the first dot. (A
	// plan writes the same name vars.my~1token, plan.JoinPath, which Start
	// reads with plan.SplitPath before it asks.) A name can also be org or
	// vars alone, for every variable of that context, each keyed the same
	// way. A variable that isn't set is left out. When secrets is false, a
	// secret variable is given without its value: the compiler reads no
	// secret's value when the run is created, and learns only that it is a
	// secret.
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

// Sources provides the workflows that a step's uses: names, and the Git
// repositories that a parallelworks/checkout step checks out.
type Sources interface {
	// Resolve finds the workflow that ref names now: a uses: such as
	// github/org/repo@main, with the file its with: $yaml names in the
	// repository and the server its $host names, as format 1 writes them.
	// It pins it to the exact text it names: its Source's Revision is a
	// commit, or the text's digest when no commit has it, its Digest the
	// text's digest, and its Ref ref. It returns ErrNotFound, or a problem
	// that wraps it, for a ref that names no workflow.
	Resolve(ctx context.Context, ref plan.SourceRef) (*Workflow, error)
	// Read reads the workflow that Resolve pinned as source, by its Ref at
	// its Revision, so that a run and its reruns run the same text however
	// its ref has moved since. It fails if the text it reads isn't the
	// digest's. It gives the Workflow's Remote as Resolve gave it.
	Read(ctx context.Context, source plan.Source) (*Workflow, error)
	// Pin finds the commit that revision, a branch, a tag or a full
	// commit, names now in the Git repository repo, as a
	// parallelworks/checkout step's with: writes them out, repo one that
	// plan.CheckRepo takes, and returns it, in full, as git writes it, so
	// that the run and its reruns check out the same tree. It returns
	// ErrNotFound, or a problem that wraps it, for a revision that names
	// no commit there.
	Pin(ctx context.Context, repo, revision string) (string, error)
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
	// workflow's sessions:, evaluated with the run's values as the phases
	// after Start read them, and the names Check gave them. read says what
	// evaluating them read: if it read a variable or a password
	// (read.Private), what they give is private, to be kept out of what
	// everyone who can see the run sees, as its private values are; and if
	// it read a secret (read.Marked), it is to be masked as what is made
	// from one is, with the run's masked values (plan.RunValues.Masked).
	// Where read.Private is set, Make mustn't log or quote what they give,
	// in an error among the rest: it can be private though the masked
	// values don't hold it, as a redirect: a password decided is. Start
	// masks a refusal's params then, and returns, for any other error, one
	// that says only that the provider couldn't make them.
	// Making them again for a run that has them, as a coordinator that
	// starts the run again after another stopped does, makes nothing more;
	// a rerun, whose sessions ended with the run, makes them again.
	Make(ctx context.Context, runID string, sessions map[string]any, names map[string]string, read expr.Read) error
	// Update updates the session u.Name with u.With, as a step of the run
	// u.Run that uses parallelworks/update-session asks. Meridian doesn't
	// restrict which session a step names: Update decides whether the run
	// may update it, as format 1's platform decides whether the run's
	// token may, from who runs it and its workflow's permissions:
	// (u.Permissions).
	//
	//   - A run whose permissions: hold '*' acts as the person who runs it,
	//     and may update any session they may: one of theirs, or one
	//     shared with them.
	//   - Any other may update only a session of its own tree, one that a
	//     run of the tree whose root is u.Root made (Make), as format 1
	//     makes a tree's sessions with its root's run, and only one of the
	//     person who runs it.
	//
	// A session the run may not update, or that isn't there, is a refusal:
	// a problem of one of a workflow's kinds of error, which fails the
	// step. Update mustn't log or quote what u.With gives where
	// u.Read.Private is set, and masks it as one of the run's secrets where
	// u.Read.Marked is. Any other error says it couldn't answer, and the
	// step asks again. Updating a session again with the same, as a worker
	// that repeats its request does, changes nothing more (UpdateSession).
	Update(ctx context.Context, u SessionUpdate) error
}

// A SessionUpdate is what a step that uses parallelworks/update-session
// asks of a session provider (Sessions.Update).
type SessionUpdate struct {
	// Run is the ID of the run whose step asks, and Root the ID of the run
	// at the top of its tree, which no step called: Run's own for a run no
	// step called.
	Run, Root string
	// Permissions are the run's workflow's permissions:
	// (plan.Run.Permissions), which say what its token may do: '*' all
	// that the person who runs it may.
	Permissions []string
	// Name is the session's name, as the step's with: gives it, read as
	// text: as sessions.<key> reads one of a run's sessions (Check), or
	// any other.
	Name string
	// With is the rest of the step's with:, as it was evaluated: where the
	// session points, as url, or remoteHost and remotePort.
	With map[string]any
	// Read says what evaluating with: read, as for Make.
	Read expr.Read
}

// UpdateSession does what a step that uses parallelworks/update-session
// asks, through s, which is nil where nothing provides sessions: u is
// what it asks, but for its Name and With, which UpdateSession takes from
// with, the step's with: as it was evaluated, its name, read as text, and
// the rest; and path is the step's in the workflow, at which its problems
// are. It returns them: none when the session was updated;
// resolver_missing where s is nil, as a feature whose interface is
// missing fails, but only as the step runs, which an if: may skip; and a
// refusal of s's, as Update gives it, as of a session the run may not
// update, with its params masked where with: is private: where it read a
// variable or a password (u.Read.Private), or a secret (u.Read.Marked),
// which a secret variable is too. It returns s's other errors, for the
// caller to ask again: where with: is private, one that says only that s
// couldn't update the session.
func UpdateSession(ctx context.Context, s Sessions, u SessionUpdate, path string, with map[string]any) (problems.Diagnostics, error) {
	private := u.Read.Private || u.Read.Marked
	at := func(k *problems.Kind, pointer string, params problems.Params) problems.Diagnostics {
		if private {
			params = mask(params)
		}
		return problems.Diagnostics{k.At("", problems.Position{}, pointer, params)}
	}
	if s == nil {
		return problems.Diagnostics{problems.ResolverMissing.At("", problems.Position{}, path+"/uses", problems.Params{"key": "uses", "interface": "Sessions"})}, nil
	}
	u.Name = expr.Text(with["name"])
	u.With = maps.Clone(with)
	delete(u.With, "name")
	err := s.Update(ctx, u)
	switch {
	case err == nil:
		return nil, nil
	case ctx.Err() != nil:
		return nil, ctx.Err()
	}
	if k, params, ok := workflowProblem(err); ok {
		return at(k, path, params), nil
	}
	if private {
		return nil, &sessionNotUpdated{ctx: contextEnd(err)}
	}
	return nil, err
}

// sessionNotUpdated is the error of a session provider that couldn't
// update a session as a with: that read a variable or a password asked. It
// says nothing of the provider's error, which can quote what with: gave,
// and wraps only the context's error, where the context was done.
type sessionNotUpdated struct{ ctx error }

func (e *sessionNotUpdated) Error() string {
	return "compile: the session provider couldn't update the session"
}

func (e *sessionNotUpdated) Unwrap() error { return e.ctx }
