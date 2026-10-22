package plan

import (
	"errors"

	"github.com/parallelworks/meridian/expr"
)

// errNotImplemented is what the parts of the phases that aren't built yet
// return.
var errNotImplemented = errors.New("plan: not implemented")

// notImplemented stops a caller of a part of the phases that isn't built
// yet and has no error to return, rather than give it a wrong answer.
func notImplemented(what string) {
	panic("plan: " + what + " is not implemented") //nolint:forbidigo // a stub that a track of the runner builds; its contract is its doc comment
}

// A Phase is when in a run an expression is evaluated, which decides the
// contexts it reads (docs/design.md, Phases and Contexts). Each of the five
// phases is one value but Ready and Step, which are two each: Ready
// evaluates a matrix before the matrix context is known, and Step a job's
// env: before its steps run.
type Phase int

const (
	// PhaseCreate is when the run is created (compile.Compile): inputs, org
	// and vars, with a secret variable's value not known, remote and
	// sessions.
	PhaseCreate Phase = iota
	// PhaseStart is when the run starts (compile.Start): the variables'
	// values, secret ones included, the passwords, and the workflow's env.
	PhaseStart
	// PhaseStrategy is in Ready, before a matrix is expanded: needs and the
	// status functions. A job's strategy: is evaluated here, and a matrix
	// job's if: that doesn't read matrix.
	PhaseStrategy
	// PhaseReady is the rest of Ready (Job.Ready): matrix. A job's if:,
	// runs-on:, resources:, retry:, timeout:, working-directory: and ssh:
	// are evaluated here.
	PhaseReady
	// PhaseJobEnv is in Step, before the job's steps run: job. The job's
	// env: is evaluated here, over the workflow's.
	PhaseJobEnv
	// PhaseStep is when a step is about to run (ReadyJob.Step): steps, and
	// the job's env. The step's env: is evaluated here, over the job's, and
	// then the rest of the step, over the step's.
	PhaseStep
	// PhaseEnd is when a job's steps have run (ReadyJob.End): steps, all of
	// them. The job's outputs: are evaluated here.
	PhaseEnd
)

// String returns the phase's name in a problem's params, such as
// context_not_known's phase, as the compiler has always given it:
// run_created for Create and Start, whose expressions the compiler checks
// as the run is created, strategy, job_ready and job_env, and other for
// Step and End. The names are part of the problems' contract.
func (p Phase) String() string {
	switch p {
	case PhaseCreate, PhaseStart:
		return "run_created"
	case PhaseStrategy:
		return "strategy"
	case PhaseReady:
		return "job_ready"
	case PhaseJobEnv:
		return "job_env"
	}
	return "other"
}

// RunValues are what the phases after Start read of the run as a whole:
// the plan's, completed by Start (compile.StartResult). An attempt receives
// the part its job reads when it is claimed (Job.Reads).
//
// A value is named by its path: its context, then the keys that lead to it
// from there, a list's items by their indexes, joined with dots, as
// Run.Secrets names one: inputs.creds.0.pass, vars.token, env.TOKEN. A
// context's name alone is all of it. The names of the format's inputs,
// variables and env never hold a dot.
type RunValues struct {
	// Format is the workflow's format, whose rules evaluation follows.
	Format int `json:"format"`
	// Inputs are the run's input values, complete, a mapping: the plan's,
	// with what Start evaluated and each password at its path.
	Inputs Data `json:"inputs,omitzero"`
	// Env is the workflow's env:, as Start evaluated it.
	Env map[string]string `json:"env,omitempty"`
	// Org and Vars are the organization's and the user's variables, by name,
	// as Start read them: every variable the run reads, plain and secret.
	Org  map[string]string `json:"org,omitempty"`
	Vars map[string]string `json:"vars,omitempty"`
	// Remote and Sessions are the run's remote and sessions contexts, from
	// when it was created.
	Remote   map[string]string `json:"remote,omitempty"`
	Sessions map[string]string `json:"sessions,omitempty"`
	// Private names what of the above was made from an organization or user
	// variable or a password, such as inputs.region or env.TOKEN, besides
	// what is private whole: org, vars, and the passwords, which Secrets
	// names. The coordinator keeps what is private only in the run's secret
	// record, never shows it, and keeps the rest as run state too (Split).
	Private []string `json:"private,omitempty"`
	// Secrets names each value above that is a secret or was made from one:
	// a secret variable, as in vars.token; a password, as in
	// inputs.creds.0.pass; and what Start made from a secret, such as
	// env.TOKEN. Each is private, and masked, and so is what an expression
	// makes from it.
	Secrets []string `json:"secrets,omitempty"`
}

// Split splits r into what the coordinator keeps only in the run's secret
// record, the values that are private, and what it keeps as run state, for
// whoever can see the run: the rest, with Private and Secrets, which name
// values but hold none. JoinRunValues puts them together again.
//
// It isn't built yet, and stops its caller.
func (r RunValues) Split() (record, state RunValues) {
	notImplemented("RunValues.Split")
	return
}

// JoinRunValues returns the run's values from what Split split them into.
//
// It isn't built yet, and stops its caller.
func JoinRunValues(record, state RunValues) RunValues {
	notImplemented("JoinRunValues")
	return RunValues{}
}

// Masked returns the text of each value that Secrets names, as a Masker
// takes it: a string as it is, and anything else as its JSON, with each
// string in it on its own too.
//
// It isn't built yet, and stops its caller.
func (r RunValues) Masked() []string {
	notImplemented("RunValues.Masked")
	return nil
}

// JobContext is the job context: the job instance an expression is
// evaluated for, and where its attempt was placed.
type JobContext struct {
	// Name is the job's name in the workflow.
	Name string `json:"name"`
	// Key is the instance's key, as in build#2.
	Key string `json:"key"`
	// Attempt is the attempt's number, from 1.
	Attempt int `json:"attempt"`
	// Group and Member are the execution group and the member of it that
	// the attempt was placed on.
	Group  string `json:"group,omitempty"`
	Member string `json:"member,omitempty"`
}

// Contexts are what is known when an expression is evaluated, from which
// NewScope builds what it reads. A phase reads only the fields it knows;
// a field of a later phase is ignored.
type Contexts struct {
	// Run is what the run knows as a whole. When the run is created, Inputs
	// holds the values known then, Env is empty, and Org and Vars hold the
	// plain variables only.
	Run RunValues
	// Unknown names the values that exist but aren't known yet, by their
	// paths: a secret variable's when the run is created, as in vars.token.
	// An expression that reads one is evaluated later (expr.UnknownError).
	Unknown []string
	// Private and Secrets name what this phase made, besides Run's, from a
	// variable or a password, and from a secret: a value of the job's env:
	// (env.TOKEN), or matrix for an instance's matrix values
	// (Instance.Private and Instance.Secret).
	Private, Secrets []string
	// Needs are the results of the jobs this one needs, by job name.
	Needs map[string]JobResult
	// Status is how the job or step got here, for the status functions.
	Status expr.Status
	// Matrix holds the instance's matrix values, a mapping, or nothing for
	// a job without a matrix.
	Matrix Data
	// Job is the job context.
	Job JobContext
	// JobEnv is the job's env:, and StepEnv the step's, as evaluated: env
	// reads them over the workflow's (Run.Env).
	JobEnv, StepEnv map[string]string
	// Steps are the outputs of the job's steps that have run, by step id.
	Steps map[string]map[string]string
}

// NewScope returns the scope an expression evaluated in phase reads: each
// context phase knows, in its one shape (docs/design.md, Contexts), and no
// other, so that reading a later one is an expr.UnknownError. A context
// that is known but empty is there all the same, so that a name it doesn't
// have reads as "" or null. Unknown's values are expr.Unknown.
//
// It marks two sets of values, which an evaluation reports reading apart
// (expr.Read): Scope.Marked, the secrets, which Run.Secrets and Secrets
// name; and Scope.Private, the private values, which are org and vars,
// every secret, and what Run.Private and Private name.
//
// The scope has no budget: the caller makes one for each group of
// templates (expr.Scope.Budgeted). Its Resolver and Index are the caller's
// to set, for input definitions. The compiler, the coordinator, the worker
// and the WebAssembly package all build their scopes with NewScope, so a
// context has one shape everywhere.
//
// It isn't built yet, and stops its caller.
func NewScope(phase Phase, c Contexts) expr.Scope {
	notImplemented("NewScope")
	return expr.Scope{}
}
