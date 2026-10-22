package plan

import (
	"errors"

	"github.com/parallelworks/meridian/expr"
)

// errNotImplemented is what the phases that aren't built yet return.
var errNotImplemented = errors.New("plan: not implemented")

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
	// status functions. A job's strategy: is evaluated here.
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

// RunValues are what the phases after Start read of the run as a whole:
// the plan's, completed by Start (compile.StartResult). They hold
// variables' values and passwords, which the coordinator keeps only in the
// run's secret record, and an attempt receives the part its job reads when
// it is claimed.
type RunValues struct {
	// Format is the workflow's format, whose rules evaluation follows.
	Format int `json:"format"`
	// Inputs are the run's input values, complete: the plan's, with what
	// Start evaluated and each password at its path.
	Inputs map[string]any `json:"inputs,omitempty"`
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
	// Secrets names each value above that is a secret or was made from one,
	// by its path from its context, as Run.Secrets names them: vars.token,
	// inputs.creds.0.pass, and what Start made from a secret, such as
	// env.TOKEN or inputs.deploy. Each is marked in a phase's scope, so what
	// an expression makes from it is masked too.
	Secrets []string `json:"secrets,omitempty"`
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
	// paths, as Run.Secrets names them: a secret variable's when the run is
	// created, as in vars.token. An expression that reads one is evaluated
	// later (expr.UnknownError).
	Unknown []string
	// Secrets names the values made from a secret in this phase, besides
	// Run.Secrets: a value of the job's env: that read one (env.TOKEN), or
	// matrix for an instance whose matrix values were (Instance.Secret).
	Secrets []string
	// Needs are the results of the jobs this one needs, by job name.
	Needs map[string]JobResult
	// Status is how the job or step got here, for the status functions.
	Status expr.Status
	// Matrix holds the instance's matrix values, or nothing for a job
	// without a matrix.
	Matrix map[string]any
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
// have reads as "" or null. Run.Secrets and Secrets are marked
// (expr.Scope.Marked), and Unknown's values are expr.Unknown.
//
// The scope has no budget: the caller makes one for each group of
// templates (expr.Scope.Budgeted). Its Resolver and Index are the caller's
// to set, for input definitions.
//
// The compiler, the coordinator, the worker and the WebAssembly package
// all build their scopes with NewScope, so a context has one shape
// everywhere. It isn't built yet, and returns an empty scope, in which
// every context is unknown.
func NewScope(phase Phase, c Contexts) expr.Scope {
	return expr.Scope{}
}
