package plan

import (
	"slices"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/problems"
)

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
	// variable or a password, such as inputs.region or env.TOKEN, and the
	// values the run was given in the place of what is made from a variable
	// (plan.Run.Private), besides what is private whole: org, vars, and the
	// passwords, which Secrets names. The coordinator keeps what is private
	// only in the run's secret record, never shows it, and keeps the rest as
	// run state too (Split). What is private and not a secret is never
	// masked.
	Private []string `json:"private,omitempty"`
	// Secrets names each value above that is a secret or was made from one:
	// a secret variable, as in vars.token; a password, as in
	// inputs.creds.0.pass; a value the run was given that the compiler keeps
	// as one (plan.Run.Secrets); and what Start made from a secret, such as
	// env.TOKEN. Each is private, and masked, and so is what an expression
	// makes from it.
	Secrets []string `json:"secrets,omitempty"`
	// Derived names the secrets, among Secrets, that are values the run was
	// given in the place of what is made from a password or a secret
	// variable (plan.Run.Derived): they can be made from a secret, or not,
	// so they are masked as what is made from a secret is, a string of at
	// least 8 bytes or a credential field of a structured secret, and not
	// as a secret's own text, which is masked however short (Masked).
	Derived []string `json:"derived,omitempty"`
}

// Split splits r into what the coordinator keeps only in the run's secret
// record, the values that are private, and what it keeps as run state, for
// whoever can see the run: the rest, with Private and Secrets, which name
// values but hold none. JoinRunValues puts them together again.
//
// The record holds org and vars, and each value that Private and Secrets
// name, at its path: an item of a list at its index, after a null for each
// item before it that isn't private. The state holds the rest, with null in
// place of a private item of a list, so that each other item keeps its
// index. Neither shares a list or mapping with r.
func (r RunValues) Split() (record, state RunValues) {
	all, rec := r.tree(), map[string]any{}
	for _, path := range r.privatePaths() {
		v, ok := jsonptr.Lookup(all, path)
		if !ok {
			continue
		}
		setPath(rec, path, v)
		removePath(all, path)
	}
	record, state = fromTree(rec), fromTree(all)
	record.Format, state.Format = r.Format, r.Format
	state.Private, state.Secrets, state.Derived = slices.Clone(r.Private), slices.Clone(r.Secrets), slices.Clone(r.Derived)
	return record, state
}

// JoinRunValues returns the run's values from what Split split them into.
// It shares no list or mapping with either.
func JoinRunValues(record, state RunValues) RunValues {
	all, rec := state.tree(), record.tree()
	for _, path := range state.privatePaths() {
		if v, ok := jsonptr.Lookup(rec, path); ok {
			setPath(all, path, v)
		}
	}
	out := fromTree(all)
	out.Format = state.Format
	out.Private, out.Secrets, out.Derived = slices.Clone(state.Private), slices.Clone(state.Secrets), slices.Clone(state.Derived)
	return out
}

// Masked returns the strings of each value that Secrets names, as a Masker
// takes them: a string as it is, and each string in a list or a mapping,
// but never the text of a boolean, a number or null (Masker). A secret's
// own text is given however short; one that Derived names gives only
// what is masked of what is made from a secret: each of its strings of at
// least 8 bytes, or that is a credential field of a secret's own text
// (credentialFields). Each is given once, the secrets' own texts first,
// in the order of Secrets, and an empty one not at all.
func (r RunValues) Masked() []string {
	own, derived := r.secretStrings()
	var fields map[string]bool
	for _, s := range derived {
		if len(s) >= minDerived {
			own = append(own, s)
			continue
		}
		if len(s) < minField || jsonWord(s) {
			continue
		}
		if fields == nil {
			fields = credentialFields(r.ownTexts())
		}
		if fields[s] {
			own = append(own, s)
		}
	}
	return compactStrings(own)
}

// ownTexts returns the strings of each secret's own text: of each value
// that Secrets names but Derived doesn't.
func (r RunValues) ownTexts() []string {
	own, _ := r.secretStrings()
	return own
}

// secretStrings returns the strings of each value that Secrets names, as
// Masked gives them, those of the secrets' own texts and those of the
// values that Derived names apart, each once and none empty.
func (r RunValues) secretStrings() (own, derived []string) {
	all := r.tree()
	seen := map[string]bool{}
	for _, name := range r.Secrets {
		v, ok := jsonptr.Lookup(all, splitPath(name))
		if !ok {
			continue
		}
		isDerived := slices.Contains(r.Derived, name)
		maskedStrings(v, func(s string) {
			if s == "" || seen[s] {
				return
			}
			seen[s] = true
			if isDerived {
				derived = append(derived, s)
			} else {
				own = append(own, s)
			}
		})
	}
	return own, derived
}

// compactStrings returns ss without a string given before.
func compactStrings(ss []string) []string {
	seen := map[string]bool{}
	out := ss[:0]
	for _, s := range ss {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// maskedStrings gives add each string of v, a value made from a secret: v
// itself if it is one, or each in it if it is a list or a mapping.
func maskedStrings(v any, add func(string)) {
	switch v := v.(type) {
	case string:
		add(v)
	case []any:
		for _, item := range v {
			maskedStrings(item, add)
		}
	case map[string]any:
		for _, item := range jsonvalue.Sorted(v) {
			maskedStrings(item, add)
		}
	}
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
	// Needs are the results of the jobs this one needs, by job name. In
	// Step and End, the job's own is there too, whose outputs are those its
	// steps without an id wrote so far, and whose steps those with one.
	Needs map[string]JobResult
	// Status is how the job or step got here, for the status functions.
	Status expr.Status
	// Matrix holds the instance's matrix values, a mapping, or nothing for
	// a job without a matrix.
	Matrix Data
	// Job is the job context.
	Job JobContext
	// JobEnv is the job's env:, and StepEnv the step's, as evaluated: env
	// reads them over the workflow's (Run.Env), the job's from JobEnv on,
	// where it holds the values evaluated so far, and the step's in Step. A
	// name that one of them gives is marked only as Private and Secrets mark
	// it, whatever Run's mark the workflow's value of the name.
	JobEnv, StepEnv map[string]string
	// WorkerEnv holds the variables the worker defines for the job's steps
	// (JobState.Env), which env reads over all the rest from JobEnv on, as
	// format 1 lays them.
	WorkerEnv map[string]string
	// Steps are the outputs of the job's steps that have run, by step id.
	Steps map[string]map[string]string
	// StrictOutputs is set where format 1 reads an output strictly: a job
	// output, needs.<job>.outputs.<name>, or a step's, steps.<id>.outputs.
	// <name> or needs.<job>.steps.<id>.outputs.<name>, that nothing wrote
	// fails the expression that reads it (output_not_written), rather than
	// read as "".
	StrictOutputs bool
}

// NewScope returns the scope an expression evaluated in phase reads: each
// context phase knows, in its one shape (docs/design.md, Contexts), and no
// other, so that reading a later one is an expr.UnknownError. A context
// that is known but empty is there all the same, so that a name it doesn't
// have reads as "" or null. Unknown's values are expr.Unknown, and a
// context that Unknown names whole isn't there.
//
// The shapes are these, each context in the phases from the one named:
//
//   - inputs (Create): Run.Inputs, a mapping.
//   - org, vars, remote and sessions (Create): Run's, by name, strings.
//   - env (Start): Run.Env, then from JobEnv JobEnv over it, in Step
//     StepEnv over that, and from JobEnv WorkerEnv over all, strings.
//   - needs (Strategy): for each job of Needs, result, its outcome, when
//     it has one; outputs, by name, strings; and steps, by step id, each
//     with its outputs.
//   - matrix (Ready): Matrix, a mapping, empty for a job without a matrix.
//   - job (JobEnv): name, key, attempt, a number, group and member.
//   - steps (Step): for each step of Steps, by its id, its outputs. A
//     reference that doesn't go on to an output by name, such as
//     steps.build, steps.build.outputs or steps.build.result, reads as its
//     own text, as format 1 reads it (expr.Scope.Text).
//
// The status functions are known from Strategy on, as Status says.
//
// It marks two sets of values, which an evaluation reports reading apart
// (expr.Read): Scope.Marked, the secrets, which Run.Secrets and Secrets
// name, and the outputs of Needs, and of their steps, that hold a secret
// (JobResult.Secrets and JobResult.SecretSteps); and Scope.Private, the
// private values, which are org and vars, every secret, what Run.Private
// and Private name, and the outputs of Needs that are private
// (JobResult.Private).
//
// The scope has no budget: the caller makes one for each group of
// templates (expr.Scope.Budgeted). Its Resolver and Index are the caller's
// to set, for input definitions. The compiler, the coordinator, the worker
// and the WebAssembly package all build their scopes with NewScope, so a
// context has one shape everywhere. The scope holds c's input values and
// matrix values as they are, which an evaluation never changes, and nothing
// else of c.
func NewScope(phase Phase, c Contexts) expr.Scope {
	r := c.Run
	inputs := c.mapping("inputs", r.Inputs)
	contexts := map[string]any{
		"inputs": inputs, "org": stringsContext(r.Org), "vars": stringsContext(r.Vars),
		"remote": stringsContext(r.Remote), "sessions": stringsContext(r.Sessions),
	}
	// layered holds the env names a later layer than the workflow's gives,
	// whose values Run's marks don't mark.
	var layered map[string]bool
	if phase >= PhaseStart {
		env := stringsContext(r.Env)
		var layers []map[string]string
		if phase >= PhaseJobEnv {
			layers = append(layers, c.JobEnv)
		}
		if phase == PhaseStep {
			layers = append(layers, c.StepEnv)
		}
		if phase >= PhaseJobEnv {
			layers = append(layers, c.WorkerEnv)
		}
		for _, layer := range layers {
			for k, v := range layer {
				if layered == nil {
					layered = map[string]bool{}
				}
				env[k], layered[k] = v, true
			}
		}
		contexts["env"] = env
	}
	scope := expr.Scope{Contexts: contexts}
	if c.StrictOutputs {
		scope.Missing = strictOutputs(c.Needs, c.Steps)
	}
	if phase >= PhaseStrategy {
		contexts["needs"] = needsContext(c.Needs)
		for name, r := range c.Needs {
			for _, output := range r.Private {
				scope.Private.Add([]string{"needs", name, "outputs", output})
			}
			for _, output := range r.Secrets {
				scope.Marked.Add([]string{"needs", name, "outputs", output})
				scope.Private.Add([]string{"needs", name, "outputs", output})
			}
			for id, outputs := range r.SecretSteps {
				for _, output := range outputs {
					scope.Marked.Add([]string{"needs", name, "steps", id, "outputs", output})
					scope.Private.Add([]string{"needs", name, "steps", id, "outputs", output})
				}
			}
		}
		status := c.Status
		scope.Status = &status
	}
	if phase >= PhaseReady {
		contexts["matrix"] = c.mapping("matrix", c.Matrix)
	}
	if phase >= PhaseJobEnv {
		j := c.Job
		contexts["job"] = map[string]any{
			"name": j.Name, "key": j.Key, "attempt": int64(j.Attempt), "group": j.Group, "member": j.Member,
		}
	}
	if phase >= PhaseStep {
		steps := make(map[string]any, len(c.Steps))
		for id, outputs := range c.Steps {
			steps[id] = map[string]any{"outputs": stringsContext(outputs)}
		}
		contexts["steps"] = steps
		scope.Text = stepText
	}
	for _, name := range c.Unknown {
		path := splitPath(name)
		if _, ok := contexts[path[0]]; !ok {
			continue
		}
		if len(path) == 1 {
			delete(contexts, path[0])
			continue
		}
		setPath(contexts, path, expr.Unknown{Name: path[0]})
	}
	// mark marks the values names names: as private, and as secrets too if
	// secret is set. Run's don't mark an env value a later layer gives.
	mark := func(names []string, run, secret bool) {
		for _, name := range names {
			path := splitPath(name)
			if run && len(path) > 1 && path[0] == "env" && layered[path[1]] {
				continue
			}
			if secret {
				scope.Marked.Add(path)
			}
			scope.Private.Add(path)
		}
	}
	scope.Private.Add([]string{"org"}, []string{"vars"})
	mark(r.Secrets, true, true)
	mark(c.Secrets, false, true)
	mark(r.Private, true, false)
	mark(c.Private, false, false)
	return scope
}

// stepText reports whether path, in context, is a reference to steps that
// doesn't go on to an output by name, as steps.build, steps.build.outputs
// and steps.build.result don't, which format 1 reads as its own text: it
// reads steps only as steps.<id>.outputs.<name>.
func stepText(context string, path []string) bool {
	return context == "steps" && (len(path) < 3 || path[1] != "outputs")
}

// mapping returns d, the mapping that is context's value, or an empty one
// for nothing. It is d's own unless Unknown names a value in it, which is
// put in a copy.
func (c *Contexts) mapping(context string, d Data) map[string]any {
	m, _ := d.Get().(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	if slices.ContainsFunc(c.Unknown, func(name string) bool { return strings.HasPrefix(name, context+".") }) {
		return jsonvalue.CloneMap(m)
	}
	return m
}

// strictOutputs returns what reads the outputs of needs and steps strictly
// (Contexts.StrictOutputs): an output a job or a step has no value for is
// output_not_written. A name past an output's, which reads the JSON it
// holds, and a job or a step the contexts don't have, read as "".
func strictOutputs(needs map[string]JobResult, steps map[string]map[string]string) func(string, []string) (*problems.Kind, problems.Params) {
	missing := func(outputs map[string]string, of, owner, name string) (*problems.Kind, problems.Params) {
		if _, ok := outputs[name]; ok {
			return nil, nil
		}
		return problems.OutputNotWritten, problems.Params{"of": of, "owner": owner, "name": name}
	}
	return func(context string, path []string) (*problems.Kind, problems.Params) {
		switch {
		case context == "steps" && len(path) >= 3 && path[1] == "outputs":
			if outputs, ok := steps[path[0]]; ok {
				return missing(outputs, "step", path[0], path[2])
			}
		case context != "needs" || len(path) < 3:
		case path[1] == "outputs":
			if r, ok := needs[path[0]]; ok {
				return missing(r.Outputs, "job", path[0], path[2])
			}
		case path[1] == "steps" && len(path) >= 5 && path[3] == "outputs":
			if r, ok := needs[path[0]]; ok {
				if outputs, ok := r.Steps[path[2]]; ok {
					return missing(outputs, "step", path[2], path[4])
				}
				return problems.OutputNotWritten, problems.Params{"of": "step", "owner": path[2], "name": path[4]}
			}
		}
		return nil, nil
	}
}

// stringsContext returns a context whose values are strings, m's.
func stringsContext(m map[string]string) map[string]any {
	out := anyMap(m)
	if out == nil {
		out = map[string]any{}
	}
	return out
}

// needsContext returns the needs context of the jobs whose results needs
// holds.
func needsContext(needs map[string]JobResult) map[string]any {
	out := make(map[string]any, len(needs))
	for name, r := range needs {
		steps := make(map[string]any, len(r.Steps))
		for id, outputs := range r.Steps {
			steps[id] = map[string]any{"outputs": stringsContext(outputs)}
		}
		job := map[string]any{"outputs": stringsContext(r.Outputs), "steps": steps}
		if r.Outcome != "" {
			job["result"] = string(r.Outcome)
		}
		out[name] = job
	}
	return out
}
