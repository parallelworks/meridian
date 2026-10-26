package plan

import (
	"maps"
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
	// PhaseReady is the rest of Ready (Plan.Ready): matrix. A job's if:,
	// runs-on:, resources:, retry:, timeout: and ssh: are evaluated here.
	// Its working-directory: reads only what is known here too, but Step
	// evaluates it, where the run's directory is known.
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

// Knows reports whether an expression evaluated in p can read context, one
// of the contexts' names (docs/design.md, Contexts), as NewScope gives it:
// a phase knows the contexts of the phases before it too. inputs, org,
// vars, remote and sessions are known from Create, env from Start, the
// workflow's, needs from Strategy, matrix from Ready, job from JobEnv, and
// steps from Step. Another name is known in no phase. The compiler reports
// reading a context where it isn't known yet with it (context_not_known).
func (p Phase) Knows(context string) bool {
	switch context {
	case "inputs", "org", "vars", "remote", "sessions":
		return true
	case "env":
		return p >= PhaseStart
	case "needs":
		return p >= PhaseStrategy
	case "matrix":
		return p >= PhaseReady
	case "job":
		return p >= PhaseJobEnv
	case "steps":
		return p >= PhaseStep
	}
	return false
}

// KnowsStatus reports whether an expression evaluated in p can call the
// status functions, as NewScope gives them: from Strategy on, with needs.
func (p Phase) KnowsStatus() bool { return p >= PhaseStrategy }

// RunValues are what the phases after Start read of the run as a whole:
// the plan's, completed by Start (compile.StartResult). An attempt receives
// the part its job reads when it is claimed (Job.Reads).
//
// A value is named by its path: its context, then the keys that lead to it
// from there, a list's items by their indexes, joined with dots, as
// Run.Secrets names one: inputs.creds.0.pass, vars.token, env.TOKEN. A
// key's own . and ~ are written ~1 and ~0, as in inputs.db~1pass for a
// submitted value's key db.pass (JoinPath, SplitPath). A context's name
// alone is all of it.
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
	// passwords, which Secrets names. So Private alone doesn't say what is
	// private, and code never reads it so: PrivatePaths does. The
	// coordinator keeps what is private only in the run's secret record,
	// never shows it, and keeps the rest as run state too (Split). What is
	// private and not a secret is never masked.
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
	//
	// So, as for a JobResult and an EndResult, what is private is what
	// Private or Secrets names, with org and vars, and a secret what Secrets
	// names. Unlike theirs, Private here doesn't name what Secrets alone
	// does: a secret variable, or a password, is private whole, as org and
	// vars are, and Private names what was made from one. PrivatePaths
	// gives the lot, which Split keeps in the secret record.
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
	for _, path := range r.PrivatePaths() {
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
	for _, path := range state.PrivatePaths() {
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
// own text is given however short, and so is each credential field it
// holds (credentialFields), such as a password a script can take out of a
// secret's JSON; one that Derived names gives only what is masked of what
// is made from a secret: each of its strings of at least 8 bytes, or that
// is a credential field of a secret's own text. A string that a secret's
// own text gives is own, though a derived value gives it too. Each is
// given once, the secrets' own texts first, in the order of Secrets, and
// an empty one not at all.
func (r RunValues) Masked() []string {
	own, derived := r.secretStrings()
	return maskedFrom(own, derived, credentialFields(own))
}

// maskedFrom returns what a Masker is given of secrets' own texts, own,
// what is made from one, derived, and the own texts' credential fields,
// fields: each of own, each of fields, and each of derived that is at
// least minDerived bytes or one of fields, once, in that order.
func maskedFrom(own, derived []string, fields map[string]bool) []string {
	out := slices.Clone(own)
	out = append(out, slices.Sorted(maps.Keys(fields))...)
	for _, s := range derived {
		if len(s) >= minDerived || fields[s] {
			out = append(out, s)
		}
	}
	return compactStrings(out)
}

// secretStrings returns the strings of each value that Secrets names, as
// Masked gives them, those of the secrets' own texts and those of the
// values that Derived names apart, each once and none empty. A string is
// own if any secret's own text gives it, whatever derived value gives it
// too, as a password in a row of a list held as derived is: the own texts
// are found first.
func (r RunValues) secretStrings() (own, derived []string) {
	// It reads the values, and changes none.
	all, isDerived := r.view(), setOf(r.Derived)
	seen := map[string]bool{}
	for _, wantDerived := range []bool{false, true} {
		for _, name := range r.Secrets {
			if isDerived[name] != wantDerived {
				continue
			}
			v, ok := jsonptr.Lookup(all, SplitPath(name))
			if !ok {
				continue
			}
			maskedStrings(v, func(s string) {
				if s == "" || seen[s] {
					return
				}
				seen[s] = true
				if wantDerived {
					derived = append(derived, s)
				} else {
					own = append(own, s)
				}
			})
		}
	}
	return own, derived
}

// needsSecrets returns the values of the outputs, and of their steps'
// outputs, of the jobs in needs that hold a secret (JobResult.Secrets and
// SecretSteps), as secretStrings returns a run's: those that hold a
// secret's own text, and those only made from one (JobResult.Derived and
// DerivedSteps), apart, each once and none empty, in the order of the
// jobs' names.
func needsSecrets(needs map[string]JobResult) (own, derived []string) {
	seen := map[string]bool{}
	for _, wantDerived := range []bool{false, true} {
		add := func(v string, isDerived bool) {
			if v == "" || seen[v] || isDerived != wantDerived {
				return
			}
			seen[v] = true
			if isDerived {
				derived = append(derived, v)
			} else {
				own = append(own, v)
			}
		}
		for _, r := range jsonvalue.Sorted(needs) {
			derivedOutputs := setOf(r.Derived)
			for _, name := range r.Secrets {
				add(r.Outputs[name], derivedOutputs[name])
			}
			for id, outputs := range jsonvalue.Sorted(r.SecretSteps) {
				derivedSteps := setOf(r.DerivedSteps[id])
				for _, name := range outputs {
					add(r.Steps[id][name], derivedSteps[name])
				}
			}
		}
	}
	return own, derived
}

// setOf returns the set of the strings list holds.
func setOf(list []string) map[string]bool {
	set := make(map[string]bool, len(list))
	for _, s := range list {
		set[s] = true
	}
	return set
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

// maskedStrings gives add the text of each scalar of v, a value made from
// a secret, as an expression writes it into what a step reads (expr.Text):
// v itself if it is a string, a number or a boolean, as 8 or true, or each
// in it if it is a list or a mapping. Format 1 reads a value as it was
// submitted, so a password can be a number or a boolean, and is masked as
// its text all the same.
func maskedStrings(v any, add func(string)) {
	switch v := v.(type) {
	case nil:
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
	default:
		add(expr.Text(v))
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
//   - steps (Step): for each step of Steps, by its id, its outputs.
//
// A reference to needs or steps that format 1 doesn't read, such as
// needs.build, steps.build.outputs or steps.build.result, reads as its own
// text, as format 1 reads it (ReadsAsText).
//
// The status functions are known from Strategy on, as Status says. Which
// phase knows each context, and the status functions, is Phase.Knows and
// Phase.KnowsStatus, which the compiler's checks of what an expression
// reads use too.
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
// context has one shape everywhere; the compiler, as it fills in the
// inputs, marks in Marked what reads a variable or a password, which is
// what it asks of each evaluation. The scope holds c's input values and
// matrix values as they are, which an evaluation never changes, and nothing
// else of c.
func NewScope(phase Phase, c Contexts) expr.Scope { return newScope(phase, c, nil) }

// sharedContexts are the needs and steps contexts that newScope built of a
// Contexts' Needs and Steps, which the scopes it builds after of the same
// Needs and Steps share, as an evaluation never changes a context.
type sharedContexts struct{ needs, steps map[string]any }

// newScope is NewScope, which takes the needs and steps contexts from
// shared, if it isn't nil, and builds them into it the first time: its
// caller gives it the same shared for each scope of the same Needs and
// Steps, and a new one when they change. A Contexts with Unknown values
// has contexts of its own, as Unknown's values are set in them.
func newScope(phase Phase, c Contexts, shared *sharedContexts) expr.Scope {
	if shared == nil || len(c.Unknown) > 0 {
		shared = &sharedContexts{}
	}
	r := c.Run
	inputs := c.mapping("inputs", r.Inputs)
	contexts := map[string]any{
		"inputs": inputs, "org": stringsContext(r.Org), "vars": stringsContext(r.Vars),
		"remote": stringsContext(r.Remote), "sessions": stringsContext(r.Sessions),
	}
	// layered holds the env names a later layer than the workflow's gives,
	// whose values Run's marks don't mark.
	var layered map[string]bool
	if phase.Knows("env") {
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
	if phase.Knows("needs") {
		if shared.needs == nil {
			shared.needs = needsContext(c.Needs)
		}
		contexts["needs"] = shared.needs
		scope.Text = ReadsAsText
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
	}
	if phase.KnowsStatus() {
		status := c.Status
		scope.Status = &status
	}
	if phase.Knows("matrix") {
		contexts["matrix"] = c.mapping("matrix", c.Matrix)
	}
	if phase.Knows("job") {
		j := c.Job
		contexts["job"] = map[string]any{
			"name": j.Name, "key": j.Key, "attempt": int64(j.Attempt), "group": j.Group, "member": j.Member,
		}
	}
	if phase.Knows("steps") {
		if shared.steps == nil {
			shared.steps = stepsContext(c.Steps)
		}
		contexts["steps"] = shared.steps
	}
	for _, name := range c.Unknown {
		path := SplitPath(name)
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
			path := SplitPath(name)
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

// ReadsAsText reports whether a reference to path, in context, reads as its
// own text, as format 1 reads a word it doesn't know (expr.Scope.Text).
// Format 1 reads steps only as steps.<id>.outputs.<name>, and needs only
// as needs.<job>.outputs.<name> and needs.<job>.steps.<id>.outputs.<name>,
// to which Meridian adds needs.<job>.result; any other reference to
// either, such as steps.<id>, needs.<job>, needs.<job>.outputs or
// needs.<job>.steps.<id>.result, is its own text, whatever step or job it
// names.
func ReadsAsText(context string, path []string) bool {
	switch context {
	case "steps":
		return len(path) < 3 || path[1] != "outputs"
	case "needs":
		switch {
		case len(path) >= 2 && path[1] == "result",
			len(path) >= 3 && path[1] == "outputs",
			len(path) >= 5 && path[1] == "steps" && path[3] == "outputs":
			return false
		}
		return true
	}
	return false
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
// output_not_written, and so is one of a step by an id that none of the
// job's steps that have run has, as format 1 fails what reads it. A name
// past an output's, which reads the JSON it holds, and a job the contexts
// don't have, read as "".
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
			return missing(steps[path[0]], "step", path[0], path[2])
		case context != "needs" || len(path) < 3:
		case path[1] == "outputs":
			if r, ok := needs[path[0]]; ok {
				return missing(r.Outputs, "job", path[0], path[2])
			}
		case path[1] == "steps" && len(path) >= 5 && path[3] == "outputs":
			if r, ok := needs[path[0]]; ok {
				return missing(r.Steps[path[2]], "step", path[2], path[4])
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

// stepsContext returns the steps context of the steps whose outputs steps
// holds, by step id, as needs holds a job's steps too.
func stepsContext(steps map[string]map[string]string) map[string]any {
	out := make(map[string]any, len(steps))
	for id, outputs := range steps {
		out[id] = map[string]any{"outputs": stringsContext(outputs)}
	}
	return out
}

// needsContext returns the needs context of the jobs whose results needs
// holds.
func needsContext(needs map[string]JobResult) map[string]any {
	out := make(map[string]any, len(needs))
	for name, r := range needs {
		out[name] = jobContext(r, stepsContext(r.Steps))
	}
	return out
}

// jobContext returns the context of a job in needs whose result r is, and
// the context of whose steps' outputs steps is (stepsContext).
func jobContext(r JobResult, steps map[string]any) map[string]any {
	job := map[string]any{"outputs": stringsContext(r.Outputs), "steps": steps}
	if r.Outcome != "" {
		job["result"] = string(r.Outcome)
	}
	return job
}
