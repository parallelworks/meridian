package compile

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// StartInput is what Start reads besides the plan.
type StartInput struct {
	// RunID is the run's ID, which its sessions are made for.
	RunID string
	// Variables reads the variables the plan lists, plain and secret. Nil
	// means there are none: a run that reads one can't start.
	Variables Variables
	// Record is the run's secret record as Create gave it (Created.Record):
	// what was submitted, which Start resolves again, the parts of the
	// input definitions the plan withheld, which it puts back in their
	// places, what each decision of the run's shape decided, and which
	// parts of what was submitted mean the expression a form made them
	// from. A record that doesn't fit the plan is an error (ErrMismatch).
	Record plan.Record
	// Inputs provides the input types that name a platform's resources, as
	// Options.Inputs does when the run is created: Start asks it again for
	// each input of a platform's type. Nil means there is no platform.
	Inputs InputResolver
	// Sessions makes the run's sessions. Nil means there is no session
	// provider: a run with sessions can't start.
	Sessions Sessions
}

// StartResult is what Start gives: the run's values, complete. It holds
// what is private beside what isn't, so the coordinator stores none of it
// as it is: Keep splits it into what the run's secret record holds and
// what run state holds (StartState).
type StartResult struct {
	// Run is what the phases after Start read of the run, whole: the input
	// values, each password and private value among them, the workflow's
	// env:, every variable Start read, plain and secret, and in Run.Private
	// and Run.Secrets what of it is private, and secret. The coordinator
	// keeps the private part only in the run's secret record, and the rest
	// as run state too (plan.RunValues.Split).
	Run plan.RunValues
	// TimeoutSeconds is the run's timeout:, in seconds, or 0 for none.
	// TimeoutPrivate is set when evaluating it read a variable or a
	// password: the coordinator keeps it only in the secret record.
	TimeoutSeconds int64
	TimeoutPrivate bool
	// Expansions are the matrices Start expanded, those whose values are
	// known once the run starts, by job name. A job with a strategy: that
	// isn't here reads needs, and Ready expands it. An expansion whose
	// instances are private (plan.Instance.Private) the coordinator keeps
	// in the secret record.
	Expansions map[string]*plan.Expansion
}

// StartState is what run state holds of what Start gives, which whoever
// can see the run can read: all of a StartResult but what Keep puts in the
// run's secret record.
type StartState struct {
	// Version is the version of the plan the run was started from, which
	// the state is encoded at, so that a release reads only the states of
	// the plans it reads.
	Version int
	// Run is the run's values but for those that are private (the state
	// half of plan.RunValues.Split), with the names of those that are.
	Run plan.RunValues
	// TimeoutSeconds is the run's timeout, in seconds, or 0 for none; and
	// when evaluating it read a variable or a password, TimeoutPrivate is
	// set, and the secret record holds the timeout instead.
	TimeoutSeconds int64
	TimeoutPrivate bool
	// Expansions are the matrices Start expanded, by job name, with each
	// private instance's matrix values left out (plan.Instance.Private),
	// and the strategy's fail-fast: and max-parallel: of a matrix whose
	// instances are: their keys and indexes are run state, and the secret
	// record holds the expansion whole (plan.Expansion.Split).
	Expansions map[string]*plan.Expansion
}

// Keep splits what Start gave into what the run's secret record holds and
// what run state holds, which it returns: the one place that says which is
// which, for each of res's members. The record gets the private half of
// the run's values (plan.RunValues.Split), the timeout if it is private,
// and each expansion whose instances are private, whole
// (plan.Expansion.Split). Neither shares a mapping or a list with res. The
// state is of the record's version, its plan's.
func (res *StartResult) Keep(record *plan.Record) StartState {
	private, values := res.Run.Split()
	record.Run = &private
	state := StartState{Version: record.Version, Run: values, TimeoutPrivate: res.TimeoutPrivate}
	if res.TimeoutPrivate {
		seconds := res.TimeoutSeconds
		record.TimeoutSeconds = &seconds
	} else {
		state.TimeoutSeconds = res.TimeoutSeconds
	}
	for job, x := range jsonvalue.Sorted(res.Expansions) {
		private, shown := x.Split()
		if state.Expansions == nil {
			state.Expansions = map[string]*plan.Expansion{}
		}
		state.Expansions[job] = shown
		if private != nil {
			if record.Expansions == nil {
				record.Expansions = map[string]*plan.Expansion{}
			}
			record.Expansions[job] = private
		}
	}
	return state
}

// ErrMismatch is what Start's error wraps when what it is given doesn't fit
// together: a plan this release's compiler didn't make, or a secret record
// that isn't the one Create gave with the plan. Start checks, in order:
//
//   - that there is a plan, and that the record names it, by its digest
//     (plan.Record.PlanDigest, plan.Digest), and holds what was submitted
//     as a mapping, if anything;
//   - that the record's withheld parts and decisions are those the plan
//     names (Run.Withheld, Run.Decisions), no more and no fewer, and that
//     each withheld part is in the input definitions and each decision is
//     of an input's condition;
//   - and, once the inputs are resolved again and the run's shape is what
//     Create decided, with no other problem, that each value the
//     resolution keeps out of the plan, as a password or a private value,
//     is one the plan names, or in one it names (Run.Secrets, Run.Private),
//     but for the parts of an input the plan holds none of, which a
//     password input never is; and that what was submitted, resolved
//     again, gives the values the plan holds (Run.Inputs), and none for an
//     input the workflow doesn't define that the plan doesn't hold, but
//     for what Create keeps out of the plan, the inputs of a platform's
//     type, which the Inputs resolver gives again, and those whose
//     definitions read what it gives (checkPlanned).
//
// So the digest binds a record to its plan, and these checks, what was
// submitted to what the plan holds; neither binds it to its run. What the
// plan doesn't hold, as a password, a value given in place of a default
// that reads a variable, or which parts of what was submitted mean their
// expressions (plan.Record.Means), a record of another run of the same
// plan can give otherwise, and Start starts with what it gives: the
// coordinator keeps each run's record with its plan. A value the plan
// names that the record doesn't hold, as a password, is the run's problem
// (input_required), not a mismatch. Starting the run again gives the same
// error, so the coordinator fails the run rather than start it again. Any
// other error but ctx's, and plan.ErrVersion's for a plan of another
// release, is a resolver that couldn't answer, which starting again can
// get past.
var ErrMismatch = errors.New("compile: the plan and the secret record don't fit")

// mismatch returns an error that wraps ErrMismatch, which says what doesn't
// fit.
func mismatch(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrMismatch}, args...)...)
}

// Start is the Start phase: it completes the run's values as it starts, as
// docs/design.md describes under Starting a run. It reads the variables p
// lists, through in.Variables, once for the run, secret ones included, and
// resolves the inputs again from what was submitted (plan.Record.Submitted),
// as Create resolved them, but with everything known: the variables, the
// passwords and the workflow's env:, which it evaluates too, and what the
// Inputs resolver gives now for each input of a platform's type. A part of
// what was submitted that Create decided means the expression a form made
// it from (plan.Record.Means) is what the expression gives now, as a
// variable can have changed since, which Start evaluates in the part's
// place, in the order the values read each other, so that what reads the
// part reads what it is; and an env: value that a value reads it evaluates
// before the value, in that order too.
//
// It then checks the run's shape against what Create decided: each input's
// condition that read a variable or a password (plan.Run.Decisions), with
// what the record says it decided, and each job's needs: that read one
// (plan.Job.NeedsExpr), with the plan's, reporting run_shape_changed where
// one differs; evaluates the workflow's timeout:; and expands each matrix
// whose values are known by then, with plan.Expand and p's Run.Limits. Only
// once all of that has passed does it make the run's sessions, through
// in.Sessions, from its sessions: evaluated with the run's values, so a
// run that fails to start makes none. A rerun calls Start
// again, with the same plan and what is submitted again, and reads every
// variable again.
//
// A relative reference reads the input definitions, which the plan holds;
// one that climbs out of them reads nothing, where the compiler read the
// rest of the workflow.
//
// What it made from a variable or a password is private, and what it made
// from a secret is a secret too, by its name, in the values it gives
// (plan.RunValues.Private and Secrets), beside what p names so. A variable
// the Variables interface gives as a secret is one, and so is one p names
// as a secret though the interface no longer does.
//
// The coordinator runs Start as the run's first job, and stores what it
// gives in the transaction that ends that job. Given the same plan, record
// and resolver answers, it gives the same result, byte for byte, so a
// coordinator that dies during Start leaves another to start it again.
//
// Its diagnostics are the run's problems, with no file position, which
// never show a value made from a variable or a password; an error among
// them fails the run, and then there is no result. It returns an error,
// and no result, when it couldn't finish for a reason that isn't the
// run's. One that wraps ErrMismatch, a plan or a record that doesn't fit,
// fails the run; one that wraps plan.ErrVersion is a plan of another
// release, which a coordinator of that release starts; ctx's, when ctx is
// done, and any other, a resolver that couldn't answer, as one that wraps
// ErrUnanswered, the coordinator starts the run again after.
func Start(ctx context.Context, p *plan.Plan, in StartInput) (*StartResult, problems.Diagnostics, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	switch {
	case p == nil:
		return nil, nil, mismatch("no plan")
	case p.Version > plan.Version:
		return nil, nil, fmt.Errorf("compile: %w %d; this release starts plans of versions up to %d", plan.ErrVersion, p.Version, plan.Version)
	case p.Version < 1:
		return nil, nil, mismatch("a plan of no version")
	}
	switch ok, err := plan.HasDigest(p, in.Record.PlanDigest); {
	case err != nil:
		return nil, nil, mismatch("the plan doesn't encode: %v", err)
	case !ok:
		return nil, nil, mismatch("the secret record is of another plan")
	}
	if _, ok := in.Record.Submitted.Get().(map[string]any); !ok && !in.Record.Submitted.IsZero() {
		return nil, nil, mismatch("what the secret record holds as submitted isn't a mapping")
	}
	st := &starter{ctx: ctx, p: p, in: in}
	if err := st.readVariables(); err != nil {
		return nil, nil, err
	}
	defs, err := st.definitions()
	if err != nil {
		return nil, nil, err
	}
	decided, err := recordFor(p.Run.Decisions, in.Record.Decisions, "decision")
	if err != nil {
		return nil, nil, err
	}
	if err := st.resolve(defs); err != nil {
		return nil, nil, err
	}
	// Create's warnings are Create's: Start keeps only what fails the run.
	st.r.diags = st.r.diags.Errors()
	if err := st.checkDecided(decided); err != nil {
		return nil, nil, err
	}
	st.checkNeeds()
	// A run whose shape changed, or that has another problem, fails as it
	// is: what it resolved can differ from the plan for that reason.
	if !st.diagnostics().HasErrors() {
		if err := st.checkFits(); err != nil {
			return nil, nil, err
		}
		if err := st.checkPlanned(); err != nil {
			return nil, nil, err
		}
	}
	run := st.runValues()
	res := &StartResult{Run: run}
	// The timeout and the matrices read the run's values, complete, as the
	// phases after Start read them.
	scope := plan.NewScope(plan.PhaseStart, plan.Contexts{Run: run})
	var read expr.Read
	var timeoutDiags problems.Diagnostics
	res.TimeoutSeconds, read, timeoutDiags = plan.RunTimeout(p.Run.Timeout, scope)
	res.TimeoutPrivate = read.Private
	st.r.diags = append(st.r.diags, timeoutDiags...)
	if err := st.expand(res, scope); err != nil {
		return nil, nil, err
	}
	sessions, sessionsRead := st.sessions(scope)
	diags := st.diagnostics()
	if diags.HasErrors() {
		return nil, diags, nil
	}
	if sessions != nil {
		if err := st.in.Sessions.Make(ctx, st.in.RunID, sessions, maps.Clone(p.Run.SessionNames), sessionsRead); err != nil {
			if err := st.sessionsError(err, sessionsRead); err != nil {
				return nil, nil, err
			}
			return nil, st.diagnostics(), nil
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return res, diags, nil
}

// roundWork is the work, in the budget's bytes, that each round of resolve
// after the first counts toward the rounds' budget for each input, each
// field of a list's rows and each env: value: what a template of 256 steps
// counts, as resolving one again costs as much as that takes, or more. So
// the rounds of a run pass the budget, 256 MiB, once they have resolved
// 128 Ki of them again, in a few seconds at most, and a run of 1,000
// inputs and env: values has more than a hundred rounds to spare. Tests
// set it.
var roundWork = 2048

// A starter is a run as Start completes its values.
type starter struct {
	ctx context.Context
	p   *plan.Plan
	in  StartInput
	// r is the input resolver that resolved the inputs last (resolve), and
	// s what it keeps of the start (r.start).
	r *inputResolver
	s *startState
	// org and vars are the variables Start read, and secretVariables names
	// those that are secrets: those the Variables interface gives as
	// secrets, and those the plan names so.
	org, vars       map[string]string
	secretVariables map[string]bool
	// envKeys are the workflow's env: names, in order.
	envKeys []string
	// diags are the problems Start found of its own, beside those the
	// input resolver reports.
	diags problems.Diagnostics
	// budget is the one budget of what each round of resolve evaluates.
	budget expr.Scope
	// demanded holds the env: values that a round evaluated as the inputs'
	// values were filled in (settle), with what each gave and read, which
	// evalEnv checks once they are; and late those that gave otherwise
	// than they give then, which Start evaluates only once a round has
	// filled the values in, from then on.
	demanded map[string]demandedEnv
	late     map[string]bool
	// envPatterns holds, by name, what envReadsOf found of each env: value
	// in the round.
	envPatterns map[string][][]string
}

// demandedEnv is what an env: value gave as the inputs' values were filled
// in, and what it read.
type demandedEnv struct {
	text string
	read expr.Read
}

// startState is what the input resolver keeps as it completes the run's
// values as the run starts (Start). The resolver marks, in its marks, what
// reads a variable or a password, which is private (expr.Scope.Private),
// and secrets marks what is a secret or was made from one, which is
// masked too (expr.Scope.Marked).
type startState struct {
	// run holds what Start's evaluations read besides the inputs: the
	// variables it read, the workflow's env as far as it is evaluated,
	// remote and sessions.
	run plan.RunValues
	// pendingEnv holds the names of the env values that aren't evaluated
	// yet, which read as not known yet.
	pendingEnv map[string]bool
	secrets    expr.Marks
	// madeSecret and madePrivate name the values Start made from a secret,
	// and from a variable or a password but no secret.
	madeSecret, madePrivate map[string]bool
	// envReads are the patterns (reach.Patterns) of the paths in the
	// inputs' values that the workflow's env: values read.
	envReads [][]string
	// settle evaluates, as a value is filled in, each env: value not
	// evaluated yet that exprs, the expressions that fill it in, read, and
	// that those read in turn, whose reads of the values ready reports are
	// filled in (starter.settle); and envReadsOf returns the patterns of
	// those reads, which the value waits for (starter.envReadsOf). Neither
	// takes a value Start evaluates only once the values are filled in
	// (starter.late).
	settle     func(exprs []*expr.Expr, ready func(pattern []string) bool)
	envReadsOf func(exprs []*expr.Expr) [][]string
	// budget is the budget that every round shares (expr.Scope.WithBudget).
	budget expr.Scope
}

// contexts returns what Start's evaluations read: the inputs' values,
// view, with what is evaluated as the run starts unknown until it is, and
// the rest as s holds it, with an env value not evaluated yet unknown.
func (s *startState) contexts(view map[string]any) plan.Contexts {
	run := s.run
	run.Inputs = plan.DataOf(view)
	return plan.Contexts{Run: run, Unknown: slices.Sorted(maps.Keys(s.pendingEnv))}
}

// made notes what the evaluation that gave the value at path in the
// inputs' values read: what read a variable or a password is private, and
// what read a secret a secret, and what reads it, in turn, reads one.
func (s *startState) made(r *inputResolver, path []string, read expr.Read) {
	if !read.Private && !read.Marked {
		return
	}
	full := slices.Concat([]string{"inputs"}, path)
	name := plan.JoinPath(full...)
	r.mark(path)
	if read.Marked {
		s.secrets.Add(full)
		s.madeSecret[name] = true
		return
	}
	s.madePrivate[name] = true
}

// readVariables reads the variables the plan lists, once, secret ones
// included, as the run's org and vars.
func (st *starter) readVariables() error {
	p := st.p
	st.secretVariables = map[string]bool{}
	whole, one := map[string]bool{}, map[string]bool{}
	var first string
	for _, names := range [][]string{p.Run.Variables, p.Run.Secrets} {
		for _, name := range names {
			path := plan.SplitPath(name)
			if path[0] != "org" && path[0] != "vars" {
				continue
			}
			if first == "" {
				first = name
			}
			if len(path) == 1 {
				whole[path[0]] = true
				continue
			}
			// The interface reads a variable's name as it is, after its
			// context and a dot: a name can hold dots of its own.
			one[path[0]+"."+path[1]] = true
		}
	}
	for _, name := range p.Run.Secrets {
		if path := plan.SplitPath(name); len(path) == 2 && (path[0] == "org" || path[0] == "vars") {
			st.secretVariables[name] = true
		}
	}
	asked := slices.Sorted(maps.Keys(whole))
	for name := range one {
		if context, _, _ := strings.Cut(name, "."); !whole[context] {
			asked = append(asked, name)
		}
	}
	slices.Sort(asked)
	if len(asked) == 0 {
		return nil
	}
	if st.in.Variables == nil {
		st.report(problems.ResolverMissing, "", problems.Params{"key": first, "interface": "Variables"})
		return nil
	}
	got, err := st.in.Variables.Read(st.ctx, asked, true)
	if err != nil {
		return st.resolverError(err, "")
	}
	for key, v := range jsonvalue.Sorted(got) {
		context, name, ok := strings.Cut(key, ".")
		if !ok || !whole[context] && !one[key] {
			continue // It wasn't asked for.
		}
		values := &st.org
		if context == "vars" {
			values = &st.vars
		}
		if *values == nil {
			*values = map[string]string{}
		}
		(*values)[name] = v.Value
		if v.Secret {
			st.secretVariables[plan.JoinPath(context, name)] = true
		}
	}
	return nil
}

// definitions returns the plan's input definitions with the parts it
// withheld put back from the secret record.
func (st *starter) definitions() (map[string]any, error) {
	p := st.p
	defs, _ := p.Run.Definitions.Written().(map[string]any)
	defs = orEmptyMap(defs)
	withheld, err := recordFor(p.Run.Withheld, st.in.Record.Withheld, "part of the input definitions")
	if err != nil {
		return nil, err
	}
	for _, pointer := range p.Run.Withheld {
		loc, err := jsonptr.Split(pointer)
		if err != nil || len(loc) <= len(definitionsPath) || !slices.Equal(loc[:len(definitionsPath)], definitionsPath) {
			return nil, mismatch("the plan withholds %s, which isn't in the input definitions", pointer)
		}
		if err := setGrow(defs, loc[len(definitionsPath):], jsonvalue.Clone(withheld[pointer].Get())); err != nil {
			return nil, err
		}
	}
	return defs, nil
}

// resolve resolves the inputs from what was submitted, as Create does, but
// with every variable known and as the run starts (inputResolver.final),
// and evaluates the workflow's env:. Its last resolver is st.r.
//
// An env: value that an input's default, or a text, reads, it evaluates as
// the values are filled in, before the value that reads it, which waits
// for what the env: value reads (settle): so inputs and env: values that
// read each other in turn are evaluated in one round, in the order they
// read each other. The rest it evaluates once the round has filled the
// values in, applied the conditions and checked the values, and checks
// that those it evaluated before give the same then, and read the same of
// what is private; if one doesn't, as one that reads an input a condition
// drops, it starts again, and evaluates that one only then (late). It
// resolves the inputs again, with the env: values it has, while what reads
// one waits for it and a round evaluated more of them. What reads a part
// that means its expression waits for it too, but for what reads it
// through a relative reference: where the input definitions make one, and
// a part's expression gave another value than was submitted, Start
// resolves the inputs again with what it gave in its place, a round for
// each such part at most. The rounds share one budget, so all of them
// together keep to its limits, and each round after the first counts the
// work of resolving every input, row and env: value again toward it
// (roundWork): a run whose rounds pass it fails with
// expressions_too_much_work.
//
// What is left once nothing more is evaluated reads itself, or each other
// (values_read_each_other), or a context Start doesn't know.
func (st *starter) resolve(defs map[string]any) error {
	p := st.p
	doc := &format.Document{Format: p.Run.Format, Value: map[string]any{
		"on": map[string]any{"execute": map[string]any{"inputs": defs}},
	}}
	// What finds what can read a secret (taint) reads each secret variable
	// as not known, as Create reads it.
	contexts := map[string]any{}
	for name, values := range map[string]map[string]string{"org": st.org, "vars": st.vars} {
		m := make(map[string]any, len(values))
		for k, v := range values {
			if st.secretVariables[plan.JoinPath(name, k)] {
				m[k] = expr.Unknown{Name: name}
			} else {
				m[k] = v
			}
		}
		contexts[name] = m
	}
	opts := Options{Inputs: st.in.Inputs, Contexts: contexts}
	st.envKeys = slices.Sorted(maps.Keys(p.Run.Env))
	var envReads [][]string
	for _, k := range st.envKeys {
		envReads = append(envReads, readPatterns(p.Run.Env[k].Exprs(), nil)...)
	}
	env, envSecret, envPrivate := map[string]string{}, map[string]bool{}, map[string]bool{}
	submitted, _ := st.in.Record.Submitted.Get().(map[string]any)
	st.budget = expr.Scope{}.Budgeted()
	reported := len(st.diags)
	remeant := 0
	for round := 0; ; round++ {
		if err := st.ctx.Err(); err != nil {
			return err
		}
		if round > 0 {
			// Resolving the inputs again is work the budget counts, so that
			// rounds without end, each as large as the run, fail the run.
			nodes := len(st.r.fields) + len(st.r.rows) + len(st.envKeys)
			if err := st.budget.Charge(roundWork * nodes); err != nil {
				if be, ok := errors.AsType[*expr.BudgetError](err); ok {
					st.report(be.Kind, jsonptr.Format(definitionsPath), problems.Params{"limit": be.Limit})
				}
				return nil
			}
		}
		st.s = &startState{
			madeSecret: map[string]bool{}, madePrivate: map[string]bool{}, pendingEnv: map[string]bool{},
			envReads: envReads, budget: st.budget,
		}
		st.demanded, st.envPatterns = map[string]demandedEnv{}, map[string][][]string{}
		st.s.settle, st.s.envReadsOf = st.settle, st.envReadsOf
		st.s.run = plan.RunValues{
			Format: p.Run.Format, Org: st.org, Vars: st.vars, Env: maps.Clone(env),
			Remote: maps.Clone(p.Run.Remote), Sessions: maps.Clone(p.Run.SessionNames),
		}
		for _, k := range st.envKeys {
			if _, ok := env[k]; !ok {
				st.s.pendingEnv[plan.JoinPath("env", k)] = true
			}
		}
		for name := range st.secretVariables {
			st.s.secrets.Add(plan.SplitPath(name))
		}
		st.r = newInputResolver(doc, submitted, opts)
		st.r.ctx, st.r.start, st.r.final, st.r.means = st.ctx, st.s, true, st.in.Record.Means
		// What a round evaluated of env: before reads as it did.
		for k := range envPrivate {
			st.r.marks.Add([]string{"env", k})
			name := plan.JoinPath("env", k)
			if envSecret[k] {
				st.s.secrets.Add([]string{"env", k})
				st.s.madeSecret[name] = true
			} else {
				st.s.madePrivate[name] = true
			}
		}
		// What the plan names as a secret, or private, is one still.
		for _, names := range [][]string{p.Run.Secrets, p.Run.Private} {
			for _, name := range names {
				if path := plan.SplitPath(name); path[0] == "inputs" && len(path) > 1 {
					st.r.mark(path[1:])
				}
			}
		}
		for _, name := range p.Run.Secrets {
			if path := plan.SplitPath(name); path[0] == "inputs" && len(path) > 1 {
				st.s.secrets.Add(path)
			}
		}
		st.r.resolve()
		if err := st.r.unansweredErr(); err != nil {
			// The Inputs resolver couldn't answer: the coordinator starts
			// the run again.
			return err
		}
		progress, late := st.evalEnv()
		again := late != nil
		for _, k := range late {
			if st.late == nil {
				st.late = map[string]bool{}
			}
			st.late[k] = true
		}
		if !again && len(st.r.remeant) > 0 && remeant < len(st.in.Record.Means) && st.readsRelative() {
			// A part whose expression gave another value than was
			// submitted, which a relative reference can have read, is what
			// it gave in its place in what was submitted, a round for each
			// such part at most.
			again = true
			remeant++
			submitted = orEmptyMap(jsonvalue.CloneMap(submitted))
			for name, v := range jsonvalue.Sorted(st.r.remeant) {
				// A part not in what was submitted, as one of a list's
				// default rows, is what the expression gives all the same.
				_ = setGrow(submitted, plan.SplitPath(name), v)
			}
		}
		if again {
			clear(env)
			clear(envSecret)
			clear(envPrivate)
			st.diags = st.diags[:reported]
			continue
		}
		for _, k := range st.envKeys {
			if name := plan.JoinPath("env", k); !st.s.pendingEnv[name] {
				env[k] = st.s.run.Env[k]
				envSecret[k] = st.s.madeSecret[name]
				if st.s.madeSecret[name] || st.s.madePrivate[name] {
					envPrivate[k] = true
				}
			}
		}
		waiting := slices.ContainsFunc(st.r.fields, func(f *field) bool { return f.deferred && !f.ignored })
		if waiting && progress {
			continue
		}
		if waiting || len(st.s.pendingEnv) > 0 {
			st.reportLeft()
		}
		return nil
	}
}

// reportLeft reports what is left once Start can evaluate nothing more:
// each input whose value reads what Start doesn't know, as needs, as that
// context's problem; and the rest, inputs and env: values that read
// themselves or each other, as values_read_each_other.
func (st *starter) reportLeft() {
	r := st.r
	var left []string
	for _, f := range r.fields {
		if !f.deferred || f.ignored {
			continue
		}
		waiting := false
		for _, part := range f.later {
			if part.variable {
				continue
			}
			_, read, err := r.evalRead(part.loc, part.index)
			if u, ok := errors.AsType[*expr.UnknownError](err); ok && u.Name != "inputs" && u.Name != "env" {
				st.reportEval(part.loc, err, read)
				continue
			}
			waiting = true
		}
		if waiting {
			left = append(left, plan.JoinPath(slices.Concat([]string{"inputs"}, f.path)...))
		}
	}
	left = append(left, slices.Sorted(maps.Keys(st.s.pendingEnv))...)
	if len(left) > 0 {
		st.report(problems.ValuesReadEachOther, "", problems.Params{"names": problems.QuoteAll(left)})
	}
}

// evalEnv evaluates the workflow's env: values not known yet, each after
// those it reads, and reports whether it evaluated any; or else returns
// those that the round evaluated as the values were filled in (demanded)
// that give otherwise than they gave then, or read otherwise of what is
// private, without evaluating any. They read an input of a platform's type
// as the Inputs resolver gave it, as everything does (resolvePlatform).
func (st *starter) evalEnv() (progress bool, late []string) {
	r := st.r
	if len(st.s.pendingEnv) == 0 && len(st.demanded) == 0 {
		return false, nil
	}
	for _, k := range slices.Sorted(maps.Keys(st.demanded)) {
		d := st.demanded[k]
		out, read, err := st.p.Run.Env[k].EvalRead(r.scope())
		if err != nil || expr.Text(out) != d.text || read.Private != d.read.Private || read.Marked != d.read.Marked {
			late = append(late, k)
		}
	}
	if late == nil {
		left := len(st.s.pendingEnv)
		visiting := map[string]bool{}
		for _, k := range st.envKeys {
			st.settleEnv(k, visiting, nil)
		}
		progress = len(st.s.pendingEnv) < left
	}
	return progress, late
}

// settle evaluates each env: value not evaluated yet that exprs read, and
// those they read in turn, that it can as the inputs' values are filled
// in: once ready reports that what it reads of them is filled in, if it
// evaluates without an error, which the round reports once the values are
// filled in, if it is one still, and unless, in a round before, it gave
// otherwise once they were (late): such a one is evaluated only then.
func (st *starter) settle(exprs []*expr.Expr, ready func(pattern []string) bool) {
	if len(st.s.pendingEnv) == 0 {
		return
	}
	var visiting map[string]bool
	for _, k := range st.envRefs(exprs) {
		if visiting == nil {
			visiting = map[string]bool{}
		}
		st.settleEnv(k, visiting, ready)
	}
}

// readsRelative reports whether an expression of the input definitions
// makes a relative reference, which doesn't make what makes it wait for
// the value it reads.
func (st *starter) readsRelative() bool {
	exprs, _ := st.r.workflow.Exprs(definitionsPath)
	return slices.ContainsFunc(exprs, (*expr.Expr).Relative)
}

// settleEnv evaluates the workflow's env: value k, if it isn't evaluated
// yet, after those it reads: as the values are filled in, only once ready
// reports that each pattern of what it reads of them is (settle), and once
// they are, with a nil ready, whatever it gives. visiting holds the values
// that are under way, or were: one that reads itself, through others,
// isn't evaluated, and reads as not evaluated yet.
func (st *starter) settleEnv(k string, visiting map[string]bool, ready func(pattern []string) bool) {
	if !st.s.pendingEnv[plan.JoinPath("env", k)] || visiting[k] || ready != nil && st.late[k] {
		return
	}
	visiting[k] = true
	exprs := st.p.Run.Env[k].Exprs()
	for _, j := range st.envRefs(exprs) {
		st.settleEnv(j, visiting, ready)
	}
	if ready != nil && slices.ContainsFunc(readPatterns(exprs, nil), func(p []string) bool { return !ready(p) }) {
		return
	}
	st.evalEnvValue(k, ready != nil)
}

// envReadsOf returns the patterns (reach.Patterns) of the paths in the
// inputs' values that the env: values exprs read, of those not evaluated
// yet, read, and that those they read read, in turn.
func (st *starter) envReadsOf(exprs []*expr.Expr) [][]string {
	if len(st.s.pendingEnv) == 0 {
		return nil
	}
	var out [][]string
	for _, k := range st.envRefs(exprs) {
		if !st.s.pendingEnv[plan.JoinPath("env", k)] {
			continue
		}
		reads, ok := st.envPatterns[k]
		if !ok {
			seen := map[string]bool{}
			var walk func(k string)
			walk = func(k string) {
				if seen[k] || st.late[k] || !st.s.pendingEnv[plan.JoinPath("env", k)] {
					return
				}
				seen[k] = true
				e := st.p.Run.Env[k].Exprs()
				reads = append(reads, readPatterns(e, nil)...)
				for _, j := range st.envRefs(e) {
					walk(j)
				}
			}
			walk(k)
			st.envPatterns[k] = reads
		}
		out = append(out, reads...)
	}
	return out
}

// envRefs returns the names of the workflow's env: values that exprs
// read, in order: all of them, for one that reads env whole.
func (st *starter) envRefs(exprs []*expr.Expr) []string {
	var out []string
	for _, e := range exprs {
		for _, ref := range e.Refs() {
			if ref.Context != "env" {
				continue
			}
			if len(ref.Path) == 0 {
				return st.envKeys
			}
			if _, ok := st.p.Run.Env[ref.Path[0]]; ok {
				out = append(out, ref.Path[0])
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// waits reports whether err is an evaluation that read what Start hasn't
// evaluated yet: an input's value, or a part of one, or an env value.
func waits(err error) bool {
	u, ok := errors.AsType[*expr.UnknownError](err)
	return ok && (u.Name == "inputs" || u.Name == "env")
}

// evalEnvValue evaluates the workflow's env: value k, and reports whether
// it did, or failed: not if it read what isn't evaluated yet, nor, for one
// demanded as the values are filled in, if it failed. What it reports is
// Start's own, which a later round, which reads the value as it is,
// doesn't report again.
func (st *starter) evalEnvValue(k string, demand bool) bool {
	r := st.r
	out, read, err := st.p.Run.Env[k].EvalRead(r.scope())
	switch {
	case waits(err), demand && err != nil:
		return false
	case err != nil:
		n := len(r.diags)
		st.reportEval([]string{"env", k}, err, read)
		st.diags = append(st.diags, r.diags[n:]...)
		r.diags = r.diags[:n]
	}
	name := plan.JoinPath("env", k)
	text := ""
	if err == nil {
		text = expr.Text(out)
	}
	st.s.run.Env[k] = text
	delete(st.s.pendingEnv, name)
	if r.view != nil {
		// The scope's contexts are the evaluators', which read the value
		// from here on.
		env, _ := r.budget.Contexts["env"].(map[string]any)
		if env != nil {
			env[k] = text
		}
	}
	if read.Private {
		r.marks.Add([]string{"env", k})
		if read.Marked {
			st.s.secrets.Add([]string{"env", k})
			st.s.madeSecret[name] = true
		} else {
			st.s.madePrivate[name] = true
		}
	}
	r.evaluators = nil
	r.forgetMeant(nil)
	if demand {
		st.demanded[k] = demandedEnv{text, read}
	}
	return true
}

// runValues returns the run's values as Start completed them, with what of
// them is private, a secret and derived, by name: what the plan names so,
// and what the resolution kept or made so.
func (st *starter) runValues() plan.RunValues {
	p, s, r := st.p, st.s, st.r
	run := s.run
	run.Format = p.Run.Format
	run.Env = maps.Clone(s.run.Env)
	if len(run.Env) == 0 {
		run.Env = nil
	}
	run.Inputs = plan.DataOf(r.values)
	secrets := setOf(p.Run.Secrets)
	maps.Copy(secrets, st.secretVariables)
	maps.Copy(secrets, s.madeSecret)
	for k := range r.secrets {
		secrets["inputs."+k] = true
	}
	for k := range r.secretsLater {
		secrets["inputs."+k] = true
	}
	derived := setOf(p.Run.Derived)
	maps.Copy(derived, s.madeSecret)
	for k := range r.derived {
		derived["inputs."+k] = true
	}
	private := map[string]bool{}
	for k := range r.private {
		private["inputs."+k] = true
	}
	if st.becameSecret() {
		// What the compiler held as private, in the place of what a variable
		// that is a secret now makes, is masked from here on as what is made
		// from one, with whatever else it held so, as the plan doesn't say
		// which.
		for _, name := range p.Run.Private {
			secrets[name], derived[name] = true, true
		}
	} else {
		for _, name := range p.Run.Private {
			private[name] = true
		}
	}
	maps.Copy(private, s.madePrivate)
	for name := range private {
		if secrets[name] {
			delete(private, name)
		}
	}
	run.Secrets = slices.Sorted(maps.Keys(secrets))
	run.Private = slices.Sorted(maps.Keys(private))
	run.Derived = slices.Sorted(maps.Keys(derived))
	return run
}

// becameSecret reports whether a variable the plan names as one that isn't
// a secret is one as the run starts.
func (st *starter) becameSecret() bool {
	wasSecret := setOf(st.p.Run.Secrets)
	for _, name := range st.p.Run.Variables {
		path := plan.SplitPath(name)
		for secret := range st.secretVariables {
			if wasSecret[secret] {
				continue
			}
			if secret == name || len(path) == 1 && plan.SplitPath(secret)[0] == path[0] {
				return true
			}
		}
	}
	return false
}

// expand expands each matrix whose values are known as the run starts,
// with plan.Expand, as Ready expands one that reads needs, into res.
func (st *starter) expand(res *StartResult, scope expr.Scope) error {
	for i := range st.p.Jobs {
		j := &st.p.Jobs[i]
		if j.Strategy == nil {
			continue
		}
		x, err := plan.Expand(st.ctx, j, scope, st.p.Run.Limits)
		switch {
		case err == nil:
			if res.Expansions == nil {
				res.Expansions = map[string]*plan.Expansion{}
			}
			res.Expansions[j.Name] = x
		case st.ctx.Err() != nil:
			return st.ctx.Err()
		case expr.IsUnknown(err):
			// It reads needs, and Ready expands it.
		default:
			if xe, ok := errors.AsType[*plan.ExpandError](err); ok {
				st.report(xe.Kind, xe.Path, xe.Params)
				continue
			}
			// Expand shows no value made from a private one.
			loc, _ := jsonptr.Split(jsonptr.Append(j.Path, "strategy"))
			st.reportEval(loc, err, expr.Read{})
		}
	}
	return nil
}

// sessions returns the workflow's sessions:, evaluated in scope, the run's,
// for the session provider, and what evaluating them read; or nil if it has
// none, or there is no session provider (resolver_missing), or evaluating
// them failed, which it reports as it reports what env: gives.
func (st *starter) sessions(scope expr.Scope) (map[string]any, expr.Read) {
	if m, _ := st.p.Run.Sessions.Written().(map[string]any); len(m) == 0 {
		return nil, expr.Read{}
	}
	if st.in.Sessions == nil {
		st.report(problems.ResolverMissing, "/sessions", problems.Params{"key": "sessions", "interface": "Sessions"})
		return nil, expr.Read{}
	}
	out, read, err := st.p.Run.Sessions.EvalRead(scope)
	if err != nil {
		st.reportEval([]string{"sessions"}, err, read)
		return nil, read
	}
	m, _ := out.(map[string]any)
	return m, read
}

// sessionsError reports err, the session provider's error making the
// run's sessions, as resolverError does; but of sessions that read a
// variable or a password as they were evaluated (read.Private), it shows
// nothing of what they gave, which the provider's error can quote: a
// refusal's params are masked, and any other error, which says the
// provider couldn't make them, is one that says only that
// (sessionsNotMade).
func (st *starter) sessionsError(err error, read expr.Read) error {
	if !read.Private {
		return st.resolverError(err, "/sessions")
	}
	if ctxErr := st.ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if k, params, ok := workflowProblem(err); ok {
		st.report(k, "/sessions", mask(params))
		return nil
	}
	return &sessionsNotMade{ctx: contextEnd(err)}
}

// sessionsNotMade is the error of a session provider that couldn't make
// sessions that read a variable or a password. It says nothing of the
// provider's error, which can quote what they gave, and wraps only the
// context's error, where the context was done, so that the run is started
// again after it, as after any resolver that couldn't answer.
type sessionsNotMade struct{ ctx error }

func (e *sessionsNotMade) Error() string {
	return "compile: the session provider couldn't make the run's sessions"
}

func (e *sessionsNotMade) Unwrap() error { return e.ctx }

// reportEval reports err, an error evaluating what is at loc in the
// workflow, which read what read says: with the values it shows masked if
// it read a variable or a password. Reading a context that isn't known as
// the run starts, as an input's definition that reads needs does, is
// context_not_known.
func (st *starter) reportEval(loc []string, err error, read expr.Read) {
	if u, ok := errors.AsType[*expr.UnknownError](err); ok {
		pointer := jsonptr.Format(loc)
		if u.Name == "status" {
			st.report(problems.StatusNotKnown, pointer, problems.Params{"phase": plan.PhaseStart.String()})
			return
		}
		st.report(problems.ContextNotKnown, pointer, problems.Params{"context": u.Name, "phase": plan.PhaseStart.String()})
		return
	}
	st.r.reportError(loc, 0, maskedIf(read.Private, err))
}

// report reports a problem of kind k at pointer, a JSON Pointer in the
// workflow, with params: the run's, so with no file position.
func (st *starter) report(k *problems.Kind, pointer string, params problems.Params) {
	st.diags = append(st.diags, k.At("", problems.Position{}, pointer, params))
}

// resolverError reports err, an error from a resolver Start asked about
// what is at pointer: a problem of one of the kinds of error a workflow
// can have is the run's, which it reports, and it returns nil; any other
// error, the resolver couldn't answer, which it returns.
func (st *starter) resolverError(err error, pointer string) error {
	if ctxErr := st.ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if k, params, ok := workflowProblem(err); ok {
		st.report(k, pointer, params)
		return nil
	}
	return err
}

// diagnostics returns Start's diagnostics, in order, each once, and the
// first of each kind (listed).
func (st *starter) diagnostics() problems.Diagnostics {
	diags := slices.Clone(st.diags)
	if st.r != nil {
		diags = append(diags, st.r.diags...)
	}
	diags.Sort()
	return listed(diags)
}

// fieldAt returns the first input whose value is at path, or nil.
func (r *inputResolver) fieldAt(path []string) *field {
	var out *field
	for f := range r.fieldsAt().Reached(path) {
		if slices.Equal(f.path, path) && (out == nil || f.seq < out.seq) {
			out = f
		}
	}
	return out
}

// setOf returns the set of the strings list holds.
func setOf(list []string) map[string]bool {
	set := make(map[string]bool, len(list))
	for _, s := range list {
		set[s] = true
	}
	return set
}

// setGrow sets the value at path in m, making the mappings on the way to
// it that aren't there, and a list's item at its index if it is the item
// after the list's last. Any other index of a list is an error, which
// changes nothing.
func setGrow(m map[string]any, path []string, v any) error {
	_, err := grow(m, path, v)
	return err
}

// grow returns c with v at path in it: c itself, changed, when it is a
// mapping, or a list that has room for the item, which only a list's last
// item, after the rest, doesn't. It changes nothing on the way to an
// error.
func grow(c any, path []string, v any) (any, error) {
	if len(path) == 0 {
		return v, nil
	}
	key := path[0]
	m, isMap := c.(map[string]any)
	if l, isList := c.([]any); isList || !isMap && isListIndex(key) {
		n, err := strconv.Atoi(key)
		if err != nil || n < 0 || n > len(l) || strconv.Itoa(n) != key {
			return nil, mismatch("an item %s of a list of %d", key, len(l))
		}
		if n == len(l) {
			l = append(l, nil)
		}
		item, err := grow(l[n], path[1:], v)
		if err != nil {
			return nil, err
		}
		l[n] = item
		return l, nil
	}
	if !isMap {
		m = map[string]any{}
	}
	item, err := grow(m[key], path[1:], v)
	if err != nil {
		return nil, err
	}
	m[key] = item
	return m, nil
}

// isListIndex reports whether key is a list's index as a path writes it: 0,
// or digits that don't start with 0.
func isListIndex(key string) bool {
	n, err := strconv.Atoi(key)
	return err == nil && n >= 0 && strconv.Itoa(n) == key
}

func orEmptyMap(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}
