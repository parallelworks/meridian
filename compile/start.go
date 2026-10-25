package compile

import (
	"cmp"
	"container/heap"
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
	"github.com/parallelworks/meridian/internal/reach"
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
	// Record is the run's secret record as Create gave it
	// (Created.Record). Start puts each password and private value at the
	// path plan.SplitPath reads from its name, as in inputs.creds.0.pass
	// for the field pass of the list creds's first row, an outer one
	// before those in it, and each part of the input definitions the plan
	// withheld back in its place; and reads what each decision of the
	// run's shape decided, and the ignored inputs' values, as it decides
	// them again. A record that doesn't fit the plan is an error.
	Record Record
	// Inputs provides the input types that name a platform's resources, as
	// Options.Inputs does when the run is created. Nil means there is no
	// platform.
	Inputs InputResolver
	// Sessions makes the run's sessions. Nil means there is no session
	// provider: a run with sessions can't start.
	Sessions Sessions
}

// StartResult is what Start gives: the run's values, complete.
//
// What goes in the run's secret record of it is what is private: the
// record half of Run.Split, and the timeout and the expansions when they
// are private. The rest is run state.
type StartResult struct {
	// Run is what the phases after Start read of the run, whole: the plan's
	// input values with Start's evaluated and each password and private
	// value at its path, the workflow's env:, every variable Start read,
	// plain and secret, and in Run.Private and Run.Secrets what of it is
	// private, and secret. The coordinator keeps the private part only in
	// the run's secret record, and the rest as run state too
	// (plan.RunValues.Split).
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

// Start is the Start phase: it completes what p leaves to the run's start,
// as docs/design.md describes under Starting a run. It reads the variables
// p lists, through in.Variables, once for the run, secret ones included,
// and puts the run's passwords and private values in their places in the
// inputs' values. It evaluates the parts of the inputs p holds for Start
// (plan.Run.DeferredParts) where the input definitions write them, as the
// compiler evaluates them, with the variables, the passwords and the
// workflow's env:, which it evaluates too, each once, in the order they
// read each other, and fills in the fields of the rows of each list it
// evaluated a part of, as the compiler fills in a row's. It decides again
// each input's condition that read a variable or a password
// (plan.Run.Decisions), reading the inputs as they were given, as format 1
// reads them, as the compiler's evaluations did. It then checks each input it
// evaluated as Compile checks one; decides again each job's needs: that
// read a variable or a password (plan.Job.NeedsExpr), reporting
// run_shape_changed for a decision that differs from what the run's
// secret record, or p for a job's needs, says Create decided;
// evaluates the workflow's timeout:; and expands each matrix whose values
// are known by then, with plan.Expand and p's Run.Limits. Only once all of
// that has passed does it make the run's sessions, through in.Sessions,
// so a run that fails to start makes none. A rerun calls Start again, with
// the same plan, and reads every variable again.
//
// A relative reference in what it evaluates reads the input definitions,
// which the plan holds; one that climbs out of them reads nothing, where
// the compiler read the rest of the workflow.
//
// What it made from a variable or a password is private, and what it made
// from a secret is a secret too, by its name, in the values it gives
// (plan.RunValues.Private and Secrets), beside what p names so: the
// variables it read, the passwords, and the values the run was given in
// the place of what is made from one, which p names as derived
// (plan.Run.Derived). A variable the Variables interface gives as a secret
// is one, and so is one p names as a secret though the interface no
// longer does.
//
// The coordinator runs Start as the run's first job, and stores what it
// gives in the transaction that ends that job. Given the same plan, input
// and resolver answers, it gives the same result, byte for byte, so a
// coordinator that dies during Start leaves another to start it again.
//
// Its diagnostics are the run's problems, with no file position, which
// never show a value made from a variable or a password; an error among
// them fails the run, and then there is no result. It returns an error,
// and no result, when it couldn't finish for a reason that isn't the
// run's: a resolver that couldn't answer, a plan that isn't one the
// compiler made, input that doesn't fit it, such as a password p doesn't
// name, or ctx done, with ctx.Err(). The coordinator starts the run again
// then.
func Start(ctx context.Context, p *plan.Plan, in StartInput) (*StartResult, problems.Diagnostics, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if p == nil || p.Version != plan.Version {
		return nil, nil, errors.New("compile: Start needs a plan of this release's version")
	}
	st := &starter{ctx: ctx, p: p, in: in, failed: map[*field]bool{}, pending: map[*field]int{}, emptied: map[*field][]laterPart{}, above: map[*field]int{}}
	if err := st.readVariables(); err != nil {
		return nil, nil, err
	}
	if err := st.restore(); err != nil {
		return nil, nil, err
	}
	// An env: value that reads an input whose ignore Start decides again
	// waits for it, as the compiler evaluates the workflow's env: once it
	// has dropped the inputs it ignores.
	st.holdEnv = true
	if err := st.evaluate(false); err != nil {
		return nil, nil, err
	}
	if err := st.checkDecisions(); err != nil {
		return nil, nil, err
	}
	st.holdEnv = false
	if err := st.evaluate(true); err != nil {
		return nil, nil, err
	}
	st.checkInputs()
	st.checkNeeds()
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
	sessions := st.sessions()
	if sessions != nil && st.in.Sessions == nil {
		st.report(problems.ResolverMissing, "/sessions", problems.Params{"key": "sessions", "interface": "Sessions"})
	}
	diags := st.diagnostics()
	if diags.HasErrors() {
		return nil, diags, nil
	}
	if sessions != nil {
		if err := st.in.Sessions.Make(ctx, st.in.RunID, sessions, maps.Clone(p.Run.SessionNames)); err != nil {
			if err := st.resolverError(err, "/sessions"); err != nil {
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

// A starter is a run as Start completes its values.
type starter struct {
	ctx context.Context
	p   *plan.Plan
	in  StartInput
	// r is the input resolver, over the plan's input definitions, which
	// evaluates what the plan holds for Start, and s what it keeps of the
	// start (r.start).
	r *inputResolver
	s *startState
	// deferred are the inputs whose values the plan holds for Start, in
	// order.
	deferred []*field
	// secretVariables names the variables that are secrets: those the
	// Variables interface gives as secrets, and those the plan names so.
	secretVariables map[string]bool
	// envKeys are the workflow's env: names, in order.
	envKeys []string
	// failed holds the inputs a part of whose value failed to evaluate,
	// which aren't checked then; pending, how many parts of each input's
	// value Start has yet to evaluate; emptied, the defaults among them
	// that gave no value, whose containers Start removes once the input's
	// parts are evaluated, if they are empty; and above, how many of the
	// mappings on the way to each input held for Start the plan's values
	// didn't have, which holding it made.
	failed  map[*field]bool
	pending map[*field]int
	emptied map[*field][]laterPart
	above   map[*field]int
	// hiddenAt and optionalAt hold whether each input Start evaluated, and
	// each group it is in, is hidden and optional, as checkDecisions
	// decided them, in the inputs' order, as the compiler does; decisionAt
	// holds the JSON Pointers of the decisions the plan names.
	hiddenAt, optionalAt map[*field]bool
	decisionAt           map[string]bool
	// holdEnv is set while Start evaluates what it does before it decides
	// the run's shape again, when an env: value that reads one of shaped,
	// the inputs whose ignore it decides again, waits (readsShaped).
	holdEnv bool
	shaped  *reach.Trie[struct{}]
	// diags are the problems Start found of its own, beside those the
	// input resolver reports.
	diags problems.Diagnostics
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
	// shared holds, by the JSON Pointer of a part's expressions, what they
	// read, for those that give the same for each row of a list (sharedPart),
	// or nil for those that don't; memo what each of those gave, until
	// what it read changes (forget); and memoAt the same by the patterns
	// of what each read.
	shared map[string]*sharedExprs
	memo   map[string]*memoized
	memoAt reach.Trie[*memoized]
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
	st.s = &startState{madeSecret: map[string]bool{}, madePrivate: map[string]bool{}}
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
		values := &st.s.run.Org
		if context == "vars" {
			values = &st.s.run.Vars
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

// restore makes the input resolver over the plan's input definitions, with
// the inputs' values the run has: the plan's, those it holds for Start,
// with each part that Start evaluates unknown until it is, and each
// password and private value at its path.
func (st *starter) restore() error {
	p := st.p
	defs, _ := p.Run.Definitions.Written().(map[string]any)
	defs = orEmptyMap(defs)
	// What the plan withheld of the definitions, which can hold a
	// password, the secret record gives back.
	withheld, err := recordFor(p.Run.Withheld, st.in.Record.Withheld, "part of the input definitions")
	if err != nil {
		return err
	}
	for _, pointer := range p.Run.Withheld {
		loc, err := jsonptr.Split(pointer)
		if err != nil || len(loc) <= len(definitionsPath) || !slices.Equal(loc[:len(definitionsPath)], definitionsPath) {
			return fmt.Errorf("compile: the plan withholds %s, which isn't in the input definitions", pointer)
		}
		if err := setGrow(defs, loc[len(definitionsPath):], jsonvalue.Clone(withheld[pointer])); err != nil {
			return err
		}
	}
	doc := &format.Document{Format: p.Run.Format, Value: map[string]any{
		"on": map[string]any{"execute": map[string]any{"inputs": defs}},
	}}
	r := newInputResolver(doc, nil, Options{Inputs: st.in.Inputs})
	r.start = st.s
	st.r = r
	r.fields = r.collect(defs, definitionsPath, nil, nil)
	for i, f := range r.fields {
		f.seq = i
	}

	values, _ := jsonvalue.Clone(p.Run.Inputs.Get()).(map[string]any)
	if values == nil {
		values = map[string]any{}
	}
	held := slices.Sorted(maps.Keys(p.Run.DeferredInputs))
	var holders reach.Trie[*field]
	for _, name := range held {
		f := r.fieldAt(plan.SplitPath(name))
		if f == nil {
			return fmt.Errorf("compile: the plan holds the input %s for Start, which its definitions don't have", name)
		}
		holders.Add(f.path, f)
		for n := len(f.path) - 1; n > 0; n-- {
			if _, ok := jsonptr.Lookup(values, f.path[:n]); ok {
				break
			}
			st.above[f]++
		}
		if err := setGrow(values, f.path, p.Run.DeferredInputs[name].Written()); err != nil {
			return err
		}
	}
	// What the run was given goes in its place, an outer one before those
	// in it, as a password in a list held as derived, or as private.
	secrets, private := setOf(p.Run.Secrets), setOf(p.Run.Private)
	type given struct {
		name string
		path []string
		v    any
	}
	var gave []given
	for _, from := range []struct {
		values map[string]any
		names  map[string]bool
		what   string
	}{{st.in.Record.Passwords, secrets, "password"}, {st.in.Record.Private, private, "private value"}} {
		for name, v := range from.values {
			path := plan.SplitPath(name)
			if len(path) < 2 || path[0] != "inputs" || !from.names[name] {
				return fmt.Errorf("compile: the plan names no %s %s", from.what, name)
			}
			gave = append(gave, given{name, path[1:], v})
		}
	}
	slices.SortFunc(gave, func(a, b given) int {
		return cmp.Or(cmp.Compare(len(a.path), len(b.path)), slices.Compare(a.path, b.path), cmp.Compare(a.name, b.name))
	})
	for _, g := range gave {
		if err := setGrow(values, g.path, jsonvalue.Clone(g.v)); err != nil {
			return err
		}
	}

	r.values = values

	// What is private and what is a secret are marked by the names the
	// plan gives them, and the variables by their contexts.
	for name := range st.secretVariables {
		st.s.secrets.Add(plan.SplitPath(name))
	}
	for _, f := range r.fields {
		if f.typ == "password" {
			r.mark(f.path)
			st.s.secrets.Add(slices.Concat([]string{"inputs"}, f.path))
		}
	}
	for _, names := range [][]string{p.Run.Secrets, p.Run.Private} {
		for _, name := range names {
			if path := plan.SplitPath(name); path[0] == "inputs" && len(path) > 1 {
				r.mark(path[1:])
				if secrets[name] {
					st.s.secrets.Add(path)
				}
			}
		}
	}

	// Each part held for Start is a part of its input's value that Start
	// evaluates, which is unknown until it does. The input is the one held
	// whose path leads to the part's, by the trie of their paths.
	parted := map[*field]bool{}
	for _, part := range p.Run.DeferredParts {
		path := plan.SplitPath(part.Name)
		loc, err := jsonptr.Split(part.Location)
		if err != nil || len(path) < 2 || path[0] != "inputs" || len(loc) <= len(definitionsPath) || !slices.Equal(loc[:len(definitionsPath)], definitionsPath) {
			return fmt.Errorf("compile: the plan's part %s, at %s, isn't one the compiler holds for Start", part.Name, part.Location)
		}
		var f *field
		for g := range holders.Reached(path[1:]) {
			if isPrefix(g.path, path[1:]) && (f == nil || len(g.path) > len(f.path) || len(g.path) == len(f.path) && g.seq < f.seq) {
				f = g
			}
		}
		if f == nil {
			return fmt.Errorf("compile: the plan's part %s is in no input it holds for Start", part.Name)
		}
		f.addLater(laterPart{path: path[1+len(f.path):], loc: loc, index: slices.Clone(part.Index), dflt: part.Default, text: part.Text, null: part.Null, containers: part.Containers})
		parted[f] = true
		st.pending[f]++
	}
	for _, f := range r.fields {
		if parted[f] {
			st.deferred = append(st.deferred, f)
		}
	}
	// What the run ignored has no value, as the compiler left it.
	for name := range jsonvalue.Sorted(st.in.Record.Ignored) {
		var f *field
		if path := plan.SplitPath(name); len(path) > 1 && path[0] == "inputs" {
			f = r.fieldAt(path[1:])
		}
		if f == nil {
			return fmt.Errorf("compile: the secret record names an ignored input %s the definitions don't have", name)
		}
		if _, has := jsonptr.Lookup(values, f.path); has {
			return fmt.Errorf("compile: the plan holds a value of the ignored input %s", name)
		}
		f.ignored = true
	}
	if len(st.deferred) != len(held) {
		return errors.New("compile: the plan holds an input for Start without any of its parts")
	}
	// Every value the plan names that Start doesn't evaluate is in the
	// secret record.
	for _, from := range []struct {
		names  []string
		values map[string]any
	}{{p.Run.Secrets, st.in.Record.Passwords}, {p.Run.Private, st.in.Record.Private}} {
		for _, name := range from.names {
			path := plan.SplitPath(name)
			if _, ok := from.values[name]; ok || path[0] != "inputs" {
				continue
			}
			if !slices.ContainsFunc(held, func(h string) bool {
				at := plan.SplitPath(h)
				return isPrefix(at, path[1:]) || isPrefix(path[1:], at)
			}) {
				return fmt.Errorf("compile: the secret record holds no value of %s, which the plan names", name)
			}
		}
	}
	// The rows of each list are known as they were to the compiler, which
	// read a row's field that has no value as null (evalValues): those of a
	// list Start evaluates a part of it collects again once it has
	// (fillRows).
	var collect func(fields []*field)
	collect = func(fields []*field) {
		for _, f := range fields {
			collect(r.collectRows(f))
		}
	}
	collect(r.fields)

	st.envKeys = slices.Sorted(maps.Keys(p.Run.Env))
	st.s.pendingEnv = map[string]bool{}
	for _, k := range st.envKeys {
		st.s.pendingEnv[plan.JoinPath("env", k)] = true
	}
	st.s.run = plan.RunValues{
		Format: p.Run.Format, Org: st.s.run.Org, Vars: st.s.run.Vars, Env: map[string]string{},
		Remote: maps.Clone(p.Run.Remote), Sessions: maps.Clone(p.Run.SessionNames),
	}
	return nil
}

// recordFor returns what the secret record holds, given, for each of the
// names the plan gives, by them, or an error if it holds another, or none
// for one: a record that doesn't fit the plan.
func recordFor[V any](names []string, given map[string]V, what string) (map[string]V, error) {
	out := make(map[string]V, len(names))
	for _, name := range names {
		v, ok := given[name]
		if !ok {
			return nil, fmt.Errorf("compile: the secret record holds no %s %s", what, name)
		}
		out[name] = v
	}
	for name := range given {
		if _, ok := out[name]; !ok {
			return nil, fmt.Errorf("compile: the plan names no %s %s", what, name)
		}
	}
	return out, nil
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

// isPrefix reports whether path is prefix or is under it.
func isPrefix(prefix, path []string) bool {
	return len(prefix) <= len(path) && slices.Equal(prefix, path[:len(prefix)])
}

// setOfFields returns the set of fields.
func setOfFields(fields []*field) map[*field]bool {
	set := make(map[*field]bool, len(fields))
	for _, f := range fields {
		set[f] = true
	}
	return set
}

// setOf returns the set of the strings list holds.
func setOf(list []string) map[string]bool {
	set := make(map[string]bool, len(list))
	for _, s := range list {
		set[s] = true
	}
	return set
}

// A startNode is what Start evaluates of what the plan left to it: the
// i'th part of f's value, or the workflow's env: value env.
type startNode struct {
	f   *field
	i   int
	env string
}

// evaluate evaluates what the plan holds for Start, the parts of the
// inputs' values and the workflow's env:, each once, in the order they
// read each other, as the compiler fills in the inputs (pendingFields):
// one is evaluated once nothing its expressions read waits any longer,
// what they read through a definition, as a relative reference does,
// among it (nodeExprs). One that reads what waits in a way the order can't
// see is evaluated again once what it read is, in another round. Once
// final, what is left once a round evaluates nothing reads itself, or each
// other. It returns ctx's error once ctx is done.
func (st *starter) evaluate(final bool) error {
	for {
		nodes := st.pendingNodes()
		if len(nodes) == 0 || !st.evaluateInOrder(nodes) {
			break
		}
	}
	if err := st.ctx.Err(); err != nil {
		return err
	}
	if !final {
		return nil
	}
	var left []string
	for _, n := range st.pendingNodes() {
		if n.f == nil {
			left = append(left, plan.JoinPath("env", n.env))
			continue
		}
		left = append(left, plan.JoinPath(slices.Concat([]string{"inputs"}, n.f.path, n.f.later[n.i].path)...))
	}
	if len(left) > 0 {
		st.report(problems.ValuesReadEachOther, "", problems.Params{"names": problems.QuoteAll(left)})
	}
	return nil
}

// pendingNodes returns what Start has yet to evaluate, in order: the
// parts of each input, in the order of the inputs and of their parts, and
// then the env: values, in the order of their names.
func (st *starter) pendingNodes() []startNode {
	var out []startNode
	for _, f := range st.deferred {
		for i, p := range f.later {
			if !p.variable {
				out = append(out, startNode{f: f, i: i})
			}
		}
	}
	for _, k := range st.envKeys {
		if st.s.pendingEnv[plan.JoinPath("env", k)] {
			out = append(out, startNode{env: k})
		}
	}
	return out
}

// A nodeGroup is the nodes that a pattern reaches, such as a list's parts,
// or all the env: values, which each node that reads them all waits for
// as one: a node waits once for each group it reads, not once for each of
// the group's nodes, so that each of a list's rows reading another list
// adds a wait for each row, not one for each row of each list. left is how
// many of the nodes aren't evaluated yet. A reader that is one of them
// waits for the others (selfReaders): it is ready once it is the one left.
type nodeGroup struct {
	members, readers, selfReaders []int
	left                          int
}

// evaluateInOrder evaluates nodes in the order they read each other, as
// their expressions say, and then once each of those left, as one that
// reads what waits through a definition is; and reports whether it
// evaluated any.
func (st *starter) evaluateInOrder(nodes []startNode) bool {
	var at reach.Trie[int]
	envAt := map[string]int{}
	paths := make([][]string, len(nodes))
	for i, n := range nodes {
		if n.f != nil {
			paths[i] = slices.Concat(n.f.path, n.f.later[n.i].path)
			at.Add(paths[i], i)
		} else {
			envAt[n.env] = i
		}
	}
	waits := make([]int, len(nodes))
	readers := make([][]int, len(nodes))
	groupAt := map[string]*nodeGroup{}
	inGroups := make([][]*nodeGroup, len(nodes))
	// group returns the group of the nodes members makes, under key, made
	// once.
	group := func(key string, members func() []int) *nodeGroup {
		g, ok := groupAt[key]
		if !ok {
			g = &nodeGroup{members: members()}
			g.left = len(g.members)
			for _, j := range g.members {
				inGroups[j] = append(inGroups[j], g)
			}
			groupAt[key] = g
		}
		return g
	}
	for i, n := range nodes {
		exprs, index := st.nodeExprs(n)
		seen := map[int]bool{i: true}
		read := func(j int) {
			if !seen[j] {
				seen[j] = true
				readers[j] = append(readers[j], i)
				waits[i]++
			}
		}
		seenGroup := map[*nodeGroup]bool{}
		readAll := func(g *nodeGroup, self bool) {
			switch {
			case len(g.members) == 1:
				read(g.members[0])
			case seenGroup[g] || len(g.members) == 0:
			case self:
				seenGroup[g] = true
				g.selfReaders = append(g.selfReaders, i)
				waits[i]++
			default:
				seenGroup[g] = true
				g.readers = append(g.readers, i)
				waits[i]++
			}
		}
		for _, pattern := range readPatterns(exprs, index) {
			g := group("inputs"+jsonptr.Format(pattern), func() []int {
				members := slices.Collect(at.Reached(pattern))
				slices.Sort(members)
				return slices.Compact(members)
			})
			readAll(g, n.f != nil && reach.Reaches(pattern, paths[i]))
		}
		for _, e := range exprs {
			for _, ref := range e.Refs() {
				switch {
				case ref.Context != "env":
				case unnamed(ref.Path):
					readAll(group("env", func() []int { return slices.Sorted(maps.Values(envAt)) }), n.f == nil)
				default:
					if j, ok := envAt[ref.Path[0]]; ok {
						read(j)
					}
				}
			}
		}
	}
	var ready intHeap
	for i := range nodes {
		if waits[i] == 0 {
			heap.Push(&ready, i)
		}
	}
	done := make([]bool, len(nodes))
	release := func(j int) {
		if waits[j]--; waits[j] == 0 {
			heap.Push(&ready, j)
		}
	}
	progress := false
	for ready.Len() > 0 {
		if st.ctx.Err() != nil {
			return false
		}
		i := heap.Pop(&ready).(int)
		if !st.evalNode(nodes[i]) {
			continue
		}
		done[i], progress = true, true
		for _, j := range readers[i] {
			release(j)
		}
		for _, g := range inGroups[i] {
			switch g.left--; g.left {
			case 0:
				for _, j := range g.readers {
					release(j)
				}
			case 1:
				if len(g.selfReaders) > 0 {
					last := g.members[slices.IndexFunc(g.members, func(j int) bool { return !done[j] })]
					if slices.Contains(g.selfReaders, last) {
						release(last)
					}
				}
			}
		}
	}
	for i, n := range nodes {
		if st.ctx.Err() != nil {
			return false
		}
		if !done[i] && st.evalNode(n) {
			progress = true
		}
	}
	return progress
}

// nodeExprs returns the expressions n evaluates, and those of what they
// read through a definition (throughDefinitions), and the list rows they
// are for.
func (st *starter) nodeExprs(n startNode) ([]*expr.Expr, []int) {
	if n.f == nil {
		return st.p.Run.Env[n.env].Exprs(), nil
	}
	p := n.f.later[n.i]
	exprs, err := st.r.workflow.Exprs(p.loc)
	if err != nil {
		return nil, nil
	}
	return st.r.throughDefinitions(exprs, p.loc), p.index
}

// throughDefinitions returns exprs, the expressions at loc in the
// workflow, with those of what their relative references read, and of
// what those read in turn: what evaluating them reads through a
// definition, as ..region.default reads what the default of region does.
// One that reads into a mapping is taken as at the mapping.
func (r *inputResolver) throughDefinitions(exprs []*expr.Expr, loc []string) []*expr.Expr {
	type at struct {
		exprs []*expr.Expr
		loc   []string
	}
	// A copy once, as exprs are the workflow's.
	out := slices.Clip(exprs)
	seen := map[string]bool{jsonptr.Format(loc): true}
	queue := []at{{exprs, loc}}
	for len(queue) > 0 {
		q := queue[0]
		queue = queue[1:]
		for _, e := range q.exprs {
			for _, rel := range e.Relatives() {
				if rel.Up > len(q.loc) {
					continue
				}
				base := len(q.loc) - rel.Up
				target := slices.Concat(q.loc[:base], rel.Path)
				// What it reads is at the target, or in a string on the way.
				for n := len(target); n > base; n-- {
					if _, ok := jsonptr.Lookup(r.doc.Value, target[:n]); !ok {
						continue
					}
					if k := jsonptr.Format(target[:n]); !seen[k] {
						seen[k] = true
						found, err := r.workflow.Exprs(target[:n])
						if err == nil && len(found) > 0 {
							out = append(out, found...)
							queue = append(queue, at{found, target[:n]})
						}
					}
					break
				}
			}
		}
	}
	return out
}

// evalNode evaluates n, if it is still to be, and reports whether it did,
// or failed: not if it read what isn't evaluated yet.
func (st *starter) evalNode(n startNode) bool {
	if n.f == nil {
		return st.s.pendingEnv[plan.JoinPath("env", n.env)] && st.evalEnv(n.env)
	}
	return !n.f.later[n.i].variable && st.evalPart(n.f, n.i)
}

// intHeap is a heap of ints, the least first.
type intHeap []int

func (h intHeap) Len() int           { return len(h) }
func (h intHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h intHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *intHeap) Push(x any)        { *h = append(*h, x.(int)) }
func (h *intHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// waits reports whether err is an evaluation that read what Start hasn't
// evaluated yet: an input's value, or a part of one, or an env value.
func waits(err error) bool {
	u, ok := errors.AsType[*expr.UnknownError](err)
	return ok && (u.Name == "inputs" || u.Name == "env")
}

// A sharedExprs is what a part's expressions, and what they read through a
// definition, read of the inputs, by its patterns (reach.Patterns), for
// expressions that read neither a row index, nor a definition by a
// relative reference, nor an env value: they give the same for each row of
// a list while what they read doesn't change.
type sharedExprs struct{ patterns [][]string }

// A memoized is what evaluating a part's expressions, at key, gave, which
// each part with the same expressions takes while nothing they read
// changes, and the patterns of what they read.
type memoized struct {
	key      string
	v        any
	read     expr.Read
	patterns [][]string
}

// sharedPart returns the key of what evaluating p's expressions gives, for
// the part at path, if it is the same for each part with them
// (sharedExprs), and the part doesn't read itself, and what they read.
// Create compares what a form sent for each of a list's rows with what
// such a default gives once (evalCompare), and Start evaluates it once,
// rather than for each row, each of which can read all of another list.
func (st *starter) sharedPart(p laterPart, path []string) (string, [][]string, bool) {
	s := st.s
	key := jsonptr.Format(p.loc)
	shared, known := s.shared[key]
	if !known {
		if exprs, err := st.r.workflow.Exprs(p.loc); err == nil {
			exprs = st.r.throughDefinitions(exprs, p.loc)
			if !slices.ContainsFunc(exprs, func(e *expr.Expr) bool {
				return readsIndex(e) || slices.ContainsFunc(e.Refs(), func(ref expr.Ref) bool { return ref.Context == "env" })
			}) {
				shared = &sharedExprs{readPatterns(exprs, nil)}
			}
		}
		if s.shared == nil {
			s.shared = map[string]*sharedExprs{}
		}
		s.shared[key] = shared
	}
	if shared == nil || slices.ContainsFunc(shared.patterns, func(q []string) bool { return reach.Reaches(q, path) }) {
		return "", nil, false
	}
	return key, shared.patterns, true
}

// remember keeps what the expressions at key gave, which read what
// patterns reach, until it changes.
func (s *startState) remember(key string, v any, read expr.Read, patterns [][]string) {
	if old := s.memo[key]; old != nil {
		s.drop(old)
	}
	// A copy, as the value can be part of what the evaluation read.
	m := &memoized{key: key, v: jsonvalue.Clone(v), read: read, patterns: patterns}
	if s.memo == nil {
		s.memo = map[string]*memoized{}
	}
	s.memo[key] = m
	for _, q := range patterns {
		s.memoAt.Add(q, m)
	}
}

// forget drops what was remembered of evaluations that read the inputs'
// value at path, which changed, or was marked.
func (s *startState) forget(path []string) {
	if len(s.memo) == 0 {
		return
	}
	var stale []*memoized
	for m := range s.memoAt.Reaching(path) {
		stale = append(stale, m)
	}
	for _, m := range stale {
		s.drop(m)
	}
}

// drop drops m.
func (s *startState) drop(m *memoized) {
	if s.memo[m.key] != m {
		return
	}
	delete(s.memo, m.key)
	for _, q := range m.patterns {
		s.memoAt.Remove(q, m)
	}
}

// evalPart evaluates the i'th part of f's value held for Start, where the
// workflow writes it, and reports whether it did, or failed: not if it
// read what isn't evaluated yet. A part whose expressions give the same
// for each row of a list takes what they gave for another, while nothing
// they read has changed (sharedPart).
func (st *starter) evalPart(f *field, i int) bool {
	r := st.r
	p := f.later[i]
	path := slices.Concat(f.path, p.path)
	key, patterns, shared := st.sharedPart(p, path)
	var (
		v    any
		read expr.Read
	)
	if m := st.s.memo[key]; shared && m != nil {
		v, read = m.v, m.read
	} else {
		var err error
		if v, read, err = st.evalHeld(p, path); waits(err) {
			return false
		} else if err != nil {
			st.reportEval(p.loc, err, read)
			f.later[i].variable = true
			st.failed[f] = true
			st.pending[f]--
			return true
		}
		if shared {
			st.s.remember(key, v, read, patterns)
		}
	}
	f.later[i].variable = true
	switch {
	case p.dflt && (v == nil || v == "") && (p.null || p.text):
		// A default of "" or null gives no value, as when the run is
		// created (setDefault), and leaves the value as it was: null, or
		// the form's text.
		var was any
		if p.text {
			was = lookup(r.doc.Value, p.loc)
		}
		r.set(path, was)
	case p.dflt && (v == nil || v == ""):
		r.remove(path)
		if p.containers > 0 || len(p.path) == 0 && st.above[f] > 0 {
			st.emptied[f] = append(st.emptied[f], f.later[i])
		}
	default:
		// A copy, as the value can be part of what the evaluation read.
		r.set(path, jsonvalue.Clone(v))
	}
	st.s.made(r, path, read)
	if st.pending[f]--; st.pending[f] == 0 {
		st.removeEmpty(f)
		st.fillRows(f)
	}
	return true
}

// evalHeld evaluates p, the part of an input's value at path held for
// Start. What reads the value it gives reads it as the compiler's
// evaluation did, rather than as what isn't known yet: a form's text as it
// is, and what was missing as null, until it gives one.
func (st *starter) evalHeld(p laterPart, path []string) (any, expr.Read, error) {
	r := st.r
	r.scope()
	self := false
	if v, ok := jsonptr.Lookup(r.view, path); ok {
		if _, unknown := v.(expr.Unknown); unknown {
			var was any
			if p.text {
				was = lookup(r.doc.Value, p.loc)
			}
			setIn(r.view, path, was)
			r.evaluators, self = nil, true
		}
	}
	v, read, err := r.evalRead(p.loc, p.index)
	if err != nil && self {
		r.refresh(path)
	}
	return v, read, err
}

// removeEmpty removes the mappings on the way to each default of f's value
// that gave no value, which the plan held only to hold it, that are empty
// now (plan.DeferredPart.Containers), as the compiler never made them; and
// for one that is all of f's value, those on the way to f that holding f
// made.
func (st *starter) removeEmpty(f *field) {
	for _, p := range st.emptied[f] {
		path := slices.Concat(f.path, p.path)
		levels := p.containers
		if len(p.path) == 0 {
			levels = st.above[f]
		}
		for n := 1; n <= levels; n++ {
			at := path[:len(path)-n]
			if m, ok := lookup(st.r.values, at).(map[string]any); !ok || len(m) > 0 {
				break
			}
			st.r.remove(at)
		}
	}
	delete(st.emptied, f)
}

// evalEnv evaluates the workflow's env: value k, and reports whether it
// did, or failed: not if it read what isn't evaluated yet.
func (st *starter) evalEnv(k string) bool {
	r := st.r
	if st.holdEnv && st.readsShaped(k) {
		return false
	}
	out, read, err := st.p.Run.Env[k].EvalRead(r.scope())
	switch {
	case waits(err):
		return false
	case err != nil:
		st.reportEval([]string{"env", k}, err, read)
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
	return true
}

// fillRows fills in the values of the fields of f's rows, and of the rows
// of the lists in them, once Start has evaluated every part of f's value
// it holds: each row's fields take their defaults, as they do when the
// run is created (fillDefaults), so a row, or a list in a row, that a part
// gives has its fields filled in, as Create filled them in before the plan
// held the part's expression in their place. The rows are collected again
// from f's value as it is, so that a field that has a value keeps it. A
// default that reads what Start hasn't evaluated yet is a part of f's
// value that it evaluates once it has, after which it fills in f's rows
// again.
func (st *starter) fillRows(f *field) {
	r := st.r
	if defs, _ := f.rowInputs(); defs == nil {
		return
	}
	r.dropRows(f)
	f.rows = nil
	var pending pendingFields
	var start func(fields []*field)
	start = func(fields []*field) {
		for _, c := range fields {
			if r.prepare(c) {
				pending.add(c)
			} else {
				start(r.collectRows(c))
			}
		}
	}
	start(r.collectRows(f))
	for p := pending.next(); p != nil; p = pending.next() {
		r.evalPending(p.f)
		start(r.collectRows(p.f))
		pending.filled(p)
	}
	if left := pending.waiting(); len(left) > 0 {
		names := make([]string, len(left))
		for i, c := range left {
			names[i] = strings.Join(c.path, ".")
		}
		r.reportAt(problems.DefaultsReadEachOther, left[0].loc, problems.Params{"inputs": problems.QuoteAll(names)})
	}
}

// conditionKeys are an input's conditions, in the order the compiler
// evaluates them (applyConditions).
var conditionKeys = []string{"hidden", "ignore", "optional"}

// checkDecisions decides again each input's condition that read a
// variable or a password as the run was created (plan.Run.Decisions),
// where the input definitions write it, and reports run_shape_changed
// where one decides otherwise than the secret record says it did. It goes
// through the inputs in the order the compiler did, and reads them as the
// compiler's conditions read them: as they were given, with each ignored
// input's value, from the secret record (Record.Ignored), until it reaches
// the input, which it then drops. On the way, it decides whether
// each input Start evaluated, and each group it is in, is hidden and
// optional, which checkInputs reads. A decision of no input's condition,
// or a record that doesn't fit the plan, is an error.
func (st *starter) checkDecisions() error {
	r, p := st.r, st.p
	decided, err := recordFor(p.Run.Decisions, st.in.Record.Decisions, "decision")
	if err != nil {
		return err
	}
	byLoc := make(map[string]*field, len(r.fields))
	for _, f := range r.fields {
		if _, ok := byLoc[jsonptr.Format(f.loc)]; !ok {
			byLoc[jsonptr.Format(f.loc)] = f
		}
	}
	for _, pointer := range p.Run.Decisions {
		loc, err := jsonptr.Split(pointer)
		if err != nil || len(loc) <= len(definitionsPath) || byLoc[jsonptr.Format(loc[:len(loc)-1])] == nil || !slices.Contains(conditionKeys, loc[len(loc)-1]) {
			return fmt.Errorf("compile: the plan's decision %s is of no input's condition", pointer)
		}
	}
	// Each ignored input that had a value has it until the conditions
	// reach it (restore found them).
	dropped := map[*field]bool{}
	for name, v := range jsonvalue.Sorted(st.in.Record.Ignored) {
		if v == nil {
			continue
		}
		f := r.fieldAt(plan.SplitPath(name)[1:])
		// What reads it reads it as private, as it can be made from a
		// variable or a password.
		f.ignored = false
		r.set(f.path, jsonvalue.Clone(v))
		r.mark(f.path)
		dropped[f] = true
	}
	// Whether an input Start evaluated, or one whose being required a
	// decision Start makes again says, is hidden and optional, checkInputs
	// reads.
	wanted := map[*field]bool{}
	for _, f := range r.fields {
		if slices.Contains(st.deferred, f) || st.shapeDecided(f) {
			for g := f; g != nil; g = g.group {
				wanted[g] = true
			}
		}
	}
	st.hiddenAt, st.optionalAt = map[*field]bool{}, map[*field]bool{}
	for _, f := range r.fields {
		if f.group != nil && f.group.ignored {
			f.ignored = true
			continue
		}
		ignore := false
		for _, key := range conditionKeys {
			pointer := jsonptr.Format(appendPath(f.loc, key))
			want, isDecision := decided[pointer]
			if !isDecision && (!wanted[f] || key == "ignore") {
				continue
			}
			got, ok := st.condition(f, key)
			if ok && isDecision && got != want {
				st.report(problems.RunShapeChanged, pointer, problems.Params{"key": key, "name": f.label()})
			}
			switch key {
			case "hidden":
				st.hiddenAt[f] = got
			case "ignore":
				// The plan holds an input whose ignore read a variable
				// or a password whatever it decided, and Start drops it.
				ignore = isDecision && got
			case "optional":
				st.optionalAt[f] = got
			}
		}
		if dropped[f] || ignore {
			f.ignored = true
			r.drop(f)
		}
	}
	return nil
}

// readsShaped reports whether the workflow's env: value k reads an input
// whose ignore, or a group's around it, is a decision Start makes again.
func (st *starter) readsShaped(k string) bool {
	if st.shaped == nil {
		st.shaped = &reach.Trie[struct{}]{}
		if st.decisionAt == nil {
			st.decisionAt = setOf(st.p.Run.Decisions)
		}
		for _, f := range st.r.fields {
			for g := f; g != nil; g = g.group {
				if st.decisionAt[jsonptr.Format(appendPath(g.loc, "ignore"))] {
					st.shaped.Add(f.path, struct{}{})
					break
				}
			}
		}
	}
	for _, p := range readPatterns(st.p.Run.Env[k].Exprs(), nil) {
		for range st.shaped.Reached(p) {
			return true
		}
	}
	return false
}

// shapeDecided reports whether whether f is required is for Start to
// decide, as the compiler left it: whether f's optional, or its or a group
// around it's ignore or hidden, is a decision of the run's shape the plan
// names (inputResolver.shapeDecided).
func (st *starter) shapeDecided(f *field) bool {
	if st.decisionAt == nil {
		st.decisionAt = setOf(st.p.Run.Decisions)
	}
	decided := func(g *field, key string) bool {
		return st.decisionAt[jsonptr.Format(appendPath(g.loc, key))]
	}
	if decided(f, "optional") {
		return true
	}
	for g := f; g != nil; g = g.group {
		if decided(g, "ignore") || decided(g, "hidden") {
			return true
		}
	}
	return false
}

// checkNeeds evaluates again each job's needs: that read a variable or a
// password as the run was created (plan.Job.NeedsExpr), and reports
// run_shape_changed where one names other jobs than the plan's. It reads
// the inputs as the compiler's needs: read them.
func (st *starter) checkNeeds() {
	r := st.r
	scope := r.scope()
	jobs := map[string]bool{}
	for _, j := range st.p.Jobs {
		jobs[j.Name] = true
	}
	for _, j := range st.p.Jobs {
		if j.NeedsExpr.IsZero() {
			continue
		}
		pointer := jsonptr.Append(j.Path, "needs")
		v, read, err := j.NeedsExpr.EvalRead(scope)
		if err != nil {
			loc, _ := jsonptr.Split(pointer)
			st.reportEval(loc, err, read)
			continue
		}
		if !sameNeeds(v, j, jobs) {
			st.report(problems.RunShapeChanged, pointer, problems.Params{"key": "needs", "name": j.Name})
		}
	}
}

// sameNeeds reports whether v, what a job's needs: gives, is what j needs:
// the same jobs, each on all its instances or any one, as the compiler
// reads them (resolveNeeds), in any order. jobs holds the names of the
// run's jobs.
func sameNeeds(v any, j plan.Job, jobs map[string]bool) bool {
	var names []any
	switch v := v.(type) {
	case string:
		names = []any{v}
	case []any:
		names = v
	default:
		return false
	}
	got := map[string]bool{}
	for _, item := range names {
		s, ok := item.(string)
		if !ok {
			return false
		}
		job, anyInstance := strings.CutSuffix(s, ":any")
		if job == j.Name || !jobs[job] {
			return false
		}
		if _, seen := got[job]; !seen {
			got[job] = anyInstance
		}
	}
	if len(got) != len(j.Needs) {
		return false
	}
	for _, n := range j.Needs {
		if anyInstance, ok := got[n.Job]; !ok || anyInstance != n.Any {
			return false
		}
	}
	return true
}

// checkInputs checks each input Start evaluated as the compiler checks
// one: its type, whether it is required, and a platform type with the
// Inputs resolver; and each password field of the rows Start filled in.
func (st *starter) checkInputs() {
	r := st.r
	for _, f := range st.deferred {
		if st.failed[f] {
			continue
		}
		if !r.missing(f) {
			r.checkField(f, false)
			continue
		}
		// An input with no value and no default is required unless it is
		// optional, or hidden or in a hidden group; a boolean with none is
		// false.
		if f.container || f.header || f.hasDefault || f.repeats() {
			continue
		}
		switch {
		case st.optionalAt[f] || st.hidden(f):
		case f.typ == "boolean":
			r.set(f.path, false)
		default:
			r.reportAt(problems.InputRequired, f.loc, problems.Params{"input": f.label()})
		}
	}
	evaluated := setOfFields(st.deferred)
	// And each the compiler left to Start, as what decides whether it is
	// required read a variable or a password.
	for _, f := range r.fields {
		if evaluated[f] || f.ignored || !st.shapeDecided(f) || f.container || f.header || f.hasDefault || f.repeats() || !r.missing(f) {
			continue
		}
		switch {
		case st.optionalAt[f] || st.hidden(f):
		case f.typ == "boolean":
			r.set(f.path, false)
		default:
			r.reportAt(problems.InputRequired, f.loc, problems.Params{"input": f.label()})
		}
	}
	for _, c := range r.liveRows() {
		if c.typ == "password" && evaluated[c.in] {
			r.checkField(c, false)
		}
	}
}

// hidden reports whether f, or a group it is in, is hidden, as
// checkDecisions decided it.
func (st *starter) hidden(f *field) bool {
	for g := f; g != nil; g = g.group {
		if st.hiddenAt[g] {
			return true
		}
	}
	return false
}

// condition evaluates one of f's hidden, ignore and optional as the run
// starts, and reports whether it could: one that fails is reported.
func (st *starter) condition(f *field, key string) (decided, ok bool) {
	v, ok := f.def[key]
	if !ok {
		return false, true
	}
	if _, isString := v.(string); isString {
		loc := appendPath(f.loc, key)
		var read expr.Read
		var err error
		if v, read, err = st.r.evalRead(loc, f.index); err != nil {
			st.reportEval(loc, err, read)
			return false, false
		}
	}
	return conditionValue(v), true
}

// runValues returns the run's values as Start completed them, with what of
// them is private, a secret and derived, by name.
func (st *starter) runValues() plan.RunValues {
	p, s := st.p, st.s
	run := s.run
	run.Format = p.Run.Format
	run.Env = maps.Clone(s.run.Env)
	if len(run.Env) == 0 {
		run.Env = nil
	}
	run.Inputs = plan.DataOf(st.r.values)
	secrets := map[string]bool{}
	for _, name := range p.Run.Secrets {
		secrets[name] = true
	}
	maps.Copy(secrets, st.secretVariables)
	maps.Copy(secrets, s.madeSecret)
	// What Start made from a secret is masked as what is made from one,
	// not as a secret's own text: an env: value that says whether a secret
	// is set, true, would mask every true. One that is a secret's own text
	// is masked whole all the same, as the secret's.
	derived := setOf(p.Run.Derived)
	maps.Copy(derived, s.madeSecret)
	private := map[string]bool{}
	if st.becameSecret() {
		// What the compiler made from a variable that is a secret now is
		// masked from here on as what is made from one, with whatever else
		// it made from a variable, as the plan doesn't say which.
		for _, name := range p.Run.Private {
			secrets[name], derived[name] = true, true
		}
	} else {
		for _, name := range p.Run.Private {
			private[name] = true
		}
	}
	for name := range s.madePrivate {
		if !secrets[name] {
			private[name] = true
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

// sessions returns the workflow's sessions:, as it writes them, for the
// session provider, or nil if it has none.
func (st *starter) sessions() map[string]any {
	m, _ := st.p.Run.Sessions.Written().(map[string]any)
	if len(m) == 0 {
		return nil
	}
	return m
}

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

// setGrow sets the value at path in m, making the mappings on the way to
// it that aren't there, and a list's item at its index if it is the item
// after the list's last, as when the list is held whole and its items
// given apart. A list holds null in place of each item a plan doesn't
// hold, so any other index isn't one the plan names, an error.
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
			return nil, fmt.Errorf("compile: the plan names an item %s of a list of %d", key, len(l))
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
