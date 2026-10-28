package compile

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// What Create asks the resolvers that take a context, Variables, Groups,
// Sources and Sessions, which Compile reaches none of: the variables the
// workflow reads, the sessions it serves, the execution groups and
// environments its jobs name, and the workflows its steps' uses: names.

// withResolvers returns opts with the contexts what Create evaluates reads
// as the Variables and Sessions resolvers give them: each variable the
// workflow reads, a plain one's value and a secret one not known yet, in
// place of Contexts' org and vars; and the sessions context, as the session
// provider gives it for the workflow's sessions:. A workflow with sessions
// and no session provider is resolver_missing, unless Contexts gives the
// sessions context, as a caller that knows it does. A resolver's problem of
// a workflow's kind is the run's, among the diagnostics; any other error
// means it couldn't answer.
func withResolvers(ctx context.Context, doc *format.Document, opts Options) (Options, problems.Diagnostics, error) {
	var diags problems.Diagnostics
	report := func(k *problems.Kind, pointer string, params problems.Params) {
		diags = append(diags, k.At(doc.File, doc.ReportPosition(pointer), pointer, params))
	}
	contexts := maps.Clone(opts.Contexts)
	if contexts == nil {
		contexts = map[string]any{}
	}
	if opts.Variables != nil {
		names := variableReads(doc)
		contexts["org"], contexts["vars"] = map[string]any{}, map[string]any{}
		if len(names) > 0 {
			got, err := opts.Variables.Read(ctx, names, false)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return opts, nil, ctxErr
				}
				k, params, ok := workflowProblem(err)
				if !ok {
					return opts, nil, err
				}
				report(k, "", params)
			}
			for key, v := range got {
				context, name, ok := strings.Cut(key, ".")
				values, isContext := contexts[context].(map[string]any)
				if !ok || !isContext {
					continue
				}
				if v.Secret {
					values[name] = expr.Unknown{Name: context}
				} else {
					values[name] = v.Value
				}
			}
		}
	}
	if sessions := mapping(doc.Value["sessions"]); len(sessions) > 0 {
		switch {
		case opts.Sessions != nil:
			names, err := opts.Sessions.Check(ctx, jsonvalue.CloneMap(sessions))
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return opts, nil, ctxErr
				}
				k, params, ok := workflowProblem(err)
				if !ok {
					return opts, nil, err
				}
				report(k, "/sessions", params)
				break
			}
			context := make(map[string]any, len(names))
			for k, v := range names {
				context[k] = v
			}
			contexts["sessions"] = context
		case contexts["sessions"] == nil:
			report(problems.ResolverMissing, "/sessions", problems.Params{"key": "sessions", "interface": "Sessions"})
		}
	}
	opts.Contexts = contexts
	return opts, diags, nil
}

// variableReads returns the names of the variables the workflow's
// expressions read, as the Variables interface takes them: org.<name> or
// vars.<name>, the name as it is, or org or vars alone for a read of every
// variable of the context, as vars.* is, in order. An expression that
// doesn't parse reads nothing here: Create reports it where it is.
func variableReads(doc *format.Document) []string {
	whole, one := map[string]bool{}, map[string]bool{}
	note := func(e *expr.Expr) {
		for _, r := range e.Refs() {
			switch {
			case r.Context != "org" && r.Context != "vars":
			case unnamed(r.Path):
				whole[r.Context] = true
			default:
				one[r.Context+"."+r.Path[0]] = true
			}
		}
	}
	var walk func(v any)
	walk = func(v any) {
		switch v := v.(type) {
		case string:
			if !strings.Contains(v, "${{") {
				return
			}
			t, err := expr.ParseTemplate(v)
			if err != nil {
				return
			}
			for _, e := range t.Exprs() {
				note(e)
			}
		case []any:
			for _, item := range v {
				walk(item)
			}
		case map[string]any:
			for _, item := range v {
				walk(item)
			}
		}
	}
	walk(doc.Value)
	// A job's or a step's if: written without ${{ }} is an expression too,
	// unless it is a lone word (condition).
	for _, job := range mapping(doc.Value["jobs"]) {
		def := mapping(job)
		conds := []any{def["if"]}
		for _, s := range list(def["steps"]) {
			conds = append(conds, mapping(s)["if"])
		}
		for _, raw := range conds {
			s, _ := raw.(string)
			if cond, _, bare := bareCondition(s); bare && !loneWord(cond) {
				if e, err := expr.Parse(cond); err == nil {
					note(e)
				}
			}
		}
	}
	names := slices.Sorted(maps.Keys(whole))
	for name := range one {
		if context, _, _ := strings.Cut(name, "."); !whole[context] {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// checkResolved checks what the plan's jobs name against the resolvers:
// each execution group and environment a job's runs-on: names, written
// out, with Groups, and each workflow a step's uses: names, with Sources,
// which pins it in the step (plan.Step.Source), and loads and validates
// it; and then the workflows those call, down the tree (walkCalls), whose
// pins the plan holds (plan.Plan.Calls), with the variables they read
// (plan.Run.TreeVariables); and each parallelworks/checkout step's
// repository and revision, of the run's own steps (plan.Step.Checkout)
// and of the tree's (plan.Plan.Checkouts), with Sources, to a commit
// (pinCheckout). A child run's steps' workflows and checkouts are those
// its root pinned (pinFromRoot). What needs a resolver that isn't there
// is resolver_missing. It returns an error, a resolver's that couldn't
// answer, or ctx's.
func (c *compiler) checkResolved(ctx context.Context, p *plan.Plan) error {
	asked := map[string]error{}
	for i := range p.Jobs {
		if err := c.checkRunsOn(ctx, &p.Jobs[i], asked); err != nil {
			return err
		}
	}
	if c.opts.Parent != nil {
		c.pinFromRoot(p)
		return ctx.Err()
	}
	t := newTree(c.opts.Calls)
	root := node{keys: []plan.SourceRef{{}}, chain: []string{p.Run.Source.Location}}
	if p.Run.Source.Ref.Uses != "" {
		root.keys[0] = p.Run.Source.Ref.Normal()
	}
	var first []node
	for i := range p.Jobs {
		j := &p.Jobs[i]
		for k := range j.Steps {
			if j.Steps[k].Uses == plan.CheckoutUses {
				if err := c.pinOwnCheckout(ctx, t, &j.Steps[k]); err != nil {
					return err
				}
				continue
			}
			n, err := c.pinSource(ctx, t, root, &j.Steps[k])
			if err != nil {
				return err
			}
			if n != nil {
				first = append(first, *n)
			}
		}
	}
	if err := c.walkCalls(ctx, t, first); err != nil {
		return err
	}
	p.Calls = t.planCalls()
	p.Run.TreeVariables = t.variables()
	p.Checkouts = t.planCheckouts()
	c.texts = t.texts()
	return ctx.Err()
}

// pinFromRoot pins, in each step of a child run's workflow whose uses:
// names one, the workflow its root pinned for that step as the root was
// created (Parent.Pin), by the digest of the run's text and the step's
// JSON Pointer in it, with no resolver: the root's Create checked the
// whole tree, so a child's checks none of it again. A step whose pin the
// root doesn't hold, as one of a root a release created that didn't pin
// its whole tree, is child_not_pinned. So it pins each checkout step
// (pinCheckoutFromRoot).
func (c *compiler) pinFromRoot(p *plan.Plan) {
	for i := range p.Jobs {
		for k := range p.Jobs[i].Steps {
			s := &p.Jobs[i].Steps[k]
			loc, err := jsonptr.Split(s.Path)
			if err != nil {
				continue
			}
			step, _ := jsonptr.Lookup(c.doc.Value, loc)
			stepMap, _ := step.(map[string]any)
			if s.Uses == plan.CheckoutUses {
				c.pinCheckoutFromRoot(p, s, loc, stepMap)
				continue
			}
			ref, ok := stepRef(&c.reporter, loc, stepMap)
			if !ok {
				continue
			}
			var source plan.Source
			found := false
			if pin := c.opts.Parent.Pin; pin != nil {
				source, found = pin(p.Run.Source.Digest, s.Path)
			}
			if !found {
				c.reportAt(problems.ChildNotPinned, appendPath(loc, "uses"), problems.Params{"uses": ref.Uses})
				continue
			}
			s.Source = &source
		}
	}
}

// pinCheckoutFromRoot pins s, a checkout step of a child run's workflow,
// step at loc, to the commit its root pinned it to (Parent.Checkout). One
// whose repo or branch is "" has no pin, as a root's own has none then;
// another that the root holds no pin of, as one of a root a release
// created that didn't pin checkouts, is child_not_pinned, rather than a
// step that would check out nothing.
func (c *compiler) pinCheckoutFromRoot(p *plan.Plan, s *plan.Step, loc []string, step map[string]any) {
	with, _ := step["with"].(map[string]any)
	repo, _ := with["repo"].(string)
	branch, _ := with["branch"].(string)
	if repo == "" || branch == "" {
		return
	}
	if pin := c.opts.Parent.Checkout; pin != nil {
		if co, ok := pin(p.Run.Source.Digest, s.Path); ok {
			s.Checkout = &co
			return
		}
	}
	c.reportAt(problems.ChildNotPinned, appendPath(loc, "uses"), problems.Params{"uses": plan.CheckoutUses})
}

// checkRunsOn checks where job's runs-on: runs it, when the workflow
// writes it out whole, with Groups: an execution group, a format 1
// environment or a compute target. One an expression gives, in whole or in
// part, as an environment's params: can be, is known only when the job is
// ready, as is a runs-on: Ready refuses (plan.ReadTarget); and an
// execution-groups input's value the Inputs resolver checks. asked holds
// Groups' answer for each target, which it is asked for once.
func (c *compiler) checkRunsOn(ctx context.Context, j *plan.Job, asked map[string]error) error {
	v, ok := j.RunsOn.Literal()
	if !ok {
		return nil
	}
	t, ok := plan.ReadTarget(v)
	if !ok {
		return nil
	}
	loc, err := jsonptr.Split(j.Path)
	if err != nil {
		return err
	}
	loc = append(loc, "runs-on")
	var (
		key      = "runs-on"
		ask      func(Groups) error
		notFound *problems.Kind
		params   problems.Params
	)
	switch {
	case t.Group != "":
		// Written out, a group is named in a mapping: a runs-on: that is
		// text is an expression.
		key = "execution-group"
		loc = append(loc, key)
		ask = func(g Groups) error { _, err := g.Group(ctx, t.Group); return err }
		notFound, params = problems.GroupUnknown, problems.Params{"group": t.Group}
	case t.Environment != nil:
		key = "environment"
		loc = append(loc, key)
		ask = func(g Groups) error { _, err := g.Environment(ctx, *t.Environment); return err }
		notFound, params = problems.EnvironmentUnknown, problems.Params{"cluster": t.Environment.Cluster, "name": t.Environment.Name}
	case t.ComputeTarget != nil:
		ask = func(g Groups) error { _, err := g.ComputeTarget(ctx, *t.ComputeTarget); return err }
		notFound, params = problems.ComputeTargetUnknown, problems.Params{"mode": t.ComputeTarget.Mode, "target": t.ComputeTarget.TargetID}
	default:
		return nil
	}
	if c.opts.Groups == nil {
		c.reportAt(problems.ResolverMissing, loc, problems.Params{"key": key, "interface": "Groups"})
		return nil
	}
	id, err := json.Marshal(t)
	if err != nil {
		return err
	}
	answer, ok := asked[string(id)]
	if !ok {
		answer = ask(c.opts.Groups)
		asked[string(id)] = answer
	}
	return resolverError(ctx, &c.reporter, answer, loc, notFound, params)
}

// workflowUses are what a uses: that names a workflow starts with: a Git
// repository's, or a workflow a platform stores. Any other uses: names a
// built-in action.
var workflowUses = []string{"github/", "gitlab/", "marketplace/", "workflow/"}

// refProblemAt is where a problem of kind k, with params, of the reference
// the step at loc names a workflow by is: what is wrong with the step's
// $yaml or $host (source_with_invalid) is at it, in its with:, and
// anything else at its uses:.
func refProblemAt(loc []string, k *problems.Kind, params problems.Params) []string {
	if key, _ := params["key"].(string); k == problems.SourceWithInvalid && (key == "$yaml" || key == "$host") {
		return appendPath(loc, "with", key)
	}
	return appendPath(loc, "uses")
}

// pinOwnCheckout pins s, a checkout step of the run's own workflow, as
// pinCheckout does, in s itself (plan.Step.Checkout).
func (c *compiler) pinOwnCheckout(ctx context.Context, t *tree, s *plan.Step) error {
	loc, err := jsonptr.Split(s.Path)
	if err != nil {
		return err
	}
	step, _ := jsonptr.Lookup(c.doc.Value, loc)
	stepMap, _ := step.(map[string]any)
	co, err := c.pinCheckout(ctx, t, &c.reporter, loc, stepMap)
	s.Checkout = co
	return err
}

// pinSource pins, in the step s of the run's own workflow, the workflow
// its uses: names, if it names one, as pinRef does, and returns the node of
// that workflow, below root, the run's, from which the walk of the tree
// goes on, or nil for none.
func (c *compiler) pinSource(ctx context.Context, t *tree, root node, s *plan.Step) (*node, error) {
	loc, err := jsonptr.Split(s.Path)
	if err != nil {
		return nil, err
	}
	step, _ := jsonptr.Lookup(c.doc.Value, loc)
	stepMap, _ := step.(map[string]any)
	ref, ok := stepRef(&c.reporter, loc, stepMap)
	if !ok || t.full {
		return nil, nil
	}
	usesLoc := appendPath(loc, "uses")
	if t.count(&c.reporter, usesLoc) {
		return nil, nil
	}
	p, err := c.pinRef(ctx, t, &c.reporter, loc, usesLoc, ref)
	if err != nil || p.source == nil {
		return nil, err
	}
	copied := *p.source
	s.Source = &copied
	chain := append(slices.Clone(root.chain), p.source.Location)
	c.checkCallInputs(t, usesLoc, chain, c.doc, loc, stepMap, p.doc)
	return &node{pin: p, keys: append(slices.Clone(root.keys), ref.Normal()), chain: chain, root: usesLoc}, nil
}

// stepRef reads the reference that step, at loc in the workflow rep
// reports in, names a workflow by: its uses:, and its with:'s $yaml and
// $host, as format 1 writes them. It says whether the step names one,
// which must be written out, as must where it is: what isn't, and what
// format 1 refuses of a reference, it reports, and the step names none.
func stepRef(rep *reporter, loc []string, step map[string]any) (plan.SourceRef, bool) {
	uses, _ := step["uses"].(string)
	written := !strings.Contains(uses, "${{")
	if uses == "" || written && !slices.ContainsFunc(workflowUses, func(prefix string) bool { return strings.HasPrefix(uses, prefix) }) {
		return plan.SourceRef{}, false
	}
	if !written {
		// ACTIVATE evaluates a uses: with the run's inputs before it reads
		// it, and so can find a workflow in one; the workflow is pinned as
		// the run is created, so the reference must be written out
		// (docs/design.md).
		rep.reportAt(problems.SourceNotKnown, appendPath(loc, "uses"), problems.Params{"key": "uses:"})
		return plan.SourceRef{}, false
	}
	ref := plan.SourceRef{Uses: uses}
	with, _ := step["with"].(map[string]any)
	for _, w := range []struct {
		key string
		to  *string
	}{{"$yaml", &ref.Path}, {"$host", &ref.Host}} {
		// ACTIVATE reads one that isn't text as if it weren't there,
		// where the schema refuses it.
		text, isText := with[w.key].(string)
		if !isText {
			continue
		}
		if strings.Contains(text, "${{") {
			// The workflow is pinned as the run is created, so where it is
			// must be known then.
			rep.reportAt(problems.SourceNotKnown, appendPath(loc, "with", w.key), problems.Params{"key": w.key})
			return plan.SourceRef{}, false
		}
		// An empty $yaml means workflow.yaml, as ACTIVATE reads it.
		*w.to = text
	}
	// What format 1 refuses of a reference is refused wherever the run is
	// created, whatever Sources resolver there is, or none.
	if err := ref.Check(); err != nil {
		k, p, _ := workflowProblem(err)
		rep.reportAt(k, refProblemAt(loc, k, p), p)
		return plan.SourceRef{}, false
	}
	return ref, true
}

// pinRef resolves ref, the reference the step at loc, in the workflow rep
// reports in, names a workflow by, through Sources, and loads and
// validates its text, whose errors are the run's, and returns its pin, by
// ref as the step writes it. A workflow pinned already, by a reference
// that reads the same (plan.SourceRef.Normal), is pinned again from the
// tree, so that a run reads one workflow at one commit however its steps
// write its name. A workflow past the tree's limits on how many workflows
// it holds, and how much text, is reported at root, the uses: of the run's
// own step the path to it starts at, and ends the walk.
func (c *compiler) pinRef(ctx context.Context, t *tree, rep *reporter, loc, root []string, ref plan.SourceRef) (*pin, error) {
	usesLoc := appendPath(loc, "uses")
	if c.opts.Sources == nil {
		rep.reportAt(problems.ResolverMissing, usesLoc, problems.Params{"key": "uses", "interface": "Sources"})
		return &pin{}, nil
	}
	key := ref.Normal()
	if p, ok := t.pins[key]; ok {
		return p.as(ref), nil
	}
	none := &pin{}
	t.pins[key] = none
	w, err := c.opts.Sources.Resolve(ctx, ref)
	if err != nil {
		at := usesLoc
		if k, p, ok := workflowProblem(err); ok {
			at = refProblemAt(loc, k, p)
		}
		return none, resolverError(ctx, rep, err, at, problems.SourceNotFound, problems.Params{"ref": ref.Uses})
	}
	if w == nil {
		return nil, errors.New("compile: Sources.Resolve gave neither a workflow nor an error")
	}
	source := w.Source
	source.Ref = ref
	// A child run of the workflow reads the remote context Sources gives
	// for it now, as format 1 stamps a called workflow's as its root is
	// created.
	source.Remote = maps.Clone(w.Remote)
	switch digest := textDigest(w.Text); source.Digest {
	case "":
		source.Digest = digest
	case digest:
	default:
		// The resolver pinned the workflow to other text than it gave.
		rep.reportAt(problems.SourceChanged, usesLoc, problems.Params{
			"location": source.Location, "commit": source.Revision, "digest": digest, "pinned": source.Digest,
		})
		return none, nil
	}
	if source.Revision == "" {
		// As the run's own workflow's, when nothing else names its text.
		source.Revision = source.Digest
	}
	t.workflows++
	t.bytes += len(w.Text)
	switch {
	case t.workflows > t.limits.MaxWorkflows:
		c.reportAt(problems.WorkflowsTooMany, root, problems.Params{"max": t.limits.MaxWorkflows, "what": "workflows"})
		t.full = true
		return none, nil
	case t.bytes > t.limits.MaxBytes:
		c.reportAt(problems.WorkflowsTooLarge, root, problems.Params{"max": t.limits.MaxBytes})
		t.full = true
		return none, nil
	}
	// The workflow is loaded and validated now, so that a run whose step
	// would run one that can't be isn't created.
	doc, diags := format.Load(source.Location, w.Text)
	c.diags = append(c.diags, diags.Errors()...)
	p := &pin{source: &source, doc: doc, text: w.Text}
	t.pins[key] = p
	return p.as(ref), nil
}

// as returns the pin p, of a workflow, as the reference ref, which reads
// the same as the one it was pinned by, names it: its source's Ref is ref.
func (p *pin) as(ref plan.SourceRef) *pin {
	if p.source == nil {
		return p
	}
	source := *p.source
	source.Ref = ref
	return &pin{source: &source, doc: p.doc, text: p.text}
}

// resolverError reports err, from a resolver asked about what is at loc in
// the workflow rep reports in: one that is, or wraps, a problem of a
// workflow's kind of error, as it is; ErrNotFound, as notFound with params;
// and returns any other, which means the resolver couldn't answer, or
// ctx's.
func resolverError(ctx context.Context, rep *reporter, err error, loc []string, notFound *problems.Kind, params problems.Params) error {
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if k, p, ok := workflowProblem(err); ok {
		rep.reportAt(k, loc, p)
		return nil
	}
	if errors.Is(err, ErrNotFound) {
		rep.reportAt(notFound, loc, params)
		return nil
	}
	return err
}
