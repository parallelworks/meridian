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
// it. What needs a resolver that isn't there is resolver_missing. It
// returns an error, a resolver's that couldn't answer, or ctx's.
func (c *compiler) checkResolved(ctx context.Context, p *plan.Plan) error {
	pinned := map[plan.SourceRef]*plan.Source{}
	asked := map[string]error{}
	for i := range p.Jobs {
		j := &p.Jobs[i]
		if err := c.checkRunsOn(ctx, j, asked); err != nil {
			return err
		}
		for k := range j.Steps {
			if err := c.pinSource(ctx, &j.Steps[k], pinned); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
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
	return c.resolverError(ctx, answer, loc, notFound, params)
}

// workflowUses are what a uses: that names a workflow starts with: a Git
// repository's, or a workflow a platform stores. Any other uses: names a
// built-in action.
var workflowUses = []string{"github/", "gitlab/", "marketplace/", "workflow/"}

// pinSource resolves the workflow step's uses: names, if it names one,
// through Sources, pins it in the step, and loads and validates its text,
// whose errors are the run's. A workflow pinned already, as another step's,
// by a reference that reads the same (plan.SourceRef.Normal), is pinned
// again from pinned, so that a run reads one workflow at one commit
// however its steps write its name.
func (c *compiler) pinSource(ctx context.Context, s *plan.Step, pinned map[plan.SourceRef]*plan.Source) error {
	written := !strings.Contains(s.Uses, "${{")
	if written && !slices.ContainsFunc(workflowUses, func(prefix string) bool { return strings.HasPrefix(s.Uses, prefix) }) {
		return nil
	}
	loc, err := jsonptr.Split(s.Path)
	if err != nil {
		return err
	}
	usesLoc := appendPath(loc, "uses")
	if !written {
		// ACTIVATE evaluates a uses: with the run's inputs before it reads
		// it, and so can find a workflow in one; the workflow is pinned as
		// the run is created, so the reference must be written out
		// (docs/design.md).
		c.reportAt(problems.SourceNotKnown, usesLoc, problems.Params{"key": "uses:"})
		return nil
	}
	ref := plan.SourceRef{Uses: s.Uses}
	for _, with := range []struct {
		key string
		to  *string
	}{{"$yaml", &ref.Path}, {"$host", &ref.Host}} {
		key, to := with.key, with.to
		raw, ok := jsonptr.Lookup(c.doc.Value, appendPath(loc, "with", key))
		if !ok {
			continue
		}
		text, isText := raw.(string)
		if !isText {
			// ACTIVATE reads one that isn't text as if it weren't there,
			// where the schema refuses it.
			continue
		}
		if strings.Contains(text, "${{") {
			// The workflow is pinned as the run is created, so where it is
			// must be known then.
			c.reportAt(problems.SourceNotKnown, appendPath(loc, "with", key), problems.Params{"key": key})
			return nil
		}
		// An empty $yaml means workflow.yaml, as ACTIVATE reads it.
		*to = text
	}
	if c.opts.Sources == nil {
		c.reportAt(problems.ResolverMissing, usesLoc, problems.Params{"key": "uses", "interface": "Sources"})
		return nil
	}
	key := ref.Normal()
	if source, ok := pinned[key]; ok {
		if source != nil {
			copied := *source
			copied.Ref = ref
			s.Source = &copied
		}
		return nil
	}
	pinned[key] = nil
	w, err := c.opts.Sources.Resolve(ctx, ref)
	if err != nil {
		at := usesLoc
		if k, p, ok := workflowProblem(err); ok && k == problems.SourceWithInvalid {
			// What is wrong is the step's $yaml or $host.
			if key, _ := p["key"].(string); key == "$yaml" || key == "$host" {
				at = appendPath(loc, "with", key)
			}
		}
		return c.resolverError(ctx, err, at, problems.SourceNotFound, problems.Params{"ref": s.Uses})
	}
	if w == nil {
		return errors.New("compile: Sources.Resolve gave neither a workflow nor an error")
	}
	source := w.Source
	source.Ref = ref
	switch digest := textDigest(w.Text); source.Digest {
	case "":
		source.Digest = digest
	case digest:
	default:
		// The resolver pinned the workflow to other text than it gave.
		c.reportAt(problems.SourceChanged, usesLoc, problems.Params{
			"location": source.Location, "commit": source.Revision, "digest": digest, "pinned": source.Digest,
		})
		return nil
	}
	if source.Revision == "" {
		// As the run's own workflow's, when nothing else names its text.
		source.Revision = source.Digest
	}
	pinned[key] = &source
	copied := source
	s.Source = &copied
	// The workflow is loaded and validated now, so that a run whose step
	// would run one that can't be isn't created.
	_, diags := format.Load(source.Location, w.Text)
	c.diags = append(c.diags, diags.Errors()...)
	return nil
}

// resolverError reports err, from a resolver asked about what is at loc:
// one that is, or wraps, a problem of a workflow's kind of error, as it
// is; ErrNotFound, as notFound with params; and returns any other, which
// means the resolver couldn't answer, or ctx's.
func (c *compiler) resolverError(ctx context.Context, err error, loc []string, notFound *problems.Kind, params problems.Params) error {
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if k, p, ok := workflowProblem(err); ok {
		c.reportAt(k, loc, p)
		return nil
	}
	if errors.Is(err, ErrNotFound) {
		c.reportAt(notFound, loc, params)
		return nil
	}
	return err
}
