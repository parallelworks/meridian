package compile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// Compile loads a workflow file and compiles it, with the input values
// submitted for a run, into a plan. It reports every problem it finds, and
// returns no plan if any is an error. It is Create, for a caller that needs
// only the plan, such as one that checks a workflow.
func Compile(name string, data []byte, submitted map[string]any, opts Options) (*plan.Plan, problems.Diagnostics) {
	created, diags := create(name, data, submitted, opts)
	if created == nil {
		return nil, diags
	}
	return created.Plan, diags
}

// Created is what Create gives: the run's plan, and what the run's secret
// record holds from its start, which the plan doesn't.
type Created struct {
	Plan   *plan.Plan
	Record Record
}

// A Record is what a run's secret record holds from the run's start, which
// the plan doesn't, as the plan names it, and as Start takes it
// (StartInput.Record). The coordinator stores it, encrypted, with the plan.
type Record struct {
	// Passwords are the values of the run's password inputs, and of the
	// other values it was given that are kept as passwords are, such as
	// one in the place of a default that reads a password, by their names,
	// as plan.Run.Secrets names them, as in inputs.creds.0.pass.
	Passwords map[string]any `json:"passwords,omitempty"`
	// Private are the values the run was given that the plan doesn't hold,
	// though they aren't secrets, by their names (plan.Run.Private). One can
	// hold a password the run was given in it, as a list's row can, which
	// Passwords names too, and the run's values mask.
	Private map[string]any `json:"private,omitempty"`
	// Withheld are the parts of the input definitions that the plan leaves
	// out, as they can hold a password, as the workflow writes them, by
	// their JSON Pointers (plan.Run.Withheld).
	Withheld map[string]any `json:"withheld,omitempty"`
	// Decisions are what each condition of the run's shape that read a
	// variable or a password decided, by its JSON Pointer
	// (plan.Run.Decisions).
	Decisions map[string]bool `json:"decisions,omitempty"`
	// Ignored are the inputs the run ignored, by their names, as in
	// inputs.region, each with its value as it was given, or null if it had
	// none. Start reads none of them, as the phases after it don't, but for
	// the conditions it decides again, which read each until they reach it,
	// as the compiler's did. The plan doesn't name them, as which inputs a
	// condition that read a password ignored can say something of it.
	Ignored map[string]any `json:"ignored,omitempty"`
}

// Create is the Create phase: it loads a workflow file and compiles it,
// with the input values submitted for a run, into the run's plan, and
// gives beside it what the run's secret record holds from the start. It
// reports every problem it finds, and gives nothing if any is an error. It
// returns an error, and nothing else, only when ctx is done before it
// starts or once it has finished, with ctx.Err().
func Create(ctx context.Context, name string, data []byte, submitted map[string]any, opts Options) (*Created, problems.Diagnostics, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	created, diags := create(name, data, submitted, opts)
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return created, diags, nil
}

// create is Create, without a context.
func create(name string, data []byte, submitted map[string]any, opts Options) (*Created, problems.Diagnostics) {
	doc, diags := format.Load(name, data)
	if diags.HasErrors() {
		return nil, listed(diags) // Load sorts them.
	}
	inputs := resolveInputs(doc, submitted, opts)
	c := &compiler{doc: doc, opts: opts, inputs: inputs, secrets: map[string]bool{}}
	c.diags = append(c.diags, diags...)
	c.diags = append(c.diags, inputs.diags...)

	source := opts.Source
	if source.Location == "" {
		source.Location = name
	}
	if source.Revision == "" {
		sum := sha256.Sum256(data)
		source.Revision = "sha256:" + hex.EncodeToString(sum[:])
	}
	p := &plan.Plan{Version: plan.Version, Run: c.run(source), Jobs: c.jobs()}
	// The inputs' secrets, derived and private values are named by their
	// paths in the inputs' values (plan.JoinPath), so inputs. and the name
	// name one in the run.
	for k := range inputs.secrets {
		c.secrets["inputs."+k] = true
	}
	// A password whose value is evaluated as the run starts is one of its
	// secrets then, and so is a list whose rows are, with a password field
	// in them.
	for k := range inputs.secretsLater {
		c.secrets["inputs."+k] = true
	}
	// The run reads the variables that what Start evaluates reads, as well
	// as its jobs': the inputs held for it, the conditions and the jobs'
	// needs: it decides again, and the rows it fills in.
	variables := map[string]bool{}
	c.eachRead(p, func(r expr.Ref) {
		c.recordSecret(r)
		c.noteVariable(variables, r)
	})
	p.Run.Secrets = slices.Sorted(maps.Keys(c.secrets))
	for k := range inputs.derived {
		p.Run.Derived = append(p.Run.Derived, "inputs."+k)
	}
	slices.Sort(p.Run.Derived)
	for k := range inputs.private {
		p.Run.Private = append(p.Run.Private, "inputs."+k)
	}
	slices.Sort(p.Run.Private)
	p.Run.Variables = slices.Sorted(maps.Keys(variables))
	c.checkVariableValues(p)

	c.diags.Sort()
	c.diags = listed(slices.DeleteFunc(c.diags, repeatedVariable()))
	if c.diags.HasErrors() {
		return nil, c.diags
	}
	for _, d := range c.diags.Warnings() {
		// A plan keeps each warning's code and params, so that each reader
		// sees it in their own language.
		d.Detail = ""
		p.Warnings = append(p.Warnings, d)
	}
	created := &Created{Plan: p, Record: Record{Withheld: c.withheld, Decisions: inputs.decided}}
	for k, v := range inputs.secrets {
		if created.Record.Passwords == nil {
			created.Record.Passwords = map[string]any{}
		}
		created.Record.Passwords["inputs."+k] = v
	}
	for k, v := range inputs.private {
		if created.Record.Private == nil {
			created.Record.Private = map[string]any{}
		}
		created.Record.Private["inputs."+k] = v
	}
	for _, name := range inputs.ignored {
		if created.Record.Ignored == nil {
			created.Record.Ignored = map[string]any{}
		}
		created.Record.Ignored[name] = inputs.ignoredValues[name]
	}
	return created, c.diags
}

// eachRead calls read with each reference of the expressions the run
// evaluates, whose reads name the variables it reads: of every expression
// the plan holds but its input definitions, of which only those Start can
// evaluate (startExprs), and those in the place of which the run was given
// a private value (privateExprs). Each expression's are found once, but
// for those of an input that is both.
func (c *compiler) eachRead(p *plan.Plan, read func(expr.Ref)) {
	defs, held := p.Run.Definitions, p.Run.DeferredInputs
	p.Run.Definitions, p.Run.DeferredInputs = plan.Value{}, nil
	exprs := p.Exprs()
	p.Run.Definitions, p.Run.DeferredInputs = defs, held
	for _, e := range exprs {
		for _, r := range e.Refs() {
			read(r)
		}
	}
	// What Start evaluates of the definitions depends on whether what the
	// plan holds for it can read one.
	readsDefinition := false
	for _, v := range held {
		for _, e := range v.Exprs() {
			readsDefinition = readsDefinition || e.Relative()
			for _, r := range e.Refs() {
				read(r)
				readsDefinition = readsDefinition || readsDefinitionKey(r)
			}
		}
	}
	for _, e := range c.startExprs(p, readsDefinition) {
		for _, r := range e.Refs() {
			read(r)
		}
	}
	// Start reads, too, the variables that what the run was given in the
	// place of reads: one that is a secret by then has what was given
	// masked as what is made from one.
	for _, e := range c.inputs.privateExprs() {
		for _, r := range e.Refs() {
			read(r)
		}
	}
}

// startExprs returns the expressions of the input definitions that Start
// can evaluate, besides the inputs held for it, which the plan holds
// apart: the conditions it decides again, and the definitions of the rows
// of each list held for it, whose fields take their defaults again once
// Start has evaluated what it holds of the list. One of these, or of the
// inputs held, that reads a definition, as a relative reference does, can
// read any string of them, and then they are all of the definitions'
// expressions: readsDefinition says whether one of the inputs held does.
func (c *compiler) startExprs(p *plan.Plan, readsDefinition bool) []*expr.Expr {
	r := c.inputs
	// All of the definitions' expressions are the workflow's: the plan's
	// Definitions withhold some (plan.Run.Withheld), which Start reads.
	all := func() []*expr.Expr {
		exprs, _ := r.workflow.Exprs(definitionsPath)
		return exprs
	}
	if readsDefinition {
		return all()
	}
	var out []*expr.Expr
	add := func(loc []string) {
		if exprs, err := r.workflow.Exprs(loc); err == nil {
			out = append(out, exprs...)
		}
	}
	for _, pointer := range p.Run.Decisions {
		if loc, err := jsonptr.Split(pointer); err == nil {
			add(loc)
		}
	}
	for _, f := range r.deferredFields() {
		if _, loc := f.rowInputs(); loc != nil {
			add(loc)
		}
	}
	if slices.ContainsFunc(out, func(e *expr.Expr) bool {
		return e.Relative() || slices.ContainsFunc(e.Refs(), readsDefinitionKey)
	}) {
		return all()
	}
	return out
}

// readsDefinitionKey reports whether r can read a key of an input's
// definition, as inputs.x.type does when x's value has none.
func readsDefinitionKey(r expr.Ref) bool {
	return r.Context == "inputs" && len(r.Path) > 1 && slices.ContainsFunc(r.Path[1:], func(k string) bool { return definitionKeys[k] })
}

// maxListed is how many problems of each kind Compile and ResolveInputs
// list. A workflow, or a run's submitted values, can have far more than
// anyone reads, and a plan keeps its warnings: problems.ProblemsNotListed
// counts the rest.
const maxListed = 100

// listed returns ds, which are sorted, with the first maxListed diagnostics
// of each code. For a code with more, one of problems.ProblemsNotListed,
// where the first it leaves out is, counts them, with what one that ds has
// already counts, as the unknown inputs past the limit have.
func listed(ds problems.Diagnostics) problems.Diagnostics {
	notListed := problems.ProblemsNotListed.Code()
	seen := map[string]int{}
	over := false
	for _, d := range ds {
		seen[string(d.Code)]++
		over = over || d.Code == notListed || seen[string(d.Code)] > maxListed
	}
	if !over {
		return ds
	}
	// rest is the first diagnostic of a code that isn't listed, and how
	// many aren't.
	type rest struct {
		first problems.Diagnostic
		count int
	}
	left := map[string]*rest{}
	out := make(problems.Diagnostics, 0, len(ds))
	clear(seen)
	for _, d := range ds {
		code, count := string(d.Code), 1
		if d.Code == notListed {
			code, _ = d.Params["code"].(string)
			count, _ = d.Params["count"].(int)
		} else if seen[code]++; seen[code] <= maxListed {
			out = append(out, d)
			continue
		}
		if left[code] == nil {
			left[code] = &rest{first: d}
		}
		left[code].count += count
	}
	for code, r := range jsonvalue.Sorted(left) {
		out = append(out, problems.ProblemsNotListed.At(r.first.File(), r.first.Position(), r.first.Path(), problems.Params{
			"code": code, "count": r.count, "limit": maxListed,
		}))
	}
	out.Sort()
	return out
}

type compiler struct {
	reporter
	opts   Options
	inputs *inputResolver
	// secrets are the secret variables and inputs the run reads.
	secrets map[string]bool
	// withheld are the parts of the input definitions the plan leaves out,
	// by their JSON Pointers (Record.Withheld).
	withheld map[string]any
	jobInfos map[string]*jobInfo
	// jobNames, names and sessionNames are the names of the jobs, the
	// inputs and the sessions, for the checks of what references name.
	jobNames     *nameList
	names        *inputNames
	sessionNames *nameList
}

// A reporter collects the problems found in a workflow.
type reporter struct {
	doc   *format.Document
	diags problems.Diagnostics
}

// reportPointer reports a problem of kind k at pos, with what is at pointer
// in the workflow.
func (r *reporter) reportPointer(k *problems.Kind, pos problems.Position, pointer string, params problems.Params) {
	r.diags = append(r.diags, k.At(r.doc.File, pos, pointer, params))
}

// A spot is a string in the workflow, at loc, whose expressions the
// compiler checks. It finds the string's JSON Pointer, and where its
// places are in the file, once a problem is reported in it.
type spot struct {
	doc     *format.Document
	loc     []string
	pointer string
	locator *format.Locator
}

func (r *reporter) spot(loc []string) *spot { return &spot{doc: r.doc, loc: loc} }

// reportIn reports a problem of kind k at byte offset at in the string in.
func (r *reporter) reportIn(in *spot, at int, k *problems.Kind, params problems.Params) {
	if in.locator == nil {
		in.pointer = jsonptr.Format(in.loc)
		in.locator = in.doc.Locator(in.pointer)
	}
	r.reportPointer(k, in.locator.Locate(at), in.pointer, params)
}

// reportAt reports a problem of kind k with what is at loc: a mapping, such
// as an input's definition, at its key, and anything else where it starts.
func (r *reporter) reportAt(k *problems.Kind, loc []string, params problems.Params) {
	pointer := jsonptr.Format(loc)
	r.reportPointer(k, r.doc.ReportPosition(pointer), pointer, params)
}

// reportError reports err, an error parsing or evaluating the expressions in
// the string at loc, at shift bytes after where the error puts it. An error
// in another string, which the expression read, is reported where it is. A
// group's budget running out is reported at the string whose evaluation ran
// it out, since no one expression is wrong.
func (r *reporter) reportError(loc []string, shift int, err error) {
	pointer, offset := jsonptr.Format(loc), shift
	report := func(k *problems.Kind, params problems.Params, at int, location string) {
		if location != "" {
			pointer, offset = location, 0
		}
		r.reportPointer(k, r.doc.Locate(pointer, offset+at), pointer, params)
	}
	if se, ok := errors.AsType[*expr.SyntaxError](err); ok {
		report(se.Kind, se.Params, se.Offset, se.Location)
		return
	}
	if ee, ok := errors.AsType[*expr.EvalError](err); ok {
		report(ee.Kind, ee.Params, ee.Offset, ee.Location)
		return
	}
	if be, ok := errors.AsType[*expr.BudgetError](err); ok {
		report(be.Kind, problems.Params{"limit": be.Limit}, 0, "")
		return
	}
	report(problems.ExpressionFailed, problems.Params{"reason": err.Error()}, 0, "")
}

// mask masks params as problems.MaskValues does.
func mask(params problems.Params) problems.Params { return problems.MaskValues(params) }

// maskedIf returns err, an error evaluating an expression, with what it
// shows of the values it read masked (expr.Masked) if the evaluation read
// a variable or a password.
func maskedIf(marked bool, err error) error {
	if marked {
		return expr.Masked(err)
	}
	return err
}

// run compiles what the plan holds about the run as a whole. Its
// expressions are checked with what Create knows (plan.PhaseCreate), so
// they read no env: sessions: and links: are evaluated as the run is
// created, and Start evaluates the workflow's env: and timeout:, but the
// workflow's env: is what env holds, and format 1 never read env in the
// workflow's timeout:, as ACTIVATE leaves an expression there that reads
// it as its text, and so ignores the timeout. Input definitions, which
// Start evaluates with the workflow's env (plan.PhaseStart), aren't
// checked for what they read yet.
func (c *compiler) run(source plan.Source) plan.Run {
	w := c.doc.Value
	known := createContexts(c.opts, nil)
	kept := c.inputs.kept()
	r := plan.Run{
		Source:      source,
		Format:      c.doc.Format,
		Name:        str(w["name"]),
		Icon:        str(w["icon"]),
		Thumbnail:   str(w["thumbnail"]),
		Inputs:      plan.DataOf(kept),
		Unconverted: c.inputs.unconvertedData(kept),
		Definitions: c.definitions(),
		Env:         c.values([]string{"env"}, plan.PhaseCreate, nil),
		Timeout:     c.value([]string{"timeout"}, plan.PhaseCreate, nil),
		Remote:      known.Run.Remote,
		// The session provider checked the sessions:, which Start makes.
		Sessions:     c.value([]string{"sessions"}, plan.PhaseCreate, nil),
		SessionNames: known.Run.Sessions,
		Limits:       c.opts.Limits,

		DeferredInputs: c.inputs.deferred(),
		DeferredParts:  c.inputs.deferredParts(),
		Decisions:      c.inputs.decisions,
	}
	r.Withheld = slices.Sorted(maps.Keys(c.withheld))
	r.Description, _ = w["description"].(string)
	for _, p := range list(w["permissions"]) {
		if s, ok := p.(string); ok {
			r.Permissions = append(r.Permissions, s)
		}
	}
	c.check([]string{"links"}, w["links"], plan.PhaseCreate, nil)
	c.checkDefinitions(slices.Clip(definitionsPath), lookup(w, definitionsPath))
	c.checkInputNames(c.inputs.fields)
	c.checkDefaultRows(c.inputs.fields)
	c.checkInputDepths(r)
	return r
}

// definitions returns the input definitions as the plan holds them, for
// Start: as the workflow writes them, each string with an expression in it
// parsed, as the input resolver parsed it, without the parts that can hold
// a password (withheldParts), which it keeps in c.withheld for the secret
// record.
func (c *compiler) definitions() plan.Value {
	defs, ok := lookup(c.doc.Value, definitionsPath).(map[string]any)
	if !ok {
		return plan.Value{}
	}
	v, ok := c.inputs.parsed(defs)
	if !ok {
		return plan.Value{} // Resolving the inputs reported it.
	}
	for _, loc := range c.inputs.withheldParts() {
		raw, ok := jsonptr.Lookup(c.doc.Value, loc)
		if !ok {
			continue
		}
		if c.withheld == nil {
			c.withheld = map[string]any{}
		}
		c.withheld[jsonptr.Format(loc)] = jsonvalue.Clone(raw)
		deletePath(v, loc[len(definitionsPath):])
	}
	// The plan holds them in its run, two levels deep.
	pv := plan.Literal(v)
	if 2+pv.Depth() > plan.MaxDepth {
		c.reportAt(problems.ValueTooDeep, definitionsPath, problems.Params{"max": plan.MaxDepth})
		return plan.Value{}
	}
	return pv
}

// checkDefinitions reports the probable mistakes in the expressions in v, at
// loc in the input definitions, such as inputs.pw-user written for
// inputs['pw-user'], and reads of variables whose names look like a
// secret's. Resolving the inputs reports their syntax errors, and the
// errors of the expressions it evaluates. It appends to loc in place, as
// check does.
func (c *compiler) checkDefinitions(loc []string, v any) {
	switch v := v.(type) {
	case string:
		if !strings.Contains(v, "${{") {
			return
		}
		t, err := c.inputs.workflow.Template(v)
		if err != nil {
			return // Resolving the inputs reported it.
		}
		in := c.spot(loc)
		for at, e := range t.Exprs() {
			for _, p := range e.Check() {
				c.reportProblem(in, at+p.Offset, p, nil)
			}
			for _, r := range e.Refs() {
				c.checkVariableName(r, in, at+r.Offset)
			}
		}
	case []any:
		for i, item := range v {
			c.checkDefinitions(append(loc, strconv.Itoa(i)), item)
		}
	case map[string]any:
		for k, item := range jsonvalue.Sorted(v) {
			c.checkDefinitions(append(loc, k), item)
		}
	}
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func list(v any) []any {
	l, _ := v.([]any)
	return l
}

func mapping(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// value checks the value at loc in the workflow and returns it as a plan
// Value, or the zero Value if it isn't there or has errors.
func (c *compiler) value(loc []string, ph plan.Phase, job *jobInfo) plan.Value {
	raw, ok := jsonptr.Lookup(c.doc.Value, loc)
	if !ok {
		return plan.Value{}
	}
	v, ok := c.check(slices.Clip(loc), raw, ph, job)
	if !ok {
		return plan.Value{}
	}
	return c.planned(loc, v)
}

// values compiles a mapping of values, such as an env:.
func (c *compiler) values(loc []string, ph plan.Phase, job *jobInfo) map[string]plan.Value {
	m := mapping(lookup(c.doc.Value, loc))
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]plan.Value, len(m))
	for k := range jsonvalue.Sorted(m) {
		if v := c.value(appendPath(loc, k), ph, job); !v.IsZero() {
			out[k] = v
		}
	}
	return out
}

// condition compiles an if:. Written without ${{ }}, it is an expression
// when it has an operator, a dot, parentheses or brackets; a lone word,
// such as always or true, stays a word. Written empty, it is null, which
// the plan keeps, and which is false.
func (c *compiler) condition(loc []string, ph plan.Phase, job *jobInfo) plan.Value {
	raw, ok := jsonptr.Lookup(c.doc.Value, loc)
	if !ok {
		return plan.Value{}
	}
	s, isString := raw.(string)
	if !isString {
		return c.planned(loc, raw)
	}
	shift := 0
	if cond, start, bare := bareCondition(s); bare {
		if !strings.ContainsAny(cond, " \t.()!=<>&|?[") {
			// A lone word stays a word: always means always(), and any other
			// is text, which is true.
			if e, err := expr.Parse(cond); err == nil {
				in := c.spot(loc)
				for _, p := range e.CheckCondition() {
					c.reportProblem(in, start+p.Offset, p, job)
				}
			}
			return plan.Literal(s)
		}
		const open = "${{ "
		s, shift = open+cond+" }}", start-len(open)
	}
	v, ok := c.checkCondition(loc, s, shift, ph, job)
	if !ok {
		return plan.Value{}
	}
	return c.planned(loc, v)
}

// bareCondition returns s, an if: written without ${{ }}, without the space
// around it, and the byte offset in s where it starts. ok is false if s has
// ${{ }}, or nothing but space.
func bareCondition(s string) (cond string, start int, ok bool) {
	cond = strings.TrimSpace(s)
	if cond == "" || strings.Contains(cond, "${{") {
		return "", 0, false
	}
	return cond, len(s) - len(strings.TrimLeftFunc(s, unicode.IsSpace)), true
}

// check checks every string in raw, the value at loc in the workflow, and
// returns raw with the expressions in each string parsed, as a plan Value
// holds them. ok is false if a string has an error. It appends to loc in
// place, so that the walk of a deep value reuses one path: the caller's loc
// must own the room after it, and nothing keeps loc once a string's check
// is done.
func (c *compiler) check(loc []string, raw any, ph plan.Phase, job *jobInfo) (any, bool) {
	switch v := raw.(type) {
	case string:
		return c.checkString(loc, v, ph, job)
	case []any:
		out, ok := make([]any, len(v)), true
		for i, item := range v {
			var itemOK bool
			out[i], itemOK = c.check(append(loc, strconv.Itoa(i)), item, ph, job)
			ok = ok && itemOK
		}
		return out, ok
	case map[string]any:
		out, ok := make(map[string]any, len(v)), true
		for k, item := range jsonvalue.Sorted(v) {
			var itemOK bool
			out[k], itemOK = c.check(append(loc, k), item, ph, job)
			ok = ok && itemOK
		}
		return out, ok
	}
	return raw, true
}

// checkString checks the expressions in s, the string at loc, and returns s
// parsed: a template, or s itself if it has no expressions.
func (c *compiler) checkString(loc []string, s string, ph plan.Phase, job *jobInfo) (any, bool) {
	t, ok := c.parse(loc, s, 0)
	if t == nil {
		return s, ok
	}
	in := c.spot(loc)
	for at, e := range t.Exprs() {
		ok = c.checkExpr(in, at, e, e.Check(), ph, job) && ok
	}
	return t, ok
}

// checkCondition checks the expressions in s, the if: at loc, which are
// shift bytes after where s puts them, as a wrapped if:'s are, and returns s
// parsed as checkString does. An if: that is one expression and nothing else
// gets the checks of a condition.
func (c *compiler) checkCondition(loc []string, s string, shift int, ph plan.Phase, job *jobInfo) (any, bool) {
	t, ok := c.parse(loc, s, shift)
	if t == nil {
		return s, ok
	}
	_, whole := t.Expr()
	in := c.spot(loc)
	for at, e := range t.Exprs() {
		found := e.Check()
		if whole {
			found = e.CheckCondition()
		}
		ok = c.checkExpr(in, at+shift, e, found, ph, job) && ok
	}
	return t, ok
}

// parse parses the expressions in s, the string at loc, which are shift
// bytes after where s puts them, and reports a syntax error. It returns nil
// if s has no expressions or doesn't parse.
func (c *compiler) parse(loc []string, s string, shift int) (*expr.Template, bool) {
	if !strings.Contains(s, "${{") {
		return nil, true
	}
	t, err := expr.ParseTemplate(s)
	if err != nil {
		c.reportError(loc, shift, err)
		return nil, false
	}
	if t.IsText() {
		return nil, true
	}
	return t, true
}

// checkExpr checks e, an expression whose source is at byte offset at in
// the string in, and reports problems, its probable mistakes. It returns
// false if e has an error.
func (c *compiler) checkExpr(in *spot, at int, e *expr.Expr, found []expr.Problem, ph plan.Phase, job *jobInfo) bool {
	for _, p := range found {
		c.reportProblem(in, at+p.Offset, p, job)
	}
	ok := true
	if e.Relative() {
		c.reportIn(in, at, problems.RelativeOutsideDefinitions, nil)
		ok = false
	}
	if used, _ := e.StatusChecks(); used && !ph.KnowsStatus() {
		c.reportIn(in, at, problems.StatusNotKnown, problems.Params{"phase": ph.String()})
		ok = false
	}
	for _, r := range e.Refs() {
		ok = c.checkRef(r, ph, job, in, at+r.Offset) && ok
	}
	return ok
}
