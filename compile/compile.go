package compile

import (
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
// returns no plan if any is an error.
func Compile(name string, data []byte, submitted map[string]any, opts Options) (*plan.Plan, problems.Diagnostics) {
	doc, diags := format.Load(name, data)
	if diags.HasErrors() {
		return nil, diags
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
	for k := range inputs.secrets {
		c.secrets["inputs."+k] = true
	}
	for _, v := range p.Run.DeferredInputs {
		for _, e := range v.Exprs() {
			for _, r := range e.Refs() {
				c.recordSecret(r)
			}
		}
	}
	p.Run.Secrets = slices.Sorted(maps.Keys(c.secrets))

	c.diags.Sort()
	if c.diags.HasErrors() {
		return nil, c.diags
	}
	for _, d := range c.diags.Warnings() {
		// A plan keeps each warning's code and params, so that each reader
		// sees it in their own language.
		d.Detail = ""
		p.Warnings = append(p.Warnings, d)
	}
	return p, c.diags
}

type compiler struct {
	reporter
	opts   Options
	inputs *inputResolver
	// secrets are the secret variables and inputs the run reads.
	secrets  map[string]bool
	jobInfos map[string]*jobInfo
}

// A reporter collects the problems found in a workflow.
type reporter struct {
	doc   *format.Document
	diags problems.Diagnostics
}

// report reports a problem of kind k at pos, with what is at loc in the
// workflow.
func (r *reporter) report(k *problems.Kind, pos problems.Position, loc []string, params problems.Params) {
	r.reportPointer(k, pos, jsonptr.Format(loc), params)
}

func (r *reporter) reportPointer(k *problems.Kind, pos problems.Position, pointer string, params problems.Params) {
	r.diags = append(r.diags, k.At(r.doc.File, pos, pointer, params))
}

// reportAt reports a problem of kind k with what is at loc: a mapping, such
// as an input's definition, at its key, and anything else where it starts.
func (r *reporter) reportAt(k *problems.Kind, loc []string, params problems.Params) {
	r.report(k, r.doc.ReportPosition(jsonptr.Format(loc)), loc, params)
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
		report(problems.ExpressionsBuildTooMuch, problems.Params{"limit": be.Limit}, 0, "")
		return
	}
	report(problems.ExpressionFailed, problems.Params{"reason": err.Error()}, 0, "")
}

// run compiles what the plan holds about the run as a whole.
func (c *compiler) run(source plan.Source) plan.Run {
	w := c.doc.Value
	r := plan.Run{
		Source:    source,
		Format:    c.doc.Format,
		Name:      str(w["name"]),
		Icon:      str(w["icon"]),
		Thumbnail: str(w["thumbnail"]),
		Inputs:    plan.DataOf(c.inputs.values),
		Env:       c.values([]string{"env"}, runCreated, nil),
		Timeout:   c.value([]string{"timeout"}, runCreated, nil),

		DeferredInputs: c.inputs.deferred(),
	}
	r.Description, _ = w["description"].(string)
	for _, p := range list(w["permissions"]) {
		if s, ok := p.(string); ok {
			r.Permissions = append(r.Permissions, s)
		}
	}
	for _, key := range []string{"sessions", "links"} {
		c.check([]string{key}, w[key], runCreated, nil)
	}
	c.checkDefinitions(definitionsPath, lookup(w, definitionsPath))
	return r
}

// checkDefinitions reports the probable mistakes in the expressions in v, at
// loc in the input definitions, such as inputs.pw-user written for
// inputs['pw-user']. Resolving the inputs reports the errors of the
// expressions it evaluates.
func (c *compiler) checkDefinitions(loc []string, v any) {
	switch v := v.(type) {
	case string:
		t, err := expr.ParseTemplate(v)
		if err != nil {
			return
		}
		ptr := jsonptr.Format(loc)
		for at, e := range t.Exprs() {
			for _, p := range e.Check() {
				c.reportProblem(c.doc.Locate(ptr, at+p.Offset), loc, p, nil)
			}
		}
	case []any:
		for i, item := range v {
			c.checkDefinitions(appendPath(loc, strconv.Itoa(i)), item)
		}
	case map[string]any:
		for k, item := range jsonvalue.Sorted(v) {
			c.checkDefinitions(appendPath(loc, k), item)
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
func (c *compiler) value(loc []string, ph phase, job *jobInfo) plan.Value {
	raw, ok := jsonptr.Lookup(c.doc.Value, loc)
	if !ok {
		return plan.Value{}
	}
	v, ok := c.check(loc, raw, ph, job)
	if !ok {
		return plan.Value{}
	}
	return plan.Literal(v)
}

// values compiles a mapping of values, such as an env:.
func (c *compiler) values(loc []string, ph phase, job *jobInfo) map[string]plan.Value {
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
// such as always or true, stays a word.
func (c *compiler) condition(loc []string, ph phase, job *jobInfo) plan.Value {
	raw, ok := jsonptr.Lookup(c.doc.Value, loc)
	if !ok {
		return plan.Value{}
	}
	s, isString := raw.(string)
	if !isString {
		return plan.Literal(raw)
	}
	shift := 0
	if cond, start, bare := bareCondition(s); bare {
		if !strings.ContainsAny(cond, " \t.()!=<>&|?[") {
			// A lone word stays a word: always means always(), and any other
			// is text, which is true.
			if e, err := expr.Parse(cond); err == nil {
				for _, p := range e.CheckCondition() {
					c.reportProblem(c.doc.Locate(jsonptr.Format(loc), start+p.Offset), loc, p, job)
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
	return plan.Literal(v)
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
// holds them. ok is false if a string has an error.
func (c *compiler) check(loc []string, raw any, ph phase, job *jobInfo) (any, bool) {
	switch v := raw.(type) {
	case string:
		return c.checkString(loc, v, ph, job)
	case []any:
		out, ok := make([]any, len(v)), true
		for i, item := range v {
			var itemOK bool
			out[i], itemOK = c.check(appendPath(loc, strconv.Itoa(i)), item, ph, job)
			ok = ok && itemOK
		}
		return out, ok
	case map[string]any:
		out, ok := make(map[string]any, len(v)), true
		for k, item := range jsonvalue.Sorted(v) {
			var itemOK bool
			out[k], itemOK = c.check(appendPath(loc, k), item, ph, job)
			ok = ok && itemOK
		}
		return out, ok
	}
	return raw, true
}

// checkString checks the expressions in s, the string at loc, and returns s
// parsed: a template, or s itself if it has no expressions.
func (c *compiler) checkString(loc []string, s string, ph phase, job *jobInfo) (any, bool) {
	t, ok := c.parse(loc, s, 0)
	if t == nil {
		return s, ok
	}
	for at, e := range t.Exprs() {
		ok = c.checkExpr(loc, at, e, e.Check(), ph, job) && ok
	}
	return t, ok
}

// checkCondition checks the expressions in s, the if: at loc, which are
// shift bytes after where s puts them, as a wrapped if:'s are, and returns s
// parsed as checkString does. An if: that is one expression and nothing else
// gets the checks of a condition.
func (c *compiler) checkCondition(loc []string, s string, shift int, ph phase, job *jobInfo) (any, bool) {
	t, ok := c.parse(loc, s, shift)
	if t == nil {
		return s, ok
	}
	_, whole := t.Expr()
	for at, e := range t.Exprs() {
		found := e.Check()
		if whole {
			found = e.CheckCondition()
		}
		ok = c.checkExpr(loc, at+shift, e, found, ph, job) && ok
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

// checkExpr checks e, an expression whose source is at byte offset at in the
// string at loc, and reports problems, its probable mistakes. It returns
// false if e has an error.
func (c *compiler) checkExpr(loc []string, at int, e *expr.Expr, found []expr.Problem, ph phase, job *jobInfo) bool {
	ptr := jsonptr.Format(loc)
	pos := func(offset int) problems.Position { return c.doc.Locate(ptr, at+offset) }
	for _, p := range found {
		c.reportProblem(pos(p.Offset), loc, p, job)
	}
	ok := true
	if e.Relative() {
		c.report(problems.RelativeOutsideDefinitions, pos(0), loc, nil)
		ok = false
	}
	if used, _ := e.StatusChecks(); used && !ph.has("status") {
		c.report(problems.StatusNotKnown, pos(0), loc, problems.Params{"phase": ph.name()})
		ok = false
	}
	for _, r := range e.Refs() {
		ok = c.checkRef(r, ph, job, pos(r.Offset), loc) && ok
	}
	return ok
}
