package compile

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/plan"
)

// Codes of the problems the compiler reports about jobs and expressions.
const (
	// CodeReference means an expression reads something the workflow
	// doesn't define: an input, a job's output, a step or a matrix value.
	CodeReference format.Code = "reference"
	// CodeContext means an expression reads a context that isn't known
	// where it is evaluated, such as steps in a job's if:.
	CodeContext format.Code = "context"
	// CodeNeeds means needs: names a job that isn't there, or jobs need
	// each other.
	CodeNeeds format.Code = "needs"
	// CodeDuplicateID means two steps of a job have the same id.
	CodeDuplicateID format.Code = "duplicate-id"
	// CodeWord means a word in an expression that is probably a mistake: it
	// looks like a reference, it decides a condition, or it is the older
	// spelling always.
	CodeWord format.Code = "word"
)

// Compile loads a workflow file and compiles it, with the input values
// submitted for a run, into a plan. It reports every problem it finds, and
// returns no plan if any is an error.
func Compile(name string, data []byte, submitted map[string]any, opts Options) (*plan.Plan, format.Diagnostics) {
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
	for _, d := range c.diags {
		if d.Severity == format.Warning {
			p.Warnings = append(p.Warnings, d)
		}
	}
	return p, c.diags
}

type compiler struct {
	doc    *format.Document
	opts   Options
	inputs *inputResolver
	// secrets are the secret variables and inputs the run reads.
	secrets  map[string]bool
	jobInfos map[string]*jobInfo
	diags    format.Diagnostics
}

func (c *compiler) report(severity format.Severity, code format.Code, pos format.Position, loc []string, msg string, args ...any) {
	c.diags = append(c.diags, format.Diagnostic{
		File: c.doc.File, Position: pos, Severity: severity, Code: code, Path: pointer(loc),
		Message: fmt.Sprintf(msg, args...),
	})
}

// reportAt reports a problem with what is at loc: a mapping at its key, and
// anything else where it starts.
func (c *compiler) reportAt(severity format.Severity, code format.Code, loc []string, msg string, args ...any) {
	ptr := pointer(loc)
	pos := c.doc.Position(ptr)
	if _, isMapping := lookup(c.doc.Value, loc).(map[string]any); isMapping {
		pos = c.doc.KeyPosition(ptr)
	}
	c.report(severity, code, pos, loc, msg, args...)
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
		c.check([]string{key}, lookup(w, []string{key}), runCreated, nil)
	}
	return r
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
	raw, ok := lookupOK(c.doc.Value, loc)
	if !ok {
		return plan.Value{}
	}
	if !c.check(loc, raw, ph, job) {
		return plan.Value{}
	}
	return c.parsed(loc, raw)
}

// parsed returns raw, the value at loc in the workflow, as a Value, and
// reports why if it can't be one.
func (c *compiler) parsed(loc []string, raw any) plan.Value {
	v, err := plan.ValueOf(raw)
	if err != nil {
		c.reportAt(format.Error, CodeExpression, loc, "%v", err)
		return plan.Value{}
	}
	return v
}

// values compiles a mapping of values, such as an env:.
func (c *compiler) values(loc []string, ph phase, job *jobInfo) map[string]plan.Value {
	m := mapping(lookup(c.doc.Value, loc))
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]plan.Value, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
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
	raw, ok := lookupOK(c.doc.Value, loc)
	if !ok {
		return plan.Value{}
	}
	s, isString := raw.(string)
	if !isString {
		return plan.Literal(raw)
	}
	if word := strings.TrimSpace(s); word != "" && !strings.Contains(s, "${{") && !strings.ContainsAny(word, " \t.()!=<>&|?[") {
		// A lone word stays a word: always means always(), and any other is
		// text, which is true.
		if e, err := expr.Parse(word); err == nil {
			for _, p := range e.CheckCondition() {
				c.report(format.Warning, CodeWord, c.doc.Locate(pointer(loc), p.Offset+len(s)-len(strings.TrimLeft(s, " \t"))), loc, "%s", p.Msg)
			}
		}
		return plan.Literal(s)
	}
	wrapped, shift := wrapBareCondition(s)
	if !c.checkString(loc, wrapped, shift, ph, job, true) {
		return plan.Value{}
	}
	return c.parsed(loc, wrapped)
}

// wrapBareCondition wraps an if: written without ${{ }} as an expression,
// and returns how far the wrapping moves a place in it.
func wrapBareCondition(s string) (string, int) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" || strings.Contains(trimmed, "${{") || !strings.ContainsAny(trimmed, " \t.()!=<>&|?[") {
		return s, 0
	}
	const open = "${{ "
	return open + trimmed + " }}", len(s) - len(strings.TrimLeft(s, " \t")) - len(open)
}

// check checks every string in raw, the value at loc in the workflow, and
// reports false if one has an error.
func (c *compiler) check(loc []string, raw any, ph phase, job *jobInfo) bool {
	ok := true
	switch v := raw.(type) {
	case string:
		ok = c.checkString(loc, v, 0, ph, job, false)
	case []any:
		for i, item := range v {
			ok = c.check(appendPath(loc, strconv.Itoa(i)), item, ph, job) && ok
		}
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(v)) {
			ok = c.check(appendPath(loc, k), v[k], ph, job) && ok
		}
	}
	return ok
}

// checkString checks the expressions in s, the string at loc, which is
// shift bytes after where s puts them, as a wrapped if: is. A condition also
// gets the checks of a condition.
func (c *compiler) checkString(loc []string, s string, shift int, ph phase, job *jobInfo, condition bool) bool {
	if !strings.Contains(s, "${{") {
		return true
	}
	ptr := pointer(loc)
	at := func(offset int) format.Position { return c.doc.Locate(ptr, offset+shift) }
	t, err := expr.ParseTemplate(s)
	if err != nil {
		var se *expr.SyntaxError
		offset := 0
		if errors.As(err, &se) {
			offset = se.Offset
		}
		c.report(format.Error, CodeExpression, at(offset), loc, "%v", err)
		return false
	}
	ok := true
	exprs, offsets := t.Exprs()
	for i, e := range exprs {
		base := offsets[i]
		problems := e.Check()
		if condition && len(exprs) == 1 && t.String() == "${{"+e.String()+"}}" {
			problems = e.CheckCondition()
		}
		for _, p := range problems {
			c.report(format.Warning, CodeWord, at(base+p.Offset), loc, "%s", p.Msg)
		}
		if e.Relative() {
			c.report(format.Error, CodeReference, at(base), loc,
				"a relative reference such as .name can only be used in input definitions")
			ok = false
		}
		if used, _ := e.StatusChecks(); used && !ph.has("status") {
			c.report(format.Error, CodeContext, at(base), loc, "status functions such as failure() aren't known %s", ph.when())
			ok = false
		}
		for _, r := range e.Refs() {
			msg, severity := c.checkRef(r, ph, job)
			if msg == "" {
				continue
			}
			code := CodeReference
			if !ph.has(r.Context) {
				code = CodeContext
			}
			c.report(severity, code, at(base+r.Offset), loc, "%s", msg)
			ok = ok && severity != format.Error
		}
	}
	return ok
}
