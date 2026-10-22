package plan

import (
	"errors"
	"strconv"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/problems"
)

// An evaluation evaluates what a phase leaves to a job or a step, each
// value in its scope as a group of its own, and gathers the problems it
// finds and the text of what it made from a secret.
type evaluation struct {
	phase Phase
	scope expr.Scope
	diags problems.Diagnostics
	// masked holds the text of each value made from a secret, once.
	masked []string
	seen   map[string]bool
	// masker, if set, masks the params of what is reported: on the worker,
	// where a step's outputs are read as they were written, which can hold
	// a secret's value. It masks what masked holds too.
	masker *Masker
}

func newEvaluation(phase Phase, c Contexts) *evaluation {
	return &evaluation{phase: phase, scope: NewScope(phase, c)}
}

// value evaluates v, at pointer in the workflow, and notes the text of what
// it gives if that was made from a secret. ok is false, and the problem
// reported, if it fails.
func (e *evaluation) value(v Value, pointer string) (out any, read expr.Read, ok bool) {
	out, read, err := v.EvalRead(e.scope)
	if err != nil {
		e.reportError(pointer, err, read)
		return nil, read, false
	}
	if read.Marked {
		maskedTexts(out, e.mask)
	}
	return out, read, true
}

// text evaluates v as value does, and gives its text, as a template makes
// text of a value (expr.Text).
func (e *evaluation) text(v Value, pointer string) (string, expr.Read, bool) {
	out, read, ok := e.value(v, pointer)
	return expr.Text(out), read, ok
}

// mask notes s, text made from a secret.
func (e *evaluation) mask(s string) {
	if s == "" || e.seen[s] {
		return
	}
	if e.seen == nil {
		e.seen = map[string]bool{}
	}
	e.seen[s] = true
	e.masked = append(e.masked, s)
	if e.masker != nil {
		e.masker.Add(s)
	}
}

// condition evaluates an if:, v, at pointer, and reports whether what it
// is on runs, with the status functions as the scope's Status says: the
// default if:, success(), when v is unset; an if: whose value is the word
// always, always(); one that calls a status function, its value's
// truthiness; and any other, its truthiness and success(), whose place it
// doesn't take. ok is false if it fails.
func (e *evaluation) condition(v Value, pointer string) (run, ok bool) {
	status := e.scope.Status
	success := !status.Failed && !status.Cancelled
	if v.IsZero() {
		return success, true
	}
	out, read, err := v.EvalRead(e.scope)
	if err != nil {
		e.reportError(pointer, err, read)
		return false, false
	}
	if s, isString := out.(string); isString && s == "always" {
		return true, true
	}
	return expr.Truthy(out) && (success || callsStatus(v)), true
}

// callsStatus reports whether one of v's expressions calls a status
// function.
func callsStatus(v Value) bool {
	for _, e := range v.Exprs() {
		if used, _ := e.StatusChecks(); used {
			return true
		}
	}
	return false
}

// readsContext reports whether one of v's expressions reads context.
func readsContext(v Value, context string) bool {
	for _, e := range v.Exprs() {
		for _, r := range e.Refs() {
			if r.Context == context {
				return true
			}
		}
	}
	return false
}

// report reports a problem of kind k with what is at pointer, with the
// params that show a value masked if read says it is private.
func (e *evaluation) report(k *problems.Kind, pointer string, params problems.Params, read expr.Read) {
	if read.Private {
		params = maskParams(params)
	}
	if e.masker != nil {
		masked := make(problems.Params, len(params))
		for name, v := range params {
			if s, ok := v.(string); ok {
				v = e.masker.Mask(s)
			}
			masked[name] = v
		}
		params = masked
	}
	e.diags = append(e.diags, k.At("", problems.Position{}, pointer, params))
}

// reportInvalid reports a value, v, that doesn't fit its key: a problem of
// kind k, with v as its param value, and key, if it isn't "", as its param
// key.
func (e *evaluation) reportInvalid(k *problems.Kind, pointer, key string, v any, read expr.Read) {
	params := problems.Params{"value": problems.Show(v)}
	if key != "" {
		params["key"] = key
	}
	e.report(k, pointer, params, read)
}

// reportType reports a value, v, at pointer that isn't of one of the types
// the format takes there, by their names in the schema, such as string.
func (e *evaluation) reportType(pointer string, v any, read expr.Read, types ...string) {
	params := problems.Params{"count": len(types), "has_got": true, "got": typeName(v)}
	for i, t := range types {
		params["type"+strconv.Itoa(i+1)] = t
	}
	// The type of a value says nothing of it, so it isn't masked.
	e.report(problems.WrongType, pointer, params, expr.Read{})
}

// typeName returns the name in a schema of v's type.
func typeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case int64, int, float64:
		return "number"
	case []any:
		return "array"
	}
	return "object"
}

// reportError reports err, an error evaluating the value at pointer, which
// read what read says: as its problem, with the values it shows masked if
// the evaluation read a private value.
func (e *evaluation) reportError(pointer string, err error, read expr.Read) {
	if ee, ok := errors.AsType[*expr.EvalError](err); ok {
		e.report(ee.Kind, pointer, ee.Params, read)
		return
	}
	if se, ok := errors.AsType[*expr.SyntaxError](err); ok {
		e.report(se.Kind, pointer, se.Params, expr.Read{})
		return
	}
	if be, ok := errors.AsType[*expr.BudgetError](err); ok {
		e.report(be.Kind, pointer, problems.Params{"limit": be.Limit}, expr.Read{})
		return
	}
	if ue, ok := errors.AsType[*expr.UnknownError](err); ok {
		if ue.Name == "status" {
			e.report(problems.StatusNotKnown, pointer, problems.Params{"phase": e.phase.String()}, expr.Read{})
			return
		}
		e.report(problems.ContextNotKnown, pointer, problems.Params{"context": ue.Name, "phase": e.phase.String()}, expr.Read{})
		return
	}
	if xe, ok := errors.AsType[*ExpandError](err); ok {
		e.report(xe.Kind, xe.Path, xe.Params, expr.Read{})
		return
	}
	e.report(problems.ExpressionFailed, pointer, problems.Params{"reason": err.Error()}, read)
}

// workflowParams are the params of the problems a value can have that show
// the workflow, such as a key or a job's name, or Meridian's own limits,
// rather than the value, as the compiler has them.
var workflowParams = map[string]bool{
	"input": true, "job": true, "key": true, "name": true, "type": true, "what": true,
	"options": true, "min": true, "max": true, "limit": true, "op": true,
}

// maskParams returns params, those of a problem with a value made from a
// variable or a password, with what they show of the value masked: each
// param but those that show the workflow is problems.Masked, and there is
// no suggestion, which would be near the value.
func maskParams(params problems.Params) problems.Params {
	out := make(problems.Params, len(params))
	for name, v := range params {
		if !workflowParams[name] {
			v = problems.Masked
		}
		out[name] = v
	}
	if _, ok := params["suggested"]; ok {
		problems.WithSuggestion(out, "", false)
	}
	return out
}

// at returns the JSON Pointer of keys under pointer.
func at(pointer string, keys ...string) string { return jsonptr.Append(pointer, keys...) }
