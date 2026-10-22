package plan

import (
	"errors"
	"strconv"
	"strings"

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
	// secrets are the texts of the run's secrets the phase reads, for mask.
	secrets *secretTexts
}

func newEvaluation(phase Phase, c Contexts) *evaluation {
	return &evaluation{phase: phase, scope: NewScope(phase, c), secrets: &secretTexts{run: c.Run}}
}

// next returns an evaluation in phase of c, which keeps e's problems and
// what e noted to mask.
func (e *evaluation) next(phase Phase, c Contexts) *evaluation {
	return &evaluation{
		phase: phase, scope: NewScope(phase, c), diags: e.diags, masked: e.masked, seen: e.seen, masker: e.masker, secrets: e.secrets,
	}
}

// secretTexts are the texts of a run's secrets, RunValues.Masked, found
// the first time they are needed.
type secretTexts struct {
	run   RunValues
	texts []string
	found bool
}

// contain reports whether one of the secrets' texts holds s.
func (t *secretTexts) contain(s string) bool {
	if !t.found {
		t.texts, t.found = t.run.Masked(), true
	}
	for _, text := range t.texts {
		if strings.Contains(text, s) {
			return true
		}
	}
	return false
}

// value evaluates v, at pointer in the workflow, and notes the strings of
// what each of its templates that read a secret gives, and each expression
// in one that read one, which are to be masked (mask): not those of the
// rest of v, nor the text of a boolean, a number or null (Masker). ok is
// false, and the problem reported, if it fails.
func (e *evaluation) value(v Value, pointer string) (out any, read expr.Read, ok bool) {
	out, read, err := v.evalLeaves(e.scope, func(out any, read expr.Read) {
		if read.Marked {
			maskedStrings(out, e.mask)
		}
	})
	if err != nil {
		e.reportError(pointer, err, read)
		return nil, read, false
	}
	return out, read, true
}

// text evaluates v as value does, and gives its text, as a template makes
// text of a value (expr.Text).
func (e *evaluation) text(v Value, pointer string) (string, expr.Read, bool) {
	out, read, ok := e.value(v, pointer)
	return expr.Text(out), read, ok
}

// minDerived is how long a string made from a secret must be for a phase
// to give it to mask: a shorter one, such as yes, would mask common text
// wherever it appears. A secret's own text is masked however short, and an
// output made from one is masked whole (End). minField is how long one
// that a secret's own text holds must be, such as a password that
// fromJSON took from a secret's JSON.
const (
	minDerived = 8
	minField   = 4
)

// mask notes s, text made from a secret, if it is at least minDerived
// bytes long, or at least minField and in a secret's own text, but for the
// JSON words true, false and null.
func (e *evaluation) mask(s string) {
	if e.seen[s] || !e.masks(s) {
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

// masks reports whether mask notes s.
func (e *evaluation) masks(s string) bool {
	switch {
	case len(s) >= minDerived:
		return true
	case len(s) < minField || s == "true" || s == "false" || s == "null":
		return false
	}
	return e.secrets != nil && e.secrets.contain(s)
}

// A decision is what an if: decided.
type decision struct {
	// run is whether what the if: is on runs, and always whether its value
	// is the word always, the older spelling of always(), which format 1
	// reads more leniently (Contexts.StrictOutputs).
	run, always bool
	// outlivesCancel is whether it can be true once its job is stopping:
	// always or its word, cancelled(), or a negated success() or failure(),
	// as format 1 reads it.
	outlivesCancel bool
}

// condition evaluates an if:, v, at pointer, and decides whether what it is
// on runs, with the status functions as the scope's Status says: the
// default if:, success(), when v is unset; an if: whose value is the word
// always, always(); one that calls a status function, its value's
// truthiness; and any other, its truthiness and success(), whose place it
// doesn't take. ok is false if it fails.
func (e *evaluation) condition(v Value, pointer string) (d decision, ok bool) {
	return e.decide(v, pointer, true)
}

// cleanupCondition evaluates the if: of a step whose cleanup: is about to
// run, v, at pointer, as condition does, but without success(): the
// cleanup runs when the if: is unset, the word always, or true, whatever
// happened to the job, as format 1 evaluates its copy of the step's if:
// with the job's status as it ends. ok is false if it fails.
func (e *evaluation) cleanupCondition(v Value, pointer string) (d decision, ok bool) {
	return e.decide(v, pointer, false)
}

// decide is condition, which takes success() into account when gated is
// set, and cleanupCondition, which doesn't.
func (e *evaluation) decide(v Value, pointer string, gated bool) (d decision, ok bool) {
	status := e.scope.Status
	success := !gated || !status.Failed && !status.Cancelled
	if v.IsZero() {
		return decision{run: success}, true
	}
	out, read, err := v.EvalRead(e.scope)
	if err != nil {
		e.reportError(pointer, err, read)
		return decision{}, false
	}
	if s, isString := out.(string); isString && s == "always" {
		return decision{run: true, always: true, outlivesCancel: true}, true
	}
	used, afterCancel := statusChecks(v)
	return decision{run: expr.Truthy(out) && (success || used), outlivesCancel: afterCancel}, true
}

// statusChecks reports whether one of v's expressions calls a status
// function, and whether one can be true once its job is stopping
// (expr.Expr.StatusChecks).
func statusChecks(v Value) (used, afterCancel bool) {
	for _, e := range v.Exprs() {
		u, a := e.StatusChecks()
		used, afterCancel = used || u, afterCancel || a
	}
	return used, afterCancel
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
// params that show a value masked if read says it is private
// (problems.MaskValues). On the worker, the text of each param is masked
// as the job's logs are too, after its value is shown, as a last guard: a
// value read that holds a secret is marked as one, and one that the phase
// shows itself is masked before it is shown (reportInvalid), since what
// shows a value can quote it, escape it or cut it short.
func (e *evaluation) report(k *problems.Kind, pointer string, params problems.Params, read expr.Read) {
	if read.Private {
		params = problems.MaskValues(params)
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
	if e.masker != nil {
		v = maskStrings(v, e.masker)
	}
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

// maskStrings returns v, JSON values, with each string in it masked by
// masker.
func maskStrings(v any, masker *Masker) any {
	switch v := v.(type) {
	case string:
		return masker.Mask(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = maskStrings(item, masker)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[masker.Mask(k)] = maskStrings(item, masker)
		}
		return out
	}
	return v
}

// at returns the JSON Pointer of keys under pointer.
func at(pointer string, keys ...string) string { return jsonptr.Append(pointer, keys...) }
