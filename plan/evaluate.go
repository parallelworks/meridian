package plan

import (
	"errors"
	"math"
	"math/big"
	"regexp"
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

// Reading the values of what a phase evaluates. Each returns false for a
// value it doesn't read.

// durationPattern is a duration as format 1 writes one: whole seconds, or a
// whole number of seconds, minutes, hours or days.
var durationPattern = regexp.MustCompile(`^([0-9]+)([smhd]?)$`)

// durationUnits are a duration's units, in seconds.
var durationUnits = map[string]int64{"": 1, "s": 1, "m": 60, "h": 60 * 60, "d": 24 * 60 * 60}

// readDuration reads a duration in seconds: a whole number of seconds, or
// text as format 1 writes one, such as 300 or 10m. Null and "" are none,
// 0.
func readDuration(v any) (int64, bool) {
	v = wide(v)
	switch v := v.(type) {
	case nil:
		return 0, true
	case int64:
		return v, v >= 0
	case float64:
		return int64(v), v >= 0 && v == math.Trunc(v) && v < 1<<63
	case string:
		if v == "" {
			return 0, true
		}
		m := durationPattern.FindStringSubmatch(v)
		if m == nil {
			return 0, false
		}
		n, err := strconv.ParseInt(m[1], 10, 64)
		unit := durationUnits[m[2]]
		if err != nil || n > math.MaxInt64/unit {
			return 0, false
		}
		return n * unit, true
	}
	return 0, false
}

// readCount reads how many times something may run again, as format 1
// reads it: a number, or text that reads as one, with a fraction rounded
// down and a negative number 0.
func readCount(v any) (int, bool) {
	v = wide(v)
	var n float64
	switch v := v.(type) {
	case int64:
		n = float64(v)
	case float64:
		n = v
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, false
		}
		n = f
	default:
		return 0, false
	}
	switch {
	case math.IsNaN(n):
		return 0, false
	case n <= 0:
		return 0, true
	case n >= math.MaxInt32:
		return math.MaxInt32, true
	}
	return int(n), true
}

// readBool reads a value that turns something on or off, as format 1 reads
// ignore-errors: true or false, text that reads as one, or a number, which
// is true unless it is 0. Anything else is def.
func readBool(v any, def bool) bool {
	v = wide(v)
	switch v := v.(type) {
	case bool:
		return v
	case string:
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	case int64:
		return v != 0
	case float64:
		return v != 0 && !math.IsNaN(v)
	}
	return def
}

// wide returns v with an int, as Go values given to a Value can hold, as
// the int64 a plan's numbers are.
func wide(v any) any {
	if n, ok := v.(int); ok {
		return int64(n)
	}
	return v
}

// cpuPattern is a number of CPUs: cores, or thousandths of one with m.
var cpuPattern = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)(m?)$`)

// memoryPattern is an amount of memory: bytes, or a number with a unit.
var memoryPattern = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)(k|M|G|T|P|E|Ki|Mi|Gi|Ti|Pi|Ei)?$`)

// memoryUnits are the units of memory, in bytes.
var memoryUnits = map[string]*big.Int{
	"":  big.NewInt(1),
	"k": pow(1000, 1), "M": pow(1000, 2), "G": pow(1000, 3), "T": pow(1000, 4), "P": pow(1000, 5), "E": pow(1000, 6),
	"Ki": pow(1024, 1), "Mi": pow(1024, 2), "Gi": pow(1024, 3), "Ti": pow(1024, 4), "Pi": pow(1024, 5), "Ei": pow(1024, 6),
}

func pow(base, exp int64) *big.Int {
	return new(big.Int).Exp(big.NewInt(base), big.NewInt(exp), nil)
}

// readCPU reads a number of CPUs in thousandths of a core: a number of
// cores, as a number or text, or text of a whole number of thousandths
// with m, as in 500m. Null and "" are none.
func readCPU(v any) (int64, bool) {
	v = wide(v)
	switch v := v.(type) {
	case nil:
		return 0, true
	case int64:
		return scaled(strconv.FormatInt(v, 10), big.NewInt(1000))
	case float64:
		if v < 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, false
		}
		return scaled(strconv.FormatFloat(v, 'f', -1, 64), big.NewInt(1000))
	case string:
		if v == "" {
			return 0, true
		}
		m := cpuPattern.FindStringSubmatch(v)
		if m == nil {
			return 0, false
		}
		if m[2] == "m" {
			return scaled(m[1], big.NewInt(1))
		}
		return scaled(m[1], big.NewInt(1000))
	}
	return 0, false
}

// readMemory reads an amount of memory in bytes: a whole number of bytes,
// or text of a number with a unit, as in 8Gi. Null and "" are none.
func readMemory(v any) (int64, bool) {
	v = wide(v)
	switch v := v.(type) {
	case nil:
		return 0, true
	case int64:
		return v, v >= 0
	case float64:
		if v < 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, false
		}
		return scaled(strconv.FormatFloat(v, 'f', -1, 64), big.NewInt(1))
	case string:
		if v == "" {
			return 0, true
		}
		m := memoryPattern.FindStringSubmatch(v)
		if m == nil {
			return 0, false
		}
		return scaled(m[1], memoryUnits[m[2]])
	}
	return 0, false
}

// readGPU reads a number of GPUs: a whole number, or text of one. Null and
// "" are none.
func readGPU(v any) (int, bool) {
	v = wide(v)
	var n int64
	switch v := v.(type) {
	case nil:
		return 0, true
	case int64:
		n = v
	case float64:
		if v != math.Trunc(v) || v < 0 || v > math.MaxInt32 {
			return 0, false
		}
		n = int64(v)
	case string:
		if v == "" {
			return 0, true
		}
		var err error
		if n, err = strconv.ParseInt(v, 10, 64); err != nil {
			return 0, false
		}
	default:
		return 0, false
	}
	if n < 0 || n > math.MaxInt32 {
		return 0, false
	}
	return int(n), true
}

// scaled returns decimal, a non-negative number written in digits with an
// optional fraction, times unit, if that is a whole number that an int64
// holds.
func scaled(decimal string, unit *big.Int) (int64, bool) {
	whole, frac, _ := strings.Cut(decimal, ".")
	n, ok := new(big.Int).SetString(whole+frac, 10)
	if !ok || n.Sign() < 0 {
		return 0, false
	}
	n.Mul(n, unit)
	div := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(len(frac))), nil)
	q, r := new(big.Int).QuoRem(n, div, new(big.Int))
	if r.Sign() != 0 || !q.IsInt64() {
		return 0, false
	}
	return q.Int64(), true
}
