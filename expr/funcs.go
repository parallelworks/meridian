package expr

import (
	"math"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/problems"
)

// A function is one that expressions can call.
type function struct {
	minArgs int
	// maxArgs is -1 for a function that takes any number of arguments.
	maxArgs int
	// eval computes the function from its arguments. It is nil for the
	// status functions, case(), map() and filter(), which the evaluator
	// handles itself.
	eval func(e *evaluator, at int, args []any) (any, error)
	// each is set for a function whose last argument is a lambda, which
	// it runs for each item of its first: map() and filter(). It returns
	// what the function's list gets for an item, given what the lambda
	// gave for it, and keep false if it gets nothing.
	each func(item, result any) (v any, keep bool)
	// reads is set for a function that reads its arguments, each of which
	// costs its size toward the work of the template that calls it.
	reads bool
}

// arity returns the params that say how many arguments f takes: arity is
// none, exact, at_least or range, between min and max.
func (f function) arity() problems.Params {
	arity := "range"
	switch {
	case f.maxArgs == 0:
		arity = "none"
	case f.maxArgs < 0:
		arity = "at_least"
	case f.minArgs == f.maxArgs:
		arity = "exact"
	}
	return problems.Params{"arity": arity, "min": f.minArgs, "max": f.maxArgs}
}

// statusFunctions report how the job or step got here. Each is true when
// its condition holds; canceled is another spelling of cancelled.
var statusFunctions = map[string]func(*Status) bool{
	"success":   func(s *Status) bool { return !s.Failed && !s.Cancelled },
	"failure":   func(s *Status) bool { return s.Failed },
	"cancelled": func(s *Status) bool { return s.Cancelled },
	"canceled":  func(s *Status) bool { return s.Cancelled },
	"always":    func(*Status) bool { return true },
}

// functions are GitHub Actions' expression functions, with its names and
// meanings, case(), and map() and filter(), which take a lambda. String
// matching is case-sensitive, like ==.
var functions = map[string]function{
	"success":   {},
	"failure":   {},
	"cancelled": {},
	"canceled":  {},
	"always":    {},
	"case":      {minArgs: 3, maxArgs: -1},
	"contains": {minArgs: 2, maxArgs: 2, reads: true, eval: func(_ *evaluator, _ int, a []any) (any, error) {
		return contains(a[0], a[1]), nil
	}},
	"startsWith": {minArgs: 2, maxArgs: 2, reads: true, eval: func(_ *evaluator, _ int, a []any) (any, error) {
		return strings.HasPrefix(stringOf(a[0]), stringOf(a[1])), nil
	}},
	"endsWith": {minArgs: 2, maxArgs: 2, reads: true, eval: func(_ *evaluator, _ int, a []any) (any, error) {
		return strings.HasSuffix(stringOf(a[0]), stringOf(a[1])), nil
	}},
	"format": {minArgs: 1, maxArgs: -1, reads: true, eval: func(e *evaluator, at int, a []any) (any, error) {
		return format(e, at, stringOf(a[0]), a[1:])
	}},
	"join": {minArgs: 1, maxArgs: 2, reads: true, eval: func(e *evaluator, at int, a []any) (any, error) {
		if len(a) == 2 {
			return e.join(at, a[0], stringOf(a[1]))
		}
		// The separator it reads when it isn't given is ",".
		const sep = ","
		if err := e.read(at, sep); err != nil {
			return nil, err
		}
		return e.join(at, a[0], sep)
	}},
	"toJSON": {minArgs: 1, maxArgs: 1, reads: true, eval: func(e *evaluator, at int, a []any) (any, error) {
		return e.built(at, toJSON(a[0], "  "))
	}},
	"map": {minArgs: 2, maxArgs: 2, each: func(_, result any) (any, bool) {
		return result, true
	}},
	// filter keeps an item its lambda gives a true value for, by the
	// language's truthiness, so the text "false" is true:
	// filter(hosts, h => h.enabled) keeps a host whose enabled is "false".
	"filter": {minArgs: 2, maxArgs: 2, each: func(item, result any) (any, bool) {
		return item, truthy(result)
	}},
}

// format replaces {N} in tmpl with the Nth argument. {{ and }} are braces.
// It counts the string it builds once, a piece at a time: each argument
// before it is written, with the text written since the last count, so that
// what it counts is the string's length.
func format(e *evaluator, at int, tmpl string, args []any) (any, error) {
	var b strings.Builder
	counted := 0
	for i := 0; i < len(tmpl); i++ {
		c := tmpl[i]
		switch {
		case c == '{' && i+1 < len(tmpl) && tmpl[i+1] == '{':
			b.WriteByte('{')
			i++
		case c == '}' && i+1 < len(tmpl) && tmpl[i+1] == '}':
			b.WriteByte('}')
			i++
		case c == '{':
			end := strings.IndexByte(tmpl[i:], '}')
			if end < 0 {
				return nil, e.errorAt(at, problems.FormatUnclosedBrace, problems.Params{"template": problems.Show(tmpl)})
			}
			n, err := strconv.Atoi(tmpl[i+1 : i+end])
			if err != nil || n < 0 || n >= len(args) {
				return nil, e.errorAt(at, problems.FormatMissingArgument,
					problems.Params{"template": problems.Show(tmpl), "placeholder": tmpl[i : i+end+1]})
			}
			s := stringOf(args[n])
			if err := e.spend(at, b.Len()-counted+len(s)); err != nil {
				return nil, err
			}
			b.WriteString(s)
			counted = b.Len()
			i += end
		case c == '}':
			return nil, e.errorAt(at, problems.FormatUnopenedBrace, problems.Params{"template": problems.Show(tmpl)})
		default:
			b.WriteByte(c)
		}
	}
	if err := e.spend(at, b.Len()-counted); err != nil {
		return nil, err
	}
	return b.String(), nil
}

// join joins a list's items, as strings, with sep. A string joins to itself,
// and anything else to "". The function and the word operator share it. It
// counts what it builds before building it, because a long separator is
// repeated for every item.
func (e *evaluator) join(at int, v any, sep string) (any, error) {
	var list []any
	switch c := collection(v).(type) {
	case string:
		return c, nil
	case []any:
		list = c
	default:
		return "", nil
	}
	parts := make([]string, len(list))
	n := len(sep) * max(len(list)-1, 0)
	for i, item := range list {
		parts[i] = stringOf(item)
		n += len(parts[i])
	}
	if err := e.spend(at, n); err != nil {
		return nil, err
	}
	return strings.Join(parts, sep), nil
}

// maxRange is the most numbers range produces.
const maxRange = 10000

// maxExact is the largest whole number that float64 holds along with every
// whole number below it.
const maxExact = 1 << 53

// rangeOf returns the whole numbers from start up to but not including end.
func (e *evaluator) rangeOf(at int, start, end any) (any, error) {
	s, t := toNumber(start), toNumber(end)
	if s != math.Trunc(s) || t != math.Trunc(t) {
		return nil, e.errorAt(at, problems.RangeNotWhole, problems.Params{"start": stringOf(start), "end": stringOf(end)})
	}
	if math.Abs(s) > maxExact || math.Abs(t) > maxExact {
		return nil, e.errorAt(at, problems.RangeOutOfBounds, problems.Params{"start": formatNumber(s), "end": formatNumber(t)})
	}
	if t <= s {
		return []any{}, nil
	}
	if t-s > maxRange {
		return nil, e.errorAt(at, problems.RangeTooLong,
			problems.Params{"start": formatNumber(s), "end": formatNumber(t), "max": maxRange})
	}
	out := make([]any, int(t-s))
	for i := range out {
		out[i] = s + float64(i)
	}
	return e.built(at, out)
}

// readsOperand reports whether the prefix operator op reads its operand v,
// which costs v's size. length reads a string, whose characters it counts or
// which it reads as JSON, but not a list or a mapping, whose length it
// knows.
func readsOperand(op string, v any) bool {
	switch op {
	case "upper", "lower", "trim", "keys", "values", "fromJSON":
		return true
	case "length", "len":
		_, isString := v.(string)
		return isString
	}
	return false
}

// roundHalfUp rounds n to a whole number as JavaScript's Math.round does: a
// half rounds up, toward +Infinity, so -2.5 is -2. n+0.5 rounded down would
// round 0.49999999999999994 to 1, because the sum rounds to 1.
func roundHalfUp(n float64) float64 {
	r := math.Floor(n)
	if n-r >= 0.5 {
		r++
	}
	return r
}

// prefix applies a unary operator.
func (e *evaluator) prefix(at int, op string, v any) (any, error) {
	if readsOperand(op, v) {
		if err := e.read(at, v); err != nil {
			return nil, err
		}
	}
	switch op {
	case "!":
		return !truthy(v), nil
	case "-":
		return -toNumber(v), nil
	case "length", "len":
		return length(v), nil
	case "upper":
		return e.built(at, strings.ToUpper(stringOf(v)))
	case "lower":
		return e.built(at, strings.ToLower(stringOf(v)))
	case "trim":
		return strings.TrimSpace(stringOf(v)), nil
	case "keys":
		return keys(v), nil
	case "values":
		return values(v), nil
	case "abs":
		return math.Abs(toNumber(v)), nil
	case "floor":
		return math.Floor(toNumber(v)), nil
	case "ceil":
		return math.Ceil(toNumber(v)), nil
	case "round":
		return roundHalfUp(toNumber(v)), nil
	case "fromJSON":
		s, ok := v.(string)
		if !ok {
			return v, nil
		}
		parsed, err := parseJSON(s)
		if err != nil {
			return nil, e.errorAt(at, problems.FromJSONInvalid, problems.Params{"text": problems.Show(s)})
		}
		return e.built(at, parsed)
	case "range":
		return e.rangeOf(at, 0.0, v)
	}
	return nil, e.errorAt(at, problems.UnknownOperator, problems.Params{"op": op})
}
