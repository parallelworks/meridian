package expr

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// A function is one that expressions can call.
type function struct {
	minArgs int
	// maxArgs is -1 for a function that takes any number of arguments.
	maxArgs int
	// eval computes the function from its arguments. It is nil for the
	// status functions and case(), which the evaluator handles itself.
	eval func(e *evaluator, at int, args []any) (any, error)
}

func (f function) arity() string {
	switch {
	case f.maxArgs == 0:
		return "no arguments"
	case f.maxArgs < 0:
		return fmt.Sprintf("at least %d arguments", f.minArgs)
	case f.minArgs == 1 && f.maxArgs == 1:
		return "1 argument"
	case f.minArgs == f.maxArgs:
		return fmt.Sprintf("%d arguments", f.minArgs)
	}
	return fmt.Sprintf("%d to %d arguments", f.minArgs, f.maxArgs)
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
// meanings, and case(). String matching is case-sensitive, like ==.
var functions = map[string]function{
	"success":   {},
	"failure":   {},
	"cancelled": {},
	"canceled":  {},
	"always":    {},
	"case":      {minArgs: 3, maxArgs: -1},
	"contains": {minArgs: 2, maxArgs: 2, eval: func(_ *evaluator, _ int, a []any) (any, error) {
		return contains(a[0], a[1]), nil
	}},
	"startsWith": {minArgs: 2, maxArgs: 2, eval: func(_ *evaluator, _ int, a []any) (any, error) {
		return strings.HasPrefix(stringOf(a[0]), stringOf(a[1])), nil
	}},
	"endsWith": {minArgs: 2, maxArgs: 2, eval: func(_ *evaluator, _ int, a []any) (any, error) {
		return strings.HasSuffix(stringOf(a[0]), stringOf(a[1])), nil
	}},
	"format": {minArgs: 1, maxArgs: -1, eval: func(e *evaluator, at int, a []any) (any, error) {
		return format(e, at, stringOf(a[0]), a[1:])
	}},
	"join": {minArgs: 1, maxArgs: 2, eval: func(e *evaluator, at int, a []any) (any, error) {
		sep := ","
		if len(a) == 2 {
			sep = stringOf(a[1])
		}
		return e.join(at, a[0], sep)
	}},
	"toJSON": {minArgs: 1, maxArgs: 1, eval: func(e *evaluator, at int, a []any) (any, error) {
		return e.built(at, toJSON(a[0], "  "))
	}},
}

// format replaces {N} in tmpl with the Nth argument. {{ and }} are braces.
func format(e *evaluator, at int, tmpl string, args []any) (any, error) {
	var b strings.Builder
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
				return nil, e.errorAt(at, "format: %q has a '{' with no '}'", tmpl)
			}
			n, err := strconv.Atoi(tmpl[i+1 : i+end])
			if err != nil || n < 0 || n >= len(args) {
				return nil, e.errorAt(at, "format: %q has no argument for %s", tmpl, tmpl[i:i+end+1])
			}
			b.WriteString(stringOf(args[n]))
			if err := e.spend(at, b.Len()); err != nil {
				return nil, err
			}
			i += end
		case c == '}':
			return nil, e.errorAt(at, "format: %q has a '}' with no '{'", tmpl)
		default:
			b.WriteByte(c)
		}
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
		return nil, e.errorAt(at, "range takes whole numbers, not %s and %s", stringOf(start), stringOf(end))
	}
	if math.Abs(s) > maxExact || math.Abs(t) > maxExact {
		return nil, e.errorAt(at, "range takes numbers from -2^53 to 2^53, not %s and %s", formatNumber(s), formatNumber(t))
	}
	if t <= s {
		return []any{}, nil
	}
	if t-s > maxRange {
		return nil, e.errorAt(at, "the range from %s to %s has more than %d numbers", formatNumber(s), formatNumber(t), maxRange)
	}
	out := make([]any, int(t-s))
	for i := range out {
		out[i] = s + float64(i)
	}
	return e.built(at, out)
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
			return nil, e.errorAt(at, "fromJSON: %q isn't valid JSON", s)
		}
		return e.built(at, parsed)
	case "range":
		return e.rangeOf(at, 0.0, v)
	}
	return nil, e.errorAt(at, "unknown operator %s", op)
}
