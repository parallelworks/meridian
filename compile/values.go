package compile

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
)

// Helpers for the JSON values a workflow and its inputs are made of.

// appendPath returns a new path: path followed by keys.
func appendPath(path []string, keys ...string) []string {
	return slices.Concat(path, keys)
}

// maxExact is 2^53: a float64 holds each whole number up to it, and no
// further.
const maxExact = 1 << 53

// floatNumbers returns v, a submitted value, with each whole number in it
// that a float64 doesn't hold exactly read as the float64 nearest it, as
// ACTIVATE reads a submitted JSON number, as a float64: 9007199254740993 is
// 9007199254740992, wherever it is read. It changes v's mappings and lists
// in place.
func floatNumbers(v any) any {
	switch v := v.(type) {
	case int:
		if n := int64(v); n > maxExact || n < -maxExact {
			return float64(v)
		}
	case int64:
		if v > maxExact || v < -maxExact {
			return float64(v)
		}
	case []any:
		for i, item := range v {
			v[i] = floatNumbers(item)
		}
	case map[string]any:
		for k, item := range v {
			v[k] = floatNumbers(item)
		}
	}
	return v
}

// lookup returns the value at path in v, or nil.
func lookup(v any, path []string) any {
	v, _ = jsonptr.Lookup(v, path)
	return v
}

// setIn sets the value at path in v, a mapping or list: it follows the
// lists on the way to the item a number names, and makes the mappings on the
// way that aren't there. It sets nothing if a list doesn't have the item.
func setIn(v any, path []string, x any) {
	for i, k := range path {
		last := i == len(path)-1
		switch c := v.(type) {
		case []any:
			n, err := strconv.Atoi(k)
			if err != nil || n < 0 || n >= len(c) {
				return
			}
			if last {
				c[n] = x
				return
			}
			v = c[n]
		case map[string]any:
			if last {
				c[k] = x
				return
			}
			switch c[k].(type) {
			case map[string]any, []any:
			default:
				c[k] = map[string]any{}
			}
			v = c[k]
		default:
			return
		}
	}
}

// clearPath removes the value at path in v, a mapping or list, as
// deletePath does, and makes a list's item there null, as a list can't
// leave it out without moving the items after it.
func clearPath(v any, path []string) {
	if n := len(path); n > 0 && isIndex(path[n-1]) {
		if _, isList := lookup(v, path[:n-1]).([]any); isList {
			setIn(v, path, nil)
			return
		}
	}
	deletePath(v, path)
}

// deletePath removes the value at path in v, a mapping or list, if a
// mapping holds one there: it follows the lists on the way, as setIn does.
func deletePath(v any, path []string) {
	for i, k := range path {
		switch c := v.(type) {
		case map[string]any:
			if i == len(path)-1 {
				delete(c, k)
				return
			}
			v = c[k]
		case []any:
			n, err := strconv.Atoi(k)
			if err != nil || n < 0 || n >= len(c) {
				return
			}
			v = c[n]
		default:
			return
		}
	}
}

// notJSON finds the first part of v that isn't a JSON value of the kinds the
// compiler takes, and says what it is, as the param what of a message that
// says so: a number that isn't finite, text or a key that isn't UTF-8, or a
// Go type, with the number or the type's name, and its path in v. It
// returns "" if v is JSON. JSON values are nil, booleans, finite float64,
// int and int64 numbers, UTF-8 text, and lists and mappings of them.
func notJSON(v any) (what, value string, at []string) {
	switch v := v.(type) {
	case nil, bool, int, int64:
		return "", "", nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return "number", formatNumber(v), nil
		}
		return "", "", nil
	case string:
		if !utf8.ValidString(v) {
			return "text", "", nil
		}
		return "", "", nil
	case []any:
		for i, item := range v {
			if what, value, at := notJSON(item); what != "" {
				return what, value, append([]string{strconv.Itoa(i)}, at...)
			}
		}
		return "", "", nil
	case map[string]any:
		for k, item := range jsonvalue.Sorted(v) {
			if !utf8.ValidString(k) {
				return "key", "", []string{k}
			}
			if what, value, at := notJSON(item); what != "" {
				return what, value, append([]string{k}, at...)
			}
		}
		return "", "", nil
	}
	return "go_type", fmt.Sprintf("%T", v), nil
}

// toNumber returns v as a number: a number, or text that is one.
func toNumber(v any) (float64, bool) {
	switch v := v.(type) {
	case float64:
		return v, !math.IsNaN(v) && !math.IsInf(v, 0)
	case int64:
		return float64(v), true
	case int:
		return float64(v), true
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		return n, err == nil && !math.IsNaN(n) && !math.IsInf(n, 0)
	}
	return 0, false
}

// number returns n as an int64 when it is whole, as a YAML integer reads.
func number(n float64) any {
	if n == math.Trunc(n) && math.Abs(n) < 1<<53 {
		return int64(n)
	}
	return n
}

func formatNumber(n float64) string {
	return strconv.FormatFloat(n, 'f', -1, 64)
}

// looseKey returns what an option is compared with a value by: its text,
// so that the option 8 matches the value "8". An option matches a value
// with the same key.
func looseKey(v any) string {
	if n, ok := toNumber(v); ok {
		if _, isString := v.(string); !isString {
			return formatNumber(n)
		}
	}
	return fmt.Sprint(v)
}

// parseFor returns v, a workflow's JSON values, with each string that holds
// an expression parsed as a template for the list rows at index.
func parseFor(v any, index []int) (any, error) {
	switch v := v.(type) {
	case string:
		t, err := expr.ParseTemplateFor(v, index)
		if err != nil || t.IsText() {
			return v, err
		}
		return t, nil
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			var err error
			if out[i], err = parseFor(item, index); err != nil {
				return nil, err
			}
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			var err error
			if out[k], err = parseFor(item, index); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	return v, nil
}

// parsed returns v, a workflow's JSON values, with each string that holds
// an expression parsed as a template, as the workflow's evaluator parses
// it, once. ok is false if one doesn't parse.
func (r *inputResolver) parsed(v any) (any, bool) {
	switch v := v.(type) {
	case string:
		if !strings.Contains(v, "${{") {
			return v, true
		}
		t, err := r.workflow.Template(v)
		if err != nil {
			return nil, false
		}
		if t.IsText() {
			return v, true
		}
		return t, true
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			var ok bool
			if out[i], ok = r.parsed(item); !ok {
				return nil, false
			}
		}
		return out, true
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			var ok bool
			if out[k], ok = r.parsed(item); !ok {
				return nil, false
			}
		}
		return out, true
	}
	return v, true
}

// isIndex reports whether a key in a path is a list's index.
func isIndex(key string) bool {
	_, err := strconv.Atoi(key)
	return err == nil
}

// indexPath returns path with each [index] in it the index it reads.
func indexPath(path []string, index []int) []string {
	out := slices.Clone(path)
	k := 0
	for i, name := range out {
		if name == "[index]" && k < len(index) {
			out[i] = strconv.Itoa(index[k])
			k++
		}
	}
	return out
}
