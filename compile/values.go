package compile

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/parallelworks/meridian/internal/hint"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
)

// Helpers for the JSON values a workflow and its inputs are made of.

// appendPath returns a new path: path followed by keys.
func appendPath(path []string, keys ...string) []string {
	return slices.Concat(path, keys)
}

// lookup returns the value at path in v, or nil.
func lookup(v any, path []string) any {
	v, _ = jsonptr.Lookup(v, path)
	return v
}

// setPath sets the value at path, making mappings on the way.
func setPath(values map[string]any, path []string, v any) {
	m := values
	for _, k := range path[:len(path)-1] {
		next, ok := m[k].(map[string]any)
		if !ok {
			next = map[string]any{}
			m[k] = next
		}
		m = next
	}
	m[path[len(path)-1]] = v
}

// deletePath removes the value at path, if there is one.
func deletePath(values map[string]any, path []string) {
	m := values
	for _, k := range path[:len(path)-1] {
		next, ok := m[k].(map[string]any)
		if !ok {
			return
		}
		m = next
	}
	delete(m, path[len(path)-1])
}

// notJSON describes the first part of v that isn't a JSON value of the
// kinds the compiler takes, or returns "". They are nil, booleans, finite
// float64, int and int64 numbers, UTF-8 text, and lists and mappings of them.
func notJSON(v any) string {
	switch v := v.(type) {
	case nil, bool, int, int64:
		return ""
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return formatNumber(v)
		}
		return ""
	case string:
		if !utf8.ValidString(v) {
			return "text that isn't UTF-8"
		}
		return ""
	case []any:
		for _, item := range v {
			if bad := notJSON(item); bad != "" {
				return bad
			}
		}
		return ""
	case map[string]any:
		for k, item := range jsonvalue.Sorted(v) {
			if !utf8.ValidString(k) {
				return "a key that isn't UTF-8"
			}
			if bad := notJSON(item); bad != "" {
				return bad
			}
		}
		return ""
	}
	return fmt.Sprintf("a Go %T", v)
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

// looseEqual compares an option with a value by their text, so that the
// option 8 matches the value "8".
func looseEqual(a, b any) bool {
	return describeRaw(a) == describeRaw(b)
}

func describeRaw(v any) string {
	if n, ok := toNumber(v); ok {
		if _, isString := v.(string); !isString {
			return formatNumber(n)
		}
	}
	return fmt.Sprint(v)
}

// describe shows a value in a message.
func describe(v any) string {
	switch v := v.(type) {
	case nil:
		return "null"
	case string:
		return strconv.Quote(v)
	case map[string]any:
		return "a mapping"
	case []any:
		return "a list"
	}
	return describeRaw(v)
}

// withSuggestion adds to msg, about name, which of names it most likely
// meant, if any.
func withSuggestion(msg, name string, names []string) string {
	if s, ok := hint.Suggest(name, names); ok {
		msg += fmt.Sprintf("; did you mean %q?", s)
	}
	return msg
}
