package compile

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Helpers for the JSON values a workflow and its inputs are made of.

func appendPath(path []string, keys ...string) []string {
	out := make([]string, 0, len(path)+len(keys))
	return append(append(out, path...), keys...)
}

// pointer formats a path as a JSON Pointer.
func pointer(path []string) string {
	escape := strings.NewReplacer("~", "~0", "/", "~1")
	var b strings.Builder
	for _, k := range path {
		b.WriteString("/")
		b.WriteString(escape.Replace(k))
	}
	return b.String()
}

// lookup returns the value at path in v, or nil.
func lookup(v any, path []string) any {
	v, _ = lookupOK(v, path)
	return v
}

func lookupOK(v any, path []string) (any, bool) {
	for _, k := range path {
		switch c := v.(type) {
		case map[string]any:
			var ok bool
			if v, ok = c[k]; !ok {
				return nil, false
			}
		case []any:
			i, err := strconv.Atoi(k)
			if err != nil || i < 0 || i >= len(c) {
				return nil, false
			}
			v = c[i]
		default:
			return nil, false
		}
	}
	return v, true
}

// lookupPath returns the value at path in a mapping of values.
func lookupPath(values map[string]any, path []string) (any, bool) {
	return lookupOK(values, path)
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

// copyMap copies a mapping of JSON values deeply.
func copyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = copyValue(v)
	}
	return out
}

func copyValue(v any) any {
	switch v := v.(type) {
	case map[string]any:
		return copyMap(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = copyValue(item)
		}
		return out
	}
	return v
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
		for _, k := range slices.Sorted(maps.Keys(v)) {
			if !utf8.ValidString(k) {
				return "a key that isn't UTF-8"
			}
			if bad := notJSON(v[k]); bad != "" {
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

// suggest returns the name a misspelled name most likely meant: one that
// differs only in case, hyphens and underscores, or else in a letter or so.
func suggest(name string, names []string) (string, bool) {
	norm := func(s string) string {
		return strings.ToLower(strings.NewReplacer("-", "", "_", "").Replace(s))
	}
	best, bestDist := "", max(1, len(name)/4)+1
	for _, n := range names {
		if norm(n) == norm(name) {
			return n, true
		}
		if d := levenshtein(name, n); d < bestDist && len(name) > 3 {
			best, bestDist = n, d
		}
	}
	return best, best != ""
}

func levenshtein(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	prev := make([]int, len(rb)+1)
	cur := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		cur[0] = i
		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(rb)]
}
