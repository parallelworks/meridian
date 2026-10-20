package expr

import (
	"bytes"
	"encoding/json"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/parallelworks/meridian/internal/jsonvalue"
)

// Values are JSON's: nil, bool, numbers, string, []any and map[string]any.
// Numbers that come into an evaluation can be float64, int64 or int; numbers
// it computes are float64, as JavaScript's are.

// number returns v as a float64 if it is a number.
func number(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	}
	return 0, false
}

// truthy reports whether v counts as true, as in JavaScript: everything but
// false, 0, NaN, "" and null.
func truthy(v any) bool {
	switch v := v.(type) {
	case nil:
		return false
	case bool:
		return v
	case string:
		return v != ""
	}
	if n, ok := number(v); ok {
		return n != 0 && !math.IsNaN(n)
	}
	return true
}

// toNumber converts v to a number as JavaScript's Number() does, except
// that a list or a mapping is NaN.
func toNumber(v any) float64 {
	switch v := v.(type) {
	case nil:
		return 0
	case bool:
		if v {
			return 1
		}
		return 0
	case string:
		return stringToNumber(v)
	}
	if n, ok := number(v); ok {
		return n
	}
	return math.NaN()
}

// stringToNumber reads a string as a number: surrounding space is ignored,
// an empty string is 0, and a string that isn't a number is NaN. Numbers
// are read as Go's strconv.ParseFloat reads them, as ACTIVATE does.
func stringToNumber(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return math.NaN()
	}
	return n
}

// formatNumber formats n as JavaScript's String(n) does: whole numbers
// without a fraction, and exponents for very large and very small numbers.
func formatNumber(n float64) string {
	switch {
	case math.IsNaN(n):
		return "NaN"
	case math.IsInf(n, 1):
		return "Infinity"
	case math.IsInf(n, -1):
		return "-Infinity"
	case n == 0:
		return "0"
	}
	// encoding/json chooses between the two forms as JavaScript does.
	if abs := math.Abs(n); abs >= 1e-6 && abs < 1e21 {
		return strconv.FormatFloat(n, 'f', -1, 64)
	}
	s := strconv.FormatFloat(n, 'e', -1, 64)
	// Go writes e-07 where JavaScript writes e-7.
	if i := strings.IndexByte(s, 'e'); i >= 0 && len(s) > i+3 && s[i+2] == '0' {
		s = s[:i+2] + s[i+3:]
	}
	return s
}

// stringOf converts v to a string as JavaScript's String() does, except
// that a list or a mapping is its JSON. The + operator and the string
// operators use it.
func stringOf(v any) string {
	switch v := v.(type) {
	case nil:
		return "null"
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	}
	if n, ok := v.(int64); ok {
		return strconv.FormatInt(n, 10)
	}
	if n, ok := number(v); ok {
		return formatNumber(n)
	}
	return toJSON(v, "")
}

// text converts v to the text that replaces an expression inside a longer
// string: like stringOf, but null is empty.
func text(v any) string {
	if v == nil {
		return ""
	}
	return stringOf(v)
}

// toJSON encodes v as JSON, with mapping keys in sorted order and indent as
// the indentation of each level, or on one line if indent is empty. A number
// JSON can't hold, NaN or an infinity, is null, as in JavaScript.
func toJSON(v any, indent string) string {
	var b bytes.Buffer
	writeJSON(&b, v, indent, 0)
	return b.String()
}

func writeJSON(b *bytes.Buffer, v any, indent string, depth int) {
	newline := func(depth int) {
		if indent != "" {
			b.WriteByte('\n')
			for range depth {
				b.WriteString(indent)
			}
		}
	}
	switch v := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		b.WriteString(strconv.FormatBool(v))
	case string:
		writeJSONString(b, v)
	case []any:
		if len(v) == 0 {
			b.WriteString("[]")
			return
		}
		b.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				b.WriteByte(',')
			}
			newline(depth + 1)
			writeJSON(b, item, indent, depth+1)
		}
		newline(depth)
		b.WriteByte(']')
	case map[string]any:
		if len(v) == 0 {
			b.WriteString("{}")
			return
		}
		b.WriteByte('{')
		for i, k := range slices.Sorted(maps.Keys(v)) {
			if i > 0 {
				b.WriteByte(',')
			}
			newline(depth + 1)
			writeJSONString(b, k)
			b.WriteByte(':')
			if indent != "" {
				b.WriteByte(' ')
			}
			writeJSON(b, v[k], indent, depth+1)
		}
		newline(depth)
		b.WriteByte('}')
	case int64:
		b.WriteString(strconv.FormatInt(v, 10))
	default:
		n, ok := number(v)
		if !ok || math.IsNaN(n) || math.IsInf(n, 0) {
			b.WriteString("null")
			return
		}
		b.WriteString(formatNumber(n))
	}
}

// writeJSONString writes s as a JSON string. Unlike encoding/json's default,
// it doesn't escape <, > and &, which mean nothing special in a workflow.
func writeJSONString(b *bytes.Buffer, s string) {
	enc := json.NewEncoder(b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s) // A string always encodes.
	b.Truncate(b.Len() - 1)
}

// parseJSON decodes JSON text into values.
func parseJSON(s string) (any, error) {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, err
	}
	return v, nil
}

// collection returns the list or mapping that a string holds as JSON, or v
// itself. Outputs are always strings, so operators that work on lists and
// mappings accept them in JSON.
func collection(v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	t := strings.TrimSpace(s)
	isList := strings.HasPrefix(t, "[") && strings.HasSuffix(t, "]")
	isMapping := strings.HasPrefix(t, "{") && strings.HasSuffix(t, "}")
	if !isList && !isMapping {
		return v
	}
	parsed, err := parseJSON(t)
	if err != nil {
		return v
	}
	return parsed
}

// looseEqual compares as JavaScript's == does for values that aren't lists
// or mappings, which are equal when their contents are.
func looseEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	an, aNum := number(a)
	bn, bNum := number(b)
	switch {
	case aNum && bNum:
		return an == bn
	case aNum || bNum:
		// A number and something else: the other side is converted, unless
		// it is a list or a mapping.
		other := b
		if bNum {
			other = a
		}
		switch other.(type) {
		case string, bool:
			return toNumber(a) == toNumber(b)
		}
		return false
	}
	switch a := a.(type) {
	case bool:
		if b, ok := b.(bool); ok {
			return a == b
		}
		if _, ok := b.(string); ok {
			return toNumber(a) == toNumber(b)
		}
		return false
	case string:
		switch b := b.(type) {
		case string:
			return a == b
		case bool:
			return toNumber(a) == toNumber(b)
		}
		return false
	}
	return deepEqual(a, b)
}

// strictEqual compares as JavaScript's === does for values that aren't lists
// or mappings, which are equal when their contents are.
func strictEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if an, ok := number(a); ok {
		bn, ok := number(b)
		return ok && an == bn
	}
	switch a := a.(type) {
	case bool:
		b, ok := b.(bool)
		return ok && a == b
	case string:
		b, ok := b.(string)
		return ok && a == b
	}
	return deepEqual(a, b)
}

// deepEqual reports whether two lists or mappings have equal contents, with
// numbers compared by value.
func deepEqual(a, b any) bool {
	switch a := a.(type) {
	case []any:
		b, ok := b.([]any)
		return ok && slices.EqualFunc(a, b, strictEqual)
	case map[string]any:
		b, ok := b.(map[string]any)
		return ok && maps.EqualFunc(a, b, strictEqual)
	}
	return false
}

// compare compares two values for <, >, <= and >=: two strings by their
// bytes, anything else as numbers. ok is false if either is NaN, which makes
// every comparison false.
func compare(a, b any) (c int, ok bool) {
	if as, isStr := a.(string); isStr {
		if bs, isStr := b.(string); isStr {
			return strings.Compare(as, bs), true
		}
	}
	an, bn := toNumber(a), toNumber(b)
	if math.IsNaN(an) || math.IsNaN(bn) {
		return 0, false
	}
	switch {
	case an < bn:
		return -1, true
	case an > bn:
		return 1, true
	}
	return 0, true
}

// property returns the value of a mapping's key, a list's index or a
// string's character. A key with a colon, such as 1:3 or -2:, is a slice of
// a list or a string. A string that holds JSON is read as its value.
func property(v any, key string) (any, bool) {
	switch v := v.(type) {
	case map[string]any:
		p, ok := v[key]
		return p, ok
	case []any:
		if start, end, ok := sliceBounds(key, len(v)); ok {
			return v[start:end], true
		}
		if i, err := strconv.Atoi(key); err == nil && i >= 0 && i < len(v) {
			return v[i], true
		}
	case string:
		if parsed, err := parseJSON(v); err == nil {
			return property(parsed, key)
		}
		runes := []rune(v)
		if start, end, ok := sliceBounds(key, len(runes)); ok {
			return string(runes[start:end]), true
		}
		if i, err := strconv.Atoi(key); err == nil && i >= 0 && i < len(runes) {
			return string(runes[i]), true
		}
	}
	return nil, false
}

// sliceBounds reads a slice such as 1:3, :2 or -2: of something n long. A
// negative bound counts from the end, and bounds are clamped to [0, n].
func sliceBounds(key string, n int) (start, end int, ok bool) {
	from, to, found := strings.Cut(key, ":")
	if !found {
		return 0, 0, false
	}
	start, end = 0, n
	var err error
	if from != "" {
		if start, err = strconv.Atoi(from); err != nil {
			return 0, 0, false
		}
	}
	if to != "" {
		if end, err = strconv.Atoi(to); err != nil {
			return 0, 0, false
		}
	}
	clamp := func(i int) int {
		if i < 0 {
			i += n
		}
		return min(max(i, 0), n)
	}
	start, end = clamp(start), clamp(end)
	return start, max(start, end), true
}

// key converts an operand of get to a property name: a number is an index.
func key(v any) (string, bool) {
	if s, ok := v.(string); ok {
		return s, true
	}
	if n, ok := number(v); ok && !math.IsNaN(n) && !math.IsInf(n, 0) {
		return strconv.FormatInt(int64(n), 10), true
	}
	return "", false
}

// contains reports whether a list has an element equal to x, a mapping has
// the key x, or a string has the substring x.
func contains(collection, x any) bool {
	switch c := collection.(type) {
	case []any:
		return slices.ContainsFunc(c, func(item any) bool { return looseEqual(item, x) })
	case map[string]any:
		k, ok := x.(string)
		if !ok {
			return false
		}
		_, ok = c[k]
		return ok
	case string:
		s, ok := x.(string)
		return ok && strings.Contains(c, s)
	}
	return false
}

// length returns the number of characters in a string, or of items in a
// list or mapping, and 0 for anything else.
func length(v any) float64 {
	switch v := collection(v).(type) {
	case string:
		return float64(utf8.RuneCountInString(v))
	case []any:
		return float64(len(v))
	case map[string]any:
		return float64(len(v))
	}
	return 0
}

// keys returns a mapping's keys in sorted order.
func keys(v any) []any {
	m, _ := collection(v).(map[string]any)
	out := make([]any, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		out = append(out, k)
	}
	return out
}

// values returns a mapping's values in the order of their keys.
func values(v any) []any {
	m, _ := collection(v).(map[string]any)
	out := make([]any, 0, len(m))
	for _, item := range jsonvalue.Sorted(m) {
		out = append(out, item)
	}
	return out
}

// elements returns a list's items, or a mapping's values in the order of
// their keys.
func elements(v any) []any {
	if list, ok := collection(v).([]any); ok {
		return list
	}
	return values(v)
}

// size estimates the bytes a value takes, to bound what an evaluation can
// build.
func size(v any) int {
	switch v := v.(type) {
	case string:
		return len(v)
	case []any:
		n := 8 * len(v)
		for _, item := range v {
			n += size(item)
		}
		return n
	case map[string]any:
		n := 8 * len(v)
		for k, item := range v {
			n += len(k) + size(item)
		}
		return n
	}
	return 8
}

func isDigit(c byte) bool { return '0' <= c && c <= '9' }
