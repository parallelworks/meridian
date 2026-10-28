// Package jsonvalue works with JSON values as Go holds them: maps of
// strings to values, lists, strings, numbers, booleans and nil.
package jsonvalue

import (
	"encoding/json/jsontext"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strconv"
)

// Decode reads a JSON value from dec, with each number as number makes it
// from its text. A key given twice, which dec reads only if its options
// allow it, has its later value, as encoding/json gives it.
func Decode(dec *jsontext.Decoder, number func(string) (any, error)) (any, error) {
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	switch tok.Kind() {
	case 'n':
		return nil, nil
	case 'f', 't':
		return tok.Bool(), nil
	case '"':
		return tok.String(), nil
	case '0':
		return number(tok.String())
	case '[':
		list := []any{}
		for i := 0; dec.PeekKind() != ']'; i++ {
			item, err := Decode(dec, number)
			if err != nil {
				return nil, fmt.Errorf("%d: %w", i, err)
			}
			list = append(list, item)
		}
		_, err := dec.ReadToken()
		return list, err
	}
	m := map[string]any{}
	for dec.PeekKind() != '}' {
		tok, err := dec.ReadToken()
		if err != nil {
			return nil, err
		}
		k := tok.String()
		item, err := Decode(dec, number)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", k, err)
		}
		m[k] = item
	}
	_, err = dec.ReadToken()
	return m, err
}

// Number returns the value of a JSON number from its text, as YAML reads
// one: an int64 when it is whole and fits, and a float64 otherwise. It
// fails for one too large for a float64, with strconv's error.
func Number(text string) (any, error) {
	if i, err := strconv.ParseInt(text, 10, 64); err == nil {
		return i, nil
	}
	return strconv.ParseFloat(text, 64)
}

// Clone copies v's lists and mappings deeply, so that changing the copy
// doesn't change v. Everything else is shared.
func Clone(v any) any {
	switch v := v.(type) {
	case map[string]any:
		return CloneMap(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = Clone(item)
		}
		return out
	}
	return v
}

// CloneMap copies a mapping as Clone does. A nil mapping copies as an empty
// one.
func CloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = Clone(v)
	}
	return out
}

// Sorted returns an iterator over a mapping's keys and values in the order
// of the keys, so that what walks a mapping does so the same way each time.
func Sorted[V any](m map[string]V) iter.Seq2[string, V] {
	return func(yield func(string, V) bool) {
		for _, k := range slices.Sorted(maps.Keys(m)) {
			if !yield(k, m[k]) {
				return
			}
		}
	}
}

// Memory estimates, from above, how many bytes of memory what the JSON
// text data decodes to holds: as Go holds JSON values, or as the structs
// of a type, with the text's strings. A flat factor on the text's length
// would not do, since a small object costs many times its text: a list of
// one-member objects holds 45 times as many bytes as its text, and one
// nested in another as deeply as it goes 55 times. So it is 16 bytes for
// each of the text's, as maps of many members cost, 384 more for each
// object, as a small map costs, and 64 more for each array. What a plan,
// or a run's start state with its text, decodes to was measured at 13% to
// 71% of it, of workflows of many shapes: those lists and nests, maps of a
// few hundred to thousands of short members, lists of short values and of
// empty ones, a matrix job of many instances and plans of many jobs and
// steps. data is valid JSON: a brace or a bracket in a string isn't
// counted.
func Memory(data []byte) int64 {
	var objects, arrays int64
	in := false // in a string
	for i := 0; i < len(data); i++ {
		switch c := data[i]; {
		case in && c == '\\':
			i++ // what is escaped, which may be a quote, is in the string
		case c == '"':
			in = !in
		case in:
		case c == '{':
			objects++
		case c == '[':
			arrays++
		}
	}
	return 16*int64(len(data)) + 384*objects + 64*arrays
}
