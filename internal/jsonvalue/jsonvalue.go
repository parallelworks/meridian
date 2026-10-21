// Package jsonvalue works with JSON values as Go holds them: maps of
// strings to values, lists, strings, numbers, booleans and nil.
package jsonvalue

import (
	"encoding/json/jsontext"
	"fmt"
	"iter"
	"maps"
	"slices"
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
