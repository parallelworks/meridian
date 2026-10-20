// Package jsonvalue works with JSON values as Go holds them: maps of
// strings to values, lists, strings, numbers, booleans and nil.
package jsonvalue

import (
	"iter"
	"maps"
	"slices"
)

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
