package reach_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/internal/reach"
)

func TestIndex(t *testing.T) {
	for key, want := range map[string]int{"0": 0, "00": 0, "+0": 0, "-0": 0, "12": 12, "007": 7} {
		if i, ok := reach.Index(key); !ok || i != want {
			t.Errorf("Index(%q) = %d, %v; want %d", key, i, ok, want)
		}
	}
	for _, key := range []string{"-1", "x", "", " 0", "1.0", "0x1", "1:2", "*", "1_0"} {
		if i, ok := reach.Index(key); ok {
			t.Errorf("Index(%q) = %d; want none", key, i)
		}
	}
}

func TestSlice(t *testing.T) {
	for _, tc := range []struct {
		key        string
		start, end int
	}{
		{"0:1", 0, 1},
		{":", 0, 5},
		{"1:", 1, 5},
		{":2", 0, 2},
		{"-1:", 4, 5},
		{"-2:-1", 3, 4},
		{"3:1", 3, 3},
		{"-9:9", 0, 5},
		{"+1:", 1, 5},
	} {
		if start, end, ok := reach.Slice(tc.key, 5); !ok || start != tc.start || end != tc.end {
			t.Errorf("Slice(%q, 5) = %d, %d, %v; want %d, %d", tc.key, start, end, ok, tc.start, tc.end)
		}
	}
	for _, key := range []string{"0", "a:1", "1:b", "::", "*"} {
		if _, _, ok := reach.Slice(key, 5); ok {
			t.Errorf("Slice(%q, 5) is a slice; want none", key)
		}
	}
}

// path splits a path written with dots.
func path(s string) []string {
	if s == "" {
		return []string{}
	}
	return strings.Split(s, ".")
}

// A path's patterns are the path itself when its keys read a list and a
// mapping alike. An index written another way reads a list's item by its
// index and a mapping's key as written. A slice reads a list's items, and
// the key after it one of them, any of them; it is a mapping's key as
// written too. A path with more than four keys that read two ways reaches
// all under the first of them.
func TestPatterns(t *testing.T) {
	for _, tc := range []struct {
		read string
		want []string
	}{
		{"rows.0.x", []string{"rows.0.x"}},
		{"rows.*.x", []string{"rows.*.x"}},
		{"rows.*.items.*.x", []string{"rows.*.items.*.x"}},
		{"rows.-1.x", []string{"rows.-1.x"}},
		{"", []string{""}},
		{"rows.00.x", []string{"rows.00.x", "rows.0.x"}},
		{"rows.+0.x", []string{"rows.+0.x", "rows.0.x"}},
		{"rows.-0.x", []string{"rows.-0.x", "rows.0.x"}},
		{"rows.007", []string{"rows.007", "rows.7"}},
		{"rows.0:1", []string{"rows.0:1", "rows"}},
		{"rows.-1:", []string{"rows.-1:", "rows"}},
		{"rows.0:1.0.x", []string{"rows.0:1.0.x", "rows.*.x"}},
		{"rows.-1:.00.x", []string{"rows.-1:.00.x", "rows.-1:.0.x", "rows.*.x"}},
		{"rows.1:.:1.0.x", []string{"rows.1:.:1.0.x", "rows.1:.*.x", "rows.*.x"}},
		{"rows.0:1.*.x", []string{"rows.0:1.*.x", "rows.*.x"}},
		{"rows.0:1.x", []string{"rows.0:1.x"}},
		{"rows.0:1.-1.x", []string{"rows.0:1.-1.x"}},
		{"a.00.b.00", []string{"a.00.b.00", "a.00.b.0", "a.0.b.00", "a.0.b.0"}},
		{"a.*.b.0:1.c.00.d.+1.e.-0.f.01", []string{"a.*.b"}},
	} {
		var got []string
		for p := range reach.Patterns(path(tc.read)) {
			got = append(got, strings.Join(p, "."))
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("Patterns(%s) = %q, want %q", tc.read, got, tc.want)
		}
	}
}

// reaches reports whether read reaches the value at value: by comparing
// each of its patterns with the value's path, with a trie that holds the
// value by its path, and with one that holds read's patterns, which must
// agree.
func reaches(t *testing.T, read, value []string) bool {
	t.Helper()
	var values, reads reach.Trie[int]
	values.Add(value, 1)
	compared, reached := false, false
	for p := range reach.Patterns(read) {
		compared = compared || reach.Reaches(p, value)
		reads.Add(p, 1)
		for range values.Reached(p) {
			reached = true
		}
	}
	reaching := false
	for range reads.Reaching(value) {
		reaching = true
	}
	if compared != reached || reached != reaching {
		t.Errorf("%q reaches %q: %v compared, %v by its value's path, %v by its patterns", read, value, compared, reached, reaching)
	}
	return reached
}

// A path reaches the values it reads, holds, or reads through, however it
// writes the keys that lead to them, and nothing beside them.
func TestReaches(t *testing.T) {
	for _, tc := range []struct {
		read, value string
		want        bool
	}{
		{"rows.0.x", "rows.0.x", true},
		{"rows", "rows.0.x", true},
		{"rows.0", "rows.0.x", true},
		{"rows.0.x.y", "rows.0.x", true},
		{"", "rows.0.x", true},
		{"rows.0.x", "", true},
		{"rows.1.x", "rows.0.x", false},
		{"rows.0.y", "rows.0.x", false},
		{"other", "rows.0.x", false},
		{"rows.00.x", "rows.0.x", true},
		{"rows.+0.x", "rows.0.x", true},
		{"rows.-0.x", "rows.0.x", true},
		{"rows.01.x", "rows.0.x", false},
		{"rows.-1.x", "rows.0.x", false},
		{"rows.00.x", "rows.00.x", true},
		{"rows.0.x", "rows.00.x", false},
		{"rows.*.x", "rows.0.x", true},
		{"rows.*.y", "rows.0.x", false},
		{"rows.*", "rows.0.x", true},
		{"*.0.x", "rows.0.x", true},
		{"*.*.*", "rows.0.x", true},
		{"rows.*.items.*.x", "rows.2.items.3.x", true},
		{"rows.*.items.*.y", "rows.2.items.3.x", false},
		{"rows.0:1", "rows.0.x", true},
		{"rows.-1:", "rows.4.x", true},
		{"rows.:", "rows.0.x", true},
		{"rows.0:1.0.x", "rows.0.x", true},
		{"rows.1:.0.x", "rows.3.x", true},
		{"rows.1:.0.y", "rows.3.x", false},
		{"rows.-1:.*.x", "rows.3.x", true},
		{"rows.-1:.x", "rows.3.x", false},
		{"rows.-1:.x", "rows", true},
		{"rows.0:1.x", "rows.0:1.x", true},
		{"m.0:1", "m.0:1.x", true},
		{"rows.0:1.-1", "rows.0", false},
		{"rows.0:1", "other.0", false},
		{"a.*.b.0:1.c.00.d.+1.e.-0.f.01", "a.x.b.y", true},
		{"a.*.b.0:1.c.00.d.+1.e.-0.f.01", "a.x.c", false},
	} {
		if got := reaches(t, path(tc.read), path(tc.value)); got != tc.want {
			t.Errorf("%s reaches %s: %v, want %v", tc.read, tc.value, got, tc.want)
		}
	}
}

// A trie finds each value whose path a pattern reaches, and each pattern
// that reaches a path, and none once removed.
func TestTrie(t *testing.T) {
	var values reach.Trie[string]
	for _, p := range []string{"a", "rows.0.x", "rows.0.y", "rows.1.x", "b.c"} {
		values.Add(path(p), p)
	}
	found := func(seq func(func(string) bool)) []string {
		var out []string
		for v := range seq {
			out = append(out, v)
		}
		slices.Sort(out)
		return out
	}
	for pattern, want := range map[string][]string{
		"rows.*.x":   {"rows.0.x", "rows.1.x"},
		"rows":       {"rows.0.x", "rows.0.y", "rows.1.x"},
		"*":          {"a", "b.c", "rows.0.x", "rows.0.y", "rows.1.x"},
		"a.z":        {"a"},
		"rows.0.x.z": {"rows.0.x"},
		"rows.2":     nil,
	} {
		if got := found(values.Reached(path(pattern))); !slices.Equal(got, want) {
			t.Errorf("Reached(%s) = %q, want %q", pattern, got, want)
		}
	}
	values.Remove(path("rows.0.x"), "rows.0.x")
	values.Remove(path("rows.9"), "rows.9")
	if got := found(values.Reached(path("rows.*.x"))); !slices.Equal(got, []string{"rows.1.x"}) {
		t.Errorf("Reached(rows.*.x) once rows.0.x is removed = %q", got)
	}

	var reads reach.Trie[string]
	for _, p := range []string{"rows.*.x", "rows", "rows.0.y", "*.0", "b"} {
		reads.Add(path(p), p)
	}
	for value, want := range map[string][]string{
		"rows.0.x": {"*.0", "rows", "rows.*.x"},
		"rows.1.y": {"rows"},
		"rows.0":   {"*.0", "rows", "rows.*.x", "rows.0.y"},
		"b.c":      {"b"},
		"*":        {"*.0"},
	} {
		if got := found(reads.Reaching(path(value))); !slices.Equal(got, want) {
			t.Errorf("Reaching(%s) = %q, want %q", value, got, want)
		}
	}
	// A caller can stop early, at any value.
	for _, seq := range []func(func(string) bool){values.Reached(path("*")), reads.Reaching(path("rows.0"))} {
		n := 0
		for range seq {
			n++
			break
		}
		if n != 1 {
			t.Errorf("stopping at the first value gave %d", n)
		}
	}
}
