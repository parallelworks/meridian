package reach_test

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/internal/reach"
)

// An index is a whole number that isn't negative. One too large for an int,
// where an int is 32 bits, is an index of no list's item.
func TestIndex(t *testing.T) {
	for key, want := range map[string]int{"0": 0, "00": 0, "+0": 0, "-0": 0, "12": 12, "007": 7} {
		if i, ok := reach.Index(key); !ok || i != want {
			t.Errorf("Index(%q) = %d, %v; want %d", key, i, ok, want)
		}
	}
	for _, key := range []string{"-1", "x", "", " 0", "1.0", "0x1", "1:2", "*", "1_0", "99999999999999999999"} {
		if i, ok := reach.Index(key); ok {
			t.Errorf("Index(%q) = %d; want none", key, i)
		}
	}
	if i, ok := reach.Index("3000000000"); ok != (strconv.IntSize == 64) || ok && strconv.Itoa(i) != "3000000000" {
		t.Errorf("Index(3000000000) = %d, %v with %d-bit ints", i, ok, strconv.IntSize)
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
		{"3000000000:", 5, 5},
		{":3000000000", 0, 5},
		{"-3000000000:", 0, 5},
		{":-3000000000", 0, 0},
		{"999999999999999999:", 5, 5},
		{"-999999999999999999:", 0, 5},
		{"-1000000000000000000:", 0, 5},
		{"9223372036854775807:", 5, 5},
		{"-9223372036854775808:", 0, 5},
	} {
		if start, end, ok := reach.Slice(tc.key, 5); !ok || start != tc.start || end != tc.end {
			t.Errorf("Slice(%q, 5) = %d, %d, %v; want %d, %d", tc.key, start, end, ok, tc.start, tc.end)
		}
	}
	for _, key := range []string{"0", "a:1", "1:b", "::", "*", "99999999999999999999:", ":-99999999999999999999"} {
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
// the index after it one of them: the item it counts to from where the
// slice starts, or slices of it start, if their bounds count from the
// list's start, and any of them if one counts from its end. A slice is a
// mapping's key as written too. A path with more than four keys that read
// two ways reaches all under the first of them.
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
		{"rows.0:1.0.x", []string{"rows.0:1.0.x", "rows.0.x"}},
		{"rows.1:.0.x", []string{"rows.1:.0.x", "rows.1.x"}},
		{"rows.1:3.1.x", []string{"rows.1:3.1.x", "rows.2.x"}},
		{"rows.1:2.1.x", []string{"rows.1:2.1.x"}},
		{"rows.:2.00.x", []string{"rows.:2.00.x", "rows.:2.0.x", "rows.0.x"}},
		{"rows.-1:.00.x", []string{"rows.-1:.00.x", "rows.-1:.0.x", "rows.*.x"}},
		{"rows.1:.:-1.0.x", []string{"rows.1:.:-1.0.x", "rows.1:.*.x", "rows.*.x"}},
		{"rows.1:.:1.0.x", []string{"rows.1:.:1.0.x", "rows.1:.0.x", "rows.1.x"}},
		{"rows.2:.1:.0.x", []string{"rows.2:.1:.0.x", "rows.2:.1.x", "rows.3.x"}},
		{"rows.1:3.5:.0.x", []string{"rows.1:3.5:.0.x", "rows.1:3.5.x"}},
		{"rows.9223372036854775807:.1.x", []string{"rows.9223372036854775807:.1.x", "rows.*.x"}},
		{"rows.3000000000:.1.x", []string{"rows.3000000000:.1.x", "rows.3000000001.x"}},
		{"rows.999999999999999999:.1.x", []string{"rows.999999999999999999:.1.x", "rows.1000000000000000000.x"}},
		{"rows.+01000000000000000000:.1.x", []string{"rows.+01000000000000000000:.1.x", "rows.1000000000000000001.x"}},
		{"rows.03000000000.x", []string{"rows.03000000000.x", "rows.3000000000.x"}},
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
		{"rows.1:.0.x", "rows.1.x", true},
		{"rows.1:.0.x", "rows.3.x", false},
		{"rows.1:.0.x", "rows.0.x", false},
		{"rows.1:.0.y", "rows.1.x", false},
		{"rows.-1:.0.x", "rows.3.x", true},
		{"rows.0:1.1.x", "rows.1.x", false},
		{"rows.0:1.1.x", "rows", true},
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

// A path of indexes and slices, slices of slices among them, reaches each
// item of a list it reads, whatever the list's length, as a path reads
// them with Index and Slice. One whose slices' bounds all count from the
// list's start reaches no other item.
func TestPatternsReachWhatPathsRead(t *testing.T) {
	keys := []string{"0", "1", "2", "00", "+1", "-0", "-1", ":", "1:", "2:", ":2", "0:1", "1:3", "3:1", "-1:", ":-1", "-2:-1"}
	fromEnd := func(k string) bool { return strings.Contains(k, ":") && strings.Contains(k, "-") && k != "-0" }
	// read returns the item of a list of n items whose x path reads, and
	// whether it reads one: it reads slices of the list's items, by their
	// indexes, until its last key, an index, reads an item. An item has
	// only x, so a path with more keys after the index reads nothing.
	read := func(n int, path []string) (int, bool) {
		items := make([]int, n)
		for i := range items {
			items[i] = i
		}
		for j, k := range path {
			if start, end, ok := reach.Slice(k, len(items)); ok {
				items = items[start:end]
				continue
			}
			if i, ok := reach.Index(k); ok && i < len(items) {
				return items[i], j == len(path)-1
			}
			return 0, false
		}
		return 0, false
	}
	var paths [][]string
	for _, a := range keys {
		paths = append(paths, []string{a})
		for _, b := range keys {
			paths = append(paths, []string{a, b})
			for _, c := range keys {
				paths = append(paths, []string{a, b, c})
			}
		}
	}
	const longest = 6
	for _, p := range paths {
		r := slices.Concat([]string{"rows"}, p, []string{"x"})
		exact := !slices.ContainsFunc(p, fromEnd)
		var reached [longest]bool
		for i := range longest {
			reached[i] = reaches(t, r, []string{"rows", strconv.Itoa(i), "x"})
		}
		for n := range longest + 1 {
			item, ok := read(n, p)
			for i, got := range reached[:n] {
				switch {
				case ok && i == item && !got:
					t.Fatalf("%s reads item %d of %d, but doesn't reach it", strings.Join(r, "."), i, n)
				case exact && got && (!ok || i != item):
					t.Fatalf("%s reaches item %d of %d, which it doesn't read", strings.Join(r, "."), i, n)
				}
			}
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
