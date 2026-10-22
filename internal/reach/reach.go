// Package reach says which values a reference can reach by following its
// path, such as inputs.rows[0:1] or inputs.rows.*.x, by the paths that lead
// to them: a mapping's keys, and a list's indexes as strconv.Itoa writes
// them. A reference reaches a value it reads, one that what it reads holds,
// and one it reads a part of.
//
// A path can reach a value by other spellings of its keys than its own: 00,
// +0 and -0 read a list's first item, a slice such as 0:1 reads a list's
// items, and * reads each item of a list or a mapping. What a path reaches
// is found without the values, so a key that reads a list one way and a
// mapping another is taken to read both.
package reach

import (
	"iter"
	"slices"
	"strconv"
	"strings"
)

// Index reads key as the index of a list's item, as a path does: a whole
// number, such as 0, 00, +0 or -0, reads the item at that index if the list
// has one. ok is false if key isn't one, or is negative, as no item's is, or
// is too large for an int, as no list is that long. Which keys are whole
// numbers is the same on every platform: those that fit in 64 bits.
func Index(key string) (i int, ok bool) {
	n, ok := index(key)
	if !ok || int64(int(n)) != n {
		return 0, false
	}
	return int(n), true
}

// index reads key as the index of a list's item, as Index does, in 64 bits
// on every platform.
func index(key string) (int64, bool) {
	i, ok := atoi(key)
	return i, ok && i >= 0
}

// atoi reads s as a whole number of 64 bits, as strconv.ParseInt does,
// without making an error for what isn't one, as most keys aren't.
func atoi(s string) (int64, bool) {
	digits := s
	if digits != "" && (digits[0] == '+' || digits[0] == '-') {
		digits = digits[1:]
	}
	if digits == "" {
		return 0, false
	}
	for i := range len(digits) {
		if digits[i] < '0' || digits[i] > '9' {
			return 0, false
		}
	}
	i, err := strconv.ParseInt(s, 10, 64)
	return i, err == nil
}

// Slice reads key as a slice of something n long, a list or a string, as a
// path does: 1:3, :2 or -2:. A negative bound counts from the end, and the
// bounds are clamped to [0, n]. ok is false if key isn't a slice. A bound
// is a whole number of 64 bits on every platform, so a slice such as
// 3000000000: reads the same where an int is 32 bits.
func Slice(key string, n int) (start, end int, ok bool) {
	from, to, found := strings.Cut(key, ":")
	if !found {
		return 0, 0, false
	}
	s, e := int64(0), int64(n)
	if from != "" {
		if s, ok = atoi(from); !ok {
			return 0, 0, false
		}
	}
	if to != "" {
		if e, ok = atoi(to); !ok {
			return 0, 0, false
		}
	}
	clamp := func(i int64) int {
		if i < 0 {
			i += int64(n)
		}
		return int(min(max(i, 0), int64(n)))
	}
	start, end = clamp(s), clamp(e)
	return start, max(start, end), true
}

// isSlice reports whether key is a slice.
func isSlice(key string) bool {
	_, _, ok := Slice(key, 0)
	return ok
}

// forks reports whether key reads a list and a mapping differently, so that
// what follows it in a path goes two ways: a slice, or an index written
// otherwise than strconv.Itoa writes it.
func forks(key string) bool {
	if strings.IndexByte(key, ':') >= 0 {
		return isSlice(key)
	}
	_, ok := index(key)
	return ok && !canonical(key)
}

// canonical reports whether key, an index, is written as strconv.Itoa
// writes it: without a sign, or a 0 before its other digits.
func canonical(key string) bool {
	return key[0] != '+' && key[0] != '-' && (len(key) == 1 || key[0] != '0')
}

// maxForks is how many keys of a path can each go two ways (forks) before
// Patterns stops following them apart. A path with more reaches all under
// where the first of them is read, which keeps the patterns of a path few:
// at most 2^maxForks.
const maxForks = 4

// Patterns returns the patterns of read, a path that a reference follows:
// paths of keys, as a mapping's keys and a list's indexes are written, and
// *, which is any key, that between them lead to each value read can reach.
// Trie.Reached says what a pattern reaches.
//
// A key that is a whole number reads a list's item by its index however it
// is written, and a mapping's key as it is written. A slice reads a list's
// items, and the key after it reads the slice, as it would the list: one
// that is a whole number reads one of the items, and * each of them. Which
// item is known if the slice's bounds count from the list's start, as 1:
// and 0:2 do, and else, as with -2:, it is any of them. A slice of a
// mapping is the key it is written as, such as 0:1. A path with more than
// maxForks keys that each go two ways reaches all under where the first of
// them is read.
//
// The patterns are new, or read itself, and the caller can keep them.
func Patterns(read []string) iter.Seq[[]string] {
	return func(yield func([]string) bool) {
		n, first := 0, -1
		for i, k := range read {
			if forks(k) {
				if first < 0 {
					first = i
				}
				n++
			}
		}
		switch {
		case first < 0:
			yield(read)
		case n > maxForks:
			yield(slices.Clone(read[:first]))
		default:
			expand(make([]string, 0, len(read)), read, nil, yield)
		}
	}
}

// A window is where a slice's items are in the list it was made from, as
// far as its bounds tell without the list: from start, and before end, or
// to the list's end if end is -1. exact is false if a bound counts from
// the list's end, which only its length tells, and then where they are
// isn't known. Indexes are of 64 bits, as Slice reads bounds.
type window struct {
	start, end int64
	exact      bool
}

// whole is the window of a list's items: all of them.
var whole = window{0, -1, true}

// slice returns the window of what key, a slice, reads of w's items.
func (w window) slice(key string) window {
	from, to, _ := strings.Cut(key, ":")
	start, end := int64(0), int64(-1)
	if from != "" {
		start, _ = atoi(from)
	}
	if to != "" {
		end, _ = atoi(to)
	}
	if start < 0 || to != "" && end < 0 {
		return window{}
	}
	s, known := w.at(start)
	if !known {
		return window{}
	}
	e := w.end
	if end >= 0 {
		if e, known = w.at(end); !known {
			return window{}
		}
		if w.end >= 0 {
			e = min(e, w.end)
		}
	}
	if e >= 0 {
		e = max(e, s)
	}
	return window{s, e, true}
}

// at returns the index in the list of the item at i, which isn't negative,
// in w. known is false if w isn't exact, or the index is too large for 64
// bits.
func (w window) at(i int64) (j int64, known bool) {
	j = w.start + i
	return j, w.exact && j >= w.start
}

// item returns the key of the list's item that the index i reads of w: its
// index, or * if which one isn't known. ok is false if w has no item at i.
func (w window) item(i int64) (key string, ok bool) {
	j, known := w.at(i)
	switch {
	case !known:
		return "*", true
	case w.end >= 0 && j >= w.end:
		return "", false
	}
	return strconv.FormatInt(j, 10), true
}

// expand yields the patterns of the path rest, from where prefix leads:
// the items of a list in window w, if it isn't nil, that a slice made. It
// reports false if yield does, to stop.
func expand(prefix, rest []string, w *window, yield func([]string) bool) bool {
	for j, k := range rest {
		i, isIndex := index(k)
		switch {
		case k == "*":
			prefix, w = append(prefix, k), nil
		case isSlice(k):
			// A slice of a mapping is a key, and a slice of a slice is a
			// slice of the list.
			of := whole
			if w == nil {
				if !expand(slices.Concat(prefix, []string{k}), rest[j+1:], nil, yield) {
					return false
				}
			} else {
				of = *w
			}
			sliced := of.slice(k)
			w = &sliced
		case w != nil:
			// An index of a slice reads one of the list's items, if it has
			// one there. A slice has no other keys, so this way the path
			// reaches no further: the list it read on the way here, the way
			// that reads the slice as a mapping's key reaches as well.
			if !isIndex {
				return true
			}
			item, ok := w.item(i)
			if !ok {
				return true
			}
			prefix, w = append(prefix, item), nil
		case isIndex && !canonical(k):
			if !expand(slices.Concat(prefix, []string{k}), rest[j+1:], nil, yield) {
				return false
			}
			prefix = append(prefix, strconv.FormatInt(i, 10))
		default:
			prefix = append(prefix, k)
		}
	}
	return yield(prefix)
}

// Reaches reports whether pattern reaches the value at path: whether it
// leads to path, and reads through it, or path leads to where it does, and
// what it reads holds the value. A * in pattern is any key. Trie.Reached
// finds the values a pattern reaches of many.
func Reaches(pattern, path []string) bool {
	for i := range min(len(pattern), len(path)) {
		if pattern[i] != "*" && pattern[i] != path[i] {
			return false
		}
	}
	return true
}

// A Trie holds values by paths, to find those whose paths overlap a path
// without comparing it with each: the paths of values, which Reached
// finds those a pattern reaches of, or the patterns of paths that read
// values (Patterns), which Reaching finds those that reach a path of.
// The zero Trie is empty and ready to use.
type Trie[T comparable] struct {
	values   []T
	children map[string]*Trie[T]
}

// Add adds v at path.
func (t *Trie[T]) Add(path []string, v T) {
	for _, k := range path {
		c, ok := t.children[k]
		if !ok {
			if t.children == nil {
				t.children = map[string]*Trie[T]{}
			}
			c = &Trie[T]{}
			t.children[k] = c
		}
		t = c
	}
	t.values = append(t.values, v)
}

// Remove removes v from path.
func (t *Trie[T]) Remove(path []string, v T) {
	for _, k := range path {
		if t = t.children[k]; t == nil {
			return
		}
	}
	t.values = slices.DeleteFunc(t.values, func(w T) bool { return w == v })
}

// Reached returns the values of a trie that holds them by the paths of
// values that pattern reaches: those on the way to where it leads, which it
// reads through, and those there and under it, which what it reads holds.
// A * in pattern is any key. A value held at more than one path can be
// returned more than once.
func (t *Trie[T]) Reached(pattern []string) iter.Seq[T] {
	return func(yield func(T) bool) { t.reached(pattern, yield) }
}

func (t *Trie[T]) reached(pattern []string, yield func(T) bool) bool {
	for i, k := range pattern {
		if !t.yieldValues(yield) {
			return false
		}
		if k == "*" {
			for _, c := range t.children {
				if !c.reached(pattern[i+1:], yield) {
					return false
				}
			}
			return true
		}
		if t = t.children[k]; t == nil {
			return true
		}
	}
	return t.each(yield)
}

// Reaching returns the values of a trie that holds them by patterns of
// paths (Patterns) that reach path, the path of a value: those on the way
// to path, which read what holds it, and those there and under it, which
// read it or through it. A * in the trie's patterns is any key. A value
// held at more than one pattern can be returned more than once.
func (t *Trie[T]) Reaching(path []string) iter.Seq[T] {
	return func(yield func(T) bool) { t.reaching(path, yield) }
}

func (t *Trie[T]) reaching(path []string, yield func(T) bool) bool {
	for i, k := range path {
		if !t.yieldValues(yield) {
			return false
		}
		if c := t.children["*"]; c != nil && k != "*" && !c.reaching(path[i+1:], yield) {
			return false
		}
		if t = t.children[k]; t == nil {
			return true
		}
	}
	return t.each(yield)
}

// yieldValues yields the values at t's path.
func (t *Trie[T]) yieldValues(yield func(T) bool) bool {
	for _, v := range t.values {
		if !yield(v) {
			return false
		}
	}
	return true
}

// each yields the values at t's path and under it.
func (t *Trie[T]) each(yield func(T) bool) bool {
	if !t.yieldValues(yield) {
		return false
	}
	for _, c := range t.children {
		if !c.each(yield) {
			return false
		}
	}
	return true
}
