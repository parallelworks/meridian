package expr

import (
	"iter"
	"slices"

	"github.com/parallelworks/meridian/internal/reach"
)

// Marks are the paths of the values in a scope's contexts that are marked
// (Scope.Marked and Scope.Private), such as the secrets a runner masks.
// Each is a context's name, vars rather than var, then the names that lead
// to the value from it, a list's items by their indexes, as in [vars
// token]. A reference reads a marked value however it spells the keys that
// lead to it, as a slice or a * does a list's items. The zero Marks mark
// nothing.
//
// Marks are a value, as a slice is: a copy holds the paths it was copied
// with and those added to it since, and none added to the Marks it was
// copied from. So a scope holds the paths its Marked and Private held when
// they were set, and not those added to the Marks they were set from
// after. The paths are kept where they never change, so that the
// evaluations that share a budget (Scope.Budgeted), which index them, find
// each path their scope's Marks hold, however Marks are copied, added to or
// assigned.
//
// Marks aren't safe for concurrent use: Add mustn't run while another
// goroutine reads the same Marks, or a copy of them, as an evaluation does.
type Marks struct {
	// s holds the paths, and these Marks are its first n.
	s *markStore
	n int
}

// A markStore holds the paths of Marks. Add only appends to it, so the
// first n paths of a store never change: Marks, and an index of them, of
// a store's first n paths stay what they are.
type markStore struct {
	paths [][]string
}

// NewMarks returns Marks that mark the values at paths.
func NewMarks(paths ...[]string) Marks {
	var m Marks
	m.Add(paths...)
	return m
}

// Add marks the values at paths. It keeps copies of them, so the caller
// can change those it gives.
func (m *Marks) Add(paths ...[]string) {
	if len(paths) == 0 {
		return
	}
	if m.s == nil || m.n != len(m.s.paths) {
		// Other Marks hold the paths of m's store past m's own, so m's go
		// in a store of their own.
		m.s = &markStore{paths: slices.Clone(m.paths())}
	}
	for _, p := range paths {
		m.s.paths = append(m.s.paths, slices.Clone(p))
	}
	m.n = len(m.s.paths)
}

// paths returns the paths m holds, which mustn't be changed.
func (m Marks) paths() [][]string {
	if m.s == nil {
		return nil
	}
	return m.s.paths[:m.n:m.n]
}

// All returns the paths m holds, in the order they were added. Each is a
// copy, which the caller can change.
func (m Marks) All() iter.Seq[[]string] {
	return func(yield func([]string) bool) {
		for _, p := range m.paths() {
			if !yield(slices.Clone(p)) {
				return
			}
		}
	}
}

// scanChecks is how many references a group checks against the paths its
// scope marks by comparing each with every path, before it indexes them:
// comparing costs a group that checks a few references less than indexing,
// and indexing costs one that checks many, as all of a run's input
// defaults are, less than comparing.
const scanChecks = 32

// A markIndex is what a group knows of the paths one of its scope's mark
// sets marks (Scope.Marked or Scope.Private): how many references it has
// checked against them, and once it has checked scanChecks, the paths by
// their contexts, so that a reference finds the marked values it reaches
// without comparing its path with each. It holds the paths added to the
// Marks since it was made too.
type markIndex struct {
	// of is the store of the Marks it knows, and n how many of its paths
	// they held when a reference was last checked.
	of       *markStore
	n        int
	checks   int
	contexts map[string]*reach.Trie[struct{}]
}

// update makes x what the group knows of m: it adds m's paths past those x
// knows, or starts anew if m's are in another store, or are fewer, as those
// of Marks copied before x's were added to are.
func (x *markIndex) update(m Marks) {
	if x.of != m.s || x.n > m.n {
		*x = markIndex{of: m.s}
	}
	if x.contexts != nil {
		x.add(m.s.paths[x.n:m.n])
	}
	x.n = m.n
}

// add indexes paths.
func (x *markIndex) add(paths [][]string) {
	for _, m := range paths {
		if len(m) == 0 {
			continue
		}
		t := x.contexts[m[0]]
		if t == nil {
			t = &reach.Trie[struct{}]{}
			x.contexts[m[0]] = t
		}
		t.Add(m[1:], struct{}{})
	}
}

// reads notes what a reference that follows path in context reads of the
// values its scope marks, in each of Scope.Marked and Scope.Private.
func (e *evaluator) reads(context string, path []string) {
	b := e.scope.budget
	if !b.read.Marked && e.scope.Marked.n > 0 && b.marks.reaches(e.scope.Marked, context, path) {
		b.read.Marked = true
	}
	if !b.read.Private && e.scope.Private.n > 0 && b.private.reaches(e.scope.Private, context, path) {
		b.read.Private = true
	}
}

// reaches reports whether what a reference reads by following path in
// context is marked by m, or holds a value m marks, or is part of one:
// whether path can reach one of the values m marks, however its keys are
// written (reach.Patterns). x is what the group knows of m.
func (x *markIndex) reaches(m Marks, context string, path []string) bool {
	x.update(m)
	if x.contexts == nil {
		if x.checks++; x.checks <= scanChecks {
			for p := range reach.Patterns(path) {
				for _, marked := range m.paths() {
					if len(marked) > 0 && marked[0] == context && reach.Reaches(p, marked[1:]) {
						return true
					}
				}
			}
			return false
		}
		x.contexts = map[string]*reach.Trie[struct{}]{}
		x.add(m.paths())
	}
	t := x.contexts[context]
	if t == nil {
		return false
	}
	for p := range reach.Patterns(path) {
		for range t.Reached(p) {
			return true
		}
	}
	return false
}
