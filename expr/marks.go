package expr

import (
	"slices"

	"github.com/parallelworks/meridian/internal/reach"
)

// Marks are the paths of the values in a scope's contexts that are marked
// (Scope.Marked), such as the secrets a runner masks. Each is a context's
// name, vars rather than var, then the names that lead to the value from
// it, a list's items by their indexes, as in [vars token]. A reference
// reads a marked value however it spells the keys that lead to it, as a
// slice or a * does a list's items.
//
// Paths are only ever added to Marks, so that the evaluations that share a
// budget (Scope.Budgeted), which index them, find each, those added since
// they began too. A copy of Marks has paths of its own: one added to it
// isn't added to the Marks it was copied from, nor the other way round.
// The zero Marks marks nothing.
type Marks struct {
	// paths are copies of the paths, and owner is the Marks that last
	// added one: a copy adds its first where nothing else reads.
	paths [][]string
	owner *Marks
}

// NewMarks returns Marks that mark the values at paths.
func NewMarks(paths ...[]string) *Marks {
	m := &Marks{}
	m.Add(paths...)
	return m
}

// Add marks the values at paths. It keeps copies of them, so the caller
// can change those it gives.
func (m *Marks) Add(paths ...[]string) {
	if m.owner != m {
		// m's paths can share their storage with those of the Marks it
		// was copied from, which appends to it.
		m.paths, m.owner = slices.Clip(m.paths), m
	}
	for _, p := range paths {
		m.paths = append(m.paths, slices.Clone(p))
	}
}

// scanChecks is how many references a group checks against the paths its
// scope marks by comparing each with every path, before it indexes them:
// comparing costs a group that checks a few references less than indexing,
// and indexing costs one that checks many, as all of a run's input
// defaults are, less than comparing.
const scanChecks = 32

// A markIndex is what a group knows of the paths its scope marks
// (Scope.Marked): how many references it has checked against them, and
// once it has checked scanChecks, the paths by their contexts, so that a
// reference finds the marked values it reaches without comparing its path
// with each. It holds the paths added to the Marks since it was made too.
type markIndex struct {
	// of is the Marks it knows, and n how many paths they had when a
	// reference was last checked.
	of       *Marks
	n        int
	checks   int
	contexts map[string]*reach.Trie[struct{}]
}

// update makes x what the group knows of m: it adds the paths added to m
// since, or starts anew if x is of other Marks, or of other paths at m, as
// Marks copied over m's would be.
func (x *markIndex) update(m *Marks) {
	if x.of != m || x.n > len(m.paths) {
		*x = markIndex{of: m}
	}
	if x.contexts != nil {
		x.add(m.paths[x.n:])
	}
	x.n = len(m.paths)
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

// marks reports whether what a reference reads by following path in
// context is marked, or holds a marked value, or is part of one: whether
// path can reach one of the values the scope marks, however its keys are
// written (reach.Patterns).
func (e *evaluator) marks(context string, path []string) bool {
	m := e.scope.Marked
	if m == nil || len(m.paths) == 0 {
		return false
	}
	x := &e.scope.budget.marks
	x.update(m)
	if x.contexts == nil {
		if x.checks++; x.checks <= scanChecks {
			for p := range reach.Patterns(path) {
				for _, marked := range m.paths {
					if len(marked) > 0 && marked[0] == context && reach.Reaches(p, marked[1:]) {
						return true
					}
				}
			}
			return false
		}
		x.contexts = map[string]*reach.Trie[struct{}]{}
		x.add(m.paths)
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
