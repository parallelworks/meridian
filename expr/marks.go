package expr

import "github.com/parallelworks/meridian/internal/reach"

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
// with each. It holds the paths appended to Marked since it was made too.
type markIndex struct {
	// first is Marked's first path, and n how many paths it had when a
	// reference was last checked.
	first    *[]string
	n        int
	checks   int
	contexts map[string]*reach.Trie[struct{}]
}

// update makes x what the group knows of marked, which isn't empty: it adds
// the paths appended to marked since, or starts anew if marked isn't what
// x is of.
func (x *markIndex) update(marked [][]string) {
	if x.first != &marked[0] || x.n > len(marked) {
		*x = markIndex{first: &marked[0]}
	}
	if x.contexts != nil {
		x.add(marked[x.n:])
	}
	x.n = len(marked)
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
	marked := e.scope.Marked
	if len(marked) == 0 {
		return false
	}
	x := &e.scope.budget.marks
	x.update(marked)
	if x.contexts == nil {
		if x.checks++; x.checks <= scanChecks {
			for p := range reach.Patterns(path) {
				for _, m := range marked {
					if len(m) > 0 && m[0] == context && reach.Reaches(p, m[1:]) {
						return true
					}
				}
			}
			return false
		}
		x.contexts = map[string]*reach.Trie[struct{}]{}
		x.add(marked)
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
