package compile

import (
	"cmp"
	"iter"
	"slices"

	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/reach"
)

// A laterPart is a part of an input's value, at path in it, that is
// evaluated once the run is underway: what the workflow writes at loc,
// evaluated for the list rows at index. variable is set for a part that
// reads a variable or a password, whose value the compiler knows, and
// keeps in the values, though the plan doesn't hold it.
type laterPart struct {
	path     []string
	variable bool
	loc      []string
	index    []int
}

// evaluatedLater reports whether f's value is evaluated as the run starts,
// as a part of its input's value is, or is in one; and if so, whether the
// compiler knows it all the same, as it does a part that reads only
// variables and passwords.
func (f *field) evaluatedLater() (later, known bool) {
	in := f.holder()
	at := f.path[len(in.path):]
	known = true
	for _, p := range in.partsAround(at) {
		if len(p.path) <= len(at) {
			later, known = true, known && p.variable
		}
	}
	return later, later && known
}

// addLater adds p to the parts of f's value that are evaluated once the run
// is underway.
func (f *field) addLater(p laterPart) {
	if f.laterAt == nil {
		f.laterAt = &reach.Trie[int]{}
	}
	f.laterAt.Add(p.path, len(f.later))
	f.later = append(f.later, p)
}

// partsAround returns the parts of f's value evaluated once the run is
// underway that are at at, a path in the value, on the way to it, or under
// it, in the order they were added.
func (f *field) partsAround(at []string) iter.Seq2[int, laterPart] {
	return func(yield func(int, laterPart) bool) {
		if len(at) == 0 {
			for i, p := range f.later {
				if !yield(i, p) {
					return
				}
			}
			return
		}
		if f.laterAt == nil {
			return
		}
		// Reached reads a * in at as any key, so it can find more than
		// these, which related leaves out.
		found := slices.Sorted(f.laterAt.Reached(at))
		for _, i := range slices.Compact(found) {
			if p := f.later[i]; related(p.path, at) && !yield(i, p) {
				return
			}
		}
	}
}

// related reports whether one of two paths leads to the other, or they are
// the same.
func related(a, b []string) bool {
	n := min(len(a), len(b))
	return slices.Equal(a[:n], b[:n])
}

// startsLater reports whether the value at path, f's or a part of it, is
// in a part of its input's value that is evaluated as the run starts, or is
// one: any such part, or with known unset, one the compiler doesn't know
// now, whose value is the text of what is evaluated then.
func (f *field) startsLater(path []string, known bool) bool {
	in := f.holder()
	at := path[len(in.path):]
	for _, p := range in.partsAround(at) {
		if len(p.path) <= len(at) && (known || !p.variable) {
			return true
		}
	}
	return false
}

// deferPart records that the part of f's value at path, raw, which the
// workflow writes at loc, which mustn't change, is evaluated once the run
// is underway, as Start resolves the inputs again: f's input's value isn't
// in the plan, all of it. A part that reads a variable keeps its value
// until then, and what reads it reads a variable too. A part that is
// evaluated as the run starts already stays one, and isn't known now if
// either says it isn't.
func (r *inputResolver) deferPart(f *field, path, loc []string, raw any, variable bool) {
	in := f.holder()
	at := slices.Concat(f.path[len(in.path):], path)
	defer r.refresh(slices.Concat(f.path, path))
	for i, p := range in.partsAround(at) {
		if slices.Equal(p.path, at) {
			in.later[i].variable = p.variable && variable
			return
		}
	}
	in.addLater(laterPart{path: at, variable: variable, loc: loc, index: f.index})
	if variable {
		r.mark(slices.Concat(f.path, path))
	}
}

// holdLater decides, of each input with parts evaluated once the run is
// underway, whether it is deferred, as a part of it isn't known, or its
// parts read only variables and passwords, which the compiler knows, but
// the plan doesn't hold. A part that gives a list's rows, or the value of
// a list, a group or a wizard step, with a password field among its
// inputs, is a secret as a whole.
func (r *inputResolver) holdLater() {
	if !slices.ContainsFunc(r.fields, func(f *field) bool { return f.later != nil }) {
		return
	}
	// The lists, groups and wizard steps with password fields among their
	// inputs, by their paths.
	holders := map[string]*field{}
	for _, fields := range [][]*field{r.fields, r.liveRows()} {
		for _, f := range fields {
			if f.typ != "password" && holdsPassword(f.def) {
				holders[jsonptr.Format(f.path)] = f
			}
		}
	}
	for _, f := range r.fields {
		if f.later == nil {
			continue
		}
		for _, p := range f.later {
			at := slices.Concat(f.path, p.path)
			h := holders[jsonptr.Format(at)]
			if n := len(p.path); h == nil && n > 0 && isIndex(p.path[n-1]) {
				h = holders[jsonptr.Format(at[:len(at)-1])]
			}
			if h != nil {
				h.secretWhole = true
			}
		}
		if slices.ContainsFunc(f.later, func(p laterPart) bool { return !p.variable }) {
			// A part of its value isn't known until the run starts, so
			// what expressions read of it is unknown, all of it, from
			// here on.
			f.deferred = true
			r.refresh(f.path)
		} else {
			f.variable = true
		}
	}
}

// holdWhole evaluates as the run starts what an evaluation made from an
// input that is evaluated as the run starts because a part of it reads what
// isn't known yet: an input's value stays whole, so what is made from any
// part of it, the parts the compiler knows too, isn't known yet either, as
// a condition that reads it isn't. The value of an input whose evaluation
// read such an input is then evaluated as the run starts as well, and so
// on. Evaluations made from the input's own parts, as a row's default that
// reads another field of its row, read the parts as they are.
func (r *inputResolver) holdWhole() {
	var held []*field
	seen := map[*field]bool{}
	for _, f := range r.fields {
		if slices.ContainsFunc(f.later, func(p laterPart) bool { return !p.variable }) {
			held = append(held, f)
			seen[f] = true
		}
	}
	for len(held) > 0 {
		h := held[0]
		held = held[1:]
		var found []*evaluation
		for e := range r.evaluated.Reaching(h.path) {
			if !e.done && e.f.holder() != h {
				e.done = true
				found = append(found, e)
			}
		}
		// The trie finds them in no particular order.
		slices.SortFunc(found, func(a, b *evaluation) int {
			return cmp.Or(cmp.Compare(a.f.seq, b.f.seq), slices.Compare(a.path, b.path))
		})
		for _, e := range found {
			r.deferPart(e.f, e.path, e.loc, e.raw, false)
			if in := e.f.holder(); !seen[in] {
				seen[in] = true
				held = append(held, in)
			}
		}
	}
}
