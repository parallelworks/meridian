package compile

import (
	"cmp"
	"container/heap"
	"iter"
	"slices"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/reach"
)

// pendingFields are the fields whose values wait for expressions to be
// evaluated, and the order they are filled in: each once none of the
// values its expressions read waits any longer.
//
// The order is that of passes over the fields that wait, in the order they
// started waiting: a pass fills in each field that reads none that still
// waits when the pass reaches it, and a field that starts waiting during a
// pass, as a list's rows do once its value is known, waits at the end of
// it. pendingFields finds that order without making the passes: each field
// counts the fields it reads that wait, and knows the earliest pass that
// can fill it in.
type pendingFields struct {
	// paths holds the fields that wait by their paths in the values, and
	// reads by the patterns of each path that their expressions read: a
	// set of them at each, which pathSets and readSets hold by the path.
	paths, reads       reach.Trie[*fieldSet]
	pathSets, readSets map[string]*fieldSet
	all                []*pendingField
	ready              readyFields
	// pass is the pass that the field being filled in is in.
	pass int
	// stamp marks the fields found while a field is added, so that each
	// is counted once.
	stamp int
}

// A pendingField is a field that waits: for its value to be filled in, or
// with candidate set, for that part of its submitted value to be compared
// with what an expression gives (inputResolver.compare). A field can wait
// for both, and for each of its candidates.
type pendingField struct {
	f         *field
	candidate *candidate
	// reads are the patterns of the paths in the values that f's
	// expressions read (reach.Patterns).
	reads [][]string
	// pos is its place among the fields that wait, and pass the earliest
	// pass that can fill it in.
	pos, pass int
	// waits counts the fields it reads that wait, and readers are the
	// fields that wait for it.
	waits   int
	readers []*pendingField
	done    bool
	stamp   int
	// sets are the sets it is in, at its path and at what it reads.
	sets []*fieldSet
}

// A fieldSet is the fields that wait at one path, or read one pattern. A
// field leaves the sets it is in once it is filled in: it is skipped, and
// dropped from a set once half of the set is, so that a field leaves a set
// that thousands of others are in, as a pattern many rows read is, without
// going through them.
type fieldSet struct {
	fields []*pendingField
	done   int
}

// addAt adds p to the set at path in t, which sets holds by their paths,
// making the set if there is none.
func addAt(t *reach.Trie[*fieldSet], sets *map[string]*fieldSet, path []string, p *pendingField) {
	key := jsonptr.Format(path)
	s := (*sets)[key]
	if s == nil {
		if *sets == nil {
			*sets = map[string]*fieldSet{}
		}
		s = &fieldSet{}
		(*sets)[key] = s
		t.Add(path, s)
	}
	s.fields = append(s.fields, p)
	p.sets = append(p.sets, s)
}

// readPatterns returns the patterns (reach.Patterns) of the paths in the
// inputs' values that exprs read, for the list rows at index.
func readPatterns(exprs []*expr.Expr, index []int) [][]string {
	var out [][]string
	for _, e := range exprs {
		for _, ref := range e.Refs() {
			if ref.Context == "inputs" {
				out = slices.AppendSeq(out, reach.Patterns(indexPath(ref.Path, index)))
			}
		}
	}
	return out
}

// leave notes that p is done, and so leaves each set it is in.
func (p *pendingField) leave() {
	for _, s := range p.sets {
		if s.done++; s.done*2 > len(s.fields) {
			s.fields = slices.DeleteFunc(s.fields, func(f *pendingField) bool { return f.done })
			s.done = 0
		}
	}
}

// waitingIn returns the fields that wait in the sets of seq.
func waitingIn(seq iter.Seq[*fieldSet]) iter.Seq[*pendingField] {
	return func(yield func(*pendingField) bool) {
		for s := range seq {
			for _, p := range s.fields {
				if !p.done && !yield(p) {
					return
				}
			}
		}
	}
}

// add adds f, which waits for the values reads read, after the others: to
// be filled in, or with c set, for the candidate c of its value to be
// compared. The fields that read the value at path, f's or the candidate's,
// wait for it in turn.
func (q *pendingFields) add(f *field, path []string, reads []*expr.Expr, c *candidate) {
	p := &pendingField{f: f, candidate: c, pos: len(q.all), pass: q.pass, reads: readPatterns(reads, f.index)}
	q.all = append(q.all, p)
	// f waits for the fields it reads, and the fields that read f wait for
	// it.
	q.stamp++
	for _, pattern := range p.reads {
		for w := range waitingIn(q.paths.Reached(pattern)) {
			if w.stamp != q.stamp {
				w.stamp = q.stamp
				w.readers = append(w.readers, p)
				p.waits++
			}
		}
	}
	q.stamp++
	for r := range waitingIn(q.reads.Reaching(path)) {
		if r.stamp != q.stamp {
			r.stamp = q.stamp
			p.readers = append(p.readers, r)
			r.waits++
		}
	}
	addAt(&q.paths, &q.pathSets, path, p)
	for _, pattern := range p.reads {
		addAt(&q.reads, &q.readSets, pattern, p)
	}
	if p.waits == 0 {
		heap.Push(&q.ready, readyField{p, p.pass})
	}
}

// next returns the next field to fill in, which no longer waits, or nil if
// none can be: none waits, or each reads another that waits. The fields
// that read it wait for it until filled.
func (q *pendingFields) next() *pendingField {
	for q.ready.Len() > 0 {
		r := heap.Pop(&q.ready).(readyField)
		p := r.p
		// The heap can hold a field from before it waited again for one
		// added since, or before it became ready for a later pass.
		if p.done || p.waits > 0 || r.pass != p.pass {
			continue
		}
		p.done = true
		p.leave()
		q.pass = p.pass
		return p
	}
	return nil
}

// filled notes that p's field is filled in, once the fields that filling
// it in added, as its rows, are added: the fields that read it wait for it
// no longer.
func (q *pendingFields) filled(p *pendingField) {
	for _, r := range p.readers {
		// A field that waits before p in a pass is filled in the pass
		// after p's.
		pass := p.pass
		if p.pos > r.pos {
			pass++
		}
		r.pass = max(r.pass, pass)
		if r.waits--; r.waits == 0 {
			heap.Push(&q.ready, readyField{r, r.pass})
		}
	}
}

// waiting returns the fields that still wait to be filled in, in the order
// they started to.
func (q *pendingFields) waiting() []*field {
	var out []*field
	for _, p := range q.all {
		if !p.done && p.candidate == nil {
			out = append(out, p.f)
		}
	}
	return out
}

// release is for when no field can be filled in, and some still wait: each
// waits for a field that waits for it, however indirectly. It returns, as
// done, the first comparison of each such cycle of fields that has one, to
// be compared with the values as they are: a comparison changes no value,
// only what reads a variable or a password, which those that wait for it
// in the cycle can't know of it. A form can't evaluate defaults that read
// each other either. It returns none if no cycle has a comparison, as for
// defaults that read each other.
func (q *pendingFields) release() []*pendingField {
	var out []*pendingField
	for _, cycle := range q.cycles() {
		first := -1
		for i, p := range cycle {
			if p.candidate != nil && (first < 0 || p.pos < cycle[first].pos) {
				first = i
			}
		}
		if first >= 0 {
			p := cycle[first]
			p.done, p.pass = true, q.pass
			p.leave()
			out = append(out, p)
		}
	}
	slices.SortFunc(out, func(a, b *pendingField) int { return cmp.Compare(a.pos, b.pos) })
	return out
}

// cycles returns the cycles of fields that wait for each other, however
// indirectly: the strongly connected components, of more than one field,
// of the fields that still wait, each a field's readers waiting for it.
// It finds them as Tarjan's algorithm does, without recursion.
func (q *pendingFields) cycles() [][]*pendingField {
	type node struct {
		index, low int
		onStack    bool
	}
	nodes := map[*pendingField]*node{}
	var out [][]*pendingField
	// path holds the fields walked that aren't yet in a component.
	var path []*pendingField
	// frame is where the walk is in one field's readers.
	type frame struct {
		p    *pendingField
		next int
	}
	index := 0
	for _, root := range q.all {
		if root.done || nodes[root] != nil {
			continue
		}
		walk := []frame{{p: root}}
		nodes[root] = &node{index: index, low: index, onStack: true}
		index++
		path = append(path, root)
		for len(walk) > 0 {
			top := &walk[len(walk)-1]
			n := nodes[top.p]
			if top.next < len(top.p.readers) {
				r := top.p.readers[top.next]
				top.next++
				if r.done {
					continue
				}
				switch m := nodes[r]; {
				case m == nil:
					nodes[r] = &node{index: index, low: index, onStack: true}
					index++
					path = append(path, r)
					walk = append(walk, frame{p: r})
				case m.onStack:
					n.low = min(n.low, m.index)
				}
				continue
			}
			walk = walk[:len(walk)-1]
			if len(walk) > 0 {
				parent := nodes[walk[len(walk)-1].p]
				parent.low = min(parent.low, n.low)
			}
			if n.low != n.index {
				continue
			}
			i := len(path) - 1
			for path[i] != top.p {
				i--
			}
			component := slices.Clone(path[i:])
			path = path[:i]
			for _, p := range component {
				nodes[p].onStack = false
			}
			if len(component) > 1 {
				out = append(out, component)
			}
		}
	}
	return out
}

// readyFields are the fields that wait for no other, in the order they are
// filled in: by pass, and in a pass in the order they started waiting.
type readyFields []readyField

// A readyField is a field that waits for no other as of pass.
type readyField struct {
	p    *pendingField
	pass int
}

func (h readyFields) Len() int { return len(h) }

func (h readyFields) Less(i, j int) bool {
	return cmp.Or(cmp.Compare(h[i].pass, h[j].pass), cmp.Compare(h[i].p.pos, h[j].p.pos)) < 0
}

func (h readyFields) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *readyFields) Push(x any) { *h = append(*h, x.(readyField)) }

func (h *readyFields) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}
