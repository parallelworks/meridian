package compile

import (
	"cmp"
	"maps"
	"slices"
	"strconv"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/internal/reach"
)

// viewState is what the inputResolver keeps of what expressions read of
// the values, and of what evaluates them.
type viewState struct {
	// byPath holds the inputs' fields by their paths in the values, once
	// the view changes, to find those whose values a change reaches
	// (fieldsAt). Rows are found from their lists.
	byPath *reach.Trie[*field]
	// view is what expressions read of the values (evalValues): made when
	// they are first read, and then changed where the values, or what is
	// known of them, change (refresh). contexts are what evaluations read,
	// the view among them.
	view     map[string]any
	contexts map[string]any
	// evaluators evaluate expressions with the values as they are, one for
	// the rows at each index. They are made again once the values change.
	evaluators map[string]*expr.Evaluator
	// checkView, which tests set, is called each time the view changes, and
	// each time an evaluation is to read it.
	checkView func()
}

// evalAt evaluates what is at loc in the workflow, where it is, for the list
// rows at index, with the values so far: the expressions in a string, and in
// each string of a list or mapping. marked is set if it read a variable,
// or a value that did.
func (r *inputResolver) evalAt(loc []string, index []int) (v any, marked bool, err error) {
	key := indexKey(index)
	ev, ok := r.evaluators[key]
	if !ok {
		scope := r.scope()
		scope.Index = index
		ev = r.workflow.Evaluator(scope)
		if r.evaluators == nil {
			r.evaluators = map[string]*expr.Evaluator{}
		}
		r.evaluators[key] = ev
	}
	return ev.EvalMarked(loc)
}

// indexKey returns the key of the evaluator for the list rows at index.
func indexKey(index []int) string {
	b := make([]byte, 0, 4*len(index))
	for _, i := range index {
		b = strconv.AppendInt(b, int64(i), 10)
		b = append(b, ',')
	}
	return string(b)
}

// scope returns the scope that what the compiler evaluates with the
// inputs' values reads: the values as they are (evalValues), and the marks,
// in the inputs' group, which shares one budget.
func (r *inputResolver) scope() expr.Scope {
	if r.view == nil {
		r.view = r.evalValues()
		r.contexts = compileTimeContexts(r.opts, r.view)
	}
	if r.checkView != nil {
		r.checkView()
	}
	scope := r.budget
	scope.Contexts = r.contexts
	scope.Marked = r.marks
	return scope
}

// set sets the value at path, and remove removes it.
func (r *inputResolver) set(path []string, v any) {
	setIn(r.values, path, v)
	r.refresh(path)
}

func (r *inputResolver) remove(path []string) {
	deletePath(r.values, path)
	r.refresh(path)
}

// refresh notes that what expressions read of the values changed at path,
// or under it: the values, or what is known of them, or which read a
// variable. The evaluations that follow read them anew, and the view is
// made at path what evalValues would make it now: the values there, each
// field's value, where it has none, as evalValues fills it in, and what is
// unknown, there, on the way to path and under it. Nothing else in the view
// changes.
func (r *inputResolver) refresh(path []string) {
	r.evaluators = nil
	if r.view == nil {
		return
	}
	if v, ok := jsonptr.Lookup(r.values, path); ok {
		setIn(r.view, path, jsonvalue.Clone(v))
	} else {
		deletePath(r.view, path)
	}
	// fill holds the fields whose values are at path or under it, which
	// are filled in in the order evalValues fills them in, as of two with
	// the same path the first's value is kept; and holders the inputs on
	// the way to path, at it or under it, with parts evaluated once the run
	// is underway.
	var fill, holders []*field
	for f := range r.fieldsAt().Reached(path) {
		if !related(f.path, path) {
			continue
		}
		if f.later != nil {
			holders = append(holders, f)
		}
		fill = appendUnder(fill, f, path)
	}
	slices.SortFunc(fill, func(a, b *field) int { return cmp.Compare(a.seq, b.seq) })
	for _, f := range fill {
		if v, ok := f.filledWith(); ok {
			if _, has := jsonptr.Lookup(r.view, f.path); !has {
				setIn(r.view, f.path, v)
			}
		}
	}
	slices.SortFunc(holders, func(a, b *field) int { return cmp.Compare(a.seq, b.seq) })
	for _, f := range holders {
		f.markUnknown(r.view, path)
	}
	if r.checkView != nil {
		r.checkView()
	}
}

// fieldsAt returns the inputs' fields by their paths in the values, putting
// them there the first time.
func (r *inputResolver) fieldsAt() *reach.Trie[*field] {
	if r.byPath == nil {
		r.byPath = &reach.Trie[*field]{}
		for _, f := range r.fields {
			r.byPath.Add(f.path, f)
		}
	}
	return r.byPath
}

// appendUnder appends to out f, if its value is at path or under it, and
// the fields of its rows whose values are.
func appendUnder(out []*field, f *field, path []string) []*field {
	if len(f.path) >= len(path) {
		out = append(out, f)
		for _, row := range f.rows {
			for _, c := range row {
				out = appendUnder(out, c, path)
			}
		}
		return out
	}
	// path is in f's value: in one of its rows, if any.
	if i, ok := rowIndex(path[len(f.path)], len(f.rows)); ok {
		for _, c := range f.rows[i] {
			if related(c.path, path) {
				out = appendUnder(out, c, path)
			}
		}
	}
	return out
}

// rowIndex returns the index of the row that key, a key of a path, names
// among n rows, as a row's fields' paths write it.
func rowIndex(key string, n int) (int, bool) {
	i, err := strconv.Atoi(key)
	return i, err == nil && i >= 0 && i < n && strconv.Itoa(i) == key
}

// filledWith returns the value evalValues gives f where it has none, and
// whether it gives one: null, or false for a boolean without a default,
// which is the value it gets, rather than its definition. A container, a
// header and an input evaluated once the run is underway, and the fields of
// its rows, get none; nor do the fields of an ignored input's rows, which
// it doesn't have.
func (f *field) filledWith() (any, bool) {
	if f.container || f.header || f.deferred && !f.ignored || f.in != nil && (f.in.deferred || f.in.ignored) {
		return nil, false
	}
	if f.typ == "boolean" && !f.hasDefault {
		return false, true
	}
	return nil, true
}

// markUnknown marks in values, as evalValues does, what is evaluated of
// f's value once the run is underway, at path, on the way to it or under
// it, or everywhere if path is nil: all of it, if f is deferred, or else
// the parts that don't read only variables and passwords, whose values the
// compiler knows.
func (f *field) markUnknown(values map[string]any, path []string) {
	unknown := expr.Unknown{Name: "inputs"}
	switch {
	case f.ignored:
	case f.deferred:
		setIn(values, f.path, unknown)
	default:
		var at []string
		if len(path) > len(f.path) {
			at = path[len(f.path):]
		}
		for _, p := range f.partsAround(at) {
			if !p.variable {
				setIn(values, slices.Concat(f.path, p.path), unknown)
			}
		}
	}
}

// evalValues returns the values for expressions to read: each input with no
// value null, or false for a boolean, which is the value it gets, rather than
// its definition, and so is each field of a row; and what is evaluated once
// the run is underway unknown. The view is made with it, and kept as it
// would make it (refresh).
func (r *inputResolver) evalValues() map[string]any {
	values := jsonvalue.CloneMap(r.values)
	for _, fields := range [][]*field{r.fields, r.rows} {
		for _, f := range fields {
			v, ok := f.filledWith()
			if !ok {
				continue
			}
			if _, has := jsonptr.Lookup(values, f.path); !has {
				setIn(values, f.path, v)
			}
		}
	}
	for _, f := range r.fields {
		f.markUnknown(values, nil)
	}
	return values
}

// compileTimeContexts returns the contexts known when the run is created:
// inputs, and the caller's org, vars, remote and sessions. org, vars and
// remote default to empty, which reads every name as "".
func compileTimeContexts(opts Options, inputs any) map[string]any {
	contexts := map[string]any{"org": map[string]any{}, "vars": map[string]any{}, "remote": map[string]any{}}
	maps.Copy(contexts, opts.Contexts)
	contexts["inputs"] = inputs
	return contexts
}
