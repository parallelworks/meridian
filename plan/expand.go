package plan

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/problems"
)

// DefaultMaxInstances is how many instances a matrix can have unless
// whoever runs the workflow sets another limit (Limits.MaxInstances). The
// limit is Meridian's, not the format's.
const DefaultMaxInstances = 256

// An Expansion is a matrix job's instances, and how the coordinator runs
// them, from its strategy:. Expand evaluates the strategy as one value, so
// each of its instances is as private, and as secret, as the others, and
// FailFast and MaxParallel follow its instances' Private and Secret: the
// coordinator keeps them only in the run's secret record when its
// instances are private.
type Expansion struct {
	// Instances are the job's instances, in the order of their indexes.
	Instances []Instance `json:"instances"`
	// FailFast is the strategy's fail-fast:, true unless it says false:
	// when an instance fails, the coordinator cancels the ones that remain.
	FailFast bool `json:"failFast,omitzero"`
	// MaxParallel is the strategy's max-parallel:, how many instances the
	// coordinator places at a time, or 0 for no limit.
	MaxParallel int `json:"maxParallel,omitzero"`
}

// An Instance is one combination of a matrix's values.
type Instance struct {
	// Key is the job's name, # and Index, as in build#2.
	Key string `json:"key"`
	// Index is the instance's index, from 0.
	Index int `json:"index"`
	// Matrix holds its matrix values, a mapping, by key.
	Matrix Data `json:"matrix"`
	// Private is set when evaluating the matrix read a variable or a
	// password: the coordinator keeps the instance's matrix values only in
	// the run's secret record, and its key as run state. Secret is set when
	// it read a secret: the values are masked wherever the job reads them.
	// The job reads either as Contexts.Private and Contexts.Secrets say.
	Private bool `json:"private,omitzero"`
	Secret  bool `json:"secret,omitzero"`
}

// An ExpandError is a matrix that can't be expanded once its values are
// known: one with no instance or more than the limit, or an include or
// exclude entry of the wrong shape. It is a problem with the job's
// strategy:, of a kind and its params, which the caller reports at Path.
type ExpandError struct {
	Kind   *problems.Kind
	Params problems.Params
	// Path is the JSON Pointer in the workflow of what is wrong: the
	// strategy's matrix:, or an entry of its include or exclude.
	Path string
}

func (e *ExpandError) Error() string { return e.Kind.Message(e.Params) }

// Expand evaluates job's strategy: in scope and expands its matrix into the
// job's instances, as docs/design.md describes under Matrices: its keys
// other than include and exclude, in the order of their names, give every
// combination of their values, the first key changing slowest; each
// exclude entry removes the combinations that have all its values; and
// each include entry then adds its values to every combination it matches
// without changing one, or else is a combination of its own. Each
// instance's key is the job's name, # and its index.
//
// It follows format 1, as ACTIVATE expands a matrix. An include entry
// matches a combination when it names at least one of the matrix's keys,
// and has the combination's value for each it names, so an entry that
// names none is a combination of its own. It matches those that earlier
// include entries made too, and adds its values over theirs. Two values
// match when they are equal, or when they are a string and a number or
// a boolean whose text it is, as 1 and "1" are; null matches only null. An
// exclude entry with a key the matrix doesn't have removes nothing, and
// one with no keys removes every combination.
//
// fail-fast is true unless it is false, and max-parallel is the whole part
// of a number of 1 or more, or else 0, for no limit, as in format 1.
//
// It is the one expansion: the compiler checks a matrix with it where the
// matrix's values are known when the run is created, Start expands one
// whose values are known when the run starts, and Ready one that reads
// needs. It counts the instances before it makes them, so a matrix far
// over the limit costs no more than one at it: it reads each value of a
// key once, to index it by its text, and looks at no more than 2^24
// combinations of its keys' values, values an exclude entry removes,
// comparisons of an exclude or include entry's values with them, each item
// of a list or a mapping it compares counting, and entries checked against
// them, together, checking its context as it goes. A matrix with more than
// that to look at is over the limit too.
//
// It returns an *ExpandError for a matrix that has no instance, more than
// limits.MaxInstances or an entry of the wrong shape, and the error of
// evaluating the strategy otherwise: an *expr.UnknownError when it reads
// what scope doesn't have yet, such as needs when the run is created. The
// params of either show no value made from what scope marks as private,
// nor a key of a matrix whose keys are made from one, which an error's
// Path leaves out too. When ctx is done before it finishes, it returns
// ctx.Err().
func Expand(ctx context.Context, job *Job, scope expr.Scope, limits Limits) (*Expansion, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := job.Strategy
	if s == nil {
		return nil, fmt.Errorf("plan: the job %s has no strategy", job.Name)
	}
	// The strategy is one value: one group, and as private as any part of
	// it.
	scope = scope.Budgeted()
	var read expr.Read
	values := make([]any, 3)
	for i, v := range []Value{s.Matrix, s.FailFast, s.MaxParallel} {
		out, r, err := v.EvalRead(scope)
		read = read.Or(r)
		if err != nil {
			if read.Private {
				err = expr.Masked(err)
			}
			return nil, err
		}
		values[i] = out
	}
	x := &expansion{
		ctx: ctx, pointer: jsonptr.Append(job.Path, "strategy", "matrix"), private: read.Private,
		max: limits.MaxInstances, written: map[string]bool{"include": true, "exclude": true},
	}
	if x.max <= 0 {
		x.max = DefaultMaxInstances
	}
	if m, ok := s.Matrix.v.(map[string]any); ok {
		for k := range m {
			x.written[k] = true
		}
	}
	combos, err := x.combinations(values[0])
	if err != nil {
		return nil, err
	}
	e := &Expansion{Instances: make([]Instance, len(combos)), FailFast: true}
	for i, c := range combos {
		e.Instances[i] = Instance{
			Key: job.Name + "#" + strconv.Itoa(i), Index: i, Matrix: DataOf(c),
			Private: read.Private, Secret: read.Marked,
		}
	}
	if ff, ok := values[1].(bool); ok && !ff {
		e.FailFast = false
	}
	switch n := wide(values[2]).(type) {
	case int64:
		e.MaxParallel = int(min(max(n, 0), math.MaxInt32))
	case float64:
		if n >= 1 {
			e.MaxParallel = int(min(n, math.MaxInt32))
		}
	}
	return e, nil
}

// maxLooks is how much Expand looks at to count a matrix's combinations,
// whatever its exclude entries remove: each combination of its keys' values,
// each comparison of an entry's value with a key's, and each entry it checks
// one against. It is Meridian's own limit, so that a matrix whose keys have
// far more combinations than it may have, and exclude entries that remove
// most of them, can't make Expand look at each.
const maxLooks = 1 << 24

// An expansion is a matrix being expanded.
type expansion struct {
	ctx context.Context
	// pointer is the matrix's JSON Pointer, and private is set when its
	// values were made from a private value, so that a problem with them
	// doesn't show them. written holds the keys the workflow writes, which
	// show nothing of a value.
	pointer string
	private bool
	written map[string]bool
	// max is the most instances the matrix may have, and looks counts what
	// it has looked at, toward maxLooks.
	max   int
	looks int
}

// look counts n more looks, and fails once there are more than maxLooks,
// or ctx is done, which it checks every 4096 looks.
func (x *expansion) look(n int) error {
	before := x.looks
	x.looks += n
	if x.looks>>12 == before>>12 {
		return nil
	}
	if err := x.ctx.Err(); err != nil {
		return err
	}
	if x.looks > maxLooks {
		return x.fail(problems.MatrixTooLarge, problems.Params{"limit": x.max})
	}
	return nil
}

// fail returns an *ExpandError of kind k at the JSON Pointer of keys under
// the matrix's. For a matrix made from a private value, the params that
// show a value are masked, and so is a key the workflow doesn't write,
// which the JSON Pointer leaves out.
func (x *expansion) fail(k *problems.Kind, params problems.Params, keys ...string) error {
	if x.private {
		params = problems.MaskValues(params)
		if key, ok := params["key"].(string); ok && !x.written[key] {
			params["key"] = problems.Masked
			keys = nil
		}
	}
	return &ExpandError{Kind: k, Params: params, Path: jsonptr.Append(x.pointer, keys...)}
}

// combinations returns the matrix's combinations, m being its value.
func (x *expansion) combinations(m any) ([]map[string]any, error) {
	matrix, ok := m.(map[string]any)
	if !ok {
		return nil, x.fail(problems.WrongType, problems.Params{"count": 1, "type1": "object", "has_got": true, "got": typeName(m)})
	}
	var keys []string
	var lists []*valueList
	var includes, excludes []map[string]any
	for k, v := range jsonvalue.Sorted(matrix) {
		if k == "include" || k == "exclude" {
			entries, err := x.entries(k, v)
			if err != nil {
				return nil, err
			}
			if k == "include" {
				includes = entries
			} else {
				excludes = entries
			}
			continue
		}
		list, ok := v.([]any)
		if !ok {
			return nil, x.fail(problems.MatrixNotList, problems.Params{"key": k, "value": problems.Show(v)}, k)
		}
		if err := x.look(len(list)); err != nil {
			return nil, err
		}
		keys, lists = append(keys, k), append(lists, newValueList(list))
	}
	combos, err := x.product(keys, lists, excludes)
	if err != nil {
		return nil, err
	}
	for _, include := range includes {
		// Matching an entry, and adding its values, looks at each of them.
		cost := nodes(include)
		merged := false
		for _, c := range combos {
			if err := x.look(cost); err != nil {
				return nil, err
			}
			if matchesIncluded(c, include, keys) {
				for k, v := range include {
					c[k] = jsonvalue.Clone(v)
				}
				merged = true
			}
		}
		if !merged {
			if len(combos) == x.max {
				return nil, x.fail(problems.MatrixTooLarge, problems.Params{"limit": x.max})
			}
			combos = append(combos, jsonvalue.CloneMap(include))
		}
	}
	if len(combos) == 0 {
		return nil, x.fail(problems.MatrixEmpty, nil)
	}
	return combos, nil
}

// entries returns the entries of the matrix's include or exclude, key,
// whose value is v: a list of mappings, or null for none.
func (x *expansion) entries(key string, v any) ([]map[string]any, error) {
	if v == nil {
		return nil, nil
	}
	list, ok := v.([]any)
	if !ok {
		return nil, x.fail(problems.MatrixNotList, problems.Params{"key": key, "value": problems.Show(v)}, key)
	}
	out := make([]map[string]any, len(list))
	for i, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, x.fail(problems.MatrixEntryNotMapping, problems.Params{"key": key, "value": problems.Show(item)}, key, strconv.Itoa(i))
		}
		out[i] = m
	}
	return out, nil
}

// A valueList is a key's values, indexed by what they match: a scalar by
// its text, null apart, and lists and mappings, which match only those
// whose items match, in a list of their own.
type valueList struct {
	values     []any
	byText     map[string][]int
	nulls      []int
	containers []int
}

func newValueList(values []any) *valueList {
	l := &valueList{values: values, byText: map[string][]int{}}
	for i, v := range values {
		switch v.(type) {
		case nil:
			l.nulls = append(l.nulls, i)
		case []any, map[string]any:
			l.containers = append(l.containers, i)
		default:
			t := expr.Text(v)
			l.byText[t] = append(l.byText[t], i)
		}
	}
	return l
}

// matching returns the indexes, in order, of the values that match want,
// and counts what it compares toward the matrix's looks.
func (x *expansion) matching(l *valueList, want any) ([]int, error) {
	switch want.(type) {
	case nil:
		return l.nulls, x.look(1)
	case []any, map[string]any:
		// Comparing a list or a mapping looks at each of its items.
		cost := nodes(want)
		var out []int
		for _, i := range l.containers {
			if err := x.look(cost); err != nil {
				return nil, err
			}
			if sameValue(l.values[i], want) {
				out = append(out, i)
			}
		}
		return out, nil
	}
	return l.byText[expr.Text(want)], x.look(1)
}

// nodes returns how many values v is made of: 1, and those its items are
// made of.
func nodes(v any) int {
	n := 1
	switch v := v.(type) {
	case []any:
		for _, item := range v {
			n += nodes(item)
		}
	case map[string]any:
		for _, item := range v {
			n += nodes(item)
		}
	}
	return n
}

// textKey returns what tells apart the scalar values matching tells apart,
// and false for a list or a mapping.
func textKey(v any) (string, bool) {
	switch v.(type) {
	case nil:
		return "null", true
	case []any, map[string]any:
		return "", false
	}
	return "text:" + expr.Text(v), true
}

// product returns the combinations of the values of keys, which lists
// holds, the first key changing slowest, without those an entry of
// excludes matches. It fails if there are more than the matrix may have.
func (x *expansion) product(keys []string, lists []*valueList, excludes []map[string]any) ([]map[string]any, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	// allowed holds the indexes of each key's values that no exclude entry
	// of that key alone removes, and others the entries of more than one
	// key, as the indexes of the values of each of their keys they match.
	// removed holds the indexes of each key's values that an entry of that
	// key alone removes, and removedText the values those entries named, so
	// that a value named again isn't removed again.
	removed := make([]map[int]bool, len(keys))
	removedText := make([]map[string]bool, len(keys))
	type entry struct {
		keys  []int
		match [][]int
	}
	var others []entry
	for _, ex := range excludes {
		e := entry{}
		matches := true
		for name, want := range jsonvalue.Sorted(ex) {
			k, ok := slices.BinarySearch(keys, name)
			if !ok {
				// A combination has only the matrix's keys.
				matches = false
				break
			}
			if len(ex) == 1 {
				// An entry of one key that names a value an entry before it
				// named removes nothing more.
				if key, ok := textKey(want); ok {
					if removedText[k][key] {
						matches = false
						if err := x.look(1); err != nil {
							return nil, err
						}
						break
					}
					if removedText[k] == nil {
						removedText[k] = map[string]bool{}
					}
					removedText[k][key] = true
				}
			}
			match, err := x.matching(lists[k], want)
			if err != nil {
				return nil, err
			}
			if len(match) == 0 {
				matches = false
				break
			}
			e.keys, e.match = append(e.keys, k), append(e.match, match)
		}
		switch {
		case !matches:
			// The entry matches no combination, or none another hasn't.
		case len(e.keys) == 0:
			return nil, nil
		case len(e.keys) == 1:
			k := e.keys[0]
			if err := x.look(len(e.match[0])); err != nil {
				return nil, err
			}
			if removed[k] == nil {
				removed[k] = map[int]bool{}
			}
			for _, i := range e.match[0] {
				removed[k][i] = true
			}
		default:
			others = append(others, e)
		}
	}
	allowed := make([][]int, len(keys))
	for k, l := range lists {
		for i := range l.values {
			if !removed[k][i] {
				allowed[k] = append(allowed[k], i)
			}
		}
		if len(allowed[k]) == 0 {
			return nil, nil
		}
	}
	var kept [][]int
	at := make([]int, len(keys))
	for {
		if err := x.look(1 + len(others)); err != nil {
			return nil, err
		}
		excluded := false
		for _, e := range others {
			all := true
			for j, k := range e.keys {
				if _, ok := slices.BinarySearch(e.match[j], allowed[k][at[k]]); !ok {
					all = false
					break
				}
			}
			if all {
				excluded = true
				break
			}
		}
		if !excluded {
			if len(kept) == x.max {
				return nil, x.fail(problems.MatrixTooLarge, problems.Params{"limit": x.max})
			}
			combo := make([]int, len(keys))
			for k := range keys {
				combo[k] = allowed[k][at[k]]
			}
			kept = append(kept, combo)
		}
		// The last key changes fastest.
		k := len(keys) - 1
		for ; k >= 0; k-- {
			if at[k]++; at[k] < len(allowed[k]) {
				break
			}
			at[k] = 0
		}
		if k < 0 {
			break
		}
	}
	combos := make([]map[string]any, len(kept))
	for i, combo := range kept {
		c := make(map[string]any, len(keys))
		for k, j := range combo {
			c[keys[k]] = jsonvalue.Clone(lists[k].values[j])
		}
		combos[i] = c
	}
	return combos, nil
}

// matchesIncluded reports whether an include entry matches a combination:
// whether it names one of the matrix's keys at least, and has the
// combination's value for each it names.
func matchesIncluded(combo, include map[string]any, keys []string) bool {
	named := false
	for _, k := range keys {
		want, ok := include[k]
		if !ok {
			continue
		}
		if have, ok := combo[k]; !ok || !sameValue(have, want) {
			return false
		}
		named = true
	}
	return named
}

// sameValue reports whether two of a matrix's values match: whether they
// are equal, or a string and a number or a boolean whose text it is. Null
// matches only null, and lists and mappings match when each of their items
// does.
func sameValue(a, b any) bool {
	switch a := a.(type) {
	case nil:
		return b == nil
	case []any:
		l, ok := b.([]any)
		return ok && len(a) == len(l) && sameItems(a, l)
	case map[string]any:
		m, ok := b.(map[string]any)
		if !ok || len(a) != len(m) {
			return false
		}
		for k, v := range a {
			w, ok := m[k]
			if !ok || !sameValue(v, w) {
				return false
			}
		}
		return true
	}
	switch b.(type) {
	case nil, []any, map[string]any:
		return false
	}
	return expr.Text(a) == expr.Text(b)
}

// sameItems reports whether each item of a matches the one of b at its
// index.
func sameItems(a, b []any) bool {
	for i := range a {
		if !sameValue(a[i], b[i]) {
			return false
		}
	}
	return true
}
