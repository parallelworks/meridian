package compile

import (
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/internal/reach"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// compareState is what the inputResolver keeps of comparing what a form
// sent with what made it (compare).
type compareState struct {
	// compareScope is Create's scope (viewState.created) with the budget
	// that comparing what a form sent with what made it spends (compare): a
	// group of its own, so that what it spends doesn't decide the run's
	// shape. comparisons holds what each string compared gave, by its
	// location and the row index it read.
	compareScope *expr.Scope
	comparisons  map[string]comparison
	// compareSlow is set once a comparison runs out of the group's time,
	// which is reported once.
	compareSlow bool
}

// A text is a string in a value, at path in it, that is the text of the
// expression at loc in the workflow: the one in the same place in the
// input's default, or one of its options' values. A form sends that text
// for an expression it can't evaluate itself.
type text struct{ path, loc []string }

// A candidate is a part of a submitted value, at path in it, that can be
// what one of the strings at locs in the workflow gave, one of which reads
// a variable or a password: the string in the same place in the input's
// default, or with a list's default rows, at the same key in any of them,
// as a form evaluates them once and the person can delete and add rows; or
// the input's options' values, for option set. With each set, each item
// of the list at path can be.
type candidate struct {
	path         []string
	locs         [][]string
	option, each bool
}

// compare decides what each of f's candidates is: a part of f's submitted
// value that a form can have made from a variable or a password. A form
// evaluates each default and option it can and sends what it gives, but it
// can send what it gave before the person changed what it read, or gave a
// row they have since moved, and evaluates some expressions otherwise than
// Meridian does. So a part that is what one of its expressions gives as the
// run is created means that expression, as a form's text for it does: as
// the run starts, it is what the expression gives then, if the expression
// can read a variable or a password, and is data if not. Which parts mean
// their expressions says something of the variables' values, so the secret
// record keeps it (means), and the plan doesn't hold such a part, whatever
// it is. A choice of an option written in the workflow is that option. Any
// other part, what a form gave before, or what the person typed in its
// place, the plan doesn't hold, but the run has (holdPath): it is a secret,
// masked as what is made from one is, if one of its expressions can read a
// password or a secret variable, and private, as a variable's value is, if
// not. A part already evaluated as the run starts, or in one, is left as
// it is. The expressions are evaluated in a group of their own
// (evalCompare). Comparing changes no value, so what an expression gives
// is the same for each candidate it is compared with, at each row index it
// reads.
func (r *inputResolver) compare(f *field) {
	value := lookup(r.values, f.path)
	// What the Inputs resolver gives for an input of a platform's type can't
	// be split by the parts that made it, so a part of one held keeps all of
	// it out of the plan, masked if a part read a secret.
	resolved := f.in == nil && platformTypes[f.typ]
	whole, wholeKind := false, heldPrivate
	hold := func(path []string, kind holding) {
		if resolved && len(path) > 0 {
			whole = true
			wholeKind = min(wholeKind, kind)
			return
		}
		r.holdPath(f.holder(), slices.Concat(f.path, path), kind)
	}
	for _, c := range f.candidates {
		var gave []comparison
		for _, path := range r.parts(f, c) {
			at := slices.Concat(f.path, path)
			if f.isText(path) || r.madeAtStart(at) {
				continue
			}
			if gave == nil {
				gave = make([]comparison, len(c.locs))
				for i, loc := range c.locs {
					gave[i] = r.evalCompare(loc, f.index)
				}
			}
			part := lookup(value, path)
			i := slices.IndexFunc(gave, func(res comparison) bool { return res.ok && sameValue(part, res.v) })
			if i >= 0 && r.readsPart(c.locs[i], f.index, at) {
				// What reads the part gives it, whatever it is, so it isn't
				// what made it.
				i = -1
			}
			name := plan.JoinPath(slices.Concat([]string{"inputs"}, at)...)
			switch {
			case i >= 0 && !r.reads(c.locs[i], false):
				// What reads no variable and no password gave it, as Create
				// and Start find alike: it is data.
				continue
			case r.final:
				// What it means Create decided, which the secret record
				// keeps (plan.Record.Means): the expression, which gives
				// what it gives as the run starts, or what was typed.
				i = -1
				if pointer, ok := r.means[name]; ok {
					i = slices.IndexFunc(c.locs, func(loc []string) bool { return jsonptr.Format(loc) == pointer })
					if i >= 0 && !gave[i].ok {
						i = -1
					}
				}
			case i >= 0:
				// As the run is created, the record keeps that it means the
				// expression, and the plan holds it out of its values,
				// whatever it is, as it holds what was typed.
				if r.means == nil {
					r.means = map[string]string{}
				}
				r.means[name] = jsonptr.Format(c.locs[i])
				i = -1
			}
			switch {
			case i >= 0:
				// The part means the expression, which Start has evaluated:
				// it is what the expression gave, made from what it read. One
				// that gives another value than was sent, as a variable
				// changed, what read the part read too soon (remeant).
				if !sameValue(part, gave[i].v) {
					if r.remeant == nil {
						r.remeant = map[string]any{}
					}
					r.remeant[plan.JoinPath(at...)] = jsonvalue.Clone(gave[i].v)
				}
				loc := c.locs[i]
				r.noteEvaluation(f, path, loc, lookup(r.doc.Value, loc))
				r.set(at, jsonvalue.Clone(gave[i].v))
				r.start.made(r, at, gave[i].read)
			case c.option && f.findOptions().literal(part):
			case f.typ == "password" && len(path) == 0:
				// A password is a secret, whatever made it.
			case slices.ContainsFunc(c.locs, func(loc []string) bool { return r.reads(loc, true) }):
				hold(path, heldDerived)
			default:
				hold(path, heldPrivate)
			}
		}
	}
	if whole {
		r.holdPath(f, f.path, wholeKind)
	}
}

// madeAtStart reports whether the value at path in the inputs' values, or
// one it is in, is what an evaluation that read a variable or a password
// made as the run starts: Create holds such a part for the start, and so
// leaves it as it is, rather than compare it with what made it.
func (r *inputResolver) madeAtStart(path []string) bool {
	if !r.final {
		return false
	}
	full := slices.Concat([]string{"inputs"}, path)
	for n := len(full); n > 1; n-- {
		name := plan.JoinPath(full[:n]...)
		if r.start.madePrivate[name] || r.start.madeSecret[name] {
			return true
		}
	}
	return false
}

// readsPart reports whether what is at loc in the workflow, for the list
// rows at index, reads the inputs' value at path: itself, or through a
// value an evaluation made from it, as a default that reads another input
// whose default reads it does.
//
// It looks from both ends, a step at a time: from what loc reads, through
// the evaluations that gave it, and from the value, through the
// evaluations that read it, either of which finds the answer alone. Each
// step is taken on the side with less to follow, the value's first when
// the two have as much, as most often nothing has read it. So a comparison costs
// what the smaller side reaches, not what loc reads through, which can be
// each row of a long list, or each default of a long chain, for each of
// many values compared.
func (r *inputResolver) readsPart(loc []string, index []int, path []string) bool {
	exprs, err := r.workflow.Exprs(loc)
	if err != nil {
		return false
	}
	reads := readPatterns(exprs, index)
	readsPath := func(patterns [][]string, path []string) bool {
		return slices.ContainsFunc(patterns, func(p []string) bool { return reach.Reaches(p, path) })
	}
	if readsPath(reads, path) {
		return true
	}
	for _, e := range r.evaluations[r.gaveTo:] {
		r.gave.Add(slices.Concat(e.f.path, e.path), e)
	}
	r.gaveTo = len(r.evaluations)
	// fromLoc holds the evaluations that gave what loc reads, and those
	// that gave what they read, and so on, and fromPart those that read the
	// value, and those that read what they gave, and so on; next and
	// before are the patterns and the paths each side follows next. When
	// the two have as much to follow, the sides take turns.
	fromLoc, fromPart := map[*evaluation]bool{}, map[*evaluation]bool{}
	next, before := reads, [][]string{path}
	forward := true
	for len(next) > 0 && len(before) > 0 {
		forward = len(next) < len(before) || len(next) == len(before) && !forward
		if !forward {
			var paths [][]string
			for _, q := range before {
				for e := range r.evaluated.Reaching(q) {
					if fromPart[e] {
						continue
					}
					fromPart[e] = true
					at := slices.Concat(e.f.path, e.path)
					if fromLoc[e] || readsPath(reads, at) {
						return true
					}
					paths = append(paths, at)
				}
			}
			before = paths
			continue
		}
		var patterns [][]string
		for _, p := range next {
			for e := range r.gave.Reached(p) {
				if fromLoc[e] {
					continue
				}
				fromLoc[e] = true
				if fromPart[e] {
					return true
				}
				if exprs, err := r.workflow.Exprs(e.loc); err == nil {
					read := readPatterns(exprs, e.f.index)
					if readsPath(read, path) {
						return true
					}
					patterns = append(patterns, read...)
				}
			}
		}
		next = patterns
	}
	return false
}

// A comparison is what a string in the workflow gave, if it gave one,
// when compare evaluated it.
type comparison struct {
	v    any
	ok   bool
	read expr.Read
}

// evalCompare evaluates what is at loc in the workflow, as evalAt does, for
// the list rows at index, in the group that comparisons share, once for
// each row index that what it reads depends on. The group is one of its
// own (compareScope), so that comparing spends nothing of what decides the
// run's shape. A comparison its budget stops gives nothing, as one that
// fails does, and so what is compared is held, the same each time for the
// same workflow and values; but running out of the group's time, which
// depends on how fast the machine is, is an error.
func (r *inputResolver) evalCompare(loc []string, index []int) comparison {
	exprs, err := r.workflow.Exprs(loc)
	if err != nil {
		return comparison{}
	}
	key := jsonptr.Format(loc)
	if slices.ContainsFunc(exprs, readsIndex) {
		key += "|" + indexKey(index)
	}
	if res, ok := r.comparisons[key]; ok {
		return res
	}
	base := r.scope()
	if r.compareScope == nil {
		s := r.created.Budgeted()
		r.compareScope = &s
	}
	scope := *r.compareScope
	scope.Marked, scope.Private, scope.Index = base.Marked, base.Private, index
	// What it read, as Start names what it makes.
	v, read, err := r.workflow.Evaluator(scope).EvalRead(loc)
	if be, ok := errors.AsType[*expr.BudgetError](err); ok && be.Kind == problems.ExpressionsTooSlow && !r.compareSlow {
		r.compareSlow = true
		r.reportError(loc, 0, err)
	}
	// A copy, as the value can be part of what the evaluation read.
	res := comparison{jsonvalue.Clone(v), err == nil, read}
	if r.comparisons == nil {
		r.comparisons = map[string]comparison{}
	}
	r.comparisons[key] = res
	return res
}

// readsIndex reports whether what e gives can depend on the row index it
// is evaluated for: whether it reads [index], or another definition.
func readsIndex(e *expr.Expr) bool {
	return e.Relative() || slices.ContainsFunc(e.Refs(), func(ref expr.Ref) bool {
		return slices.Contains(ref.Path, "[index]")
	})
}

// parts returns the paths in f's value of the parts that c, one of its
// candidates, is: its own, or with each set, each item there that isn't a
// form's text.
func (r *inputResolver) parts(f *field, c candidate) [][]string {
	if !c.each {
		return [][]string{c.path}
	}
	var out [][]string
	list, _ := lookup(r.values, slices.Concat(f.path, c.path)).([]any)
	for i := range list {
		if path := appendPath(c.path, strconv.Itoa(i)); !f.isText(path) {
			out = append(out, path)
		}
	}
	return out
}

// isText reports whether the part of f's value at path is a form's text
// for an expression, or is evaluated as the run starts already, or is in
// such a part.
func (f *field) isText(path []string) bool {
	for _, t := range f.texts {
		if related(t.path, path) && len(t.path) <= len(path) {
			return true
		}
	}
	return f.startsLater(slices.Concat(f.path, path), true)
}

// sameValue reports whether a submitted value, a, is b, what an expression
// gave, as a form sends it: a list or a mapping holds the same values, and
// any other value has the same text (looseKey), as the number 8 and the
// text "8" do.
func sameValue(a, b any) bool {
	switch a := a.(type) {
	case nil:
		return b == nil
	case []any:
		l, ok := b.([]any)
		if !ok || len(a) != len(l) {
			return false
		}
		for i := range a {
			if !sameValue(a[i], l[i]) {
				return false
			}
		}
		return true
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
	// A string is its own text, which needn't be made again.
	as, aString := a.(string)
	bs, bString := b.(string)
	switch {
	case aString && bString:
		return as == bs
	case aString:
		return as == looseKey(b)
	case bString:
		return looseKey(a) == bs
	}
	return looseKey(a) == looseKey(b)
}

// findTexts finds the strings in f's value that are a form's text for an
// expression: the one in the same place in its default, or one of its
// options' values, or with a list's default rows, the one at the same key
// in any of them. Every other string is data. It sets f's texts, and the
// expressions they hold; and its candidates, the other parts of the value
// in the place of one of those expressions, or that the options can have
// given, where one of them can read a variable or a password.
func (r *inputResolver) findTexts(f *field) {
	value := lookup(r.values, f.path)
	var texts []text
	var candidates []candidate
	// items holds the keys of the paths of one key that have a text, so
	// that each item of a list has one.
	items := map[string]bool{}
	dflt, loc := f.def["default"], appendPath(f.loc, "default")
	if rows, ok := dflt.([]any); ok && f.typ == "list" {
		texts, candidates = rowTexts(rows, value, loc)
	} else {
		textPaths, candidatePaths := defaultTexts(dflt, value, nil, nil, nil)
		for _, p := range textPaths {
			texts = append(texts, text{p, slices.Concat(loc, p)})
			if len(p) == 1 {
				items[p[0]] = true
			}
		}
		for _, p := range candidatePaths {
			candidates = append(candidates, candidate{path: p, locs: [][]string{slices.Concat(loc, p)}})
		}
	}
	list, isList := value.([]any)
	var options [][]string
	for _, o := range f.findOptions().exprs {
		options = append(options, o.loc)
		if value == o.value {
			texts = append(texts, text{nil, o.loc})
			options = nil
		}
		for i, item := range list {
			if item != o.value {
				continue
			}
			if k := strconv.Itoa(i); !items[k] {
				items[k] = true
				texts = append(texts, text{[]string{k}, o.loc})
			}
		}
	}
	if options != nil && value != nil && !slices.ContainsFunc(texts, func(t text) bool { return t.path == nil }) {
		candidates = append(candidates, candidate{locs: options, option: true, each: isList})
	}
	f.texts, f.reads = texts, nil
	for _, t := range texts {
		exprs, err := r.workflow.Exprs(t.loc)
		if err != nil {
			continue // checkSyntax reported it.
		}
		f.reads = append(f.reads, exprs...)
	}
	f.candidates = slices.DeleteFunc(candidates, func(c candidate) bool {
		return !slices.ContainsFunc(c.locs, func(loc []string) bool { return r.reads(loc, false) })
	})
}

// rowTexts finds, in value, a list's submitted rows, the strings that are
// the text of an expression in the same place in rows, the list's default
// rows, at loc in the workflow; and the candidates in the place of an
// expression at the same key in any default row, since a person can
// delete, add and move the rows a form filled in from the default.
func rowTexts(rows []any, value any, loc []string) ([]text, []candidate) {
	// byKey holds the places of the expressions in a row, by their paths in
	// it, and the default rows they are in.
	type place struct {
		at   []string
		rows []int
	}
	var places []*place
	byKey := map[string]*place{}
	for j, row := range rows {
		exprStrings(row, nil, nil, func(_, at []string) {
			key := jsonptr.Format(at)
			if byKey[key] == nil {
				byKey[key] = &place{at: at}
				places = append(places, byKey[key])
			}
			byKey[key].rows = append(byKey[key].rows, j)
		})
	}
	var texts []text
	var candidates []candidate
	list, _ := value.([]any)
	for i, item := range list {
		for _, p := range places {
			v := lookup(item, p.at)
			if v == nil {
				continue
			}
			path := slices.Concat([]string{strconv.Itoa(i)}, p.at)
			// A form's text is a string; a row that a default row's
			// expression gives, all of it, is a mapping, which == can't
			// compare.
			if s, isText := v.(string); isText && i < len(rows) && s == lookup(rows[i], p.at) {
				texts = append(texts, text{path, slices.Concat(loc, path)})
				continue
			}
			c := candidate{path: path}
			for _, j := range p.rows {
				c.locs = append(c.locs, slices.Concat(loc, []string{strconv.Itoa(j)}, p.at))
			}
			candidates = append(candidates, c)
		}
	}
	return texts, candidates
}

// defaultTexts appends to texts the paths, from path, of the strings in v
// that are the text of an expression in the same place in def, and to
// candidates those of the other values in v in the place of a string in
// def that holds one. It appends to path in place, so that the walk reuses
// one path, and copies the paths it keeps.
func defaultTexts(def, v any, path []string, texts, candidates [][]string) ([][]string, [][]string) {
	switch def := def.(type) {
	case string:
		if !strings.Contains(def, "${{") || v == nil {
			break
		}
		if s, ok := v.(string); ok && s == def {
			texts = append(texts, slices.Clone(path))
		} else {
			candidates = append(candidates, slices.Clone(path))
		}
	case []any:
		list, _ := v.([]any)
		for i := range min(len(def), len(list)) {
			texts, candidates = defaultTexts(def[i], list[i], append(path, strconv.Itoa(i)), texts, candidates)
		}
	case map[string]any:
		m, _ := v.(map[string]any)
		for k, d := range jsonvalue.Sorted(def) {
			if item, ok := m[k]; ok {
				texts, candidates = defaultTexts(d, item, append(path, k), texts, candidates)
			}
		}
	}
	return texts, candidates
}
