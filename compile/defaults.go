package compile

import (
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/problems"
)

// An evaluation is a part of a field's value, at path in it, that the
// compiler evaluated from raw, the workflow's value at loc, or that a form
// evaluated from it, which a default, a form's text or a comparison gave.
// done is set once it is evaluated as the run starts instead (holdWhole).
type evaluation struct {
	f         *field
	path, loc []string
	raw       any
	done      bool
}

// fillDefaults fills in the values. Each input without a value gets its
// default, and so does each field of a list's rows, from the list's
// template, once the list's rows are known. A string a form sent as the
// text of an expression gets the expression's value. Expressions are
// evaluated with the other inputs' values, defaults included, in the order
// they need. A submitted value that a form can have made from a variable or
// a password is read as one of them from the start, and once the values
// are filled in, is evaluated as the run starts if it is what made it, and
// is kept as a secret if not (compare).
func (r *inputResolver) fillDefaults() {
	r.taintInputs()
	var pending pendingFields
	var compared []*field
	var start func(fields []*field)
	start = func(fields []*field) {
		for _, f := range fields {
			waits := r.prepare(f)
			if waits {
				pending.add(f)
			}
			if f.candidates != nil {
				compared = append(compared, f)
				for _, c := range f.candidates {
					for _, path := range r.parts(f, c) {
						r.mark(slices.Concat(f.path, path))
					}
				}
			}
			if !waits {
				start(r.collectRows(f))
			}
		}
	}
	start(r.fields)
	for p := pending.next(); p != nil; p = pending.next() {
		r.evalPending(p.f)
		start(r.collectRows(p.f))
		pending.filled(p)
	}
	if left := pending.waiting(); len(left) > 0 {
		names := make([]string, len(left))
		for i, f := range left {
			names[i] = strings.Join(f.path, ".")
			// Its rows aren't known, and so neither are the passwords among
			// them: all of its value is kept as a secret.
			if f.typ != "password" && holdsPassword(f.def) {
				r.holdPath(f, f.path, heldSecret)
			}
		}
		r.reportAt(problems.DefaultsReadEachOther, left[0].loc, problems.Params{"inputs": problems.QuoteAll(names)})
	}
	for _, f := range compared {
		r.compare(f)
	}
	r.holdWhole()
	r.heldParts()
	r.holdLater()
}

// prepare fills in what it can of f's value without evaluating anything: a
// default that has no expressions. It reports whether f waits for its
// default's expressions, or its texts', to be evaluated; and finds the
// candidates of a submitted value, which are compared once the values are
// filled in.
func (r *inputResolver) prepare(f *field) bool {
	if f.container || f.header {
		return false
	}
	if !r.missing(f) {
		r.findTexts(f)
		return f.texts != nil
	}
	if !f.hasDefault {
		return false
	}
	// The workflow keeps what it parses, for the evaluation.
	reads, err := r.workflow.Exprs(appendPath(f.loc, "default"))
	if err != nil {
		return false // checkSyntax reported it.
	}
	if reads == nil {
		r.setDefault(f, f.def["default"])
		return false
	}
	if rows, ok := f.def["default"].([]any); ok && f.typ == "list" {
		// The rows are filled in as a form's starting rows are, each
		// expression where it is, so that what is known of them is, and
		// their fields' defaults can be filled in too.
		r.set(f.path, jsonvalue.Clone(rows))
		r.findTexts(f)
		return true
	}
	f.reads = reads
	return true
}

// evalPending evaluates f's texts, or else its default, and fills in its
// value with them. What reads something the compiler doesn't know is
// evaluated once the run is underway, and so is what reads a variable or a
// password, whose value the plan doesn't hold, though the compiler fills it
// in.
func (r *inputResolver) evalPending(f *field) {
	if f.texts == nil {
		loc := appendPath(f.loc, "default")
		before, ok := jsonptr.Lookup(r.values, f.path)
		f.nullBefore = ok && before == nil
		f.textBefore = ok && before != nil && before == f.def["default"]
		switch v, read, err := r.evalRead(loc, f.index); {
		case expr.IsUnknown(err):
			r.deferPart(f, nil, loc, f.def["default"], false)
		case err != nil:
			r.reportError(loc, 0, maskedIf(read.Private, err))
		default:
			r.setDefault(f, v)
			r.noteEvaluation(f, nil, loc, f.def["default"])
			r.noteRead(f, nil, loc, f.def["default"], read)
		}
		return
	}
	value := lookup(r.values, f.path)
	for _, t := range f.texts {
		text := lookup(value, t.path)
		switch v, read, err := r.evalRead(t.loc, f.index); {
		case expr.IsUnknown(err):
			r.deferPart(f, t.path, t.loc, text, false)
		case err != nil:
			r.reportError(t.loc, 0, maskedIf(read.Private, err))
		default:
			// A copy, as the value can be part of what the evaluation read.
			r.set(slices.Concat(f.path, t.path), jsonvalue.Clone(v))
			r.noteEvaluation(f, t.path, t.loc, text)
			r.noteRead(f, t.path, t.loc, text, read)
		}
	}
}

// noteRead notes what the evaluation that gave the part of f's value at
// path, raw at loc in the workflow, read. As the run is created, a part
// that read a variable or a password is evaluated again as it starts, as a
// plan never holds what is made from one; as it starts, Start names it
// among what it made from one (startState.made).
func (r *inputResolver) noteRead(f *field, path, loc []string, raw any, read expr.Read) {
	switch {
	case r.start != nil:
		r.start.made(r, slices.Concat(f.path, path), read)
	case read.Private:
		r.deferPart(f, path, loc, raw, true)
	}
}

// noteEvaluation notes that the part of f's value at path is what the
// expressions in raw, at loc in the workflow, gave, by the paths of the
// values they read: if one of them is evaluated as the run starts, so is
// the part (holdWhole).
func (r *inputResolver) noteEvaluation(f *field, path, loc []string, raw any) {
	exprs, err := r.workflow.Exprs(loc)
	if err != nil {
		return
	}
	e := &evaluation{f: f, path: path, loc: loc, raw: raw}
	for _, p := range readPatterns(exprs, f.index) {
		r.evaluated.Add(p, e)
	}
	r.gave.Add(slices.Concat(f.path, path), e)
}

// setDefault sets f's value to its default, unless the default is "", which
// gives no value. The value is a copy, so that filling in a list's rows
// doesn't change the workflow, nor what the evaluation read.
func (r *inputResolver) setDefault(f *field, v any) {
	if v != "" && v != nil {
		r.set(f.path, jsonvalue.Clone(v))
	}
}

// collectRows returns the fields of f's rows, if f is a list or a repeated
// step whose value is known: each of its rows' inputs, for each row that is
// a mapping.
func (r *inputResolver) collectRows(f *field) []*field {
	defs, loc := f.rowInputs()
	if defs == nil {
		return nil
	}
	value := lookup(r.values, f.path)
	rows, isList := value.([]any)
	// A password among the rows' inputs is kept out of the plan wherever it
	// is in the value, so a row, or a group or list of a row on the way to
	// one, that isn't the shape the rows' inputs say it is, which can hold a
	// password where the compiler can't find it, is kept as a secret, as it
	// was given.
	secret := holdsPassword(f.def) && !f.startsLater(f.path, false)
	if secret && f.in != nil && value != nil && !isList {
		r.holdPath(f.holder(), f.path, heldSecret)
		return nil
	}
	t := r.template(defs, loc)
	var out []*field
	for i, row := range rows {
		rowPath := appendPath(f.path, strconv.Itoa(i))
		if _, ok := row.(map[string]any); !ok {
			if secret && row != nil && !f.startsLater(rowPath, false) {
				r.holdPath(f.holder(), rowPath, heldSecret)
			}
			continue
		}
		if len(t.fields) == 0 {
			continue
		}
		index := append(slices.Clip(f.index), i)
		fields := t.row(rowPath)
		for _, c := range fields {
			c.index, c.in = index, f.holder()
			c.seq = len(r.fields) + len(r.rows) + len(out)
			r.markPassword(c)
			out = append(out, c)
			if v := lookup(r.values, c.path); c.container && !c.flatten && v != nil && holdsPassword(c.def) && !c.startsLater(c.path, false) {
				if _, ok := v.(map[string]any); !ok {
					r.holdPath(c.holder(), c.path, heldSecret)
				}
			}
		}
		if f.rows == nil {
			f.rows = make([][]*field, len(rows))
		}
		f.rows[i] = fields
	}
	if len(out) > 0 {
		r.rows = append(r.rows, out...)
		r.refresh(f.path)
	}
	return out
}

// missing reports whether f has no value: none at all, "" when "" isn't one
// of its options, or the text of its default when that is an expression,
// which a form sends when it can't evaluate the default itself.
func (r *inputResolver) missing(f *field) bool {
	v, ok := jsonptr.Lookup(r.values, f.path)
	if !ok || v == nil {
		return true
	}
	if s, isString := v.(string); isString && s != "" && s == f.def["default"] && strings.Contains(s, "${{") {
		return true
	}
	return v == "" && !f.findOptions().empty
}
