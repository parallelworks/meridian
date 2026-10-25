package compile

import (
	"errors"
	"slices"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// applyConditions evaluates each input's hidden, ignore and optional, drops
// the values of ignored inputs, and reports required inputs with no value.
func (r *inputResolver) applyConditions() {
	for _, f := range r.fields {
		if f.group != nil && f.group.ignored {
			f.ignored = true
			r.noteIgnored(f)
			r.refresh(f.path)
			continue
		}
		f.hidden = r.condition(f, "hidden") || f.group != nil && f.group.hidden
		if f.ignored = r.condition(f, "ignore"); f.ignored {
			// What expressions read of f changes, before its value is
			// dropped.
			r.refresh(f.path)
		}
		f.optional = r.condition(f, "optional")
		if f.ignored {
			r.noteIgnored(f)
			r.drop(f)
		}
	}
	for _, f := range r.fields {
		// A repeated step with no value has no rows.
		if f.container || f.header || f.ignored || f.hasDefault || f.deferred || f.repeats() || !r.missing(f) {
			continue
		}
		switch {
		case f.optional || f.hidden:
		case f.typ == "boolean":
			r.set(f.path, false)
		default:
			r.reportAt(problems.InputRequired, f.loc, problems.Params{"input": f.label()})
		}
	}
}

// condition evaluates one of f's hidden, ignore and optional, which is true
// if it is true or the text "true". One that read a variable or a password
// is a decision of the run's shape that Start makes again, which the plan
// names, and the secret record keeps (decisions).
func (r *inputResolver) condition(f *field, key string) bool {
	raw, ok := f.def[key]
	if !ok {
		return false
	}
	v := raw
	marked := false
	loc := appendPath(f.loc, key)
	if _, isString := raw.(string); isString {
		var err error
		if v, marked, err = r.evalAt(loc, f.index); err != nil {
			if u, ok := errors.AsType[*expr.UnknownError](err); ok {
				r.reportAt(problems.InputConditionNotKnown, loc,
					problems.Params{"key": key, "input": f.label(), "name": u.Name})
			} else {
				r.reportError(loc, 0, maskedIf(marked, err))
			}
			return false
		}
	}
	decided := conditionValue(v)
	if marked {
		pointer := jsonptr.Format(loc)
		if r.decided == nil {
			r.decided = map[string]bool{}
		}
		if _, ok := r.decided[pointer]; !ok {
			r.decisions = append(r.decisions, pointer)
		}
		r.decided[pointer] = decided
	}
	return decided
}

// conditionValue is what a condition whose value is v decides: true if v
// is true or the text "true".
func conditionValue(v any) bool {
	switch v := v.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true")
	}
	return false
}

// noteIgnored notes that f is ignored, by its name, for the secret record
// (Record.Ignored), as the run is created.
func (r *inputResolver) noteIgnored(f *field) {
	if r.start == nil {
		r.ignored = append(r.ignored, plan.JoinPath(slices.Concat([]string{"inputs"}, f.path)...))
	}
}

// drop removes the values of an ignored input, and of the inputs it holds.
// As the run is created, it keeps each value it removes, as it was given,
// for the conditions Start decides again (ignoredValues).
func (r *inputResolver) drop(f *field) {
	if !f.container || !f.flatten {
		if v, ok := jsonptr.Lookup(r.values, f.path); ok && r.start == nil {
			if r.ignoredValues == nil {
				r.ignoredValues = map[string]any{}
			}
			r.ignoredValues[plan.JoinPath(slices.Concat([]string{"inputs"}, f.path)...)] = jsonvalue.Clone(v)
		}
		r.remove(f.path)
		return
	}
	for _, c := range f.children {
		r.drop(c)
	}
}
