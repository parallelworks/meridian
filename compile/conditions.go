package compile

import (
	"errors"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// applyConditions evaluates each input's hidden, ignore and optional, drops
// the values of ignored inputs, and reports required inputs with no value.
func (r *inputResolver) applyConditions() {
	for _, f := range r.fields {
		if f.group != nil && f.group.ignored {
			f.ignored = true
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
// keeps (decisions).
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
		r.decisions = append(r.decisions, plan.Decision{Path: jsonptr.Format(loc), Value: decided})
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

// drop removes the values of an ignored input, and of the inputs it holds.
func (r *inputResolver) drop(f *field) {
	if !f.container || !f.flatten {
		r.remove(f.path)
		return
	}
	for _, c := range f.children {
		r.drop(c)
	}
}
