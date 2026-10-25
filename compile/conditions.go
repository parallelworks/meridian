package compile

import (
	"errors"
	"maps"
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
//
// What a condition that read a variable or a password decided mustn't show
// in the plan, which can be shown to whoever can see the run. So an input
// whose ignore read one, and each input in a group whose did (shaped), has
// its value held, whatever the ignore decided: once the conditions have
// read it as dropped, it is put back, kept out of the plan, in the secret
// record (holdShaped), and Start drops it if the ignore decides so again.
// The conditions of the inputs in such a group are each a decision of the
// run's shape, whatever they read, and evaluated even if the group is
// ignored, so the plan names the same ones either way. Whether an input
// whose ignore, hidden or optional read one, or a group's around it did,
// is required, Start decides (shapeDecided).
func (r *inputResolver) applyConditions() {
	var shaped, ignored []*field
	var restore []heldValue
	for _, f := range r.fields {
		g := f.group
		if g != nil && g.ignored && !g.ignoredLater {
			f.ignored = true
			r.noteIgnored(f)
			r.refresh(f.path)
			continue
		}
		inShaped := g != nil && g.shaped
		if g != nil && g.ignored {
			// The group's ignore read a variable or a password: what
			// expressions read of f is that it is ignored, as Start reads
			// it once it drops the group, while its conditions are
			// decided all the same.
			f.ignored = true
			r.refresh(f.path)
		}
		hidden, _ := r.condition(f, "hidden", inShaped)
		f.hidden = hidden || g != nil && g.hidden
		ignore, private := r.condition(f, "ignore", inShaped)
		f.shaped = inShaped || private
		if f.ignored = ignore || g != nil && g.ignored; f.ignored {
			// What expressions read of f changes, before its value is
			// dropped.
			r.refresh(f.path)
		}
		// An ignore that is no decision Start makes again, as true written
		// out is, the compiler applies.
		f.ignoredLater = f.ignored && (!ignore || f.ignoreDecides)
		f.optional, _ = r.condition(f, "optional", inShaped)
		switch {
		case f.ignoredLater:
			restore = r.dropKept(f, restore)
		case f.ignored:
			r.noteIgnored(f)
			r.drop(f)
			ignored = append(ignored, f)
		}
		if private && !inShaped {
			shaped = append(shaped, f)
			// What reads f from here on reads what a variable or a
			// password decided, as a later condition that reads it does,
			// which is then a decision too.
			r.markFields(f)
		}
	}
	later := false
	for _, f := range r.fields {
		if f.ignoredLater {
			f.ignored, later = false, true
		}
	}
	if later {
		// The values go back, and what expressions read of the values is
		// made again, at once, in the map the evaluators' scope holds.
		for _, v := range restore {
			setIn(r.values, v.path, v.v)
		}
		r.evaluators = nil
		if r.view != nil {
			fresh := r.evalValues()
			clear(r.view)
			maps.Copy(r.view, fresh)
			if r.checkView != nil {
				r.checkView()
			}
		}
	}
	// What is put back can hold an input the compiler ignored, which stays
	// dropped.
	for _, f := range ignored {
		if _, ok := jsonptr.Lookup(r.values, f.path); ok {
			r.drop(f)
		}
	}
	for _, f := range shaped {
		r.holdShaped(f)
	}
	r.heldParts()
	for _, f := range r.fields {
		// A repeated step with no value has no rows.
		if f.container || f.header || f.ignored || f.hasDefault || f.deferred || f.repeats() || !r.missing(f) || shapeDecided(f) {
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
// if it is true or the text "true", and reports whether it read a variable
// or a password. One that did, or one that is an expression in a group
// whose ignore did (always), is a decision of the run's shape that Start
// makes again, which the plan names, and the secret record keeps
// (decisions).
func (r *inputResolver) condition(f *field, key string, always bool) (decided, private bool) {
	raw, ok := f.def[key]
	if !ok {
		return false, false
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
			return false, marked
		}
	}
	decided = conditionValue(v)
	if _, isString := raw.(string); marked || always && isString {
		pointer := jsonptr.Format(loc)
		if r.decided == nil {
			r.decided = map[string]bool{}
		}
		if _, ok := r.decided[pointer]; !ok {
			r.decisions = append(r.decisions, pointer)
		}
		r.decided[pointer] = decided
		switch key {
		case "hidden":
			f.hiddenDecides = true
		case "ignore":
			f.ignoreDecides = true
		case "optional":
			f.optionalDecides = true
		}
	}
	return decided, marked
}

// A heldValue is a value the inputs' values had at path.
type heldValue struct {
	path []string
	v    any
}

// dropKept drops f's value, as drop does, and appends to restore what it
// dropped, to put back once the conditions have read it as dropped.
func (r *inputResolver) dropKept(f *field, restore []heldValue) []heldValue {
	if !f.container || !f.flatten {
		if v, ok := jsonptr.Lookup(r.values, f.path); ok {
			restore = append(restore, heldValue{f.path, jsonvalue.Clone(v)})
		}
		r.remove(f.path)
		return restore
	}
	for _, c := range f.children {
		restore = r.dropKept(c, restore)
	}
	return restore
}

// markFields marks f's value, as mark does, or those of the inputs a
// wizard step whose inputs' values are beside its own holds.
func (r *inputResolver) markFields(f *field) {
	if f.container && f.flatten {
		for _, c := range f.children {
			r.markFields(c)
		}
		return
	}
	r.mark(f.path)
}

// holdShaped keeps the value of f, an input whose ignore read a variable
// or a password, out of the plan, whatever the ignore decided: as derived,
// as a secret's would be, if the ignore read an input, which can be a
// password, and as private if it read only variables.
func (r *inputResolver) holdShaped(f *field) {
	kind := heldPrivate
	if exprs, err := r.workflow.Exprs(appendPath(f.loc, "ignore")); err == nil && slices.ContainsFunc(exprs, func(e *expr.Expr) bool {
		return slices.ContainsFunc(e.Refs(), func(ref expr.Ref) bool { return ref.Context == "inputs" })
	}) {
		kind = heldDerived
	}
	var hold func(f *field)
	hold = func(f *field) {
		if f.container && f.flatten {
			for _, c := range f.children {
				hold(c)
			}
			return
		}
		r.holdPath(f.holder(), f.path, kind)
	}
	hold(f)
}

// shapeDecided reports whether Start decides whether f is required:
// whether its ignore, hidden or optional, or a group's around it's ignore
// or hidden, read a variable or a password.
func shapeDecided(f *field) bool {
	if f.optionalDecides {
		return true
	}
	for g := f; g != nil; g = g.group {
		if g.shaped || g.hiddenDecides {
			return true
		}
	}
	return false
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
