package compile

import (
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// checkValues checks each value against its input's type, converts numbers
// and booleans given as text, resolves platform types, and puts password
// values among the secrets: a password input's, and a password field's in
// a list's or a repeated step's rows, which is checked as an input's is,
// though the rows' other fields aren't. A value that isn't JSON, which a
// plan can't hold, such as a default that gives NaN, is reported and
// dropped, as a submitted one is. The value of an input that reads a
// variable or a password is checked too, and kept for what the compiler
// evaluates, but what is wrong with it is reported without it, as is what
// is wrong with a password's. A password whose value is evaluated as the
// run starts isn't among the secrets, but the run's secrets name it. Of an
// input whose value is evaluated as the run starts because a part of it
// reads what isn't known yet, what the compiler knows is checked to be
// JSON (checkKnown).
func (r *inputResolver) checkValues() {
	// passwords holds the password fields of each input's rows, once an
	// input is checked with checkKnown.
	var passwords map[*field][]*field
	for _, fields := range [][]*field{r.fields, r.rows} {
		for _, f := range fields {
			if f.header || f.holder().ignored {
				continue
			}
			if f.secretWhole {
				r.secretsLater[strings.Join(f.path, ".")] = true
			}
			if f.container || f.in != nil && f.typ != "password" {
				continue // A row's other fields aren't checked.
			}
			later, known := f.deferred || f.variable, f.variable
			if f.in != nil {
				later, known = f.evaluatedLater()
			}
			if later && f.typ == "password" {
				r.secretsLater[strings.Join(f.path, ".")] = true
			}
			switch {
			case !later || known:
				r.checkField(f, later)
			case f.in == nil:
				if passwords == nil {
					passwords = map[*field][]*field{}
					for _, c := range r.passwords {
						if c.in != nil {
							passwords[c.in] = append(passwords[c.in], c)
						}
					}
				}
				r.checkKnown(f, passwords[f])
			}
		}
	}
}

// checkKnown checks what the compiler knows of the value of f, an input
// whose value is evaluated as the run starts because a part of it reads
// what isn't known yet: the plan holds the rest of the value as it is, so
// it must be JSON, as a value that is known must. A part that isn't is
// reported at the innermost field of the input's rows that holds it, and
// the input has no value to hold. What the parts that read a variable or a
// password gave isn't shown, and the input's passwords, the password
// fields of its rows, aren't checked here.
func (r *inputResolver) checkKnown(f *field, passwords []*field) {
	known := jsonvalue.Clone(lookup(r.values, f.path))
	for _, p := range f.later {
		if !p.variable {
			if len(p.path) == 0 {
				return
			}
			setIn(known, p.path, nil)
		}
	}
	for _, c := range passwords {
		setIn(known, c.path[len(f.path):], nil)
	}
	what, value, at := notJSON(known)
	if what == "" {
		return
	}
	params := problems.Params{"what": what, "value": value}
	variable := false
	for _, p := range f.partsAround(at) {
		variable = variable || p.variable && len(p.path) <= len(at)
	}
	if variable || holdsPassword(f.def) || r.marked(slices.Concat(f.path, at)) {
		params = mask(params)
	}
	in := r.innermostRow(f, slices.Concat(f.path, at))
	params["input"] = in.label()
	r.reportAt(problems.InputNotJSON, in.loc, params)
	f.dflt = plan.Value{}
}

// checkField checks f's value, and moves it to the secrets if f is a
// password, unless later is set: the value is evaluated again as the run
// starts. A value given for a password is never kept, even one of the
// wrong type. A password's value that is checked stays among the values,
// for what the compiler evaluates, which reads it as it reads a variable,
// but isn't among those the plan keeps (kept).
func (r *inputResolver) checkField(f *field, later bool) {
	v, ok := jsonptr.Lookup(r.values, f.path)
	if !ok || v == nil {
		return
	}
	password := holdsPassword(f.def)
	out, bad := r.checkValue(f, v)
	at := f
	if bad.kind == nil {
		at, bad = r.checkJSONValue(f, out)
	}
	switch {
	case bad.kind != nil:
		bad.params["input"] = at.label()
		if f.variable || password || r.marked(f.path) {
			bad.params = mask(bad.params)
		}
		r.reportAt(bad.kind, at.loc, bad.params)
		if !password && bad.kind != problems.InputNotJSON {
			return
		}
		if bad.kind == problems.InputNotJSON && f.in == nil {
			// Nor can the plan hold what is evaluated as the run starts.
			f.dflt = plan.Value{}
		}
		r.remove(f.path)
		return
	case f.typ == "password" && !later:
		r.secrets[strings.Join(f.path, ".")] = out
	}
	r.set(f.path, out)
}

// checkValue checks v, the value of f, and returns it as the run sees it, or
// what is wrong with it: a finding whose params the caller adds the input
// to.
func (r *inputResolver) checkValue(f *field, v any) (any, finding) {
	wrong := func(k *problems.Kind, params problems.Params) (any, finding) {
		if params == nil {
			params = problems.Params{}
		}
		return nil, finding{k, params}
	}
	switch {
	case platformTypes[f.typ]:
		if r.opts.Inputs == nil {
			return wrong(problems.PlatformInput, problems.Params{"type": f.typ})
		}
		out, err := r.opts.Inputs.ResolveInput(f.typ, f.def, v)
		switch {
		case errors.Is(err, ErrInputType):
			return wrong(problems.PlatformInput, problems.Params{"type": f.typ})
		case err != nil:
			// A problem of one of the kinds of error a workflow can have,
			// foundation's shared rules among them, is reported as itself, so
			// that each reader sees it in their own language. Any other error,
			// a warning's kind among them, which would let the value through,
			// or the command's own, is the platform's reason, as it gives it.
			if p, ok := errors.AsType[*problem.Problem](err); ok {
				_, command := problems.CommandRegistry.Lookup(p.Code)
				if k, ok := problems.Lookup(p.Code); ok && !command && k.Severity() == problems.Error {
					return wrong(k, maps.Clone(p.Params))
				}
			}
			return wrong(problems.InputRejected, problems.Params{"reason": err.Error()})
		}
		if what, bad, _ := notJSON(out); what != "" {
			return wrong(problems.PlatformValueNotJSON, problems.Params{"what": what, "value": bad})
		}
		return out, finding{}
	case f.typ == "number":
		n, ok := toNumber(v)
		if !ok {
			return wrong(problems.InputNotNumber, problems.Params{"value": problems.Show(v)})
		}
		if lo, ok := toNumber(f.def["min"]); ok && n < lo {
			return wrong(problems.InputBelowMin, problems.Params{"value": formatNumber(n), "min": formatNumber(lo)})
		}
		if hi, ok := toNumber(f.def["max"]); ok && n > hi {
			return wrong(problems.InputAboveMax, problems.Params{"value": formatNumber(n), "max": formatNumber(hi)})
		}
		return number(n), finding{}
	case f.typ == "boolean":
		switch v := v.(type) {
		case bool:
			return v, finding{}
		case string:
			switch strings.ToLower(strings.TrimSpace(v)) {
			case "true":
				return true, finding{}
			case "false":
				return false, finding{}
			}
		}
		return wrong(problems.InputNotBoolean, problems.Params{"value": problems.Show(v)})
	case f.typ == "list" || f.repeats():
		if _, ok := v.([]any); !ok {
			return wrong(problems.InputNotList, problems.Params{"value": problems.Show(v)})
		}
		return v, finding{}
	case f.typ == "multi-dropdown":
		list, ok := v.([]any)
		if !ok {
			return wrong(problems.InputNotOptionList, problems.Params{"value": problems.Show(v)})
		}
		for _, item := range list {
			if bad := checkOption(f, item); bad.kind != nil {
				return nil, bad
			}
		}
		return v, finding{}
	case f.typ == "dropdown":
		if bad := checkOption(f, v); bad.kind != nil {
			return nil, bad
		}
		return v, finding{}
	}
	// Text: string, password, editor, color-picker and the like.
	switch v := v.(type) {
	case string:
		return v, finding{}
	case bool:
		return strconv.FormatBool(v), finding{}
	case map[string]any, []any:
		return wrong(problems.InputNotText, problems.Params{"value": problems.Show(v)})
	}
	if n, ok := toNumber(v); ok {
		return formatNumber(n), finding{}
	}
	return v, finding{}
}

// checkJSONValue checks that v, the value of f as the run sees it, is JSON:
// an expression can give NaN or an infinity, as 0/0 and 1/0 do, which a
// run's values can't hold. It returns what is wrong, and the field it is
// at: the innermost field of f's rows that holds the part that isn't JSON,
// or else f.
func (r *inputResolver) checkJSONValue(f *field, v any) (*field, finding) {
	what, value, at := notJSON(v)
	if what == "" {
		return f, finding{}
	}
	return r.innermostRow(f, slices.Concat(f.path, at)), finding{problems.InputNotJSON, problems.Params{"what": what, "value": value}}
}

// innermostRow returns the innermost field of lists' rows whose value holds
// the value at path, a path in f's value, or f if none does. Of fields with
// the same path, it returns the first.
func (r *inputResolver) innermostRow(f *field, path []string) *field {
	in := f
	var walk func(g *field)
	walk = func(g *field) {
		if len(g.path) >= len(path) {
			return
		}
		i, ok := rowIndex(path[len(g.path)], len(g.rows))
		if !ok {
			return
		}
		for _, c := range g.rows[i] {
			if len(c.path) > len(path) || !slices.Equal(c.path, path[:len(c.path)]) {
				continue
			}
			if len(c.path) > len(in.path) || in != f && len(c.path) == len(in.path) && c.seq < in.seq {
				in = c
			}
			walk(c)
		}
	}
	for g := range r.fieldsAt().Reached(path) {
		if len(g.path) <= len(path) && related(g.path, path) {
			walk(g)
		}
	}
	return in
}

// checkOption checks that v is one of a dropdown's options, when they are
// written in the workflow rather than computed: that its looseKey is one of
// theirs.
func checkOption(f *field, v any) finding {
	o := f.findOptions()
	if len(o.values) == 0 || o.template {
		return finding{}
	}
	if o.keys == nil {
		o.keys = make(map[string]bool, len(o.values))
		for _, value := range o.values {
			o.keys[looseKey(value)] = true
		}
	}
	if o.keys[looseKey(v)] {
		return finding{}
	}
	shown := make([]string, len(o.values))
	for i, value := range o.values {
		shown[i] = problems.Show(value)
	}
	return finding{problems.InputNotOption, problems.Params{"value": problems.Show(v), "options": problems.List(shown)}}
}
