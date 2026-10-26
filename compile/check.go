package compile

import (
	"errors"
	"reflect"
	"slices"
	"strings"

	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// checkValues checks each value against its input's type, which reads it
// as it was submitted, text for a number among them (readAs), takes what
// the Inputs resolver gave for a platform's type (resolvePlatform), and
// puts password
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
	for _, fields := range [][]*field{r.fields, r.liveRows()} {
		for _, f := range fields {
			if f.header || f.holder().ignored {
				continue
			}
			if f.secretWhole {
				r.secretsLater[plan.JoinPath(f.path...)] = true
			}
			if f.container || f.in != nil && f.typ != "password" {
				continue // A row's other fields aren't checked.
			}
			later, known := f.deferred || f.variable, f.variable
			if f.in != nil {
				later, known = f.evaluatedLater()
			}
			if later && f.typ == "password" {
				r.secretsLater[plan.JoinPath(f.path...)] = true
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
// what isn't known yet: the rest of the value is known now, so it must be
// JSON, as a value that is known must, and is reported now rather than as
// the run starts. A part that isn't is reported at the innermost field of
// the input's rows that holds it. What the parts that read a variable or a
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
}

// readsPrivate reports whether an expression in f's definition, in its
// rows' and its items' among them, reads an organization or user variable,
// or an input that holds a password or reads one: a value given in its
// place, as a list given as text whose rows' defaults read a variable, can
// be what it would have made, which a problem with it mustn't show.
func (r *inputResolver) readsPrivate(f *field) bool {
	exprs, err := r.workflow.Exprs(f.loc)
	if err != nil {
		return true
	}
	for _, e := range exprs {
		for _, ref := range e.Refs() {
			switch {
			case ref.Context == "org" || ref.Context == "vars":
				return true
			case ref.Context != "inputs":
			case unnamed(ref.Path):
				// Any input, a password among them.
				if slices.ContainsFunc(r.fields, func(g *field) bool { return holdsPassword(g.def) || g.variable || g.deferred }) {
					return true
				}
			default:
				for n := len(ref.Path); n > 0; n-- {
					if g := r.fieldAt(ref.Path[:n]); g != nil {
						if holdsPassword(g.def) || g.variable || g.deferred || r.marked(g.path) {
							return true
						}
						break
					}
				}
			}
		}
	}
	return false
}

// checkField checks f's value, and moves it to the secrets if f is a
// password, unless later is set: the value is evaluated again as the run
// starts. A value given for a password is never kept, even one of the
// wrong type. A password's value that is checked stays among the values,
// for what the compiler evaluates, which reads it as it reads a variable,
// but isn't among those the plan keeps (kept).
func (r *inputResolver) checkField(f *field, later bool) {
	v, ok := jsonptr.Lookup(r.values, f.path)
	if !ok || f.failed {
		// What the Inputs resolver couldn't give was reported as it was
		// asked (resolvePlatform).
		return
	}
	if v == nil {
		// A password's null is a value, as any input's is, which the
		// secret record holds, as the plan holds another input's.
		if f.typ == "password" && !later {
			r.secrets[plan.JoinPath(f.path...)] = nil
		}
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
		if f.variable || password || r.marked(f.path) || r.readsPrivate(f) {
			bad.params = mask(bad.params)
		}
		r.reportAt(bad.kind, at.loc, bad.params)
		if !password && bad.kind != problems.InputNotJSON {
			return
		}
		r.remove(f.path)
		return
	case f.typ == "password" && !later:
		r.secrets[plan.JoinPath(f.path...)] = out
	}
	if given := readAs(f, v); given != "" && r.submittedAs(f.path, v) {
		typ := f.typ
		if typ == "" {
			typ = "string"
		}
		r.reportAt(problems.InputReadAsSubmitted, f.loc, problems.Params{"input": f.label(), "type": typ, "given": given})
	}
	r.set(f.path, out)
}

// readAs returns what v, the value of f, is read as when it isn't of f's
// type, which format 1 reads as it was submitted, as ACTIVATE does: text,
// for a number or a boolean input given text, as "5" or "true", and a
// number or a boolean, for a text input given one. It returns "" for any
// other value.
func readAs(f *field, v any) string {
	_, isText := v.(string)
	switch {
	case platformTypes[f.typ] || f.typ == "list" || f.repeats() || f.typ == "multi-dropdown" || f.typ == "dropdown":
		return ""
	case f.typ == "number" || f.typ == "boolean":
		if isText {
			return "text"
		}
		return ""
	}
	if _, isBool := v.(bool); isBool {
		return "boolean"
	}
	if _, ok := toNumber(v); ok && !isText {
		return "number"
	}
	return ""
}

// submittedAs reports whether v, the value at path in the inputs' values,
// is the value submitted there, as Create checks it: not a default, nor
// what an expression a form sent the text of gave.
func (r *inputResolver) submittedAs(path []string, v any) bool {
	if r.start != nil {
		return false
	}
	given, ok := jsonptr.Lookup(r.submitted, path)
	return ok && reflect.DeepEqual(given, v)
}

// resolvePlatform asks the Inputs resolver for the value of f, an input of
// a platform's type or a field of one in a list's rows, as soon as the
// value is known: the value submitted, its default, or what its default
// gave. So what reads it, the other inputs' defaults and conditions, the
// workflow's env: and the jobs, reads what the resolver gave, as a
// cluster's details for its name, as ACTIVATE resolves a run's resources
// before it evaluates any of its expressions. A value that is evaluated as
// the run starts is asked for then. What is wrong with the value is
// reported at once, whether or not the input is ignored, as ACTIVATE fails
// the run; what reads it then reads it as it was. A value that holds a part
// made from a variable or a password is marked whole, as what the resolver
// gave can't be split by the parts that made it. A value with a part that
// means the expression a form made it from, which compare evaluates as the
// run starts, is asked for then, with what the expression gives in the
// part's place (meansLater), never as it was submitted.
func (r *inputResolver) resolvePlatform(f *field) {
	if !platformTypes[f.typ] || f.resolved || f.deferred || f.resolveLater {
		return
	}
	if r.meansLater(f) {
		f.resolveLater = true
		return
	}
	r.askPlatformValue(f)
}

// askPlatformValue asks the Inputs resolver for f's value, as
// resolvePlatform says, and puts what it gives in its place.
func (r *inputResolver) askPlatformValue(f *field) {
	v, ok := jsonptr.Lookup(r.values, f.path)
	if !ok || v == nil {
		return
	}
	f.resolved = true
	out, bad := r.askPlatform(f, v)
	if bad.kind != nil {
		f.failed = true
		bad.params["input"] = f.label()
		if f.variable || r.marked(f.path) || r.readsPrivate(f) {
			bad.params = mask(bad.params)
		}
		r.reportAt(bad.kind, f.loc, bad.params)
		return
	}
	f.given = v
	if r.marked(f.path) {
		secret := r.final && r.markedSecret(f.path)
		r.mark(f.path)
		if secret {
			r.markSecret(f.path)
		}
	}
	r.set(f.path, out)
}

// askPlatform asks the Inputs resolver for v, the value of f, an input of a
// platform's type, and returns what it gave, or what is wrong with v: a
// finding whose params the caller adds the input to.
func (r *inputResolver) askPlatform(f *field, v any) (any, finding) {
	wrong := func(k *problems.Kind, params problems.Params) (any, finding) {
		if params == nil {
			params = problems.Params{}
		}
		return nil, finding{k, params}
	}
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
		if k, params, ok := workflowProblem(err); ok {
			return wrong(k, params)
		}
		return wrong(problems.InputRejected, problems.Params{"reason": err.Error()})
	}
	if what, bad, _ := notJSON(out); what != "" {
		return wrong(problems.PlatformValueNotJSON, problems.Params{"what": what, "value": bad})
	}
	return out, finding{}
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
	case platformTypes[f.typ] && f.resolved:
		// The Inputs resolver gave it as soon as it was known.
		return v, finding{}
	case platformTypes[f.typ]:
		return r.askPlatform(f, v)
	case f.typ == "number":
		// Text that reads as a number passes, and stays text: format 1
		// reads a value as it was submitted (readAs).
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
		if _, isText := v.(string); isText {
			return v, finding{}
		}
		return number(n), finding{}
	case f.typ == "boolean":
		switch b := v.(type) {
		case bool:
			return b, finding{}
		case string:
			switch strings.ToLower(strings.TrimSpace(b)) {
			case "true", "false":
				return v, finding{}
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
	// Text: string, password, editor, color-picker and the like, which a
	// number or a boolean is given for as it is (readAs).
	switch v.(type) {
	case map[string]any, []any:
		return wrong(problems.InputNotText, problems.Params{"value": problems.Show(v)})
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
