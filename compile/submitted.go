package compile

import (
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/internal/hint"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/problems"
)

// checkJSON reports, and drops, submitted values that aren't JSON values,
// which a plan can't hold.
func (r *inputResolver) checkJSON(values map[string]any) {
	for key, v := range jsonvalue.Sorted(values) {
		if what, bad, _ := notJSON(v); what != "" {
			r.reportAt(problems.InputNotJSON, definitionsPath, problems.Params{"input": key, "what": what, "value": bad})
			delete(values, key)
		}
	}
}

// checkUnknownInputs reports the submitted values for inputs the workflow
// doesn't define, and for the inputs of a list's or a repeated step's
// rows that its rows don't have: the first maxListed one by one, which
// suggest the name each probably meant, and then how many more there are.
// The values are kept.
func (r *inputResolver) checkUnknownInputs() {
	r.checkUnknown(r.values, nil, nil, knownInputsOf(r.fields), false)
	if more := r.unknown - maxListed; more > 0 {
		r.reportAt(problems.ProblemsNotListed, definitionsPath, problems.Params{
			"code": string(problems.UnknownInput.Code()), "count": more, "limit": maxListed,
		})
	}
}

// knownInputsOf returns fields, inputs, by the paths of the mappings
// their values are in.
func knownInputsOf(fields []*field) map[string]*knownInputs {
	known := map[string]*knownInputs{}
	for _, f := range fields {
		if !f.container || !f.flatten {
			at := jsonptr.Format(f.path[:len(f.path)-1])
			k := known[at]
			if k == nil {
				k = &knownInputs{byKey: map[string]*field{}}
				known[at] = k
			}
			k.byKey[f.key] = f
			if defs, _ := f.rowInputs(); defs != nil {
				if k.rows == nil {
					k.rows = map[string][]*field{}
				}
				k.rows[f.key] = append(k.rows[f.key], f)
			}
		}
	}
	return known
}

// knownInputs are the inputs whose values are in a mapping, by their keys,
// and their names, sorted, and those of the password inputs and of the
// inputs with a password among theirs, once a value there isn't one of
// them. rows holds, by their keys, the lists and repeated steps there, more
// than one where inputs share a path, as a wizard step's can, and
// rowInputs the inputs of their rows, by key, once a row is checked. In a
// row, defaulted holds the keys there that aren't inputs' but that the
// default rows written in the workflow have, which a form sends back, with
// the values the default rows have at each.
type knownInputs struct {
	byKey                     map[string]*field
	rows                      map[string][]*field
	rowInputs                 map[string]map[string]*knownInputs
	defaulted                 map[string][]any
	names, passwords, holders []string
}

// isDefault reports whether v, a value submitted for key, which isn't an
// input's, is one that the default rows have at key, written in the
// workflow, as a form sends it back.
func (k *knownInputs) isDefault(key string, v any) bool {
	return slices.ContainsFunc(k.defaulted[key], func(d any) bool { return sameValue(v, d) })
}

// rowsOf returns the inputs of the rows of the lists and repeated steps at
// key, those of each one's rows, by the paths in a row of the mappings
// their values are in, with the keys that aren't inputs' that their
// default rows have.
func (k *knownInputs) rowsOf(r *inputResolver, key string) map[string]*knownInputs {
	if known, ok := k.rowInputs[key]; ok {
		return known
	}
	var fields []*field
	for _, f := range k.rows[key] {
		defs, loc := f.rowInputs()
		fields = append(fields, r.template(defs, loc).fields...)
	}
	known := knownInputsOf(fields)
	for _, f := range k.rows[key] {
		rows, _ := f.def["default"].([]any)
		for _, row := range rows {
			addDefaulted(known, row, nil)
		}
	}
	if k.rowInputs == nil {
		k.rowInputs = map[string]map[string]*knownInputs{}
	}
	k.rowInputs[key] = known
	return known
}

// addDefaulted adds to known, the inputs of rows by the paths in a row of
// the mappings their values are in, the keys of v, a default row or a
// group's mapping in one at rel, that aren't inputs' there, and those in
// its groups' mappings (knownInputs.defaulted).
func addDefaulted(known map[string]*knownInputs, v any, rel []string) {
	m, ok := v.(map[string]any)
	if !ok {
		return
	}
	at := jsonptr.Format(rel)
	k := known[at]
	if k == nil {
		k = &knownInputs{byKey: map[string]*field{}}
		known[at] = k
	}
	for key, item := range m {
		switch f, ok := k.byKey[key]; {
		case !ok:
			if k.defaulted == nil {
				k.defaulted = map[string][]any{}
			}
			k.defaulted[key] = append(k.defaulted[key], item)
		case f.container && !f.flatten:
			addDefaulted(known, item, appendPath(rel, key))
		}
	}
}

// holdsOnly reports whether values holds only values of k's inputs that
// hold no others' values for checkUnknown to check, as most rows do: no
// group's or step's mapping, and no list's or repeated step's rows.
func (k *knownInputs) holdsOnly(values map[string]any) bool {
	if k == nil {
		return len(values) == 0
	}
	for key, v := range values {
		f, ok := k.byKey[key]
		if !ok || v != nil && (f.container && !f.flatten || len(k.rows[key]) > 0) {
			return false
		}
	}
	return true
}

// checkUnknown reports submitted values for inputs the workflow doesn't
// define, in values, which are at path in the values and, in a row of a
// list or a repeated step, at rel in the row, or else at path; known holds
// the inputs there, the workflow's or a row's, by the paths of the
// mappings they are in. A value that names a password (namesPassword) is
// taken for one: it is kept among the secrets, and not in the plan. A
// group's value that isn't a mapping is an error, but in a row, where it
// is taken as it was given, and kept as a secret if it can hold a
// password (collectRows).
func (r *inputResolver) checkUnknown(values map[string]any, path, rel []string, known map[string]*knownInputs, row bool) {
	inputs := known[jsonptr.Format(rel)]
	if inputs.holdsOnly(values) {
		return
	}
	if inputs == nil {
		inputs = &knownInputs{}
	}
	for key, v := range jsonvalue.Sorted(values) {
		f, ok := inputs.byKey[key]
		if !ok {
			at := appendPath(path, key)
			// A value that the default rows have at key, written in the
			// workflow, is the workflow's, not a secret.
			if r.namesPassword(key, v, inputs) && !inputs.isDefault(key, v) {
				r.secrets[strings.Join(at, ".")] = v
				r.unknownSecrets = append(r.unknownSecrets, at)
				r.mark(at)
			}
			if _, ok := inputs.defaulted[key]; ok {
				// The workflow's default rows have it, which a form sends
				// back, and compiling reports it there, once
				// (checkDefaultRows).
				continue
			}
			if r.unknown++; r.unknown > maxListed {
				continue
			}
			if inputs.names == nil {
				inputs.names = slices.Sorted(maps.Keys(inputs.byKey))
			}
			s, ok := hint.Suggest(key, inputs.names)
			params := problems.Params{"input": strings.Join(at, ".")}
			r.reportAt(problems.UnknownInput, definitionsPath, problems.WithSuggestion(params, strings.Join(appendPath(path, s), "."), ok))
			continue
		}
		if rows, ok := v.([]any); ok && len(inputs.rows[key]) > 0 {
			rowInputs := inputs.rowsOf(r, key)
			for i, item := range rows {
				if m, ok := item.(map[string]any); ok && !rowInputs[""].holdsOnly(m) {
					r.checkUnknown(m, appendPath(path, key, strconv.Itoa(i)), nil, rowInputs, true)
				}
			}
		}
		if !f.container || f.flatten || v == nil {
			continue
		}
		child, ok := v.(map[string]any)
		switch {
		case ok:
			r.checkUnknown(child, appendPath(path, key), appendPath(rel, key), known, row)
		case !row:
			params := problems.Params{"input": f.label(), "value": problems.Show(v)}
			if holdsPassword(f.def) {
				params = mask(params)
			}
			r.reportAt(problems.InputGroupNotMapping, f.loc, params)
			delete(values, key)
		}
	}
}

// namesPassword reports whether v, a value submitted for an input the
// workflow doesn't define, named key, among the inputs, is a password's: if
// key is a password field's name, wherever the workflow defines one, as a
// repeated step's password sent beside the step is; or can be a secret's
// (namesSecret), as one sent for a password field that was renamed can;
// or is close to the name of a password input among them, as a
// misspelling is; or v is a mapping or a list, and key is close to the
// name of one with a password among its inputs, or v holds a key that is a
// password field's name or can be a secret's, as a flattened wizard step's
// inputs sent under its name do.
func (r *inputResolver) namesPassword(key string, v any, inputs *knownInputs) bool {
	if r.passwordKeys == nil {
		r.passwordKeys = map[string]bool{}
		defs, _ := lookup(r.doc.Value, definitionsPath).(map[string]any)
		passwordKeys(defs, r.passwordKeys)
	}
	if r.passwordKeys[key] || namesSecret(key) || r.holdsPasswordKey(v) {
		return true
	}
	if inputs.passwords == nil {
		inputs.passwords, inputs.holders = []string{}, []string{}
		for name, f := range jsonvalue.Sorted(inputs.byKey) {
			switch {
			case f.typ == "password":
				inputs.passwords = append(inputs.passwords, name)
			case holdsPassword(f.def):
				inputs.holders = append(inputs.holders, name)
			}
		}
	}
	if _, ok := hint.Suggest(key, inputs.passwords); ok {
		return true
	}
	switch v.(type) {
	case map[string]any, []any:
		_, ok := hint.Suggest(key, inputs.holders)
		return ok
	}
	return false
}

// holdsPasswordKey reports whether v holds, at any depth, a key that is a
// password field's name, wherever the workflow defines one, or that can be
// a secret's (namesSecret).
func (r *inputResolver) holdsPasswordKey(v any) bool {
	switch v := v.(type) {
	case map[string]any:
		for k, item := range v {
			if r.passwordKeys[k] || namesSecret(k) || r.holdsPasswordKey(item) {
				return true
			}
		}
	case []any:
		return slices.ContainsFunc(v, r.holdsPasswordKey)
	}
	return false
}

// passwordKeys adds to out the names of the password inputs that defs,
// input definitions, define, and those among their inputs.
func passwordKeys(defs map[string]any, out map[string]bool) {
	for name, v := range defs {
		def, ok := v.(map[string]any)
		if !ok || strings.HasPrefix(name, "$") {
			continue
		}
		if def["type"] == "password" {
			out[name] = true
		}
		for _, key := range []string{"items", "options", "template"} {
			if inner, ok := def[key].(map[string]any); ok {
				passwordKeys(inner, out)
			}
		}
	}
}
