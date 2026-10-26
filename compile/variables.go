package compile

import (
	"maps"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/hint"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// noteVariable notes in read the variable that isn't a secret that r, a
// reference of an expression the run evaluates (eachRead), reads, as
// org.<name> and vars.<name>. A read of every variable of a context, as
// org.* is, or of one an [index] chooses, is the context's name alone.
func (c *compiler) noteVariable(read map[string]bool, r expr.Ref) {
	switch {
	case r.Context != "org" && r.Context != "vars":
	case unnamed(r.Path):
		read[r.Context] = true
	case !c.isSecret(r.Context, r.Path[0]):
		read[plan.JoinPath(r.Context, r.Path[0])] = true
	}
}

// checkVariableValues warns about each variable the run reads that the
// caller's contexts give a value that isn't text (Options.Contexts): a
// variable's value is text, which the Variables interface gives as the run
// starts, so the run reads one that isn't as "", as Create does.
func (c *compiler) checkVariableValues(p *plan.Plan) {
	seen := map[string]bool{}
	for _, name := range p.Run.Variables {
		path := plan.SplitPath(name)
		values := mapping(c.opts.Contexts[path[0]])
		names := path[1:]
		if len(names) == 0 {
			names = slices.Sorted(maps.Keys(values))
		}
		for _, n := range names {
			k := plan.JoinPath(path[0], n)
			v, ok := values[n]
			if !ok || seen[k] {
				continue
			}
			seen[k] = true
			switch v.(type) {
			case string, expr.Unknown:
				continue
			}
			c.reportPointer(problems.VariableNotText, problems.Position{}, "", problems.Params{"context": path[0], "name": n})
		}
	}
}

// checkVariableName warns about r, a reference at byte offset at in the
// string in, if it reads a variable that isn't a secret but whose name looks
// like a secret's. A variable's value isn't masked unless it is a secret, so
// a secret in a plain variable shows wherever it is written. Compile keeps
// one warning for each variable, where it is first read.
func (c *compiler) checkVariableName(r expr.Ref, in *spot, at int) {
	if r.Context != "org" && r.Context != "vars" || len(r.Path) == 0 {
		return
	}
	if name := r.Path[0]; looksSecret(name) && !c.isSecret(r.Context, name) {
		c.reportIn(in, at, problems.VariableLooksSecret, problems.Params{"context": r.Context, "name": name})
	}
}

// repeatedVariable returns a test of the diagnostics, in order, that is
// true of each warning about a variable that looks like a secret but the
// first.
func repeatedVariable() func(problems.Diagnostic) bool {
	seen := map[string]bool{}
	return func(d problems.Diagnostic) bool {
		if d.Code != problems.VariableLooksSecret.Code() {
			return false
		}
		k := d.Params["context"].(string) + "." + d.Params["name"].(string)
		if seen[k] {
			return true
		}
		seen[k] = true
		return false
	}
}

// textTypes are the input types whose value is text a person types, which
// can be a secret.
var textTypes = map[string]bool{"string": true, "editor": true}

// checkInputNames warns about each of fields, and each field of a list's or
// a repeated step's rows, that takes text and whose name looks like a
// secret's. Unlike a password input's, its value is kept with the run, and
// isn't masked.
func (c *compiler) checkInputNames(fields []*field) {
	for _, f := range fields {
		if textTypes[f.typ] && looksSecret(f.key) {
			c.reportAt(problems.InputLooksSecret, f.loc, problems.Params{"input": f.label()})
		}
		if defs, loc := f.rowInputs(); defs != nil {
			c.checkInputNames(c.inputs.collect(defs, loc, appendPath(f.path, "[index]"), nil))
		}
	}
}

// checkDefaultRows warns about each key of fields' default rows, those of
// each list among them and among its rows' inputs, that isn't one of its
// rows' inputs, where the workflow writes it: once, as a run doesn't
// report it when a form sends the default rows back (knownInputs.defaulted).
// A row's inputs are those of every list and repeated step at its list's
// path, as a run checks them, where inputs share a path, as a wizard
// step's can. A list among a row's inputs is named by its path with
// [index] for the row's, as input_looks_secret names one.
func (c *compiler) checkDefaultRows(fields []*field) {
	known := knownInputsOf(fields)
	for _, f := range fields {
		defs, loc := f.rowInputs()
		if defs == nil {
			continue
		}
		if rows, ok := f.def["default"].([]any); ok {
			known := known[jsonptr.Format(f.path[:len(f.path)-1])].rowsOf(c.inputs, f.key)
			for i, row := range rows {
				if m, ok := row.(map[string]any); ok {
					c.checkDefaultRow(f, m, appendPath(f.loc, "default", strconv.Itoa(i)), nil, known)
				}
			}
		}
		c.checkDefaultRows(c.inputs.collect(defs, loc, appendPath(f.path, "[index]"), nil))
	}
}

// checkDefaultRow warns about each key of values, a default row of f's, or
// a group's mapping in one, at loc in the workflow and at rel in the row,
// that isn't one of the inputs known holds there, or is the key of a group
// there whose inputs' values are beside it.
func (c *compiler) checkDefaultRow(f *field, values map[string]any, loc, rel []string, known map[string]*knownInputs) {
	inputs := known[jsonptr.Format(rel)]
	if inputs == nil {
		inputs = &knownInputs{}
	}
	for key, v := range jsonvalue.Sorted(values) {
		in, ok := inputs.byKey[key]
		if _, flat := inputs.flattened[key]; !ok && flat {
			c.reportAt(problems.FlattenedGroupValue, appendPath(loc, key), problems.Params{"input": strings.Join(appendPath(rel, key), "."), "source": "row"})
			continue
		}
		if !ok {
			if inputs.names == nil {
				inputs.names = slices.Sorted(maps.Keys(inputs.byKey))
			}
			s, ok := hint.Suggest(key, inputs.names)
			params := problems.Params{"input": f.label(), "key": strings.Join(appendPath(rel, key), ".")}
			c.reportAt(problems.DefaultRowUnknownInput, appendPath(loc, key), problems.WithSuggestion(params, strings.Join(appendPath(rel, s), "."), ok))
			continue
		}
		if child, ok := v.(map[string]any); ok && in.container && !in.flatten {
			c.checkDefaultRow(f, child, appendPath(loc, key), appendPath(rel, key), known)
		}
	}
}

// secretWords are the words that make a name look like a secret's, and
// secretPairs the pairs of words in a row that do, as in api_key.
var (
	secretWords = map[string]bool{
		"token": true, "password": true, "passwd": true, "pwd": true, "passphrase": true, "secret": true,
		"apikey": true, "privatekey": true, "accesskey": true, "credential": true, "credentials": true,
	}
	secretPairs = map[[2]string]bool{{"api", "key"}: true, {"private", "key"}: true, {"access", "key"}: true}
)

// looksSecret reports whether a name looks like a secret's: whether one of
// its words is one of secretWords, or two in a row are one of secretPairs. A
// word is whole, so tokenizer and secretary aren't.
func looksSecret(name string) bool {
	words := nameWords(name)
	for i, w := range words {
		if secretWords[w] || i > 0 && secretPairs[[2]string{words[i-1], w}] {
			return true
		}
	}
	return false
}

// secretSpellings are the secretWords long enough that a word a letter
// from one, as pasword is from password, is most likely it misspelled. A
// shorter one's neighbours are common words, as taken and tokens are
// token's.
var secretSpellings = []string{"credential", "passphrase", "passwd", "password", "secret"}

// namesSecret reports whether name, of a value submitted for an input the
// workflow doesn't define, can be a secret's: whether it looks like one
// (looksSecret), or one of its words is a letter from one of
// secretSpellings (oneEdit), as a misspelling such as pasword is. It errs
// toward a secret, as passed, a letter from passwd, is taken for one: a
// value is kept as a secret, and still given to the run.
func namesSecret(name string) bool {
	if looksSecret(name) {
		return true
	}
	for _, w := range nameWords(name) {
		if slices.ContainsFunc(secretSpellings, func(s string) bool { return oneEdit(w, s) }) {
			return true
		}
	}
	return false
}

// oneEdit reports whether a and b differ by one letter: one more, one
// fewer, one changed, or two next to each other swapped.
func oneEdit(a, b string) bool {
	x, y := []rune(a), []rune(b)
	if len(x) < len(y) {
		x, y = y, x
	}
	if len(x)-len(y) > 1 {
		return false
	}
	i := 0
	for i < len(y) && x[i] == y[i] {
		i++
	}
	if i == len(y) {
		return len(x) == len(y)+1
	}
	if len(x) > len(y) {
		return string(x[i+1:]) == string(y[i:])
	}
	if string(x[i+1:]) == string(y[i+1:]) {
		return true
	}
	return i+1 < len(x) && x[i] == y[i+1] && x[i+1] == y[i] && string(x[i+2:]) == string(y[i+2:])
}

// nameWords splits a name into its words, in lower case. A word ends at
// anything that isn't a letter or a digit, such as _, -, . or a space, and
// where letters and digits meet. An upper-case letter after a lower-case one
// starts a word, as in apiKey, and so does the last of a run of them before
// a lower-case one, as in APIKey.
func nameWords(name string) []string {
	var words []string
	runes := []rune(name)
	start := -1
	end := func(i int) {
		if start >= 0 {
			words = append(words, strings.ToLower(string(runes[start:i])))
			start = -1
		}
	}
	for i, r := range runes {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			end(i)
			continue
		}
		if start >= 0 {
			prev := runes[i-1]
			if unicode.IsDigit(r) != unicode.IsDigit(prev) ||
				unicode.IsUpper(r) && unicode.IsLower(prev) ||
				unicode.IsUpper(r) && unicode.IsUpper(prev) && i+1 < len(runes) && unicode.IsLower(runes[i+1]) {
				end(i)
			}
		}
		if start < 0 {
			start = i
		}
	}
	end(len(runes))
	return words
}
