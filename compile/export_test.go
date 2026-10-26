package compile

import (
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/jsonvalue"
)

// SetRoundWork sets roundWork, the work each round of Start's resolution
// after the first counts for each input, row field and env: value, and
// returns what restores it. Tests that set it don't run in parallel.
func SetRoundWork(n int) (restore func()) {
	old := roundWork
	roundWork = n
	return func() { roundWork = old }
}

// NamesSecret is namesSecret, for TestSecretNames.
var NamesSecret = namesSecret

// FormValues returns what a launch form submits for a run, given the
// values in submitted: every input's value, with each default and each
// form's text it can evaluate evaluated, as ACTIVATE's form evaluates them,
// those that read a password or a variable too. What reads a secret
// variable, which it can't evaluate, it leaves out, as if it sent the
// text. It sends the values of inputs that are ignored too. What reads an
// input of a platform's type reads what opts.Inputs gives for it, as the
// form reads the resource a person picks, but the input's own value is the
// one opts.Inputs was given, as a form sends a resource's name, as
// pw://user/name, rather than its details.
func FormValues(doc *format.Document, submitted map[string]any, opts Options) map[string]any {
	plain := *doc
	plain.Value = jsonvalue.CloneMap(doc.Value)
	// A form evaluates a password input's value as it does a text input's.
	asText(lookup(plain.Value, definitionsPath))
	r := newInputResolver(&plain, submitted, opts)
	r.marks = expr.Marks{}
	r.fill()
	// What the form sends is the values before the compiler compares them,
	// less those evaluated as the run starts because they read what the
	// form can't evaluate either.
	out := jsonvalue.CloneMap(r.values)
	for _, f := range r.fields {
		if f.deferred {
			deletePath(out, f.path)
		}
	}
	for _, fields := range [][]*field{r.fields, r.liveRows()} {
		for _, f := range fields {
			if f.resolved && !f.failed {
				setIn(out, f.path, jsonvalue.Clone(f.given))
			}
		}
	}
	return out
}

// asText makes each password input that defs, input definitions, define a
// string input, and each among their inputs.
func asText(defs any) {
	m, _ := defs.(map[string]any)
	for _, v := range m {
		def, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if def["type"] == "password" {
			def["type"] = "string"
		}
		for _, key := range []string{"items", "options", "template"} {
			asText(def[key])
		}
	}
}
