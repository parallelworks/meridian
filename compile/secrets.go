package compile

import (
	"slices"
	"strconv"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/internal/reach"
	"github.com/parallelworks/meridian/plan"
)

// secretState is what the inputResolver keeps of the values the plan
// doesn't hold, and of what reads them.
type secretState struct {
	// secrets are the values of password inputs, by their paths, and
	// secretsLater the paths of those whose values are evaluated as the run
	// starts, and of lists whose rows are, with one in them: the run's
	// secrets name both.
	secrets      map[string]any
	secretsLater map[string]bool
	// marks are the paths in the contexts of what reads a variable or a
	// password: org, vars, password inputs, password fields of lists' rows,
	// and the parts of the values that read them; and markedInputs their
	// paths in the inputs' values.
	marks        expr.Marks
	markedInputs reach.Trie[struct{}]
	// passwords are the password inputs, and the password fields of lists'
	// rows; unknownSecrets the paths of values submitted for inputs the
	// workflow doesn't define that are taken for passwords; and held the
	// other parts of the values kept out of the plan (holdPath), which are
	// among the secrets or private (keepHeld). derived names those among
	// the secrets that can be made from a secret, and private holds the
	// values of the private ones, by their paths.
	passwords      []*field
	unknownSecrets [][]string
	held           []heldPart
	derived        map[string]bool
	private        map[string]any
}

// A heldPart is a part of the values, at path, in the value of the input
// in, or nil for a value of an input the workflow doesn't define, that the
// plan doesn't hold (holdPath), and how it is kept.
type heldPart struct {
	in   *field
	path []string
	kind holding
}

// A holding is how a value the plan doesn't hold is kept.
type holding int

const (
	// heldSecret is a value kept as a secret, as a password's is: one
	// that can hold a password where the compiler can't find it.
	heldSecret holding = iota
	// heldDerived is a value kept as a secret in the place of what is made
	// from a password or a secret variable, that isn't what makes it: it
	// can be made from one, or not, and is masked as what is made from a
	// secret is.
	heldDerived
	// heldPrivate is a value in the place of what is made from an
	// organization or user variable that isn't a secret, and no password,
	// that isn't what makes it: it is private, as such a variable's value
	// is, and isn't masked.
	heldPrivate
)

// markPassword marks f's value if f is a password input, or a password
// field of a list's row, as a variable's is: what is made from it is
// evaluated as the run starts, and what is wrong with it isn't shown.
func (r *inputResolver) markPassword(f *field) {
	if f.typ == "password" {
		r.passwords = append(r.passwords, f)
		r.mark(f.path)
		r.markSecret(f.path)
	}
}

// markSecret marks the value at path in the inputs' values as a secret's,
// as the run starts: what reads it is made from a secret
// (expr.Scope.Marked).
func (r *inputResolver) markSecret(path []string) {
	if r.final {
		r.start.secrets.Add(slices.Concat([]string{"inputs"}, path))
		r.forgetMeant(path)
	}
}

// markedSecret reports whether the value at path in the inputs' values is
// marked as a secret's as the run starts (markSecret), holds such a value,
// or is in one.
func (r *inputResolver) markedSecret(path []string) bool {
	full := slices.Concat([]string{"inputs"}, path)
	for p := range r.start.secrets.All() {
		if related(p, full) {
			return true
		}
	}
	return false
}

// mark marks the value at path in the inputs' values: what reads it reads a
// variable or a password. The evaluations that follow read the marks anew.
func (r *inputResolver) mark(path []string) {
	r.marks.Add(slices.Concat([]string{"inputs"}, path))
	r.markedInputs.Add(path, struct{}{})
	r.evaluators = nil
	r.forgetMeant(path)
}

// marked reports whether the value at path in the inputs' values is
// marked, holds a marked value, or is in one: as the value of another input
// with the same path, as a wizard step's input can have, can be.
func (r *inputResolver) marked(path []string) bool {
	for range r.markedInputs.Reached(path) {
		return true
	}
	return false
}

// holdPath keeps the value at path in the inputs' values, in the value of
// the input in, out of the plan, as kind says, and what reads it reads a
// variable or a password. Once every part evaluated as the run starts is
// known, a value with such parts in it is held but for them (heldParts).
// The plan holds null in place of a list's item, and the run's start puts
// the value in its place.
func (r *inputResolver) holdPath(in *field, path []string, kind holding) {
	r.mark(path)
	if kind != heldPrivate {
		r.markSecret(path)
	}
	r.held = append(r.held, heldPart{in, path, kind})
}

// heldParts makes the values held the parts of them that aren't evaluated
// as the run starts: a value in a part that is is left to it, and of a
// value with such parts in it, each of its keys or items that isn't one is
// held instead.
func (r *inputResolver) heldParts() {
	var out []heldPart
	var split func(p heldPart, at []string)
	split = func(p heldPart, at []string) {
		if p.in == nil {
			// No part of it is evaluated as the run starts.
			out = append(out, p)
			return
		}
		rel := at[len(p.in.path):]
		in := false
		for _, part := range p.in.partsAround(rel) {
			if len(part.path) <= len(rel) {
				return
			}
			in = true
		}
		if !in {
			out = append(out, heldPart{p.in, at, p.kind})
			return
		}
		switch v := lookup(r.values, at).(type) {
		case map[string]any:
			for k := range jsonvalue.Sorted(v) {
				split(p, appendPath(at, k))
			}
		case []any:
			for i := range v {
				split(p, appendPath(at, strconv.Itoa(i)))
			}
		}
	}
	for _, p := range r.held {
		split(p, p.path)
	}
	r.held = out
}

// keepHeld puts the values kept out of the plan (holdPath) among the
// secrets, or among the private values, once they are checked: those of
// the inputs that have them, a null among them, which is a value as the
// plan's is. derived names the secrets that are what can be made from one.
func (r *inputResolver) keepHeld() {
	for _, p := range r.held {
		v, ok := jsonptr.Lookup(r.values, p.path)
		if !ok {
			continue
		}
		key := plan.JoinPath(p.path...)
		switch p.kind {
		case heldPrivate:
			r.private[key] = v
		case heldDerived:
			if r.derived == nil {
				r.derived = map[string]bool{}
			}
			r.derived[key] = true
			fallthrough
		default:
			r.secrets[key] = v
		}
	}
}
