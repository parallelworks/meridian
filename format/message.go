package format

import (
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/parallelworks/meridian/internal/hint"
)

// expression is the pattern the schema gives a value that must be a ${{ }}
// expression.
const expression = `^\${{.*}}$`

var printer = message.NewPrinter(language.English)

// describe says what is wrong with a value.
func (s *formatSchema) describe(e *jsonschema.ValidationError) string {
	switch k := e.ErrorKind.(type) {
	case *kind.Type:
		if len(e.InstanceLocation) == 0 && k.Got == "null" {
			return "the workflow is empty"
		}
		return mustBeType(k.Want, k.Got)
	case *kind.Required:
		return "missing " + quoteList(k.Missing, "and")
	case *kind.Pattern:
		if k.Want == expression {
			return fmt.Sprintf("%q must be a ${{ }} expression", k.Got)
		}
		return fmt.Sprintf("%q doesn't match %s", k.Got, k.Want)
	case *kind.Const:
		return "must be " + show(k.Want)
	case *kind.Enum:
		if len(k.Want) == 1 {
			return "must be " + show(k.Want[0])
		}
		return "must be one of " + showList(k.Want, "or")
	case *kind.MinLength:
		return fmt.Sprintf("is %d characters long, shorter than the minimum of %d", k.Got, k.Want)
	case *kind.MaxLength:
		return fmt.Sprintf("is %d characters long, more than the limit of %d", k.Got, k.Want)
	case *kind.MinItems:
		return fmt.Sprintf("has %d items, fewer than the minimum of %d", k.Got, k.Want)
	case *kind.MaxItems:
		return fmt.Sprintf("has %d items, more than the limit of %d", k.Got, k.Want)
	case *kind.MinProperties:
		return fmt.Sprintf("has %d keys, fewer than the minimum of %d", k.Got, k.Want)
	case *kind.MaxProperties:
		return fmt.Sprintf("has %d keys, more than the limit of %d", k.Got, k.Want)
	case *kind.Minimum:
		return "must be at least " + number(k.Want)
	case *kind.Maximum:
		return "must be at most " + number(k.Want)
	case *kind.ExclusiveMinimum:
		return "must be more than " + number(k.Want)
	case *kind.ExclusiveMaximum:
		return "must be less than " + number(k.Want)
	case *kind.UniqueItems:
		return fmt.Sprintf("has the same item twice, at %d and %d", k.Duplicates[0], k.Duplicates[1])
	case *kind.Not:
		return s.describeNot(e)
	case *kind.OneOf:
		if k.Subschemas != nil {
			return "matches more than one of the forms allowed here"
		}
		return s.describeForms(e)
	case *kind.AnyOf:
		return s.describeForms(e)
	default:
		return e.ErrorKind.LocalizedString(printer)
	}
}

// describeForms says why a value matches none of the forms of an anyOf or
// oneOf, when which form was meant can't be told.
func (s *formatSchema) describeForms(e *jsonschema.ValidationError) string {
	const unknown = "doesn't match any of the forms allowed here"
	forms := s.forms(e)

	// The value would match more than one form without some of its keys.
	var mixed []string
	for _, f := range forms {
		if len(f.errors) == 0 {
			for _, u := range f.unknown {
				mixed = append(mixed, u.key)
			}
		}
	}
	if len(mixed) > 0 {
		return fmt.Sprintf("can't have %s together", quoteList(slices.Compact(slices.Sorted(slices.Values(mixed))), "and"))
	}

	// Each form has one problem with the value itself: say what it can be.
	var types, missing, values []string
	var got any
	for _, f := range forms {
		if len(f.unknown) > 0 || len(f.errors) != 1 || !slices.Equal(f.errors[0].InstanceLocation, e.InstanceLocation) {
			return unknown
		}
		switch k := f.errors[0].ErrorKind.(type) {
		case *kind.Type:
			types, got = append(types, k.Want...), k.Got
		case *kind.Required:
			missing = append(missing, k.Missing...)
		case *kind.Const:
			values, got = append(values, show(k.Want)), k.Got
		case *kind.Enum:
			for _, w := range k.Want {
				values = append(values, show(w))
			}
			got = k.Got
		case *kind.Pattern:
			values, got = append(values, "pattern:"+k.Want), k.Got
		default:
			return unknown
		}
	}
	switch {
	case len(types) > 0 && len(missing) == 0 && len(values) == 0:
		name, _ := got.(string)
		return mustBeType(slices.Compact(slices.Sorted(slices.Values(types))), name)
	case len(missing) > 0 && len(types) == 0 && len(values) == 0:
		return "missing " + quoteList(slices.Compact(missing), "or")
	case len(values) > 0 && len(types) == 0 && len(missing) == 0:
		return show(got) + " must " + mustBeValue(values)
	default:
		return unknown
	}
}

// mustBeValue phrases what a value can be: one of some values, an
// expression, or a match for some patterns.
func mustBeValue(values []string) string {
	var consts, patterns []string
	expr := false
	for _, v := range values {
		p, ok := strings.CutPrefix(v, "pattern:")
		switch {
		case !ok:
			consts = append(consts, v)
		case p == expression:
			expr = true
		default:
			patterns = append(patterns, p)
		}
	}
	var parts []string
	switch len(consts) {
	case 0:
	case 1:
		parts = append(parts, "be "+consts[0])
	default:
		parts = append(parts, "be one of "+hint.JoinList(consts, "or"))
	}
	if expr {
		parts = append(parts, "be a ${{ }} expression")
	}
	if len(patterns) > 0 {
		parts = append(parts, "match "+hint.JoinList(patterns, "or"))
	}
	return strings.Join(parts, ", or ")
}

func (s *formatSchema) describeNot(e *jsonschema.ValidationError) string {
	obj, _ := s.lookup(e.SchemaURL).(map[string]any)
	not, _ := obj["not"].(map[string]any)
	if required, ok := not["required"].([]any); ok && len(not) == 1 {
		keys := make([]string, len(required))
		for i, r := range required {
			keys[i], _ = r.(string)
		}
		if len(keys) == 1 {
			return fmt.Sprintf("can't have %q here", keys[0])
		}
		return fmt.Sprintf("can't have %s together", quoteList(keys, "and"))
	}
	return "has a form that isn't allowed here"
}

func mustBeType(want []string, got string) string {
	names := make([]string, len(want))
	for i, t := range want {
		names[i] = typeName(t)
	}
	msg := "must be " + hint.JoinList(names, "or")
	if got != "" {
		msg += ", not " + typeName(got)
	}
	return msg
}

func typeName(t string) string {
	switch t {
	case "object":
		return "a mapping"
	case "array":
		return "a list"
	case "boolean":
		return "true or false"
	case "integer":
		return "an integer"
	case "null":
		return "empty"
	default:
		return "a " + t
	}
}

func quoteList(items []string, conj string) string {
	q := make([]string, len(items))
	for i, item := range items {
		q[i] = strconv.Quote(item)
	}
	return hint.JoinList(q, conj)
}

func showList(items []any, conj string) string {
	s := make([]string, len(items))
	for i, item := range items {
		s[i] = show(item)
	}
	return hint.JoinList(s, conj)
}

func show(v any) string {
	if s, ok := v.(string); ok {
		return strconv.Quote(s)
	}
	return fmt.Sprint(v)
}

func number(r *big.Rat) string {
	if r.IsInt() {
		return r.Num().String()
	}
	f, _ := r.Float64()
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// typeNames maps type names from other languages to the input types that
// mean them.
var typeNames = map[string]string{
	"integer": "number",
	"int":     "number",
	"float":   "number",
	"bool":    "boolean",
}

// suggestType returns the allowed input type that got most likely means.
func suggestType(got any, types []string) (string, bool) {
	name, ok := got.(string)
	if !ok {
		return "", false
	}
	if t, ok := typeNames[strings.ToLower(name)]; ok && slices.Contains(types, t) {
		return t, true
	}
	return hint.Suggest(name, types)
}
