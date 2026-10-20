package format

import (
	"math/big"
	"slices"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/parallelworks/meridian/internal/hint"
	"github.com/parallelworks/meridian/problems"
)

// expression is the pattern the schema gives a value that must be a ${{ }}
// expression.
const expression = `^\${{.*}}$`

var printer = message.NewPrinter(language.English)

// A finding is what is wrong with a value: a kind of problem, and the params
// its message shows.
type finding struct {
	kind   *problems.Kind
	params problems.Params
}

// describe says what is wrong with a value.
func (s *formatSchema) describe(e *jsonschema.ValidationError) finding {
	switch k := e.ErrorKind.(type) {
	case *kind.Type:
		if len(e.InstanceLocation) == 0 && k.Got == "null" {
			return finding{problems.WorkflowEmpty, nil}
		}
		return mustBeType(k.Want, k.Got)
	case *kind.Required:
		return finding{problems.MissingKeys, problems.Params{"keys": problems.QuoteAll(k.Missing)}}
	case *kind.Pattern:
		if k.Want == expression {
			return finding{problems.NotExpression, problems.Params{"value": problems.Show(k.Got)}}
		}
		return finding{problems.PatternMismatch, problems.Params{"value": problems.Show(k.Got), "pattern": k.Want}}
	case *kind.Const:
		return finding{problems.MustEqual, problems.Params{"allowed": problems.Show(k.Want)}}
	case *kind.Enum:
		if len(k.Want) == 1 {
			return finding{problems.MustEqual, problems.Params{"allowed": problems.Show(k.Want[0])}}
		}
		return finding{problems.InvalidChoice, problems.Params{"allowed": showList(k.Want)}}
	case *kind.MinLength:
		return finding{problems.TooShort, problems.Params{"min": k.Want}}
	case *kind.MaxLength:
		return finding{problems.TooLong, problems.Params{"max": k.Want}}
	case *kind.MinItems:
		return finding{problems.TooFewItems, problems.Params{"min": k.Want}}
	case *kind.MaxItems:
		return finding{problems.TooManyItems, problems.Params{"max": k.Want}}
	case *kind.MinProperties:
		return finding{problems.TooFewKeys, problems.Params{"count": k.Got, "min": k.Want}}
	case *kind.MaxProperties:
		return finding{problems.TooManyKeys, problems.Params{"count": k.Got, "max": k.Want}}
	case *kind.Minimum:
		return finding{problems.BelowMinimum, problems.Params{"min": number(k.Want), "exclusive": "false"}}
	case *kind.ExclusiveMinimum:
		return finding{problems.BelowMinimum, problems.Params{"min": number(k.Want), "exclusive": "true"}}
	case *kind.Maximum:
		return finding{problems.AboveMaximum, problems.Params{"max": number(k.Want), "exclusive": "false"}}
	case *kind.ExclusiveMaximum:
		return finding{problems.AboveMaximum, problems.Params{"max": number(k.Want), "exclusive": "true"}}
	case *kind.UniqueItems:
		return finding{problems.DuplicateItems, nil}
	case *kind.Not:
		return s.describeNot(e)
	case *kind.OneOf:
		if k.Subschemas != nil {
			return finding{problems.SeveralForms, nil}
		}
		return s.describeForms(e)
	case *kind.AnyOf:
		return s.describeForms(e)
	default:
		return finding{problems.SchemaRule, problems.Params{"reason": e.ErrorKind.LocalizedString(printer)}}
	}
}

// describeForms says why a value matches none of the forms of an anyOf or
// oneOf, when which form was meant can't be told.
func (s *formatSchema) describeForms(e *jsonschema.ValidationError) finding {
	unknown := finding{problems.NoForm, nil}
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
		keys := slices.Compact(slices.Sorted(slices.Values(mixed)))
		return finding{problems.KeysNotAllowedTogether, problems.Params{"keys": problems.QuoteAll(keys)}}
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
			values, got = append(values, problems.Show(k.Want)), k.Got
		case *kind.Enum:
			for _, w := range k.Want {
				values = append(values, problems.Show(w))
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
		return finding{problems.MissingAnyKey, problems.Params{"keys": problems.QuoteAll(slices.Compact(missing))}}
	case len(values) > 0 && len(types) == 0 && len(missing) == 0:
		return mustBeValue(got, values)
	default:
		return unknown
	}
}

// mustBeValue says what a value can be: one of some values, an expression,
// or a match for some patterns.
func mustBeValue(got any, values []string) finding {
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
	value := problems.Show(got)
	switch {
	case len(patterns) > 0 && len(consts) == 0 && !expr:
		return finding{problems.ValueMustMatch, problems.Params{"value": value, "patterns": problems.List(patterns)}}
	case len(patterns) > 0:
		return finding{problems.NoForm, nil}
	case len(consts) == 0:
		return finding{problems.NotExpression, problems.Params{"value": value}}
	}
	params := problems.Params{"value": value, "allowed": problems.List(consts)}
	switch {
	case len(consts) == 1 && expr:
		return finding{problems.ValueMustEqualOrExpression, params}
	case expr:
		return finding{problems.ValueMustBeOneOfOrExpression, params}
	case len(consts) == 1:
		return finding{problems.ValueMustEqual, params}
	default:
		return finding{problems.ValueMustBeOneOf, params}
	}
}

func (s *formatSchema) describeNot(e *jsonschema.ValidationError) finding {
	obj, _ := s.lookup(e.SchemaURL).(map[string]any)
	not, _ := obj["not"].(map[string]any)
	if required, ok := not["required"].([]any); ok && len(not) == 1 {
		keys := make([]string, len(required))
		for i, r := range required {
			keys[i], _ = r.(string)
		}
		if len(keys) == 1 {
			return finding{problems.KeyNotAllowed, problems.Params{"key": keys[0]}}
		}
		return finding{problems.KeysNotAllowedTogether, problems.Params{"keys": problems.QuoteAll(keys)}}
	}
	return finding{problems.FormNotAllowed, nil}
}

// mustBeType says that a value must have one of the JSON Schema types want,
// and isn't got, if got isn't "". The message names up to three types; a
// value that could have more matches no form it can name.
func mustBeType(want []string, got string) finding {
	if len(want) == 0 || len(want) > 3 {
		return finding{problems.NoForm, nil}
	}
	params := problems.Params{"count": len(want), "type1": "", "type2": "", "type3": "", "has_got": strconv.FormatBool(got != ""), "got": got}
	for i, t := range want {
		params["type"+string(rune('1'+i))] = t
	}
	return finding{problems.WrongType, params}
}

func showList(items []any) string {
	s := make([]string, len(items))
	for i, item := range items {
		s[i] = problems.Show(item)
	}
	return problems.List(s)
}

// number returns a schema's number, which its message formats.
func number(r *big.Rat) float64 {
	f, _ := r.Float64()
	return f
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
