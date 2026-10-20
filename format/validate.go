package format

import (
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"

	"github.com/parallelworks/meridian/internal/hint"
	"github.com/parallelworks/meridian/internal/jsonptr"
)

//go:embed schema/1.schema.json
var schema1 []byte

// latestFormat is the newest format this package runs.
const latestFormat = 1

// schemas holds each format's schema, compiled on first use.
var schemas = map[int]func() *formatSchema{
	1: sync.OnceValue(func() *formatSchema { return compileSchema("urn:meridian:format:1", schema1) }),
}

// A formatSchema is a format's compiled schema, and its JSON, in which
// validate looks up the keywords that an error came from.
type formatSchema struct {
	*jsonschema.Schema
	doc any
}

// compileSchema compiles an embedded schema, whose URI is id. A schema that
// doesn't compile is a bug in this package, which its tests catch.
func compileSchema(id string, data []byte) *formatSchema {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		panic(fmt.Sprintf("format: schema %s: %v", id, err))
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	if err := c.AddResource(id, doc); err != nil {
		panic(fmt.Sprintf("format: schema %s: %v", id, err))
	}
	s, err := c.Compile(id)
	if err != nil {
		panic(fmt.Sprintf("format: schema %s: %v", id, err))
	}
	return &formatSchema{s, doc}
}

// maxPasses bounds how many times validate drops unknown keys and validates
// again. One pass drops every unknown key it finds. Another finds more only
// when dropping keys changed which form of a value is the closest match.
const maxPasses = 4

// validate validates a workflow against its format's schema. A key the schema
// doesn't define is a warning, and is dropped from the value validate
// returns, so that later stages ignore it as ACTIVATE does.
func (p *parser) validate(s *formatSchema, value any) any {
	for pass := 1; ; pass++ {
		err := s.Validate(value)
		if err == nil {
			return value
		}
		verr, ok := errors.AsType[*jsonschema.ValidationError](err)
		if !ok {
			p.report(Error, Position{}, CodeSchema, "", "%v", err)
			return value
		}
		var x explanation
		s.explain(verr, &x)
		if len(x.unknown) == 0 || pass == maxPasses {
			for _, e := range x.errors {
				p.schemaError(s, e)
			}
			for _, u := range x.unknown {
				p.unknownKey(Error, u)
			}
			return value
		}
		for _, u := range x.unknown {
			p.unknownKey(Warning, u)
			obj, _ := jsonptr.Lookup(value, u.object)
			if obj, ok := obj.(map[string]any); ok {
				delete(obj, u.key)
			}
		}
	}
}

// An explanation sorts the causes of a validation error into keys the schema
// doesn't define and everything else.
type explanation struct {
	unknown []unknownKey
	errors  []*jsonschema.ValidationError
	// formless holds the locations of values that have none of their
	// forms' types.
	formless [][]string
}

// An unknownKey is a key that an object's schema doesn't define.
type unknownKey struct {
	object  []string // the object's location in the workflow
	key     string
	defined []string // the keys the object's schema defines
}

// explain sorts the causes of e into x. Causes that must all be fixed are
// explained one by one. An anyOf or oneOf that no form matched is explained by
// the form the value was most likely meant to have.
func (s *formatSchema) explain(e *jsonschema.ValidationError, x *explanation) {
	switch k := e.ErrorKind.(type) {
	case *kind.AdditionalProperties:
		obj, _ := s.lookup(e.SchemaURL).(map[string]any)
		if namesEntries(e.InstanceLocation, obj) {
			x.errors = append(x.errors, e)
			return
		}
		props, _ := obj["properties"].(map[string]any)
		defined := slices.Sorted(maps.Keys(props))
		for _, key := range k.Properties {
			x.unknown = append(x.unknown, unknownKey{e.InstanceLocation, key, defined})
		}
	case *kind.Schema, *kind.Group, *kind.AllOf, *kind.Reference:
		for _, c := range e.Causes {
			s.explain(c, x)
		}
	case *kind.AnyOf:
		s.explainForms(e, x)
	case *kind.OneOf:
		if k.Subschemas != nil {
			// More than one form matched.
			x.errors = append(x.errors, e)
			return
		}
		s.explainForms(e, x)
	default:
		x.errors = append(x.errors, e)
	}
}

// explainForms explains an anyOf or oneOf that none of its forms matched by
// the form that was meant, or else as a whole.
func (s *formatSchema) explainForms(e *jsonschema.ValidationError, x *explanation) {
	forms := s.forms(e)
	if f := meant(forms); f != nil {
		x.unknown = append(x.unknown, f.unknown...)
		x.errors = append(x.errors, f.errors...)
		x.formless = append(x.formless, f.formless...)
		return
	}
	if !slices.ContainsFunc(forms, func(f form) bool { return !f.mismatched }) {
		x.formless = append(x.formless, e.InstanceLocation)
	}
	x.errors = append(x.errors, e)
}

// A form is one branch of an anyOf or oneOf, and why a value didn't match it.
type form struct {
	explanation
	// mismatched is whether the value has another type than the form's, or
	// another value for the form's type key: the key that tells input
	// types and layouts apart.
	mismatched bool
}

// forms explains each form of an anyOf or oneOf that none of them matched.
func (s *formatSchema) forms(e *jsonschema.ValidationError) []form {
	forms := make([]form, len(e.Causes))
	for i, c := range e.Causes {
		f := &forms[i]
		s.explain(c, &f.explanation)
		f.mismatched = f.mismatches(e.InstanceLocation)
	}
	return forms
}

func (x *explanation) mismatches(loc []string) bool {
	if slices.ContainsFunc(x.formless, func(l []string) bool { return slices.Equal(l, loc) }) {
		return true
	}
	return slices.ContainsFunc(x.errors, func(e *jsonschema.ValidationError) bool {
		switch e.ErrorKind.(type) {
		case *kind.Type:
			return slices.Equal(e.InstanceLocation, loc)
		case *kind.Const, *kind.Enum:
			return isTypeKey(e.InstanceLocation, loc)
		}
		return false
	})
}

func isTypeKey(loc, object []string) bool {
	return len(loc) == len(object)+1 && loc[len(object)] == "type" && slices.Equal(loc[:len(object)], object)
}

// meant returns the form a value that matched none of its forms was meant to
// have, or nil if that can't be told. If a form would match without some
// unknown keys, it is the one that needs the fewest dropped. Otherwise, it is
// the form with the fewest problems of those whose type the value has. A tie
// can't be told apart.
func meant(forms []form) *form {
	var fixable, likely []*form
	for i := range forms {
		f := &forms[i]
		switch {
		case len(f.errors) == 0:
			fixable = append(fixable, f)
		case !f.mismatched:
			likely = append(likely, f)
		}
	}
	if len(fixable) > 0 {
		return fewest(fixable, func(f *form) int { return len(f.unknown) })
	}
	return fewest(likely, func(f *form) int { return len(f.errors) + len(f.unknown) })
}

// fewest returns the form with the lowest cost, or nil if none or more than
// one has it.
func fewest(forms []*form, cost func(*form) int) *form {
	var best *form
	tie := false
	for _, f := range forms {
		switch {
		case best == nil || cost(f) < cost(best):
			best, tie = f, false
		case cost(f) == cost(best):
			tie = true
		}
	}
	if tie {
		return nil
	}
	return best
}

// namesEntries reports whether an object is a map of names, such as jobs or
// env, rather than an object with a fixed set of keys. In a map of names, a
// key that isn't a valid name is an error, not an unknown key. with: is never
// one: its keys are the inputs of what uses: names, and one that isn't
// taken is an unknown key.
func namesEntries(loc []string, schema map[string]any) bool {
	if len(loc) > 0 && loc[len(loc)-1] == "with" {
		return false
	}
	_, ok := schema["patternProperties"]
	return ok
}

// lookup returns the schema JSON at an error's schema location, a URI whose
// fragment is a JSON Pointer.
func (s *formatSchema) lookup(location string) any {
	_, frag, _ := strings.Cut(location, "#")
	frag, err := url.PathUnescape(frag)
	if err != nil {
		return nil
	}
	tokens, err := jsonptr.Split(frag)
	if err != nil {
		return nil
	}
	v, _ := jsonptr.Lookup(s.doc, tokens)
	return v
}

func (p *parser) unknownKey(severity Severity, u unknownKey) {
	path := jsonptr.Append(jsonptr.Format(u.object), u.key)
	msg := fmt.Sprintf("unknown key %q", u.key)
	if severity == Warning {
		msg += " is ignored"
	}
	if s, ok := hint.Suggest(u.key, u.defined); ok {
		msg += fmt.Sprintf("; did you mean %q?", s)
	}
	p.report(severity, p.keys[path], CodeUnknownKey, path, "%s", msg)
}

// schemaError reports an error that is left once unknown keys are set aside.
func (p *parser) schemaError(s *formatSchema, e *jsonschema.ValidationError) {
	path := jsonptr.Format(e.InstanceLocation)
	switch k := e.ErrorKind.(type) {
	case *kind.AdditionalProperties:
		obj, _ := s.lookup(e.SchemaURL).(map[string]any)
		patterns, _ := obj["patternProperties"].(map[string]any)
		for _, key := range slices.Sorted(slices.Values(k.Properties)) {
			kp := jsonptr.Append(path, key)
			p.report(Error, p.keys[kp], CodeSchema, kp, "%q isn't a valid name here; a name must match %s",
				key, strings.Join(slices.Sorted(maps.Keys(patterns)), " or "))
		}
		return
	case *kind.AnyOf, *kind.OneOf:
		if got, types, ok := wantedTypeKeys(e); ok {
			kp := jsonptr.Append(path, "type")
			if t, ok := suggestType(got, types); ok {
				p.report(Error, p.values[kp], CodeSchema, kp, "type %s isn't one allowed here; did you mean %q?", show(got), t)
			} else {
				p.report(Error, p.values[kp], CodeSchema, kp, "type %s isn't one allowed here, which are %s",
					show(got), quoteList(types, "and"))
			}
			return
		}
	}
	p.report(Error, p.values[path], CodeSchema, path, "%s", s.describe(e))
}

// wantedTypeKeys returns the value's type key and the values the forms of
// the value allow for it, if every form failed because of it.
func wantedTypeKeys(e *jsonschema.ValidationError) (got any, types []string, ok bool) {
	for _, form := range e.Causes {
		found := false
		walk(form, func(c *jsonschema.ValidationError) {
			if !isTypeKey(c.InstanceLocation, e.InstanceLocation) {
				return
			}
			switch k := c.ErrorKind.(type) {
			case *kind.Const:
				got, found = k.Got, true
				types = append(types, fmt.Sprint(k.Want))
			case *kind.Enum:
				got, found = k.Got, true
				for _, w := range k.Want {
					types = append(types, fmt.Sprint(w))
				}
			}
		})
		if !found {
			return nil, nil, false
		}
	}
	return got, slices.Compact(slices.Sorted(slices.Values(types))), len(types) > 0
}

func walk(e *jsonschema.ValidationError, f func(*jsonschema.ValidationError)) {
	f(e)
	for _, c := range e.Causes {
		walk(c, f)
	}
}
