// Package jsonshape checks JSON against the Go type that encoding/json/v2
// decoded it into, for what v2 takes that Meridian's encodings never
// write, and their JSON Schemas refuse: a member that is missing though
// its type always writes it, as one without omitempty or omitzero is;
// null where the encoding never writes it; a Go int out of the 32 bits it
// has on some systems; and a string that isn't one of those its field's
// enum tag lists, as foundation's types list them.
package jsonshape

import (
	"bytes"
	"encoding"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// Options say how the JSON that Check checks was written.
type Options struct {
	// NilAsNull is set for an encoding that writes a nil list or mapping
	// as null, as a plan's does: one that is always written can be null.
	NilAsNull bool
	// Any are the types that hold any JSON, null among it, and decode it
	// themselves, as a plan's Value does: null is one of their values
	// wherever they are, and Check doesn't look in them. An interface,
	// such as any, is one.
	Any []reflect.Type
	// Zero are the types that hold any JSON and decode it themselves, and
	// whose zero value, which holds nothing, is written as null, as a
	// plan's Data's is: null is one of their values where their zero is
	// written, as a member that is always written, or an item of a list or
	// a mapping. Check doesn't look in them.
	Zero []reflect.Type
}

// A place is where a value is in what holds it.
type place int

const (
	// optional is a member left out when it is zero, as one with omitempty
	// or omitzero is.
	optional place = iota
	// always is a member always written, or the whole JSON.
	always
	// item is an item of a list or a mapping.
	item
)

// Check checks data, JSON that encoding/json/v2 decoded into a value of
// type t without an error, for what v2 takes that the encoding never
// writes. A type that decodes itself whole, with UnmarshalJSON or
// UnmarshalText, as time.Time does, holds what its own decoding takes;
// Check looks in a struct that checks itself as it decodes, with
// UnmarshalJSONFrom.
func Check(data []byte, t reflect.Type, o *Options) error {
	return o.value(jsontext.NewDecoder(bytes.NewReader(data)), t, always)
}

// value checks the value dec reads next, of type t, at a place.
func (o *Options) value(dec *jsontext.Decoder, t reflect.Type, at place) error {
	if dec.PeekKind() == 'n' {
		if _, err := dec.ReadToken(); err != nil {
			return err
		}
		if !o.null(t, at) {
			return fmt.Errorf("null within %q", dec.StackPointer())
		}
		return nil
	}
	if o.opaque(t) {
		return dec.SkipValue()
	}
	switch t.Kind() {
	case reflect.Pointer:
		return o.value(dec, t.Elem(), at)
	case reflect.Struct:
		return o.object(dec, t)
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return dec.SkipValue()
		}
		return o.items(dec, func() error { return o.value(dec, t.Elem(), item) })
	case reflect.Map:
		return o.items(dec, func() error {
			if _, err := dec.ReadToken(); err != nil {
				return err
			}
			return o.value(dec, t.Elem(), item)
		})
	case reflect.Int:
		tok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		if _, err := strconv.ParseInt(tok.String(), 10, 32); tok.Kind() == '0' && err != nil {
			return fmt.Errorf("%s within %q is out of range: a number of this kind has 32 bits on some systems", tok.String(), dec.StackPointer())
		}
		return nil
	}
	return dec.SkipValue()
}

// items checks the items of the list or the mapping dec reads next, with
// one for each.
func (o *Options) items(dec *jsontext.Decoder, one func() error) error {
	if _, err := dec.ReadToken(); err != nil {
		return err
	}
	for dec.PeekKind() != ']' && dec.PeekKind() != '}' {
		if err := one(); err != nil {
			return err
		}
	}
	_, err := dec.ReadToken()
	return err
}

// object checks the members of the object dec reads next, of the struct
// type t.
func (o *Options) object(dec *jsontext.Decoder, t reflect.Type) error {
	s := shapeOf(t)
	if _, err := dec.ReadToken(); err != nil {
		return err
	}
	at := dec.StackPointer()
	seen := make([]bool, len(s.members))
	for dec.PeekKind() != '}' {
		tok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		i, ok := s.index[tok.String()]
		if !ok {
			// v2 decoded it, so it is what the type reads otherwise.
			if err := dec.SkipValue(); err != nil {
				return err
			}
			continue
		}
		m := s.members[i]
		seen[i] = true
		if m.enum != nil {
			if err := m.check(dec); err != nil {
				return err
			}
			continue
		}
		place := optional
		if m.always {
			place = always
		}
		if err := o.value(dec, m.typ, place); err != nil {
			return err
		}
	}
	if _, err := dec.ReadToken(); err != nil {
		return err
	}
	for i, m := range s.members {
		if m.always && !seen[i] {
			return fmt.Errorf("no %q within %q", m.name, at)
		}
	}
	return nil
}

// null reports whether null is one of the values of type t at a place.
func (o *Options) null(t reflect.Type, at place) bool {
	switch {
	case t.Kind() == reflect.Interface || slices.Contains(o.Any, t):
		return true
	case slices.Contains(o.Zero, t):
		return at != optional
	case t.Kind() == reflect.Pointer:
		return at == always
	case t.Kind() == reflect.Slice || t.Kind() == reflect.Map:
		return at == always && o.NilAsNull
	}
	return false
}

var (
	unmarshalerType     = reflect.TypeFor[jsonv2.Unmarshaler]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
)

// opaque reports whether t decodes itself, so that Check doesn't look in
// it.
func (o *Options) opaque(t reflect.Type) bool {
	if t.Kind() == reflect.Interface || slices.Contains(o.Any, t) || slices.Contains(o.Zero, t) {
		return true
	}
	p := reflect.PointerTo(t)
	return p.Implements(unmarshalerType) || p.Implements(textUnmarshalerType)
}

// A shape is a struct type's JSON members, as v2 reads them.
type shape struct {
	members []member
	// index is each member's, by its name.
	index map[string]int
}

// A member is one of a struct's JSON members.
type member struct {
	name string
	typ  reflect.Type
	// always is set when the type always writes it: it has neither
	// omitempty nor omitzero.
	always bool
	// enum is what its field's enum tag lists, as foundation's tags list
	// a string's values, as in enum:"query,path".
	enum []string
}

// check checks a value of a member with an enum: one of its strings.
func (m member) check(dec *jsontext.Decoder) error {
	tok, err := dec.ReadToken()
	if err != nil {
		return err
	}
	if tok.Kind() == '"' && slices.Contains(m.enum, tok.String()) {
		return nil
	}
	value := tok.String()
	if tok.Kind() == '"' {
		value = strconv.Quote(value)
	}
	return fmt.Errorf("%s within %q isn't one of %s", value, dec.StackPointer(), strings.Join(m.enum, ", "))
}

var shapes sync.Map

// shapeOf returns the shape of a struct type.
func shapeOf(t reflect.Type) *shape {
	if s, ok := shapes.Load(t); ok {
		return s.(*shape)
	}
	s := &shape{index: map[string]int{}}
	s.add(t)
	shapes.Store(t, s)
	return s
}

// add adds the members of the struct type t, and of the structs it embeds
// without a name, which v2 reads as its own.
func (s *shape) add(t reflect.Type) {
	for f := range t.Fields() {
		tag := f.Tag.Get("json")
		name, options, _ := strings.Cut(tag, ",")
		if name == "-" && options == "" || !f.IsExported() && !f.Anonymous {
			continue
		}
		if f.Anonymous && name == "" {
			embedded := f.Type
			if embedded.Kind() == reflect.Pointer {
				embedded = embedded.Elem()
			}
			s.add(embedded)
			continue
		}
		if name == "" {
			name = f.Name
		}
		m := member{name: name, typ: f.Type, always: !strings.Contains(options, "omitempty") && !strings.Contains(options, "omitzero")}
		if enum := f.Tag.Get("enum"); enum != "" {
			m.enum = strings.Split(enum, ",")
		}
		s.index[name] = len(s.members)
		s.members = append(s.members, m)
	}
}
