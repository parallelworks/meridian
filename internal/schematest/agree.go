package schematest

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// replacements are what Agree puts in the place of each value: null, true,
// and, for a number, numbers at the bounds an integer can have, past them,
// and with a fraction.
var (
	everywhere = []any{nil, true}
	numbers    = []any{
		json.Number("-1"), json.Number("0"), json.Number("2147483647"), json.Number("2147483648"),
		json.Number("-2147483649"), json.Number("9223372036854775808"), json.Number("1.5"),
	}
)

// Agree checks that decode takes each of a set of changes to data, JSON
// that the schema at path takes, exactly when the schema does, so that a
// decoder is as strict as the schema that describes what it reads, and no
// stricter: each member of each object left out, described as no "name";
// an unknown member added to each object, as an unknown member; and each
// value replaced with null, with true, and, for a number, with numbers at
// an integer's bounds and past them, and with a fraction, as new for old.
// A change that excused takes, by its description, they may disagree on,
// for what the decoder checks that the schema leaves to it. It reports the
// first changes they disagree on.
func Agree(t testing.TB, path, what string, data []byte, decode func([]byte) error, excused func(change string) bool) {
	t.Helper()
	s, fits := Schema(t, path), probe(t, path)
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(doc); err != nil {
		t.Fatalf("%s doesn't fit its schema: %v", what, err)
	}
	failed := 0
	check := func(change string, at jsontext.Pointer) {
		if failed >= 10 || excused(change) {
			return
		}
		changed := appendJSON(nil, doc)
		schemaOK, decodeErr := fits.Validate(doc) == nil, decode(changed)
		if schemaOK != (decodeErr == nil) {
			failed++
			t.Errorf("%s with %s at %s: the schema says %v, and decoding %v\n%s", what, change, at, s.Validate(doc), decodeErr, changed)
		}
	}
	var walk func(v any, set func(any), at jsontext.Pointer)
	walk = func(v any, set func(any), at jsontext.Pointer) {
		var old string
		switch v.(type) {
		case []any:
			old = "a list"
		case map[string]any:
			old = "an object"
		default:
			old = string(appendJSON(nil, v))
		}
		replacements := everywhere
		if _, ok := v.(json.Number); ok {
			replacements = append(slices.Clone(everywhere), numbers...)
		}
		for _, r := range replacements {
			set(r)
			check(fmt.Sprintf("%s for %s", appendJSON(nil, r), old), at)
		}
		set(v)
		switch v := v.(type) {
		case map[string]any:
			v["zzUnknown"] = json.Number("1")
			check("an unknown member", at)
			delete(v, "zzUnknown")
			for _, k := range slices.Sorted(maps.Keys(v)) {
				item := v[k]
				delete(v, k)
				check(fmt.Sprintf("no %q", k), at)
				v[k] = item
				walk(item, func(r any) { v[k] = r }, at.AppendToken(k))
			}
		case []any:
			for i, item := range v {
				walk(item, func(r any) { v[i] = r }, at.AppendToken(strconv.Itoa(i)))
			}
		}
	}
	walk(doc, func(r any) { doc = r }, "")
}

// Set returns data, JSON, with the value at the JSON Pointer at replaced
// with v, or, if v is Removed, the member there left out. Its mappings'
// keys are in order.
func Set(t testing.TB, data []byte, at jsontext.Pointer, v any) []byte {
	t.Helper()
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	parent, last := at.Parent(), at.LastToken()
	holder := doc
	for token := range parent.Tokens() {
		switch h := holder.(type) {
		case map[string]any:
			holder = h[token]
		case []any:
			i, err := strconv.Atoi(token)
			if err != nil || i >= len(h) {
				t.Fatalf("schematest: nothing at %s", at)
			}
			holder = h[i]
		default:
			t.Fatalf("schematest: nothing at %s", at)
		}
	}
	switch h := holder.(type) {
	case map[string]any:
		if _, ok := h[last]; !ok {
			t.Fatalf("schematest: nothing at %s", at)
		}
		if v == Removed {
			delete(h, last)
		} else {
			h[last] = v
		}
	case []any:
		i, err := strconv.Atoi(last)
		if err != nil || i >= len(h) || v == Removed {
			t.Fatalf("schematest: can't set %s", at)
		}
		h[i] = v
	default:
		t.Fatalf("schematest: nothing at %s", at)
	}
	return appendJSON(nil, doc)
}

// Removed is what Set sets a member to that it leaves out.
var Removed any = new(int)

// Bounds checks that prop, the schema of what names, a member of Go type
// typ, bounds an integer as encoding/json/v2 does in every build: an int
// to 32 bits, as it has on some systems, and an int64 to 64. Its minimum
// may be more, as a format's is 1, and a const needs none.
func Bounds(t testing.TB, what string, typ reflect.Type, prop any) {
	t.Helper()
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	p, _ := prop.(map[string]any)
	var lo, hi float64
	switch typ.Kind() {
	case reflect.Int:
		lo, hi = math.MinInt32, math.MaxInt32
	case reflect.Int64:
		lo, hi = math.MinInt64, math.MaxInt64
	default:
		if p["type"] == "integer" {
			t.Errorf("%s: the schema says an integer, where it is a %s", what, typ)
		}
		return
	}
	if _, ok := p["const"]; ok {
		return
	}
	minimum, _ := p["minimum"].(float64)
	if p["type"] != "integer" || p["maximum"] != hi || p["minimum"] == nil || minimum < lo {
		t.Errorf("%s: the schema bounds it to %v and %v, where a %s has %v to %v", what, p["minimum"], p["maximum"], typ, lo, hi)
	}
}

// appendJSON appends v, JSON as jsonschema.UnmarshalJSON gives it, with
// each number as it was written, to b.
func appendJSON(b []byte, v any) []byte {
	switch v := v.(type) {
	case nil:
		return append(b, "null"...)
	case bool:
		return strconv.AppendBool(b, v)
	case json.Number:
		return append(b, v...)
	case string:
		b, _ = jsontext.AppendQuote(b, v)
		return b
	case []any:
		b = append(b, '[')
		for i, item := range v {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendJSON(b, item)
		}
		return append(b, ']')
	case map[string]any:
		b = append(b, '{')
		first := true
		for _, k := range slices.Sorted(maps.Keys(v)) {
			if !first {
				b = append(b, ',')
			}
			first = false
			b, _ = jsontext.AppendQuote(b, k)
			b = appendJSON(append(b, ':'), v[k])
		}
		return append(b, '}')
	}
	panic(fmt.Sprintf("schematest: %T in JSON", v)) //nolint:forbidigo // jsonschema.UnmarshalJSON gives only the types above, so another is a bug here
}
