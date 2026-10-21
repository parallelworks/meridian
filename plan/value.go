package plan

import (
	"bytes"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
)

// A Value is part of a workflow that may hold expressions: JSON values, in
// which each string with a ${{ }} in it is a parsed template. A step's run,
// its with: and a job's timeout are Values, because what they are can depend
// on what is known only once the run is underway.
//
// A Value encodes as JSON. A template encodes as an object whose one key is
// ${{, holding the template's parts, so a value without expressions encodes
// as itself. A mapping whose only key starts with $, as ${{ does, encodes
// inside an object whose one key is $, so that it is never taken for a
// template, whatever the workflow or a run's data put in it.
type Value struct {
	v any
	// set is false only for the zero Value, which is unset. A Value of null
	// is set: a step's working-directory: null opts out of its job's.
	set bool
}

const (
	// templateKey is the key that marks a template in a Value's JSON.
	templateKey = "${{"
	// dataKey is the key that marks a mapping whose only key starts with $.
	dataKey = "$"
)

// ValueOf makes a Value of v, a workflow's JSON values, parsing the
// expressions in each string. It fails if an expression doesn't parse.
func ValueOf(v any) (Value, error) {
	out, err := parseValue(v)
	if err != nil {
		return Value{}, err
	}
	return Value{out, true}, nil
}

// Literal returns a Value that holds v as it is: no string in it is parsed,
// and an *expr.Template in it stays a template.
func Literal(v any) Value { return Value{v, true} }

func parseValue(v any) (any, error) {
	switch v := v.(type) {
	case string:
		t, err := expr.ParseTemplate(v)
		if err != nil || t.IsText() {
			return v, err
		}
		return t, nil
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			var err error
			if out[i], err = parseValue(item); err != nil {
				return nil, fmt.Errorf("%d: %w", i, err)
			}
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range jsonvalue.Sorted(v) {
			var err error
			if out[k], err = parseValue(item); err != nil {
				return nil, fmt.Errorf("%s: %w", k, err)
			}
		}
		return out, nil
	}
	return v, nil
}

// IsZero reports whether the Value is unset.
func (v Value) IsZero() bool { return !v.set }

// Depth returns how many levels the Value's JSON nests, as a plan writes
// it: none for a scalar, and for a list or a mapping one more than its
// deepest item. A mapping whose only key starts with $ is two more, since
// it is written inside {"$": ...}, and a template nests as its parts' JSON
// does, inside {"${{": ...}.
func (v Value) Depth() int { return valueDepth(v.v) }

func valueDepth(v any) int {
	switch v := v.(type) {
	case nil, bool, string, int64, float64:
		return 0
	case *expr.Template:
		parts, _ := v.MarshalJSON()
		return 1 + jsonDepth(parts)
	case []any:
		depth := 0
		for _, item := range v {
			depth = max(depth, valueDepth(item))
		}
		return 1 + depth
	case map[string]any:
		depth := 0
		for _, item := range v {
			depth = max(depth, valueDepth(item))
		}
		if marked(v) {
			return 2 + depth
		}
		return 1 + depth
	}
	data, _ := jsonv2.Marshal(v, encoding)
	return jsonDepth(data)
}

// jsonDepth returns how many levels JSON nests.
func jsonDepth(data []byte) int {
	depth, deepest := 0, 0
	inString := false
	for i := 0; i < len(data); i++ {
		switch c := data[i]; {
		case inString && c == '\\':
			i++
		case c == '"':
			inString = !inString
		case inString:
		case c == '[' || c == '{':
			depth++
			deepest = max(deepest, depth)
		case c == ']' || c == '}':
			depth--
		}
	}
	return deepest
}

// Literal returns the Value's JSON value if it holds no expressions.
func (v Value) Literal() (any, bool) {
	if hasTemplate(v.v) {
		return nil, false
	}
	return v.v, true
}

func hasTemplate(v any) bool {
	switch v := v.(type) {
	case *expr.Template:
		return true
	case []any:
		return slices.ContainsFunc(v, hasTemplate)
	case map[string]any:
		for _, item := range v {
			if hasTemplate(item) {
				return true
			}
		}
	}
	return false
}

// Eval evaluates the Value's templates in scope and returns its JSON value.
// Its templates are one group: each has the language's limit on what it
// builds, and together they share Meridian's wider budget for a group, which
// isn't part of the format. It returns an *expr.BudgetError if they build
// more than that together, and an *expr.UnknownError if a template reads
// something scope doesn't have yet.
func (v Value) Eval(scope expr.Scope) (any, error) {
	out, _, err := v.EvalMarked(scope)
	return out, err
}

// EvalMarked evaluates the Value as Eval does, and reports whether one of
// its templates read a value that scope marks, as
// expr.Template.EvalMarked does: then what the Value gives is made from it,
// or decided by it.
func (v Value) EvalMarked(scope expr.Scope) (out any, marked bool, err error) {
	out, err = evalValue(v.v, scope.Budgeted(), &marked)
	return out, marked, err
}

// evalValue evaluates the templates in v, and notes in marked whether one
// read a marked value.
func evalValue(v any, scope expr.Scope, marked *bool) (any, error) {
	switch v := v.(type) {
	case *expr.Template:
		out, m, err := v.EvalMarked(scope)
		*marked = *marked || m
		return out, err
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			var err error
			if out[i], err = evalValue(item, scope, marked); err != nil {
				return nil, err
			}
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range jsonvalue.Sorted(v) {
			var err error
			if out[k], err = evalValue(item, scope, marked); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	return v, nil
}

// Exprs returns every expression in the Value.
func (v Value) Exprs() []*expr.Expr {
	var out []*expr.Expr
	var walk func(v any)
	walk = func(v any) {
		switch v := v.(type) {
		case *expr.Template:
			for _, e := range v.Exprs() {
				out = append(out, e)
			}
		case []any:
			for _, item := range v {
				walk(item)
			}
		case map[string]any:
			for _, item := range jsonvalue.Sorted(v) {
				walk(item)
			}
		}
	}
	walk(v.v)
	return out
}

// MarshalJSON encodes the Value, with each template as {"${{": [parts]}.
func (v Value) MarshalJSON() ([]byte, error) {
	return jsonv2.Marshal(v, encoding)
}

// encodeValue writes JSON values, with each template as {"${{": [parts]}
// and each marked mapping inside {"$": ...}.
func encodeValue(enc *jsontext.Encoder, v any) error {
	switch v := v.(type) {
	case *expr.Template:
		parts, err := v.MarshalJSON()
		if err != nil {
			return err
		}
		return writeObject(enc, templateKey, func() error { return enc.WriteValue(parts) })
	case []any:
		if err := enc.WriteToken(jsontext.BeginArray); err != nil {
			return err
		}
		for _, item := range v {
			if err := encodeValue(enc, item); err != nil {
				return err
			}
		}
		return enc.WriteToken(jsontext.EndArray)
	case map[string]any:
		if marked(v) {
			return writeObject(enc, dataKey, func() error { return encodeMapping(enc, v) })
		}
		return encodeMapping(enc, v)
	case string:
		return enc.WriteToken(jsontext.String(v))
	case bool:
		return enc.WriteToken(jsontext.Bool(v))
	case nil:
		return enc.WriteToken(jsontext.Null)
	case int64:
		return enc.WriteToken(jsontext.Int(v))
	case float64:
		// NaN and the infinities aren't JSON: MarshalEncode fails on them.
		if !math.IsNaN(v) && !math.IsInf(v, 0) {
			return enc.WriteToken(jsontext.Float(v))
		}
	}
	return jsonv2.MarshalEncode(enc, v)
}

func encodeMapping(enc *jsontext.Encoder, m map[string]any) error {
	if err := enc.WriteToken(jsontext.BeginObject); err != nil {
		return err
	}
	for k, item := range jsonvalue.Sorted(m) {
		if err := enc.WriteToken(jsontext.String(k)); err != nil {
			return err
		}
		if err := encodeValue(enc, item); err != nil {
			return err
		}
	}
	return enc.WriteToken(jsontext.EndObject)
}

// writeObject writes an object whose one key is key, with value writing its
// value.
func writeObject(enc *jsontext.Encoder, key string, value func() error) error {
	if err := enc.WriteToken(jsontext.BeginObject); err != nil {
		return err
	}
	if err := enc.WriteToken(jsontext.String(key)); err != nil {
		return err
	}
	if err := value(); err != nil {
		return err
	}
	return enc.WriteToken(jsontext.EndObject)
}

// marked reports whether a mapping's only key starts with $, which in a
// Value's JSON would mark it as something other than a mapping.
func marked(m map[string]any) bool {
	if len(m) != 1 {
		return false
	}
	for k := range m {
		return strings.HasPrefix(k, "$")
	}
	return false
}

// UnmarshalJSON decodes a Value that MarshalJSON encoded. Numbers decode as
// int64 when whole and in range, and float64 otherwise.
func (v *Value) UnmarshalJSON(data []byte) error {
	return jsonv2.Unmarshal(data, v, decoding)
}

// errUnmarked reports a mapping that is marked, but as nothing a Value's
// JSON has.
var errUnmarked = errors.New("plan: a mapping whose only key starts with $ must be a template or be inside {\"$\": ...}")

// decodeValue reads JSON values, with a template from each {"${{": [parts]}
// and a mapping from each {"$": ...}.
func decodeValue(dec *jsontext.Decoder) (any, error) {
	switch dec.PeekKind() {
	case '[':
		if _, err := dec.ReadToken(); err != nil {
			return nil, err
		}
		list := []any{}
		for i := 0; dec.PeekKind() != ']'; i++ {
			item, err := decodeValue(dec)
			if err != nil {
				return nil, fmt.Errorf("%d: %w", i, err)
			}
			list = append(list, item)
		}
		_, err := dec.ReadToken()
		return list, err
	case '{':
		return decodeMapping(dec)
	}
	return jsonvalue.Decode(dec, number)
}

// decodeMapping reads a mapping, or what a mapping whose only key starts
// with $ marks.
func decodeMapping(dec *jsontext.Decoder) (any, error) {
	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}
	m := map[string]any{}
	for first := true; dec.PeekKind() != '}'; first = false {
		tok, err := dec.ReadToken()
		if err != nil {
			return nil, err
		}
		k := tok.String()
		var item any
		if first && strings.HasPrefix(k, "$") {
			// What k's value is depends on whether k is the only key, which
			// is known only after the value. So the value is read once as
			// it is written, and then taken as one or the other, rather
			// than read again for each level of mappings nested this way.
			var n any
			if k == templateKey && dec.PeekKind() == '[' {
				// Most are a template's parts, which are kept as JSON for
				// the template to decode.
				var raw jsontext.Value
				if raw, err = dec.ReadValue(); err != nil {
					return nil, err
				}
				raw = raw.Clone()
				if dec.PeekKind() == '}' {
					if _, err := dec.ReadToken(); err != nil {
						return nil, err
					}
					return decodeTemplate(raw)
				}
				n, err = readWritten(jsontext.NewDecoder(bytes.NewBuffer(raw)))
			} else {
				n, err = readWritten(dec)
			}
			if err != nil {
				return nil, err
			}
			if dec.PeekKind() == '}' {
				if _, err := dec.ReadToken(); err != nil {
					return nil, err
				}
				return markedValue(k, n)
			}
			item, err = writtenValue(n)
		} else {
			item, err = decodeValue(dec)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", k, err)
		}
		m[k] = item
	}
	_, err := dec.ReadToken()
	return m, err
}

// decodeTemplate decodes a template's parts.
func decodeTemplate(raw jsontext.Value) (*expr.Template, error) {
	t := new(expr.Template)
	if err := t.UnmarshalJSON(raw); err != nil {
		return nil, err
	}
	if t.IsText() {
		return nil, errors.New("plan: a template has no expressions")
	}
	return t, nil
}

// JSON as it is written, read before what it holds is known: nil, bools,
// strings and lists as JSON values are, a number as its text, and an object
// as its members, in order, so that a template's parts are written again
// exactly as they were read.
type (
	writtenNumber string
	writtenObject []writtenMember
	writtenMember struct {
		key   string
		value any
	}
)

// readWritten reads a JSON value as it is written.
func readWritten(dec *jsontext.Decoder) (any, error) {
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	switch tok.Kind() {
	case 'n':
		return nil, nil
	case 'f', 't':
		return tok.Bool(), nil
	case '"':
		return tok.String(), nil
	case '0':
		return writtenNumber(tok.String()), nil
	case '[':
		list := []any{}
		for dec.PeekKind() != ']' {
			item, err := readWritten(dec)
			if err != nil {
				return nil, err
			}
			list = append(list, item)
		}
		_, err := dec.ReadToken()
		return list, err
	}
	var obj writtenObject
	for dec.PeekKind() != '}' {
		tok, err := dec.ReadToken()
		if err != nil {
			return nil, err
		}
		key := tok.String()
		item, err := readWritten(dec)
		if err != nil {
			return nil, err
		}
		obj = append(obj, writtenMember{key, item})
	}
	_, err = dec.ReadToken()
	return obj, err
}

// writtenValue returns the value that n, JSON as it is written, holds, as
// decodeValue reads it.
func writtenValue(n any) (any, error) {
	switch n := n.(type) {
	case []any:
		list := make([]any, len(n))
		for i, item := range n {
			var err error
			if list[i], err = writtenValue(item); err != nil {
				return nil, fmt.Errorf("%d: %w", i, err)
			}
		}
		return list, nil
	case writtenObject:
		if len(n) == 1 && strings.HasPrefix(n[0].key, "$") {
			return markedValue(n[0].key, n[0].value)
		}
		m := make(map[string]any, len(n))
		for _, member := range n {
			item, err := writtenValue(member.value)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", member.key, err)
			}
			m[member.key] = item
		}
		return m, nil
	case writtenNumber:
		return number(string(n))
	}
	return n, nil
}

// markedValue returns what n, the value of a mapping whose only key, k,
// starts with $, marks: a template's parts, or a mapping whose only key
// starts with $.
func markedValue(k string, n any) (any, error) {
	switch k {
	case templateKey:
		raw, err := appendWritten(nil, n)
		if err != nil {
			return nil, err
		}
		return decodeTemplate(raw)
	case dataKey:
		obj, ok := n.(writtenObject)
		if !ok || len(obj) != 1 || !strings.HasPrefix(obj[0].key, "$") {
			return nil, errUnmarked
		}
		item, err := writtenValue(obj[0].value)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", obj[0].key, err)
		}
		return map[string]any{obj[0].key: item}, nil
	}
	return nil, errUnmarked
}

// appendWritten appends the JSON of n, JSON as it is written, to b.
func appendWritten(b []byte, n any) ([]byte, error) {
	switch n := n.(type) {
	case nil:
		return append(b, "null"...), nil
	case bool:
		return strconv.AppendBool(b, n), nil
	case string:
		return jsontext.AppendQuote(b, n)
	case writtenNumber:
		return append(b, n...), nil
	case []any:
		b = append(b, '[')
		for i, item := range n {
			if i > 0 {
				b = append(b, ',')
			}
			var err error
			if b, err = appendWritten(b, item); err != nil {
				return nil, err
			}
		}
		return append(b, ']'), nil
	}
	b = append(b, '{')
	for i, member := range n.(writtenObject) {
		if i > 0 {
			b = append(b, ',')
		}
		var err error
		if b, err = jsontext.AppendQuote(b, member.key); err != nil {
			return nil, err
		}
		b = append(b, ':')
		if b, err = appendWritten(b, member.value); err != nil {
			return nil, err
		}
	}
	return append(b, '}'), nil
}
