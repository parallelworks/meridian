package plan

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/parallelworks/meridian/expr"
)

// A Value is part of a workflow that may hold expressions: JSON values, in
// which each string with a ${{ }} in it is a parsed template. A step's run,
// its with: and a job's timeout are Values, because what they are can depend
// on what is known only once the run is underway.
//
// A Value encodes as JSON. A template encodes as an object whose one key is
// ${{, holding the template's parts, so a value without expressions encodes
// as itself.
type Value struct {
	v any
}

// templateKey is the key that marks a template in a Value's JSON.
const templateKey = "${{"

// ValueOf makes a Value of v, a workflow's JSON values, parsing the
// expressions in each string. It fails if an expression doesn't parse, or if
// a mapping's only key is ${{, which its JSON couldn't tell from a template.
func ValueOf(v any) (Value, error) {
	out, err := parseValue(v)
	return Value{out}, err
}

// Literal returns a Value that holds v as it is: no string in it is parsed.
func Literal(v any) Value { return Value{v} }

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
		if _, ok := v[templateKey]; ok && len(v) == 1 {
			return nil, fmt.Errorf("a mapping can't have %s as its only key", templateKey)
		}
		out := make(map[string]any, len(v))
		for _, k := range slices.Sorted(maps.Keys(v)) {
			var err error
			if out[k], err = parseValue(v[k]); err != nil {
				return nil, fmt.Errorf("%s: %w", k, err)
			}
		}
		return out, nil
	}
	return v, nil
}

// IsZero reports whether the Value is unset.
func (v Value) IsZero() bool { return v.v == nil }

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
// It returns an *expr.UnknownError if a template reads something scope
// doesn't have yet.
func (v Value) Eval(scope expr.Scope) (any, error) {
	return evalValue(v.v, scope)
}

func evalValue(v any, scope expr.Scope) (any, error) {
	switch v := v.(type) {
	case *expr.Template:
		return v.Eval(scope)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			var err error
			if out[i], err = evalValue(item, scope); err != nil {
				return nil, err
			}
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			var err error
			if out[k], err = evalValue(item, scope); err != nil {
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
			exprs, _ := v.Exprs()
			out = append(out, exprs...)
		case []any:
			for _, item := range v {
				walk(item)
			}
		case map[string]any:
			for _, k := range slices.Sorted(maps.Keys(v)) {
				walk(v[k])
			}
		}
	}
	walk(v.v)
	return out
}

// MarshalJSON encodes the Value, with each template as {"${{": [parts]}.
func (v Value) MarshalJSON() ([]byte, error) {
	return json.Marshal(encodeValue(v.v))
}

func encodeValue(v any) any {
	switch v := v.(type) {
	case *expr.Template:
		return map[string]any{templateKey: v}
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = encodeValue(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[k] = encodeValue(item)
		}
		return out
	}
	return v
}

// UnmarshalJSON decodes a Value that MarshalJSON encoded. Numbers decode as
// json.Number's value: int64 when whole and in range, float64 otherwise.
func (v *Value) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return err
	}
	out, err := decodeValue(raw)
	if err != nil {
		return err
	}
	v.v = out
	return nil
}

func decodeValue(v any) (any, error) {
	switch v := v.(type) {
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return n, nil
		}
		return v.Float64()
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			var err error
			if out[i], err = decodeValue(item); err != nil {
				return nil, err
			}
		}
		return out, nil
	case map[string]any:
		if parts, ok := v[templateKey]; ok && len(v) == 1 {
			data, err := json.Marshal(parts)
			if err != nil {
				return nil, err
			}
			t := new(expr.Template)
			if err := t.UnmarshalJSON(data); err != nil {
				return nil, err
			}
			if t.IsText() {
				return nil, errors.New("plan: a template has no expressions")
			}
			return t, nil
		}
		out := make(map[string]any, len(v))
		for k, item := range v {
			var err error
			if out[k], err = decodeValue(item); err != nil {
				return nil, fmt.Errorf("%s: %w", k, err)
			}
		}
		return out, nil
	}
	return v, nil
}
