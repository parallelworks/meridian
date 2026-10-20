package plan

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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
	return evalValue(v.v, scope.Budgeted())
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
		for k, item := range jsonvalue.Sorted(v) {
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
		if marked(v) {
			return map[string]any{dataKey: out}
		}
		return out
	}
	return v
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
// json.Number's value: int64 when whole and in range, float64 otherwise.
func (v *Value) UnmarshalJSON(data []byte) error {
	raw, err := decodeJSON(data)
	if err != nil {
		return err
	}
	out, err := decodeValue(raw)
	if err != nil {
		return err
	}
	*v = Value{out, true}
	return nil
}

func decodeValue(v any) (any, error) {
	switch v := v.(type) {
	case json.Number:
		return number(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			var err error
			if out[i], err = decodeValue(item); err != nil {
				return nil, fmt.Errorf("%d: %w", i, err)
			}
		}
		return out, nil
	case map[string]any:
		if !marked(v) {
			return decodeMapping(v)
		}
		if parts, ok := v[templateKey]; ok {
			return decodeTemplate(parts)
		}
		if m, ok := v[dataKey].(map[string]any); ok && marked(m) {
			return decodeMapping(m)
		}
		return nil, errors.New("plan: a mapping whose only key starts with $ must be a template or be inside {\"$\": ...}")
	}
	return v, nil
}

func decodeMapping(m map[string]any) (any, error) {
	out := make(map[string]any, len(m))
	for k, item := range jsonvalue.Sorted(m) {
		var err error
		if out[k], err = decodeValue(item); err != nil {
			return nil, fmt.Errorf("%s: %w", k, err)
		}
	}
	return out, nil
}

func decodeTemplate(parts any) (any, error) {
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
