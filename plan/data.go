package plan

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Data is JSON that a run was given rather than what its workflow says, such
// as its input values. Nothing in it is evaluated: a string with ${{ }} in it
// is text, and a mapping is a mapping, whatever its keys.
type Data struct {
	v any
}

// DataOf returns Data that holds v, JSON values.
func DataOf(v any) Data { return Data{v} }

// Get returns the JSON values d holds.
func (d Data) Get() any { return d.v }

// IsZero reports whether d holds nothing.
func (d Data) IsZero() bool { return d.v == nil }

// MarshalJSON encodes the JSON d holds.
func (d Data) MarshalJSON() ([]byte, error) { return json.Marshal(d.v) }

// UnmarshalJSON decodes JSON into d, with numbers as a Value's are.
func (d *Data) UnmarshalJSON(data []byte) error {
	raw, err := decodeJSON(data)
	if err != nil {
		return err
	}
	out, err := numbers(raw)
	if err != nil {
		return err
	}
	d.v = out
	return nil
}

// decodeJSON decodes JSON, keeping its numbers as json.Number.
func decodeJSON(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// numbers replaces each json.Number in v with its value.
func numbers(v any) (any, error) {
	switch v := v.(type) {
	case json.Number:
		return number(v)
	case []any:
		for i, item := range v {
			var err error
			if v[i], err = numbers(item); err != nil {
				return nil, fmt.Errorf("%d: %w", i, err)
			}
		}
	case map[string]any:
		for _, k := range sortedKeys(v) {
			var err error
			if v[k], err = numbers(v[k]); err != nil {
				return nil, fmt.Errorf("%s: %w", k, err)
			}
		}
	}
	return v, nil
}

// number returns n's value: an int64 when n is whole and in range, and a
// float64 otherwise.
func number(n json.Number) (any, error) {
	if i, err := n.Int64(); err == nil {
		return i, nil
	}
	return n.Float64()
}
