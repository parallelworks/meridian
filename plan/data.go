package plan

import (
	jsonv2 "encoding/json/v2"
	"strconv"
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
func (d Data) MarshalJSON() ([]byte, error) { return jsonv2.Marshal(d, encoding) }

// UnmarshalJSON decodes JSON into d, with numbers as a Value's are.
func (d *Data) UnmarshalJSON(data []byte) error { return jsonv2.Unmarshal(data, d, decoding) }

// number returns the value of a number, from its text: an int64 when it is
// whole and in range, and a float64 otherwise.
func number(s string) (any, error) {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, nil
	}
	return strconv.ParseFloat(s, 64)
}
