package plan

import (
	jsonv2 "encoding/json/v2"
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

// Depth returns how many levels d's JSON nests: none for a scalar, and for
// a list or a mapping one more than its deepest item, or none for a nil
// one, which is written as null.
func (d Data) Depth() int { return dataDepth(d.v) }

func dataDepth(v any) int {
	switch v := v.(type) {
	case nil, bool, string, int64, float64:
		return 0
	case []any:
		if v == nil {
			return 0
		}
		depth := 0
		for _, item := range v {
			depth = max(depth, dataDepth(item))
		}
		return 1 + depth
	case map[string]any:
		if v == nil {
			return 0
		}
		depth := 0
		for _, item := range v {
			depth = max(depth, dataDepth(item))
		}
		return 1 + depth
	}
	data, _ := jsonv2.Marshal(v, encoding)
	return jsonDepth(data)
}

// MarshalJSON encodes the JSON d holds.
func (d Data) MarshalJSON() ([]byte, error) { return jsonv2.Marshal(d, encoding) }

// UnmarshalJSON decodes JSON into d, with numbers as a Value's are.
func (d *Data) UnmarshalJSON(data []byte) error { return jsonv2.Unmarshal(data, d, decoding) }
