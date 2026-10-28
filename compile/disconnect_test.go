package compile_test

import (
	"math"
	"testing"

	"github.com/parallelworks/meridian/compile"
)

// TestDisconnectTimeout checks that a disconnect-timeout is read as format
// 1 reads one, as text: a number of seconds, or a whole number with one of
// d, h, m or s after it, or nothing, for its default; and is negative, and
// never gives up, as format 1 has it, once multiplied out to nanoseconds.
func TestDisconnectTimeout(t *testing.T) {
	for _, tc := range []struct {
		v               any
		negative, valid bool
	}{
		{nil, false, true},
		{int64(300), false, true},
		{int64(0), false, true},
		{int64(-1), true, true},
		{-0.5, true, true},
		{math.NaN(), false, false},
		{"", false, true},
		{"  ", false, true},
		{"300", false, true},
		{" -1 ", true, true},
		{"-0.5", true, true},
		{"1e3", false, true},
		{"5m", false, true},
		{"-5m", true, true},
		{"-2d", true, true},
		{"-0s", false, true},
		{"30s", false, true},
		// A unit's count past what int64 nanoseconds hold wraps, as in
		// format 1.
		{"9223372036854775807s", true, true},
		{"1h30m", false, false},
		{"-1h30m", false, false},
		{"1.5h", false, false},
		{"300ms", false, false},
		{"5w", false, false},
		{"m", false, false},
		{"inf", false, false},
		{"NaN", false, false},
		{"ten", false, false},
		{true, false, false},
	} {
		negative, valid := compile.DisconnectTimeout(tc.v)
		if negative != tc.negative || valid != tc.valid {
			t.Errorf("%#v: negative %v, valid %v; want %v, %v", tc.v, negative, valid, tc.negative, tc.valid)
		}
	}
}
