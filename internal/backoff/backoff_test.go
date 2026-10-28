package backoff

import (
	"testing"
	"time"
)

// Exponential doubles first n times, up to most, as each caller's own
// arithmetic did: first for n of 0, or less, and most once the doubling
// reaches it, however large n is.
func TestExponential(t *testing.T) {
	for _, tc := range []struct {
		first, most time.Duration
		n           int
		want        time.Duration
	}{
		{time.Second, 30 * time.Second, -1, time.Second},
		{time.Second, 30 * time.Second, 0, time.Second},
		{time.Second, 30 * time.Second, 1, 2 * time.Second},
		{time.Second, 30 * time.Second, 4, 16 * time.Second},
		{time.Second, 30 * time.Second, 5, 30 * time.Second},
		{time.Second, 30 * time.Second, 1 << 30, 30 * time.Second},
		{250 * time.Millisecond, 30 * time.Second, 29, 30 * time.Second},
		{time.Minute, time.Second, 0, time.Second},
		{time.Minute, time.Second, 3, time.Second},
		{time.Duration(1) << 61, time.Duration(1<<63 - 1), 10, time.Duration(1<<63 - 1)},
	} {
		if got := Exponential(tc.first, tc.most, tc.n); got != tc.want {
			t.Errorf("Exponential(%v, %v, %d) = %v, want %v", tc.first, tc.most, tc.n, got, tc.want)
		}
	}
}

// Jitter takes up to a quarter of d away, and never more.
func TestJitter(t *testing.T) {
	for _, d := range []time.Duration{0, 1, 4, time.Second, 30 * time.Second} {
		for range 1000 {
			if got := Jitter(d); got > d || got < d-d/4 {
				t.Fatalf("Jitter(%v) = %v, outside [%v, %v]", d, got, d-d/4, d)
			}
		}
	}
}
