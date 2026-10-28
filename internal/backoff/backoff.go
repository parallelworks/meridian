// Package backoff says how long to wait before trying again: doubling
// from a first wait up to a most, and, with Jitter, less up to a quarter
// at random, so that what failed together doesn't all try again at once.
package backoff

import (
	"math"
	"math/rand/v2"
	"time"
)

// Exponential returns first doubled n times, but never more than most:
// first, or most if that is less, for n of 0, or less.
func Exponential(first, most time.Duration, n int) time.Duration {
	d := first
	for range n {
		if d >= most || d > math.MaxInt64/2 {
			return most
		}
		d *= 2
	}
	return min(d, most)
}

// Jitter returns d, less up to a quarter of it at random.
func Jitter(d time.Duration) time.Duration {
	return d - rand.N(d/4+1) //nolint:gosec // jitter, not a secret
}
