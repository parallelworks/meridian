// Package scaletest times how work grows with its size, for the tests that
// check that compiling, starting and running a workflow grow with it as
// they should, on runners that slow down for a while.
package scaletest

import (
	"math"
	"runtime"
	"runtime/debug"
	"time"
)

// Factor is how much larger the larger of the two sizes a timed check
// compares is than the smaller: far enough apart that work that grows with
// its size, Factor times, and work that grows with its square, Factor
// squared, are told apart by Bound(Factor), their geometric mean, though a
// runner slows the one size's runs and not the other's by up to the square
// root of Factor, 2.8 times. On a runner of two cores, busy with other
// tests, work that grows as it should, at four times its size, once took
// 8.2 times as long.
const Factor = 8

// Bound returns the geometric mean of linear and quadratic growth at
// factor times the size, factor to the power 1.5: the most work that grows
// with its size may take, at factor times it, and less than work that
// grows with its square takes, by the same ratio.
func Bound(factor float64) float64 {
	return math.Sqrt(factor * factor * factor)
}

// Within reports whether large takes at most bound times as long as small,
// and the times it compared, each the best of five runs (Fastest). One
// that takes longer is measured again, and fails only if it does again: a
// runner slow for a while doesn't fail it, and work that grows faster than
// bound allows takes longer every time.
func Within(bound float64, small, large func()) ([]time.Duration, bool) {
	var times []time.Duration
	for range 2 {
		times = Fastest(small, large)
		if float64(times[1])/float64(times[0]) <= bound {
			return times, true
		}
	}
	return times, false
}

// Fastest returns the shortest time of five that each of fs takes. It runs
// them in turn, each once a round, so that a runner that slows down for a
// while slows each of them alike. It times the work each does, without the
// garbage collector, which collects before each run and not during one:
// what collecting costs depends on what else the process holds, and on how
// many cores it has to itself, and on a runner of two, busy, the larger of
// two runs that grow as they should took nine times as long as the smaller
// at four times its size. The checks of what each allocates say how much
// it allocates.
func Fastest(fs ...func()) []time.Duration {
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	best := make([]time.Duration, len(fs))
	for i := range 5 {
		for j, f := range fs {
			runtime.GC()
			start := time.Now()
			f()
			if took := time.Since(start); i == 0 || took < best[j] {
				best[j] = took
			}
		}
	}
	return best
}
