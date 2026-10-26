package scaletest

import (
	"math"
	"testing"
)

// The timing check tells work that grows with its size from work that
// grows with its square: work that grows as the size does passes it, and
// work that grows as its square fails it, every time.
func TestWithinTellsSquareFromLinear(t *testing.T) {
	var sink uint64
	work := func(iterations int) func() {
		return func() {
			x := sink
			for i := range iterations {
				x = x*6364136223846793005 + uint64(i)
			}
			sink = x
		}
	}
	const n = 1000
	if times, ok := Within(Bound(Factor), work(n*n), work(Factor*n*n)); !ok {
		t.Errorf("work that grows with its size failed: %v", times)
	}
	if times, ok := Within(Bound(Factor), work(n*n/16), work(Factor*n*Factor*n/16)); ok {
		t.Errorf("work that grows with its size's square passed: %v", times)
	}
}

// Bound is the geometric mean of linear and quadratic growth.
func TestBound(t *testing.T) {
	for factor, want := range map[float64]float64{4: 8, 8: math.Sqrt(512), 20: math.Sqrt(8000)} {
		if got := Bound(factor); math.Abs(got-want) > 1e-9 || math.Abs(got*got-factor*factor*factor) > 1e-6 {
			t.Errorf("Bound(%v) = %v, want %v", factor, got, want)
		}
	}
}
