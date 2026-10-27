package coordinator

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
)

// matrixOf is a workflow of one matrix job of n instances, each a step.
func matrixOf(n int) string {
	values := make([]string, n)
	for i := range values {
		values[i] = strconv.Itoa(i)
	}
	return fmt.Sprintf(`
jobs:
  build:
    strategy:
      matrix:
        n: [%s]
    steps:
      - run: echo ${{ matrix.n }}
`, strings.Join(values, ", "))
}

// BenchmarkMatrix runs a run of a matrix job of 50, and of 200, instances
// from its creation to its end, through the coordinator, which serves it,
// with the test driver's workers, which claim and end each attempt in this
// process: so what it measures is the coordinator's and the store's, each
// instance's supervision, placement and end, and the work of the matrix
// job's own supervisor and the run's finisher, which grows with the
// instances. It reports the time, the bytes and the allocations per
// instance, which stay flat as the instances grow unless something costs
// more for each instance the more there are.
func BenchmarkMatrix(b *testing.B) {
	for _, n := range []int{50, 200} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			c := open(b, Options{})
			serve(b, c)
			workflow := matrixOf(n)
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			runs := 0
			for b.Loop() {
				id, _ := create(b, c, workflow, nil, nil)
				if r := waitEnded(b, c, id); r.Outcome != plan.OutcomeSuccess {
					b.Fatalf("the run ended with %s", r.Outcome)
				}
				runs++
			}
			runtime.ReadMemStats(&after)
			instances := float64(runs * n)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/instances, "ns/instance")
			b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/instances, "B/instance")
			b.ReportMetric(float64(after.Mallocs-before.Mallocs)/instances, "allocs/instance")
		})
	}
}

// BenchmarkRunOfMatrix reads, as each supervisor's look and each request of
// a worker's does, a run that has started, whose start state holds the
// instances of its matrix jobs: four of 256 instances each.
func BenchmarkRunOfMatrix(b *testing.B) {
	c := open(b, Options{})
	var workflow strings.Builder
	workflow.WriteString("jobs:\n")
	for j := range 4 {
		values := make([]string, 256)
		for i := range values {
			values[i] = fmt.Sprintf("%q", fmt.Sprintf("value %d of job %d", i, j))
		}
		fmt.Fprintf(&workflow, "  job%d:\n    strategy:\n      matrix:\n        v: [%s]\n    steps:\n      - run: echo ${{ matrix.v }}\n", j, strings.Join(values, ", "))
	}
	id, _ := create(b, c, workflow.String(), nil, nil)
	if _, err := c.startRun(b.Context(), id, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := c.store.View(b.Context(), func(tx *store.Tx) error {
			r, err := tx.Run(id)
			if err == nil && len(r.StartState.Expansions) != 4 {
				err = fmt.Errorf("the run's start state has %d expansions", len(r.StartState.Expansions))
			}
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
}
