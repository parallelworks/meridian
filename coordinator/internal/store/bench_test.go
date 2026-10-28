package store

import (
	"strconv"
	"testing"

	"github.com/parallelworks/meridian/plan"
)

// BenchmarkRunsInTurn reads, in turn, the runs of a tree wider than the
// store once kept the start states of, as its workers' requests read
// them: 200 runs, each started with inputs of a few hundred rows.
func BenchmarkRunsInTurn(b *testing.B) {
	const runs = 200
	s := open(b)
	rows := make([]any, 300)
	for i := range rows {
		rows[i] = map[string]any{"name": "row " + strconv.Itoa(i), "path": "/data/set/" + strconv.Itoa(i)}
	}
	state := testState()
	state.Run.Inputs = plan.DataOf(map[string]any{"rows": rows})
	ids := make([]string, runs)
	for i := range ids {
		ids[i] = createRun(b, s)
		inTx(b, s, func(tx *Tx) error { return tx.StartRun(ids[i], state, nil) })
	}
	read := func(id string) {
		if err := s.View(b.Context(), func(tx *Tx) error {
			_, err := tx.Run(id)
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
	// Each run's first read decodes its start state, as any cache does.
	for _, id := range ids {
		read(id)
	}
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		read(ids[i%runs])
	}
}
