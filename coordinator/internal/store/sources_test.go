package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
)

// pinned is a text a run pins, by its digest.
func pinned(text string) compile.PinnedText {
	sum := sha256.Sum256([]byte(text))
	return compile.PinnedText{Digest: "sha256:" + hex.EncodeToString(sum[:]), Text: []byte(text)}
}

// TestKeepTexts checks that a run keeps the texts of the workflows it pins,
// each once in the store, whatever runs pin it, and reads only its own,
// as they were: a text that isn't its digest's is refused as it is kept,
// and found out as it is read; and a run that has ended keeps none. Two
// runs that pin the same bytes, as from a private repository and from a
// public one, share them, and the store keeps nothing but the bytes, by
// their digest, so neither learns where the other found them.
func TestKeepTexts(t *testing.T) {
	t.Parallel()
	s := open(t)
	a, b := pinned("jobs: {a: {}}\n"), pinned("jobs: {b: {}}\n")
	first, second := createRun(t, s), createRun(t, s)
	inTx(t, s, func(tx *Tx) error { return tx.KeepTexts(first, []compile.PinnedText{a, b}) })
	inTx(t, s, func(tx *Tx) error { return tx.KeepTexts(second, []compile.PinnedText{b}) })
	inTx(t, s, func(tx *Tx) error {
		for _, tc := range []struct {
			run  string
			want compile.PinnedText
			err  error
		}{{first, a, nil}, {first, b, nil}, {second, b, nil}, {second, a, ErrNotFound}, {"9", a, ErrNotFound}} {
			got, err := tx.Text(tc.run, tc.want.Digest)
			switch {
			case tc.err != nil && !errors.Is(err, tc.err):
				t.Errorf("run %s's %s: %v, want %v", tc.run, tc.want.Digest, err, tc.err)
			case tc.err == nil && (err != nil || string(got) != string(tc.want.Text)):
				t.Errorf("run %s's %s: %q, %v", tc.run, tc.want.Digest, got, err)
			}
		}
		var n int
		if err := tx.queryRow("SELECT count(*) FROM meridian_sources").Scan(&n); err != nil || n != 2 {
			t.Errorf("the store holds %d texts, %v; want each once, 2", n, err)
		}
		// What the store keeps of the texts, and of which runs pin them,
		// names no place a run found one.
		for table, want := range map[string][]string{
			"meridian_sources":     {"digest", "text"},
			"meridian_run_sources": {"digest", "run_id"},
		} {
			rows, err := tx.SQL().QueryContext(t.Context(), "SELECT name FROM pragma_table_info(?) ORDER BY name", table)
			if err != nil {
				return err
			}
			var columns []string
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					return err
				}
				columns = append(columns, name)
			}
			if err := errors.Join(rows.Err(), rows.Close()); err != nil {
				return err
			}
			if !slices.Equal(columns, want) {
				t.Errorf("%s keeps %q, want only %q", table, columns, want)
			}
		}
		return nil
	})
	checkIntegrity(t, s)

	bad := a
	bad.Text = []byte("jobs: {}\n")
	if err := s.Tx(t.Context(), func(tx *Tx) error { return tx.KeepTexts(first, []compile.PinnedText{bad}) }); err == nil {
		t.Error("a text that isn't its digest's was kept")
	}
	inTx(t, s, func(tx *Tx) error {
		_, err := tx.exec("UPDATE meridian_sources SET text = ? WHERE digest = ?", []byte("changed"), a.Digest)
		return err
	})
	inTx(t, s, func(tx *Tx) error {
		if _, err := tx.Text(first, a.Digest); !errors.Is(err, ErrMalformed) {
			t.Errorf("a text the store changed: %v, want ErrMalformed", err)
		}
		return nil
	})
	inTx(t, s, func(tx *Tx) error { return tx.EndRun(second, plan.OutcomeSuccess, "") })
	if err := s.Tx(t.Context(), func(tx *Tx) error { return tx.KeepTexts(second, []compile.PinnedText{a}) }); !errors.Is(err, ErrState) {
		t.Errorf("a run that has ended kept a text: %v", err)
	}
}
