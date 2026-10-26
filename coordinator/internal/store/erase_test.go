package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/parallelworks/meridian/plan"
)

// TestSecureDelete checks that a transaction of the store's runs with
// SQLite's secure_delete on, though a connection starts with it off, so
// that what it deletes is overwritten with zeros.
func TestSecureDelete(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	// No connection is kept idle, so each transaction runs on a
	// connection of its own, just opened.
	s.db.SetMaxIdleConns(0)
	conn, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var fresh int
	if err := conn.QueryRowContext(t.Context(), "PRAGMA secure_delete").Scan(&fresh); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if fresh != 0 {
		t.Fatalf("a connection just opened has secure_delete %d, want SQLite's default, 0, for this test to tell", fresh)
	}
	inTx(t, s, func(tx *Tx) error {
		var on int
		if err := tx.tx.QueryRowContext(tx.ctx, "PRAGMA secure_delete").Scan(&on); err != nil {
			return err
		}
		if on != 1 {
			t.Errorf("a transaction of the store's has secure_delete %d, want 1", on)
		}
		return tx.EndRun(id, plan.OutcomeSuccess, "")
	})
}

// TestRecordErased checks that a run's secret record, and its data key,
// are in neither the database's file nor its write-ahead log once the run
// has ended, as the earlier generations of the record aren't.
func TestRecordErased(t *testing.T) {
	path := filepath.Join(privateDir(t), "meridian.db")
	s := openAt(t, path)
	p := testPlan(t)
	// Large enough for SQLite to keep on overflow pages of their own, which
	// split them, so the files are searched for any piece of one: the
	// unit it repeats.
	firstUnit, secondUnit, partUnit := []byte("first-sealed-record."), []byte("second-sealed-record."), []byte("a-job's-sealed-part.")
	first, second, part := bytes.Repeat(firstUnit, 600), bytes.Repeat(secondUnit, 600), bytes.Repeat(partUnit, 600)
	key := []byte("the-wrapped-data-key")
	var id string
	inTx(t, s, func(tx *Tx) error {
		var err error
		if id, err = tx.NewRunID(); err != nil {
			return err
		}
		if err := tx.InsertRun(NewRun{ID: id, Plan: p, DefaultGroup: "local", RunDir: "/r", WorkflowID: fakeJobID(0)}); err != nil {
			return err
		}
		if err := tx.InsertJob(NewJob{Run: id, Name: "build", Key: "build", SupervisorID: fakeJobID(1)}); err != nil {
			return err
		}
		return tx.InsertRecord(id, 1, key, first)
	})
	inTx(t, s, func(tx *Tx) error { return tx.Reseal(id, 2, second) })
	inTx(t, s, func(tx *Tx) error { return tx.PutJobRecord(id, "build", 1, part) })

	files := []string{path, path + "-wal"}
	holds := func(secret []byte) []string {
		var in []string
		for _, f := range files {
			data, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(data, secret) {
				in = append(in, filepath.Base(f))
			}
		}
		return in
	}
	// The test can tell: before the run ends, the files hold the record.
	for _, secret := range [][]byte{secondUnit, partUnit, key} {
		if len(holds(secret)) == 0 {
			t.Fatalf("before the run ends, the files don't hold %.20q", secret)
		}
	}

	inTx(t, s, func(tx *Tx) error { return tx.EndRun(id, plan.OutcomeSuccess, "") })
	check := func(when string) {
		t.Helper()
		for _, secret := range [][]byte{firstUnit, secondUnit, partUnit, key} {
			if in := holds(secret); len(in) != 0 {
				t.Errorf("%s, %v hold %.20q", when, in, secret)
			}
		}
	}
	check("once the run has ended")
	if info, err := os.Stat(path + "-wal"); err != nil || info.Size() != 0 {
		t.Errorf("once the run has ended, the write-ahead log is %v (%v), want truncated", info.Size(), err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	files = files[:1]
	check("once the store is closed")
}
