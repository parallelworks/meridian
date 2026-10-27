package store

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/coordtest"

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
	path := filepath.Join(coordtest.PrivateDir(t), "meridian.db")
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
	holds := func(secret []byte) []string { return holding(t, files, secret) }
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

// holding returns those of files that hold secret, by their base names.
func holding(t *testing.T, files []string, secret []byte) []string {
	t.Helper()
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

// TestDigestsErased checks that the plain SHA-256 digests that releases
// before kept, of attempts' ends and of actions' requests, are erased from
// the database's file and its write-ahead log: by the migration that
// stopped keeping them; again each time the store opens, and each time a
// run ends, as one that a release before, still running on the database,
// wrote after that migration is; and that an erase finds them by indexes
// of the rows that hold one. An action whose digest was plain reads as
// not keyed, and takes any request for its try as a repeat.
func TestDigestsErased(t *testing.T) {
	path := filepath.Join(coordtest.PrivateDir(t), "meridian.db")
	files := []string{path, path + "-wal"}
	s := openAt(t, path)
	a := claimedAttempt(t, s, 'a')
	inTx(t, s, func(tx *Tx) error {
		return tx.EndAttempt(a.ID, AttemptEnd{Kind: EndCompleted, Outcome: plan.OutcomeSuccess})
	})
	// keepDigests writes digest as the attempt's end's and as its action's,
	// as a release before kept them, and checks that the test can tell the
	// files hold it.
	keepDigests := func(s *Store, digest string) {
		t.Helper()
		end, action := []byte(digest+"-end"), []byte(digest+"-act")
		inTx(t, s, func(tx *Tx) error {
			if _, err := tx.exec("UPDATE meridian_attempts SET complete_digest = ? WHERE id = ?", end, a.ID); err != nil {
				return err
			}
			_, err := tx.exec(`INSERT INTO meridian_actions (attempt_id, step, try, digest, answer, stored_at) VALUES (?, 1, 0, ?, ?, 0)
ON CONFLICT (attempt_id, step, try) DO UPDATE SET digest = excluded.digest`, a.ID, action, []byte(`{"outcome":"success"}`))
			return err
		})
		s.truncateWAL(t.Context())
		for _, d := range [][]byte{end, action} {
			if len(holding(t, files, d)) == 0 {
				t.Fatalf("the files don't hold the digest %q", d)
			}
		}
	}
	erased := func(when, digest string) {
		t.Helper()
		for _, d := range []string{digest + "-end", digest + "-act"} {
			if in := holding(t, files, []byte(d)); len(in) != 0 {
				t.Errorf("%s, %v hold the digest %q", when, in, d)
			}
		}
	}

	i := slices.IndexFunc(migrations, func(m migration) bool { return m.name == "0015_erase_digests.sql" })
	if i < 0 {
		t.Fatal("no migration erases the digests")
	}
	// The store as it was before that migration.
	inTx(t, s, func(tx *Tx) error {
		for _, stmt := range []string{
			"DROP INDEX meridian_attempts_end_digest",
			"DROP INDEX meridian_actions_plain_digest",
			"ALTER TABLE meridian_actions DROP COLUMN keyed",
		} {
			if _, err := tx.exec(stmt); err != nil {
				return err
			}
		}
		return nil
	})
	keepDigests(s, "kept-before-the-migration")
	inTx(t, s, func(tx *Tx) error {
		_, err := tx.tx.ExecContext(tx.ctx, migrations[i].script)
		return err
	})
	s.truncateWAL(t.Context())
	erased("after the migration", "kept-before-the-migration")
	inTx(t, s, func(tx *Tx) error {
		act, err := tx.Action(a.ID, 1, 0)
		if err != nil {
			return err
		}
		if act.Keyed || act.Digest != [32]byte{} || !act.SameRequest([32]byte{1}) {
			t.Errorf("an action whose digest was plain is %+v", act)
		}
		return nil
	})

	keepDigests(s, "written-after-the-migration")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openAt(t, path)
	erased("once the store is opened again", "written-after-the-migration")

	b := claimedAttempt(t, s, 'b')
	inTx(t, s, func(tx *Tx) error { return tx.EndAttempt(b.ID, AttemptEnd{Kind: EndWithdrawn}) })
	keepDigests(s, "written-before-a-run-ends")
	inTx(t, s, func(tx *Tx) error { return tx.EndRun(b.Run, plan.OutcomeCancelled, plan.ReasonCancelled) })
	erased("once a run has ended", "written-before-a-run-ends")

	// Each erase finds the rows by the index of those that hold a digest.
	inTx(t, s, func(tx *Tx) error {
		for table, index := range map[string]string{"meridian_attempts": "meridian_attempts_end_digest", "meridian_actions": "meridian_actions_plain_digest"} {
			stmt := "UPDATE meridian_attempts SET complete_digest = NULL WHERE complete_digest IS NOT NULL"
			if table == "meridian_actions" {
				stmt = "UPDATE meridian_actions SET digest = " + noDigest + " WHERE keyed = 0 AND digest <> " + noDigest
			}
			rows, err := tx.tx.QueryContext(tx.ctx, "EXPLAIN QUERY PLAN "+stmt)
			if err != nil {
				return err
			}
			var plan []string
			for rows.Next() {
				var id, parent, unused int
				var detail string
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					rows.Close()
					return err
				}
				plan = append(plan, detail)
			}
			rows.Close()
			if !slices.ContainsFunc(plan, func(d string) bool { return strings.Contains(d, index) }) {
				t.Errorf("the erase of %s's digests is %v, not by %s", table, plan, index)
			}
		}
		return nil
	})
}
