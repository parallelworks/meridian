package store

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
)

// TestSecureDelete checks that a transaction of the store's runs with
// SQLite's secure_delete on, though a connection starts with it off, so
// that what it deletes is overwritten with zeros.
func TestSecureDelete(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	path := newPath(t)
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
	t.Parallel()
	path := newPath(t)
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

// erasureRig is a store whose warnings a test reads, with a run whose
// secret record holds unit, which a reader can hold a snapshot of as the
// run ends.
type erasureRig struct {
	t     *testing.T
	s     *Store
	path  string
	id    string
	unit  []byte
	logMu sync.Mutex
	log   bytes.Buffer
}

func newErasureRig(t *testing.T) *erasureRig {
	t.Helper()
	r := &erasureRig{t: t, path: newPath(t), unit: []byte("a-sealed-record-held.")}
	s, err := Open(t.Context(), r.path, &Options{Log: slog.New(slog.NewTextHandler(lockedWriter{&r.logMu, &r.log}, &slog.HandlerOptions{Level: slog.LevelWarn}))})
	if err != nil {
		t.Fatal(err)
	}
	r.s = s
	inTx(t, s, func(tx *Tx) error {
		var err error
		if r.id, err = tx.NewRunID(); err != nil {
			return err
		}
		if err := tx.InsertRun(NewRun{ID: r.id, Plan: testPlan(t), DefaultGroup: "local", RunDir: "/r", WorkflowID: fakeJobID(0)}); err != nil {
			return err
		}
		return tx.InsertRecord(r.id, 1, []byte("the-wrapped-data-key"), bytes.Repeat(r.unit, 600))
	})
	return r
}

// hold has a reader hold a snapshot of the store from before the run
// ends, as a follower's transaction does while it ends, until release is
// called, which returns the reader's error.
func (r *erasureRig) hold() (release func() error) {
	reading, done := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- r.s.View(context.WithoutCancel(r.t.Context()), func(tx *Tx) error {
			if _, err := tx.Run(r.id); err != nil {
				return err
			}
			close(reading)
			<-done
			return nil
		})
	}()
	<-reading
	return sync.OnceValue(func() error {
		close(done)
		return <-result
	})
}

// erased reports whether the write-ahead log is truncated, and neither
// file holds the record.
func (r *erasureRig) erased() bool {
	info, err := os.Stat(r.path + "-wal")
	return err == nil && info.Size() == 0 && len(holding(r.t, []string{r.path, r.path + "-wal"}, r.unit)) == 0
}

func (r *erasureRig) warnings() string {
	r.logMu.Lock()
	defer r.logMu.Unlock()
	return r.log.String()
}

// TestRecordErasedPastReader checks that a run's secret record is erased
// from the write-ahead log, though a reader, as a follower's transaction,
// kept the log from being truncated as the run ended, and for a while
// after: the store tries again until it is, with no other transaction of
// its own to prompt it, within about a second of the reader being done,
// and says nothing of a reader it waited out. Before, one try was all,
// and the log held the record until the next run ended, or the store
// opened again, and the store warned of it, which meridian run printed.
func TestRecordErasedPastReader(t *testing.T) {
	t.Parallel()
	r := newErasureRig(t)
	t.Cleanup(func() {
		if err := r.s.Close(); err != nil {
			t.Error(err)
		}
	})
	release := r.hold()
	inTx(t, r.s, func(tx *Tx) error { return tx.EndRun(r.id, plan.OutcomeSuccess, "") })
	// Held past a few tries, whose waits double up to a second.
	time.Sleep(1500 * time.Millisecond) //nolint:forbidigo // the reader holds the log past the store's first tries
	if r.erased() {
		t.Fatal("the log was truncated though a reader held it, so the test can't tell")
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	released := time.Now()
	for !r.erased() {
		if time.Since(released) > 3*time.Second {
			t.Fatalf("%v after the reader was done, the log isn't truncated, or holds the record", time.Since(released))
		}
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo // polls the files
	}
	if w := r.warnings(); w != "" {
		t.Errorf("the store warned of a reader it waited out:\n%s", w)
	}
}

// TestCloseEndsEraser checks that a store closing while a reader still
// keeps the log from being truncated ends the goroutine that tries again,
// and waits for it, before it returns; tries once more; and warns that
// the log may hold the record until the next Open, which truncates it.
func TestCloseEndsEraser(t *testing.T) {
	t.Parallel()
	r := newErasureRig(t)
	var ended atomic.Bool
	r.s.erased = func() { ended.Store(true) }
	release := r.hold()
	defer func() { _ = release() }()
	inTx(t, r.s, func(tx *Tx) error { return tx.EndRun(r.id, plan.OutcomeSuccess, "") })
	closed := make(chan error, 1)
	go func() { closed <- r.s.Close() }()
	// Close tries once more, which the reader holds up, and warns, before
	// it closes the database, which waits for the reader.
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(r.warnings(), "the next Open truncates it") {
		if time.Now().After(deadline) {
			t.Fatalf("Close didn't warn that the log may hold the record; it logged:\n%s", r.warnings())
		}
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo // polls the log
	}
	if !ended.Load() {
		t.Error("Close tried the truncation again before the goroutine that tries it had ended")
	}
	if err := release(); err != nil && !strings.Contains(err.Error(), "closed") {
		t.Error(err)
	}
	if err := <-closed; err != nil {
		t.Error(err)
	}
	// The next Open truncates it.
	s, err := Open(t.Context(), r.path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !r.erased() {
		t.Error("the next Open didn't truncate the log")
	}
	if err := s.Close(); err != nil {
		t.Error(err)
	}
}

// A lockedWriter writes to w under mu, for a log that goroutines write.
type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
