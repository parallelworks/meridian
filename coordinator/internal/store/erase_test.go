package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"io/fs"
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

	// An erase says whether it found any, which the store warns of where
	// it can't truncate the log after.
	keepDigests(s, "found-by-an-erase")
	inTx(t, s, func(tx *Tx) error {
		for i, want := range []bool{true, false} {
			if found, err := tx.eraseDigests(); err != nil || found != want {
				t.Errorf("erase %d found any: %t, %v; want %t", i+1, found, err, want)
			}
		}
		return nil
	})

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
// run ends; and the units of the other runs' records (addRun).
type erasureRig struct {
	t     *testing.T
	s     *Store
	path  string
	id    string
	unit  []byte
	units [][]byte
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
	r.id = r.addRun(r.unit)
	return r
}

// addRun stores a run whose secret record holds unit, and returns its ID.
func (r *erasureRig) addRun(unit []byte) string {
	r.t.Helper()
	r.units = append(r.units, unit)
	var id string
	inTx(r.t, r.s, func(tx *Tx) error {
		var err error
		if id, err = tx.NewRunID(); err != nil {
			return err
		}
		if err := tx.InsertRun(NewRun{ID: id, Plan: testPlan(r.t), DefaultGroup: "local", RunDir: "/r", WorkflowID: fakeJobID(len(r.units) * 10)}); err != nil {
			return err
		}
		return tx.InsertRecord(id, 1, []byte("the-wrapped-data-key"), bytes.Repeat(unit, 600))
	})
	return id
}

// end ends the run id, which deletes its record.
func (r *erasureRig) end(id string) {
	r.t.Helper()
	inTx(r.t, r.s, func(tx *Tx) error { return tx.EndRun(id, plan.OutcomeSuccess, "") })
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

// erased reports whether the write-ahead log is truncated, or gone, as
// the store's last connection leaves it once the store has closed, and
// neither file holds any of the records.
func (r *erasureRig) erased() bool {
	files := []string{r.path, r.path + "-wal"}
	info, err := os.Stat(r.path + "-wal")
	switch {
	case errors.Is(err, fs.ErrNotExist):
		files = files[:1]
	case err != nil:
		r.t.Fatal(err)
	case info.Size() != 0:
		return false
	}
	for _, unit := range r.units {
		if len(holding(r.t, files, unit)) != 0 {
			return false
		}
	}
	return true
}

// waitErased waits for erased, for at most within, from now.
func (r *erasureRig) waitErased(within time.Duration) {
	r.t.Helper()
	start := time.Now()
	for !r.erased() {
		if time.Since(start) > within {
			r.t.Fatalf("%v on, the log isn't truncated, or holds a record", time.Since(start).Round(time.Millisecond))
		}
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo // polls the files
	}
}

// pendingState returns the store's pending and erasing.
func (r *erasureRig) pendingState() (pending, erasing bool) {
	r.s.walMu.Lock()
	defer r.s.walMu.Unlock()
	return r.s.pending, r.s.erasing
}

// beginClose does what Close does before its last try: it stops the
// goroutine that tries again, and waits for it.
func (r *erasureRig) beginClose() {
	r.s.walMu.Lock()
	r.s.close()
	r.s.walMu.Unlock()
	r.s.erasers.Wait()
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
	r.end(r.id)
	// Held past a few tries, whose waits double up to a second.
	time.Sleep(1500 * time.Millisecond) //nolint:forbidigo // the reader holds the log past the store's first tries
	if r.erased() {
		t.Fatal("the log was truncated though a reader held it, so the test can't tell")
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	r.waitErased(3 * time.Second)
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
	r.end(r.id)
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

// TestEraseRetryWaitsForNoOne checks that the tries the store makes again
// don't wait for a reader that holds the log, as a truncation that waits
// holds back writers meanwhile: each runs its checkpoint with a busy
// timeout of 0, and leaves its connection's busy timeout as it found it.
func TestEraseRetryWaitsForNoOne(t *testing.T) {
	t.Parallel()
	r := newErasureRig(t)
	t.Cleanup(func() {
		if err := r.s.Close(); err != nil {
			t.Error(err)
		}
	})
	var (
		mu       sync.Mutex
		timeouts []int64
	)
	r.s.checkpointing = func(conn *sql.Conn) {
		var timeout int64
		if err := conn.QueryRowContext(context.WithoutCancel(t.Context()), "PRAGMA busy_timeout").Scan(&timeout); err != nil {
			t.Error(err)
		}
		mu.Lock()
		defer mu.Unlock()
		timeouts = append(timeouts, timeout)
	}
	release := r.hold()
	r.end(r.id)
	// Two connections at most from here: the reader's, and the one each
	// try takes, which the check after takes again.
	r.s.db.SetMaxOpenConns(2)
	// The goroutine that tries again has tried, past the first, held up.
	deadline := time.Now().Add(30 * time.Second)
	for {
		mu.Lock()
		n := len(timeouts)
		mu.Unlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the store didn't try the truncation again")
		}
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo // polls the tries
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	r.waitErased(3 * time.Second)
	mu.Lock()
	for _, timeout := range timeouts {
		if timeout != 0 {
			t.Errorf("a try made again ran its checkpoint with a busy timeout of %d ms, want 0", timeout)
		}
	}
	mu.Unlock()
	for range 2 {
		conn, err := r.s.db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		var timeout int64
		if err := conn.QueryRowContext(t.Context(), "PRAGMA busy_timeout").Scan(&timeout); err != nil {
			t.Fatal(err)
		}
		if timeout == 0 {
			t.Error("a connection of the store's was left without its busy timeout")
		}
		defer conn.Close()
	}
}

// TestEraseHeldUpAsOneSucceeds checks that a try that truncates the log
// as another, of a record deleted meanwhile, is held up, doesn't clear
// what is pending: the goroutine that tries again goes on until it
// truncates the log after the record was deleted. Before, it cleared it
// and ended, and nothing tried again.
func TestEraseHeldUpAsOneSucceeds(t *testing.T) {
	t.Parallel()
	r := newErasureRig(t)
	t.Cleanup(func() {
		if err := r.s.Close(); err != nil {
			t.Error(err)
		}
	})
	next := r.endHeldUpAsTruncated()
	var tries atomic.Int64
	r.s.checkpointing = func(*sql.Conn) { tries.Add(1) }
	release := r.hold()
	r.end(r.id)
	if err := release(); err != nil {
		t.Fatal(err)
	}
	var e heldUpEnd
	select {
	case e = <-next:
	case <-time.After(30 * time.Second):
		t.Fatal("the goroutine didn't truncate the log once the first reader was done")
	}
	if e.err != nil {
		t.Fatal(e.err)
	}
	// The goroutine tries again, past what was held up as it truncated
	// the log, rather than ending.
	for after, deadline := tries.Load(), time.Now().Add(5*time.Second); tries.Load() == after; {
		if time.Now().After(deadline) {
			t.Fatal("the goroutine didn't try again once a truncation was held up as it truncated the log")
		}
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo // polls the tries
	}
	if pending, erasing := r.pendingState(); !pending || !erasing {
		t.Errorf("with a truncation held up as another succeeded, pending is %v, and tried again %v, want both", pending, erasing)
	}
	if err := e.release(); err != nil {
		t.Fatal(err)
	}
	r.waitErased(3 * time.Second)
}

// TestEraseHeldUpAsOneSucceedsAsStoreCloses checks the same of the try a
// run's end makes as the store closes, when none tries again: it leaves
// pending what another, held up as it ran, set, which has Close try once
// more.
func TestEraseHeldUpAsOneSucceedsAsStoreCloses(t *testing.T) {
	t.Parallel()
	r := newErasureRig(t)
	next := r.endHeldUpAsTruncated()
	r.beginClose()
	r.end(r.id)
	e := <-next
	if e.err != nil {
		t.Fatal(e.err)
	}
	if pending, _ := r.pendingState(); !pending {
		t.Error("with a truncation held up as another succeeded, nothing is pending")
	}
	if err := e.release(); err != nil {
		t.Fatal(err)
	}
	if err := r.s.Close(); err != nil {
		t.Fatal(err)
	}
	if !r.erased() {
		t.Error("the log isn't truncated, or holds a record")
	}
	if w := r.warnings(); w != "" {
		t.Errorf("the store warned:\n%s", w)
	}
}

// A heldUpEnd is a run that ended as a reader held the log, and the
// reader's release.
type heldUpEnd struct {
	release func() error
	err     error
}

// endHeldUpAsTruncated adds a run that, as the store's first try that
// succeeds has truncated the log, before it reads what was held up,
// ends as a reader holds the log, so that its own try is held up.
func (r *erasureRig) endHeldUpAsTruncated() <-chan heldUpEnd {
	other := r.addRun([]byte("another-sealed-record."))
	next := make(chan heldUpEnd, 1)
	var once sync.Once
	r.s.truncated = func() {
		once.Do(func() {
			release := r.hold()
			err := r.s.Tx(r.t.Context(), func(tx *Tx) error { return tx.EndRun(other, plan.OutcomeSuccess, "") })
			next <- heldUpEnd{release, err}
		})
	}
	return next
}

// TestErasePendingWarnsOnce checks that the store warns, once, that the
// log may still hold a secret record once a reader has kept it from being
// truncated for a while, and goes on trying until it is.
func TestErasePendingWarnsOnce(t *testing.T) {
	t.Parallel()
	r := newErasureRig(t)
	t.Cleanup(func() {
		if err := r.s.Close(); err != nil {
			t.Error(err)
		}
	})
	r.s.warnAfter = 100 * time.Millisecond
	release := r.hold()
	r.end(r.id)
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(r.warnings(), "for a while") {
		if time.Now().After(deadline) {
			t.Fatalf("the store didn't warn of an erasure held up for a while; it logged:\n%s", r.warnings())
		}
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo // polls the log
	}
	// Past more tries.
	time.Sleep(1500 * time.Millisecond) //nolint:forbidigo // the reader holds the log past more of the store's tries
	if n := strings.Count(r.warnings(), "for a while"); n != 1 {
		t.Errorf("the store warned %d times of an erasure held up, want once:\n%s", n, r.warnings())
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	r.waitErased(3 * time.Second)
}

// TestEraseAsStoreCloses checks that a truncation held up as the store
// closes, after Close has stopped the goroutine that tries again but
// before its last try, is covered by that try: nothing is pending until
// a run ends then, and the reader is done by the last try, which
// truncates the log.
func TestEraseAsStoreCloses(t *testing.T) {
	t.Parallel()
	r := newErasureRig(t)
	other := r.addRun([]byte("another-sealed-record."))
	r.end(r.id)
	if pending, _ := r.pendingState(); pending {
		t.Fatal("a truncation that no reader held up is pending")
	}
	r.beginClose()
	// A run that ends now: its truncation is held up, and none tries again.
	release := r.hold()
	r.end(other)
	if pending, erasing := r.pendingState(); !pending || erasing {
		t.Errorf("a truncation held up as the store closes is pending %v, tried again %v, want pending, and not tried again", pending, erasing)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	if err := r.s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.s.Close(); err != nil {
		t.Errorf("a second Close = %v", err)
	}
	if !r.erased() {
		t.Error("Close's last try didn't truncate the log")
	}
	if w := r.warnings(); w != "" {
		t.Errorf("the store warned:\n%s", w)
	}
}

// TestEraseAfterLastTry checks that a truncation held up once the store
// has made its last try, which can't cover it, warns itself that the log
// may hold the record until the next Open.
func TestEraseAfterLastTry(t *testing.T) {
	t.Parallel()
	r := newErasureRig(t)
	t.Cleanup(func() {
		if err := r.s.Close(); err != nil {
			t.Error(err)
		}
	})
	// As Close has made its last try, but not yet closed the database.
	r.beginClose()
	r.s.walMu.Lock()
	r.s.lastTried = true
	r.s.walMu.Unlock()
	release := r.hold()
	r.end(r.id)
	if w := r.warnings(); !strings.Contains(w, "the next Open truncates it") {
		t.Errorf("a truncation held up past the last try didn't warn; the store logged:\n%s", w)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
}

// TestCloseWarnsOfItsOwn checks that a store warns, as it closes, that the
// write-ahead log may still hold a secret record only where its own
// process deleted one, which another process's reader kept it from
// truncating: a short command, as a meridian session url run beside a
// meridian run, which deletes none, says nothing, though the truncation
// its Open tries, for a process that ended before, is held up too; one
// that ends a run, which deletes its record, warns.
func TestCloseWarnsOfItsOwn(t *testing.T) {
	t.Parallel()
	r := newErasureRig(t)
	t.Cleanup(func() {
		if err := r.s.Close(); err != nil {
			t.Error(err)
		}
	})
	release := r.hold()
	defer func() { _ = release() }()
	open := func() (*Store, *lockedWriter, *bytes.Buffer) {
		t.Helper()
		var mu sync.Mutex
		var buf bytes.Buffer
		w := &lockedWriter{&mu, &buf}
		s, err := Open(t.Context(), r.path, &Options{Log: slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelWarn}))})
		if err != nil {
			t.Fatal(err)
		}
		return s, w, &buf
	}
	read := func(w *lockedWriter, buf *bytes.Buffer) string {
		w.mu.Lock()
		defer w.mu.Unlock()
		return buf.String()
	}

	short, w, buf := open()
	short.walMu.Lock()
	pending, here := short.pending, short.erasedHere
	short.walMu.Unlock()
	if !pending || here {
		t.Errorf("the truncation Open tried, held up, is pending %t, of its own %t; want pending, not its own", pending, here)
	}
	// Tried again for a while, it says nothing either.
	short.walMu.Lock()
	short.warnAfter = 10 * time.Millisecond
	short.walMu.Unlock()
	time.Sleep(300 * time.Millisecond) //nolint:forbidigo // the store tries again, held up, past warnAfter
	inTx(t, short, func(tx *Tx) error {
		_, err := tx.NewRunID()
		return err
	})
	if err := short.Close(); err != nil {
		t.Fatal(err)
	}
	if got := read(w, buf); got != "" {
		t.Errorf("a store that deleted no secret record warned as it closed:\n%s", got)
	}

	ending, w, buf := open()
	var id string
	inTx(t, ending, func(tx *Tx) error {
		var err error
		if id, err = tx.NewRunID(); err != nil {
			return err
		}
		if err := tx.InsertRun(NewRun{ID: id, Plan: testPlan(t), DefaultGroup: "local", RunDir: "/r", WorkflowID: fakeJobID(990)}); err != nil {
			return err
		}
		return tx.InsertRecord(id, 1, []byte("the-wrapped-data-key"), []byte("another-secret-record"))
	})
	inTx(t, ending, func(tx *Tx) error { return tx.EndRun(id, plan.OutcomeSuccess, "") })
	if err := ending.Close(); err != nil {
		t.Fatal(err)
	}
	if got := read(w, buf); !strings.Contains(got, "the next Open truncates it") {
		t.Errorf("a store that deleted a secret record, which it couldn't truncate, didn't warn as it closed:\n%s", got)
	}
}

// TestCloseWarnsOfLeftRecord checks the store's mark of a secret record
// the write-ahead log may hold, which outlives a process that ended before
// it truncated the log: a later process whose Open can't truncate it
// either, as a short command beside a run going on, warns of it as it
// closes; once a truncation has finished, which deletes the mark, one that
// can't truncate says nothing. The mark is a count, which holds nothing of
// what was deleted.
func TestCloseWarnsOfLeftRecord(t *testing.T) {
	t.Parallel()
	r := newErasureRig(t)
	t.Cleanup(func() {
		if err := r.s.Close(); err != nil {
			t.Error(err)
		}
	})
	open := func() (*Store, func() string) {
		t.Helper()
		var mu sync.Mutex
		var buf bytes.Buffer
		w := &lockedWriter{&mu, &buf}
		s, err := Open(t.Context(), r.path, &Options{Log: slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelWarn}))})
		if err != nil {
			t.Fatal(err)
		}
		return s, func() string {
			mu.Lock()
			defer mu.Unlock()
			return buf.String()
		}
	}
	mark := func() string {
		t.Helper()
		var value string
		inTx(t, r.s, func(tx *Tx) error {
			err := tx.queryRow("SELECT value FROM meridian_store WHERE key = '" + walErasePending + "'").Scan(&value)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		})
		return value
	}

	release := r.hold()
	// A process that ends a run, and ends itself before it can truncate
	// the log: its record stays in it, and so does the mark.
	left, _ := open()
	var id string
	inTx(t, left, func(tx *Tx) error {
		var err error
		if id, err = tx.NewRunID(); err != nil {
			return err
		}
		if err := tx.InsertRun(NewRun{ID: id, Plan: testPlan(t), DefaultGroup: "local", RunDir: "/r", WorkflowID: fakeJobID(991)}); err != nil {
			return err
		}
		return tx.InsertRecord(id, 1, []byte("the-wrapped-data-key"), []byte("a-record-left-behind"))
	})
	inTx(t, left, func(tx *Tx) error { return tx.EndRun(id, plan.OutcomeSuccess, "") })
	if err := left.Close(); err != nil {
		t.Fatal(err)
	}
	if got := mark(); got != "1" {
		t.Fatalf("the mark of a record left in the log is %q, want a count of 1", got)
	}
	short, warnings := open()
	if err := short.Close(); err != nil {
		t.Fatal(err)
	}
	if got := warnings(); !strings.Contains(got, "the next Open truncates it") {
		t.Errorf("a store whose Open couldn't truncate a record left in the log didn't warn:\n%s", got)
	}

	// Once a truncation has finished, the mark is gone, and a store that
	// can't truncate says nothing.
	if err := release(); err != nil {
		t.Fatal(err)
	}
	cleared, _ := open()
	if err := cleared.Close(); err != nil {
		t.Fatal(err)
	}
	if got := mark(); got != "" {
		t.Errorf("once the log was truncated, the mark is %q", got)
	}
	release = r.hold()
	defer func() { _ = release() }()
	quiet, warnings := open()
	if err := quiet.Close(); err != nil {
		t.Fatal(err)
	}
	if got := warnings(); got != "" {
		t.Errorf("once the mark was gone, a store that couldn't truncate warned:\n%s", got)
	}
}

// TestOpenMarksDigests checks that an Open that erases plain digests, a
// release before kept, marks them as a deletion the write-ahead log may
// hold, where it can't truncate the log, as a run's end does its record.
func TestOpenMarksDigests(t *testing.T) {
	t.Parallel()
	path := newPath(t)
	s := openAt(t, path)
	a := claimedAttempt(t, s, 'a')
	inTx(t, s, func(tx *Tx) error {
		if err := tx.EndAttempt(a.ID, AttemptEnd{Kind: EndCompleted, Outcome: plan.OutcomeSuccess}); err != nil {
			return err
		}
		_, err := tx.exec("UPDATE meridian_attempts SET complete_digest = ? WHERE id = ?", []byte("a-plain-digest"), a.ID)
		return err
	})
	// Another process reads as this one opens.
	reading, done, result := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		result <- s.View(context.WithoutCancel(t.Context()), func(tx *Tx) error {
			if _, err := tx.Attempt(a.ID); err != nil {
				return err
			}
			close(reading)
			<-done
			return nil
		})
	}()
	<-reading
	again := openAt(t, path)
	var value string
	if err := again.View(t.Context(), func(tx *Tx) error {
		return tx.queryRow("SELECT value FROM meridian_store WHERE key = '" + walErasePending + "'").Scan(&value)
	}); err != nil || value != "1" {
		t.Errorf("the mark of digests an Open erased, which it couldn't truncate, is %q, %v", value, err)
	}
	close(done)
	if err := <-result; err != nil {
		t.Fatal(err)
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
