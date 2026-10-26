// Package store is the coordinator's store: hopper's tables and Meridian's
// own, in one SQLite database (docs/design.md, On hopper).
//
// hopper keeps a run's scheduling, as a hopper workflow: Start, a
// supervisor for each of the run's jobs and matrix instances, and the run's
// finisher. Meridian's tables keep what hopper doesn't model: the run with
// its plan, the run's sealed secret record, each job's result, each attempt
// with its lease, its fence and its credentials, and the endpoints that
// workers reach the coordinator through. Both share the database and its
// transactions, so what a phase stores in Meridian's tables commits with
// what it does to its hopper jobs, or neither does: a hopper call made in
// a transaction of the store's (Tx.SQL) is in it.
//
// The store's clock (Tx.Now) is hopper's: the host's, which every process
// that opens the database shares, since the database is on a local disk,
// and never earlier, in one process, than a time it read before. Every
// time the store records is by it.
//
// A run's ID is its number in the store, from 1, which only grows, and the
// store has an ID of its own, random, which a run's secret record is
// bound to beside the run's, so that a record can't be opened for a run of
// the same number in another store, or in this one remade.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parallelworks/hopper/driver/hoppersqlite"
	"github.com/parallelworks/hopper/hoppermigrate"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
)

// The errors of the store's calls, besides those of the database.
var (
	// ErrNotFound is what a call returns for a run, a job, an attempt, a
	// record or an endpoint the store doesn't have, and for a bootstrap
	// token or a credential that is no attempt's.
	ErrNotFound = errors.New("store: not found")
	// ErrState is what a call returns for a run, a job or an attempt that
	// isn't where its life allows the call, such as a job that has ended
	// for a new attempt, or a run with attempts that haven't ended for its
	// own end.
	ErrState = errors.New("store: not in a state that allows it")
	// ErrFenced is what a call returns for an attempt that no longer holds
	// its job: one that has ended, or whose job has, or that a newer
	// attempt superseded. Nothing it reports is stored.
	ErrFenced = errors.New("store: the attempt no longer holds its job")
	// ErrCredential is what Claim returns for an attempt claimed with
	// another credential.
	ErrCredential = errors.New("store: the attempt was claimed with another credential")
	// ErrLapsed is what a call with an attempt's credential returns once
	// the lease of an attempt whose job fences itself has lapsed: its
	// credential lapses with it.
	ErrLapsed = errors.New("store: the attempt's lease has lapsed")
	// ErrMalformed is what a call returns for what the store holds that
	// this release doesn't read as it would write it, such as a job's
	// result with a member it doesn't know.
	ErrMalformed = errors.New("store: malformed")
)

// Options say how to open a store. A nil *Options means the defaults.
type Options struct {
	// Log hears of what the store does that its calls don't say: the
	// migrations Open applies, and a checkpoint of the write-ahead log
	// that couldn't finish. Nil means none.
	Log *slog.Logger
}

// A Store is the coordinator's store, open. Its methods are safe for
// concurrent use, and so is the database: several processes may open it.
type Store struct {
	driver *hoppersqlite.Driver
	db     *sql.DB
	log    *slog.Logger
	// file is the database's file, as SQLite opened it, its links
	// resolved: the same for every process that opens the database.
	file string
	id   string

	// holds are the endpoints this store holds, kept here until they are
	// released, or the store closed: a lock's file that nothing reaches
	// would be closed by the garbage collector, which releases the lock.
	holdsMu sync.Mutex
	holds   map[*Hold]struct{}
}

// openSQLite is hoppersqlite.OpenContext, which tests replace.
var openSQLite = hoppersqlite.OpenContext

// Open opens the store whose database is the file at path, creating it if
// it isn't there, readable only by its user, and brings its schema,
// hopper's and Meridian's, up to this release's, as several processes may
// do at once. It waits for another process that holds the database for as
// long as ctx allows, and while SQLite reports the database busy as it
// opens it or migrates it, as it can when processes open a new database at
// once, for up to openBusyFor.
//
// Whoever can write the database can claim an attempt, and with it the
// private values its job reads, so Open trusts nothing at path that
// another user could have put there or could change (checkFiles), whoever
// chose the path. What is wrong is a problem: of a kind
// safepath's checks give, such as directory_not_private or
// file_not_private; of kind network_filesystem for a database on a
// network filesystem, where SQLite can't lock it reliably; and of kind
// database_too_new for one whose schema a newer release of Meridian
// brought past what this one knows. Any other error is the operating
// system's or SQLite's.
func Open(ctx context.Context, path string, opts *Options) (*Store, error) {
	log := slog.New(slog.DiscardHandler)
	if opts != nil && opts.Log != nil {
		log = opts.Log
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := checkFiles(path); err != nil {
		return nil, err
	}
	created, err := createPrivate(path)
	if err != nil {
		return nil, err
	}
	start, wait := time.Now(), openBusyFirstWait
	for {
		s, err := openOnce(ctx, path, log)
		switch {
		case errors.Is(err, hoppersqlite.ErrNetworkFilesystem):
			if created {
				_ = os.Remove(path) //nolint:gosec // the empty file Open made just now
			}
			return nil, networkFilesystem(path, err)
		case !busy(err):
			return s, err
		case time.Since(start)+wait > openBusyFor:
			return nil, fmt.Errorf("store: the database was busy for %s as it opened: %w", time.Since(start).Round(time.Millisecond), err)
		}
		log.DebugContext(ctx, "store: the database is busy as it opens; trying again", "error", err, "after", wait)
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, fmt.Errorf("store: the database was busy for %s as it opened: %w", time.Since(start).Round(time.Millisecond), errors.Join(ctx.Err(), err))
		case <-t.C:
		}
		wait = min(2*wait, openBusyMaxWait)
	}
}

// openBusyFor is the longest Open tries for while SQLite reports the
// database busy as it opens it, and openBusyFirstWait and openBusyMaxWait
// the shortest and the longest it waits between tries, doubling from the
// one to the other. A connection that hoppersqlite opens sets itself up,
// before its statements wait for another's lock, with statements that wait
// for one no more than 50 ms: when processes open a new database at once,
// one of them sets its connection up while another holds the database to
// turn its write-ahead log on.
const (
	openBusyFor       = time.Minute
	openBusyFirstWait = 10 * time.Millisecond
	openBusyMaxWait   = time.Second
)

// openOnce opens the store at path once, and brings its schema up to this
// release's.
func openOnce(ctx context.Context, path string, log *slog.Logger) (*Store, error) {
	d, err := openSQLite(ctx, path, nil)
	if err != nil {
		return nil, err
	}
	s := &Store{driver: d, db: d.DB(), log: log}
	if err := s.setUp(ctx, log); err != nil {
		return nil, errors.Join(err, d.Close())
	}
	return s, nil
}

// busy reports whether err is SQLite's, of its busy family: another
// connection held a lock it needed for longer than it waited.
func busy(err error) bool {
	var e *sqlite.Error
	return errors.As(err, &e) && e.Code()&0xff == sqlite3.SQLITE_BUSY
}

// sqliteFiles are the suffixes of the files SQLite keeps beside a
// database: its write-ahead log and the log's index, and a rollback
// journal, which it would play back as it opens the database.
var sqliteFiles = []string{"-wal", "-shm", "-journal"}

// checkFiles checks the database at path, an absolute path, before SQLite
// opens anything: that no other user can change the directories on the way
// to it, and that its directory is the user's, which no other user can
// read or change (safepath.CheckDir); and that the database, and each file
// SQLite keeps beside it, is, where it is there, a regular file of the
// user's, not a link, which no other user can read or change
// (safepath.CheckFileInfo). The directory can be a link to one elsewhere,
// whose way is checked too.
func checkFiles(path string) error {
	if err := safepath.CheckDir(filepath.Dir(path), safepath.Private); err != nil {
		return err
	}
	for _, f := range append([]string{path}, suffixed(path, sqliteFiles)...) {
		info, err := os.Lstat(f) //nolint:gosec // the path being checked
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if err := safepath.CheckFileInfo(f, info, safepath.Private); err != nil {
			return err
		}
	}
	return nil
}

// suffixed returns path with each of suffixes.
func suffixed(path string, suffixes []string) []string {
	out := make([]string, len(suffixes))
	for i, s := range suffixes {
		out[i] = path + s
	}
	return out
}

// createPrivate creates the database's file, readable and writable only
// by its user, if nothing is at path, so that SQLite opens it rather than
// creating it as others may read it; the files it keeps beside it take its
// mode. It reports whether it created the file.
func createPrivate(path string) (bool, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // the coordinator's own database
	if errors.Is(err, fs.ErrExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, f.Close()
}

// networkFilesystem is the problem of a database that hoppersqlite refused,
// with err, for being on a network filesystem: the path it checked and the
// filesystem it found, as its error says them, around the last "is on";
// or if it doesn't, the database's directory and the error.
func networkFilesystem(path string, err error) error {
	dir, kind := filepath.Dir(path), err.Error()
	if _, rest, ok := strings.Cut(err.Error(), hoppersqlite.ErrNetworkFilesystem.Error()+": "); ok {
		if checked, found, ok := strings.CutLast(rest, " is on "); ok {
			dir, kind = checked, found
		}
	}
	return problems.NetworkFilesystem.Problem(problems.Params{"path": dir, "type": kind})
}

// setUp migrates a store just opened, and reads its ID and its file.
func (s *Store) setUp(ctx context.Context, log *slog.Logger) error {
	if err := s.View(ctx, func(tx *Tx) error {
		var err error
		if s.file, err = tx.dbFile(); err == nil && !filepath.IsAbs(s.file) {
			err = fmt.Errorf("store: SQLite opened %q, not a file", s.file)
		}
		return err
	}); err != nil {
		return err
	}
	// hoppermigrate.Up leaves a schema a newer release of hopper brought
	// past what this one knows as it is, which this release's hopper may
	// not read: it is refused before Up, and after, in case a newer
	// release migrated it meanwhile.
	if err := s.checkHopperSchema(ctx); err != nil {
		return err
	}
	if _, err := hoppermigrate.Up(ctx, s.driver, &hoppermigrate.Options{Logger: log}); err != nil {
		return err
	}
	if err := s.checkHopperSchema(ctx); err != nil {
		return err
	}
	if err := s.migrate(ctx, log); err != nil {
		return err
	}
	if err := s.View(ctx, func(tx *Tx) error {
		if err := tx.tx.QueryRowContext(ctx, "SELECT value FROM meridian_store WHERE key = 'store_id'").Scan(&s.id); err != nil {
			return fmt.Errorf("store: read the store's ID: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	// A process that ended between a run's end and the checkpoint after it
	// left the log holding the run's secret record.
	s.truncateLog(ctx)
	return nil
}

// checkHopperSchema refuses hopper's schema at a version past the latest
// this release's hopper knows.
func (s *Store) checkHopperSchema(ctx context.Context) error {
	v, err := hoppermigrate.Version(ctx, s.driver)
	if err != nil {
		return err
	}
	if v > hoppermigrate.Latest() {
		return s.tooNew("hopper", v, hoppermigrate.Latest())
	}
	return nil
}

// tooNew is the problem of a database whose schema, hopper's or
// meridian's, a newer release brought to version, past latest.
func (s *Store) tooNew(schema string, version, latest int) error {
	return problems.DatabaseTooNew.Problem(problems.Params{"path": s.file, "schema": schema, "version": version, "latest": latest})
}

// newID returns n random bytes, as hex.
func newID(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b) // never fails
	return hex.EncodeToString(b)
}

// ID returns the store's ID: 32 hex digits, random, made when the store was
// first opened.
func (s *Store) ID() string { return s.id }

// Driver returns the hopper driver the store is on, for the coordinator's
// hopper client.
func (s *Store) Driver() *hoppersqlite.Driver { return s.driver }

// Close closes the store's database. It unlocks the endpoints its Holds
// hold, but leaves their records, as a process that ends does.
func (s *Store) Close() error {
	s.holdsMu.Lock()
	for h := range s.holds {
		h.unlock()
	}
	s.holds = nil
	s.holdsMu.Unlock()
	return s.driver.Close()
}

// A Tx is a transaction of the store's, which the calls of a function
// that Store.Tx or Store.View runs go through. A hopper call given SQL()
// runs in it too.
//
// A call that fails may have written part of what it does, as one that
// runs several statements does: return its error from the function,
// which rolls the transaction back, rather than go on and commit.
//
// The calls that read and then write what they read, as NewAttempt,
// Claim, Authorize, Renew, StepStarted, EndAttempt and Reseal do, rely on
// SQLite's one writer: a transaction holds the database's write lock from
// its first statement (BEGIN IMMEDIATE), so nothing changes between the
// read and the write. A store on Postgres needs SELECT ... FOR UPDATE, or
// SERIALIZABLE transactions, for them.
type Tx struct {
	//nolint:containedctx // a Tx lives for the call of the function it is given, as the context does
	ctx context.Context
	tx  *sql.Tx
	s   *Store
	// begun is set once a statement has run in the transaction, which,
	// for one that may write, holds the write lock from then on.
	begun bool
	// erased is set once the transaction has deleted a secret record,
	// which a checkpoint of the write-ahead log then erases from it.
	erased bool
}

// Tx runs fn in a transaction, which it commits if fn returns nil and
// rolls back otherwise, returning fn's error. Everything fn does through
// the Tx, and every hopper call it gives SQL(), commits together, or none
// of it does.
//
// A transaction holds the database's write lock from its first statement
// until it ends, so every other write, in this process or another, waits
// for it: keep fn short, and make no call in it that waits on another
// transaction, such as Tx itself. In a hopper job's work, begin it with
// the context hopper gives the work, as hopper's CompleteTx asks.
func (s *Store) Tx(ctx context.Context, fn func(*Tx) error) error {
	return s.run(ctx, nil, fn)
}

// View runs fn in a transaction that only reads, and so takes no lock:
// what fn reads is the database as it was at fn's first read. A call of
// fn's that writes fails.
func (s *Store) View(ctx context.Context, fn func(*Tx) error) error {
	return s.run(ctx, &sql.TxOptions{ReadOnly: true}, fn)
}

func (s *Store) run(ctx context.Context, opts *sql.TxOptions, fn func(*Tx) error) error {
	sqlTx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	// After a commit, this is a no-op.
	defer func() { _ = sqlTx.Rollback() }()
	if opts == nil {
		// What a transaction deletes or overwrites, a secret record and
		// its data key among it, SQLite overwrites with zeros in the
		// database and its log, rather than leaving it in free space.
		// The setting is the connection's, and is set on each that runs
		// a transaction of the store's, since hoppersqlite's Config has
		// no setting for its connections: once it has, it moves there.
		if _, err := sqlTx.ExecContext(ctx, "PRAGMA secure_delete = ON"); err != nil {
			return err
		}
	}
	tx := &Tx{ctx: ctx, tx: sqlTx, s: s, begun: opts == nil}
	if err := fn(tx); err != nil {
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return err
	}
	if tx.erased {
		s.truncateLog(context.WithoutCancel(ctx))
	}
	return nil
}

// truncateLog checkpoints the write-ahead log into the database and
// truncates it, so that what a transaction deleted, which secure_delete
// overwrote with zeros in the pages the checkpoint writes, is in neither
// any more. A reader in another process that still reads from the log, or
// a writer, keeps the checkpoint from finishing, and the next one, of the
// next run to end, of the next Open, or of hopper's maintenance, truncates
// it: the store says so in its log.
func (s *Store) truncateLog(ctx context.Context) {
	var busy, logPages, checkpointed int64
	err := s.db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logPages, &checkpointed)
	switch {
	case err != nil:
		s.log.WarnContext(ctx, "store: checkpoint the write-ahead log", "error", err)
	case busy != 0:
		s.log.WarnContext(ctx, "store: a reader or a writer kept the write-ahead log from being checkpointed and truncated", "pages", logPages, "checkpointed", checkpointed)
	}
}

// SQL returns the transaction, for hopper's calls in it, such as
// InsertWorkflowTx and CompleteTx.
func (tx *Tx) SQL() *sql.Tx { return tx.tx }

// Now returns the store's clock: hopper's. It reads it once the
// transaction has run a statement, which takes the write lock in one that
// may write, so that the times transactions record are in the order they
// commit: one read before the lock could be earlier than a time another
// transaction recorded, and committed, while this one waited for it.
// Today a transaction's first statement is secure_delete's pragma; it
// stays right once that moves to hoppersqlite's Config.
func (tx *Tx) Now() (time.Time, error) {
	if !tx.begun {
		if _, err := tx.tx.ExecContext(tx.ctx, "SELECT 1"); err != nil {
			return time.Time{}, err
		}
		tx.begun = true
	}
	return tx.s.driver.UnwrapTx(tx.tx).Now(tx.ctx)
}

// now returns the store's clock in microseconds since the Unix epoch, as
// the store records a time.
func (tx *Tx) now() (int64, error) {
	t, err := tx.Now()
	return t.UnixMicro(), err
}

// timeOf returns the time the store recorded as v, or the zero time.
func timeOf(v sql.NullInt64) time.Time {
	if !v.Valid {
		return time.Time{}
	}
	return time.UnixMicro(v.Int64).UTC()
}

// runNumber returns the number a run's ID is, if it is one as the store
// writes it: a decimal number from 1, without leading zeros.
func runNumber(id string) (int64, bool) {
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil || n < 1 || strconv.FormatInt(n, 10) != id {
		return 0, false
	}
	return n, true
}

// runID is the ID of the run whose number is n.
func runID(n int64) string { return strconv.FormatInt(n, 10) }
