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
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parallelworks/hopper/driver/hoppersqlite"
	"github.com/parallelworks/hopper/hoppermigrate"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/plan"
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
	// ErrNotPlaced is what Claim returns for an attempt whose placement
	// isn't recorded yet (RecordPlacement), as one whose worker claims it
	// before the call that started it returns: it is claimed once it is.
	ErrNotPlaced = errors.New("store: the attempt's placement isn't recorded yet")
	// ErrRecordChanged is what Reseal and PutJobRecord return, beside
	// ErrState, for a run's secret record, or a job's part of it, that was
	// sealed again since the generation given was read: read it again, and
	// add to that.
	ErrRecordChanged = errors.New("store: the run's secret record was sealed again meanwhile")
	// ErrMalformed is what a call returns for what the store holds that
	// this release doesn't read as it would write it, such as a job's
	// result with a member it doesn't know.
	ErrMalformed = errors.New("store: malformed")
	// ErrReadOnly is what a call that writes returns of a store opened
	// only to read it (Options.ReadOnly).
	ErrReadOnly = errors.New("store: opened only to read it")
)

// Options say how to open a store. A nil *Options means the defaults.
type Options struct {
	// Log hears of what the store does that its calls don't say: the
	// migrations Open applies, and a checkpoint of the write-ahead log
	// that couldn't finish. Nil means none.
	Log *slog.Logger
	// LastTry reports, in tx, whether try, from 0, of step, its index, is
	// surely its last, for the attempt's job: as its plan says when it holds
	// the step's retry: written out (plan.Step.LastTry), so that a step
	// that failed marks its run as soon as its event is stored. Nil, or
	// false, has the store wait for the next event, which shows it. It is
	// asked of an attempt claimed at version 1 of the worker API only: a
	// later version's worker says so in the event (workerapi.Event.LastTry).
	LastTry func(tx *Tx, attempt string, step, try int) bool
	// ReadOnly opens the store only to read it, as a command that shows
	// runs does, which mustn't change a store that an earlier release,
	// still running a workflow, uses: Open neither makes the database, nor
	// brings its schema up to this release's, nor checkpoints its
	// write-ahead log, and refuses a database whose schema isn't this
	// release's, whether newer (database_too_new) or older
	// (database_too_old); and each call that writes returns ErrReadOnly.
	// Only Meridian's tables are read, so only Meridian's schema is
	// checked.
	ReadOnly bool
	// RetriesFor reports, in tx, whether the retry: of the attempt's job
	// lists reason, so that an end of an attempt of it for that reason can
	// be followed by another (plan.Job.RetriesFor): such an end marks the
	// run only as the job ends, failed, once no attempt follows (Tx.
	// RetriesFor). Nil reports false.
	RetriesFor func(tx *Tx, attempt string, reason plan.Reason) bool
	// Now is the clock the store reads in place of the host's, as a
	// test's that it moves on itself. Nil means the host's.
	Now func() time.Time
	// MaxLead is how far the store's clock's mark may lead the host's
	// clock before the store takes it that the host's clock was reset
	// (Tx.Now): the coordinator's grace. Zero means DefaultMaxLead.
	MaxLead time.Duration
	// OnClockReset is called once a transaction that recorded a reset of
	// the store's clock has committed, with the transaction's context, so
	// that the coordinator wakes what waits on hopper's schedule, which a
	// clock that was ahead may have put far off. Nil means nothing is.
	OnClockReset func(ctx context.Context)
	// OnControlsChanged is called once a transaction that changed
	// attempts' controls (Tx.UpdateControl) has committed, with the
	// transaction's context and the attempts' IDs, in order (Tx.
	// ControlsChanged), so that whoever holds their control polls in this
	// process answers them at once (workerapisrv.Server.Notify); those of
	// other processes read them again every second. Nil means nothing is.
	OnControlsChanged func(ctx context.Context, ids []string)
	// OnPlacementsRecorded is called once a transaction that recorded
	// attempts' placements (Tx.RecordPlacement) has committed, with the
	// transaction's context and the attempts' IDs, in order, so that
	// whoever holds their first claims in this process answers them at
	// once (workerapisrv.Server.Placed); those of other processes read
	// them again every so often. Nil means nothing is.
	OnPlacementsRecorded func(ctx context.Context, ids []string)
	// OnAttemptsEnded is called in a transaction that ended attempts, as
	// it ends them, before it commits, with the transaction's context and
	// the attempts' IDs: those Tx.EndAttempt ended, and those a new attempt
	// of their job superseded (Tx.NewAttempt). The coordinator stops the
	// children of their steps in it, as nothing waits for them any more,
	// so that no child outlives the attempt that ran its step, whatever
	// ended it. An error it returns is the transaction's, which then
	// stores nothing. Nil means nothing is.
	OnAttemptsEnded func(ctx context.Context, tx *Tx, ids []string) error
	// OnRunsEnded is called once a transaction that ended runs (Tx.EndRun)
	// has committed, with the transaction's context and the runs' IDs, in
	// order, so that whoever holds polls for their ends, as children, in
	// this process answers them at once (workerapisrv.Server.Ended); those
	// of other processes read them again every second. Nil means nothing
	// is.
	OnRunsEnded func(ctx context.Context, ids []string)
}

// hostClock is the host's clock, which the store reads, unless
// Options.Now gives another: a test of the default path replaces it.
var hostClock = time.Now

// DefaultMaxLead is how far the clock's mark leads the host's clock, at
// most, when Options doesn't say.
const DefaultMaxLead = time.Minute

// A Store is the coordinator's store, open. Its methods are safe for
// concurrent use, and so is the database: several processes may open it.
type Store struct {
	driver *hoppersqlite.Driver
	db     *sql.DB
	log    *slog.Logger
	// lastTry is Options.LastTry, and retriesFor Options.RetriesFor, or
	// nil.
	lastTry    func(tx *Tx, attempt string, step, try int) bool
	retriesFor func(tx *Tx, attempt string, reason plan.Reason) bool
	// file is the database's file, as SQLite opened it, its links
	// resolved: the same for every process that opens the database.
	file string
	id   string
	// readOnly says the store was opened only to read it.
	readOnly bool
	// clock is Options.Now, or nil for the host's, and maxLead
	// Options.MaxLead.
	clock   func() time.Time
	maxLead time.Duration
	// onReset is Options.OnClockReset, onControls
	// Options.OnControlsChanged, and onPlaced Options.OnPlacementsRecorded,
	// or nil.
	onReset    func(ctx context.Context)
	onControls func(ctx context.Context, ids []string)
	onPlaced   func(ctx context.Context, ids []string)
	onEnded    func(ctx context.Context, ids []string)
	// attemptsEnded is Options.OnAttemptsEnded, or nil.
	attemptsEnded func(ctx context.Context, tx *Tx, ids []string) error
	// migrated is set once Meridian's schema, which keeps the clock's
	// mark, is there: Open sets it before it returns the store.
	migrated bool

	// holds are the endpoints this store holds, kept here until they are
	// released, or the store closed: a lock's file that nothing reaches
	// would be closed by the garbage collector, which releases the lock.
	holdsMu sync.Mutex
	holds   map[*Hold]struct{}

	// changed is closed, and replaced, once a transaction of this process's
	// that added a run's events commits (Changes).
	changedMu sync.Mutex
	changed   chan struct{}

	// starts are the start states Tx.Run decoded.
	starts startCache

	// stmts are the statements the store's transactions run, by their SQL,
	// each prepared once on the database (Tx.stmt).
	stmtsMu sync.Mutex
	stmts   map[string]*sql.Stmt

	// walEraser erases from the write-ahead log the secret records the
	// store's transactions delete (eraseWAL).
	walEraser

	// closeOnce has Close close the store once; closeErr is what it
	// returned.
	closeOnce sync.Once
	closeErr  error
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
	readOnly := opts != nil && opts.ReadOnly
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := checkFiles(path); err != nil {
		return nil, err
	}
	var created bool
	if readOnly {
		// SQLite would make a database that isn't there.
		if _, err := os.Lstat(path); err != nil { //nolint:gosec // the database checkFiles checked
			return nil, err
		}
	} else if created, err = createPrivate(path); err != nil {
		return nil, err
	}
	start, wait := time.Now(), openBusyFirstWait
	for {
		s, err := openOnce(ctx, path, log, opts, readOnly)
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

// openOnce opens the store at path once, with opts, and brings its schema
// up to this release's, or, readOnly, checks that it is.
func openOnce(ctx context.Context, path string, log *slog.Logger, opts *Options, readOnly bool) (*Store, error) {
	d, err := openSQLite(ctx, path, nil)
	if err != nil {
		return nil, err
	}
	s := &Store{driver: d, db: d.DB(), log: log, readOnly: readOnly}
	s.initEraser(ctx)
	s.maxLead = DefaultMaxLead
	if opts != nil {
		s.lastTry, s.retriesFor, s.clock = opts.LastTry, opts.RetriesFor, opts.Now
		s.onReset, s.onControls, s.onPlaced, s.onEnded = opts.OnClockReset, opts.OnControlsChanged, opts.OnPlacementsRecorded, opts.OnRunsEnded
		s.attemptsEnded = opts.OnAttemptsEnded
		if opts.MaxLead > 0 {
			s.maxLead = opts.MaxLead
		}
	}
	setUp := s.setUp
	if readOnly {
		setUp = s.checkSchema
	}
	if err := setUp(ctx, log); err != nil {
		// A truncation of the log setUp began to try again ends first.
		s.stopErasing()
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
	if err := s.View(ctx, s.readFile); err != nil {
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
	s.migrated = true
	if err := s.View(ctx, s.readID); err != nil {
		return err
	}
	var pending bool
	if err := s.Tx(ctx, func(tx *Tx) error {
		found, err := tx.eraseDigests()
		if err != nil {
			return err
		}
		if found {
			if err := tx.markErasePending(); err != nil {
				return err
			}
		}
		pending, err = tx.erasePending()
		return err
	}); err != nil {
		return err
	}
	// A process that ended between a run's end and the checkpoint after it
	// left the log holding the run's secret record, which the store's
	// marker says, as it says of the digests just erased, if any: this
	// process warns of those as of its own, if it can't truncate the log.
	s.eraseWAL(context.WithoutCancel(ctx), pending)
	return nil
}

// noDigest is the digest the store overwrites a plain one with, as a
// release before kept of an action's request: all zeros.
const noDigest = "x'0000000000000000000000000000000000000000000000000000000000000000'"

// eraseDigests erases the plain SHA-256 digests that releases before kept
// of what can hold a secret, against which whoever holds the database
// could test a guess at it: of attempts' ends, which the store no longer
// keeps a digest of, and of actions' requests, whose digests it keys now
// (Action.Keyed). The migration that stopped keeping them erased those
// there were then; a release before it, still running on the database,
// goes on writing them, so each process that opens the store erases them
// again, and so does each run's end. Each finds only the rows that hold
// one, by an index of them. The store's writes overwrite what they
// replace with zeros (secure_delete), and the write-ahead log is truncated
// after. It reports whether it found any.
func (tx *Tx) eraseDigests() (bool, error) {
	attempts, err := tx.exec("UPDATE meridian_attempts SET complete_digest = NULL WHERE complete_digest IS NOT NULL")
	if err != nil {
		return false, err
	}
	actions, err := tx.exec("UPDATE meridian_actions SET digest = " + noDigest + " WHERE keyed = 0 AND digest <> " + noDigest)
	if err != nil {
		return false, err
	}
	a, err := attempts.RowsAffected()
	if err != nil {
		return false, err
	}
	b, err := actions.RowsAffected()
	return a+b > 0, err
}

// checkSchema checks that a store just opened only to read it has
// Meridian's schema at this release's version, and reads its ID and its
// file, changing nothing.
func (s *Store) checkSchema(ctx context.Context, _ *slog.Logger) error {
	return s.View(ctx, func(tx *Tx) error {
		if err := s.readFile(tx); err != nil {
			return err
		}
		version, err := tx.schemaVersion()
		switch {
		case err != nil:
			return err
		case version > len(migrations):
			return s.tooNew("meridian", version, len(migrations))
		case version < len(migrations):
			return problems.DatabaseTooOld.Problem(problems.Params{"path": s.file, "schema": "meridian", "version": version, "latest": len(migrations)})
		}
		s.migrated = true
		return s.readID(tx)
	})
}

// readFile reads the database's file, as SQLite opened it.
func (s *Store) readFile(tx *Tx) error {
	var err error
	if s.file, err = tx.dbFile(); err == nil && !filepath.IsAbs(s.file) {
		err = fmt.Errorf("store: SQLite opened %q, not a file", s.file)
	}
	return err
}

// readID reads the store's ID.
func (s *Store) readID(tx *Tx) error {
	if err := tx.queryRow("SELECT value FROM meridian_store WHERE key = 'store_id'").Scan(&s.id); err != nil {
		return fmt.Errorf("store: read the store's ID: %w", err)
	}
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
	s.closeOnce.Do(func() { s.closeErr = s.closeStore() })
	return s.closeErr
}

// closeStore closes the store, once (Close).
func (s *Store) closeStore() error {
	// A truncation of the write-ahead log that a reader or a writer held
	// up is tried once more here, as the store closes (eraseLast).
	s.eraseLast()
	s.holdsMu.Lock()
	for h := range s.holds {
		h.unlock()
	}
	s.holds = nil
	s.holdsMu.Unlock()
	// The statements are closed outside their lock: closing one locks the
	// connections it was prepared on.
	s.stmtsMu.Lock()
	stmts := s.stmts
	s.stmts = nil
	s.stmtsMu.Unlock()
	for _, st := range stmts {
		_ = st.Close()
	}
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
	// writes is set for a transaction that may write: Tx's, not View's;
	// and reset once it has recorded a reset of the store's clock.
	writes bool
	reset  bool
	// begun is set once a statement has run in the transaction, which,
	// for one that may write, holds the write lock from then on.
	begun bool
	// erased is set once the transaction has deleted a secret record,
	// which a checkpoint of the write-ahead log then erases from it.
	erased bool
	// events is set once the transaction has added a run's event, which
	// Changes says once it commits.
	events bool
	// controls are the attempts whose controls the transaction has
	// changed (UpdateControl).
	controls map[string]bool
	// placed are the attempts whose placements the transaction has
	// recorded (RecordPlacement), in order.
	placed []string
	// ended are the runs the transaction ended (EndRun), in order, which
	// Options.OnRunsEnded is given once it commits.
	ended []string
	// after are what is called once the transaction commits (AfterCommit),
	// in order.
	after []func(context.Context)
	// shared are the values the transaction's callers share (Shared), by
	// key.
	shared map[any]any
}

// AfterCommit has f called once the transaction has committed, with its
// context, uncancelled, last of all that the commit does, after the hooks
// of Options, the erasing of a secret record from the write-ahead log, and
// the closing of what Changes gave, in the order AfterCommit was called:
// never for a transaction that doesn't commit. A hook of Options that runs
// in the transaction, as OnAttemptsEnded, so has what it does once the
// transaction commits done then.
func (tx *Tx) AfterCommit(f func(ctx context.Context)) {
	tx.after = append(tx.after, f)
}

// Shared returns the transaction's value for key, which newValue makes the
// first time one of its callers asks for it: a value they share for as
// long as the transaction lasts, as what each gathers for one thing to be
// done once it commits (AfterCommit). key is compared as a map's is, and
// is best a type of the caller's own, as context.WithValue's is.
func (tx *Tx) Shared(key any, newValue func() any) any {
	if v, ok := tx.shared[key]; ok {
		return v
	}
	if tx.shared == nil {
		tx.shared = map[any]any{}
	}
	v := newValue()
	tx.shared[key] = v
	return v
}

// stmt returns the statement of query for the transaction: the store's,
// prepared on the database the first time a transaction runs it, which
// database/sql keeps prepared on each connection that has run it, so that
// SQLite parses a statement once a connection rather than at each call. A
// query is one of the store's own: SQL whose text doesn't change from call
// to call, so that the store keeps a statement of each.
//
// A transaction runs the same statement on its connection each time, so
// it mustn't run a query again while rows that query returned are open:
// SQLite refuses (SQLITE_MISUSE, 21). Each caller closes its rows before
// it runs another query.
//
// It prepares a statement holding the store's lock of them, so that no
// statement is prepared twice: a transaction mustn't close one, as the
// one of two prepared at once that isn't kept, since closing it locks
// each connection it was prepared on, which another transaction may hold
// as it waits for this one's write lock. Preparing takes a connection no
// transaction holds, which database/sql opens when none is idle, as long
// as nothing bounds how many it opens: hoppersqlite sets no
// SetMaxOpenConns. And it takes no lock of SQLite's.
func (tx *Tx) stmt(query string) (*sql.Stmt, error) {
	st, err := tx.s.prepared(tx.ctx, query)
	if err != nil {
		return nil, err
	}
	return tx.tx.StmtContext(tx.ctx, st), nil
}

// prepared returns the store's statement of query, which it prepares on the
// database the first time, holding the lock of its statements (Tx.stmt).
func (s *Store) prepared(ctx context.Context, query string) (*sql.Stmt, error) {
	s.stmtsMu.Lock()
	defer s.stmtsMu.Unlock()
	if st := s.stmts[query]; st != nil {
		return st, nil
	}
	st, err := s.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	if s.stmts == nil {
		s.stmts = map[string]*sql.Stmt{}
	}
	s.stmts[query] = st
	return st, nil
}

// exec runs query, which writes, in the transaction, with args (stmt).
func (tx *Tx) exec(query string, args ...any) (sql.Result, error) {
	st, err := tx.stmt(query)
	if err != nil {
		return nil, err
	}
	return st.ExecContext(tx.ctx, args...)
}

// query runs query in the transaction, with args, and returns its rows,
// which the caller closes before it runs the query again (stmt).
func (tx *Tx) query(query string, args ...any) (*sql.Rows, error) {
	st, err := tx.stmt(query)
	if err != nil {
		return nil, err
	}
	return st.QueryContext(tx.ctx, args...)
}

// queryRow runs query in the transaction, with args, and returns its first
// row (stmt). A statement that can't be prepared is run unprepared, whose
// row gives the error.
func (tx *Tx) queryRow(query string, args ...any) *sql.Row {
	st, err := tx.stmt(query)
	if err != nil {
		return tx.tx.QueryRowContext(tx.ctx, query, args...)
	}
	return st.QueryRowContext(tx.ctx, args...)
}

// ControlsChanged returns the attempts whose controls the transaction has
// changed (UpdateControl), in order, which Options.OnControlsChanged is
// given once it commits.
func (tx *Tx) ControlsChanged() []string { return slices.Sorted(maps.Keys(tx.controls)) }

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
	if opts == nil && s.readOnly {
		return ErrReadOnly
	}
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
	tx := &Tx{ctx: ctx, tx: sqlTx, s: s, begun: opts == nil, writes: opts == nil}
	if err := fn(tx); err != nil {
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return err
	}
	if tx.reset && s.onReset != nil {
		s.onReset(context.WithoutCancel(ctx))
	}
	if len(tx.controls) > 0 && s.onControls != nil {
		s.onControls(context.WithoutCancel(ctx), tx.ControlsChanged())
	}
	if len(tx.placed) > 0 && s.onPlaced != nil {
		s.onPlaced(context.WithoutCancel(ctx), tx.placed)
	}
	if len(tx.ended) > 0 {
		s.starts.forget(tx.ended)
		if s.onEnded != nil {
			s.onEnded(context.WithoutCancel(ctx), tx.ended)
		}
	}
	if tx.erased {
		s.eraseWAL(context.WithoutCancel(ctx), true)
	}
	if tx.events {
		s.changedMu.Lock()
		if s.changed != nil {
			close(s.changed)
			s.changed = nil
		}
		s.changedMu.Unlock()
	}
	for _, f := range tx.after {
		f(context.WithoutCancel(ctx))
	}
	return nil
}

// Changes returns a channel that is closed once a transaction of this
// process's that adds a run's events, as each change to a run does, next
// commits: one in another process isn't said, and whoever follows a run
// reads its events again every so often for those. Take the channel
// before reading the events, so that none added in between is missed.
func (s *Store) Changes() <-chan struct{} {
	s.changedMu.Lock()
	defer s.changedMu.Unlock()
	if s.changed == nil {
		s.changed = make(chan struct{})
	}
	return s.changed
}

// SQL returns the transaction, for hopper's calls in it, such as
// InsertWorkflowTx and CompleteTx.
func (tx *Tx) SQL() *sql.Tx { return tx.tx }

// Now returns the store's clock: the host's, or the later time the store
// last read from it, its high-water mark, which a transaction that may
// write records. It reads it once the transaction has run a statement,
// which takes the write lock in one that may write, so that the times
// transactions record are in the order they commit: one read before the
// lock could be earlier than a time another transaction recorded, and
// committed, while this one waited for it. Today a transaction's first
// statement is secure_delete's pragma; it stays right once that moves to
// hoppersqlite's Config.
//
// It reads the host's wall clock itself, not hopper's, which never
// returns less than it returned before in its process: a process that
// read the host's clock far ahead once would keep that time, and set the
// mark ahead again with each write, after the host's clock was set right.
// The host's clock can step back, and a process that starts after it did
// reads it behind another's. The mark keeps every process's store clock
// past every time any of them recorded, so that a lease another process
// renewed is never taken for older than it is, and a job that fences
// itself never given a new attempt early: a clock stepped back a little
// stops the store's until the host's catches up, which only puts off
// what it times.
//
// A mark that leads the host's clock by more than Options.MaxLead is a
// clock reset, as a host set back to an old date, or one bad reading
// that set the mark ahead: the store's clock then takes the host's, rather
// than stand still until it catches up. A transaction that may write says
// so in the store's log, records the host's time as the mark, brings each
// time a run, a job, an attempt or an endpoint recorded later than it
// back to it, so that none is ahead of the clock (recordReset), and
// starts a grace (StartGrace), in which no lapsed lease counts: a lease
// renewed ahead of the clock is fresh, and lapses a lease period after
// the reset, never early. Once it commits, Options.OnClockReset is
// called.
//
// On Postgres, one row that every transaction that writes updates would
// make them wait for each other: a store there keeps the mark otherwise,
// as with a row per process, read with max().
func (tx *Tx) Now() (time.Time, error) {
	if !tx.begun {
		if _, err := tx.exec("SELECT 1"); err != nil {
			return time.Time{}, err
		}
		tx.begun = true
	}
	clock := hostClock
	if tx.s.clock != nil {
		clock = tx.s.clock
	}
	now := clock().UTC().Truncate(time.Microsecond)
	if !tx.s.migrated {
		return now, nil
	}
	var mark sql.NullString
	err := tx.queryRow("SELECT value FROM meridian_store WHERE key = 'clock'").Scan(&mark)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, err
	}
	if m, err := strconv.ParseInt(mark.String, 10, 64); err == nil && m > now.UnixMicro() {
		lead := time.Duration(m-now.UnixMicro()) * time.Microsecond
		if lead <= tx.s.maxLead {
			return time.UnixMicro(m).UTC(), nil
		}
		if tx.writes {
			if err := tx.recordReset(now, lead); err != nil {
				return time.Time{}, err
			}
			tx.reset = true
		}
		return now, nil
	}
	if tx.writes {
		if _, err := tx.exec(`INSERT INTO meridian_store (key, value) VALUES ('clock', ?)
ON CONFLICT (key) DO UPDATE SET value = excluded.value`, strconv.FormatInt(now.UnixMicro(), 10)); err != nil {
			return time.Time{}, err
		}
	}
	return now, nil
}

// recordReset records a reset of the host's clock, which the clock's mark
// led by lead, at now: the mark is now; every time a run, a job, an
// attempt or an endpoint recorded later than now is now; and a grace
// starts. A run's created_at stays, since a trigger keeps it from ever
// changing, as do a secret record's updated_at, an attempt's event's
// stored_at and a run's event's at, which say when they were written and
// time nothing.
func (tx *Tx) recordReset(now time.Time, lead time.Duration) error {
	tx.s.log.WarnContext(tx.ctx, "store: the store's clock was ahead of the host's, as after the host's clock was set back, and takes the host's clock from now, with a grace in which no lapsed lease counts",
		"ahead", lead)
	at := now.UnixMicro()
	for _, stmt := range []string{
		"UPDATE meridian_runs SET started_at = min(started_at, ?1), ended_at = min(ended_at, ?1) WHERE started_at > ?1 OR ended_at > ?1",
		"UPDATE meridian_jobs SET created_at = min(created_at, ?1), ready_at = min(ready_at, ?1), ended_at = min(ended_at, ?1) WHERE created_at > ?1 OR ready_at > ?1 OR ended_at > ?1",
		"UPDATE meridian_attempts SET created_at = min(created_at, ?1), claimed_at = min(claimed_at, ?1), renewed_at = min(renewed_at, ?1), step_started_at = min(step_started_at, ?1), ended_at = min(ended_at, ?1), problem_since = min(problem_since, ?1)",
		"UPDATE meridian_endpoints SET taken_at = min(taken_at, ?1), grace_from = min(grace_from, ?1)",
		"INSERT INTO meridian_store (key, value) VALUES ('clock', ?1) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
	} {
		if _, err := tx.tx.ExecContext(tx.ctx, stmt, at); err != nil {
			return err
		}
	}
	return tx.startGrace(at)
}

// StartGrace starts a grace for every coordinator of the store, from now,
// in which no lapsed lease counts (GraceFrom), as after a host that slept
// woke, or its clock was reset.
func (tx *Tx) StartGrace() error {
	now, err := tx.now()
	if err != nil {
		return err
	}
	return tx.startGrace(now)
}

// startGrace records at, in microseconds, as the latest start of a grace.
func (tx *Tx) startGrace(at int64) error {
	_, err := tx.exec(`INSERT INTO meridian_store (key, value) VALUES ('grace', ?)
ON CONFLICT (key) DO UPDATE SET value = excluded.value WHERE CAST(meridian_store.value AS INTEGER) < CAST(excluded.value AS INTEGER)`,
		strconv.FormatInt(at, 10))
	return err
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
