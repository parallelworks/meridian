package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"
	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver/hoppersqlite"
	"github.com/parallelworks/hopper/hoppermigrate"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

func TestMain(m *testing.M) {
	// A test that runs the store in processes of its own starts this
	// binary again, which does what MERIDIAN_STORE_CHILD names
	// (multiprocess_test.go).
	if role := os.Getenv(envChild); role != "" {
		os.Exit(runChild(role))
	}
	os.Exit(m.Run())
}

// open opens a new store in a directory of the test's.
func open(t *testing.T) *Store {
	t.Helper()
	return openAt(t, filepath.Join(privateDir(t), "meridian.db"))
}

// openAt opens the store at path, which the test closes.
func openAt(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(t.Context(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

// testWorkflow is the workflow the tests' runs are of: two jobs, one of
// which needs the other.
const testWorkflow = `
jobs:
  build:
    steps:
      - run: echo build
  test:
    needs: [build]
    steps:
      - run: echo test
`

// testPlan returns the plan of testWorkflow.
func testPlan(t testing.TB) *plan.Plan {
	t.Helper()
	p, err := makePlan(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func makePlan(ctx context.Context) (*plan.Plan, error) {
	created, diags, err := compile.Create(ctx, "w.yaml", []byte(testWorkflow), nil, compile.Options{Source: plan.Source{Location: "w.yaml"}})
	if err != nil {
		return nil, err
	}
	if diags.HasErrors() {
		return nil, fmt.Errorf("the test workflow has errors: %w", diags)
	}
	return created.Plan, nil
}

// fakeJobID returns a hopper job ID for a test that inserts no hopper
// job, which differs for each i.
func fakeJobID(i int) hopper.JobID {
	var id hopper.JobID
	id[15] = byte(i + 1)
	id[14] = byte((i + 1) >> 8)
	return id
}

// createRun stores a run of testPlan, with its jobs and a secret record, as
// the coordinator does, without a hopper workflow, and returns its ID.
func createRun(t testing.TB, s *Store) string {
	t.Helper()
	p := testPlan(t)
	var id string
	err := s.Tx(t.Context(), func(tx *Tx) error {
		var err error
		if id, err = tx.NewRunID(); err != nil {
			return err
		}
		return insertRun(tx, id, p, fakeJobID(0), []hopper.JobID{fakeJobID(1), fakeJobID(2)})
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// insertRun stores the run id of p, with its hopper workflow and its jobs'
// supervisors, in the order of the plan's jobs, and a secret record.
func insertRun(tx *Tx, id string, p *plan.Plan, workflow hopper.JobID, supervisors []hopper.JobID) error {
	if err := tx.InsertRun(NewRun{ID: id, Plan: p, DefaultGroup: "local", RunDir: "/runs/" + id, WorkflowID: workflow}); err != nil {
		return err
	}
	for i, j := range p.Jobs {
		if err := tx.InsertJob(NewJob{Run: id, Name: j.Name, Key: j.Name, SupervisorID: supervisors[i]}); err != nil {
			return err
		}
	}
	return tx.InsertRecord(id, 1, []byte("wrapped "+id), []byte("sealed "+id+" 1"))
}

// inTx runs fn in a transaction of s's, failing the test on an error.
func inTx(t testing.TB, s *Store, fn func(*Tx) error) {
	t.Helper()
	if err := s.Tx(t.Context(), fn); err != nil {
		t.Fatal(err)
	}
}

// checkIntegrity checks that the database is whole, and that every row
// Meridian's tables refer to is there, as their foreign keys say.
func checkIntegrity(t *testing.T, s *Store) {
	t.Helper()
	err := s.View(t.Context(), func(tx *Tx) error {
		var ok string
		if err := tx.tx.QueryRowContext(tx.ctx, "PRAGMA integrity_check").Scan(&ok); err != nil {
			return err
		}
		if ok != "ok" {
			return fmt.Errorf("integrity_check: %s", ok)
		}
		rows, err := tx.tx.QueryContext(tx.ctx, "PRAGMA foreign_key_check")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var table, parent string
			var rowid, fk any
			if err := rows.Scan(&table, &rowid, &parent, &fk); err != nil {
				return err
			}
			t.Errorf("a row of %s refers to a row of %s that isn't there", table, parent)
		}
		return rows.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
}

var hexID = regexp.MustCompile(`^[0-9a-f]{32}$`)

func TestOpen(t *testing.T) {
	dir := privateDir(t)
	path := filepath.Join(dir, "meridian.db")
	s, err := Open(t.Context(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := s.ID()
	if !hexID.MatchString(id) {
		t.Errorf("the store's ID is %q, want 32 hex digits", id)
	}
	if v, err := hoppermigrate.Version(t.Context(), s.Driver()); err != nil || v != hoppermigrate.Latest() {
		t.Errorf("hopper's schema is at %d (%v), want %d", v, err, hoppermigrate.Latest())
	}
	createRun(t, s) // writes the log
	if runtime.GOOS != "windows" {
		for _, name := range []string{"meridian.db", "meridian.db-wal", "meridian.db-shm"} {
			info, err := os.Stat(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm()&0o077 != 0 {
				t.Errorf("%s is %v, which others can read", name, info.Mode())
			}
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openAt(t, path)
	if s.ID() != id {
		t.Errorf("the store's ID is %q once opened again, was %q", s.ID(), id)
	}
	if other := open(t); other.ID() == id {
		t.Error("two stores have one ID")
	}
}

// TestOpenWhileBusy checks that Open tries again while SQLite reports the
// database busy as it opens it, as it does when another process holds it
// to turn its write-ahead log on, and gives up once its context ends.
func TestOpenWhileBusy(t *testing.T) {
	// lock holds the database at path exclusively until the function it
	// returns is called.
	lock := func(t *testing.T, path string) func() {
		t.Helper()
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(t.Context(), "BEGIN EXCLUSIVE"); err != nil {
			t.Fatal(err)
		}
		return func() {
			_, _ = conn.ExecContext(context.WithoutCancel(t.Context()), "ROLLBACK")
			_ = conn.Close()
			_ = db.Close()
		}
	}
	t.Run("waits", func(t *testing.T) {
		path := filepath.Join(privateDir(t), "meridian.db")
		writeFile(t, path, 0o600)
		unlock := lock(t, path)
		const held = 300 * time.Millisecond
		start := time.Now()
		timer := time.AfterFunc(held, unlock)
		defer timer.Stop()
		openAt(t, path)
		if took := time.Since(start); took < held {
			t.Errorf("Open returned after %v, before the lock was let go", took)
		}
	})
	t.Run("gives up", func(t *testing.T) {
		path := filepath.Join(privateDir(t), "meridian.db")
		writeFile(t, path, 0o600)
		defer lock(t, path)()
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		s, err := Open(ctx, path, nil)
		if err == nil {
			_ = s.Close()
			t.Fatal("Open opened a database another connection held")
		}
		if !errors.Is(err, context.DeadlineExceeded) || !busy(err) {
			t.Errorf("Open gave %v, not its context's end and SQLite's busy", err)
		}
	})
}

// TestOpenRefuses checks that Open trusts nothing at the database's path
// that another user could have put there or could change, before SQLite
// opens anything: whoever can write the database can claim an attempt.
func TestOpenRefuses(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows doesn't say who owns a file, so Open refuses every path")
	}
	for _, tc := range []struct {
		name string
		// set up puts what is at the database's path, path, in dir, and
		// returns where SQLite would have made a file it shouldn't, or "".
		setUp func(t *testing.T, dir, path string) string
		kind  *problems.Kind
		at    string // the suffix of the path the problem names, after dir
	}{
		{"a database others can read", func(t *testing.T, _, path string) string {
			writeFile(t, path, 0o644)
			return ""
		}, problems.FileNotPrivate, "/meridian.db"},
		{"a database others can change", func(t *testing.T, _, path string) string {
			writeFile(t, path, 0o666)
			return ""
		}, problems.FileNotPrivate, "/meridian.db"},
		{"a link to a database elsewhere", func(t *testing.T, _, path string) string {
			target := filepath.Join(t.TempDir(), "elsewhere.db")
			writeFile(t, target, 0o600)
			symlink(t, target, path)
			return target + "-wal"
		}, problems.FileNotRegular, "/meridian.db"},
		{"a link to nothing", func(t *testing.T, _, path string) string {
			target := filepath.Join(t.TempDir(), "elsewhere.db")
			symlink(t, target, path)
			return target
		}, problems.FileNotRegular, "/meridian.db"},
		{"a directory at the database's path", func(t *testing.T, _, path string) string {
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
			return ""
		}, problems.FileNotRegular, "/meridian.db"},
		{"a log others can change, before the database", func(t *testing.T, _, path string) string {
			writeFile(t, path+"-wal", 0o666)
			return path
		}, problems.FileNotPrivate, "/meridian.db-wal"},
		{"a log's index that is a link", func(t *testing.T, _, path string) string {
			symlink(t, filepath.Join(t.TempDir(), "shm"), path+"-shm")
			return path
		}, problems.FileNotRegular, "/meridian.db-shm"},
		{"a journal others can read", func(t *testing.T, _, path string) string {
			writeFile(t, path, 0o600)
			writeFile(t, path+"-journal", 0o644)
			return ""
		}, problems.FileNotPrivate, "/meridian.db-journal"},
		{"a directory others can change", func(t *testing.T, dir, _ string) string {
			chmod(t, dir, 0o777)
			return filepath.Join(dir, "meridian.db")
		}, problems.DirectoryNotPrivate, ""},
		{"a directory others can read", func(t *testing.T, dir, _ string) string {
			chmod(t, dir, 0o755)
			return filepath.Join(dir, "meridian.db")
		}, problems.DirectoryNotPrivate, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := privateDir(t)
			path := filepath.Join(dir, "meridian.db")
			made := tc.setUp(t, dir, path)
			s, err := Open(t.Context(), path, nil)
			if err == nil {
				s.Close()
				t.Fatal("Open succeeded")
			}
			p, ok := errors.AsType[*problem.Problem](err)
			if !ok || p.Code != tc.kind.Code() {
				t.Fatalf("Open = %v, want a problem of kind %s", err, tc.kind.Code())
			}
			if want := dir + tc.at; p.Params["path"] != want {
				t.Errorf("the problem names %v, want %s", p.Params["path"], want)
			}
			if made != "" {
				if _, err := os.Lstat(made); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("Open made %s: %v", made, err)
				}
			}
		})
	}
}

// privateDir returns a directory of the test's that only its user can
// use, as Open asks of the database's: t.TempDir makes it as the umask
// allows.
func privateDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	chmod(t, dir, 0o700)
	return dir
}

// writeFile makes an empty file at path with the mode perm.
func writeFile(t *testing.T, path string, perm os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	chmod(t, path, perm)
}

func chmod(t *testing.T, path string, perm os.FileMode) {
	t.Helper()
	if err := os.Chmod(path, perm); err != nil {
		t.Fatal(err)
	}
}

func symlink(t *testing.T, target, path string) {
	t.Helper()
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
}

// TestMigrations checks that Open migrates an empty database, and that
// opening it again changes nothing.
func TestMigrations(t *testing.T) {
	path := filepath.Join(privateDir(t), "meridian.db")
	applied := func() map[int]int64 {
		s := openAt(t, path)
		out := map[int]int64{}
		err := s.View(t.Context(), func(tx *Tx) error {
			rows, err := tx.tx.QueryContext(tx.ctx, "SELECT version, applied_at FROM meridian_schema")
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var v int
				var at int64
				if err := rows.Scan(&v, &at); err != nil {
					return err
				}
				out[v] = at
			}
			return rows.Err()
		})
		if err != nil {
			t.Fatal(err)
		}
		checkIntegrity(t, s)
		return out
	}
	first := applied()
	if len(first) != SchemaVersion() || SchemaVersion() < 1 {
		t.Fatalf("applied %v, want versions 1 to %d", first, SchemaVersion())
	}
	for v := 1; v <= SchemaVersion(); v++ {
		if _, ok := first[v]; !ok {
			t.Errorf("version %d wasn't applied", v)
		}
	}
	if again := applied(); fmt.Sprint(again) != fmt.Sprint(first) {
		t.Errorf("opened again, applied %v, was %v", again, first)
	}
}

// TestDatabaseTooNew checks that Open refuses a database a newer release
// migrated past what this one knows: Meridian's schema, or hopper's, which
// hoppermigrate.Up leaves as it is.
func TestDatabaseTooNew(t *testing.T) {
	for _, tc := range []struct {
		schema, table   string
		version, latest int
	}{
		{"meridian", "meridian_schema", SchemaVersion() + 1, SchemaVersion()},
		{"hopper", "hopper_schema", hoppermigrate.Latest() + 1, hoppermigrate.Latest()},
	} {
		t.Run(tc.schema, func(t *testing.T) {
			path := filepath.Join(privateDir(t), "meridian.db")
			s, err := Open(t.Context(), path, nil)
			if err != nil {
				t.Fatal(err)
			}
			inTx(t, s, func(tx *Tx) error {
				_, err := tx.tx.ExecContext(tx.ctx, "INSERT INTO "+tc.table+" (version, applied_at) VALUES (?, 0)", tc.version)
				return err
			})
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(t.Context(), path, nil)
			if err == nil {
				s.Close()
			}
			p, ok := errors.AsType[*problem.Problem](err)
			if !ok || p.Code != problems.DatabaseTooNew.Code() {
				t.Fatalf("Open = %v, want a problem of kind %s", err, problems.DatabaseTooNew.Code())
			}
			resolved, err := filepath.EvalSymlinks(path)
			if err != nil {
				t.Fatal(err)
			}
			if p.Params["path"] != resolved || p.Params["schema"] != tc.schema || p.Params["version"] != tc.version || p.Params["latest"] != tc.latest {
				t.Errorf("the problem's params are %v", p.Params)
			}
		})
	}
}

// TestNetworkFilesystem checks that Open refuses a database hoppersqlite
// finds on a network filesystem with a problem, which says where and what
// it found, as hoppersqlite's error says them.
func TestNetworkFilesystem(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"checked", fmt.Errorf("%w: %s is on %s", hoppersqlite.ErrNetworkFilesystem, "/home/someone is on/x", "NFS (filesystem type 0x6969)")},
		{"after open", fmt.Errorf("hoppersqlite: open /a: %w", fmt.Errorf("%w: %s is on %s", hoppersqlite.ErrNetworkFilesystem, "/home/someone is on/x", "NFS (filesystem type 0x6969)"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			was := openSQLite
			t.Cleanup(func() { openSQLite = was })
			openSQLite = func(context.Context, string, *hoppersqlite.Config) (*hoppersqlite.Driver, error) { return nil, tc.err }
			path := filepath.Join(privateDir(t), "meridian.db")
			_, err := Open(t.Context(), path, nil)
			p, ok := errors.AsType[*problem.Problem](err)
			if !ok || p.Code != problems.NetworkFilesystem.Code() {
				t.Fatalf("Open = %v, want a problem of kind %s", err, problems.NetworkFilesystem.Code())
			}
			if p.Params["path"] != "/home/someone is on/x" || p.Params["type"] != "NFS (filesystem type 0x6969)" {
				t.Errorf("the problem's params are %v", p.Params)
			}
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("Open left the database's file it refused: %v", err)
			}
		})
	}

	// An error that doesn't say what hoppersqlite's do is the problem's
	// type, with the database's directory.
	was := openSQLite
	t.Cleanup(func() { openSQLite = was })
	openSQLite = func(context.Context, string, *hoppersqlite.Config) (*hoppersqlite.Driver, error) {
		return nil, fmt.Errorf("%w, somehow", hoppersqlite.ErrNetworkFilesystem)
	}
	dir := privateDir(t)
	_, err := Open(t.Context(), filepath.Join(dir, "meridian.db"), nil)
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Params["path"] != dir || p.Params["type"] != hoppersqlite.ErrNetworkFilesystem.Error()+", somehow" {
		t.Errorf("Open = %v", err)
	}
}

// TestTxRollsBack checks that a transaction whose function fails, or
// panics, stores nothing of what it did.
func TestTxRollsBack(t *testing.T) {
	s := open(t)
	boom := errors.New("boom")
	err := s.Tx(t.Context(), func(tx *Tx) error {
		if _, err := tx.NewRunID(); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Tx = %v, want the function's error", err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("Tx didn't panic again")
			}
		}()
		_ = s.Tx(t.Context(), func(tx *Tx) error {
			if _, err := tx.NewRunID(); err != nil {
				return err
			}
			panic("boom") //nolint:forbidigo // what the test checks Tx does with
		})
	}()
	if id := createRun(t, s); id != "1" {
		t.Errorf("the first run stored is %s, want 1", id)
	}
}

func TestViewDoesNotWrite(t *testing.T) {
	s := open(t)
	err := s.View(t.Context(), func(tx *Tx) error {
		_, err := tx.NewRunID()
		return err
	})
	if err == nil {
		t.Fatal("NewRunID in a View succeeded")
	}
	if id := createRun(t, s); id != "1" {
		t.Errorf("the first run stored is %s, want 1", id)
	}
}
