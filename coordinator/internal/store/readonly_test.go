package store

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/coordtest"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/problems"
)

// schemaOf returns the version of Meridian's schema that the database at
// path records, read without the store.
func schemaOf(t *testing.T, path string) int {
	t.Helper()
	d, err := openSQLite(t.Context(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var version int
	if err := d.DB().QueryRowContext(t.Context(), "SELECT coalesce(max(version), 0) FROM meridian_schema").Scan(&version); err != nil {
		t.Fatal(err)
	}
	return version
}

// TestOpenReadOnly checks a store opened only to read it: one at this
// release's schema reads, and refuses each call that writes; one that isn't
// there isn't made; and one at an earlier schema, which an earlier release
// still running a workflow may use, or a later one, is refused, and left
// as it is.
func TestOpenReadOnly(t *testing.T) {
	t.Parallel()
	ro := &Options{ReadOnly: true}
	missing := filepath.Join(coordtest.PrivateDir(t), "meridian.db")
	if _, err := Open(t.Context(), missing, ro); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Open read-only of a database that isn't there = %v", err)
	}
	if _, err := os.Lstat(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Open read-only made the database: %v", err)
	}

	path := filepath.Join(coordtest.PrivateDir(t), "meridian.db")
	s := openAt(t, path)
	id := createRun(t, s)
	r, err := Open(t.Context(), path, ro)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.ID() != s.ID() {
		t.Errorf("the store read-only is %s, want %s", r.ID(), s.ID())
	}
	if err := r.View(t.Context(), func(tx *Tx) error {
		_, err := tx.RunSummary(id)
		return err
	}); err != nil {
		t.Errorf("reading a run read-only: %v", err)
	}
	if err := r.Tx(t.Context(), func(*Tx) error { return nil }); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Tx of a store read-only = %v, want ErrReadOnly", err)
	}
	if _, err := r.HoldEndpoint(t.Context(), "socket", "test"); !errors.Is(err, ErrReadOnly) {
		t.Errorf("HoldEndpoint of a store read-only = %v, want ErrReadOnly", err)
	}
	if _, err := r.HoldStart(id); !errors.Is(err, ErrReadOnly) {
		t.Errorf("HoldStart of a store read-only = %v, want ErrReadOnly", err)
	}

	// The store as an earlier release, whose schema's version is one
	// before this release's, left it, and as a later one did.
	inTx(t, s, func(tx *Tx) error {
		_, err := tx.tx.ExecContext(tx.ctx, "DELETE FROM meridian_schema WHERE version = ?", SchemaVersion())
		return err
	})
	before := schemaOf(t, path)
	for _, tc := range []struct {
		kind   *problems.Kind
		insert int
	}{{kind: problems.DatabaseTooOld}, {kind: problems.DatabaseTooNew, insert: SchemaVersion() + 1}} {
		if tc.insert > 0 {
			inTx(t, s, func(tx *Tx) error {
				_, err := tx.tx.ExecContext(tx.ctx, "INSERT INTO meridian_schema (version, applied_at) VALUES (?, 0)", tc.insert)
				return err
			})
			before = tc.insert
		}
		r, err := Open(t.Context(), path, ro)
		if err == nil {
			r.Close()
		}
		if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != tc.kind.Code() {
			t.Errorf("Open read-only = %v, want a problem of kind %s", err, tc.kind.Code())
		}
		if version := schemaOf(t, path); version != before {
			t.Errorf("Open read-only changed the schema from version %d to %d", before, version)
		}
	}
}

// TestHoldStart checks that one holds a run's Start at a time, until it
// lets it go, which removes the hold's file.
func TestHoldStart(t *testing.T) {
	t.Parallel()
	s := open(t)
	release, err := s.HoldStart("1")
	if err != nil {
		t.Fatal(err)
	}
	file := s.file + ".start-1.lock"
	if _, err := os.Lstat(file); err != nil {
		t.Errorf("the hold's file: %v", err)
	}
	if _, err := s.HoldStart("1"); !errors.Is(err, ErrStartHeld) {
		t.Errorf("HoldStart of a Start held = %v, want ErrStartHeld", err)
	}
	other, err := s.HoldStart("2")
	if err != nil {
		t.Fatalf("HoldStart of another run's Start = %v", err)
	}
	other()
	release()
	if _, err := os.Lstat(file); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the hold's file is there once it is let go: %v", err)
	}
	again, err := s.HoldStart("1")
	if err != nil {
		t.Fatalf("HoldStart once let go = %v", err)
	}
	again()
	for _, bad := range []string{"0", "x", "../1"} {
		if _, err := s.HoldStart(bad); !errors.Is(err, ErrNotFound) {
			t.Errorf("HoldStart(%q) = %v, want ErrNotFound", bad, err)
		}
	}
}
