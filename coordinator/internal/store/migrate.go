package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"regexp"
	"strconv"
)

// A migration is one version of Meridian's schema, beside hopper's: the
// script that brings the schema from the version before to it. Versions
// only add to what is there, from 1, without a gap, and a script, once
// released, never changes.
type migration struct {
	version int
	name    string
	script  string
}

//go:embed migrations/sqlite/*.sql
var migrationFiles embed.FS

var migrationName = regexp.MustCompile(`^(\d{4})_[a-z0-9_]+\.sql$`)

// migrations are SQLite's, in the order of their versions.
var migrations = func() []migration {
	entries, err := fs.ReadDir(migrationFiles, "migrations/sqlite")
	if err != nil {
		panic(err) //nolint:forbidigo // the directory is embedded, so this is a bug in the build
	}
	var out []migration
	for _, e := range entries {
		m := migrationName.FindStringSubmatch(e.Name())
		if m == nil {
			panic("store: malformed migration file name " + e.Name()) //nolint:forbidigo // the files are embedded, so this is a bug in the build
		}
		version, _ := strconv.Atoi(m[1])
		if version != len(out)+1 {
			panic("store: migration " + e.Name() + " out of sequence") //nolint:forbidigo // the files are embedded, so this is a bug in the build
		}
		body, err := fs.ReadFile(migrationFiles, path.Join("migrations/sqlite", e.Name()))
		if err != nil {
			panic(err) //nolint:forbidigo // the directory is embedded, so this is a bug in the build
		}
		out = append(out, migration{version: version, name: e.Name(), script: string(body)})
	}
	return out
}()

// SchemaVersion is the version of Meridian's schema this release brings a
// store to.
func SchemaVersion() int { return len(migrations) }

// migrate brings Meridian's schema up to SchemaVersion, in one
// transaction, which holds the database's write lock from its first
// statement, so that processes that open the store at once migrate it one
// after another, and each finds what the one before did. It refuses a
// schema a newer release brought past what this release knows, and gives
// a store without an ID one.
func (s *Store) migrate(ctx context.Context, log *slog.Logger) error {
	return s.Tx(ctx, func(tx *Tx) error {
		now, err := tx.now()
		if err != nil {
			return err
		}
		if _, err := tx.tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS meridian_schema (
  version INTEGER PRIMARY KEY,
  applied_at INTEGER NOT NULL
) STRICT`); err != nil {
			return fmt.Errorf("store: create meridian_schema: %w", err)
		}
		current, err := tx.schemaVersion()
		if err != nil {
			return err
		}
		if current > len(migrations) {
			return s.tooNew("meridian", current, len(migrations))
		}
		for _, m := range migrations[current:] {
			if _, err := tx.tx.ExecContext(ctx, m.script); err != nil {
				return fmt.Errorf("store: apply %s: %w", m.name, err)
			}
			if _, err := tx.tx.ExecContext(ctx, "INSERT INTO meridian_schema (version, applied_at) VALUES (?, ?)", m.version, now); err != nil {
				return err
			}
			log.InfoContext(ctx, "store: applied a migration", "version", m.version, "name", m.name)
		}
		_, err = tx.tx.ExecContext(ctx, "INSERT INTO meridian_store (key, value) VALUES ('store_id', ?) ON CONFLICT (key) DO NOTHING", newID(16))
		return err
	})
}

// schemaVersion returns the version of Meridian's schema: 0 for a
// database without it.
func (tx *Tx) schemaVersion() (int, error) {
	var exists bool
	if err := tx.tx.QueryRowContext(tx.ctx, "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'meridian_schema')").Scan(&exists); err != nil || !exists {
		return 0, err
	}
	var version int
	if err := tx.tx.QueryRowContext(tx.ctx, "SELECT coalesce(max(version), 0) FROM meridian_schema").Scan(&version); err != nil {
		return 0, fmt.Errorf("store: read the schema's version: %w", err)
	}
	return version, nil
}

// dbFile returns the database's file, as SQLite opened it: with its links
// resolved, the same for every process that opens the database.
func (tx *Tx) dbFile() (string, error) {
	var file string
	err := tx.tx.QueryRowContext(tx.ctx, "SELECT file FROM pragma_database_list WHERE name = 'main'").Scan(&file)
	return file, err
}
