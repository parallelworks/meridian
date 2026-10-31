package store

import (
	"errors"

	"github.com/parallelworks/meridian/internal/flock"
)

// The kinds of the store's holds that lockFile names: a run's Start, and
// a session. An endpoint's, the first the store had, has none.
const (
	holdStart   = "start"
	holdSession = "session"
)

// lockFile returns the path of the file whose lock is the hold of id, of
// kind: beside the database, named for the database, the kind and id, as
// <database>.<kind>-<id>.lock; or, for an endpoint, whose kind is "",
// <database>.<name>.lock. The names never change, since processes of two
// releases can share a store.
func (s *Store) lockFile(kind, id string) string {
	if kind == "" {
		return s.file + "." + id + ".lock"
	}
	return s.file + "." + kind + "-" + id + ".lock"
}

// tryHold takes the hold of id, of kind (lockFile), unless a process
// holds it already, this one or another, which is held, and returns what
// lets go of it, and removes its file. On a system without file locks, it
// fails.
func (s *Store) tryHold(kind, id string, held error) (release func(), err error) {
	release, err = flock.TryLock(s.lockFile(kind, id))
	if errors.Is(err, flock.ErrHeld) {
		return nil, held
	}
	return release, err
}
