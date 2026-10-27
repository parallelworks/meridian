package store

import (
	"errors"

	"github.com/parallelworks/meridian/internal/flock"
)

// ErrStartHeld is what HoldStart returns for a run whose Start another
// holds, in this process or another.
var ErrStartHeld = errors.New("store: another holds the run's Start")

// HoldStart holds the Start of the run id, as the coordinator that works
// the run's start queue of its own does, so that no other does as long as
// it lives: until release is called, or the process ends. It is a lock on
// a file beside the database, named for the database and the run, which
// release removes: ErrStartHeld while another holds it. On a system
// without file locks, it fails.
func (s *Store) HoldStart(id string) (release func(), err error) {
	if s.readOnly {
		return nil, ErrReadOnly
	}
	if _, ok := runNumber(id); !ok {
		return nil, ErrNotFound
	}
	release, err = flock.TryLock(s.file + ".start-" + id + ".lock")
	if errors.Is(err, flock.ErrHeld) {
		return nil, ErrStartHeld
	}
	return release, err
}
