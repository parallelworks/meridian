package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/parallelworks/meridian/internal/flock"
)

// An Endpoint is one that workers reach the coordinator through, such as
// the local socket, as the store holds it.
type Endpoint struct {
	Name string
	// Holder says who holds it, as its holder said itself, and TakenAt
	// since when.
	Holder  string
	TakenAt time.Time
	// GraceFrom is when the grace period after which the coordinator acts
	// on lapsed leases began: when the endpoint was taken, or when its
	// holder's host last woke (RestartGrace).
	GraceFrom time.Time
}

// endpointName is what an endpoint's name is.
var endpointName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// Endpoint returns the endpoint named name, as its holder recorded it:
// ErrNotFound if no one has recorded it since its last holder released it.
// A holder that ended without releasing it leaves it recorded, but no
// longer holds its lock, which the next to take it waits for.
func (tx *Tx) Endpoint(name string) (*Endpoint, error) {
	e := Endpoint{Name: name}
	var taken, grace int64
	err := tx.queryRow("SELECT holder, taken_at, grace_from FROM meridian_endpoints WHERE name = ?", name).Scan(&e.Holder, &taken, &grace)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	e.TakenAt, e.GraceFrom = time.UnixMicro(taken).UTC(), time.UnixMicro(grace).UTC()
	return &e, nil
}

// RestartGrace starts the endpoint's grace period again, from now, as for a
// host that woke from sleep, whose workers' leases lapsed while it slept:
// ErrNotFound if it isn't recorded. Like every time the store records,
// now is read once the transaction holds the write lock (Tx.Now).
func (tx *Tx) RestartGrace(name string) error {
	now, err := tx.now()
	if err != nil {
		return err
	}
	res, err := tx.exec("UPDATE meridian_endpoints SET grace_from = ? WHERE name = ?", now, name)
	if err != nil {
		return err
	}
	return oneRow(res, ErrNotFound)
}

// GraceFrom returns when the latest grace began, in which no lapsed lease
// counts: the latest time an endpoint of the store's was taken, or its
// holder's host woke (RestartGrace), or a grace for the whole store
// started (StartGrace); or the zero time if none is recorded.
func (tx *Tx) GraceFrom() (time.Time, error) {
	var from sql.NullInt64
	err := tx.queryRow(`SELECT max(t) FROM (SELECT max(grace_from) AS t FROM meridian_endpoints
UNION ALL SELECT CAST(value AS INTEGER) FROM meridian_store WHERE key = 'grace')`).Scan(&from)
	return timeOf(from), err
}

// EndpointHeld reports whether a process holds the endpoint named name now
// (HoldEndpoint), as the lock on its file says, without taking it for
// longer than it takes to ask. On a system without file locks, it fails.
func (s *Store) EndpointHeld(name string) (bool, error) {
	if !endpointName.MatchString(name) {
		return false, fmt.Errorf("store: %q isn't an endpoint's name", name)
	}
	return flock.Held(s.file + "." + name + ".lock")
}

// A Hold is an endpoint that this process holds, until it releases it,
// closes the store, or ends. The store keeps it, so that a Hold dropped
// without Release keeps holding the endpoint.
type Hold struct {
	s        *Store
	endpoint Endpoint
	unlock   func()
}

// HoldEndpoint takes the endpoint named name, for holder, which says who
// holds it, and records it, its grace period starting now. Only one
// process holds an endpoint at a time: it holds an exclusive lock on a
// file beside the database, named for the database and the endpoint,
// which the system releases if the process ends. HoldEndpoint waits for
// the process that holds it for as long as ctx allows. Since the database
// is on a local disk, so is the lock, which every process that opens the
// database shares. On a system without file locks, it fails.
func (s *Store) HoldEndpoint(ctx context.Context, name, holder string) (*Hold, error) {
	if s.readOnly {
		return nil, ErrReadOnly
	}
	if !endpointName.MatchString(name) {
		return nil, fmt.Errorf("store: %q isn't an endpoint's name", name)
	}
	if holder == "" {
		return nil, errors.New("store: an endpoint without a holder")
	}
	unlock, err := flock.Lock(ctx, s.file+"."+name+".lock")
	if err != nil {
		return nil, fmt.Errorf("store: lock the endpoint %s: %w", name, err)
	}
	h := &Hold{s: s, unlock: unlock}
	err = s.Tx(ctx, func(tx *Tx) error {
		// Read once the transaction holds the write lock (Tx.Now), so
		// that the grace starts no earlier than the last that another
		// process recorded.
		now, err := tx.now()
		if err != nil {
			return err
		}
		if _, err := tx.tx.ExecContext(ctx, `INSERT INTO meridian_endpoints (name, holder, taken_at, grace_from) VALUES (?, ?, ?, ?)
ON CONFLICT (name) DO UPDATE SET holder = excluded.holder, taken_at = excluded.taken_at, grace_from = excluded.grace_from`,
			name, holder, now, now); err != nil {
			return err
		}
		e, err := tx.Endpoint(name)
		if err == nil {
			h.endpoint = *e
		}
		return err
	})
	if err != nil {
		unlock()
		return nil, err
	}
	s.holdsMu.Lock()
	defer s.holdsMu.Unlock()
	if s.holds == nil {
		s.holds = map[*Hold]struct{}{}
	}
	s.holds[h] = struct{}{}
	return h, nil
}

// Endpoint returns the endpoint as it was when it was taken.
func (h *Hold) Endpoint() Endpoint { return h.endpoint }

// Release stops holding the endpoint: it deletes its record, if it is
// still this hold's, and unlocks it, even if ctx has ended.
func (h *Hold) Release(ctx context.Context) error {
	h.s.holdsMu.Lock()
	delete(h.s.holds, h)
	h.s.holdsMu.Unlock()
	defer h.unlock()
	return h.s.Tx(context.WithoutCancel(ctx), func(tx *Tx) error {
		_, err := tx.exec("DELETE FROM meridian_endpoints WHERE name = ? AND holder = ? AND taken_at = ?",
			h.endpoint.Name, h.endpoint.Holder, h.endpoint.TakenAt.UnixMicro())
		return err
	})
}
