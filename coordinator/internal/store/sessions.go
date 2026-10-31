package store

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// A Session is a session a run's sessions: made, as the store holds it
// (meridian_sessions).
type Session struct {
	// ID is the session's, Root the run at the top of the tree that made
	// it, which owns it, and Run the run whose sessions: made it, under
	// its key Key; Name is its name, which sessions.<key> gives.
	ID, Root, Run, Key, Name string
	// Type is tunnel or link, and Redirect whether its redirect: is true.
	Type     string
	Redirect bool
	// Label is the session's, at whose host name it is served, and Salt
	// the session's, which its secret is made of.
	Label string
	Salt  []byte
	// Data is what the session is, and where it points, as the
	// coordinator encodes it: in the clear, or Sealed, as a part of the
	// root's secrets, of generation Generation, once what gave it read a
	// variable or a password; the other is nil.
	Data       []byte
	Sealed     []byte
	Generation uint64
	// Port is the port of 127.0.0.1 the session is served at, or 0
	// before it points somewhere, and PortGiven whether the step that
	// pointed it gave it.
	Port      int
	PortGiven bool
	// UpdatedAt is when the session last changed, and EndedAt when its
	// root ended, or the zero time while it hasn't.
	UpdatedAt, EndedAt time.Time
}

// ErrSessionNameTaken is MakeSession's error for a session whose name is
// that of another the store has, which hasn't ended.
var ErrSessionNameTaken = errors.New("store: a live session has the name")

// sessionColumns are what Session reads of a session, in order.
const sessionColumns = "id, root_id, run_id, key, name, type, redirect, label, salt, data, data_sealed, data_gen, port, port_given, updated_at, ended_at"

// MakeSession stores s, a session the run s.Run's sessions: made, under
// its key s.Key, whose tree's root is s.Root, with its ID, name, type,
// redirect, label and salt, and its data, in the clear or sealed: or nothing, for
// a run that has made the session under that key already, as one started
// again does, and reports whether it made it. A name another session that
// hasn't ended has is ErrSessionNameTaken; a run the store hasn't, or that
// has ended, ErrNotFound and ErrState.
func (tx *Tx) MakeSession(s Session) (bool, error) {
	run, err := tx.liveRun(s.Run)
	if err != nil {
		return false, err
	}
	root, err := tx.runNumberOf(s.Root)
	if err != nil {
		return false, err
	}
	var made bool
	if err := tx.queryRow("SELECT EXISTS (SELECT 1 FROM meridian_sessions WHERE run_id = ? AND key = ?)", run, s.Key).Scan(&made); err != nil || made {
		return false, err
	}
	var taken bool
	if err := tx.queryRow("SELECT EXISTS (SELECT 1 FROM meridian_sessions WHERE name = ? AND ended_at IS NULL)", s.Name).Scan(&taken); err != nil {
		return false, err
	}
	if taken {
		return false, ErrSessionNameTaken
	}
	now, err := tx.now()
	if err != nil {
		return false, err
	}
	_, err = tx.exec(`INSERT INTO meridian_sessions (id, root_id, run_id, key, name, type, redirect, label, salt, data, data_sealed, data_gen, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		s.ID, root, run, s.Key, s.Name, s.Type, s.Redirect, s.Label, s.Salt, nullText(s.Data), s.Sealed, s.Generation, now, now)
	return err == nil, err
}

// Session returns the session whose ID is id: ErrNotFound if the store
// hasn't it.
func (tx *Tx) Session(id string) (*Session, error) {
	return tx.oneSession("SELECT "+sessionColumns+" FROM meridian_sessions WHERE id = ?", id)
}

// SessionNamed returns the session named name that hasn't ended:
// ErrNotFound if the store has none.
func (tx *Tx) SessionNamed(name string) (*Session, error) {
	return tx.oneSession("SELECT "+sessionColumns+" FROM meridian_sessions WHERE name = ? AND ended_at IS NULL", name)
}

// Sessions returns the sessions of the tree whose root is the run root,
// those that have ended too, in the order they were made: ErrNotFound for a
// run the store hasn't.
func (tx *Tx) Sessions(root string) ([]Session, error) {
	n, err := tx.runNumberOf(root)
	if err != nil {
		return nil, err
	}
	return tx.sessions("SELECT "+sessionColumns+" FROM meridian_sessions WHERE root_id = ? ORDER BY created_at, run_id, key", n)
}

// LiveSessions returns the store's sessions that haven't ended, in the
// order they were made.
func (tx *Tx) LiveSessions() ([]Session, error) {
	return tx.sessions("SELECT " + sessionColumns + " FROM meridian_sessions WHERE ended_at IS NULL ORDER BY created_at, run_id, key")
}

// PointSession stores what the session id is, and where it points, as its
// data, in the clear or sealed, of the generation gen, and the port it is
// served at: ErrNotFound for a session the store hasn't, and ErrState for
// one that has ended. Data sealed once is never stored in the clear again,
// which is ErrState too, nor sealed again as an older generation.
func (tx *Tx) PointSession(id string, data, sealed []byte, gen uint64, port int, portGiven bool) error {
	s, err := tx.Session(id)
	if err != nil {
		return err
	}
	switch {
	case !s.EndedAt.IsZero():
		return fmt.Errorf("%w: session %s has ended", ErrState, id)
	case s.Sealed != nil && sealed == nil:
		return fmt.Errorf("%w: session %s's data is sealed, and isn't stored in the clear again", ErrState, id)
	case sealed != nil && gen <= s.Generation:
		return fmt.Errorf("%w: session %s's data is of generation %d, and %d isn't newer", ErrState, id, s.Generation, gen)
	}
	now, err := tx.now()
	if err != nil {
		return err
	}
	_, err = tx.exec("UPDATE meridian_sessions SET data = ?, data_sealed = ?, data_gen = ?, port = ?, port_given = ?, updated_at = ? WHERE id = ?",
		nullText(data), sealed, gen, nullInt(port), portGiven, now, id)
	return err
}

// ResetSession replaces the salt of the session id, which signs out every
// browser that holds its secret, and deletes its one-time codes:
// ErrNotFound for a session the store hasn't, and ErrState for one that
// has ended.
func (tx *Tx) ResetSession(id string, salt []byte) error {
	now, err := tx.now()
	if err != nil {
		return err
	}
	res, err := tx.exec("UPDATE meridian_sessions SET salt = ?, updated_at = ? WHERE id = ? AND ended_at IS NULL", salt, now, id)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		if err != nil {
			return err
		}
		if _, err := tx.Session(id); err != nil {
			return err
		}
		return fmt.Errorf("%w: session %s has ended", ErrState, id)
	}
	_, err = tx.exec("DELETE FROM meridian_session_codes WHERE session_id = ?", id)
	return err
}

// AddSessionCode keeps a one-time code of the session id, by its keyed
// hash, mac, until expires, and deletes the store's codes that have
// expired: ErrNotFound for a session the store hasn't, and ErrState for one
// that has ended.
func (tx *Tx) AddSessionCode(id string, mac []byte, expires time.Time) error {
	s, err := tx.Session(id)
	if err != nil {
		return err
	}
	if !s.EndedAt.IsZero() {
		return fmt.Errorf("%w: session %s has ended", ErrState, id)
	}
	now, err := tx.now()
	if err != nil {
		return err
	}
	if _, err := tx.exec("DELETE FROM meridian_session_codes WHERE expires_at <= ?", now); err != nil {
		return err
	}
	_, err = tx.exec("INSERT INTO meridian_session_codes (session_id, mac, expires_at) VALUES (?, ?, ?)", id, mac, expires.UnixMicro())
	return err
}

// RedeemSessionCode uses the one-time code of the session id whose keyed
// hash is mac, and reports whether it was one: kept, of the session, and
// unexpired by the store's clock. One statement deletes it, whether or not
// it had expired, so two that redeem it at once can't both use it.
func (tx *Tx) RedeemSessionCode(id string, mac []byte) (bool, error) {
	now, err := tx.now()
	if err != nil {
		return false, err
	}
	var expires int64
	err = tx.queryRow("DELETE FROM meridian_session_codes WHERE mac = ? AND session_id = ? RETURNING expires_at", mac, id).Scan(&expires)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil && now < expires, err
}

// endSessions ends the sessions of the tree whose root is the run numbered
// root, as it ends: their data is erased, and their codes deleted.
func (tx *Tx) endSessions(root int64) error {
	now, err := tx.now()
	if err != nil {
		return err
	}
	if _, err := tx.exec("DELETE FROM meridian_session_codes WHERE session_id IN (SELECT id FROM meridian_sessions WHERE root_id = ?)", root); err != nil {
		return err
	}
	_, err = tx.exec("UPDATE meridian_sessions SET data = NULL, data_sealed = NULL, ended_at = ?, updated_at = ? WHERE root_id = ? AND ended_at IS NULL",
		now, now, root)
	return err
}

// oneSession returns the session query, with args, finds: ErrNotFound for
// none.
func (tx *Tx) oneSession(query string, args ...any) (*Session, error) {
	ss, err := tx.sessions(query, args...)
	switch {
	case err != nil:
		return nil, err
	case len(ss) == 0:
		return nil, ErrNotFound
	}
	return &ss[0], nil
}

// sessions returns the sessions query, with args, finds, which selects
// sessionColumns.
func (tx *Tx) sessions(query string, args ...any) ([]Session, error) {
	rows, err := tx.query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Session
	for rows.Next() {
		var (
			s             Session
			root, run     int64
			data          sql.NullString
			port          sql.NullInt64
			updated       int64
			ended         sql.NullInt64
			gen           int64
			redirect, pgv bool
		)
		if err := rows.Scan(&s.ID, &root, &run, &s.Key, &s.Name, &s.Type, &redirect, &s.Label, &s.Salt, &data, &s.Sealed, &gen, &port, &pgv, &updated, &ended); err != nil {
			return nil, err
		}
		s.Root, s.Run, s.Redirect, s.PortGiven = runID(root), runID(run), redirect, pgv
		if data.Valid {
			s.Data = []byte(data.String)
		}
		s.Generation, s.Port = uint64(gen), int(port.Int64) //nolint:gosec // a generation the store wrote, from 0
		s.UpdatedAt, s.EndedAt = time.UnixMicro(updated).UTC(), timeOf(ended)
		out = append(out, s)
	}
	return out, rows.Err()
}

// nullText is data as text, or NULL for nil.
func nullText(data []byte) sql.NullString {
	return sql.NullString{String: string(data), Valid: data != nil}
}

// nullInt is n, or NULL for 0.
func nullInt(n int) sql.NullInt64 { return sql.NullInt64{Int64: int64(n), Valid: n != 0} }
