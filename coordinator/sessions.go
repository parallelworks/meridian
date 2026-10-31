package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/sessionproxy"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// A SessionHost serves a store's sessions where its coordinator runs, as
// meridian run serves them on this machine (docs/design.md, Serving a
// session). A coordinator with one, and no Sessions of its own, provides
// Sessions itself: it keeps the sessions in the store, authorizes each
// step that points one, and ends them with their tree; the host decides
// what a session may be, and where it points, and serves it.
type SessionHost interface {
	// Check checks a workflow's sessions:, as they are written, and names
	// each (compile.Sessions.Check).
	Check(ctx context.Context, sessions map[string]any) (map[string]string, error)
	// Point reads what a step that uses parallelworks/update-session asks
	// of the session s, as u gives it, which the coordinator has
	// authorized, and, where serve is set, serves the session where it
	// says: it returns where the session points, encoded, which the store
	// keeps, and which the host is given again, and the port it serves it
	// at. Where serve isn't set, as for a session another process serves,
	// it serves nothing, and returns the port the step gave, or the one
	// s is served at already. A refusal, as of a port in use, is a
	// problem of a workflow's kind, which fails the step; any other error,
	// one it couldn't answer, which the step asks again. It mustn't log or
	// quote what u.With gives where u.Read says it read a variable, a
	// password or a secret.
	Point(ctx context.Context, s Session, u compile.SessionUpdate, serve bool) (SessionPoint, error)
	// Serve has the host serve live, the store's sessions that haven't
	// ended that this process serves, as the store has them, and no
	// other: those it doesn't serve yet it serves, as a process that takes
	// over from another does, those whose salt or point changed it serves
	// so, and others it stops serving; a session that points nowhere yet
	// it serves nothing for. The coordinator calls it whenever they may
	// have changed, and every so often, for changes another process made,
	// and, as it stops serving, with none (Coordinator.ServeSessions).
	Serve(ctx context.Context, live []Session)
}

// A Session is a session a run's sessions: made, as the coordinator gives
// it to its session host, and to who shows it.
type Session struct {
	// ID is the session's, Root the run at the top of the tree that made
	// it, which owns it, and Run the run whose sessions: made it, under
	// its key Key; Name is its name, which sessions.<key> gives.
	ID, Root, Run, Key, Name string
	// Type is tunnel or link, and Redirect whether its redirect: is true.
	Type     string
	Redirect bool
	// Label is the session's, at whose host name, <label>.localhost, it is
	// served (sessionproxy.HostOf), made once, and Salt the session's,
	// which its secret is made of, and which a reset replaces.
	Label string
	Salt  []byte
	// Def is the session's definition, as its run's sessions: gave it
	// once evaluated, and Point where it points, as its host encoded it
	// (SessionPoint.Point), or nil before a step points it. Neither is
	// there for a private one that a coordinator opened only to read the
	// store reads, nor once the session has ended.
	Def   map[string]any
	Point []byte
	// Port is the port of 127.0.0.1 the session is served at, or 0 before
	// it points somewhere, and PortGiven whether the step gave it.
	Port      int
	PortGiven bool
	// Private is set where what the session is, or where it points, read
	// a variable, a password or a secret: the store keeps its data sealed,
	// and whoever shows it shows none of it.
	Private bool
	// UpdatedAt is when the session last changed, and EndedAt when its
	// root ended, or the zero time while it hasn't.
	UpdatedAt, EndedAt time.Time
}

// A SessionPoint is where a session points, as its host read it of a step
// that uses parallelworks/update-session (SessionHost.Point).
type SessionPoint struct {
	// Point is where it points, encoded as the host reads it again: JSON.
	Point []byte
	// Port is the port of 127.0.0.1 the host serves the session at, and
	// PortGiven whether the step gave it.
	Port      int
	PortGiven bool
}

// sessionData is what the store keeps of a session as its data
// (store.Session.Data): in the clear, or sealed.
type sessionData struct {
	Def   map[string]any `json:"def"`
	Point jsontext.Value `json:"point,omitzero"`
}

// sessionProvider is the coordinator's compile.Sessions, where it has a
// session host and no Sessions of its own (Options.SessionHost).
type sessionProvider struct{ c *Coordinator }

var _ compile.Sessions = sessionProvider{}

// Check checks the workflow's sessions:, and names them, as the host says
// (SessionHost.Check).
func (p sessionProvider) Check(ctx context.Context, sessions map[string]any) (map[string]string, error) {
	return p.c.opts.SessionHost.Check(ctx, sessions)
}

// Make keeps the sessions of the run runID, under the names Check gave
// them, each with a salt of its own, owned by the root of the run's tree:
// in the clear, or sealed under the root's data key where evaluating them
// read a variable, a password or a secret (read). A session the run made
// already is made no more, as for a run started again; one whose name
// another session that hasn't ended has is session_name_taken, which
// fails the run.
func (p sessionProvider) Make(ctx context.Context, runID string, sessions map[string]any, names map[string]string, read expr.Read) error {
	c := p.c
	var root string
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		run, err := tx.Run(runID)
		if err != nil {
			return err
		}
		root = run.ID
		if run.Parent != nil {
			root = run.Parent.Root
		}
		return nil
	}); err != nil {
		return err
	}
	private := read.Private || read.Marked
	for _, key := range slices.Sorted(maps.Keys(sessions)) {
		name, ok := names[key]
		if !ok {
			return fmt.Errorf("coordinator: the session %q has no name", key)
		}
		def, _ := sessions[key].(map[string]any)
		s := store.Session{
			ID: newSessionID(), Root: root, Run: runID, Key: key, Name: name,
			Type: "tunnel", Redirect: def["redirect"] == true, Label: sessionproxy.NewLabel(), Salt: newSalt(),
		}
		if def["type"] == "link" {
			s.Type = "link"
		}
		data, err := jsonv2.Marshal(sessionData{Def: def}, jsonv2.Deterministic(true))
		if err != nil {
			return err
		}
		err = c.store.Tx(ctx, func(tx *store.Tx) error {
			s.Data, s.Sealed, s.Generation = data, nil, 0
			if private {
				sealed, err := c.sealSession(ctx, tx, root, s.ID, 1, data)
				if err != nil {
					return err
				}
				s.Data, s.Sealed, s.Generation = nil, sealed, 1
			}
			_, err := tx.MakeSession(s)
			return err
		})
		clear(data)
		if errors.Is(err, store.ErrSessionNameTaken) {
			return problems.SessionNameTaken.Problem(problems.Params{"name": name})
		}
		if err != nil {
			return err
		}
	}
	c.wakeSessions()
	return nil
}

// Update points the session u.Name names, as a step of the run u.Run that
// uses parallelworks/update-session asks, once it has authorized it: a run
// whose tree's permissions: hold '*' may point any session of the
// store's, every one of which is the person's who runs it; any other only
// one its own tree made (session_not_permitted). A session the store has
// none of, that hasn't ended, is session_unknown, and a with: that names
// none session_name_empty. The host reads where it points, and serves it
// there (SessionHost.Point), where this process serves the session, as it
// does once it has taken it (takeSession); the store keeps it, sealed from
// then on where with:, or what the session was made of, read a variable,
// a password or a secret.
func (p sessionProvider) Update(ctx context.Context, u compile.SessionUpdate) error {
	c := p.c
	if u.Name == "" {
		return problems.SessionNameEmpty.Problem(nil)
	}
	c.sessionsMu.Lock()
	defer c.sessionsMu.Unlock()
	var stored *store.Session
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		stored, err = tx.SessionNamed(u.Name)
		return err
	})
	switch {
	case errors.Is(err, store.ErrNotFound):
		return problems.SessionUnknown.Problem(problems.Params{"name": u.Name})
	case err != nil:
		return err
	case stored.Root != u.Root && !slices.Contains(u.Permissions, "*"):
		return problems.SessionNotPermitted.Problem(problems.Params{"name": u.Name})
	}
	s, err := c.openSession(ctx, *stored)
	if err != nil {
		return err
	}
	s, serve, err := c.takeSession(ctx, s)
	if err != nil {
		return err
	}
	point, err := c.opts.SessionHost.Point(ctx, s, u, serve)
	if err != nil {
		return err
	}
	data, err := jsonv2.Marshal(sessionData{Def: s.Def, Point: point.Point}, jsonv2.Deterministic(true))
	if err != nil {
		return err
	}
	defer clear(data)
	private := s.Private || u.Read.Private || u.Read.Marked
	err = c.store.Tx(ctx, func(tx *store.Tx) error {
		now, err := tx.Session(s.ID)
		if err != nil {
			return err
		}
		if !private {
			return tx.PointSession(s.ID, data, nil, 0, point.Port, point.PortGiven)
		}
		gen := now.Generation + 1
		sealed, err := c.sealSession(ctx, tx, s.Root, s.ID, gen, data)
		if err != nil {
			return err
		}
		return tx.PointSession(s.ID, nil, sealed, gen, point.Port, point.PortGiven)
	})
	if err != nil {
		return err
	}
	c.wakeSessions()
	return nil
}

// ServeSessions has the coordinator's session host serve the store's
// sessions that haven't ended, and that no other process serves
// (SessionHost.Serve), until ctx is done: at once, as a process that takes
// over from another serves those it served, and then whenever they may
// have changed in this process, as a step points one or a run ends, and
// every pollInterval, for what another process changed, as one that served
// a session stopped. It needs a session host. As it stops, it has the host
// serve none, and lets them go.
//
// It takes each session that points somewhere, and that no process serves,
// to serve it, which resets it (takeSession): those that another process
// serves, which they hold, it leaves to them, salt and codes and all.
func (c *Coordinator) ServeSessions(ctx context.Context) error {
	if err := c.writable(); err != nil {
		return err
	}
	host := c.opts.SessionHost
	if host == nil {
		return errors.New("coordinator: no session host to serve sessions")
	}
	c.sessionsMu.Lock()
	if c.sessionLeases != nil {
		c.sessionsMu.Unlock()
		return errors.New("coordinator: ServeSessions is already running")
	}
	c.sessionLeases = map[string]func(){}
	c.sessionsMu.Unlock()
	defer func() {
		c.sessionsMu.Lock()
		defer c.sessionsMu.Unlock()
		// The host stops serving them before they are let go, so that
		// no other process serves one while this one still does.
		host.Serve(context.WithoutCancel(ctx), nil)
		for _, release := range c.sessionLeases {
			release()
		}
		c.sessionLeases = nil
	}()
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()
	for {
		err := c.serveSessions(ctx)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			c.log.Warn("coordinator: couldn't read the store's sessions", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-c.sessionsWake:
		case <-ticker.C:
		}
	}
}

// serveSessions has the host serve the store's sessions that haven't ended
// that this process serves: those it took already, and those that point
// somewhere that no process serves, which it takes (takeSession). It lets
// go of those that have ended, once the host serves them no more.
func (c *Coordinator) serveSessions(ctx context.Context) error {
	c.sessionsMu.Lock()
	defer c.sessionsMu.Unlock()
	live, err := c.liveSessions(ctx)
	if err != nil {
		return err
	}
	mine := make([]Session, 0, len(c.sessionLeases))
	ids := make(map[string]bool, len(live))
	for _, s := range live {
		ids[s.ID] = true
		if _, ok := c.sessionLeases[s.ID]; !ok && s.Point == nil {
			// One that points nowhere is served by none, and taken by
			// the process whose step points it first.
			continue
		}
		s, ok, err := c.takeSession(ctx, s)
		switch {
		case errors.Is(err, store.ErrState) || errors.Is(err, store.ErrNotFound):
			// It ended since it was read.
		case err != nil:
			c.log.Warn("coordinator: couldn't take a session to serve", "session", s.ID, "err", err)
		case ok:
			mine = append(mine, s)
		}
	}
	c.opts.SessionHost.Serve(ctx, mine)
	for id, release := range c.sessionLeases {
		if !ids[id] {
			release()
			delete(c.sessionLeases, id)
		}
	}
	return nil
}

// takeSession takes the session s for this process to serve, unless it
// serves it already (store.Store.ServeSession), which resets it, and
// returns s as the host is to serve it, with its new salt, and whether
// this process serves it: not while ServeSessions doesn't run, nor while
// another process serves it, which holds it. The caller holds sessionsMu,
// from before it read s until the host has been asked to serve it, so that
// no reset of this process's comes between.
func (c *Coordinator) takeSession(ctx context.Context, s Session) (Session, bool, error) {
	if c.sessionLeases == nil {
		return s, false, nil
	}
	if _, ok := c.sessionLeases[s.ID]; ok {
		return s, true, nil
	}
	salt := newSalt()
	release, err := c.store.ServeSession(ctx, s.ID, salt)
	switch {
	case errors.Is(err, store.ErrSessionServed):
		return s, false, nil
	case err != nil:
		return s, false, err
	}
	c.sessionLeases[s.ID] = release
	s.Salt = salt
	return s, true, nil
}

// wakeSessions tells ServeSessions that the store's sessions may have
// changed.
func (c *Coordinator) wakeSessions() {
	select {
	case c.sessionsWake <- struct{}{}:
	default:
	}
}

// RunSessions returns the sessions of the tree whose root is the run root,
// those that have ended too, in the order they were made: what a private
// one is, and where it points, only where the coordinator holds the keys
// to open it, which one opened only to read the store doesn't.
func (c *Coordinator) RunSessions(ctx context.Context, root string) ([]Session, error) {
	var stored []store.Session
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		stored, err = tx.Sessions(root)
		return err
	}); err != nil {
		return nil, err
	}
	out := make([]Session, len(stored))
	for i, s := range stored {
		if c.sealer == nil || s.Sealed == nil {
			out[i] = sessionOf(s)
			if s.Sealed == nil {
				if err := decodeSession(&out[i], s.Data); err != nil {
					return nil, err
				}
			}
			continue
		}
		var err error
		if out[i], err = c.openSession(ctx, s); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// AddSessionCode keeps a one-time code of the session id, by its keyed
// hash, mac, for lifetime, by the store's clock, which a link to it holds.
func (c *Coordinator) AddSessionCode(ctx context.Context, id string, mac []byte, lifetime time.Duration) error {
	if err := c.writable(); err != nil {
		return err
	}
	return c.store.Tx(ctx, func(tx *store.Tx) error { return tx.AddSessionCode(id, mac, lifetime) })
}

// RedeemSessionCode uses the one-time code of the session id whose keyed
// hash is mac, and reports whether it was one: kept, and unexpired. What
// can't answer, as a store that can't be written, answers false.
func (c *Coordinator) RedeemSessionCode(ctx context.Context, id string, mac []byte) bool {
	if c.writable() != nil {
		return false
	}
	var ok bool
	err := c.store.Tx(ctx, func(tx *store.Tx) error {
		var err error
		ok, err = tx.RedeemSessionCode(id, mac)
		return err
	})
	if err != nil {
		c.log.Warn("coordinator: couldn't redeem a session's code", "session", id, "err", err)
		return false
	}
	return ok
}

// ResetSession replaces the salt of the session id, which signs out every
// browser that holds its secret, and deletes its one-time codes.
func (c *Coordinator) ResetSession(ctx context.Context, id string) error {
	if err := c.writable(); err != nil {
		return err
	}
	if err := c.store.Tx(ctx, func(tx *store.Tx) error { return tx.ResetSession(id, newSalt()) }); err != nil {
		return err
	}
	c.wakeSessions()
	return nil
}

// liveSessions returns the store's sessions that haven't ended, opened.
func (c *Coordinator) liveSessions(ctx context.Context) ([]Session, error) {
	var stored []store.Session
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		stored, err = tx.LiveSessions()
		return err
	}); err != nil {
		return nil, err
	}
	out := make([]Session, 0, len(stored))
	for _, s := range stored {
		opened, err := c.openSession(ctx, s)
		if err != nil {
			return nil, err
		}
		out = append(out, opened)
	}
	return out, nil
}

// openSession returns s, as the store holds it, as a Session: its data in
// the clear, or, where it is sealed, opened with its root's data key.
func (c *Coordinator) openSession(ctx context.Context, s store.Session) (Session, error) {
	out := sessionOf(s)
	data := s.Data
	if s.Sealed != nil {
		var wrapped []byte
		if err := c.store.View(ctx, func(tx *store.Tx) error {
			var err error
			wrapped, err = tx.WrappedKey(s.Root)
			return err
		}); err != nil {
			return Session{}, err
		}
		opened, err := c.sealer.OpenParts(ctx, s.Root, wrapped, []seal.SealedPart{{
			Purpose: seal.PurposeSession, ID: s.ID, Generation: s.Generation, Sealed: s.Sealed,
		}})
		if err != nil {
			return Session{}, err
		}
		data = opened[0]
		defer clear(data)
	}
	if err := decodeSession(&out, data); err != nil {
		return Session{}, err
	}
	return out, nil
}

// sessionOf returns what s, as the store holds it, says of a session, but
// its data.
func sessionOf(s store.Session) Session {
	return Session{
		ID: s.ID, Root: s.Root, Run: s.Run, Key: s.Key, Name: s.Name, Type: s.Type, Redirect: s.Redirect,
		Label: s.Label, Salt: s.Salt, Port: s.Port, PortGiven: s.PortGiven, Private: s.Sealed != nil,
		UpdatedAt: s.UpdatedAt, EndedAt: s.EndedAt,
	}
}

// decodeSession fills in s's definition and point from data, the
// session's data in the clear, if it has any.
func decodeSession(s *Session, data []byte) error {
	if data == nil {
		return nil
	}
	var d sessionData
	if err := jsonv2.Unmarshal(data, &d); err != nil {
		// What the decoder says can quote what the session holds.
		return fmt.Errorf("%w: session %s's data doesn't decode", plan.ErrMalformed, s.ID)
	}
	s.Def = d.Def
	if len(d.Point) > 0 {
		s.Point = slices.Clone([]byte(d.Point))
	}
	return nil
}

// sealSession seals data, the data of the session id, as generation gen,
// under the data key of its root's record, as tx reads it.
func (c *Coordinator) sealSession(ctx context.Context, tx *store.Tx, root, id string, gen uint64, data []byte) ([]byte, error) {
	wrapped, err := tx.WrappedKey(root)
	if err != nil {
		return nil, err
	}
	return c.sealer.SealPart(ctx, root, wrapped, seal.PartID{Purpose: seal.PurposeSession, ID: id, Generation: gen}, data)
}

// newSessionID returns a new session's ID: 128 random bits, in hex.
func newSessionID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b) // crypto/rand doesn't fail
	return hex.EncodeToString(b)
}

// newSalt returns a new salt for a session.
func newSalt() []byte {
	b := make([]byte, sessionproxy.SaltLen)
	_, _ = rand.Read(b) // crypto/rand doesn't fail
	return b
}
