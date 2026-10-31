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
	"strconv"
	"strings"
	"time"

	"github.com/parallelworks/foundation/problem"

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
	// each (compile.Sessions.Check); or, where called is set, for a
	// workflow a step calls, checks them alone, since the coordinator
	// names those, by where the workflow is called, as format 1 does, and
	// prompt-for-name asks nothing of them.
	Check(ctx context.Context, sessions map[string]any, called bool) (map[string]string, error)
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
	// Problem is why the process that serves the session can't, as its
	// host last said (Coordinator.SessionServed), or nil.
	Problem *problem.Problem
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
type sessionProvider struct {
	c *Coordinator
	// call is the call of the workflow a step calls that it compiles, as
	// childSessions gives it, or nil for a run no step called.
	call *sessionCall
}

var _ compile.Sessions = sessionProvider{}

// A sessionCall is where in its tree a workflow a step calls is called:
// the workflow of the tree's root, as format 1 names it (workflowName),
// the root's ID, and the calls from the root down to it.
type sessionCall struct {
	workflow, root string
	path           []callLevel
}

// A callLevel is a call of a workflow in a tree, as where a session's
// workflow is called says it (store.Session.CallPath): the calling step's
// job, without a matrix instance's index, the step's index in its job,
// and whether it is the step's cleanup, but neither its try nor the
// attempt, so that every try, attempt and matrix instance of the call
// shares its sessions, as format 1's do, which it makes once for each
// call, as a run is submitted.
type callLevel struct {
	Job     string `json:"job"`
	Step    int    `json:"step"`
	Cleanup bool   `json:"cleanup,omitzero"`
}

// levelOf returns the call that link links a child run to.
func levelOf(link *store.Link) callLevel {
	job, _, _ := strings.Cut(link.Job, "#")
	return callLevel{Job: job, Step: link.Step, Cleanup: link.Cleanup}
}

// callPathOf returns the calls from its tree's root down to run, as tx
// reads them: none for the root.
func callPathOf(tx *store.Tx, run *store.Run) ([]callLevel, error) {
	var path []callLevel
	for r := run; r.Parent != nil; {
		path = append(path, levelOf(r.Parent))
		var err error
		if r, err = tx.Run(r.Parent.Run); err != nil {
			return nil, err
		}
	}
	slices.Reverse(path)
	return path, nil
}

// encodeCallPath returns path as the store keeps it
// (store.Session.CallPath): "" for the root's own sessions, or JSON.
func encodeCallPath(path []callLevel) (string, error) {
	if len(path) == 0 {
		return "", nil
	}
	b, err := jsonv2.Marshal(path, jsonv2.Deterministic(true))
	return string(b), err
}

// sessionName returns the name of the session key of the workflow that is
// called at path in the tree whose root is the run root, of the workflow
// named workflow, as format 1 names it: the workflow, the root, each call's
// job and step, as step_<n>, and the key, joined with underscores, what of
// each a session's path can't hold as it is made a hyphen.
func sessionName(workflow, root string, path []callLevel, key string) string {
	parts := []string{workflow, root}
	for _, l := range path {
		parts = append(parts, l.Job, "step_"+strconv.Itoa(l.Step))
		if l.Cleanup {
			parts = append(parts, "cleanup")
		}
	}
	parts = append(parts, key)
	for i, part := range parts {
		parts[i] = strings.Map(func(r rune) rune {
			if 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || '0' <= r && r <= '9' || r == '.' || r == '_' || r == '-' {
				return r
			}
			return '-'
		}, part)
	}
	return strings.Join(parts, "_")
}

// workflowName returns the name of the workflow at location, a run's own
// (plan.Source.Location), as a session's name begins with it: its file's,
// without the directory, the repository, the revision or the extension,
// as a workflow saved on a platform is named; or "workflow" where nothing
// is left.
func workflowName(location string) string {
	name := location[strings.LastIndexAny(location, "/\\:")+1:]
	name, _, _ = strings.Cut(name, "@")
	for _, ext := range []string{".yaml", ".yml"} {
		name = strings.TrimSuffix(name, ext)
	}
	name = strings.Trim(name, "<>")
	if name == "" {
		return "workflow"
	}
	return name
}

// childSessions returns the Sessions the child run that link links to is
// compiled with, as a run of the tree whose root's plan is rootPlan: where
// the coordinator provides Sessions, its own, which names the child's
// sessions by where in the tree it is called (sessionName); otherwise
// Options.Sessions.
func (c *Coordinator) childSessions(ctx context.Context, link *store.Link, rootPlan *plan.Plan) (compile.Sessions, error) {
	p, ok := c.opts.Sessions.(sessionProvider)
	if !ok {
		return c.opts.Sessions, nil
	}
	var path []callLevel
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		run, err := tx.Run(link.Run)
		if err != nil {
			return err
		}
		path, err = callPathOf(tx, run)
		return err
	}); err != nil {
		return nil, err
	}
	p.call = &sessionCall{workflow: workflowName(rootPlan.Run.Source.Location), root: link.Root, path: append(path, levelOf(link))}
	return p, nil
}

// Check checks the workflow's sessions:, and names them: as the host says
// (SessionHost.Check), for a run no step called; or, for a workflow a step
// calls, once the host has checked them, by where it is called
// (sessionName), the same for every try, attempt and matrix instance of
// the call, whose runs share them.
func (p sessionProvider) Check(ctx context.Context, sessions map[string]any) (map[string]string, error) {
	if p.call == nil {
		return p.c.opts.SessionHost.Check(ctx, sessions, false)
	}
	if _, err := p.c.opts.SessionHost.Check(ctx, sessions, true); err != nil {
		return nil, err
	}
	names := make(map[string]string, len(sessions))
	for key := range sessions {
		names[key] = sessionName(p.call.workflow, p.call.root, p.call.path, key)
	}
	return names, nil
}

// Make keeps the sessions of the run runID, under the names Check gave
// them, each with a salt of its own, owned by the root of the run's tree:
// in the clear, or sealed under the root's data key where evaluating them
// read a variable, a password or a secret (read). It keeps all of them, or
// none, in one transaction. A session made already, where the run is called
// in its tree, is made no more, as for a run started again, or another
// try, attempt or matrix instance of a call, which shares its sessions;
// one whose name another session that hasn't ended has is
// session_name_taken, which fails the run. A session made points nowhere,
// so its host has nothing to serve yet.
func (p sessionProvider) Make(ctx context.Context, runID string, sessions map[string]any, names map[string]string, read expr.Read) error {
	c := p.c
	private := read.Private || read.Marked
	var taken string
	err := c.store.Tx(ctx, func(tx *store.Tx) error {
		taken = ""
		run, err := tx.Run(runID)
		if err != nil {
			return err
		}
		root := run.ID
		if run.Parent != nil {
			root = run.Parent.Root
		}
		path, err := callPathOf(tx, run)
		if err != nil {
			return err
		}
		callPath, err := encodeCallPath(path)
		if err != nil {
			return err
		}
		for _, key := range slices.Sorted(maps.Keys(sessions)) {
			name, ok := names[key]
			if !ok {
				return fmt.Errorf("coordinator: the session %q has no name", key)
			}
			def, _ := sessions[key].(map[string]any)
			s := store.Session{
				ID: newSessionID(), Root: root, Run: runID, CallPath: callPath, Key: key, Name: name,
				Type: "tunnel", Redirect: def["redirect"] == true, Label: sessionproxy.NewLabel(), Salt: newSalt(),
			}
			if def["type"] == "link" {
				s.Type = "link"
			}
			data, err := jsonv2.Marshal(sessionData{Def: def}, jsonv2.Deterministic(true))
			if err != nil {
				return err
			}
			s.Data = data
			if private {
				s.Sealed, err = c.sealSession(ctx, tx, root, s.ID, 1, data)
				clear(data)
				if err != nil {
					return err
				}
				s.Data, s.Generation = nil, 1
			}
			_, err = tx.MakeSession(s)
			clear(data)
			if errors.Is(err, store.ErrSessionNameTaken) {
				taken = name
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	if taken != "" {
		return problems.SessionNameTaken.Problem(problems.Params{"name": taken})
	}
	return err
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
	read, err := c.readSessions(ctx, func(tx *store.Tx) ([]store.Session, error) {
		s, err := tx.SessionNamed(u.Name)
		if err != nil {
			return nil, err
		}
		return []store.Session{*s}, nil
	})
	switch {
	case errors.Is(err, store.ErrNotFound):
		return problems.SessionUnknown.Problem(problems.Params{"name": u.Name})
	case err != nil:
		return err
	case read.stored[0].Root != u.Root && !slices.Contains(u.Permissions, "*"):
		return problems.SessionNotPermitted.Problem(problems.Params{"name": u.Name})
	}
	s, err := read.open(ctx, 0)
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
	err = c.store.Tx(ctx, func(tx *store.Tx) error {
		// Another process's step can have pointed it since it was read,
		// as another instance of a matrix job that shares it does: the
		// last to point it is where it points, sealed from then on where
		// either sealed it, at the next generation.
		now, err := tx.Session(s.ID)
		if err != nil {
			return err
		}
		if now.Sealed == nil && !s.Private && !u.Read.Private && !u.Read.Marked {
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
// every sessionsPoll, for what another process changed, as one that served
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
		c.sessionLeases, c.sessionsSeen = nil, nil
	}()
	ticker := time.NewTicker(c.sessionsPoll)
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
	live, seen, err := c.liveSessions(ctx)
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
	// The lease files of those that have ended since, which a process
	// that ended while it held them left, go too.
	for id := range c.sessionsSeen {
		if seen[id] {
			continue
		}
		if err := c.store.ForgetSessionLease(id); err != nil {
			c.log.Warn("coordinator: couldn't remove an ended session's lease", "session", id, "err", err)
		}
	}
	c.sessionsSeen = seen
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
	read, err := c.readSessions(ctx, func(tx *store.Tx) ([]store.Session, error) { return tx.Sessions(root) })
	if err != nil {
		return nil, err
	}
	out := make([]Session, len(read.stored))
	for i, s := range read.stored {
		if c.sealer == nil && s.Sealed != nil {
			out[i] = sessionOf(s)
			continue
		}
		if out[i], err = read.open(ctx, i); err != nil {
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

// SessionServed records how the host serves the session id, which this
// process serves, as it serves it anew, or at another port than before, or
// can't (store.Tx.SessionServed): the port it serves it at, and, where p is
// nil, no problem; or, where p is set, why it can't, which leaves the port
// as it is. What shows the session, as meridian status and meridian
// session url do, in any process, shows it so.
func (c *Coordinator) SessionServed(ctx context.Context, id string, port int, p *problem.Problem) error {
	if err := c.writable(); err != nil {
		return err
	}
	return c.store.Tx(ctx, func(tx *store.Tx) error { return tx.SessionServed(id, port, p) })
}

// ResetSession replaces the salt of the session id, which signs out every
// browser that holds its secret, and deletes its one-time codes. It holds
// sessionsMu as it does, so that a look of ServeSessions's, and a step's
// point, reads the session wholly before the reset or after it; the look
// it wakes serves the new salt.
func (c *Coordinator) ResetSession(ctx context.Context, id string) error {
	if err := c.writable(); err != nil {
		return err
	}
	c.sessionsMu.Lock()
	err := c.store.Tx(ctx, func(tx *store.Tx) error { return tx.ResetSession(id, newSalt()) })
	c.sessionsMu.Unlock()
	if err != nil {
		return err
	}
	c.wakeSessions()
	return nil
}

// liveSessions returns the store's sessions that haven't ended, opened,
// but those it can't open, which it leaves out, and logs, so that one
// doesn't keep the others from being served; and the IDs of all of them.
func (c *Coordinator) liveSessions(ctx context.Context) ([]Session, map[string]bool, error) {
	read, err := c.readSessions(ctx, func(tx *store.Tx) ([]store.Session, error) { return tx.LiveSessions() })
	if err != nil {
		return nil, nil, err
	}
	out := make([]Session, 0, len(read.stored))
	ids := make(map[string]bool, len(read.stored))
	for i, s := range read.stored {
		ids[s.ID] = true
		opened, err := read.open(ctx, i)
		if err != nil {
			c.log.Warn("coordinator: couldn't open a session to serve it", "session", s.ID, "err", err)
			continue
		}
		out = append(out, opened)
	}
	return out, ids, nil
}

// sessionsRead are sessions as the store holds them, read with the data
// keys that open those sealed (readSessions).
type sessionsRead struct {
	c      *Coordinator
	stored []store.Session
	// wrapped are the wrapped data keys of the roots of the sessions that
	// are sealed, by root.
	wrapped map[string][]byte
}

// readSessions reads the sessions that read returns, and, where the
// coordinator holds the keys to open them, the wrapped data keys of the
// roots of those that are sealed, in one View, so that each is opened with
// the key it was sealed under, as the store had both at once. Every root
// that hasn't ended has its key.
func (c *Coordinator) readSessions(ctx context.Context, read func(*store.Tx) ([]store.Session, error)) (*sessionsRead, error) {
	r := &sessionsRead{c: c, wrapped: map[string][]byte{}}
	err := c.store.View(ctx, func(tx *store.Tx) error {
		clear(r.wrapped)
		var err error
		if r.stored, err = read(tx); err != nil || c.sealer == nil {
			return err
		}
		for _, s := range r.stored {
			if s.Sealed == nil {
				continue
			}
			if _, ok := r.wrapped[s.Root]; ok {
				continue
			}
			if r.wrapped[s.Root], err = tx.WrappedKey(s.Root); err != nil {
				return err
			}
		}
		return nil
	})
	return r, err
}

// open returns the i'th session read as a Session: its data in the clear,
// or, where it is sealed, opened with its root's data key.
func (r *sessionsRead) open(ctx context.Context, i int) (Session, error) {
	s := r.stored[i]
	out := sessionOf(s)
	data := s.Data
	if s.Sealed != nil {
		if r.c.sealer == nil {
			return Session{}, fmt.Errorf("coordinator: no keys to open session %s, which is sealed", s.ID)
		}
		opened, err := r.c.sealer.OpenParts(ctx, s.Root, r.wrapped[s.Root], []seal.SealedPart{{
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
		Label: s.Label, Salt: s.Salt, Port: s.Port, PortGiven: s.PortGiven, Problem: s.Problem, Private: s.Sealed != nil,
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
