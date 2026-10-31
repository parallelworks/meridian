package coordinator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/internal/sessionproxy"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// runSessions returns the sessions of the tree whose root is the run root,
// those that have ended too, in the order they were made, as CallSessions
// opens them.
func (c *Coordinator) runSessions(ctx context.Context, root string) ([]Session, error) {
	read, err := c.readSessions(ctx, func(tx *store.Tx) ([]store.Session, error) { return tx.Sessions(root) })
	if err != nil {
		return nil, err
	}
	return read.openAll(ctx)
}

// testHost is a session host of a test's. It refuses a session whose
// refuse: is true, names each session of a run no step called by its key
// and a number of its own, points a session where with: says, at a port
// it gives, refusing a with: whose remoteHost is refuse as a port in use,
// and records whether it checked a called workflow's sessions, what it is
// asked to point, and whether to serve it there, and what it is asked to
// serve.
type testHost struct {
	mu     sync.Mutex
	n      int
	called []bool
	keys   []string
	points []string
	served [][]Session
}

func (h *testHost) Check(_ context.Context, sessions map[string]any, called bool) (map[string]string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.called = append(h.called, called)
	for key, def := range sessions {
		if d, _ := def.(map[string]any); d["refuse"] == true {
			return nil, fmt.Errorf("the host refuses the session %s", key)
		}
	}
	if called {
		return nil, nil
	}
	names := map[string]string{}
	for _, key := range slices.Sorted(maps.Keys(sessions)) {
		h.n++
		names[key] = fmt.Sprintf("%s-%d", key, h.n)
	}
	return names, nil
}

func (h *testHost) Point(_ context.Context, s Session, u compile.SessionUpdate, serve bool) (SessionPoint, error) {
	if u.With["remoteHost"] == "refuse" {
		return SessionPoint{}, problems.SessionLocalPortTaken.Problem(problems.Params{"port": "1", "picked": "false"})
	}
	data, err := json.Marshal(u.With, json.Deterministic(true))
	if err != nil {
		return SessionPoint{}, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.points = append(h.points, fmt.Sprintf("%s %s %v %s serve=%t", s.Name, s.Key, s.Def, data, serve))
	h.keys = append(h.keys, u.SessionKey)
	return SessionPoint{Point: data, Port: 20000 + len(h.points)}, nil
}

func (h *testHost) Serve(_ context.Context, live []Session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.served = append(h.served, live)
}

// lastServed returns what the host was last asked to serve.
func (h *testHost) lastServed() []Session {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.served) == 0 {
		return nil
	}
	return h.served[len(h.served)-1]
}

// serveSessions has c serve its sessions until the test ends.
func serveSessions(t *testing.T, c *Coordinator) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	done := make(chan error, 1)
	go func() { done <- c.ServeSessions(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
}

// wantProblem fails the test unless err is a problem of kind k.
func wantProblem(t *testing.T, err error, k *problems.Kind) {
	t.Helper()
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != k.Code() {
		t.Errorf("err = %v, want %s", err, k.Code())
	}
}

const plainWorkflow = "jobs:\n  main:\n    steps:\n      - run: echo\n"

// TestSessionsMade checks a run's sessions, as a coordinator with a
// session host keeps them: each with the name Check gave it, the root of
// its tree as its owner, its type and redirect, its definition, a label
// and a salt of its own; made once, however often Make is asked; and,
// where a live session has the name already, refused as
// session_name_taken. Its host isn't asked to serve them while they point
// nowhere.
func TestSessionsMade(t *testing.T) {
	parallel(t)
	h := &testHost{}
	c := open(t, Options{SessionHost: h})
	serveSessions(t, c)
	p := c.opts.Sessions
	sessions := map[string]any{"desktop": map[string]any{"redirect": true}, "notes": map[string]any{"type": "link"}}
	names, err := p.Check(t.Context(), sessions)
	if err != nil {
		t.Fatal(err)
	}
	if h.called == nil || h.called[0] {
		t.Errorf("a run no step called had its sessions checked as a called workflow's: %v", h.called)
	}
	id, _ := create(t, c, plainWorkflow, nil, nil)
	for range 2 {
		if err := p.Make(t.Context(), id, sessions, names, expr.Read{}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := c.runSessions(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("the run's sessions are %+v", got)
	}
	byKey := map[string]Session{}
	for _, s := range got {
		byKey[s.Key] = s
	}
	desktop, notes := byKey["desktop"], byKey["notes"]
	switch {
	case desktop.Name != names["desktop"] || desktop.Root != id || desktop.Run != id || desktop.Type != "tunnel" || !desktop.Redirect ||
		desktop.Def["redirect"] != true || desktop.Point != nil || desktop.Private:
		t.Errorf("the desktop session is %+v", desktop)
	case notes.Type != "link" || notes.Redirect:
		t.Errorf("the notes session is %+v", notes)
	case !sessionproxy.ValidLabel(desktop.Label) || desktop.Label == notes.Label || len(desktop.Salt) != sessionproxy.SaltLen || string(desktop.Salt) == string(notes.Salt):
		t.Errorf("the sessions' labels and salts are %q %q, %x %x", desktop.Label, notes.Label, desktop.Salt, notes.Salt)
	}
	h.mu.Lock()
	before := len(h.served)
	h.mu.Unlock()
	c.wakeSessions()
	waitFor(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.served) > before
	})
	if live := h.lastServed(); len(live) != 0 {
		t.Errorf("the host was asked to serve %+v, which point nowhere", live)
	}

	other, _ := create(t, c, plainWorkflow, nil, nil)
	wantProblem(t, p.Make(t.Context(), other, map[string]any{"desktop": map[string]any{}}, map[string]string{"desktop": names["desktop"]}, expr.Read{}),
		problems.SessionNameTaken)
}

// TestSessionsUpdate checks a step that points a session, as the
// coordinator authorizes it, as format 1's platform does: a run may point
// a session its own tree made, a child of the tree among them; a run of
// another tree may not, unless its tree's permissions: hold '*'; a name no
// live session has is session_unknown, and none session_name_empty. The
// host reads where it points, which the store keeps, with the port the
// host serves it at, and a refusal of the host's fails the step. Each
// time, the host is asked to serve it so, since no other process does.
func TestSessionsUpdate(t *testing.T) {
	parallel(t)
	h := &testHost{}
	c := open(t, Options{SessionHost: h})
	serveSessions(t, c)
	p := c.opts.Sessions
	root, _ := create(t, c, plainWorkflow, nil, nil)
	other, _ := create(t, c, plainWorkflow, nil, nil)
	if err := p.Make(t.Context(), root, map[string]any{"desktop": map[string]any{"useTLS": false}}, map[string]string{"desktop": "desktop-x"}, expr.Read{}); err != nil {
		t.Fatal(err)
	}
	update := func(run, treeRoot string, permissions []string, name string, with map[string]any) error {
		return p.Update(t.Context(), compile.SessionUpdate{Run: run, Root: treeRoot, Permissions: permissions, Name: name, With: with})
	}
	// A run of the tree, as a child is, whose root is the session's.
	if err := update("child-run", root, nil, "desktop-x", map[string]any{"remoteHost": "node-1", "remotePort": float64(8888)}); err != nil {
		t.Fatal(err)
	}
	wantProblem(t, update(other, other, nil, "desktop-x", map[string]any{"remotePort": float64(1)}), problems.SessionNotPermitted)
	if err := update(other, other, []string{"*"}, "desktop-x", map[string]any{"remotePort": float64(9999), "localPort": float64(7000)}); err != nil {
		t.Errorf("a run whose tree's permissions: hold '*': %v", err)
	}
	wantProblem(t, update(root, root, nil, "nothing", nil), problems.SessionUnknown)
	wantProblem(t, update(root, root, nil, "", nil), problems.SessionNameEmpty)
	wantProblem(t, update(root, root, nil, "desktop-x", map[string]any{"remoteHost": "refuse"}), problems.SessionLocalPortTaken)

	h.mu.Lock()
	points := slices.Clone(h.points)
	h.mu.Unlock()
	want := []string{
		`desktop-x desktop map[useTLS:false] {"remoteHost":"node-1","remotePort":8888} serve=true`,
		`desktop-x desktop map[useTLS:false] {"localPort":7000,"remotePort":9999} serve=true`,
	}
	if !slices.Equal(points, want) {
		t.Errorf("the host pointed\n%s\nwant\n%s", strings.Join(points, "\n"), strings.Join(want, "\n"))
	}
	sessions, err := c.runSessions(t.Context(), root)
	if err != nil || len(sessions) != 1 {
		t.Fatal(sessions, err)
	}
	if s := sessions[0]; string(s.Point) != `{"localPort":7000,"remotePort":9999}` || s.Port != 20002 {
		t.Errorf("the session as the store keeps it: %+v", s)
	}
	waitFor(t, func() bool {
		live := h.lastServed()
		return len(live) == 1 && string(live[0].Point) == `{"localPort":7000,"remotePort":9999}`
	})
}

// TestSessionsPrivate checks that what a session is, or where it points,
// that read a variable, a password or a secret is kept sealed in the
// store, under its tree's root's data key, and given its host opened; that
// a session's data, once sealed, is sealed from then on; and that a
// coordinator opened only to read the store shows such a session as
// private, without what it is.
func TestSessionsPrivate(t *testing.T) {
	parallel(t)
	h := &testHost{}
	c := open(t, Options{SessionHost: h})
	p := c.opts.Sessions
	root, _ := create(t, c, plainWorkflow, nil, nil)
	sessions := map[string]any{
		"secret": map[string]any{"prompt-for-name": map[string]any{"default": "from-a-password"}},
		"plain":  map[string]any{"useTLS": false},
	}
	if err := p.Make(t.Context(), root, map[string]any{"secret": sessions["secret"]}, map[string]string{"secret": "secret-1"}, expr.Read{Private: true}); err != nil {
		t.Fatal(err)
	}
	// What reads a secret's mark, as a value a step made of one, is
	// private too, as what reads a password is.
	if err := p.Make(t.Context(), root, map[string]any{"marked": map[string]any{}}, map[string]string{"marked": "marked-1"}, expr.Read{Marked: true}); err != nil {
		t.Fatal(err)
	}
	if err := p.Make(t.Context(), root, map[string]any{"plain": sessions["plain"], "other": map[string]any{}}, map[string]string{"plain": "plain-1", "other": "other-1"}, expr.Read{}); err != nil {
		t.Fatal(err)
	}
	if err := p.Update(t.Context(), compile.SessionUpdate{Root: root, Name: "plain-1", With: map[string]any{"remoteHost": "from-a-secret"}, Read: expr.Read{Marked: true}}); err != nil {
		t.Fatal(err)
	}
	if err := p.Update(t.Context(), compile.SessionUpdate{Root: root, Name: "other-1", With: map[string]any{"remoteHost": "from-a-password"}, Read: expr.Read{Private: true}}); err != nil {
		t.Fatal(err)
	}
	stored := map[string]store.Session{}
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		ss, err := tx.Sessions(root)
		for _, s := range ss {
			stored[s.Key] = s
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for key, s := range stored {
		if s.Data != nil || s.Sealed == nil || strings.Contains(string(s.Sealed), "from-a") {
			t.Errorf("the %s session is kept %q, sealed %q", key, s.Data, s.Sealed)
		}
	}
	if len(stored) != 4 || stored["plain"].Generation != 1 || stored["other"].Generation != 1 {
		t.Errorf("the plain and other sessions, sealed once they pointed from a secret and a password, are %+v", stored)
	}
	opened, err := c.runSessions(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range opened {
		if !s.Private || s.Def == nil {
			t.Errorf("opened, the %s session is %+v", s.Key, s)
		}
	}
	if err := p.Update(t.Context(), compile.SessionUpdate{Root: root, Name: "plain-1", With: map[string]any{"remoteHost": "in-the-clear"}}); err != nil {
		t.Fatal(err)
	}
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		s, err := tx.Session(stored["plain"].ID)
		if err == nil && (s.Data != nil || s.Generation != 2) {
			t.Errorf("a session once sealed, pointed in the clear, is kept %q, of generation %d", s.Data, s.Generation)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}

	ro := open(t, Options{Database: c.opts.Database, ReadOnly: true})
	shown, err := ro.runSessions(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range shown {
		if !s.Private || s.Def != nil || s.Point != nil {
			t.Errorf("read only, the %s session shows %+v", s.Key, s)
		}
	}
}

// TestSessionCodesAndReset checks a session's one-time codes, as the
// coordinator keeps them: a code is used once, and not once the session is
// reset, which replaces its salt, which its host is asked to serve.
// Pointed by a step of this process's, which takes it to serve, the
// session is reset then too.
func TestSessionCodesAndReset(t *testing.T) {
	parallel(t)
	h := &testHost{}
	c := open(t, Options{SessionHost: h})
	serveSessions(t, c)
	root, _ := create(t, c, plainWorkflow, nil, nil)
	if err := c.opts.Sessions.Make(t.Context(), root, map[string]any{"desktop": map[string]any{}}, map[string]string{"desktop": "desktop-1"}, expr.Read{}); err != nil {
		t.Fatal(err)
	}
	made, err := c.runSessions(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.opts.Sessions.Update(t.Context(), compile.SessionUpdate{Root: root, Name: "desktop-1", With: map[string]any{"remotePort": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	sessions, err := c.runSessions(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	s := sessions[0]
	if string(s.Salt) == string(made[0].Salt) {
		t.Error("the session, taken to serve as a step pointed it, kept its salt")
	}
	later := time.Hour
	for _, mac := range []string{"one", "two"} {
		if err := c.AddSessionCode(t.Context(), s.ID, []byte(mac), later); err != nil {
			t.Fatal(err)
		}
	}
	if !c.RedeemSessionCode(t.Context(), s.ID, []byte("one")) || c.RedeemSessionCode(t.Context(), s.ID, []byte("one")) {
		t.Error("a code isn't used once")
	}
	if err := c.ResetSession(t.Context(), s.ID); err != nil {
		t.Fatal(err)
	}
	if c.RedeemSessionCode(t.Context(), s.ID, []byte("two")) {
		t.Error("a code outlived the session's reset")
	}
	waitFor(t, func() bool {
		live := h.lastServed()
		return len(live) == 1 && string(live[0].Salt) != string(s.Salt)
	})
}

// TestSessionsEndWithRun checks sessions end to end, as meridian's worker
// runs a run whose step points its session: the host reads where the step
// points it, and is asked to serve it, and to serve none once the run
// ends, as its tree's root, when the store keeps it as ended: at once, not
// at its process's next look at the store.
func TestSessionsEndWithRun(t *testing.T) {
	parallel(t)
	h := &testHost{}
	r := newWorkerRigOf(t, Options{SessionHost: h})
	r.c.sessionsPoll = time.Hour
	serveSessions(t, r.c)
	contexts := conformancetest.Variables{}.Contexts()
	contexts["sessions"] = map[string]any{"desktop": "desktop-run"}
	id := r.create(t, []byte(`
sessions:
  desktop:
    redirect: true
jobs:
  main:
    steps:
      - uses: parallelworks/update-session
        with:
          name: ${{ sessions.desktop }}
          remoteHost: node-1
          remotePort: 8888
`), nil, contexts)
	serve(t, r.c)
	if run := waitEnded(t, r.c, id); run.Outcome != plan.OutcomeSuccess {
		t.Fatalf("the run ended %s", run.Outcome)
	}
	h.mu.Lock()
	points := slices.Clone(h.points)
	served := slices.Clone(h.served)
	keys := slices.Clone(h.keys)
	h.mu.Unlock()
	if len(points) != 1 || !strings.HasPrefix(points[0], "desktop-run desktop ") {
		t.Errorf("the host pointed %q", points)
	}
	// The step names it by its key, as format 1 reads its name as written.
	if !slices.Equal(keys, []string{"desktop"}) {
		t.Errorf("the host was told the step names the session by %q", keys)
	}
	if !slices.ContainsFunc(served, func(live []Session) bool { return len(live) == 1 && live[0].Point != nil }) {
		t.Error("the host was never asked to serve the session where the step pointed it")
	}
	waitFor(t, func() bool { return h.lastServed() != nil && len(h.lastServed()) == 0 })
	sessions, err := r.c.runSessions(t.Context(), id)
	if err != nil || len(sessions) != 1 || sessions[0].EndedAt.IsZero() || sessions[0].Point != nil {
		t.Fatalf("once the run ended, its sessions are %+v, %v", sessions, err)
	}
	// Its hold is let go of, and its file gone.
	hold := r.c.opts.Database + ".session-" + sessions[0].ID + ".lock"
	waitFor(t, func() bool { _, err := os.Lstat(hold); return errors.Is(err, fs.ErrNotExist) })
}

// TestSessionHoldLeft checks that the file of the hold of a session
// whose process ended while it served it, which no process took over,
// goes as the session ends.
func TestSessionHoldLeft(t *testing.T) {
	parallel(t)
	h := &testHost{}
	c := open(t, Options{SessionHost: h})
	serveSessions(t, c)
	root, _ := create(t, c, plainWorkflow, nil, nil)
	if err := c.opts.Sessions.Make(t.Context(), root, map[string]any{"desktop": map[string]any{}}, map[string]string{"desktop": "desktop-1"}, expr.Read{}); err != nil {
		t.Fatal(err)
	}
	made, err := c.runSessions(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	// The file another process's hold left, as it ended.
	hold := c.opts.Database + ".session-" + made[0].ID + ".lock"
	if err := os.WriteFile(hold, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// Two looks at the store, the second after one that saw the session.
	h.mu.Lock()
	looks := len(h.served)
	h.mu.Unlock()
	waitFor(t, func() bool { h.mu.Lock(); defer h.mu.Unlock(); return len(h.served) > looks+1 })
	if _, err := os.Lstat(hold); err != nil {
		t.Fatalf("the hold's file of a session that hasn't ended is gone: %v", err)
	}
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error { return tx.EndRun(root, plan.OutcomeSuccess, "") }); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, err := os.Lstat(hold); return errors.Is(err, fs.ErrNotExist) })
}

// TestServeSessionsNeedsHost checks that a coordinator without a session
// host, or opened only to read the store, serves no sessions, and the
// latter makes and points none; and that one serves them once at a time.
func TestServeSessionsNeedsHost(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	if err := c.ServeSessions(t.Context()); err == nil {
		t.Error("ServeSessions without a host served")
	}
	ro := open(t, Options{Database: c.opts.Database, ReadOnly: true, SessionHost: &testHost{}})
	if err := ro.ServeSessions(t.Context()); !errors.Is(err, ErrReadOnly) {
		t.Errorf("ServeSessions read only = %v", err)
	}
	if ro.opts.Sessions != nil {
		t.Error("a coordinator opened only to read the store provides Sessions")
	}
	// One process serves its sessions once: a second ServeSessions would
	// take its holds from under the first.
	served := open(t, Options{SessionHost: &testHost{}})
	serveSessions(t, served)
	waitFor(t, func() bool {
		served.sessionsMu.Lock()
		defer served.sessionsMu.Unlock()
		return served.sessionHolds != nil
	})
	// Without the guard, it would serve until its context ends.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := served.ServeSessions(ctx); err == nil {
		t.Error("ServeSessions served twice at once")
	}
}

// TestSessionsTakenOver checks the sessions that each of a store's
// processes serves: each serves those it took, as its step pointed them,
// and none that another process that lives serves, which keep their salts
// and their codes, as another's step points one too; and a session whose
// process stopped serving it, as it ended, another takes over, by its next
// look at the store, with a new salt, and so a new secret: a cookie that
// another program took while no process served the session, as a page of
// it that reconnected sent to whatever listened at its port, doesn't work
// once it is served again; and its codes are gone with it.
func TestSessionsTakenOver(t *testing.T) {
	parallel(t)
	hb, hc, ha := &testHost{}, &testHost{}, &testHost{}
	b := open(t, Options{SessionHost: hb})
	c := open(t, Options{Database: b.opts.Database, Keys: b.opts.Keys, SessionHost: hc})
	a := open(t, Options{Database: b.opts.Database, Keys: b.opts.Keys, SessionHost: ha})
	root, _ := create(t, b, plainWorkflow, nil, nil)
	sessions := map[string]any{"one": map[string]any{}, "two": map[string]any{}}
	if err := b.opts.Sessions.Make(t.Context(), root, sessions, map[string]string{"one": "one-1", "two": "two-1"}, expr.Read{}); err != nil {
		t.Fatal(err)
	}
	serving := func(c *Coordinator) (stop func()) {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- c.ServeSessions(ctx) }()
		return func() {
			cancel()
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	}
	point := func(c *Coordinator, name string, port float64) {
		t.Helper()
		if err := c.opts.Sessions.Update(t.Context(), compile.SessionUpdate{Root: root, Name: name, With: map[string]any{"remotePort": port}}); err != nil {
			t.Fatal(err)
		}
	}
	// c looks at the store only when the test says, so that it is a that
	// takes over from b.
	c.sessionsPoll = time.Hour
	stopB, stopC := serving(b), serving(c)
	defer stopC()
	point(b, "one-1", 1)
	point(c, "two-1", 2)
	byName := func() map[string]Session {
		t.Helper()
		got, err := a.runSessions(t.Context(), root)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]Session{}
		for _, s := range got {
			out[s.Name] = s
		}
		return out
	}
	was := byName()
	for _, s := range was {
		if err := a.AddSessionCode(t.Context(), s.ID, []byte("code"), time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	served := func(h *testHost, name string) []byte {
		live := h.lastServed()
		for _, s := range live {
			if s.Name == name {
				return s.Salt
			}
		}
		return nil
	}
	waitFor(t, func() bool { return served(hb, "one-1") != nil && served(hc, "two-1") != nil })
	if serving, err := a.SessionServing(t.Context(), was["one-1"].ID); err != nil || !serving {
		t.Errorf("a sees the session b serves as served: %t, %v", serving, err)
	}

	stopA := serving(a)
	defer stopA()
	// Another's step points a session c serves: a's host serves nothing.
	point(a, "two-1", 3)
	ha.mu.Lock()
	points := slices.Clone(ha.points)
	ha.mu.Unlock()
	if len(points) != 1 || !strings.HasSuffix(points[0], `{"remotePort":3} serve=false`) {
		t.Errorf("a's host pointed %q", points)
	}
	// Two looks of a's at the store, with b and c alive: it takes neither.
	for range 2 {
		ha.mu.Lock()
		before := len(ha.served)
		ha.mu.Unlock()
		waitFor(t, func() bool {
			ha.mu.Lock()
			defer ha.mu.Unlock()
			return len(ha.served) > before+1
		})
	}
	if live := ha.lastServed(); len(live) != 0 {
		t.Errorf("a serves %+v, which b and c serve", live)
	}
	// c, at its next look, serves two where a's step pointed it, with its
	// salt.
	c.wakeSessions()
	waitFor(t, func() bool {
		for _, s := range hc.lastServed() {
			if s.Name == "two-1" {
				return string(s.Salt) == string(was["two-1"].Salt) && string(s.Point) == `{"remotePort":3}`
			}
		}
		return false
	})

	stopB()
	if live := hb.lastServed(); len(live) != 0 {
		t.Errorf("b, stopped, was last asked to serve %+v", live)
	}
	waitFor(t, func() bool { return served(ha, "one-1") != nil })
	now := byName()
	if salt := served(ha, "one-1"); string(salt) == string(was["one-1"].Salt) || string(salt) != string(now["one-1"].Salt) {
		t.Errorf("one, taken over, is served with the salt %x, as made %x, as the store has it %x", salt, was["one-1"].Salt, now["one-1"].Salt)
	}
	if a.RedeemSessionCode(t.Context(), was["one-1"].ID, []byte("code")) {
		t.Error("a code of one's from before a took it over was used")
	}
	if string(now["two-1"].Salt) != string(was["two-1"].Salt) || served(ha, "two-1") != nil {
		t.Errorf("two, which c still serves, has the salt %x, made with %x, and a serves it %t", now["two-1"].Salt, was["two-1"].Salt, served(ha, "two-1") != nil)
	}
	if !a.RedeemSessionCode(t.Context(), was["two-1"].ID, []byte("code")) {
		t.Error("two's code, which c still serves, was gone")
	}
}

// TestSessionsOfAChild checks the sessions a child run's sessions: make,
// as a step's try that calls a workflow makes them: the root of the tree
// owns them, whose end ends them, and those private are sealed under the
// root's data key, which opens them.
func TestSessionsOfAChild(t *testing.T) {
	parallel(t)
	vars := &closingVariables{Variables: conformancetest.Variables{"vars.CHILD_VAR": {Value: "from-the-root"}}}
	c := open(t, Options{Variables: vars, SessionHost: &testHost{}})
	buildGo := make(chan struct{})
	defer close(buildGo)
	byJob(c, map[string]testEnd{"build": {wait: buildGo, result: plan.JobResult{Outcome: plan.OutcomeSuccess}}})
	root, _ := createTree(t, c, vars)
	serve(t, c)
	var attempt *store.Attempt
	waitFor(t, func() bool {
		return c.store.View(t.Context(), func(tx *store.Tx) error {
			var err error
			attempt, err = tx.LatestAttempt(root, "build")
			return err
		}) == nil
	})
	child, diags, err := askChild(t, c, attempt.ID, workerapi.ChildRequest{
		Step: 0, Try: 0, With: plan.DataOf(map[string]any{"$yaml": "w.yaml", "region": "east"}),
	})
	if err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	if err := c.opts.Sessions.Make(t.Context(), child, map[string]any{"desktop": map[string]any{"redirect": true}}, map[string]string{"desktop": "child-desktop"}, expr.Read{Private: true}); err != nil {
		t.Fatal(err)
	}
	got, err := c.runSessions(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Root != root || got[0].Run != child || got[0].Key != "desktop" || !got[0].Private || got[0].Def["redirect"] != true {
		t.Errorf("the root's sessions are %+v", got)
	}
}

// TestSessionsWake checks that ServeSessions has its host serve what
// changed in this process as it changes, without waiting for its next
// look at the store: a session a step points, and one reset; and that it
// looks at the store every so often, for what another process changed, as
// a reset another process made.
func TestSessionsWake(t *testing.T) {
	parallel(t)
	h := &testHost{}
	c := open(t, Options{SessionHost: h})
	c.sessionsPoll = time.Hour
	serveSessions(t, c)
	root, _ := create(t, c, plainWorkflow, nil, nil)
	if err := c.opts.Sessions.Make(t.Context(), root, map[string]any{"desktop": map[string]any{}}, map[string]string{"desktop": "desktop-1"}, expr.Read{}); err != nil {
		t.Fatal(err)
	}
	if err := c.opts.Sessions.Update(t.Context(), compile.SessionUpdate{Root: root, Name: "desktop-1", With: map[string]any{"remotePort": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { live := h.lastServed(); return len(live) == 1 && live[0].Point != nil })
	salt := h.lastServed()[0].Salt
	if err := c.ResetSession(t.Context(), h.lastServed()[0].ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { live := h.lastServed(); return len(live) == 1 && string(live[0].Salt) != string(salt) })

	// Another process's reset: only a look at the store finds it.
	other := open(t, Options{Database: c.opts.Database, Keys: c.opts.Keys, SessionHost: &testHost{}})
	h2 := &testHost{}
	c2 := open(t, Options{Database: c.opts.Database, Keys: c.opts.Keys, SessionHost: h2})
	serveSessions(t, c2)
	root2, _ := create(t, c2, plainWorkflow, nil, nil)
	if err := c2.opts.Sessions.Make(t.Context(), root2, map[string]any{"desktop": map[string]any{}}, map[string]string{"desktop": "desktop-2"}, expr.Read{}); err != nil {
		t.Fatal(err)
	}
	if err := c2.opts.Sessions.Update(t.Context(), compile.SessionUpdate{Root: root2, Name: "desktop-2", With: map[string]any{"remotePort": float64(2)}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { live := h2.lastServed(); return len(live) == 1 && live[0].Point != nil })
	salt2 := h2.lastServed()[0].Salt
	if err := other.ResetSession(t.Context(), h2.lastServed()[0].ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { live := h2.lastServed(); return len(live) == 1 && string(live[0].Salt) != string(salt2) })
}

// TestSessionsOfAnEndedRun checks that a run that has ended makes no
// sessions, as one started again after it ended would.
func TestSessionsOfAnEndedRun(t *testing.T) {
	parallel(t)
	c := open(t, Options{SessionHost: &testHost{}})
	root, _ := create(t, c, plainWorkflow, nil, nil)
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error { return tx.EndRun(root, plan.OutcomeCancelled, "") }); err != nil {
		t.Fatal(err)
	}
	if err := c.opts.Sessions.Make(t.Context(), root, map[string]any{"desktop": map[string]any{}}, map[string]string{"desktop": "desktop-1"}, expr.Read{}); !errors.Is(err, store.ErrState) {
		t.Errorf("Make for an ended run = %v, want ErrState", err)
	}
	if got, err := c.runSessions(t.Context(), root); err != nil || len(got) != 0 {
		t.Errorf("the ended run's sessions are %+v, %v", got, err)
	}
}

// TestSessionsMadeTogether checks that a run's sessions are made together,
// or not at all: one whose name another session has makes none of them.
func TestSessionsMadeTogether(t *testing.T) {
	parallel(t)
	c := open(t, Options{SessionHost: &testHost{}})
	first, _ := create(t, c, plainWorkflow, nil, nil)
	if err := c.opts.Sessions.Make(t.Context(), first, map[string]any{"b": map[string]any{}}, map[string]string{"b": "taken"}, expr.Read{}); err != nil {
		t.Fatal(err)
	}
	root, _ := create(t, c, plainWorkflow, nil, nil)
	// a is made before b, whose name is taken.
	wantProblem(t, c.opts.Sessions.Make(t.Context(), root, map[string]any{"a": map[string]any{}, "b": map[string]any{}}, map[string]string{"a": "free", "b": "taken"}, expr.Read{}),
		problems.SessionNameTaken)
	if got, err := c.runSessions(t.Context(), root); err != nil || len(got) != 0 {
		t.Errorf("the run's sessions, once one's name was taken, are %+v, %v", got, err)
	}
}

// TestSessionsOpenedOnceAGeneration checks that ServeSessions opens a
// sealed session once for each generation it is sealed as, and serves it
// as that opened to on the looks after, until a step points it again,
// sealed as a newer one; and forgets it once it has ended.
func TestSessionsOpenedOnceAGeneration(t *testing.T) {
	parallel(t)
	h := &testHost{}
	c := open(t, Options{SessionHost: h})
	p := c.opts.Sessions
	root, _ := create(t, c, plainWorkflow, nil, nil)
	if err := p.Make(t.Context(), root, map[string]any{"secret": map[string]any{}}, map[string]string{"secret": "secret-1"}, expr.Read{Private: true}); err != nil {
		t.Fatal(err)
	}
	serveSessions(t, c)
	point := func(port float64) {
		t.Helper()
		if err := p.Update(t.Context(), compile.SessionUpdate{Root: root, Name: "secret-1", With: map[string]any{"remotePort": port}}); err != nil {
			t.Fatal(err)
		}
	}
	opened := func() (uint64, int) {
		c.sessionsMu.Lock()
		defer c.sessionsMu.Unlock()
		for _, o := range c.sessionsOpened {
			return o.generation, len(c.sessionsOpened)
		}
		return 0, 0
	}
	servedAt := func(port string) bool {
		live := h.lastServed()
		return len(live) == 1 && strings.Contains(string(live[0].Point), `"remotePort":`+port)
	}
	// Made sealed as generation 1, and pointed as 2.
	point(1)
	waitFor(t, func() bool { gen, n := opened(); return gen == 2 && n == 1 && servedAt("1") })
	served := len(h.served)
	waitFor(t, func() bool { h.mu.Lock(); defer h.mu.Unlock(); return len(h.served) > served+2 })
	if !servedAt("1") {
		t.Errorf("served from what it opened, the session is %+v", h.lastServed())
	}
	point(2)
	waitFor(t, func() bool { gen, n := opened(); return gen == 3 && n == 1 && servedAt("2") })

	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error { return tx.EndRun(root, plan.OutcomeSuccess, "") }); err != nil {
		t.Fatal(err)
	}
	c.wakeSessions()
	waitFor(t, func() bool { _, n := opened(); return n == 0 && h.lastServed() != nil && len(h.lastServed()) == 0 })
}

// TestSessionsServedPastOneUnopened checks that a session its process
// can't open, as one whose sealed data is damaged, keeps none of the
// others from being served: its host serves the others, as they change,
// and stops serving it.
func TestSessionsServedPastOneUnopened(t *testing.T) {
	parallel(t)
	h := &testHost{}
	c := open(t, Options{SessionHost: h})
	serveSessions(t, c)
	root, _ := create(t, c, plainWorkflow, nil, nil)
	if err := c.opts.Sessions.Make(t.Context(), root, map[string]any{"good": map[string]any{}, "bad": map[string]any{}}, map[string]string{"good": "good-1", "bad": "bad-1"}, expr.Read{Private: true}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"good-1", "bad-1"} {
		if err := c.opts.Sessions.Update(t.Context(), compile.SessionUpdate{Root: root, Name: name, With: map[string]any{"remotePort": float64(1)}}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return len(h.lastServed()) == 2 })
	byName := map[string]Session{}
	for _, s := range h.lastServed() {
		byName[s.Name] = s
	}
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		bad, err := tx.Session(byName["bad-1"].ID)
		if err != nil {
			return err
		}
		return tx.PointSession(bad.ID, nil, []byte("damaged"), bad.Generation+1, bad.Port)
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.ResetSession(t.Context(), byName["good-1"].ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		live := h.lastServed()
		return len(live) == 1 && live[0].Name == "good-1" && string(live[0].Salt) != string(byName["good-1"].Salt)
	})
}

// TestSessionServed checks what a session's host records of how it serves
// it: the port it serves it at, or why it can't, which leaves the port as
// it was, and which any process shows, until a step points the session
// again.
func TestSessionServed(t *testing.T) {
	parallel(t)
	c := open(t, Options{SessionHost: &testHost{}})
	root, _ := create(t, c, plainWorkflow, nil, nil)
	if err := c.opts.Sessions.Make(t.Context(), root, map[string]any{"desktop": map[string]any{}}, map[string]string{"desktop": "desktop-1"}, expr.Read{}); err != nil {
		t.Fatal(err)
	}
	update := func() {
		t.Helper()
		if err := c.opts.Sessions.Update(t.Context(), compile.SessionUpdate{Root: root, Name: "desktop-1", With: map[string]any{"remotePort": float64(1)}}); err != nil {
			t.Fatal(err)
		}
	}
	update()
	ro := open(t, Options{Database: c.opts.Database, ReadOnly: true})
	shown := func() Session {
		t.Helper()
		got, err := ro.runSessions(t.Context(), root)
		if err != nil || len(got) != 1 {
			t.Fatal(got, err)
		}
		return got[0]
	}
	id := shown().ID
	if err := c.SessionServed(t.Context(), id, 31000, nil); err != nil {
		t.Fatal(err)
	}
	if s := shown(); s.Port != 31000 || s.Problem != nil {
		t.Errorf("served at another port, the session shows %+v", s)
	}
	taken := problems.SessionLocalPortTaken.Problem(problems.Params{"port": "31000", "picked": "false"})
	if err := c.SessionServed(t.Context(), id, 0, taken); err != nil {
		t.Fatal(err)
	}
	if s := shown(); s.Port != 31000 || s.Problem == nil || s.Problem.Code != problems.SessionLocalPortTaken.Code() {
		t.Errorf("unserved, the session shows %+v", s)
	}
	update()
	if s := shown(); s.Problem != nil {
		t.Errorf("pointed again, the session shows the problem %+v", s.Problem)
	}
	if err := ro.SessionServed(t.Context(), id, 1, nil); !errors.Is(err, ErrReadOnly) {
		t.Errorf("SessionServed read only = %v", err)
	}
}

// callSources give a workflow with sessions that callRoot's steps call,
// one of which asks for its name, which a called workflow's sessions
// don't; and which calls another with a session of its own.
var callSources = childSources(map[string]string{
	"github/t/sess@main": `sessions:
  desktop: {}
  notes:
    prompt-for-name: {default: fixed}
jobs:
  main:
    steps:
      - run: echo
      - uses: github/t/leaf@main
        with: {$yaml: w.yaml}
`,
	"github/t/leaf@main": `sessions:
  leaf: {}
jobs:
  main:
    steps:
      - run: echo
`,
})

// callRoot calls callSources' workflow from two steps of build, whose
// first is tried again, and which is attempted again once its worker is
// lost; and from fan, a matrix job of two instances.
const callRoot = `jobs:
  build:
    retry: {reasons: [worker-lost], max-retries: 1, interval: 0s}
    steps:
      - uses: github/t/sess@main
        with: {$yaml: w.yaml}
        retry: {max-retries: 3}
      - uses: github/t/sess@main
        with: {$yaml: w.yaml}
  fan:
    strategy:
      matrix: {n: [1, 2]}
    steps:
      - uses: github/t/sess@main
        with: {$yaml: w.yaml}
`

// TestSessionsOfCalls checks the sessions of workflows that steps call, as
// format 1 makes them, once for each call in the tree, as a run is
// submitted: every try of a call, every attempt of its job, as one after a
// lost worker, and every instance of a matrix job, shares them, and each
// call has its own, a call of a called workflow among them; each is named
// by where it is called, whatever its prompt-for-name says, once its host
// has checked it.
func TestSessionsOfCalls(t *testing.T) {
	parallel(t)
	c := open(t, Options{SessionHost: &testHost{}})
	driverOf(c).set(func(d *testDriver) {
		d.worker = func(backend.PlaceRequest) testEnd { return testEnd{running: true} }
	})
	created, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(callRoot), nil,
		compile.Options{Source: plan.Source{Location: "flows/workflow.yaml"}, Sources: callSources})
	if err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	root, err := c.CreateRun(t.Context(), created, runOptions)
	if err != nil {
		t.Fatal(err)
	}
	serve(t, c)
	claimedOf := func(run, key string) *store.Attempt {
		t.Helper()
		var a *store.Attempt
		waitFor(t, func() bool {
			var err error
			a, err = latestAttemptOf(t, c, run, key)
			return err == nil && a.State == store.AttemptRunning
		})
		return a
	}
	claimed := func(key string) *store.Attempt { return claimedOf(root, key) }
	with := plan.DataOf(map[string]any{"$yaml": "w.yaml"})
	call := func(a *store.Attempt, step, try int) string {
		t.Helper()
		id, diags, err := askChild(t, c, a.ID, workerapi.ChildRequest{Step: step, Try: try, With: with})
		if err != nil || diags.HasErrors() {
			t.Fatal(diags, err)
		}
		return id
	}
	build := claimed("build")
	children := map[string]string{
		"first":    call(build, 0, 0),
		"retried":  call(build, 0, 1),
		"second":   call(build, 1, 0),
		"instance": call(claimed("fan#0"), 0, 0),
		"other":    call(claimed("fan#1"), 0, 0),
	}
	children["leaf"] = call(claimedOf(children["first"], "main"), 1, 0)
	children["other leaf"] = call(claimedOf(children["second"], "main"), 1, 0)
	started := func() {
		t.Helper()
		// Each child makes its sessions as it starts: those its call
		// made already, it makes no more.
		waitFor(t, func() bool {
			for _, id := range children {
				if storedRun(t, c, id).StartState == nil {
					return false
				}
			}
			return true
		})
	}
	started()
	// The attempt is lost, and the job, attempted again, calls again.
	if err := c.lose(t.Context(), build, backend.Status{}, nil); err != nil && !errors.Is(err, errAgain) {
		t.Fatal(err)
	}
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error { return c.wakeWorkerJob(t.Context(), tx, root, "build") }); err != nil {
		t.Fatal(err)
	}
	var again *store.Attempt
	waitFor(t, func() bool {
		a, err := latestAttemptOf(t, c, root, "build")
		again = a
		return err == nil && a.ID != build.ID && a.State == store.AttemptRunning
	})
	children["attempted"] = call(again, 0, 0)
	started()

	names := map[string]map[string]string{}
	for which, id := range children {
		p, err := c.plans.get(storedRun(t, c, id))
		if err != nil {
			t.Fatal(err)
		}
		names[which] = p.Run.SessionNames
	}
	prefix := "workflow_" + root + "_"
	want := map[string]map[string]string{
		"first":  {"desktop": prefix + "build_step_0_desktop", "notes": prefix + "build_step_0_notes"},
		"second": {"desktop": prefix + "build_step_1_desktop", "notes": prefix + "build_step_1_notes"},
		"fan":    {"desktop": prefix + "fan_step_0_desktop", "notes": prefix + "fan_step_0_notes"},
		"leaf":   {"leaf": prefix + "build_step_0_main_step_1_leaf"},
		"leaf 2": {"leaf": prefix + "build_step_1_main_step_1_leaf"},
	}
	for which, call := range map[string]string{
		"first": "first", "retried": "first", "attempted": "first", "second": "second", "instance": "fan", "other": "fan", "leaf": "leaf", "other leaf": "leaf 2",
	} {
		if !maps.Equal(names[which], want[call]) {
			t.Errorf("the %s child's sessions are named %v, want %v", which, names[which], want[call])
		}
	}
	got, err := c.runSessions(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	var made []string
	for _, s := range got {
		made = append(made, s.Name)
	}
	slices.Sort(made)
	var all []string
	for _, n := range want {
		all = append(all, slices.Collect(maps.Values(n))...)
	}
	slices.Sort(all)
	if !slices.Equal(made, all) {
		t.Errorf("the tree's sessions are %q, want %q", made, all)
	}
	// Any run of a call finds the sessions it shares.
	for which, call := range map[string]string{"retried": "first", "attempted": "first", "other": "fan", "other leaf": "leaf 2"} {
		shared, err := c.CallSessions(t.Context(), children[which])
		if err != nil {
			t.Fatal(err)
		}
		found := map[string]string{}
		for _, s := range shared {
			found[s.Key] = s.Name
		}
		if !maps.Equal(found, want[call]) {
			t.Errorf("the %s child's call's sessions are %v, want %v", which, found, want[call])
		}
	}
	if own, err := c.CallSessions(t.Context(), root); err != nil || len(own) != 0 {
		t.Errorf("the root's own sessions are %+v, %v", own, err)
	}
	if _, err := c.CallSessions(t.Context(), "999"); !errors.Is(err, ErrRunNotFound) {
		t.Errorf("CallSessions of a run the store hasn't = %v", err)
	}
}

// TestSessionsPointedTogether checks steps of two processes that point one
// session at once, as instances of a matrix job that share it do: each
// step succeeds, the last to point it is where it points, and once one
// read a secret's mark the session is sealed from then on, each later
// point at the next generation, whichever process read it first.
func TestSessionsPointedTogether(t *testing.T) {
	parallel(t)
	a := open(t, Options{SessionHost: &testHost{}})
	b := open(t, Options{Database: a.opts.Database, Keys: a.opts.Keys, SessionHost: &testHost{}})
	root, _ := create(t, a, plainWorkflow, nil, nil)
	if err := a.opts.Sessions.Make(t.Context(), root, map[string]any{"desktop": map[string]any{}}, map[string]string{"desktop": "shared"}, expr.Read{}); err != nil {
		t.Fatal(err)
	}
	const steps = 8
	errs := make(chan error, steps)
	var wg sync.WaitGroup
	for i := range steps {
		c := []*Coordinator{a, b}[i%2]
		wg.Go(func() {
			errs <- c.opts.Sessions.Update(t.Context(), compile.SessionUpdate{
				Root: root, Name: "shared", With: map[string]any{"remotePort": float64(i + 1)}, Read: expr.Read{Marked: i == 1},
			})
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	var stored *store.Session
	if err := a.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		stored, err = tx.SessionNamed("shared")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if stored.Sealed == nil || stored.Data != nil || stored.Generation < 1 || stored.Generation > steps {
		t.Errorf("pointed together, the session is kept %q, sealed %t, of generation %d", stored.Data, stored.Sealed != nil, stored.Generation)
	}
	got, err := b.runSessions(t.Context(), root)
	if err != nil || len(got) != 1 || !got[0].Private || !strings.HasPrefix(string(got[0].Point), `{"remotePort":`) {
		t.Errorf("pointed together, the session opens as %+v, %v", got, err)
	}
}

// TestSessionsOfACallChecked checks that the sessions of a workflow a step
// calls, which the coordinator names, its host checks all the same.
func TestSessionsOfACallChecked(t *testing.T) {
	parallel(t)
	h := &testHost{}
	c := open(t, Options{SessionHost: h})
	p := c.opts.Sessions.(sessionProvider)
	p.call = &sessionCall{workflow: "w", root: "1", path: []callLevel{{Job: "build", Step: 0}}}
	if _, err := p.Check(t.Context(), map[string]any{"desktop": map[string]any{"refuse": true}}); err == nil {
		t.Error("a called workflow's session the host refuses was named")
	}
	names, err := p.Check(t.Context(), map[string]any{"desktop": map[string]any{}})
	if err != nil || names["desktop"] != "w_1_build_step_0_desktop" {
		t.Errorf("Check = %v, %v", names, err)
	}
	if !slices.Equal(h.called, []bool{true, true}) {
		t.Errorf("the host checked a called workflow's sessions as %v", h.called)
	}
}

// TestSessionNames checks a called workflow's sessions' names, as format 1
// makes them, of what a session's path holds as it is.
func TestSessionNames(t *testing.T) {
	parallel(t)
	for _, tc := range []struct{ location, want string }{
		{"workflow.yaml", "workflow"},
		{"flows/train.yml", "train"},
		{"github.com/o/r@main:flows/a.yaml", "a"},
		{`C:\flows\b.yaml`, "b"},
		{"<stdin>", "stdin"},
		{"", "workflow"},
	} {
		if got := workflowName(tc.location); got != tc.want {
			t.Errorf("workflowName(%q) = %q, want %q", tc.location, got, tc.want)
		}
	}
	path := []callLevel{{Job: "build", Step: 2}, {Job: "main job", Step: 0, Cleanup: true}}
	if got := sessionName("my flow", "12", path, "desk/top"); got != "my-flow_12_build_step_2_main-job_step_0_cleanup_desk-top" {
		t.Errorf("sessionName = %q", got)
	}
	// A deep tree's, too long for a cookie's path, is made short, the same
	// each time, and another call's another.
	deep := slices.Repeat([]callLevel{{Job: strings.Repeat("j", 30), Step: 1}}, 5)
	long, again := sessionName("w", "1", deep, "desktop"), sessionName("w", "1", deep, "desktop")
	other := sessionName("w", "1", deep, "notes")
	if len(long) != maxSessionName || long != again || other == long || !strings.HasPrefix(long, "w_1_"+strings.Repeat("j", 30)+"_step_1_") ||
		!regexp.MustCompile(`-[0-9a-f]{16}$`).MatchString(long) {
		t.Errorf("a deep tree's session is named %q, and %q, and another %q", long, again, other)
	}
	full := "w_1_" + strings.Repeat(strings.Repeat("j", 30)+"_step_1_", 5) + "desktop"
	sum := sha256.Sum256([]byte(full))
	if want := full[:maxSessionName-17] + "-" + hex.EncodeToString(sum[:8]); long != want {
		t.Errorf("a deep tree's session is named %q, want %q", long, want)
	}
}
