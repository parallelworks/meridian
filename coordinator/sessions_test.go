package coordinator

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/internal/sessionproxy"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// testHost is a session host of a test's. It names each session by its key
// and a number of its own, points a session where with: says, at a port
// it gives, refusing a with: whose remoteHost is refuse as a port in use,
// and records what it is asked to serve.
type testHost struct {
	mu     sync.Mutex
	n      int
	points []string
	served [][]Session
}

func (h *testHost) Check(_ context.Context, sessions map[string]any) (map[string]string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	names := map[string]string{}
	for _, key := range slices.Sorted(maps.Keys(sessions)) {
		h.n++
		names[key] = fmt.Sprintf("%s-%d", key, h.n)
	}
	return names, nil
}

func (h *testHost) Point(_ context.Context, s Session, u compile.SessionUpdate) (SessionPoint, error) {
	if u.With["remoteHost"] == "refuse" {
		return SessionPoint{}, problems.SessionLocalPortTaken.Problem(problems.Params{"port": "1"})
	}
	data, err := json.Marshal(u.With, json.Deterministic(true))
	if err != nil {
		return SessionPoint{}, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.points = append(h.points, fmt.Sprintf("%s %s %v %s", s.Name, s.Key, s.Def, data))
	return SessionPoint{Point: data, Port: 20000 + len(h.points), PortGiven: u.With["localPort"] != nil}, nil
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
// session_name_taken. Its host is asked to serve them, without being
// asked, as soon as they are made.
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
	id, _ := create(t, c, plainWorkflow, nil, nil)
	for range 2 {
		if err := p.Make(t.Context(), id, sessions, names, expr.Read{}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := c.RunSessions(t.Context(), id)
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
	waitFor(t, func() bool { return len(h.lastServed()) == 2 })

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
// time, the host is asked to serve it so.
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
		`desktop-x desktop map[useTLS:false] {"remoteHost":"node-1","remotePort":8888}`,
		`desktop-x desktop map[useTLS:false] {"localPort":7000,"remotePort":9999}`,
	}
	if !slices.Equal(points, want) {
		t.Errorf("the host pointed\n%s\nwant\n%s", strings.Join(points, "\n"), strings.Join(want, "\n"))
	}
	sessions, err := c.RunSessions(t.Context(), root)
	if err != nil || len(sessions) != 1 {
		t.Fatal(sessions, err)
	}
	if s := sessions[0]; string(s.Point) != `{"localPort":7000,"remotePort":9999}` || s.Port != 20002 || !s.PortGiven {
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
	if err := p.Make(t.Context(), root, map[string]any{"plain": sessions["plain"]}, map[string]string{"plain": "plain-1"}, expr.Read{}); err != nil {
		t.Fatal(err)
	}
	if err := p.Update(t.Context(), compile.SessionUpdate{Root: root, Name: "plain-1", With: map[string]any{"remoteHost": "from-a-secret"}, Read: expr.Read{Marked: true}}); err != nil {
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
	if stored["plain"].Generation != 1 {
		t.Errorf("the plain session, sealed once it pointed from a secret, is of generation %d", stored["plain"].Generation)
	}
	opened, err := c.RunSessions(t.Context(), root)
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
	shown, err := ro.RunSessions(t.Context(), root)
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
func TestSessionCodesAndReset(t *testing.T) {
	parallel(t)
	h := &testHost{}
	c := open(t, Options{SessionHost: h})
	serveSessions(t, c)
	root, _ := create(t, c, plainWorkflow, nil, nil)
	if err := c.opts.Sessions.Make(t.Context(), root, map[string]any{"desktop": map[string]any{}}, map[string]string{"desktop": "desktop-1"}, expr.Read{}); err != nil {
		t.Fatal(err)
	}
	sessions, err := c.RunSessions(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	s := sessions[0]
	later := time.Now().Add(time.Hour)
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
// ends, as its tree's root, when the store keeps it as ended.
func TestSessionsEndWithRun(t *testing.T) {
	parallel(t)
	h := &testHost{}
	r := newWorkerRigOf(t, Options{SessionHost: h})
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
	h.mu.Unlock()
	if len(points) != 1 || !strings.HasPrefix(points[0], "desktop-run desktop ") {
		t.Errorf("the host pointed %q", points)
	}
	if !slices.ContainsFunc(served, func(live []Session) bool { return len(live) == 1 && live[0].Point != nil }) {
		t.Error("the host was never asked to serve the session where the step pointed it")
	}
	waitFor(t, func() bool { return h.lastServed() != nil && len(h.lastServed()) == 0 })
	sessions, err := r.c.RunSessions(t.Context(), id)
	if err != nil || len(sessions) != 1 || sessions[0].EndedAt.IsZero() || sessions[0].Point != nil {
		t.Errorf("once the run ended, its sessions are %+v, %v", sessions, err)
	}
}

// TestServeSessionsNeedsHost checks that a coordinator without a session
// host, or opened only to read the store, serves no sessions.
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
}
