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
	"github.com/parallelworks/meridian/workerapi"
)

// testHost is a session host of a test's. It names each session by its key
// and a number of its own, points a session where with: says, at a port
// it gives, refusing a with: whose remoteHost is refuse as a port in use,
// and records what it is asked to point, and whether to serve it there,
// and what it is asked to serve.
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

func (h *testHost) Point(_ context.Context, s Session, u compile.SessionUpdate, serve bool) (SessionPoint, error) {
	if u.With["remoteHost"] == "refuse" {
		return SessionPoint{}, problems.SessionLocalPortTaken.Problem(problems.Params{"port": "1"})
	}
	data, err := json.Marshal(u.With, json.Deterministic(true))
	if err != nil {
		return SessionPoint{}, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.points = append(h.points, fmt.Sprintf("%s %s %v %s serve=%t", s.Name, s.Key, s.Def, data, serve))
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
	made, err := c.RunSessions(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.opts.Sessions.Update(t.Context(), compile.SessionUpdate{Root: root, Name: "desktop-1", With: map[string]any{"remotePort": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	sessions, err := c.RunSessions(t.Context(), root)
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
// host, or opened only to read the store, serves no sessions, and the
// latter makes and points none.
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
		got, err := a.RunSessions(t.Context(), root)
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
	got, err := c.RunSessions(t.Context(), root)
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
	if got, err := c.RunSessions(t.Context(), root); err != nil || len(got) != 0 {
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
	if got, err := c.RunSessions(t.Context(), root); err != nil || len(got) != 0 {
		t.Errorf("the run's sessions, once one's name was taken, are %+v, %v", got, err)
	}
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
		return tx.PointSession(bad.ID, nil, []byte("damaged"), bad.Generation+1, bad.Port, false)
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
		got, err := ro.RunSessions(t.Context(), root)
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
	taken := problems.SessionLocalPortTaken.Problem(problems.Params{"port": "31000"})
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
