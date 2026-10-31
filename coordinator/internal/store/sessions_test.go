package store

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// testSession returns a session of the run run, whose root is root, under
// the key key and the name name.
func testSession(id, root, run, key, name string) Session {
	return Session{
		ID: id, Root: root, Run: run, Key: key, Name: name, Type: "tunnel", Redirect: true,
		Label: "label" + id, Salt: []byte("salt-" + id), Data: []byte(`{"def":{}}`),
	}
}

// TestSessions checks what the store keeps of a run's sessions: a run makes
// each of its sessions once, under its key, whatever it is started again;
// a call in the tree makes each once too, as every try, attempt and
// matrix instance of it shares them, and each call its own;
// a name that another session that hasn't ended has is
// ErrSessionNameTaken; where a session points is stored in the clear, or
// sealed, and once sealed never in the clear again, nor sealed as an older
// generation; and the store finds them by ID, by name, by their tree's
// root, and as those that haven't ended.
func TestSessions(t *testing.T) {
	t.Parallel()
	s := open(t)
	root, other := createRun(t, s), createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		for i := range 2 {
			made, err := tx.MakeSession(testSession("a", root, root, "desktop", "desktop-1"))
			if err != nil || made != (i == 0) {
				t.Errorf("MakeSession, time %d = %v, %v", i+1, made, err)
			}
		}
		if _, err := tx.MakeSession(testSession("b", other, other, "desktop", "desktop-1")); !errors.Is(err, ErrSessionNameTaken) {
			t.Errorf("a name another session has = %v, want ErrSessionNameTaken", err)
		}
		if _, err := tx.MakeSession(testSession("c", root, root, "link", "link-1")); err != nil {
			t.Error(err)
		}
		if _, err := tx.MakeSession(testSession("d", root, "999", "x", "x-1")); !errors.Is(err, ErrNotFound) {
			t.Errorf("a session of a run the store hasn't = %v", err)
		}
		return nil
	})
	// Runs of the tree a step calls: the first at a call makes its
	// sessions, and another of it, as its next try, makes none; a run of
	// another call makes its own.
	tries := []string{createRun(t, s), createRun(t, s), createRun(t, s)}
	inTx(t, s, func(tx *Tx) error {
		for i, c := range []struct {
			id, run, path, name string
			made                bool
		}{
			{"e", tries[0], `[["build",0]]`, "called-0", true},
			{"f", tries[1], `[["build",0]]`, "called-0", false},
			{"g", tries[2], `[["build",1]]`, "called-1", true},
		} {
			x := testSession(c.id, root, c.run, "desktop", c.name)
			x.CallPath = c.path
			if made, err := tx.MakeSession(x); err != nil || made != c.made {
				t.Errorf("call %d: MakeSession = %t, %v; want %t", i, made, err, c.made)
			}
		}
		got, err := tx.Session("e")
		if err == nil && (got.CallPath != `[["build",0]]` || got.Run != tries[0]) {
			t.Errorf("the call's session is %+v", got)
		}
		return err
	})
	inTx(t, s, func(tx *Tx) error {
		got, err := tx.Session("a")
		if err != nil {
			return err
		}
		if got.Root != root || got.Run != root || got.Key != "desktop" || got.Name != "desktop-1" || got.Type != "tunnel" || !got.Redirect ||
			got.Label != "labela" || string(got.Salt) != "salt-a" || string(got.Data) != `{"def":{}}` || got.Sealed != nil || got.Port != 0 || !got.EndedAt.IsZero() {
			t.Errorf("the session is %+v", got)
		}
		if named, err := tx.SessionNamed("link-1"); err != nil || named.ID != "c" {
			t.Errorf("SessionNamed = %+v, %v", named, err)
		}
		if _, err := tx.SessionNamed("nothing"); !errors.Is(err, ErrNotFound) {
			t.Errorf("SessionNamed of no session = %v", err)
		}
		ids := func(ss []Session, err error) []string {
			if err != nil {
				t.Fatal(err)
			}
			var out []string
			for _, s := range ss {
				out = append(out, s.ID)
			}
			return out
		}
		if got := ids(tx.Sessions(root)); len(got) != 4 || got[0] != "a" || got[1] != "c" {
			t.Errorf("the root's sessions are %v", got)
		}
		if got := ids(tx.Sessions(other)); len(got) != 0 {
			t.Errorf("the other run's sessions are %v", got)
		}
		if got := ids(tx.LiveSessions()); len(got) != 4 {
			t.Errorf("the live sessions are %v", got)
		}
		return nil
	})

	inTx(t, s, func(tx *Tx) error {
		if err := tx.PointSession("a", []byte(`{"point":1}`), nil, 0, 8080, true); err != nil {
			return err
		}
		got, _ := tx.Session("a")
		if string(got.Data) != `{"point":1}` || got.Port != 8080 || !got.PortGiven {
			t.Errorf("pointed in the clear: %+v", got)
		}
		if err := tx.PointSession("a", nil, []byte("sealed 1"), 1, 0, false); err != nil {
			return err
		}
		got, _ = tx.Session("a")
		if got.Data != nil || string(got.Sealed) != "sealed 1" || got.Generation != 1 || got.Port != 0 || got.PortGiven {
			t.Errorf("pointed sealed: %+v", got)
		}
		if err := tx.PointSession("a", []byte(`{}`), nil, 0, 1, false); !errors.Is(err, ErrState) {
			t.Errorf("in the clear once sealed = %v, want ErrState", err)
		}
		if err := tx.PointSession("a", nil, []byte("sealed again"), 1, 1, false); !errors.Is(err, ErrState) {
			t.Errorf("sealed as a generation that isn't newer = %v, want ErrState", err)
		}
		if err := tx.PointSession("nothing", nil, nil, 0, 0, false); !errors.Is(err, ErrNotFound) {
			t.Errorf("a session the store hasn't = %v", err)
		}
		return nil
	})
	checkIntegrity(t, s)
}

// TestSessionCodes checks a session's one-time codes: a code is used once,
// only for its own session, only until it expires by the store's clock,
// and goes with the session's reset, which replaces its salt, as with its
// end.
func TestSessionCodes(t *testing.T) {
	t.Parallel()
	s := open(t)
	root := createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		for _, id := range []string{"a", "b"} {
			if _, err := tx.MakeSession(testSession(id, root, root, id, id+"-1")); err != nil {
				return err
			}
		}
		later := time.Hour
		for _, code := range []string{"once", "other", "reset"} {
			if err := tx.AddSessionCode("a", []byte(code), later); err != nil {
				return err
			}
		}
		// A code whose lifetime has passed, by the store's clock, as
		// it is added.
		if err := tx.AddSessionCode("a", []byte("expired"), -time.Minute); err != nil {
			return err
		}
		if err := tx.AddSessionCode("nothing", []byte("x"), later); !errors.Is(err, ErrNotFound) {
			t.Errorf("a code of a session the store hasn't = %v", err)
		}
		return nil
	})
	// The next code added deletes those expired, of any session's.
	inTx(t, s, func(tx *Tx) error {
		if err := tx.AddSessionCode("b", []byte("next"), time.Hour); err != nil {
			return err
		}
		var expired int
		if err := tx.queryRow("SELECT count(*) FROM meridian_session_codes WHERE mac = ?", []byte("expired")).Scan(&expired); err != nil || expired != 0 {
			t.Errorf("%d expired codes are kept once another is added, %v", expired, err)
		}
		return nil
	})
	redeem := func(id, mac string) bool {
		t.Helper()
		var ok bool
		inTx(t, s, func(tx *Tx) error {
			var err error
			ok, err = tx.RedeemSessionCode(id, []byte(mac))
			return err
		})
		return ok
	}
	if !redeem("a", "once") || redeem("a", "once") {
		t.Error("a code isn't used once")
	}
	if redeem("b", "other") || !redeem("a", "other") {
		t.Error("a code is another session's")
	}
	if redeem("a", "expired") {
		t.Error("an expired code was used")
	}
	inTx(t, s, func(tx *Tx) error {
		before, _ := tx.Session("a")
		if err := tx.ResetSession("a", []byte("new salt")); err != nil {
			return err
		}
		after, _ := tx.Session("a")
		if bytes.Equal(before.Salt, after.Salt) || string(after.Salt) != "new salt" {
			t.Errorf("the salt after a reset is %q", after.Salt)
		}
		if err := tx.ResetSession("nothing", nil); !errors.Is(err, ErrNotFound) {
			t.Errorf("a reset of a session the store hasn't = %v", err)
		}
		return nil
	})
	if redeem("a", "reset") {
		t.Error("a code outlived its session's reset")
	}
	checkIntegrity(t, s)
}

// TestSessionsEndWithRoot checks that a tree's sessions end as its root
// ends, those its children made among them, and not before: their data and
// their salts, and why they couldn't be served, are erased, their codes
// deleted, the store no longer finds them by name or
// among those live, a reset or a point of one is ErrState, and another
// session may take the name.
func TestSessionsEndWithRoot(t *testing.T) {
	t.Parallel()
	s := open(t)
	root, child := createRun(t, s), createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		if _, err := tx.MakeSession(testSession("a", root, root, "desktop", "desktop-1")); err != nil {
			return err
		}
		// A session a run of the tree made, as a child does: its root
		// owns it.
		if _, err := tx.MakeSession(testSession("b", root, child, "child", "child-1")); err != nil {
			return err
		}
		if err := tx.PointSession("a", nil, []byte("sealed"), 1, 9000, false); err != nil {
			return err
		}
		if err := tx.SessionServed("a", 0, problems.SessionLocalPortTaken.Problem(problems.Params{"port": "9000"})); err != nil {
			return err
		}
		return tx.AddSessionCode("a", []byte("code"), time.Hour)
	})
	inTx(t, s, func(tx *Tx) error { return tx.EndRun(child, plan.OutcomeSuccess, "") })
	inTx(t, s, func(tx *Tx) error {
		if live, err := tx.LiveSessions(); err != nil || len(live) != 2 {
			t.Errorf("once a run of the tree ended, the live sessions are %d, %v", len(live), err)
		}
		return tx.EndRun(root, plan.OutcomeSuccess, "")
	})
	inTx(t, s, func(tx *Tx) error {
		if live, err := tx.LiveSessions(); err != nil || len(live) != 0 {
			t.Errorf("once the root ended, the live sessions are %d, %v", len(live), err)
		}
		for _, id := range []string{"a", "b"} {
			got, err := tx.Session(id)
			if err != nil {
				return err
			}
			if got.EndedAt.IsZero() || got.Data != nil || got.Sealed != nil || len(got.Salt) != 0 || got.Problem != nil {
				t.Errorf("session %s once its root ended: %+v", id, got)
			}
		}
		if _, err := tx.SessionNamed("desktop-1"); !errors.Is(err, ErrNotFound) {
			t.Errorf("SessionNamed of an ended session = %v", err)
		}
		if err := tx.ResetSession("a", []byte("x")); !errors.Is(err, ErrState) {
			t.Errorf("a reset of an ended session = %v, want ErrState", err)
		}
		if err := tx.PointSession("a", []byte("{}"), nil, 0, 1, false); !errors.Is(err, ErrState) {
			t.Errorf("pointing an ended session = %v, want ErrState", err)
		}
		if err := tx.AddSessionCode("a", []byte("x"), time.Hour); !errors.Is(err, ErrState) {
			t.Errorf("a code of an ended session = %v, want ErrState", err)
		}
		var codes int
		if err := tx.queryRow("SELECT count(*) FROM meridian_session_codes").Scan(&codes); err != nil || codes != 0 {
			t.Errorf("%d codes are kept once the sessions ended, %v", codes, err)
		}
		return nil
	})
	next := createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		_, err := tx.MakeSession(testSession("c", next, next, "desktop", "desktop-1"))
		return err
	})
	checkIntegrity(t, s)
}

// TestServeSession checks a session's lease, which its process holds as
// long as it serves it: one at a time, in this process or another; taken,
// it resets the session, its codes with it, before it returns; let go, its
// file is gone, and another may take it; and a session that has ended, or
// that the store hasn't, is never taken, nor one of a store opened only to
// read.
func TestServeSession(t *testing.T) {
	t.Parallel()
	s := open(t)
	root := createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		for _, id := range []string{"a", "b"} {
			if _, err := tx.MakeSession(testSession(id, root, root, id, id+"-1")); err != nil {
				return err
			}
		}
		return tx.AddSessionCode("a", []byte("code"), time.Hour)
	})
	release, err := s.ServeSession(t.Context(), "a", []byte("taken"))
	if err != nil {
		t.Fatal(err)
	}
	file := s.file + ".session-a.lock"
	if _, err := os.Lstat(file); err != nil {
		t.Errorf("the lease's file: %v", err)
	}
	inTx(t, s, func(tx *Tx) error {
		got, err := tx.Session("a")
		if err == nil && string(got.Salt) != "taken" {
			t.Errorf("the session, taken, has the salt %q", got.Salt)
		}
		if ok, err := tx.RedeemSessionCode("a", []byte("code")); ok || err != nil {
			t.Errorf("a code from before the session was taken = %t, %v", ok, err)
		}
		return err
	})
	if _, err := s.ServeSession(t.Context(), "a", []byte("again")); !errors.Is(err, ErrSessionServed) {
		t.Errorf("ServeSession of a session served = %v, want ErrSessionServed", err)
	}
	inTx(t, s, func(tx *Tx) error {
		got, err := tx.Session("a")
		if err == nil && string(got.Salt) != "taken" {
			t.Errorf("a session another serves was reset, to %q", got.Salt)
		}
		return err
	})
	other, err := s.ServeSession(t.Context(), "b", []byte("b"))
	if err != nil {
		t.Fatalf("ServeSession of another session = %v", err)
	}
	other()
	release()
	if _, err := os.Lstat(file); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the lease's file is there once it is let go: %v", err)
	}
	again, err := s.ServeSession(t.Context(), "a", []byte("again"))
	if err != nil {
		t.Fatalf("ServeSession once let go = %v", err)
	}
	again()

	inTx(t, s, func(tx *Tx) error { return tx.EndRun(root, plan.OutcomeSuccess, "") })
	if _, err := s.ServeSession(t.Context(), "a", []byte("ended")); !errors.Is(err, ErrState) {
		t.Errorf("ServeSession of an ended session = %v, want ErrState", err)
	}
	if _, err := os.Lstat(file); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the lease of an ended session is held: %v", err)
	}
	for _, bad := range []string{"nothing", "../a", ""} {
		if _, err := s.ServeSession(t.Context(), bad, []byte("x")); !errors.Is(err, ErrNotFound) {
			t.Errorf("ServeSession(%q) = %v, want ErrNotFound", bad, err)
		}
	}
	ro, err := Open(t.Context(), s.file, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	// Read only, it takes nothing, even a session another serves.
	live := createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		_, err := tx.MakeSession(testSession("c", live, live, "c", "c-1"))
		return err
	})
	held, err := s.ServeSession(t.Context(), "c", []byte("c"))
	if err != nil {
		t.Fatal(err)
	}
	defer held()
	if _, err := ro.ServeSession(t.Context(), "c", []byte("x")); !errors.Is(err, ErrReadOnly) {
		t.Errorf("ServeSession read only = %v, want ErrReadOnly", err)
	}
}

// TestSessionServed checks what the store keeps of how a session's process
// serves it: the port, or why it can't, which leaves the port as it was,
// masked for a session whose data is sealed, and which a step that points
// it again clears; never of a session that has ended.
func TestSessionServed(t *testing.T) {
	t.Parallel()
	s := open(t)
	root := createRun(t, s)
	taken := problems.SessionLocalPortTaken.Problem(problems.Params{"port": "7000"})
	inTx(t, s, func(tx *Tx) error {
		for _, id := range []string{"a", "b"} {
			if _, err := tx.MakeSession(testSession(id, root, root, id, id+"-1")); err != nil {
				return err
			}
		}
		if err := tx.PointSession("a", []byte(`{}`), nil, 0, 7000, true); err != nil {
			return err
		}
		if err := tx.PointSession("b", nil, []byte("sealed"), 1, 7001, true); err != nil {
			return err
		}
		if err := tx.SessionServed("a", 0, taken); err != nil {
			return err
		}
		if err := tx.SessionServed("b", 0, taken); err != nil {
			return err
		}
		a, _ := tx.Session("a")
		if a.Port != 7000 || a.Problem == nil || a.Problem.Code != problems.SessionLocalPortTaken.Code() || a.Problem.Params["port"] != "7000" {
			t.Errorf("a session that can't be served is %+v, its problem %+v", a, a.Problem)
		}
		b, _ := tx.Session("b")
		if b.Problem == nil || b.Problem.Code != problems.SessionLocalPortTaken.Code() || b.Problem.Params["port"] == "7000" {
			t.Errorf("a sealed session that can't be served has the problem %+v", b.Problem)
		}
		if err := tx.SessionServed("a", 7002, nil); err != nil {
			return err
		}
		a, _ = tx.Session("a")
		if a.Port != 7002 || a.Problem != nil {
			t.Errorf("a session served at another port is %+v", a)
		}
		if err := tx.PointSession("b", nil, []byte("sealed again"), 2, 7001, true); err != nil {
			return err
		}
		b, _ = tx.Session("b")
		if b.Problem != nil {
			t.Errorf("a session pointed again has the problem %+v", b.Problem)
		}
		if err := tx.SessionServed("nothing", 1, nil); !errors.Is(err, ErrNotFound) {
			t.Errorf("SessionServed of a session the store hasn't = %v", err)
		}
		return tx.EndRun(root, plan.OutcomeSuccess, "")
	})
	inTx(t, s, func(tx *Tx) error {
		if err := tx.SessionServed("a", 1, nil); !errors.Is(err, ErrState) {
			t.Errorf("SessionServed of an ended session = %v, want ErrState", err)
		}
		return nil
	})
	checkIntegrity(t, s)
}
