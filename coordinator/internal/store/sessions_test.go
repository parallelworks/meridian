package store

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
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
		if got := ids(tx.Sessions(root)); len(got) != 2 || got[0] != "a" || got[1] != "c" {
			t.Errorf("the root's sessions are %v", got)
		}
		if got := ids(tx.Sessions(other)); len(got) != 0 {
			t.Errorf("the other run's sessions are %v", got)
		}
		if got := ids(tx.LiveSessions()); len(got) != 2 {
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
		later := time.Now().Add(time.Hour)
		for _, code := range []string{"once", "other", "reset"} {
			if err := tx.AddSessionCode("a", []byte(code), later); err != nil {
				return err
			}
		}
		if err := tx.AddSessionCode("a", []byte("expired"), time.Now().Add(-time.Minute)); err != nil {
			return err
		}
		if err := tx.AddSessionCode("nothing", []byte("x"), later); !errors.Is(err, ErrNotFound) {
			t.Errorf("a code of a session the store hasn't = %v", err)
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
// ends, those its children made among them, and not before: their data is
// erased, their codes deleted, the store no longer finds them by name or
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
		return tx.AddSessionCode("a", []byte("code"), time.Now().Add(time.Hour))
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
			if got.EndedAt.IsZero() || got.Data != nil || got.Sealed != nil {
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
		if err := tx.AddSessionCode("a", []byte("x"), time.Now().Add(time.Hour)); !errors.Is(err, ErrState) {
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
