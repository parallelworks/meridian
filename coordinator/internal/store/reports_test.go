package store

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// testPlacement is where the tests' attempts are placed.
var testPlacement = Placement{Group: "local", Member: "laptop", Backend: "local", ID: "12:34", WorkerDigest: "sha256:" + strings.Repeat("ab", 32)}

// placedAttempt starts an attempt, as NewAttempt does, and records its
// placement, as the coordinator does once its backend has placed it, which
// its claim waits for.
func placedAttempt(tx *Tx, n NewAttempt) (*Attempt, string, error) {
	a, token, err := tx.NewAttempt(n)
	if err != nil {
		return nil, "", err
	}
	if err := tx.RecordPlacement(a.ID, testPlacement); err != nil {
		return nil, "", err
	}
	a, err = tx.Attempt(a.ID)
	return a, token, err
}

// claimedAttempt starts and claims an attempt of a new run's build job.
func claimedAttempt(t *testing.T, s *Store, c byte) *Attempt {
	t.Helper()
	id := createRun(t, s)
	var a *Attempt
	inTx(t, s, func(tx *Tx) error {
		_, token, err := placedAttempt(tx, NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		a, err = tx.Claim(token, credential(c), testWorker, workerapi.Version)
		return err
	})
	return a
}

func event(seq int64, finished bool) workerapi.Event {
	return workerapi.Event{Seq: seq, Time: time.Unix(seq, 0).UTC(), Finished: finished}
}

// AppendEvents stores events in order, each once, and none of a batch that
// skips one; a step's start makes the attempt running.
func TestAppendEvents(t *testing.T) {
	s := open(t)
	a := claimedAttempt(t, s, 'a')
	inTx(t, s, func(tx *Tx) error {
		if last, err := tx.AppendEvents(a.ID, []workerapi.Event{event(1, false), event(2, true)}); err != nil || last != 2 {
			t.Errorf("AppendEvents = %d, %v; want 2", last, err)
		}
		if last, err := tx.AppendEvents(a.ID, []workerapi.Event{event(2, true), event(3, false)}); err != nil || last != 3 {
			t.Errorf("AppendEvents of a repeat and one more = %d, %v; want 3", last, err)
		}
		last, err := tx.AppendEvents(a.ID, []workerapi.Event{event(4, true), event(6, true)})
		if gap, ok := errors.AsType[*GapError](err); !ok || gap.Expected != 4 || last != 3 {
			t.Errorf("AppendEvents with a gap = %d, %v; want a gap expecting 4", last, err)
		}
		events, err := tx.Events(a.ID)
		if err != nil || len(events) != 3 || events[2].Seq != 3 {
			t.Errorf("Events = %+v, %v", events, err)
		}
		var size int64
		for _, e := range events {
			body, err := workerapi.Marshal(e)
			if err != nil {
				return err
			}
			size += int64(len(body))
		}
		if n, bytes, err := tx.EventsStored(a.ID); err != nil || n != 3 || bytes != size {
			t.Errorf("EventsStored = %d, %d, %v; want 3, %d", n, bytes, err, size)
		}
		if _, _, err := tx.EventsStored("nothing"); !errors.Is(err, ErrNotFound) {
			t.Errorf("EventsStored of no attempt = %v, want ErrNotFound", err)
		}
		got, err := tx.Attempt(a.ID)
		if err != nil || got.State != AttemptRunning || got.StepStartedAt.IsZero() {
			t.Errorf("after a step's start the attempt is %+v, %v", got, err)
		}
		return nil
	})
}

// UpdateControl asks more at a version that only grows, whatever the
// control it is given says, which Control gives; an attempt that has ended
// is asked nothing more.
func TestControl(t *testing.T) {
	s := open(t)
	a := claimedAttempt(t, s, 'a')
	inTx(t, s, func(tx *Tx) error {
		if c, err := tx.Control(a.ID); err != nil || c != (workerapi.Control{}) {
			t.Errorf("Control before any = %+v, %v", c, err)
		}
		for want := int64(1); want <= 2; want++ {
			v, err := tx.UpdateControl(a.ID, func(c *workerapi.Control) bool {
				*c = workerapi.Control{Version: 99, Cancel: true, Reason: plan.ReasonCancelled}
				return true
			})
			if err != nil || v != want {
				t.Errorf("UpdateControl = %d, %v; want %d", v, err, want)
			}
		}
		if c, err := tx.Control(a.ID); err != nil || c.Version != 2 || !c.Cancel || c.Reason != plan.ReasonCancelled {
			t.Errorf("Control = %+v, %v", c, err)
		}
		if _, err := tx.Control("nothing"); !errors.Is(err, ErrNotFound) {
			t.Errorf("Control of no attempt = %v", err)
		}
		if err := tx.EndAttempt(a.ID, AttemptEnd{Kind: EndLost, Reason: plan.ReasonWorkerLost}); err != nil {
			return err
		}
		if _, err := tx.UpdateControl(a.ID, func(c *workerapi.Control) bool { c.Cancel = true; return true }); !errors.Is(err, ErrState) {
			t.Errorf("UpdateControl of an attempt that ended = %v, want ErrState", err)
		}
		return nil
	})
}

// RecordPlacement records where an attempt was placed, with the digest of
// the worker binary it started, until it is claimed, and its first claim
// waits for it; an attempt's end keeps its report and its problem; and
// StartedBefore counts the attempts before one that started a step.
func TestPlacementAndEnd(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		a, token, err := tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		if _, err := tx.Claim(token, credential('a'), testWorker, workerapi.Version); !errors.Is(err, ErrNotPlaced) {
			t.Errorf("Claim before the placement is recorded = %v, want ErrNotPlaced", err)
		}
		p := testPlacement
		for _, bad := range []Placement{
			{Backend: "local", ID: "1:2"},
			{Backend: "local", ID: "1:2", WorkerDigest: "sha256:ab"},
			{Backend: "local", WorkerDigest: p.WorkerDigest},
			{ID: "1:2", WorkerDigest: p.WorkerDigest},
		} {
			if err := tx.RecordPlacement(a.ID, bad); err == nil {
				t.Errorf("RecordPlacement(%+v) succeeded", bad)
			}
		}
		if err := tx.RecordPlacement(a.ID, p); err != nil {
			return err
		}
		if got, err := tx.AttemptByToken(token); err != nil || got.Placement != p || got.ID != a.ID {
			t.Errorf("AttemptByToken = %+v, %v", got, err)
		}
		if _, err := tx.Claim(token, credential('a'), testWorker, workerapi.Version); err != nil {
			return err
		}
		if err := tx.RecordPlacement(a.ID, p); !errors.Is(err, ErrState) {
			t.Errorf("RecordPlacement once claimed = %v, want ErrState", err)
		}
		if err := tx.StepStarted(a.ID); err != nil {
			return err
		}
		refusal := problems.WorkerDigestMismatch.Problem(problems.Params{"placed": "a", "claimed": "b"})
		if err := tx.EndAttempt(a.ID, AttemptEnd{Kind: EndSuperseded, Problem: refusal}); err == nil {
			t.Error("an attempt superseded with a problem")
		}
		report := []byte(`{"seq":0,"result":{"outcome":"failure"},"reason":"setup-failed"}`)
		if err := tx.EndAttempt(a.ID, AttemptEnd{
			Kind: EndCompleted, Outcome: plan.OutcomeFailure, Reason: plan.ReasonSetupFailed, Report: report, Problem: refusal,
		}); err != nil {
			return err
		}
		got, err := tx.Attempt(a.ID)
		if err != nil {
			return err
		}
		end, err := got.Complete()
		if err != nil || end == nil || end.Reason != plan.ReasonSetupFailed {
			t.Errorf("the attempt's report is %+v, %v", end, err)
		}
		if got.End.Problem == nil || got.End.Problem.Code != problems.WorkerDigestMismatch.Code() || got.End.Problem.Params["claimed"] != "b" {
			t.Errorf("the attempt's problem is %+v", got.End.Problem)
		}
		b, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		if n, err := tx.StartedBefore(b); err != nil || n != 1 {
			t.Errorf("StartedBefore = %d, %v; want 1", n, err)
		}
		if n, err := tx.StartedBefore(a); err != nil || n != 0 {
			t.Errorf("StartedBefore of the first = %d, %v; want 0", n, err)
		}
		return nil
	})
}

// Lapsed is set once the lease of an attempt whose job fences itself has
// lapsed, and never for another job's.
func TestLapsed(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		for _, selfFencing := range []bool{false, true} {
			_, token, err := placedAttempt(tx, NewAttempt{Run: id, Job: "build", SelfFencing: selfFencing, LeaseSeconds: 60, HeartbeatSeconds: 10})
			if err != nil {
				return err
			}
			a, err := tx.Claim(token, credential(byte('a'+boolInt(selfFencing))), testWorker, workerapi.Version)
			if err != nil {
				return err
			}
			if lapsed, err := tx.Lapsed(a); err != nil || lapsed {
				t.Errorf("a lease just renewed lapsed: %v, %v", lapsed, err)
			}
			a.RenewedAt = a.RenewedAt.Add(-time.Hour)
			if lapsed, err := tx.Lapsed(a); err != nil || lapsed != selfFencing {
				t.Errorf("an hour on, a lease of a job that fences itself %v lapsed: %v, %v", selfFencing, lapsed, err)
			}
		}
		return nil
	})
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// AddLogBytes counts an attempt's bytes of logs, which LogBytes gives.
func TestLogBytes(t *testing.T) {
	s := open(t)
	a := claimedAttempt(t, s, 'a')
	inTx(t, s, func(tx *Tx) error {
		if n, err := tx.LogBytes(a.ID); err != nil || n != 0 {
			t.Errorf("LogBytes before any = %d, %v", n, err)
		}
		for _, add := range []struct{ n, all int64 }{{10, 10}, {0, 10}, {15, 25}} {
			if all, err := tx.AddLogBytes(a.ID, add.n); err != nil || all != add.all {
				t.Errorf("AddLogBytes(%d) = %d, %v; want %d", add.n, all, err, add.all)
			}
		}
		if n, err := tx.LogBytes(a.ID); err != nil || n != 25 {
			t.Errorf("LogBytes = %d, %v; want 25", n, err)
		}
		if _, err := tx.AddLogBytes(a.ID, -1); err == nil {
			t.Error("AddLogBytes counted -1 bytes")
		}
		if _, err := tx.AddLogBytes("nothing", 1); !errors.Is(err, ErrNotFound) {
			t.Errorf("AddLogBytes of no attempt = %v, want ErrNotFound", err)
		}
		return nil
	})
}

// An attempt whose placement is private is recorded without its group,
// member, backend, the backend's ID for it and the run's directory on the
// member, which are held only sealed,
// and is claimed once the digest of the worker binary it started is
// recorded; the problem it ends with is stored masked, since it can name
// the member.
func TestPrivatePlacement(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		a, token, err := tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		// As the coordinator records where it is to be placed, sealed.
		if _, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_attempts SET private_placement = x'01' WHERE id = ?", a.ID); err != nil {
			return err
		}
		if _, err := tx.Claim(token, credential('a'), testWorker, workerapi.Version); !errors.Is(err, ErrNotPlaced) {
			t.Errorf("Claim before the placement is recorded = %v, want ErrNotPlaced", err)
		}
		for _, bad := range []Placement{
			testPlacement,
			{Member: "laptop", WorkerDigest: testPlacement.WorkerDigest},
			{Backend: "local", WorkerDigest: testPlacement.WorkerDigest},
			{ID: "pod-on-host-7", WorkerDigest: testPlacement.WorkerDigest},
			{RunDir: "/home/alice/.local/state/meridian/runs/r1", WorkerDigest: testPlacement.WorkerDigest},
			{},
		} {
			if err := tx.RecordPlacement(a.ID, bad); err == nil {
				t.Errorf("RecordPlacement of a private placement with %+v succeeded", bad)
			}
		}
		p := Placement{WorkerDigest: testPlacement.WorkerDigest}
		if err := tx.RecordPlacement(a.ID, p); err != nil {
			return err
		}
		if got, err := tx.Attempt(a.ID); err != nil || !got.Private || !got.Placed() || got.Placement != p {
			t.Errorf("the attempt is %+v, %v; want private, placed at %+v", got, err, p)
		}
		var group, member, backend, placementID, runDir sql.NullString
		if err := tx.tx.QueryRowContext(tx.ctx, "SELECT grp, member, backend, placement_id, run_dir FROM meridian_attempts WHERE id = ?", a.ID).Scan(
			&group, &member, &backend, &placementID, &runDir); err != nil {
			return err
		}
		if group.Valid || member.Valid || backend.Valid || placementID.Valid || runDir.Valid {
			t.Errorf("a private placement is held in the clear: %v %v %v %v %v", group, member, backend, placementID, runDir)
		}
		if _, err := tx.Claim(token, credential('a'), testWorker, workerapi.Version); err != nil {
			return err
		}
		refusal := problems.MemberInvalid.Problem(problems.Params{"group": "secret-group", "key": "cpu", "value": "secret-member"})
		if err := tx.EndAttempt(a.ID, AttemptEnd{Kind: EndSetupFailed, Outcome: plan.OutcomeFailure, Reason: plan.ReasonSetupFailed, Problem: refusal}); err != nil {
			return err
		}
		var stored []byte
		if err := tx.tx.QueryRowContext(tx.ctx, "SELECT problem FROM meridian_attempts WHERE id = ?", a.ID).Scan(&stored); err != nil {
			return err
		}
		if strings.Contains(string(stored), "secret-") || !strings.Contains(string(stored), problems.Masked) {
			t.Errorf("the problem of a private attempt is stored as %s", stored)
		}
		if refusal.Params["value"] != "secret-member" {
			t.Error("EndAttempt changed its caller's problem")
		}
		got, err := tx.Attempt(a.ID)
		if err != nil || got.End.Problem == nil || got.End.Problem.Code != problems.MemberInvalid.Code() || got.End.Problem.Params["key"] != "cpu" {
			t.Errorf("the attempt's problem is %+v, %v", got.End.Problem, err)
		}
		return nil
	})
}
