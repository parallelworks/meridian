package store

import (
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
		a, err = tx.Claim(token, credential(c), testWorker)
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
		got, err := tx.Attempt(a.ID)
		if err != nil || got.State != AttemptRunning || got.StepStartedAt.IsZero() {
			t.Errorf("after a step's start the attempt is %+v, %v", got, err)
		}
		return nil
	})
}

// SetControl asks more at a version that only grows, which Control gives;
// an attempt that has ended is asked nothing more.
func TestControl(t *testing.T) {
	s := open(t)
	a := claimedAttempt(t, s, 'a')
	inTx(t, s, func(tx *Tx) error {
		if c, err := tx.Control(a.ID); err != nil || c != (workerapi.Control{}) {
			t.Errorf("Control before any = %+v, %v", c, err)
		}
		for want := int64(1); want <= 2; want++ {
			v, err := tx.SetControl(a.ID, workerapi.Control{Version: 99, Cancel: true, Reason: plan.ReasonCancelled})
			if err != nil || v != want {
				t.Errorf("SetControl = %d, %v; want %d", v, err, want)
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
		if _, err := tx.SetControl(a.ID, workerapi.Control{Cancel: true}); !errors.Is(err, ErrState) {
			t.Errorf("SetControl of an attempt that ended = %v, want ErrState", err)
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
		if _, err := tx.Claim(token, credential('a'), testWorker); !errors.Is(err, ErrNotPlaced) {
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
		if _, err := tx.Claim(token, credential('a'), testWorker); err != nil {
			return err
		}
		if err := tx.RecordPlacement(a.ID, p); !errors.Is(err, ErrState) {
			t.Errorf("RecordPlacement once claimed = %v, want ErrState", err)
		}
		if err := tx.StepStarted(a.ID); err != nil {
			return err
		}
		refusal := problems.WorkerDigestMismatch.Problem(problems.Params{"placed": "a", "claimed": "b"})
		if err := tx.EndAttempt(a.ID, AttemptEnd{Kind: EndLost, Problem: refusal}); err == nil {
			t.Error("an attempt lost with a problem")
		}
		report := []byte(`{"seq":0,"result":{"outcome":"failure"},"reason":"setup-failed"}`)
		if err := tx.EndAttempt(a.ID, AttemptEnd{
			Kind: EndCompleted, Outcome: plan.OutcomeFailure, Reason: plan.ReasonSetupFailed, CompleteDigest: make([]byte, 32), Report: report, Problem: refusal,
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
			a, err := tx.Claim(token, credential(byte('a'+boolInt(selfFencing))), testWorker)
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
