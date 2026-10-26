package store

import (
	"errors"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// A run's changes each add an event to its events, in the transaction that
// makes them, numbered in order, which RunEvents reads with what each
// reports as it is now; a commit that adds one says so (Changes), and one
// rolled back adds none.
func TestRunEvents(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	changes := s.Changes()
	inTx(t, s, func(tx *Tx) error { return tx.StartRun(id, testState(), nil) })
	select {
	case <-changes:
	default:
		t.Error("the run's start didn't say it changed the store")
	}
	quiet := s.Changes()
	inTx(t, s, func(tx *Tx) error {
		_, err := tx.Wake(id, "build")
		return err
	})
	_ = s.Tx(t.Context(), func(tx *Tx) error {
		if _, err := tx.PinReady(id, "build", false, nil); err != nil {
			t.Error(err)
		}
		return errors.New("rolled back")
	})
	select {
	case <-quiet:
		t.Error("a transaction that added no event, or was rolled back, said it changed the store")
	default:
	}

	step := func(seq int64, finished bool) workerapi.Event {
		e := workerapi.Event{Seq: seq, Time: time.Unix(seq, 0).UTC(), Step: 0, Finished: finished}
		if finished {
			e.Outcome = plan.OutcomeSuccess
		}
		return e
	}
	inTx(t, s, func(tx *Tx) error {
		for range 2 {
			// Readied again, as a supervisor does at each claim, the job
			// adds no event.
			if _, err := tx.PinReady(id, "build", false, nil); err != nil {
				return err
			}
		}
		a, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		if err := tx.ChoosePlace(a.ID, testPlace); err != nil {
			return err
		}
		// A newer attempt supersedes it, placed where the job runs is
		// private.
		b, token, err := tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		if err := tx.ChoosePlace(b.ID, Place{Private: []byte("sealed"), Generation: 1}); err != nil {
			return err
		}
		if err := tx.RecordPlacement(b.ID, Placement{WorkerDigest: testPlacement.WorkerDigest}); err != nil {
			return err
		}
		if _, err := tx.Claim(token, credential('b'), testWorker); err != nil {
			return err
		}
		for range 2 {
			// Sent again, the events add none.
			if _, err := tx.AppendEvents(b.ID, []workerapi.Event{step(1, false), step(2, true)}); err != nil {
				return err
			}
		}
		if err := tx.EndAttempt(b.ID, AttemptEnd{Kind: EndCompleted, Outcome: plan.OutcomeSuccess, CompleteDigest: make([]byte, 32)}); err != nil {
			return err
		}
		if err := tx.EndJob(id, "build", JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeSuccess}}); err != nil {
			return err
		}
		if err := tx.EndJob(id, "test", JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeSkipped}, Reason: plan.ReasonCancelled}); err != nil {
			return err
		}
		return tx.EndRun(id, plan.OutcomeFailure, plan.ReasonTimeout)
	})

	var events []RunEvent
	inView(t, s, func(tx *Tx) error {
		var err error
		events, err = tx.RunEvents(id, 0, 100)
		return err
	})
	type want struct {
		kind, job       string
		attempt         int
		group, member   string
		step            int64
		end             EndKind
		outcome, reason string
	}
	wants := []want{
		{kind: EventRunStarted},
		{kind: EventJobReady, job: "build"},
		{kind: EventJobPlacing, job: "build", attempt: 1, group: "g", member: "m"},
		{kind: EventAttemptEnded, job: "build", attempt: 1, group: "g", member: "m", end: EndSuperseded},
		{kind: EventJobPlacing, job: "build", attempt: 2},
		{kind: EventJobRunning, job: "build", attempt: 2},
		{kind: EventStepStarted, job: "build", attempt: 2, step: 1},
		{kind: EventStepFinished, job: "build", attempt: 2, step: 2},
		{kind: EventAttemptEnded, job: "build", attempt: 2, end: EndCompleted, outcome: "success"},
		{kind: EventJobFinished, job: "build", outcome: "success"},
		{kind: EventJobFinished, job: "test", outcome: "skipped", reason: "cancelled"},
		{kind: EventRunFinished, outcome: "failure", reason: "timeout"},
	}
	if len(events) != len(wants) {
		t.Fatalf("the run has %d events, want %d: %+v", len(events), len(wants), events)
	}
	for i, e := range events {
		w := wants[i]
		got := want{kind: e.Kind, job: e.Job, attempt: e.Attempt, group: e.Group, member: e.Member, outcome: string(e.Outcome), reason: string(e.Reason)}
		if e.Step != nil {
			got.step = e.Step.Seq
		}
		if e.End != nil {
			got.end = e.End.Kind
			got.outcome = string(e.End.Outcome)
		}
		if got != w || e.Seq != int64(i+1) || e.At.IsZero() {
			t.Errorf("event %d is %d %+v at %v, want %+v", i, e.Seq, got, e.At, w)
		}
	}
	if s := events[7].Step; s == nil || !s.Finished || s.Outcome != plan.OutcomeSuccess {
		t.Errorf("the step's finished event is %+v", s)
	}

	inView(t, s, func(tx *Tx) error {
		page, err := tx.RunEvents(id, 5, 3)
		if err != nil || len(page) != 3 || page[0].Seq != 6 || page[2].Seq != 8 {
			t.Errorf("RunEvents after 5, 3 of them = %+v, %v", page, err)
		}
		if rest, err := tx.RunEvents(id, 12, 10); err != nil || len(rest) != 0 {
			t.Errorf("RunEvents after the last = %+v, %v", rest, err)
		}
		if _, err := tx.RunEvents("9", 0, 10); !errors.Is(err, ErrNotFound) {
			t.Errorf("RunEvents of no run = %v, want ErrNotFound", err)
		}
		if _, err := tx.RunEvents(id, 0, 0); err == nil {
			t.Error("RunEvents took a limit of 0")
		}
		return nil
	})
}

// inView runs fn in a View of s's, failing the test on an error.
func inView(t testing.TB, s *Store, fn func(*Tx) error) {
	t.Helper()
	if err := s.View(t.Context(), fn); err != nil {
		t.Fatal(err)
	}
}
