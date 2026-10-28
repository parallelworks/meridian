package store

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/coordtest"
	"github.com/parallelworks/meridian/plan"
)

// TestChildLinks checks that a child run keeps its link to the try of the
// step that called it, which finds it again, one child at most for each
// try; that a root counts the runs of its tree that haven't ended, at any
// depth; and that a run keeps its finisher, and when it began to wait for
// its tree.
func TestChildLinks(t *testing.T) {
	s := open(t)
	root := createRun(t, s)
	var attempt *Attempt
	inTx(t, s, func(tx *Tx) error {
		if err := tx.StartRun(root, testState(), nil); err != nil {
			return err
		}
		var err error
		attempt, _, err = tx.NewAttempt(NewAttempt{Run: root, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		return err
	})
	p := testPlan(t)
	child := func(link Link) (string, error) {
		var id string
		err := s.Tx(t.Context(), func(tx *Tx) error {
			var err error
			if id, err = tx.NewRunID(); err != nil {
				return err
			}
			return tx.InsertRun(NewRun{ID: id, Plan: p, DefaultGroup: "local", RunDir: "/runs/x", WorkflowID: fakeJobID(10), FinisherID: fakeJobID(11), Parent: &link})
		})
		return id, err
	}
	first := Link{Run: root, Job: "build", Attempt: attempt.ID, Step: 1, Try: 0, Root: root, Depth: 1}
	a, err := child(first)
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.Try = 1
	b, err := child(second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := child(first); err == nil {
		t.Error("a try's second child was stored")
	}
	inTx(t, s, func(tx *Tx) error {
		for _, tc := range []struct {
			step, try int
			want      string
		}{{1, 0, a}, {1, 1, b}, {0, 0, ""}, {1, 2, ""}} {
			got, err := tx.Child(attempt.ID, tc.step, tc.try)
			switch {
			case tc.want == "" && !errors.Is(err, ErrNotFound):
				t.Errorf("step %d, try %d: %q, %v, want none", tc.step, tc.try, got, err)
			case tc.want != "" && (err != nil || got != tc.want):
				t.Errorf("step %d, try %d: %q, %v, want %s", tc.step, tc.try, got, err, tc.want)
			}
		}
		r, err := tx.Run(a)
		if err != nil {
			return err
		}
		if r.Parent == nil || *r.Parent != first || r.FinisherID != fakeJobID(11) {
			t.Errorf("the child's link is %+v, its finisher %v", r.Parent, r.FinisherID)
		}
		if r, err = tx.Run(root); err != nil || r.Parent != nil || !r.FinisherID.IsZero() || !r.WaitedAt.IsZero() {
			t.Errorf("the root is %+v, %v", r, err)
		}
		return nil
	})
	// A grandchild counts toward its root's tree.
	grand := Link{Run: a, Job: "build", Attempt: attempt.ID, Step: 2, Root: root, Depth: 2}
	c, err := child(grand)
	if err != nil {
		t.Fatal(err)
	}
	live := func() int {
		var n int
		inTx(t, s, func(tx *Tx) error {
			var err error
			n, err = tx.LiveDescendants(root)
			return err
		})
		return n
	}
	if n := live(); n != 3 {
		t.Errorf("the root's tree has %d runs that haven't ended, want 3", n)
	}
	inTx(t, s, func(tx *Tx) error {
		for _, id := range []string{a, c} {
			if err := tx.EndRun(id, plan.OutcomeSuccess, ""); err != nil {
				return err
			}
		}
		return nil
	})
	if n := live(); n != 1 {
		t.Errorf("the root's tree has %d runs that haven't ended, want 1", n)
	}
	inTx(t, s, func(tx *Tx) error {
		if err := tx.WaitForDescendants(root); err != nil {
			return err
		}
		r, err := tx.Run(root)
		if err != nil || r.WaitedAt.IsZero() {
			t.Errorf("the root waits since %v, %v", r.WaitedAt, err)
		}
		return nil
	})
	checkIntegrity(t, s)

	// A link that names no step's try is refused.
	for _, bad := range []Link{
		{Run: root, Job: "", Attempt: attempt.ID, Root: root, Depth: 1},
		{Run: root, Job: "build", Attempt: attempt.ID, Step: -1, Root: root, Depth: 1},
		{Run: root, Job: "build", Attempt: attempt.ID, Root: root, Depth: 0},
		{Run: "x", Job: "build", Attempt: attempt.ID, Root: root, Depth: 1},
	} {
		if _, err := child(bad); err == nil {
			t.Errorf("a link %+v was stored", bad)
		}
	}
}

// TestAttemptsEnded checks that whoever opened the store is told, in the
// transaction that ends attempts, which ones it ended: those EndAttempt
// ends, and those a new attempt of their job supersedes; and finds the
// children of their steps that haven't ended there (LiveChildren). What it
// has done once the transaction commits (AfterCommit) is done then, and
// never for one that doesn't; and an error it returns is the
// transaction's, which then stores nothing.
func TestAttemptsEnded(t *testing.T) {
	var (
		mu        sync.Mutex
		told      [][]string
		children  []LiveChild
		committed []string
		fail      error
	)
	s, err := Open(t.Context(), filepath.Join(coordtest.PrivateDir(t), "meridian.db"), &Options{
		OnAttemptsEnded: func(_ context.Context, tx *Tx, ids []string) error {
			live, err := tx.LiveChildren(ids...)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			told = append(told, ids)
			children = append(children, live...)
			tx.AfterCommit(func(context.Context) {
				mu.Lock()
				defer mu.Unlock()
				committed = append(committed, ids...)
			})
			return fail
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	root := createRun(t, s)
	var first *Attempt
	inTx(t, s, func(tx *Tx) error {
		if err := tx.StartRun(root, testState(), nil); err != nil {
			return err
		}
		var err error
		first, _, err = tx.NewAttempt(NewAttempt{Run: root, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		return err
	})
	var child, ended string
	inTx(t, s, func(tx *Tx) error {
		var err error
		for _, id := range []*string{&child, &ended} {
			if *id, err = tx.NewRunID(); err != nil {
				return err
			}
			try := 0
			if id == &ended {
				try = 1
			}
			link := &Link{Run: root, Job: "build", Attempt: first.ID, Step: 1, Try: try, Root: root, Depth: 1}
			run := NewRun{ID: *id, Plan: testPlan(t), DefaultGroup: "local", RunDir: "/runs/x", WorkflowID: fakeJobID(20 + try), Parent: link}
			if err := tx.InsertRun(run); err != nil {
				return err
			}
		}
		return tx.EndRun(ended, plan.OutcomeSuccess, "")
	})
	// A new attempt supersedes the first, whose step's child is live.
	var second *Attempt
	inTx(t, s, func(tx *Tx) error {
		var err error
		second, _, err = tx.NewAttempt(NewAttempt{Run: root, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		return err
	})
	mu.Lock()
	if !slices.EqualFunc(told, [][]string{{first.ID}}, slices.Equal) || !slices.Equal(committed, []string{first.ID}) ||
		!slices.Equal(children, []LiveChild{{ID: child, Attempt: first.ID, Step: 1}}) {
		t.Errorf("superseding told %v, committed %v, with the children %+v", told, committed, children)
	}
	told, children, committed, fail = nil, nil, nil, errors.New("no")
	mu.Unlock()
	// An error rolls back the end, and nothing is done after it.
	err = s.Tx(t.Context(), func(tx *Tx) error {
		return tx.EndAttempt(second.ID, AttemptEnd{Kind: EndLost, Reason: plan.ReasonWorkerLost})
	})
	mu.Lock()
	if err == nil || !slices.EqualFunc(told, [][]string{{second.ID}}, slices.Equal) || len(committed) > 0 {
		t.Errorf("a failed hook: %v, told %v, committed %v", err, told, committed)
	}
	told, fail = nil, nil
	mu.Unlock()
	inTx(t, s, func(tx *Tx) error {
		a, err := tx.Attempt(second.ID)
		if err == nil && a.State == AttemptEnded {
			t.Error("an attempt was ended though its hook failed")
		}
		return tx.EndAttempt(second.ID, AttemptEnd{Kind: EndLost, Reason: plan.ReasonWorkerLost})
	})
	mu.Lock()
	defer mu.Unlock()
	if !slices.EqualFunc(told, [][]string{{second.ID}}, slices.Equal) || !slices.Equal(committed, []string{second.ID}) {
		t.Errorf("ending told %v, committed %v", told, committed)
	}
}
