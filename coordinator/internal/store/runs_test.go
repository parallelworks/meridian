package store

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// testState is what a run's Start keeps as run state, for a test.
func testState() *compile.StartState {
	return &compile.StartState{
		Version:        1,
		Run:            plan.RunValues{Format: 1, Inputs: plan.DataOf(map[string]any{"region": "us"}), Secrets: []string{"inputs.pw"}},
		TimeoutSeconds: 60,
	}
}

// TestStartStateKept checks that the start state Run reads is decoded once
// a run, and kept, while the bytes the run holds are the ones it was
// decoded from: a Start whose transaction rolled back, and the next whose
// start state differs, has the next decoded.
func TestStartStateKept(t *testing.T) {
	t.Parallel()
	s := open(t)
	id := createRun(t, s)
	read := func() *compile.StartState {
		t.Helper()
		var r *Run
		if err := s.View(t.Context(), func(tx *Tx) error {
			var err error
			r, err = tx.Run(id)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return r.StartState
	}
	rolledBack := errors.New("rolled back")
	if err := s.Tx(t.Context(), func(tx *Tx) error {
		if err := tx.StartRun(id, testState(), nil); err != nil {
			return err
		}
		if r, err := tx.Run(id); err != nil || r.StartState.TimeoutSeconds != 60 {
			t.Errorf("the run started in the transaction is %+v, %v", r, err)
		}
		return rolledBack
	}); !errors.Is(err, rolledBack) {
		t.Fatal(err)
	}
	if st := read(); st != nil {
		t.Fatalf("a Start that rolled back left the start state %+v", st)
	}
	started := testState()
	started.TimeoutSeconds = 90
	inTx(t, s, func(tx *Tx) error { return tx.StartRun(id, started, nil) })
	first := read()
	if first == nil || first.TimeoutSeconds != 90 {
		t.Fatalf("the start state is %+v", first)
	}
	if again := read(); again != first {
		t.Error("the start state was decoded again")
	}
}

// TestStartsForgotten checks that the store keeps the start states of the
// runs read most recently, as many as its count and its bytes allow,
// forgetting the one read longest ago to keep another; and doesn't keep
// one larger than its bytes.
func TestStartsForgotten(t *testing.T) {
	t.Parallel()
	raw, err := compile.EncodeStartState(testState())
	if err != nil {
		t.Fatal(err)
	}
	decode := func(c *startCache, n int64) *compile.StartState {
		t.Helper()
		st, err := c.decode(n, raw)
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	if maxStarts < 1024 || maxStartBytes < 16<<20 {
		t.Errorf("the store keeps %d start states, and %d bytes of them, at most: too few for a tree of runs", maxStarts, maxStartBytes)
	}
	for name, c := range map[string]*startCache{
		"by count": {maxCount: 4},
		"by bytes": {maxBytes: 4 * int64(len(raw))},
	} {
		first, second := decode(c, 1), decode(c, 2)
		decode(c, 3)
		decode(c, 4)
		if again := decode(c, 1); again != first {
			t.Errorf("%s: a start state was forgotten before the store was full", name)
		}
		decode(c, 5)
		if c.runs.Len() != 4 || c.bytes != 4*int64(len(raw)) {
			t.Errorf("%s: the store keeps %d start states, of %d bytes", name, c.runs.Len(), c.bytes)
		}
		if again := decode(c, 1); again != first {
			t.Errorf("%s: the start state read last was forgotten", name)
		}
		if again := decode(c, 2); again == second {
			t.Errorf("%s: the start state read longest ago wasn't forgotten", name)
		}
	}
	c := &startCache{maxBytes: int64(len(raw)) - 1}
	decode(c, 1)
	if c.runs.Len() != 0 || c.bytes != 0 {
		t.Errorf("a start state larger than the store's bytes was kept: %d, of %d bytes", c.runs.Len(), c.bytes)
	}
}

// TestStatementsKept checks that a statement the store runs is prepared
// once, and kept for the transactions after, read-only or not.
func TestStatementsKept(t *testing.T) {
	t.Parallel()
	s := open(t)
	id := createRun(t, s)
	kept := func() int {
		s.stmtsMu.Lock()
		defer s.stmtsMu.Unlock()
		return len(s.stmts)
	}
	readRun := func(tx *Tx) error { _, err := tx.Run(id); return err }
	if err := s.View(t.Context(), readRun); err != nil {
		t.Fatal(err)
	}
	n := kept()
	if n == 0 {
		t.Fatal("no statement was kept")
	}
	if err := s.View(t.Context(), readRun); err != nil {
		t.Fatal(err)
	}
	inTx(t, s, readRun)
	if kept() != n {
		t.Errorf("reading the run again kept %d statements, then %d", n, kept())
	}
}

func TestRun(t *testing.T) {
	t.Parallel()
	s := open(t)
	p := testPlan(t)
	var before time.Time
	inTx(t, s, func(tx *Tx) error {
		var err error
		before, err = tx.Now()
		return err
	})
	id := createRun(t, s)
	if id != "1" {
		t.Errorf("the first run is %q, want 1", id)
	}
	if next := createRun(t, s); next != "2" {
		t.Errorf("the second run is %q, want 2", next)
	}
	wantPlan, err := plan.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest, err := plan.Digest(p)
	if err != nil {
		t.Fatal(err)
	}
	inTx(t, s, func(tx *Tx) error {
		r, err := tx.Run(id)
		if err != nil {
			return err
		}
		switch {
		case r.ID != id || r.State != RunCreated || r.Outcome != "" || !r.StartedAt.IsZero() || !r.EndedAt.IsZero():
			t.Errorf("run = %+v", r)
		case !bytes.Equal(r.Plan, wantPlan) || r.PlanVersion != 1 || r.PlanDigest != wantDigest:
			t.Errorf("the run's plan is version %d, digest %s, %d bytes; want 1, %s, %d", r.PlanVersion, r.PlanDigest, len(r.Plan), wantDigest, len(wantPlan))
		case r.Name != p.Run.Name || r.Location != "w.yaml" || r.Release != p.Run.Release:
			t.Errorf("the run's name, location and release are %q, %q, %q", r.Name, r.Location, r.Release)
		case r.DefaultGroup != "local" || r.RunDir != "/runs/1" || r.WorkflowID != fakeJobID(0) || r.RecordGeneration != 1:
			t.Errorf("run = %+v", r)
		case r.CreatedAt.Before(before):
			t.Errorf("the run was created at %v, before %v", r.CreatedAt, before)
		}
		if _, err := plan.Decode(r.Plan); err != nil {
			t.Errorf("the stored plan doesn't decode: %v", err)
		}
		return nil
	})

	// The plan never changes.
	err = s.Tx(t.Context(), func(tx *Tx) error {
		_, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_runs SET plan = x'00' WHERE id = 1")
		return err
	})
	if err == nil {
		t.Error("the run's plan changed")
	}

	for _, bad := range []string{"0", "01", "+1", "x", "", "3"} {
		inTx(t, s, func(tx *Tx) error {
			if _, err := tx.Run(bad); !errors.Is(err, ErrNotFound) {
				t.Errorf("Run(%q) = %v, want ErrNotFound", bad, err)
			}
			return nil
		})
	}
	checkIntegrity(t, s)
}

// TestRunIDs checks that a run's ID is one NewRunID gave, that a
// transaction rolled back gives its number back, and that a number
// committed is never given again.
func TestRunIDs(t *testing.T) {
	t.Parallel()
	s := open(t)
	p := testPlan(t)
	err := s.Tx(t.Context(), func(tx *Tx) error {
		return tx.InsertRun(NewRun{ID: "1", Plan: p, DefaultGroup: "local", RunDir: "/r", WorkflowID: fakeJobID(0)})
	})
	if err == nil {
		t.Error("InsertRun stored a run whose ID NewRunID didn't give")
	}
	_ = s.Tx(t.Context(), func(tx *Tx) error {
		if _, err := tx.NewRunID(); err != nil {
			return err
		}
		return errors.New("rolled back")
	})
	inTx(t, s, func(tx *Tx) error {
		id, err := tx.NewRunID()
		if id != "1" {
			t.Errorf("NewRunID after a rollback = %q (%v), want 1", id, err)
		}
		for _, r := range []NewRun{
			{ID: id, DefaultGroup: "local", RunDir: "/r", WorkflowID: fakeJobID(0)},
			{ID: id, Plan: p, RunDir: "/r", WorkflowID: fakeJobID(0)},
			{ID: id, Plan: p, DefaultGroup: "local", WorkflowID: fakeJobID(0)},
			{ID: id, Plan: p, DefaultGroup: "local", RunDir: "/r"},
			{ID: "01", Plan: p, DefaultGroup: "local", RunDir: "/r", WorkflowID: fakeJobID(0)},
		} {
			if err := tx.InsertRun(r); err == nil {
				t.Errorf("InsertRun(%+v) succeeded", r)
			}
		}
		return nil
	})
	// The number 1 was committed, though no run has it.
	if id := createRun(t, s); id != "2" {
		t.Errorf("the run after 1 was given is %s, want 2", id)
	}
}

func TestRunLifecycle(t *testing.T) {
	t.Parallel()
	s := open(t)
	id := createRun(t, s)
	warning := problems.UnknownKey.At("", problems.Position{}, "", problems.Params{"key": "x", "suggested": false, "suggestion": ""})
	inTx(t, s, func(tx *Tx) error {
		for i := 1; i <= 2; i++ {
			if tries, err := tx.StartTried(id); err != nil || tries != i {
				t.Errorf("StartTried = %d, %v; want %d", tries, err, i)
			}
		}
		return tx.StartRun(id, testState(), problems.Diagnostics{warning})
	})
	inTx(t, s, func(tx *Tx) error {
		r, err := tx.Run(id)
		if err != nil {
			return err
		}
		if r.State != RunRunning || r.StartedAt.IsZero() || r.StartTries != 2 {
			t.Errorf("a started run is %s, started at %v, after %d tries", r.State, r.StartedAt, r.StartTries)
		}
		want, err := compile.EncodeStartState(testState())
		if err != nil {
			return err
		}
		if got, err := compile.EncodeStartState(r.StartState); err != nil || string(got) != string(want) {
			t.Errorf("the run's start state is %s (%v), want %s", got, err, want)
		}
		if len(r.Diagnostics) != 1 || r.Diagnostics[0].Code != warning.Code || !reflect.DeepEqual(r.Diagnostics[0].Params["key"], "x") {
			t.Errorf("the run's diagnostics are %v", r.Diagnostics)
		}
		if err := tx.StartRun(id, testState(), nil); !errors.Is(err, ErrState) {
			t.Errorf("StartRun again = %v, want ErrState", err)
		}
		if _, err := tx.StartTried(id); !errors.Is(err, ErrState) {
			t.Errorf("StartTried of a started run = %v, want ErrState", err)
		}
		if err := tx.StartRun("9", testState(), nil); !errors.Is(err, ErrNotFound) {
			t.Errorf("StartRun of no run = %v, want ErrNotFound", err)
		}
		if err := tx.StartRun(id, nil, nil); err == nil {
			t.Error("StartRun without a start state succeeded")
		}
		// Diagnostics added go after those the run has.
		failed := problems.RunNotStarted.At("", problems.Position{}, "", problems.Params{"run": id})
		if err := tx.AddDiagnostics(id, problems.Diagnostics{failed}); err != nil {
			return err
		}
		if r, err := tx.Run(id); err != nil || len(r.Diagnostics) != 2 || r.Diagnostics[1].Code != failed.Code {
			t.Errorf("once added to, the run's diagnostics are %v (%v)", r.Diagnostics, err)
		}
		if err := tx.EndRun(id, plan.OutcomeSkipped, ""); err == nil {
			t.Error("EndRun skipped succeeded")
		}
		// A run with an attempt that hasn't ended doesn't end.
		a, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		if err := tx.EndRun(id, plan.OutcomeFailure, plan.ReasonTimeout); !errors.Is(err, ErrState) {
			t.Errorf("EndRun with an attempt that hasn't ended = %v, want ErrState", err)
		}
		return tx.EndAttempt(a.ID, AttemptEnd{Kind: EndWithdrawn})
	})
	inTx(t, s, func(tx *Tx) error { return tx.EndRun(id, plan.OutcomeFailure, plan.ReasonTimeout) })
	inTx(t, s, func(tx *Tx) error {
		r, err := tx.Run(id)
		if err != nil {
			return err
		}
		if r.State != RunEnded || r.Outcome != plan.OutcomeFailure || r.Reason != plan.ReasonTimeout || r.EndedAt.IsZero() {
			t.Errorf("an ended run is %+v", r)
		}
		// The record is deleted with the run's end, and the run takes no
		// other.
		if _, err := tx.Record(id); !errors.Is(err, ErrNotFound) {
			t.Errorf("Record of an ended run = %v, want ErrNotFound", err)
		}
		if err := tx.InsertRecord(id, 1, []byte("k"), []byte("s")); !errors.Is(err, ErrState) {
			t.Errorf("InsertRecord of an ended run = %v, want ErrState", err)
		}
		if err := tx.EndRun(id, plan.OutcomeSuccess, ""); !errors.Is(err, ErrState) {
			t.Errorf("EndRun again = %v, want ErrState", err)
		}
		if err := tx.InsertJob(NewJob{Run: id, Name: "build", Key: "build#0", SupervisorID: fakeJobID(9)}); !errors.Is(err, ErrState) {
			t.Errorf("InsertJob of an ended run = %v, want ErrState", err)
		}
		if _, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "test", LeaseSeconds: 60, HeartbeatSeconds: 10}); !errors.Is(err, ErrState) {
			t.Errorf("NewAttempt of an ended run = %v, want ErrState", err)
		}
		return nil
	})
	checkIntegrity(t, s)
}

// TestRecord checks that a run's secret record and its generation, which
// the run's row keeps, change together, or neither does.
func TestRecord(t *testing.T) {
	t.Parallel()
	s := open(t)
	id := createRun(t, s)
	check := func(gen uint64, sealed string) {
		t.Helper()
		inTx(t, s, func(tx *Tx) error {
			rec, err := tx.Record(id)
			if err != nil {
				return err
			}
			r, err := tx.Run(id)
			if err != nil {
				return err
			}
			if rec.Generation != gen || r.RecordGeneration != gen || string(rec.Sealed) != sealed || string(rec.WrappedKey) != "wrapped "+id {
				t.Errorf("the record is generation %d (the run says %d), %q under %q; want %d, %q", rec.Generation, r.RecordGeneration, rec.Sealed, rec.WrappedKey, gen, sealed)
			}
			if rec.Run != id || rec.UpdatedAt.IsZero() {
				t.Errorf("record = %+v", rec)
			}
			return nil
		})
	}
	check(1, "sealed 1 1")

	inTx(t, s, func(tx *Tx) error { return tx.Reseal(id, 2, []byte("sealed 1 2")) })
	check(2, "sealed 1 2")

	// A generation that isn't the next changes neither.
	for _, gen := range []uint64{2, 4, 1, 0} {
		inTx(t, s, func(tx *Tx) error {
			if err := tx.Reseal(id, gen, []byte("sealed 1 x")); !errors.Is(err, ErrState) {
				t.Errorf("Reseal with generation %d = %v, want ErrState", gen, err)
			}
			return nil
		})
	}
	check(2, "sealed 1 2")

	// A reseal rolled back changes neither.
	_ = s.Tx(t.Context(), func(tx *Tx) error {
		if err := tx.Reseal(id, 3, []byte("sealed 1 3")); err != nil {
			t.Error(err)
		}
		return errors.New("rolled back")
	})
	check(2, "sealed 1 2")

	inTx(t, s, func(tx *Tx) error {
		if err := tx.InsertRecord(id, 1, []byte("k"), []byte("s")); !errors.Is(err, ErrState) {
			t.Errorf("InsertRecord again = %v, want ErrState", err)
		}
		if err := tx.Reseal("9", 2, []byte("s")); !errors.Is(err, ErrNotFound) {
			t.Errorf("Reseal of no run = %v, want ErrNotFound", err)
		}
		if err := tx.InsertRecord("9", 1, []byte("k"), []byte("s")); !errors.Is(err, ErrNotFound) {
			t.Errorf("InsertRecord of no run = %v, want ErrNotFound", err)
		}
		for _, r := range []struct {
			gen         uint64
			key, sealed string
		}{{2, "k", "s"}, {0, "k", "s"}, {1, "", "s"}, {1, "k", ""}} {
			if err := tx.InsertRecord("9", r.gen, []byte(r.key), []byte(r.sealed)); err == nil || errors.Is(err, ErrNotFound) {
				t.Errorf("InsertRecord(%+v) = %v, want the caller's mistake", r, err)
			}
		}
		return nil
	})

	// The data key never changes.
	err := s.Tx(t.Context(), func(tx *Tx) error {
		_, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_records SET wrapped_key = x'00'")
		return err
	})
	if err == nil {
		t.Error("the record's data key changed")
	}
	check(2, "sealed 1 2")
}

// TestMalformedRun checks that a run's start state and diagnostics are
// read only as the store writes them.
func TestMalformedRun(t *testing.T) {
	t.Parallel()
	s := open(t)
	id := createRun(t, s)
	for _, set := range []string{
		`start_state = CAST('{"version":1,"run":{"format":1},"later":1}' AS BLOB)`,
		`start_state = CAST('{"version":1}' AS BLOB)`,
		`diagnostics = CAST('[{"type":"x","code":"c","later":1}]' AS BLOB)`,
		`diagnostics = CAST('[{"type":"x"}]' AS BLOB)`,
		`diagnostics = CAST('not JSON' AS BLOB)`,
	} {
		inTx(t, s, func(tx *Tx) error {
			_, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_runs SET start_state = NULL, diagnostics = NULL, "+set)
			return err
		})
		inTx(t, s, func(tx *Tx) error {
			if _, err := tx.Run(id); !errors.Is(err, ErrMalformed) {
				t.Errorf("Run with %s = %v, want ErrMalformed", set, err)
			}
			return nil
		})
	}

	// A start state of a version a newer release writes is that release's,
	// not malformed.
	inTx(t, s, func(tx *Tx) error {
		_, err := tx.tx.ExecContext(tx.ctx, `UPDATE meridian_runs SET diagnostics = NULL, start_state = CAST('{"version":3,"later":1}' AS BLOB)`)
		return err
	})
	inTx(t, s, func(tx *Tx) error {
		if _, err := tx.Run(id); !errors.Is(err, plan.ErrVersion) || errors.Is(err, ErrMalformed) {
			t.Errorf("Run with a start state of version 3 = %v, want plan.ErrVersion", err)
		}
		return nil
	})
}

// A job's part of a run's secret record is stored at the next generation
// only, under the run's data key, which WrappedKey gives without the
// record; JobRecords gives the parts of the jobs that have one; and the
// run's end deletes them.
func TestJobRecords(t *testing.T) {
	t.Parallel()
	s := open(t)
	id := createRun(t, s)
	parts := func() map[string]JobRecord {
		t.Helper()
		var got map[string]JobRecord
		inTx(t, s, func(tx *Tx) error {
			var err error
			got, err = tx.JobRecords(id, []string{"build", "test", "nothing"})
			return err
		})
		return got
	}
	if got := parts(); len(got) != 0 {
		t.Errorf("before any part, JobRecords = %v", got)
	}
	inTx(t, s, func(tx *Tx) error {
		if key, err := tx.WrappedKey(id); err != nil || string(key) != "wrapped "+id {
			t.Errorf("WrappedKey = %q, %v", key, err)
		}
		if _, err := tx.WrappedKey("9"); !errors.Is(err, ErrNotFound) {
			t.Errorf("WrappedKey of no run = %v, want ErrNotFound", err)
		}
		return tx.PutJobRecord(id, "build", 1, []byte("build 1"))
	})
	inTx(t, s, func(tx *Tx) error { return tx.PutJobRecord(id, "build", 2, []byte("build 2")) })
	if got := parts(); len(got) != 1 || got["build"].Generation != 2 || string(got["build"].Sealed) != "build 2" {
		t.Errorf("JobRecords = %v, want build's at generation 2", got)
	}

	// A generation that isn't the next changes neither the part nor its
	// generation, and nor does a write rolled back.
	inTx(t, s, func(tx *Tx) error {
		for _, gen := range []uint64{0, 1, 2, 4} {
			if err := tx.PutJobRecord(id, "build", gen, []byte("build x")); !errors.Is(err, ErrState) || !errors.Is(err, ErrRecordChanged) {
				t.Errorf("PutJobRecord with generation %d = %v, want ErrState and ErrRecordChanged", gen, err)
			}
		}
		if err := tx.PutJobRecord(id, "nothing", 1, []byte("x")); !errors.Is(err, ErrNotFound) {
			t.Errorf("PutJobRecord of no job = %v, want ErrNotFound", err)
		}
		if err := tx.PutJobRecord("9", "build", 1, []byte("x")); !errors.Is(err, ErrNotFound) {
			t.Errorf("PutJobRecord of no run = %v, want ErrNotFound", err)
		}
		if err := tx.PutJobRecord(id, "build", 3, nil); err == nil {
			t.Error("PutJobRecord stored a part without its bytes")
		}
		return nil
	})
	_ = s.Tx(t.Context(), func(tx *Tx) error {
		if err := tx.PutJobRecord(id, "build", 3, []byte("build 3")); err != nil {
			t.Error(err)
		}
		return errors.New("rolled back")
	})
	if got := parts(); got["build"].Generation != 2 || string(got["build"].Sealed) != "build 2" {
		t.Errorf("after a write rolled back, JobRecords = %v", got)
	}
	// The parts are the record's own: the run's record keeps its generation.
	inTx(t, s, func(tx *Tx) error {
		if rec, err := tx.Record(id); err != nil || rec.Generation != 1 {
			t.Errorf("the run's record is %+v, %v; want generation 1", rec, err)
		}
		return tx.EndRun(id, plan.OutcomeSuccess, "")
	})
	if got := parts(); len(got) != 0 {
		t.Errorf("once the run has ended, JobRecords = %v", got)
	}
	inTx(t, s, func(tx *Tx) error {
		if err := tx.PutJobRecord(id, "build", 3, []byte("build 3")); !errors.Is(err, ErrState) {
			t.Errorf("PutJobRecord once the run has ended = %v, want ErrState", err)
		}
		return nil
	})
}
