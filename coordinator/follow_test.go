package coordinator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/coordinator/internal/runlog/runlogtest"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// followed is what Follow reported, in the order it did: each event, and
// each part of a log.
type followed struct {
	mu    sync.Mutex
	items []any
}

// options are the options of a Follow that reports to f, with logs from
// logs, unless it is nil.
func (f *followed) options(logs *runlogtest.Logs) FollowOptions {
	opts := FollowOptions{Event: func(e Event) error { f.add(e); return nil }}
	if logs != nil {
		opts.Logs, opts.Log = logs, func(p LogPart) error { f.add(p); return nil }
	}
	return opts
}

func (f *followed) add(item any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items = append(f.items, item)
}

// events returns the events f has, in order.
func (f *followed) events() []Event {
	var out []Event
	for _, item := range f.items {
		if e, ok := item.(Event); ok {
			out = append(out, e)
		}
	}
	return out
}

// logs returns the logs f has, each whole, by job, attempt and which of
// the attempt's logs it is, and how it ended: "ended", "cut", or "".
func (f *followed) logs() map[string]string {
	out := map[string]string{}
	for _, item := range f.items {
		if p, ok := item.(LogPart); ok {
			name := logName(p.Key)
			out[name] += string(p.Data)
			switch {
			case p.Ended:
				out[name+" end"] += "ended"
			case p.Cut:
				out[name+" end"] += "cut"
			}
		}
	}
	return out
}

func logName(k LogKey) string {
	return fmt.Sprintf("%s/%d %s %d.%d", k.Job, k.Attempt, k.Part, k.Step, k.Try)
}

// caseNamed returns the conformance suite's case named name.
func caseNamed(t *testing.T, name string) conformance.Case {
	t.Helper()
	cases, err := conformance.Cases("runs")
	if err != nil {
		t.Fatal(err)
	}
	for _, cs := range cases {
		if cs.Name == name {
			return cs
		}
	}
	t.Fatalf("no case %s", name)
	return conformance.Case{}
}

// Follow reports a run's events as it runs, from its first, in order, each
// once, and its logs as they are stored, each whole and once, before the
// event that ends it; then the run's end, and how it ended. Followed again,
// once it has ended, it reports the same.
func TestFollow(t *testing.T) {
	r, id := startCase(t, caseNamed(t, "runs/cleanup"))
	logs := r.logs
	var live followed
	status, err := r.c.Follow(t.Context(), id, live.options(logs))
	if err != nil {
		t.Fatal(err)
	}
	if status.State != RunEnded || status.Outcome != plan.OutcomeFailure {
		t.Errorf("Follow ended with %+v", status)
	}
	events := live.events()
	var kinds []string
	for i, e := range events {
		if e.Seq != int64(i+1) || e.Run != id || e.Time.IsZero() {
			t.Errorf("event %d is %+v", i, e)
		}
		k := string(e.Kind)
		if e.Kind == StepStarted || e.Kind == StepFinished {
			k += fmt.Sprintf(" %d", e.Step)
			if e.Cleanup {
				k += " cleanup"
			}
		}
		kinds = append(kinds, k)
	}
	want := []string{
		"run.started", "job.ready", "job.placing", "job.running",
		"step.started 0", "step.finished 0", "step.started 1", "step.finished 1",
		// The step after the one that failed, and its cleanup, don't run.
		"step.finished 2", "step.finished 2 cleanup",
		"step.started 1 cleanup", "step.finished 1 cleanup", "step.started 0 cleanup", "step.finished 0 cleanup",
		"attempt.ended", "job.finished", "run.finished",
	}
	if !slices.Equal(kinds, want) {
		t.Errorf("the events are\n%q\nwant\n%q", kinds, want)
	}
	for _, e := range events {
		switch {
		case e.Kind == JobPlacing && e.Member != "local/m1":
			t.Errorf("the job was placed on %q", e.Member)
		case e.Kind == StepFinished && !e.Cleanup && e.Step == 1 && (e.Outcome != plan.OutcomeFailure || e.ExitCode != 1):
			t.Errorf("the failing step finished as %+v", e)
		case e.Kind == AttemptEnded && (e.End != AttemptCompleted || e.Outcome != plan.OutcomeFailure):
			t.Errorf("the attempt ended as %+v", e)
		case e.Kind == JobFinished && e.Outcome != plan.OutcomeFailure:
			t.Errorf("the job finished as %+v", e)
		}
	}
	got := live.logs()
	for name, text := range map[string]string{
		"main/1 step 0.0": "", "main/1 step 1.0": "", "main/1 cleanup 0.0": "cancel 42\n", "main/1 cleanup 1.0": "failed=true\n",
	} {
		if got[name] != text || got[name+" end"] != "ended" {
			t.Errorf("the log %s is %q, %q; want %q, ended once", name, got[name], got[name+" end"], text)
		}
	}
	if got["main/1 setup 0.0 end"] != "ended" {
		t.Errorf("the setup log ended %q", got["main/1 setup 0.0 end"])
	}
	// Each log part is reported after the event that began its log, and
	// before the run's end.
	began := map[string]bool{}
	for _, item := range live.items {
		switch item := item.(type) {
		case Event:
			key := LogKey{Job: item.Job, Attempt: item.Attempt, Step: item.Step, Try: item.Try}
			switch {
			case item.Kind == JobRunning:
				key.Part = workerapi.LogSetup
			case item.Kind == StepStarted && item.Cleanup:
				key.Part = workerapi.LogCleanup
			case item.Kind == StepStarted:
				key.Part = workerapi.LogStep
			}
			began[logName(key)] = true
		case LogPart:
			k := item.Key
			k.Run = ""
			if !began[logName(k)] {
				t.Errorf("a part of the log %s came before the event that began it", logName(k))
			}
		}
	}
	if _, ok := live.items[len(live.items)-1].(Event); !ok {
		t.Error("the run's end isn't the last thing Follow reported")
	}

	var again followed
	if _, err := r.c.Follow(t.Context(), id, again.options(logs)); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(again.events()) != fmt.Sprint(events) || fmt.Sprint(again.logs()) != fmt.Sprint(got) {
		t.Error("followed again once the run had ended, the run reads otherwise")
	}
}

// A log whose attempt ended before the log did, as one whose worker was
// lost, is reported as far as it was stored, and then cut, once the run
// has ended; and Follow refuses a run the store doesn't have, and options
// it can't follow with.
func TestFollowCut(t *testing.T) {
	c := open(t, Options{})
	id, _ := create(t, c, oneJob, nil, nil)
	logs := &runlogtest.Logs{}
	step := LogKey{Run: id, Job: "main", Attempt: 1, Part: workerapi.LogStep}
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		if err := tx.StartRun(id, &compile.StartState{Run: plan.RunValues{Format: 1}}, nil); err != nil {
			return err
		}
		if _, err := tx.PinReady(id, "main", false, nil); err != nil {
			return err
		}
		a, token, err := tx.NewAttempt(store.NewAttempt{Run: id, Job: "main", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		if err := tx.ChoosePlace(a.ID, store.Place{Group: "local", Member: "m1", Backend: testBackend}); err != nil {
			return err
		}
		if err := tx.RecordPlacement(a.ID, store.Placement{Group: "local", Member: "m1", Backend: testBackend, ID: "p", WorkerDigest: testDigest}); err != nil {
			return err
		}
		if _, err := tx.Claim(token, strings.Repeat("c", 64), workerapi.Worker{Version: "test", Digest: testDigest}); err != nil {
			return err
		}
		if _, err := tx.AppendEvents(a.ID, []workerapi.Event{{Seq: 1, Time: time.Unix(1, 0).UTC()}}); err != nil {
			return err
		}
		if _, _, err := logs.Append(t.Context(), step, 0, []byte("partial"), false); err != nil {
			return err
		}
		if err := tx.EndAttempt(a.ID, store.AttemptEnd{Kind: store.EndLost, Reason: plan.ReasonWorkerLost}); err != nil {
			return err
		}
		if err := tx.EndJob(id, "main", store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Reason: plan.ReasonWorkerLost}); err != nil {
			return err
		}
		return tx.EndRun(id, plan.OutcomeFailure, "")
	}); err != nil {
		t.Fatal(err)
	}
	var f followed
	if _, err := c.Follow(t.Context(), id, f.options(logs)); err != nil {
		t.Fatal(err)
	}
	got := f.logs()
	if got["main/1 step 0.0"] != "partial" || got["main/1 step 0.0 end"] != "cut" || got["main/1 setup 0.0 end"] != "cut" {
		t.Errorf("the logs are %q", got)
	}
	events := f.events()
	if e := events[len(events)-3]; e.Kind != AttemptEnded || e.End != AttemptLost || e.Reason != plan.ReasonWorkerLost {
		t.Errorf("the attempt ended as %+v", e)
	}

	if _, err := c.Follow(t.Context(), "99", f.options(nil)); !errors.Is(err, ErrRunNotFound) {
		t.Errorf("Follow of no run = %v, want ErrRunNotFound", err)
	}
	for _, opts := range []FollowOptions{{}, {Event: func(Event) error { return nil }, Logs: logs}} {
		if _, err := c.Follow(t.Context(), id, opts); err == nil {
			t.Errorf("Follow with %+v followed", opts)
		}
	}
	stop := errors.New("stop")
	if _, err := c.Follow(t.Context(), id, FollowOptions{Event: func(Event) error { return stop }}); !errors.Is(err, stop) {
		t.Errorf("Follow whose Event failed = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Follow(ctx, id, f.options(nil)); !errors.Is(err, context.Canceled) {
		t.Errorf("Follow with its context done = %v", err)
	}
}
