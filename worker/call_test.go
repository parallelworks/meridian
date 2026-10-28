package worker

import (
	"errors"
	"io/fs"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// calling returns a step that calls the workflow tools, with with, and $yaml
// w.yaml: the plan pinned it.
func calling(with map[string]any) plan.Step {
	w := map[string]any{"$yaml": "w.yaml"}
	maps.Copy(w, with)
	return plan.Step{
		Uses: "github/acme/tools@main", With: plan.Literal(w),
		Source: &plan.Source{Location: "github.com/acme/tools/w.yaml", Revision: "0123", Ref: plan.SourceRef{Uses: "github/acme/tools@main", Path: "w.yaml"}},
	}
}

// ends answers each ask for a child, made, as ending with the outcome
// outcomes gives its step's try, by step and try, as "0.1": it ends it
// once the ask has been answered, so that the worker waits for its end.
// A try outcomes doesn't name runs until something else ends it.
func ends(f *workerapitest.Fake, outcomes map[string]plan.Outcome) func(req workerapi.ChildRequest) *workerapi.Child {
	return func(req workerapi.ChildRequest) *workerapi.Child {
		if outcome, ok := outcomes[strconv.Itoa(req.Step)+"."+strconv.Itoa(req.Try)]; ok {
			go f.EndChild(req.Step, req.Try, outcome)
		}
		return nil
	}
}

// TestCall checks that a step that calls a workflow asks the coordinator
// for its child, once for each try, with exactly what childRequest builds,
// of Step's with: and env and the worker's variables, never the worker's
// own environment or the platform's credential, and with what the worker
// masks; waits for its end; and ends as format 1 maps it: a success
// succeeds; a failure fails the try, which its retry: tries again; a child
// cancelled by itself ends the step cancelled, but its job goes on, the
// step counting as a success for it; a child the coordinator didn't make
// fails the step with the coordinator's problems, in its log too; and the
// step writes no outputs and no log of its own.
func TestCall(t *testing.T) {
	t.Parallel()
	ok := calling(map[string]any{"region": "east"})
	ok.Env = map[string]plan.Value{"FROM_STEP": plan.Literal("step")}
	cancelled := calling(nil)
	retried := calling(nil)
	retried.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: plan.Literal("0s")}
	notMade := calling(nil)
	notMade.If = cond("always()")
	job := newJob(ok, cancelled, command("echo after"), retried, notMade)
	job.Masked = []string{"masked-by-the-job"}
	f := workerapitest.NewFake(t, job)
	f.Claim.Env = map[string]workerapi.EnvVar{"PW_API_KEY": {Value: "k3y-of-the-platform", Secret: true}, "PW_USER": {Value: "u"}}
	notCreated := problems.ChildNotCreated.At("", problems.Position{}, "/jobs/build/steps/4/uses", nil)
	made := ends(f, map[string]plan.Outcome{"0.0": plan.OutcomeSuccess, "1.0": plan.OutcomeCancelled, "3.0": plan.OutcomeFailure, "3.1": plan.OutcomeFailure})
	f.Child = func(req workerapi.ChildRequest) *workerapi.Child {
		if req.Step == 4 {
			return &workerapi.Child{Ended: true, Outcome: plan.OutcomeFailure, Diagnostics: problems.Diagnostics{notCreated}}
		}
		return made(req)
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	// The step whose child was cancelled by itself doesn't stop the job,
	// whose next step runs, as after a success; the one whose child failed
	// fails it.
	checkEvents(t, f,
		"started 0.0", "finished 0.0 success -1",
		"started 1.0", "finished 1.0 cancelled -1",
		"started 2.0", "finished 2.0 success 0",
		"started 3.0", "finished 3.0 failure -1", "started 3.1", "finished 3.1 failure -1",
		"started 4.0", "finished 4.0 failure -1",
	)
	checkEnd(t, f, plan.OutcomeFailure, "")
	if e := finishedEvent(t, f, 4, 0, false); !slices.Equal(codes(e.Diagnostics), []string{"child_not_created"}) {
		t.Errorf("the step whose child wasn't made has the diagnostics %v", e.Diagnostics)
	}
	checkLog(t, f, logKey{workerapi.LogStep, 4, 0}, notCreated.Message()+"\n")
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "")
	if e := finishedEvent(t, f, 0, 0, false); len(e.Outputs) > 0 {
		t.Errorf("a step that calls a workflow wrote outputs: %v", e.Outputs)
	}

	asks := f.ChildAsks()
	var tries []string
	for _, a := range asks {
		tries = append(tries, strconv.Itoa(a.Step)+"."+strconv.Itoa(a.Try))
	}
	if want := []string{"0.0", "1.0", "3.0", "3.1", "4.0"}; !slices.Equal(tries, want) {
		t.Fatalf("the worker asked for the children of %v, want %v", tries, want)
	}
	// The request as sent: Step's env, with the worker's format 1
	// variables over it, and nothing of the worker's own environment, nor
	// the platform's credential.
	first := asks[0]
	with, err := workerapi.Marshal(first.With)
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case string(with) != `{"$yaml":"w.yaml","region":"east"}`:
		t.Errorf("the request's with: is %s", with)
	case first.Env["FROM_STEP"] != "step" || first.Env["PW_JOB_DIR"] != f.Claim.RunDir || first.Env["PW_USER"] != "u" || first.Env["OUTPUTS"] == "":
		t.Errorf("the request's env is %v", first.Env)
	case first.Env["PW_API_KEY"] != "" || first.Env["FROM_WORKER"] != "" || first.Env["PATH"] != "":
		t.Errorf("the request's env holds what isn't Step's nor a format 1 variable: %v", first.Env)
	case !slices.Contains(first.Masked, "masked-by-the-job") || !slices.Contains(first.Masked, "k3y-of-the-platform"):
		t.Errorf("the request masks %q", first.Masked)
	}
	if _, has := first.Env["PW_WORKFLOW_STEP_CURRENT_RETRY"]; has || asks[2].Env["PW_WORKFLOW_STEP_CURRENT_RETRY"] != "0" || asks[3].Env["PW_WORKFLOW_STEP_CURRENT_RETRY"] != "1" {
		t.Errorf("the retry counters sent are %q, %q and %q", first.Env["PW_WORKFLOW_STEP_CURRENT_RETRY"],
			asks[2].Env["PW_WORKFLOW_STEP_CURRENT_RETRY"], asks[3].Env["PW_WORKFLOW_STEP_CURRENT_RETRY"])
	}
	// Each child's directory is made in the run's, where the coordinator
	// puts it, readable by the user alone.
	for _, step := range []string{"step_0", "step_1", "step_3", "step_4"} {
		fi, err := os.Lstat(filepath.Join(f.Claim.RunDir, "subworkflows", "build", step))
		if err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o700 {
			t.Errorf("the directory of the child of %s: %v, %v", step, fi, err)
		}
	}
}

// TestCallStopped checks that a step that calls a workflow stops its child
// as the step is stopped, once, and waits for the child's end, and ends as
// format 1 maps a stopped call, whatever the child's outcome: the job's
// cancellation stops it, cancelled, and cancels the job; its early-cancel:,
// cancelled, and the timeout: of a step without a retry:, timeout, end it
// cancelled while the job goes on; its retry:'s timeout, timeout, ends a
// try as a command's that ran out of time: tried again, and the last
// failed, or a success if it ignores its errors; the timeout: of a step
// with a retry: fails it, not tried again; and the job's timeout: stops it,
// timeout, and ends it and the job cancelled, for the reason timeout,
// rather than failed.
func TestCallStopped(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// step and job make the step that calls the workflow, and its job.
		step func(s *plan.Step)
		job  func(j *plan.ReadyJob)
		// control is what the coordinator asks once the child is made.
		control *workerapi.Control
		stops   []workerapi.ChildStop
		events  []string
		// timedOut are the tries, as "0.1", whose event says the step ran
		// out of time (step_timed_out).
		timedOut []string
		outcome  plan.Outcome
		reason   plan.Reason
	}{
		{
			name:    "the job's cancel",
			control: &workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled},
			stops:   []workerapi.ChildStop{{Reason: plan.ReasonCancelled}},
			events:  []string{"started 0.0", "finished 0.0 cancelled -1", "finished 1.0 skipped -1"},
			outcome: plan.OutcomeCancelled, reason: plan.ReasonCancelled,
		},
		{
			name:    "its early-cancel:",
			step:    func(s *plan.Step) { s.EarlyCancel = plan.EarlyCancelAnyJobFailed },
			control: &workerapi.Control{AnyJobFailed: true},
			stops:   []workerapi.ChildStop{{Reason: plan.ReasonCancelled}},
			events:  []string{"started 0.0", "finished 0.0 cancelled -1", "started 1.0", "finished 1.0 success 0"},
			outcome: plan.OutcomeSuccess,
		},
		{
			name:    "its timeout:",
			step:    func(s *plan.Step) { s.Timeout = plan.Literal("1s") },
			stops:   []workerapi.ChildStop{{Reason: plan.ReasonTimeout}},
			events:  []string{"started 0.0", "finished 0.0 cancelled -1", "started 1.0", "finished 1.0 success 0"},
			outcome: plan.OutcomeSuccess,
		},
		{
			name: "its retry:'s timeout",
			step: func(s *plan.Step) {
				s.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: plan.Literal("0s"), Timeout: plan.Literal("1s")}
			},
			stops: []workerapi.ChildStop{{Reason: plan.ReasonTimeout}, {Try: 1, Reason: plan.ReasonTimeout}},
			events: []string{
				"started 0.0", "finished 0.0 failure -1", "started 0.1", "finished 0.1 failure -1",
				"finished 1.0 skipped -1",
			},
			timedOut: []string{"0.0", "0.1"},
			outcome:  plan.OutcomeFailure,
		},
		{
			name: "its retry:'s timeout, its errors ignored",
			step: func(s *plan.Step) {
				s.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: plan.Literal("0s"), Timeout: plan.Literal("1s")}
				s.IgnoreErrors = plan.Literal(true)
			},
			stops: []workerapi.ChildStop{{Reason: plan.ReasonTimeout}, {Try: 1, Reason: plan.ReasonTimeout}},
			events: []string{
				"started 0.0", "finished 0.0 failure -1", "started 0.1", "finished 0.1 success -1",
				"started 1.0", "finished 1.0 success 0",
			},
			timedOut: []string{"0.0"},
			outcome:  plan.OutcomeSuccess,
		},
		{
			name: "its timeout:, with a retry:",
			step: func(s *plan.Step) {
				s.Timeout = plan.Literal("1s")
				s.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: plan.Literal("0s")}
			},
			stops:    []workerapi.ChildStop{{Reason: plan.ReasonTimeout}},
			events:   []string{"started 0.0", "finished 0.0 failure -1", "finished 1.0 skipped -1"},
			timedOut: []string{"0.0"},
			outcome:  plan.OutcomeFailure,
		},
		{
			name:    "the job's timeout:",
			job:     func(j *plan.ReadyJob) { j.TimeoutSeconds = 1 },
			stops:   []workerapi.ChildStop{{Reason: plan.ReasonTimeout}},
			events:  []string{"started 0.0", "finished 0.0 cancelled -1", "finished 1.0 skipped -1"},
			outcome: plan.OutcomeCancelled, reason: plan.ReasonTimeout,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			step := calling(nil)
			if tc.step != nil {
				tc.step(&step)
			}
			job := newJob(step, command("echo after"))
			if tc.job != nil {
				tc.job(job)
			}
			f := workerapitest.NewFake(t, job)
			f.Child = func(workerapi.ChildRequest) *workerapi.Child {
				if tc.control != nil {
					go f.SetControl(*tc.control)
				}
				return nil
			}
			// The child ends failed, as a run whose stop failed its jobs
			// does: its step ends as its stop says all the same.
			f.OnChildStop = func(workerapi.ChildStop) plan.Outcome { return plan.OutcomeFailure }
			if err := runWorker(t, f, t.Context(), Config{}); err != nil {
				t.Fatal(err)
			}
			checkEvents(t, f, tc.events...)
			checkEnd(t, f, tc.outcome, tc.reason)
			if got := f.Stored().ChildStops; !slices.Equal(got, tc.stops) {
				t.Errorf("the child's stops are %+v, want %+v", got, tc.stops)
			}
			var timedOut []string
			for _, e := range f.Events() {
				if e.Finished && slices.Contains(codes(e.Diagnostics), "step_timed_out") {
					timedOut = append(timedOut, strconv.Itoa(e.Step)+"."+strconv.Itoa(e.Try))
				}
			}
			if !slices.Equal(timedOut, tc.timedOut) {
				t.Errorf("the tries that ran out of time are %v, want %v", timedOut, tc.timedOut)
			}
		})
	}
}

// TestCallStoppedOnce checks that a child is stopped once, for the first of
// the try's stops, though another comes after that asks for SIGKILL
// sooner, as a step's timeout: after a cancel with a long grace does.
func TestCallStoppedOnce(t *testing.T) {
	t.Parallel()
	step := calling(nil)
	step.Timeout = plan.Literal("1s")
	f := workerapitest.NewFake(t, newJob(step))
	f.Child = func(workerapi.ChildRequest) *workerapi.Child {
		go f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 60})
		return nil
	}
	// The child runs on past the step's timeout:, as one whose cleanups
	// take a while does.
	f.OnChildStop = func(stop workerapi.ChildStop) plan.Outcome {
		go func() {
			<-time.After(2 * time.Second)
			f.EndChild(stop.Step, stop.Try, plan.OutcomeCancelled)
		}()
		return ""
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	if got, want := f.Stored().ChildStops, []workerapi.ChildStop{{Reason: plan.ReasonCancelled}}; !slices.Equal(got, want) {
		t.Errorf("the child's stops are %+v, want %+v", got, want)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 cancelled -1")
	checkEnd(t, f, plan.OutcomeCancelled, plan.ReasonCancelled)
}

// TestCallEndUnread checks that a step whose poll for its child's end fails
// for good fails, with child_request_failed, and stops its child first,
// cancelled, so that it doesn't outlive the step.
func TestCallEndUnread(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(calling(nil)))
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Path == workerapi.PathChildEnd {
			return workerapitest.BadRequest
		}
		return workerapitest.Answer
	}
	f.Lenient = true
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 failure -1")
	if e := finishedEvent(t, f, 0, 0, false); !slices.Equal(codes(e.Diagnostics), []string{"child_request_failed"}) {
		t.Errorf("the step whose child's end couldn't be read has the diagnostics %v", e.Diagnostics)
	}
	if got, want := f.Stored().ChildStops, []workerapi.ChildStop{{Reason: plan.ReasonCancelled}}; !slices.Equal(got, want) {
		t.Errorf("the child's stops are %+v, want %+v", got, want)
	}
}

// TestCallTimedFromItsStart checks that a step's timeout: counts from its
// try's start, as a command's does, not from when its child was made: the
// child of a step whose timeout passed as it was asked for is stopped as
// soon as it is made.
func TestCallTimedFromItsStart(t *testing.T) {
	t.Parallel()
	step := calling(nil)
	step.Timeout = plan.Literal("1s")
	f := workerapitest.NewFake(t, newJob(step))
	var (
		mu            sync.Mutex
		made, stopped time.Time
	)
	f.Child = func(workerapi.ChildRequest) *workerapi.Child {
		// The coordinator takes a while to make it.
		<-time.After(1500 * time.Millisecond)
		mu.Lock()
		made = time.Now()
		mu.Unlock()
		return nil
	}
	f.OnChildStop = func(workerapi.ChildStop) plan.Outcome {
		mu.Lock()
		stopped = time.Now()
		mu.Unlock()
		return plan.OutcomeCancelled
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if stopped.IsZero() || stopped.Sub(made) > 700*time.Millisecond {
		t.Errorf("the child was made at %v, and stopped %v after", made, stopped.Sub(made))
	}
}

// TestCallNotMadeCancelled checks that a step whose child the coordinator
// didn't make, answering cancelled, ends as the stop it stands for: with a
// reason, the job's cancel, which the worker heeds from then on as though
// it had read the control; without one, the step's early-cancel:, which
// ends it cancelled while its job goes on.
func TestCallNotMadeCancelled(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		reason  plan.Reason
		events  []string
		outcome plan.Outcome
	}{
		{plan.ReasonFailFast, []string{"started 0.0", "finished 0.0 cancelled -1", "finished 1.0 skipped -1"}, plan.OutcomeCancelled},
		{"", []string{"started 0.0", "finished 0.0 cancelled -1", "started 1.0", "finished 1.0 success 0"}, plan.OutcomeSuccess},
	} {
		f := workerapitest.NewFake(t, newJob(calling(nil), command("echo after")))
		f.Child = func(workerapi.ChildRequest) *workerapi.Child {
			return &workerapi.Child{Ended: true, Outcome: plan.OutcomeCancelled, Reason: tc.reason}
		}
		if err := runWorker(t, f, t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		checkEvents(t, f, tc.events...)
		checkEnd(t, f, tc.outcome, tc.reason)
	}
}

// TestCallDir checks that a step that calls a workflow cleans its child's
// directory before each try after the first, as its clean-on-retry: says,
// and that one whose directory a symbolic link is in the way of fails,
// without asking for its child, and with nothing made or removed where
// the link points.
func TestCallDir(t *testing.T) {
	t.Parallel()
	retried := calling(nil)
	retried.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: plan.Literal("0s"), CleanOnRetry: plan.Literal(true)}
	kept := calling(nil)
	kept.If = cond("always()")
	kept.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: plan.Literal("0s"), CleanOnRetry: plan.Literal(false)}
	f := workerapitest.NewFake(t, newJob(retried, kept))
	var (
		mu    sync.Mutex
		found = map[string]bool{}
	)
	f.Child = func(req workerapi.ChildRequest) *workerapi.Child {
		left := filepath.Join(f.Claim.RunDir, "subworkflows", "build", "step_"+strconv.Itoa(req.Step), "left")
		mu.Lock()
		_, err := os.Stat(left)
		found[strconv.Itoa(req.Step)+"."+strconv.Itoa(req.Try)] = err == nil
		mu.Unlock()
		if err := os.WriteFile(left, nil, 0o600); err != nil {
			t.Error(err)
		}
		return &workerapi.Child{Run: "c", Ended: true, Outcome: plan.OutcomeFailure}
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	if want := map[string]bool{"0.0": false, "0.1": false, "1.0": false, "1.1": true}; !maps.Equal(found, want) {
		t.Errorf("what a try found its child's directory held: %v, want %v", found, want)
	}

	elsewhere := t.TempDir()
	f = workerapitest.NewFake(t, newJob(calling(nil)))
	if err := os.MkdirAll(f.Claim.RunDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, filepath.Join(f.Claim.RunDir, "subworkflows")); err != nil {
		t.Fatal(err)
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 failure -1")
	if e := finishedEvent(t, f, 0, 0, false); !slices.Equal(codes(e.Diagnostics), []string{"child_dir_unusable"}) {
		t.Errorf("the step whose directory a link is in the way of has the diagnostics %v", e.Diagnostics)
	}
	if len(f.ChildAsks()) > 0 {
		t.Error("the worker asked for a child it had no directory for")
	}
	if entries, err := os.ReadDir(elsewhere); err != nil || len(entries) > 0 {
		t.Errorf("the worker made %v where the link points: %v", entries, err)
	}
}

// TestMakeDirIn checks that makeDirIn makes a directory a directory at a
// time, readable by its user alone, and cleans it, what it holds with it,
// but never follows a symbolic link: one on the way, or the directory
// itself, is too many levels of links, and a file on the way not a
// directory; and a link in the directory it cleans is removed, not what it
// points to.
func TestMakeDirIn(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := makeDirIn(root, filepath.Join("a", "b", "c"), false); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(filepath.Join(root, "a", "b", "c")); err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o700 {
		t.Errorf("the directory made: %v, %v", fi, err)
	}
	target := t.TempDir()
	kept := filepath.Join(target, "kept")
	if err := os.WriteFile(kept, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	c := filepath.Join(root, "a", "b", "c")
	if err := os.WriteFile(filepath.Join(c, "old"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(c, "link")); err != nil {
		t.Fatal(err)
	}
	if err := makeDirIn(root, filepath.Join("a", "b", "c"), true); err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(c); err != nil || len(entries) > 0 {
		t.Errorf("the directory cleaned holds %v: %v", entries, err)
	}
	if _, err := os.Stat(kept); err != nil {
		t.Errorf("cleaning removed what a link in the directory points to: %v", err)
	}

	if err := os.Symlink(target, filepath.Join(root, "a", "linked")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "a", "file"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for rel, want := range map[string]error{
		filepath.Join("a", "linked", "x"): syscall.ELOOP,
		filepath.Join("a", "linked"):      syscall.ELOOP,
		filepath.Join("a", "file", "x"):   syscall.ENOTDIR,
	} {
		for _, clean := range []bool{false, true} {
			err := makeDirIn(root, rel, clean)
			if pe, ok := errors.AsType[*fs.PathError](err); !ok || !errors.Is(err, want) || pe.Path == "" {
				t.Errorf("making %s, clean %v: %v, want %v", rel, clean, err, want)
			}
		}
	}
	if entries, err := os.ReadDir(target); err != nil || len(entries) != 1 {
		t.Errorf("where the link points holds %v: %v", entries, err)
	}
}

// TestMakeDirInSwapped checks that makeDirIn opens the directory it
// checked, not one that a symbolic link put in its place between its look
// and its open, though the link points in its root: it is too many levels
// of links, and nothing is made where the link points.
func TestMakeDirInSwapped(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	at := filepath.Join(root, "a")
	other := filepath.Join(root, "other")
	for _, dir := range []string{at, other} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	hook := func(path string) {
		if path != at {
			return
		}
		if err := os.Rename(at, at+"-old"); err != nil {
			t.Error(err)
		}
		if err := os.Symlink("other", at); err != nil {
			t.Error(err)
		}
	}
	beforeOpen.Store(&hook)
	defer beforeOpen.Store(nil)
	if err := makeDirIn(root, filepath.Join("a", "b"), false); !errors.Is(err, syscall.ELOOP) {
		t.Errorf("making a directory in one a link replaced: %v", err)
	}
	if entries, err := os.ReadDir(other); err != nil || len(entries) > 0 {
		t.Errorf("the worker made %v where the link points: %v", entries, err)
	}
}

// TestMakeChildDir checks that a child's directory outside its root run's
// is left for the coordinator, and nothing made; that one that is the
// attempt's run directory, or holds it, isn't cleaned, as format 1 never
// cleans its root's; and that one in a run directory the member chose,
// a host's copy of it, is made but never cleaned, as format 1 cleans one
// only where the run started.
func TestMakeChildDir(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	runDir := filepath.Join(root, "subworkflows", "build", "step_0")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "kept"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	a := &attempt{claim: &workerapi.Claim{RunDir: runDir, Tree: &workerapi.Tree{RootDir: root}}}
	outside := filepath.Join(t.TempDir(), "x")
	if err := a.makeChildDir(outside, true); err != nil {
		t.Errorf("a directory outside the root's: %v", err)
	}
	if _, err := os.Stat(outside); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a directory outside the root's was made: %v", err)
	}
	for _, dir := range []string{runDir, filepath.Join(root, "subworkflows")} {
		if err := a.makeChildDir(dir, true); err != nil {
			t.Errorf("making %s: %v", dir, err)
		}
		if _, err := os.Stat(filepath.Join(runDir, "kept")); err != nil {
			t.Errorf("making %s, which holds the run's directory, cleaned it: %v", dir, err)
		}
	}
	child := filepath.Join(runDir, "subworkflows", "main", "step_1")
	for _, onMember := range []bool{true, false} {
		a.claim.RunDirOnMember = onMember
		if err := os.MkdirAll(child, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(child, "left"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := a.makeChildDir(child, true); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(child, "left")); (err == nil) != onMember {
			t.Errorf("a child's directory in a run directory its member chose: %t, what the last try left: %v", onMember, err)
		}
	}
}

// TestChildRunDir checks that a child run's job makes its directory in its
// root's, never following a symbolic link on the way, and fails before its
// first step if one is.
func TestChildRunDir(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("echo hi")))
	root := filepath.Dir(f.Claim.RunDir)
	f.Claim.Tree = &workerapi.Tree{JobID: "1", RootDir: root}
	f.Claim.RunDir = filepath.Join(root, "subworkflows", "main", "step_0")
	elsewhere := t.TempDir()
	if err := os.Symlink(elsewhere, filepath.Join(root, "subworkflows")); err != nil {
		t.Fatal(err)
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	end := f.End()
	if end == nil || end.Problem == nil || end.Problem.Code != problems.RunDirUnusable.Code() {
		t.Fatalf("the child run's job ended %+v", end)
	}
	if entries, err := os.ReadDir(elsewhere); err != nil || len(entries) > 0 {
		t.Errorf("the worker made %v where the link points: %v", entries, err)
	}
}
