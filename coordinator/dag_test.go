package coordinator

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// jobOf returns the key of the job req places an attempt of, from its
// name, meridian-<run>-<job>.
func jobOf(req backend.PlaceRequest) string {
	rest := strings.TrimPrefix(req.Name, "meridian-")
	_, key, _ := strings.Cut(rest, "-")
	return key
}

// byJob has the driver's workers end each job's attempts as ends says,
// and with success a job it doesn't name.
func byJob(c *Coordinator, ends map[string]testEnd) {
	driverOf(c).set(func(d *testDriver) {
		d.worker = func(req backend.PlaceRequest) testEnd {
			if end, ok := ends[jobOf(req)]; ok {
				return end
			}
			return testEnd{result: plan.JobResult{Outcome: plan.OutcomeSuccess}}
		}
	})
}

// failed is how a job whose step failed ends.
var failed = testEnd{result: plan.JobResult{Outcome: plan.OutcomeFailure}}

// placedJobs returns the keys of the jobs the driver placed attempts of, in
// order.
func placedJobs(c *Coordinator) []string {
	reqs, _ := driverOf(c).requests()
	var keys []string
	for _, r := range reqs {
		keys = append(keys, jobOf(r))
	}
	return keys
}

// TestNeedsFailed checks that the jobs below a failure are skipped, each
// read as failed by the jobs that need it (NeedsFailed), and never placed;
// and that a run with a job skipped so ends failed, as format 1's does.
func TestNeedsFailed(t *testing.T) {
	c := open(t, Options{})
	byJob(c, map[string]testEnd{"a": failed})
	r := runToEnd(t, c, `
jobs:
  a:
    steps:
      - run: exit 1
  b:
    needs: [a]
    steps:
      - run: echo b
  c:
    needs: [b]
    steps:
      - run: echo c
`, nil)
	if r.Outcome != plan.OutcomeFailure {
		t.Errorf("the run ended with %s", r.Outcome)
	}
	if j := storedJob(t, c, r.ID, "a"); j.Outcome != plan.OutcomeFailure {
		t.Errorf("a ended with %s", j.Outcome)
	}
	for _, key := range []string{"b", "c"} {
		if j := storedJob(t, c, r.ID, key); j.Outcome != plan.OutcomeSkipped || j.Result == nil || !j.Result.NeedsFailed || j.Attempts != 0 {
			t.Errorf("%s is %+v, with %+v", key, j, j.Result)
		}
	}
	if placed := placedJobs(c); len(placed) != 1 || placed[0] != "a" {
		t.Errorf("the jobs placed are %v", placed)
	}
}

// TestNeedsFailedEndsRun checks that a run none of whose jobs failed, but
// one of which was skipped because a job it needs did, as only an
// always() job after it, ends failed.
func TestNeedsFailedEndsRun(t *testing.T) {
	c := open(t, Options{})
	byJob(c, map[string]testEnd{"a": failed})
	r := runToEnd(t, c, `
jobs:
  a:
    steps:
      - run: exit 1
  b:
    needs: [a]
    steps:
      - run: echo b
  after:
    needs: [b]
    if: ${{ always() }}
    steps:
      - run: echo after
`, nil)
	if r.Outcome != plan.OutcomeFailure {
		t.Errorf("the run ended with %s", r.Outcome)
	}
	if j := storedJob(t, c, r.ID, "after"); j.Outcome != plan.OutcomeSuccess {
		t.Errorf("the always() job ended with %s", j.Outcome)
	}
}

// TestAlwaysAfterFailure checks that a job whose if: is always() runs
// after one it needs failed, and the run still fails.
func TestAlwaysAfterFailure(t *testing.T) {
	c := open(t, Options{})
	byJob(c, map[string]testEnd{"a": failed})
	r := runToEnd(t, c, `
jobs:
  a:
    steps:
      - run: exit 1
  b:
    needs: [a]
    if: ${{ always() }}
    steps:
      - run: echo b
`, nil)
	if j := storedJob(t, c, r.ID, "b"); r.Outcome != plan.OutcomeFailure || j.Outcome != plan.OutcomeSuccess {
		t.Errorf("the run ended with %s, and b with %s", r.Outcome, j.Outcome)
	}
}

// primarySecondary is design.md's fallback, which runs only when the job
// it falls back for failed.
const primarySecondary = `
jobs:
  primary:
    steps:
      - run: ./deploy.sh primary
  secondary:
    needs: [primary]
    if: ${{ always() && needs.primary.result == 'failure' }}
    steps:
      - run: ./deploy.sh secondary
`

// TestPrimarySecondary checks design.md's fallback: secondary runs when
// primary failed, and is skipped, as a success its dependents read, when
// it succeeded.
func TestPrimarySecondary(t *testing.T) {
	t.Run("primary fails", func(t *testing.T) {
		c := open(t, Options{})
		byJob(c, map[string]testEnd{"primary": failed})
		r := runToEnd(t, c, primarySecondary, nil)
		if j := storedJob(t, c, r.ID, "secondary"); r.Outcome != plan.OutcomeFailure || j.Outcome != plan.OutcomeSuccess {
			t.Errorf("the run ended with %s, and secondary with %s", r.Outcome, j.Outcome)
		}
	})
	t.Run("primary succeeds", func(t *testing.T) {
		c := open(t, Options{})
		r := runToEnd(t, c, primarySecondary, nil)
		j := storedJob(t, c, r.ID, "secondary")
		if r.Outcome != plan.OutcomeSuccess || j.Outcome != plan.OutcomeSkipped || j.Result.NeedsFailed {
			t.Errorf("the run ended with %s, and secondary is %+v", r.Outcome, j.Result)
		}
		if placed := placedJobs(c); len(placed) != 1 || placed[0] != "primary" {
			t.Errorf("the jobs placed are %v", placed)
		}
	})
}

// TestPrivateOutputDownstream checks that a secret output a job ends with
// is kept only in the run's secret record, and that the job that needs it
// is readied with it joined from there, still marked as a secret, which
// the worker masks.
func TestPrivateOutputDownstream(t *testing.T) {
	c := open(t, Options{})
	release := make(chan struct{})
	byJob(c, map[string]testEnd{
		"a": {result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"token": "tok-SECRET-456"}, Private: []string{"token"}, Secrets: []string{"token"}}},
		"b": {result: plan.JobResult{Outcome: plan.OutcomeSuccess}, wait: release},
	})
	id, _ := create(t, c, `
jobs:
  a:
    steps:
      - run: echo token=x >> $OUTPUTS
  b:
    needs: [a]
    if: ${{ needs.a.outputs.token == 'tok-SECRET-456' }}
    steps:
      - run: echo ${{ needs.a.outputs.token }}
`, nil, nil)
	serve(t, c)
	// b is placed, so its if: read the token, joined from the record.
	waitFor(t, func() bool { return len(placedJobs(c)) == 2 })
	a := storedJob(t, c, id, "a")
	if a.Result == nil || a.Result.Outputs["token"] != "" || !strings.Contains(strings.Join(a.Result.Secrets, " "), "token") {
		t.Errorf("run state holds a's result as %+v", a.Result)
	}
	rec := storedRecord(t, c, id)
	if got := neededResult(a, rec); got.Outputs["token"] != "tok-SECRET-456" || len(got.Secrets) != 1 {
		t.Errorf("b reads a's result as %+v", got)
	}
	close(release)
	if r := waitEnded(t, c, id); r.Outcome != plan.OutcomeSuccess {
		t.Errorf("the run ended with %s", r.Outcome)
	}
}

// TestReadyEnds checks how a job ends when Ready ends it without running
// it: failed with Ready's errors, skipped as its if: says, with NeedsFailed
// as Ready gives it, or cancelled, as a job of a run stopped before it was
// readied whose if: gives it that path; and that a job Ready gives runs.
func TestReadyEnds(t *testing.T) {
	broken := problems.Diagnostics{diagnostic(problems.RunMalformed, problems.Params{"run": "1"})}
	for _, tc := range []struct {
		r    readied
		want *store.JobEnd
	}{
		{readied{result: &plan.ReadyResult{}, diags: broken}, &store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Diagnostics: broken}},
		{readied{result: &plan.ReadyResult{Skipped: true}}, &store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeSkipped}}},
		{readied{result: &plan.ReadyResult{Skipped: true, NeedsFailed: true}}, &store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeSkipped, NeedsFailed: true}}},
		{readied{result: &plan.ReadyResult{Cancelled: true}}, &store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeCancelled}}},
		{readied{result: &plan.ReadyResult{Job: &plan.ReadyJob{Key: "main"}}}, nil},
	} {
		end, ok := tc.r.end()
		switch {
		case ok != (tc.want != nil):
			t.Errorf("end of %+v = %+v, %v", tc.r.result, end, ok)
		case ok && (end.Result.Outcome != tc.want.Result.Outcome || end.Result.NeedsFailed != tc.want.Result.NeedsFailed || len(end.Diagnostics) != len(tc.want.Diagnostics)):
			t.Errorf("end of %+v = %+v, want %+v", tc.r.result, end, tc.want)
		}
	}
}

// countNotifier counts what it hears.
type countNotifier struct{ n atomic.Int64 }

func (n *countNotifier) Notify() { n.n.Add(1) }

// TestAnyJobFailed checks early-cancel: any-job-failed's part on the
// coordinator: the first job of the run to fail marks the run, which the
// attempt of a job then running tells its worker, and an attempt made after
// it does from its first control; and the worker API hears of it.
func TestAnyJobFailed(t *testing.T) {
	c := open(t, Options{})
	notifier := &countNotifier{}
	c.controls = notifier
	bStarted := make(chan struct{})
	byJob(c, map[string]testEnd{
		"a": {result: plan.JobResult{Outcome: plan.OutcomeFailure}, wait: bStarted},
		"b": {running: true, started: bStarted},
	})
	id, _ := create(t, c, `
jobs:
  a:
    steps:
      - run: exit 1
  b:
    steps:
      - run: sleep 600
        early-cancel: any-job-failed
  c:
    needs: [a]
    if: ${{ always() }}
    steps:
      - run: echo c
`, nil, nil)
	serve(t, c)
	waitFor(t, func() bool {
		j := storedJob(t, c, id, "c")
		return j.Outcome != ""
	})
	if r := storedRun(t, c, id); r.AnyJobFailedAt.IsZero() {
		t.Error("the run isn't marked as one a job of which failed")
	}
	control := func(key string) workerapi.Control {
		a := latestAttempt(t, c, id, key)
		var ctl workerapi.Control
		if err := c.store.View(t.Context(), func(tx *store.Tx) error {
			var err error
			ctl, err = tx.Control(a.ID)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return ctl
	}
	if ctl := control("b"); !ctl.AnyJobFailed || ctl.Cancel {
		t.Errorf("the running job's attempt tells its worker %+v", ctl)
	}
	if ctl := control("c"); !ctl.AnyJobFailed || ctl.Version != 1 {
		t.Errorf("the attempt made after the failure tells its worker %+v", ctl)
	}
	if ctl := control("a"); ctl.AnyJobFailed {
		t.Errorf("the job that failed tells its worker %+v", ctl)
	}
	if n := notifier.n.Load(); n != 1 {
		t.Errorf("the worker API heard of %d changes", n)
	}
}
