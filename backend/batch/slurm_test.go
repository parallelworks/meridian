//go:build unix

package batch_test

import (
	"crypto/rand"
	"encoding/json/jsontext"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/backend/batch"
	"github.com/parallelworks/meridian/backend/slurm"
	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/internal/schedtest"
	"github.com/parallelworks/meridian/problems"
)

func TestMain(m *testing.M) {
	schedtest.Main()
	os.Exit(m.Run())
}

// A clock is a driver's clock, which a test moves.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

var member = &backend.Member{Name: "m", Backend: "slurm", Settings: jsontext.Value(`{"partition":"debug"}`)}

// slurmDriver returns a driver of a fake Slurm cluster that holds every job
// it is given, and the cluster, as opts set it up.
func slurmDriver(t *testing.T, opts ...func(*schedtest.State)) (*batch.Driver, *schedtest.Cluster, *clock) {
	t.Helper()
	c := schedtest.New(t, append([]func(*schedtest.State){func(s *schedtest.State) { s.Hold = true }}, opts...)...)
	key := make([]byte, batch.MinKeyLen)
	_, _ = rand.Read(key)
	d := &batch.Driver{
		Scheduler: &slurm.Scheduler{Environ: c.Env(nil)}, Dir: filepath.Join(t.TempDir(), "batch"), Key: key,
		Binary:  func() (string, string, error) { return "/bin/false", "sha256:00", nil },
		Environ: c.Env(nil),
	}
	cl := &clock{t: time.Now()}
	batch.SetClock(d, cl.now)
	batch.SetInterfaces(d, func() ([]netip.Prefix, error) { return []netip.Prefix{netip.MustParsePrefix("10.9.0.5/16")}, nil })
	return d, c, cl
}

// TestSlurmMemberKeys checks that slurm members whose relay ports differ,
// as Slurm's settings read them, have listeners of their own, and those
// whose other settings alone differ share one.
func TestSlurmMemberKeys(t *testing.T) {
	t.Parallel()
	d, _, _ := slurmDriver(t)
	key := func(settings string) string {
		t.Helper()
		k, err := d.MemberKey(&backend.Member{Name: "m", Backend: "slurm", Settings: jsontext.Value(settings)})
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	a, b := key(`{"partition":"debug"}`), key(`{"partition":"debug","account":"x"}`)
	c, e := key(`{"relay":{"ports":"60001-60100"}}`), key(`{"relay":{"ports":"60001-60200"}}`)
	if a != b || a == c || c == e {
		t.Errorf("the keys are %s and %s alike, and %s and %s", a, b, c, e)
	}
}

// TestSlurmCheck checks each row of the design's table of what Slurm and
// the records say, for every Slurm state, as Slurm names them.
func TestSlurmCheck(t *testing.T) {
	t.Parallel()
	dead := func(end backend.End) backend.Status { return backend.Status{State: backend.Dead, End: end} }
	for _, tc := range []struct {
		state, reason string
		want          backend.Status
	}{
		{"PENDING", "Priority", backend.Status{State: backend.Queued}},
		{"CONFIGURING", "None", backend.Status{State: backend.Queued}},
		{"REQUEUE_HOLD", "None", backend.Status{State: backend.Queued}},
		{"RESV_DEL_HOLD", "None", backend.Status{State: backend.Queued}},
		{"RUNNING", "None", backend.Status{State: backend.Running}},
		{"COMPLETING", "None", backend.Status{State: backend.Running}},
		{"SUSPENDED", "None", backend.Status{State: backend.Running}},
		{"STAGE_OUT", "None", backend.Status{State: backend.Running}},
		{"SIGNALING", "None", backend.Status{State: backend.Running}},
		{"RESIZING", "None", backend.Status{State: backend.Running}},
		{"COMPLETED", "None", dead(backend.EndExited)},
		{"FAILED", "NonZeroExitCode", dead(backend.EndExited)},
		{"SPECIAL_EXIT", "None", dead(backend.EndExited)},
		{"TIMEOUT", "TimeLimit", dead(backend.EndTimedOut)},
		{"DEADLINE", "None", dead(backend.EndTimedOut)},
		{"OUT_OF_MEMORY", "OutOfMemory", dead(backend.EndOutOfMemory)},
		{"NODE_FAIL", "None", dead(backend.EndNodeFailed)},
		{"BOOT_FAIL", "None", dead(backend.EndNodeFailed)},
		{"PREEMPTED", "None", dead(backend.EndPreempted)},
		{"CANCELLED", "None", dead(backend.EndExited)},
	} {
		t.Run(tc.state, func(t *testing.T) {
			t.Parallel()
			d, c, _ := slurmDriver(t)
			p, err := d.Place(t.Context(), member, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "t"})
			if err != nil {
				t.Fatal(err)
			}
			sub := filepath.Join(d.Dir, "a1", "1")
			// The exit record of a job that ran meridian batch.
			if err := batch.WriteRecord(sub, batch.StartedFile, &batch.Started{Job: "2", Start: time.Now()}); err != nil {
				t.Fatal(err)
			}
			if err := batch.WriteRecord(sub, batch.ExitFile, &batch.Exit{At: time.Now()}); err != nil {
				t.Fatal(err)
			}
			c.SetJob(t, 2, func(j *schedtest.Job) {
				j.State, j.Reason = tc.state, tc.reason
				if !slices.Contains([]string{"PENDING", "CONFIGURING", "REQUEUE_HOLD", "RESV_DEL_HOLD"}, tc.state) {
					j.Start = time.Now()
				}
				if tc.want.State == backend.Dead {
					j.End = time.Now()
				}
			})
			if st, err := d.Check(t.Context(), member, p); err != nil || st != tc.want {
				t.Errorf("a job %s: %+v, %v, want %+v", tc.state, st, err, tc.want)
			}
		})
	}
}

// TestSlurmGone checks the rows of jobs that squeue no longer lists, once
// MinJobAge has passed: those accounting knows, where the cluster keeps
// it; those whose records say how they ended; and those whose records
// don't, once NFS has had two minutes to show them.
func TestSlurmGone(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		accounting bool
		records    func(t *testing.T, sub string)
		want       backend.Status
		problem    *problems.Kind
	}{
		{
			name: "accounting", accounting: true,
			want: backend.Status{State: backend.Dead, End: backend.EndNodeFailed},
		},
		{
			name: "started only",
			records: func(t *testing.T, sub string) {
				t.Helper()
				if err := batch.WriteRecord(sub, batch.StartedFile, &batch.Started{Job: "2", Start: time.Now()}); err != nil {
					t.Fatal(err)
				}
			},
			want: backend.Status{State: backend.Dead}, problem: problems.SchedulerJobVanished,
		},
		{name: "no records", want: backend.Status{State: backend.Dead}, problem: problems.SchedulerWorkerNotStarted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, c, cl := slurmDriver(t, func(s *schedtest.State) { s.MinJobAge, s.Accounting = 0, tc.accounting })
			p, err := d.Place(t.Context(), member, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "t"})
			if err != nil {
				t.Fatal(err)
			}
			if tc.records != nil {
				tc.records(t, filepath.Join(d.Dir, "a1", "1"))
			}
			c.SetJob(t, 2, func(j *schedtest.Job) {
				j.State, j.Start, j.End = "NODE_FAIL", time.Now(), time.Now().Add(-time.Second)
			})
			st, err := d.Check(t.Context(), member, p)
			if !tc.accounting {
				if st.State != backend.Unknown || err == nil {
					t.Fatalf("within the grace: %+v, %v", st, err)
				}
				cl.advance(batch.Grace)
				st, err = d.Check(t.Context(), member, p)
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.problem != nil {
				kindtest.Want(t, st.Problem, tc.problem)
				st.Problem = nil
			}
			if st != tc.want {
				t.Errorf("%+v, want %+v", st, tc.want)
			}
		})
	}
}

// TestSlurmCannotStart checks that a job pending for a partition's time
// limit, as a cluster that doesn't refuse it holds it, is cancelled once
// it has been so for two minutes, and fails before its first step.
func TestSlurmCannotStart(t *testing.T) {
	t.Parallel()
	d, c, cl := slurmDriver(t, func(s *schedtest.State) {
		s.Hold = false
		s.Partitions[0].MaxTime = "00:10:00"
	})
	p, err := d.Place(t.Context(), member, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "t", TimeoutSeconds: 3600})
	if err != nil {
		t.Fatal(err)
	}
	if st, err := d.Check(t.Context(), member, p); err != nil || st.State != backend.Queued {
		t.Fatalf("first: %+v, %v", st, err)
	}
	cl.advance(batch.Stuck)
	batch.Refresh(d)
	st, err := d.Check(t.Context(), member, p)
	if err != nil || st.State != backend.Dead {
		t.Fatalf("after two minutes: %+v, %v", st, err)
	}
	if got := kindtest.Want(t, st.Problem, problems.SchedulerJobCannotStart); got.Params["reason"] != "PartitionTimeLimit" || got.Params["id"] != "2" {
		t.Errorf("params %v", got.Params)
	}
	if j := c.Job(t, 2); j.State != "CANCELLED" {
		t.Errorf("the job is %s", j.State)
	}
	if got := c.Calls(t, "scancel"); len(got) != 1 || !slices.Equal(got[0], []string{"2"}) {
		t.Errorf("scancel was called with %q", got)
	}
}

// TestSlurmCancelForget checks that Cancel withdraws a job, and its token
// goes, and that Forget, once the job has ended, removes its submissions.
func TestSlurmCancelForget(t *testing.T) {
	t.Parallel()
	d, c, _ := slurmDriver(t)
	p, err := d.Place(t.Context(), member, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Forget(t.Context(), member, p); err == nil {
		t.Error("Forget of a queued job")
	}
	if err := d.Cancel(t.Context(), member, p); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(d.Dir, "a1", "1", batch.TokenFile)); err == nil {
		t.Error("the token is there after Cancel")
	}
	batch.Refresh(d)
	if st, err := d.Check(t.Context(), member, p); err != nil || st != (backend.Status{State: backend.Dead, End: backend.EndWithdrawn}) {
		t.Errorf("the cancelled job: %+v, %v", st, err)
	}
	if err := d.Forget(t.Context(), member, p); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(d.Dir, "a1")); err == nil {
		t.Error("the attempt's submissions are there after Forget")
	}
	if j := c.Job(t, 2); j.State != "CANCELLED" {
		t.Errorf("the job is %s", j.State)
	}
}

// TestSlurmPoll checks that the snapshot asks squeue about every job
// watched in chunks of 500, in one call each.
func TestSlurmPoll(t *testing.T) {
	t.Parallel()
	d, c, _ := slurmDriver(t)
	var p backend.Placement
	for i := range 3 {
		var err error
		if p, err = d.Place(t.Context(), member, backend.PlaceRequest{Attempt: "a" + strconv.Itoa(i), BootstrapToken: "t"}); err != nil {
			t.Fatal(err)
		}
	}
	for range 5 {
		if st, err := d.Check(t.Context(), member, p); err != nil || st.State != backend.Queued {
			t.Fatalf("%+v, %v", st, err)
		}
	}
	if got := c.Calls(t, "squeue"); len(got) != 1 || !slices.Equal(got[0], []string{"--noheader", "--states=all", "--jobs=2,3,4", "--format=%i|%T|%r"}) {
		t.Errorf("squeue was called with %q", got)
	}
}

// TestSlurmRunaway checks that a job accounting still has as running,
// which the queue no longer lists, as a controller that stopped leaves, is
// taken for gone: its records decide, once NFS has had two minutes.
func TestSlurmRunaway(t *testing.T) {
	t.Parallel()
	d, c, cl := slurmDriver(t, func(s *schedtest.State) { s.Accounting = true })
	p, err := d.Place(t.Context(), member, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.WriteRecord(filepath.Join(d.Dir, "a1", "1"), batch.StartedFile, &batch.Started{Job: "2", Start: time.Now()}); err != nil {
		t.Fatal(err)
	}
	c.SetJob(t, 2, func(j *schedtest.Job) { j.State, j.Start, j.Runaway = "RUNNING", time.Now(), true })
	if st, err := d.Check(t.Context(), member, p); st.State != backend.Unknown || err == nil {
		t.Fatalf("within the grace: %+v, %v", st, err)
	}
	cl.advance(batch.Grace)
	st, err := d.Check(t.Context(), member, p)
	if err != nil {
		t.Fatal(err)
	}
	kindtest.Want(t, st.Problem, problems.SchedulerJobVanished)
}
