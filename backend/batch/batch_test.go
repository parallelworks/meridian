package batch

import (
	"context"
	"crypto/rand"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/problems"
)

// A fakeScheduler is a Scheduler whose cluster a test sets, and which
// records what it was asked.
type fakeScheduler struct {
	mu    sync.Mutex
	info  Info
	parts map[string]Partition
	// queue and acct are what the queue and accounting list, by ID.
	queue, acct map[string]JobStatus
	// named are the IDs of the jobs of each name.
	named     map[string][]string
	submitted []*Job
	submitErr error
	cancelled []string
	// statuses and accounting are the IDs each call asked about.
	statuses, accounting [][]string
	// hold, if not nil, holds each Statuses until it is closed.
	hold chan struct{}
	// infoErr is what Info fails with, and infos how many times it was
	// asked; infoHold, if not nil, holds each Info until it is closed.
	infoErr  error
	infos    int
	infoHold chan struct{}
}

func newFake() *fakeScheduler {
	return &fakeScheduler{
		info:  Info{Version: "23.11.4", Cluster: "test"},
		parts: map[string]Partition{"debug": {Name: "debug", State: "UP", Up: true, MaxSeconds: 3600, DefaultSeconds: 1200}},
		queue: map[string]JobStatus{}, acct: map[string]JobStatus{}, named: map[string][]string{},
	}
}

func (f *fakeScheduler) Name() string { return "fake" }

// Settings takes partition and host, as text, and walltime, as seconds.
func (f *fakeScheduler) Settings(member string, raw jsontext.Value) (*Settings, error) {
	var keys struct {
		Partition string `json:"partition"`
		Host      string `json:"host"`
		Walltime  int64  `json:"walltime"`
	}
	if len(raw) > 0 {
		if err := jsonv2.Unmarshal(raw, &keys, jsonv2.RejectUnknownMembers(true)); err != nil {
			return nil, problems.MemberSettingInvalid.Problem(problems.Params{"member": member, "key": "?", "reason": "unknown"})
		}
	}
	return &Settings{Partition: keys.Partition, Host: keys.Host, WalltimeSeconds: keys.Walltime}, nil
}

func (f *fakeScheduler) Info(context.Context) (*Info, error) {
	f.mu.Lock()
	hold := f.infoHold
	f.mu.Unlock()
	if hold != nil {
		<-hold
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.infos++
	if f.infoErr != nil {
		return nil, f.infoErr
	}
	info := f.info
	return &info, nil
}

func (f *fakeScheduler) Partition(_ context.Context, name string) (Partition, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if name == "" {
		name = "debug"
	}
	p, ok := f.parts[name]
	return p, ok, nil
}

func (f *fakeScheduler) Submit(_ context.Context, job *Job) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.submitErr != nil {
		return "", f.submitErr
	}
	f.submitted = append(f.submitted, job)
	id := strconv.Itoa(100 + len(f.submitted))
	f.queue[id] = JobStatus{State: backend.Queued, Reason: "Priority"}
	return id, nil
}

func (f *fakeScheduler) Statuses(_ context.Context, ids []string, _ bool) (map[string]JobStatus, error) {
	f.mu.Lock()
	f.statuses = append(f.statuses, slices.Clone(ids))
	hold := f.hold
	f.mu.Unlock()
	if hold != nil {
		<-hold
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]JobStatus{}
	for _, id := range ids {
		if st, ok := f.queue[id]; ok {
			out[id] = st
		}
	}
	return out, nil
}

func (f *fakeScheduler) Accounting(_ context.Context, ids []string) (map[string]JobStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.accounting = append(f.accounting, slices.Clone(ids))
	out := map[string]JobStatus{}
	for _, id := range ids {
		if st, ok := f.acct[id]; ok {
			out[id] = st
		}
	}
	return out, nil
}

func (f *fakeScheduler) Named(_ context.Context, names []string, _ bool) (map[string][]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string][]string{}
	for _, name := range names {
		if ids := f.named[name]; len(ids) > 0 {
			out[name] = ids
		}
	}
	return out, nil
}

func (f *fakeScheduler) Cancel(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled = append(f.cancelled, id)
	f.queue[id] = JobStatus{State: backend.Dead, End: backend.EndExited, Cancelled: true}
	return nil
}

func (f *fakeScheduler) Allocation(context.Context, func(string) string) (Allocation, error) {
	return Allocation{}, nil
}

// set sets what the queue lists of the job id.
func (f *fakeScheduler) set(id string, st JobStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queue[id] = st
}

// calls returns how many times the queue was asked.
func (f *fakeScheduler) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.statuses)
}

// A fakeClock is the clock a driver counts grace by.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// testDigest is the digest of the binary the tests' drivers submit.
const testDigest = "sha256:" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// newDriver returns a driver of f's cluster, with its directory in a
// temporary one, a snapshot good until the test refreshes it, and a
// clock the test moves.
func newDriver(t *testing.T, f *fakeScheduler) (*Driver, *fakeClock) {
	t.Helper()
	key := make([]byte, MinKeyLen)
	_, _ = rand.Read(key)
	clock := &fakeClock{t: time.Unix(1_800_000_000, 0)}
	d := &Driver{
		Scheduler: f, Dir: filepath.Join(t.TempDir(), "batch"), Key: key,
		Binary:  func() (string, string, error) { return "/opt/meridian's/bin/meridian", testDigest, nil },
		Environ: []string{"PATH=/usr/bin", "GODEBUG=fips140=off", "MERIDIAN_STATE_DIR=/x", "SECRET=s", "HOME=/home/u"}, Withhold: []string{"SECRET"},
	}
	d.timing = defaultTiming
	d.timing.now = clock.now
	return d, clock
}

var testMember = &backend.Member{Name: "m", Backend: "slurm", Settings: jsontext.Value(`{"partition":"debug"}`)}

// place places the attempt, and returns its placement and its submission's
// directory.
func place(t *testing.T, d *Driver, attempt string) (backend.Placement, string) {
	t.Helper()
	p, err := d.Place(t.Context(), testMember, backend.PlaceRequest{
		Attempt: attempt, BootstrapToken: "token-" + attempt, TimeoutSeconds: 600, RunDir: "/runs/1",
		Coordinator: backend.Endpoint{Address: "/run/meridian.sock"},
	})
	if err != nil {
		t.Fatalf("Place(%s): %v", attempt, err)
	}
	return p, filepath.Join(d.Dir, attempt, "1")
}

// fresh has d's next check ask the scheduler again.
func fresh(d *Driver) {
	d.poll.mu.Lock()
	defer d.poll.mu.Unlock()
	d.poll.snap = snapshot{}
}

func TestPlace(t *testing.T) {
	t.Parallel()
	f := newFake()
	d, _ := newDriver(t, f)
	p, sub := place(t, d, "a1")
	if !strings.HasPrefix(p.ID, "job:") || !strings.HasSuffix(p.ID, "/101") || p.WorkerDigest != testDigest || p.RunDir != "/runs/1" {
		t.Errorf("the placement is %+v", p)
	}
	job := f.submitted[0]
	if job.Name != "meridian-a1-1" || job.Dir != sub || job.WalltimeSeconds != 600+DefaultMarginSeconds {
		t.Errorf("the job is %+v", job)
	}
	want := "#!/bin/sh\nexec '/opt/meridian'\\''s/bin/meridian' batch '" + sub + "'\n"
	if string(job.Script) != want {
		t.Errorf("the script is %q, want %q", job.Script, want)
	}
	if want := []string{"PATH=/usr/bin", "HOME=/home/u"}; !slices.Equal(job.Environ, want) {
		t.Errorf("the job's environment is %q, want %q", job.Environ, want)
	}
	for name, mode := range map[string]fs.FileMode{TokenFile: 0o600, SpecFile: 0o600, JobFile: 0o600, ".": fs.ModeDir | 0o700} {
		info, err := os.Lstat(filepath.Join(sub, name))
		if err != nil || info.Mode() != mode {
			t.Errorf("%s: %v, %v, want %v", name, info.Mode(), err, mode)
		}
	}
	if token, err := os.ReadFile(filepath.Join(sub, TokenFile)); err != nil || string(token) != "token-a1" {
		t.Errorf("the token's file holds %q, %v", token, err)
	}
	spec, err := ReadSpec(sub)
	if err != nil || spec.Attempt != "a1" || spec.Submission != 1 || spec.Coordinator.Address != "/run/meridian.sock" || spec.MarginSeconds != DefaultMarginSeconds || spec.Digest != testDigest {
		t.Errorf("the spec is %+v, %v", spec, err)
	}
	if strings.Contains(string(job.Script), "token-a1") || slices.ContainsFunc(job.Environ, func(kv string) bool { return strings.Contains(kv, "token-a1") }) {
		t.Error("the token is in the job's script or its environment")
	}
}

func TestPlaceFailed(t *testing.T) {
	t.Parallel()
	t.Run("rejected", func(t *testing.T) {
		t.Parallel()
		f := newFake()
		f.submitErr = &Rejected{Said: "Invalid account or account/partition combination specified"}
		d, _ := newDriver(t, f)
		_, err := d.Place(t.Context(), testMember, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "t"})
		got := kindtest.Want(t, err, problems.SchedulerRejected)
		if r, _ := errors.AsType[*Rejected](f.submitErr); got.Params["member"] != "m" || got.Params["reason"] != r.Said {
			t.Errorf("the problem's params are %v", got.Params)
		}
		// Nothing reads the token: the submission goes, and it with it.
		if _, err := os.Lstat(filepath.Join(d.Dir, "a1", "1")); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("the refused submission's directory: %v", err)
		}
	})
	t.Run("busy", func(t *testing.T) {
		t.Parallel()
		f := newFake()
		f.submitErr = errors.New("Socket timed out on send/recv operation")
		d, _ := newDriver(t, f)
		_, err := d.Place(t.Context(), testMember, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "t"})
		if err == nil || problems.IsProblem(err) {
			t.Fatalf("Place with a busy scheduler: %v, want an error to try again", err)
		}
		// The scheduler may have the job: its token stays for it.
		if _, err := os.Lstat(filepath.Join(d.Dir, "a1", "1", TokenFile)); err != nil {
			t.Errorf("the token's file: %v", err)
		}
	})
}

func TestProbe(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		member backend.Member
		parts  map[string]Partition
		demand backend.Resources
		want   *problems.Kind
		fit    backend.Fit
	}{
		{
			name: "fits", member: *testMember,
			fit: backend.Fit{Availability: backend.WouldQueue, MaxWalltimeSeconds: 3600, MarginSeconds: 300, DefaultTimeoutSeconds: 900},
		},
		{
			name: "own limits", member: backend.Member{Name: "m", Settings: jsontext.Value(`{}`), MaxWalltimeSeconds: 1800, MarginSeconds: 60, DefaultTimeoutSeconds: 100},
			fit: backend.Fit{Availability: backend.WouldQueue, MaxWalltimeSeconds: 1800, MarginSeconds: 60, DefaultTimeoutSeconds: 100},
		},
		{
			name: "fixed walltime", member: backend.Member{Name: "m", Settings: jsontext.Value(`{"walltime":1000}`)},
			fit: backend.Fit{Availability: backend.WouldQueue, MaxWalltimeSeconds: 3600, MarginSeconds: 300, DefaultTimeoutSeconds: 700},
		},
		{name: "unknown partition", member: backend.Member{Name: "m", Settings: jsontext.Value(`{"partition":"gpu"}`)}, want: problems.SchedulerPartitionUnknown},
		{
			name: "down", member: *testMember, want: problems.SchedulerPartitionDown,
			parts: map[string]Partition{"debug": {Name: "debug", State: "DRAIN"}},
		},
		{name: "no default", member: backend.Member{Name: "m"}, parts: map[string]Partition{}, want: problems.MemberSettingInvalid},
		{name: "longer than the partition", member: backend.Member{Name: "m", MaxWalltimeSeconds: 7200}, want: problems.MemberSettingInvalid},
		{name: "walltime longer", member: backend.Member{Name: "m", Settings: jsontext.Value(`{"walltime":7200}`)}, want: problems.MemberSettingInvalid},
		{name: "host", member: backend.Member{Name: "m", Backend: "slurm", Settings: jsontext.Value(`{"host":"login1"}`)}, want: problems.BackendNotBuilt},
		{name: "container", member: backend.Member{Name: "m", Isolation: "apptainer"}, want: problems.IsolationNotBuilt},
		{name: "more than declared", member: backend.Member{Name: "m", Resources: backend.Resources{GPU: 1}}, demand: backend.Resources{GPU: 2}, want: problems.ResourcesExceedMember},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFake()
			if tc.parts != nil {
				f.parts = tc.parts
			}
			d, _ := newDriver(t, f)
			fit, err := d.Probe(t.Context(), &tc.member, backend.Demand{Resources: tc.demand})
			if err != nil {
				t.Fatal(err)
			}
			if tc.want != nil {
				kindtest.Want(t, fit.Problem, tc.want)
				return
			}
			if fit != tc.fit {
				t.Errorf("the fit is %+v, want %+v", fit, tc.fit)
			}
		})
	}
}

// TestProbeCaller checks that a job that calls a workflow, whose run it
// would hold its allocation for, can't run on a member, whatever the
// member is, and that the scheduler isn't asked.
func TestProbeCaller(t *testing.T) {
	t.Parallel()
	f := newFake()
	f.infoErr = errors.New("the controller isn't asked")
	d, _ := newDriver(t, f)
	fit, err := d.Probe(t.Context(), testMember, backend.Demand{CallsWorkflow: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := kindtest.Want(t, fit.Problem, problems.SchedulerCallerRefused); got.Params["member"] != "m" {
		t.Errorf("the problem's params are %v", got.Params)
	}
	if f.infos != 0 || f.calls() != 0 {
		t.Errorf("the scheduler was asked %d and %d times", f.infos, f.calls())
	}
}

// writeRecords writes a submission's records for a test.
func writeRecords(t *testing.T, sub string, r records) {
	t.Helper()
	for name, v := range map[string]any{StartedFile: r.started, TermFile: r.term, ExitFile: r.exit, WithdrawnFile: r.withdrawn} {
		if !isNil(v) {
			if err := WriteRecord(sub, name, v); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func isNil(v any) bool {
	switch v := v.(type) {
	case *Started:
		return v == nil
	case *Term:
		return v == nil
	case *Exit:
		return v == nil
	case *Withdrawn:
		return v == nil
	}
	return true
}

// TestCheck checks each row of the design's table of what a scheduler and
// the records say, with the scheduler's states as a scheduler maps them.
func TestCheck(t *testing.T) {
	t.Parallel()
	start := time.Unix(1_800_000_000, 0)
	started := &Started{Job: "101", Node: "n1", Start: start, End: start.Add(time.Hour)}
	exited := &Exit{Status: 0, At: start.Add(time.Minute)}
	dead := func(end backend.End) backend.Status { return backend.Status{State: backend.Dead, End: end} }
	for _, tc := range []struct {
		name string
		// listed is what the queue lists, or nil for nothing.
		listed *JobStatus
		r      records
		want   backend.Status
		// problem is the kind of the Status's problem.
		problem *problems.Kind
	}{
		{name: "pending", listed: &JobStatus{State: backend.Queued, Reason: "Priority"}, want: backend.Status{State: backend.Queued}},
		{name: "running", listed: &JobStatus{State: backend.Running}, want: backend.Status{State: backend.Running}},
		{name: "completed", listed: &JobStatus{State: backend.Dead, End: backend.EndExited}, r: records{started: started, exit: exited}, want: dead(backend.EndExited)},
		{name: "timeout", listed: &JobStatus{State: backend.Dead, End: backend.EndTimedOut}, want: dead(backend.EndTimedOut)},
		{
			name: "signalled at the end", listed: &JobStatus{State: backend.Dead, End: backend.EndExited},
			r:    records{started: started, term: &Term{At: start.Add(time.Hour - time.Second), Signal: 15}, exit: &Exit{Status: 143, At: start.Add(time.Hour)}},
			want: dead(backend.EndTimedOut),
		},
		{name: "out of memory", listed: &JobStatus{State: backend.Dead, End: backend.EndOutOfMemory}, want: dead(backend.EndOutOfMemory)},
		{
			name: "killed for memory", listed: &JobStatus{State: backend.Dead, End: backend.EndExited},
			r:    records{started: started, exit: &Exit{Status: -1, Signal: 9, OOMKills: 1, At: start}},
			want: dead(backend.EndOutOfMemory),
		},
		{
			name: "a step killed for memory", listed: &JobStatus{State: backend.Dead, End: backend.EndExited},
			r:    records{started: started, exit: &Exit{Status: 0, OOMKills: 1, At: start}},
			want: dead(backend.EndExited),
		},
		{name: "node failed", listed: &JobStatus{State: backend.Dead, End: backend.EndNodeFailed}, want: dead(backend.EndNodeFailed)},
		{name: "preempted", listed: &JobStatus{State: backend.Dead, End: backend.EndPreempted}, want: dead(backend.EndPreempted)},
		{
			name: "withdrawn", listed: &JobStatus{State: backend.Dead, End: backend.EndExited, Cancelled: true},
			r: records{withdrawn: &Withdrawn{At: start}}, want: dead(backend.EndWithdrawn),
		},
		{name: "cancelled by someone", listed: &JobStatus{State: backend.Dead, End: backend.EndExited, Cancelled: true}, want: dead(backend.EndExited)},
		{name: "gone, ended", r: records{started: started, exit: exited}, want: dead(backend.EndExited)},
		{name: "gone, withdrawn", r: records{withdrawn: &Withdrawn{At: start}}, want: dead(backend.EndWithdrawn)},
		{name: "gone, started only", r: records{started: started}, want: backend.Status{State: backend.Dead}, problem: problems.SchedulerJobVanished},
		{
			name: "gone, signalled at the end", r: records{started: started, term: &Term{At: start.Add(time.Hour), Signal: 15}},
			want: dead(backend.EndTimedOut),
		},
		{name: "gone, no records", want: backend.Status{State: backend.Dead}, problem: problems.SchedulerWorkerNotStarted},
		{
			name: "exited, no records", listed: &JobStatus{State: backend.Dead, End: backend.EndExited},
			want: backend.Status{State: backend.Dead}, problem: problems.SchedulerWorkerNotStarted,
		},
		{
			name: "relay unreachable", listed: &JobStatus{State: backend.Dead, End: backend.EndExited},
			r: records{started: started, exit: &Exit{Status: 2, At: start, Problem: Record(problems.SchedulerRelayUnreachable.Problem(problems.Params{
				"node": "n1", "addresses": `"10.0.0.1:4000"`,
			}))}},
			want: backend.Status{State: backend.Dead}, problem: problems.SchedulerRelayUnreachable,
		},
		{
			name: "a code no release records", listed: &JobStatus{State: backend.Dead, End: backend.EndExited},
			r:    records{started: started, exit: &Exit{Status: 2, At: start, Problem: &Recorded{Code: "secret_variable_unset"}}},
			want: dead(backend.EndExited),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFake()
			d, clock := newDriver(t, f)
			p, sub := place(t, d, "a1")
			f.mu.Lock()
			delete(f.queue, "101")
			if tc.listed != nil {
				f.queue["101"] = *tc.listed
			}
			f.mu.Unlock()
			writeRecords(t, sub, tc.r)
			st, err := d.Check(t.Context(), testMember, p)
			if st.State == backend.Unknown {
				// Records that a network filesystem might not show yet are
				// waited for.
				if err == nil {
					t.Error("an Unknown check without why")
				}
				clock.advance(defaultTiming.grace)
				st, err = d.Check(t.Context(), testMember, p)
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.problem != nil {
				kindtest.Want(t, st.Problem, tc.problem)
				st.Problem = nil
			} else if st.Problem != nil {
				t.Errorf("the status has the problem %v", st.Problem)
			}
			if st != tc.want {
				t.Errorf("the status is %+v, want %+v", st, tc.want)
			}
		})
	}
}

// TestCheckGrace checks that a job the scheduler no longer lists, whose
// records don't say how it ended, is Unknown until two minutes after a
// check first found it so, for a record NFS hasn't shown yet; and that one
// whose record shows meanwhile is concluded from it.
func TestCheckGrace(t *testing.T) {
	t.Parallel()
	f := newFake()
	d, clock := newDriver(t, f)
	p, sub := place(t, d, "a1")
	delete(f.queue, "101")
	st, err := d.Check(t.Context(), testMember, p)
	if st.State != backend.Unknown || err == nil {
		t.Fatalf("a job gone without records: %+v, %v", st, err)
	}
	clock.advance(defaultTiming.grace - time.Second)
	if st, err := d.Check(t.Context(), testMember, p); st.State != backend.Unknown || err == nil {
		t.Fatalf("a second before its grace ends: %+v, %v", st, err)
	}
	writeRecords(t, sub, records{exit: &Exit{Status: 0, At: time.Now()}})
	if st, err := d.Check(t.Context(), testMember, p); err != nil || st != (backend.Status{State: backend.Dead, End: backend.EndExited}) {
		t.Fatalf("once its record shows: %+v, %v", st, err)
	}
}

// TestCheckAccounting checks that a job the queue no longer lists is
// asked of accounting, where the cluster keeps it, but for one whose exit
// record says how it ended.
func TestCheckAccounting(t *testing.T) {
	t.Parallel()
	f := newFake()
	f.info.Accounting = true
	d, _ := newDriver(t, f)
	p1, _ := place(t, d, "a1")
	p2, sub2 := place(t, d, "a2")
	writeRecords(t, sub2, records{exit: &Exit{At: time.Now()}})
	f.mu.Lock()
	clear(f.queue)
	f.acct["101"] = JobStatus{State: backend.Dead, End: backend.EndNodeFailed}
	f.mu.Unlock()
	if st, err := d.Check(t.Context(), testMember, p1); err != nil || st != (backend.Status{State: backend.Dead, End: backend.EndNodeFailed}) {
		t.Errorf("the job accounting has: %+v, %v", st, err)
	}
	if st, err := d.Check(t.Context(), testMember, p2); err != nil || st != (backend.Status{State: backend.Dead, End: backend.EndExited}) {
		t.Errorf("the job whose record says: %+v, %v", st, err)
	}
	if want := [][]string{{"101"}}; !slices.EqualFunc(f.accounting, want, slices.Equal) {
		t.Errorf("accounting was asked about %v, want %v", f.accounting, want)
	}
}

// TestCheckCannotStart checks that a job pending for a reason that won't
// clear, for two minutes, is withdrawn, its token removed, and Dead with
// scheduler_job_cannot_start, which a check after its cancel says too.
func TestCheckCannotStart(t *testing.T) {
	t.Parallel()
	f := newFake()
	d, clock := newDriver(t, f)
	p, sub := place(t, d, "a1")
	f.set("101", JobStatus{State: backend.Queued, Reason: "PartitionTimeLimit", Stuck: true})
	// A scheduler can say such a reason for a while of a job it then runs.
	if st, err := d.Check(t.Context(), testMember, p); err != nil || st.State != backend.Queued || len(f.cancelled) > 0 {
		t.Fatalf("a job first found stuck: %+v, %v, cancelled %v", st, err, f.cancelled)
	}
	clock.advance(defaultTiming.stuck)
	for i := range 2 {
		st, err := d.Check(t.Context(), testMember, p)
		if err != nil || st.State != backend.Dead {
			t.Fatalf("check %d: %+v, %v", i, st, err)
		}
		if got := kindtest.Want(t, st.Problem, problems.SchedulerJobCannotStart); got.Params["id"] != "101" || got.Params["reason"] != "PartitionTimeLimit" {
			t.Errorf("check %d's params are %v", i, got.Params)
		}
		fresh(d)
	}
	if !slices.Equal(f.cancelled, []string{"101"}) {
		t.Errorf("cancelled %v", f.cancelled)
	}
	if _, err := os.Lstat(filepath.Join(sub, TokenFile)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the token's file: %v", err)
	}
}

// TestPoll checks that checks share the scheduler's answers: many at once
// make one call, which covers every job watched, in chunks of 500; one
// within 15 seconds of it makes none; and one after makes another.
func TestPoll(t *testing.T) {
	t.Parallel()
	t.Run("one at a time", func(t *testing.T) {
		t.Parallel()
		f := newFake()
		d, _ := newDriver(t, f)
		var ps []backend.Placement
		for i := range 50 {
			p, _ := place(t, d, "a"+strconv.Itoa(i))
			ps = append(ps, p)
		}
		f.hold = make(chan struct{})
		var wg sync.WaitGroup
		for _, p := range ps {
			wg.Go(func() {
				if st, err := d.Check(t.Context(), testMember, p); err != nil || st.State != backend.Queued {
					t.Errorf("%s: %+v, %v", p.Attempt, st, err)
				}
			})
		}
		waitFor(t, func() bool { return f.calls() == 1 })
		close(f.hold)
		wg.Wait()
		if f.calls() != 1 || len(f.statuses[0]) != 50 {
			t.Errorf("50 checks at once asked the queue %d times, about %d jobs", f.calls(), len(f.statuses[0]))
		}
		if _, err := d.Check(t.Context(), testMember, ps[0]); err != nil || f.calls() != 1 {
			t.Errorf("a check within 15 seconds asked again: %d calls, %v", f.calls(), err)
		}
		d.timing.poll = time.Nanosecond
		if _, err := d.Check(t.Context(), testMember, ps[0]); err != nil || f.calls() != 2 {
			t.Errorf("a check after the snapshot is old: %d calls, %v", f.calls(), err)
		}
	})
	t.Run("chunks", func(t *testing.T) {
		t.Parallel()
		f := newFake()
		d, _ := newDriver(t, f)
		var p backend.Placement
		for i := range 1201 {
			id := strconv.Itoa(1000 + i)
			d.init()
			d.poll.watch(id, "")
			f.set(id, JobStatus{State: backend.Running})
		}
		p, _ = place(t, d, "a1")
		if _, err := d.Check(t.Context(), testMember, p); err != nil {
			t.Fatal(err)
		}
		var sizes []int
		for _, ids := range f.statuses {
			sizes = append(sizes, len(ids))
		}
		if !slices.Equal(sizes, []int{500, 500, 202}) {
			t.Errorf("the queue was asked in chunks of %v", sizes)
		}
	})
}

// waitFor waits until cond holds.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		runtime.Gosched()
	}
}

func TestFind(t *testing.T) {
	t.Parallel()
	t.Run("recorded", func(t *testing.T) {
		t.Parallel()
		f := newFake()
		d, _ := newDriver(t, f)
		placed, _ := place(t, d, "a1")
		p, found, err := d.Find(t.Context(), testMember, "a1")
		if err != nil || !found || p != placed {
			t.Errorf("Find: %+v, %v, %v; placed %+v", p, found, err, placed)
		}
	})
	t.Run("started only", func(t *testing.T) {
		t.Parallel()
		f := newFake()
		d, _ := newDriver(t, f)
		placed, sub := place(t, d, "a1")
		if err := os.Remove(filepath.Join(sub, JobFile)); err != nil {
			t.Fatal(err)
		}
		writeRecords(t, sub, records{started: &Started{Job: "101", Start: time.Now()}})
		p, found, err := d.Find(t.Context(), testMember, "a1")
		if err != nil || !found || p != placed {
			t.Errorf("Find: %+v, %v, %v; placed %+v", p, found, err, placed)
		}
	})
	t.Run("by name", func(t *testing.T) {
		t.Parallel()
		f := newFake()
		d, _ := newDriver(t, f)
		placed, sub := place(t, d, "a1")
		// A coordinator stopped after the scheduler took the job, before
		// the driver recorded its ID.
		if err := os.Remove(filepath.Join(sub, JobFile)); err != nil {
			t.Fatal(err)
		}
		f.named["meridian-a1-1"] = []string{"101"}
		p, found, err := d.Find(t.Context(), testMember, "a1")
		if err != nil || !found || p != placed {
			t.Errorf("Find: %+v, %v, %v; placed %+v", p, found, err, placed)
		}
		if recordedJob(sub) != "101" {
			t.Error("Find didn't record the job it found")
		}
	})
	t.Run("several", func(t *testing.T) {
		t.Parallel()
		f := newFake()
		d, _ := newDriver(t, f)
		_, sub1 := place(t, d, "a1")
		placed, err := d.Place(t.Context(), testMember, backend.PlaceRequest{Attempt: "a1", BootstrapToken: "second"})
		if err != nil {
			t.Fatal(err)
		}
		p, found, err := d.Find(t.Context(), testMember, "a1")
		if err != nil || !found || p.ID != placed.ID {
			t.Errorf("Find: %+v, %v, %v; placed %+v", p, found, err, placed)
		}
		if !slices.Equal(f.cancelled, []string{"101"}) {
			t.Errorf("cancelled %v, want the older job", f.cancelled)
		}
		if r := readRecords(sub1); r.withdrawn == nil {
			t.Error("the older job isn't recorded as withdrawn")
		}
	})
	t.Run("none", func(t *testing.T) {
		t.Parallel()
		f := newFake()
		d, _ := newDriver(t, f)
		for _, attempt := range []string{"a1", "../x"} {
			if p, found, err := d.Find(t.Context(), testMember, attempt); err != nil || found {
				t.Errorf("Find(%q): %+v, %v, %v", attempt, p, found, err)
			}
		}
		// A submission whose job the scheduler never had.
		sub, _, err := d.newSubmission("a2")
		if err != nil {
			t.Fatal(err)
		}
		if p, found, err := d.Find(t.Context(), testMember, "a2"); err != nil || found {
			t.Errorf("Find with a submission without its job: %+v, %v, %v (%s)", p, found, err, sub)
		}
	})
}

func TestCancelForget(t *testing.T) {
	t.Parallel()
	f := newFake()
	d, _ := newDriver(t, f)
	p, sub := place(t, d, "a1")
	f.set("101", JobStatus{State: backend.Running})
	if err := d.Forget(t.Context(), testMember, p); err == nil {
		t.Error("Forget of a running job")
	}
	if err := d.Cancel(t.Context(), testMember, p); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.cancelled, []string{"101"}) {
		t.Errorf("cancelled %v", f.cancelled)
	}
	if _, err := os.Lstat(filepath.Join(sub, TokenFile)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("after Cancel, the token's file: %v", err)
	}
	fresh(d)
	if st, err := d.Check(t.Context(), testMember, p); err != nil || st != (backend.Status{State: backend.Dead, End: backend.EndWithdrawn}) {
		t.Errorf("the cancelled job: %+v, %v", st, err)
	}
	if err := d.Forget(t.Context(), testMember, p); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(d.Dir, "a1")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("after Forget, the attempt's directory: %v", err)
	}

	// Forget removes the token of a job that ended without taking it.
	p, sub = place(t, d, "a2")
	f.set("102", JobStatus{State: backend.Dead, End: backend.EndNodeFailed})
	fresh(d)
	if err := d.Forget(t.Context(), testMember, p); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(sub); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("after Forget, the submission: %v", err)
	}
}

// TestOtherCluster checks that a placement made on another cluster isn't
// taken for one of this cluster's jobs, whose IDs it may share.
func TestOtherCluster(t *testing.T) {
	t.Parallel()
	f := newFake()
	d, _ := newDriver(t, f)
	p, _ := place(t, d, "a1")
	other, _ := newDriver(t, f)
	other.Dir = d.Dir
	if st, err := other.Check(t.Context(), testMember, p); err == nil || st.State != backend.Unknown {
		t.Errorf("a check with another key: %+v, %v", st, err)
	}
	if err := other.Cancel(t.Context(), testMember, p); err == nil || len(f.cancelled) > 0 {
		t.Errorf("a cancel with another key: %v, cancelled %v", err, f.cancelled)
	}
}

func TestSweep(t *testing.T) {
	t.Parallel()
	f := newFake()
	d, _ := newDriver(t, f)
	_, ended := place(t, d, "a1")
	_, running := place(t, d, "a2")
	_, abandoned := place(t, d, "a4")
	_, withdrawn := place(t, d, "a5")
	// A job whose allocation ended a day ago, without its exit record, as
	// one its time limit killed; and one whose allocation runs on, quietly,
	// past 30 days.
	_, endedQuietly := place(t, d, "a6")
	_, long := place(t, d, "a7")
	writeRecords(t, endedQuietly, records{started: &Started{Job: "106", Start: time.Now().Add(-26 * time.Hour), End: time.Now().Add(-25 * time.Hour)}})
	writeRecords(t, long, records{started: &Started{Job: "107", Start: time.Now().Add(-40 * 24 * time.Hour), End: time.Now().Add(time.Hour)}})
	writeRecords(t, ended, records{exit: &Exit{At: time.Now()}})
	writeRecords(t, withdrawn, records{withdrawn: &Withdrawn{At: time.Now()}})
	age := func(d time.Duration, paths ...string) {
		t.Helper()
		old := time.Now().Add(-d)
		for _, path := range paths {
			if err := os.Chtimes(path, old, old); err != nil {
				t.Fatal(err)
			}
		}
	}
	age(25*time.Hour, filepath.Join(ended, ExitFile), running, filepath.Join(withdrawn, WithdrawnFile))
	// A job killed without its exit record, as at its time limit, whose
	// submission nothing has changed for a month.
	entries, err := os.ReadDir(abandoned)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		age(31*24*time.Hour, filepath.Join(abandoned, e.Name()))
	}
	age(31*24*time.Hour, abandoned)
	for _, sub := range []string{long, endedQuietly} {
		entries, err := os.ReadDir(sub)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			age(31*24*time.Hour, filepath.Join(sub, e.Name()))
		}
		age(31*24*time.Hour, sub)
	}
	// A fresh driver sweeps as it first places a job.
	d2, _ := newDriver(t, f)
	d2.Dir, d2.Key = d.Dir, d.Key
	place(t, d2, "a3")
	for _, gone := range []string{"a1", "a4", "a5", "a6"} {
		if _, err := os.Lstat(filepath.Join(d.Dir, gone)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("the attempt %s: %v", gone, err)
		}
	}
	for _, kept := range []string{running, long} {
		if _, err := os.Lstat(kept); err != nil {
			t.Errorf("the attempt whose job may run: %v", err)
		}
	}
}

func TestSaid(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"sbatch: error: Batch job submission failed: Invalid qos specification\n": "sbatch: error: Batch job submission failed: Invalid qos specification",
		"a\x1b[31mred\tb":                `a\x1b[31mred\tb`,
		strings.Repeat("é", 300):         strings.Repeat("é", 256),
		"/bin/sh: 1: exec: x: not found": "/bin/sh: 1: exec: x: not found",
	} {
		if got := Said(in); got != want {
			t.Errorf("Said(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLastLine(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	out := strings.Repeat("noise\n", 2000) + "/bin/sh: 2: exec: /home/u/.cache/meridian/bin/x/meridian: Permission denied\n\n"
	if err := os.WriteFile(filepath.Join(dir, OutFile), []byte(out), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, want := lastLine(dir), "/bin/sh: 2: exec: /home/u/.cache/meridian/bin/x/meridian: Permission denied"; got != want {
		t.Errorf("lastLine = %q, want %q", got, want)
	}
	if got := lastLine(t.TempDir()); got != "" {
		t.Errorf("lastLine without output = %q", got)
	}
}

func TestPlacementID(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		id     string
		tag, j string
		ok     bool
	}{
		{"job:abc/123", "abc", "123", true},
		{"job:abc/123.server", "abc", "123.server", true},
		{"login:abc/123", "", "", false},
		{"job:/123", "", "", false},
		{"job:abc/", "", "", false},
		{"12:34", "", "", false},
	} {
		tag, j, ok := parseID(tc.id)
		if tag != tc.tag || j != tc.j || ok != tc.ok {
			t.Errorf("parseID(%q) = %q, %q, %v", tc.id, tag, j, ok)
		}
	}
	if id := placementID("abc", "9"); id != "job:abc/9" {
		t.Errorf("placementID = %q", id)
	}
}

// TestCheckStuckAgain checks that a job found pending for a reason that
// won't clear, and then for one that will, as 23.11 says InvalidAccount
// of a job waiting for nodes, waits the whole time again once it is found
// stuck again.
func TestCheckStuckAgain(t *testing.T) {
	t.Parallel()
	f := newFake()
	d, clock := newDriver(t, f)
	p, _ := place(t, d, "a1")
	check := func(st JobStatus) backend.Status {
		t.Helper()
		f.set("101", st)
		fresh(d)
		got, err := d.Check(t.Context(), testMember, p)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	stuck := JobStatus{State: backend.Queued, Reason: "InvalidAccount", Stuck: true}
	check(stuck)
	clock.advance(defaultTiming.stuck - time.Second)
	check(JobStatus{State: backend.Queued, Reason: "Resources"})
	clock.advance(time.Hour)
	if st := check(stuck); st.State != backend.Queued {
		t.Fatalf("found stuck again: %+v", st)
	}
	clock.advance(defaultTiming.stuck - time.Second)
	if st := check(stuck); st.State != backend.Queued || len(f.cancelled) > 0 {
		t.Fatalf("a second before its time again: %+v, cancelled %v", st, f.cancelled)
	}
	clock.advance(time.Second)
	if st := check(stuck); st.State != backend.Dead {
		t.Fatalf("its time again: %+v", st)
	}
}

// TestCheckSaid checks that what a scheduler said of a job is shown on one
// line, its control characters escaped.
func TestCheckSaid(t *testing.T) {
	t.Parallel()
	f := newFake()
	d, clock := newDriver(t, f)
	p, _ := place(t, d, "a1")
	f.set("101", JobStatus{State: backend.Queued, Reason: "Bad\x1b[31m\nConstraints", Stuck: true})
	if _, err := d.Check(t.Context(), testMember, p); err != nil {
		t.Fatal(err)
	}
	clock.advance(defaultTiming.stuck)
	fresh(d)
	st, err := d.Check(t.Context(), testMember, p)
	if err != nil {
		t.Fatal(err)
	}
	if got := kindtest.Want(t, st.Problem, problems.SchedulerJobCannotStart); strings.ContainsAny(got.Params["reason"].(string), "\x1b\n") {
		t.Errorf("the reason is %q", got.Params["reason"])
	}
}

// TestInfoCached checks that the scheduler's Info is kept, and a failure
// for a while: one that passes gives the last good Info, a problem itself;
// and one caller at a time asks.
func TestInfoCached(t *testing.T) {
	t.Parallel()
	f := newFake()
	d, _ := newDriver(t, f)
	d.timing.facts = time.Nanosecond
	if _, err := d.info(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.infoErr = errors.New("Unable to contact slurm controller (connect failure)")
	f.mu.Unlock()
	infos := func() int {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.infos
	}
	if info, err := d.info(t.Context()); err != nil || info.Cluster != "test" {
		t.Errorf("with the controller down: %+v, %v", info, err)
	}
	waitFor(t, func() bool { return infos() == 2 })
	for range 3 {
		if info, err := d.info(t.Context()); err != nil || info.Cluster != "test" {
			t.Errorf("with the controller down: %+v, %v", info, err)
		}
	}
	if infos() != 2 {
		t.Errorf("Info was asked %d times, want 2: the failure is kept", infos())
	}
	// While a fetch hangs, a caller gets the last good Info at once.
	d.timing.failed = time.Nanosecond
	f.mu.Lock()
	f.infoHold = make(chan struct{})
	f.mu.Unlock()
	if info, err := d.info(t.Context()); err != nil || info.Cluster != "test" {
		t.Errorf("as a fetch hangs: %+v, %v", info, err)
	}
	f.mu.Lock()
	close(f.infoHold)
	f.infoHold = nil
	f.mu.Unlock()
	d2, _ := newDriver(t, f)
	f.mu.Lock()
	f.infoErr = problems.WorkerBinaryUnsafe.Problem(problems.Params{"path": "/usr/bin/sbatch", "code": "file_changeable"})
	f.mu.Unlock()
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			_, err := d2.info(t.Context())
			kindtest.Want(t, err, problems.WorkerBinaryUnsafe)
		})
	}
	wg.Wait()
	if n := infos(); n > 5 {
		t.Errorf("20 callers at once asked %d times", n-3)
	}
}

// TestForgetUnknown checks that Forget keeps a job the scheduler can't
// say of.
func TestForgetUnknown(t *testing.T) {
	t.Parallel()
	f := newFake()
	d, _ := newDriver(t, f)
	p, sub := place(t, d, "a1")
	f.set("101", JobStatus{State: backend.Unknown, Reason: "WEIRD"})
	if err := d.Forget(t.Context(), testMember, p); err == nil {
		t.Error("Forget of a job in a state the driver doesn't know")
	}
	if _, err := os.Lstat(sub); err != nil {
		t.Errorf("the submission: %v", err)
	}
}

// TestAccountingOpen checks that accounting's pending or running record
// of a job the queue no longer lists is taken for none: one a controller
// that stopped never closed.
func TestAccountingOpen(t *testing.T) {
	t.Parallel()
	f := newFake()
	f.info.Accounting = true
	d, clock := newDriver(t, f)
	p, _ := place(t, d, "a1")
	f.mu.Lock()
	clear(f.queue)
	f.acct["101"] = JobStatus{State: backend.Running}
	f.mu.Unlock()
	if st, err := d.Check(t.Context(), testMember, p); st.State != backend.Unknown || err == nil {
		t.Fatalf("%+v, %v", st, err)
	}
	clock.advance(defaultTiming.grace)
	st, err := d.Check(t.Context(), testMember, p)
	if err != nil {
		t.Fatal(err)
	}
	kindtest.Want(t, st.Problem, problems.SchedulerWorkerNotStarted)
}

// TestGoneProblemFirst checks that a gone job's exit record's problem wins
// over its withdrawal, as for a job the scheduler still lists.
func TestGoneProblemFirst(t *testing.T) {
	t.Parallel()
	f := newFake()
	d, _ := newDriver(t, f)
	p, sub := place(t, d, "a1")
	delete(f.queue, "101")
	writeRecords(t, sub, records{
		withdrawn: &Withdrawn{At: time.Now()},
		exit:      &Exit{Status: -1, At: time.Now(), Problem: Record(problems.WorkerBinaryUnsafe.Problem(problems.Params{"path": "/x", "code": "file_changeable"}))},
	})
	st, err := d.Check(t.Context(), testMember, p)
	if err != nil {
		t.Fatal(err)
	}
	kindtest.Want(t, st.Problem, problems.WorkerBinaryUnsafe)
}
