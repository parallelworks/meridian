package coordinator

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	osexec "os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/parallelworks/meridian/backend"
	backendssh "github.com/parallelworks/meridian/backend/ssh"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/sshtest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// hostJobs are n jobs, each on the host farm.
func hostJobs(n int) string {
	var b strings.Builder
	b.WriteString("jobs:\n")
	for i := range n {
		fmt.Fprintf(&b, "  j%d:\n    ssh:\n      remoteHost: farm\n    steps:\n      - run: \"true\"\n", i)
	}
	return b.String()
}

// farmDriver returns the ssh backend's driver, which reaches the host farm
// at addr, as the SSH config it reads says, with a connect timeout of 2
// seconds, and whose known_hosts has server's keys for it, if there is
// a server.
func farmDriver(t *testing.T, addr string, server *sshtest.Server) *backendssh.Driver {
	t.Helper()
	if _, err := osexec.LookPath("ssh"); err != nil {
		t.Skip("no ssh client to read the SSH config with")
	}
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	client := t.TempDir()
	block, err := ssh.MarshalPrivateKey(k, "")
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	knownHostsLines := ""
	if server != nil {
		knownHostsLines = server.KnownHosts("")
	}
	keyFile, knownHosts, config := filepath.Join(client, "id_ecdsa"), filepath.Join(client, "known_hosts"), filepath.Join(client, "config")
	for path, data := range map[string][]byte{
		keyFile:    pem.EncodeToMemory(block),
		knownHosts: []byte(knownHostsLines),
		config: fmt.Appendf(nil, `Host farm
  HostName %s
  Port %s
  IdentityFile %s
  IdentitiesOnly yes
  IdentityAgent none
  UserKnownHostsFile %s
  GlobalKnownHostsFile /dev/null
  ConnectTimeout 2
`, host, port, keyFile, knownHosts),
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	key := make([]byte, backendssh.MinKeyLen)
	_, _ = rand.Read(key)
	d := backendssh.New(backendssh.Options{
		ConfigFile: config, Home: client, User: "tester", Getenv: func(string) string { return "" },
		Version: "test", Binaries: t.TempDir(), Store: "teststore", Key: key,
	})
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// TestStalledHostHoldsNoWorkers checks that a host that doesn't answer,
// as one whose network drops what it is sent, doesn't hold the workers
// that every run's supervisors share while the jobs placed on it are
// probed: another run, on the local backend, is placed and ends at once
// meanwhile, rather than once the host's probes have each tried it in
// full, which, before, held every worker for as long as they took.
func TestStalledHostHoldsNoWorkers(t *testing.T) {
	parallel(t)
	server := sshtest.New(t, sshtest.Options{})
	server.Stall()
	t.Cleanup(server.Resume)
	sshDriver := farmDriver(t, server.Addr(), server)
	c := open(t, Options{Drivers: map[string]backend.Driver{testBackend: newTestDriver(), "ssh": sshDriver}})
	stalled, _ := create(t, c, hostJobs(sharedWorkers), nil, nil)
	serve(t, c)
	// The stalled host's jobs are being placed, each probing the host.
	waitFor(t, func() bool { return !storedRun(t, c, stalled).StartedAt.IsZero() })
	time.Sleep(time.Second) //nolint:forbidigo // the supervisors' first looks, which nothing signals
	began := time.Now()
	local, _ := create(t, c, oneJob, nil, nil)
	for deadline := began.Add(time.Minute); storedRun(t, c, local).State != store.RunEnded; {
		if time.Now().After(deadline) {
			t.Fatal("a run on the local backend didn't end while a host that doesn't answer was probed")
		}
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo // what is waited for has no signal
	}
	// Held by the probes, it took about 29 s; not held, about 1 s, and
	// several under the race detector on a loaded runner.
	if took := time.Since(began); took > 15*time.Second {
		t.Errorf("a run on the local backend took %v to end while a host that doesn't answer was probed", took)
	}
	if r := storedRun(t, c, local); r.Outcome != plan.OutcomeSuccess {
		t.Errorf("the run on the local backend ended %s", r.Outcome)
	}
}

// triesDriver is a testDriver whose Probe gives answers, in order, before
// it answers as testDriver does, and records the tries each was asked
// with (backend.Demand.Tries).
type triesDriver struct {
	*testDriver
	mu      sync.Mutex
	answers []error
	tries   []int
}

func (d *triesDriver) Probe(ctx context.Context, m *backend.Member, demand backend.Demand) (backend.Fit, error) {
	d.mu.Lock()
	d.tries = append(d.tries, demand.Tries)
	var err error
	if len(d.answers) > 0 {
		err, d.answers = d.answers[0], d.answers[1:]
	}
	d.mu.Unlock()
	if err != nil {
		return backend.Fit{}, err
	}
	return d.testDriver.Probe(ctx, m, demand)
}

// TestNotYetUncounted checks that a member's backend that hasn't tried the
// job yet, but will (backend.ErrNotYet), is asked again, as many times as
// it says so, without the look's counting as one that couldn't answer, of
// which there are as many as the supervision allows; and that a Probe is
// given the job's count of those (backend.Demand.Tries), by which a
// driver counts the job's own tries.
func TestNotYetUncounted(t *testing.T) {
	parallel(t)
	notYet := fmt.Errorf("test: %w", backend.ErrNotYet)
	unanswered := errors.New("test: no answer")
	d := &triesDriver{testDriver: newTestDriver()}
	for range fastSupervision.tries + 2 {
		d.answers = append(d.answers, notYet)
	}
	d.answers = append(d.answers, unanswered, unanswered)
	c := open(t, Options{Drivers: map[string]backend.Driver{testBackend: d}})
	d.c, d.ctx = c, t.Context()
	t.Cleanup(d.workers.Wait)
	if r := runToEnd(t, c, oneJob, nil); r.Outcome != plan.OutcomeSuccess {
		t.Fatalf("the run ended %s", r.Outcome)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	want := make([]int, fastSupervision.tries+3)
	want = append(want, 1, 2)
	if !slices.Equal(d.tries, want) {
		t.Errorf("Probe was given the tries %v, want %v", d.tries, want)
	}
}

// probeTries runs a job whose member's backend gives answers, in order,
// before it answers as testDriver does, and checks that the run succeeds,
// and that the job's looks gave Probe the tries want
// (backend.Demand.Tries).
func probeTries(t *testing.T, answers []error, want []int) {
	t.Helper()
	d := &triesDriver{testDriver: newTestDriver(), answers: answers}
	c := open(t, Options{Drivers: map[string]backend.Driver{testBackend: d}})
	d.c, d.ctx = c, t.Context()
	t.Cleanup(d.workers.Wait)
	if r := runToEnd(t, c, oneJob, nil); r.Outcome != plan.OutcomeSuccess {
		t.Fatalf("the run ended %s", r.Outcome)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !slices.Equal(d.tries, want) {
		t.Errorf("Probe was given the tries %v, want %v", d.tries, want)
	}
}

// TestManyFailuresAtOnce checks that a look that counts many failures to
// reach a member at once, as one after its supervisor stalled, takes the
// job's tries no further than the one before its last, which the driver,
// given them, can say it gave up at: not past the supervision's, which
// would fail the job with placement_unanswered first.
func TestManyFailuresAtOnce(t *testing.T) {
	parallel(t)
	refused := errors.New("test: refused")
	notYet := fmt.Errorf("test: %w", backend.ErrNotYet)
	probeTries(t, []error{
		&backend.FailedError{Failed: 1, Err: refused},
		&backend.FailedError{Failed: 100, Err: notYet},
	}, []int{0, 1, fastSupervision.tries - 1})
}

// TestGiveUpForgets checks that a job that gave up on its member, with
// placement_unanswered, counts none of the failures to reach it it saw
// before at the first look of its next placement, as any job's first.
func TestGiveUpForgets(t *testing.T) {
	parallel(t)
	refused := errors.New("test: refused")
	notYet := fmt.Errorf("test: %w", backend.ErrNotYet)
	var answers []error
	want := make([]int, 0, fastSupervision.tries+2)
	for i := range fastSupervision.tries {
		answers = append(answers, &backend.FailedError{Failed: i + 1, Err: refused})
		want = append(want, i)
	}
	answers = append(answers, &backend.FailedError{Failed: 100, Err: notYet})
	probeTries(t, answers, append(want, 0, 0))
}

// retryAtDriver is a testDriver whose Probe answers, until retryAt, that
// it is reaching the member again then, as a driver waiting out its
// backoff after a failure does, and counts the looks it answered so.
type retryAtDriver struct {
	*testDriver
	retryAt time.Time
	waiting atomic.Int32
}

func (d *retryAtDriver) Probe(ctx context.Context, m *backend.Member, demand backend.Demand) (backend.Fit, error) {
	if time.Now().Before(d.retryAt) {
		d.waiting.Add(1)
		return backend.Fit{}, &backend.FailedError{RetryAt: d.retryAt, Err: fmt.Errorf("test: %w", backend.ErrNotYet)}
	}
	return d.testDriver.Probe(ctx, m, demand)
}

// TestNotYetUntilRetry checks that a job whose member's backend will try
// the member at a time it says (backend.FailedError.RetryAt) is looked at
// again about then: not at each of the supervision's shortest waits
// meanwhile, nor much later.
func TestNotYetUntilRetry(t *testing.T) {
	parallel(t)
	d := &retryAtDriver{testDriver: newTestDriver()}
	c := open(t, Options{Drivers: map[string]backend.Driver{testBackend: d}})
	d.c, d.ctx = c, t.Context()
	t.Cleanup(d.workers.Wait)
	c.supervision.most = time.Minute
	d.retryAt = time.Now().Add(time.Second)
	r := runToEnd(t, c, oneJob, nil)
	if r.Outcome != plan.OutcomeSuccess {
		t.Fatalf("the run ended %s", r.Outcome)
	}
	// The first look, and one more at most, as one that comes before
	// RetryAt by less than the clock tells.
	if n := d.waiting.Load(); n > 2 {
		t.Errorf("the job was looked at %d times before its member was to be tried", n)
	}
	if late := r.EndedAt.Sub(d.retryAt); late > 5*time.Second {
		t.Errorf("the run ended %v after its member was to be tried", late)
	}
}

// cancelLaterDriver is a testDriver that can't cancel a placement the
// first fails times it is asked, as one whose host it is reaching again,
// and records the placements it cancelled.
type cancelLaterDriver struct {
	*testDriver
	mu        sync.Mutex
	fails     int
	asked     int
	cancelled []string
}

func (d *cancelLaterDriver) Cancel(_ context.Context, _ *backend.Member, p backend.Placement) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.asked++; d.asked <= d.fails {
		return fmt.Errorf("test: %w", backend.ErrNotYet)
	}
	d.cancelled = append(d.cancelled, p.Attempt)
	return nil
}

// TestCancelLater checks that a placement a driver couldn't cancel as its
// attempt was withdrawn, as one on a host it is reaching again, is
// cancelled again, in the background, once the driver can.
func TestCancelLater(t *testing.T) {
	parallel(t)
	d := &cancelLaterDriver{testDriver: newTestDriver(), fails: 2}
	c := open(t, Options{Drivers: map[string]backend.Driver{testBackend: d}})
	d.c, d.ctx = c, t.Context()
	serve(t, c)
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.forgetCtx != nil
	})
	m := &backend.Member{Name: "m1", Backend: testBackend}
	w := &withdrawal{cand: &candidate{group: "local", member: m, driver: d}, placement: backend.Placement{Attempt: "a1", ID: "p1"}}
	c.cancelPlacements(t.Context(), "1", map[string]*withdrawal{"a1": w}, []string{"a1"})
	for deadline := time.Now().Add(10 * time.Second); ; {
		d.mu.Lock()
		cancelled, asked := slices.Clone(d.cancelled), d.asked
		d.mu.Unlock()
		if len(cancelled) > 0 {
			if !slices.Equal(cancelled, []string{"a1"}) || asked != 3 {
				t.Errorf("the driver cancelled %v, asked %d times", cancelled, asked)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the placement wasn't cancelled, asked %d times", asked)
		}
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo // the cancel in the background, which nothing signals
	}
}

// TestJobsGiveUpTogether checks that jobs waiting on a host that refuses
// every connection give up on it together, each once it has seen four
// failures to reach it, its own tries and those for the others alike, as
// format 1 gives each step four tries: not one after another, as each
// waited its turn to try it four times itself, which took about four
// tries for each job in all.
func TestJobsGiveUpTogether(t *testing.T) {
	parallel(t)
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var tries atomic.Int32
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			tries.Add(1)
			_ = c.Close()
		}
	}()
	const jobs = 5
	c := open(t, Options{Drivers: map[string]backend.Driver{testBackend: newTestDriver(), "ssh": farmDriver(t, ln.Addr().String(), nil)}})
	// As many looks that couldn't answer as the coordinator allows by
	// default, more than the driver's tries.
	c.supervision.tries = defaultSupervision.tries
	began := time.Now()
	id, _ := create(t, c, hostJobs(jobs), nil, nil)
	serve(t, c)
	for deadline := time.Now().Add(2 * time.Minute); storedRun(t, c, id).State != store.RunEnded; {
		if time.Now().After(deadline) {
			t.Fatalf("the run hasn't ended after %d tries of the host", tries.Load())
		}
		time.Sleep(50 * time.Millisecond) //nolint:forbidigo // what is waited for has no signal
	}
	for i := range jobs {
		j := storedJob(t, c, id, fmt.Sprintf("j%d", i))
		if j.Outcome != plan.OutcomeFailure || len(j.Diagnostics) < 2 || j.Diagnostics[1].Code != problems.SSHUnreachable.Code() {
			t.Errorf("job j%d ended %s, with %v", i, j.Outcome, j.Diagnostics)
		}
	}
	// As many as the driver's, and one more, as one that failed before a
	// job's first look, which counts none it didn't try itself: none of
	// the jobs that waited tried the host, after the first look, itself.
	if n := tries.Load(); n > backendssh.Tries+1 {
		t.Errorf("the jobs gave up after %d tries of the host, in all", n)
	}
	t.Logf("%d jobs gave up after %d tries of the host, in %v", jobs, tries.Load(), time.Since(began))
	// Nothing places them again: the coordinator forgets what it saw of
	// the host as it placed them.
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.seenFailed) == 0
	})
}

// TestSupervisionOutlastsSSH checks that the coordinator asks a backend
// that couldn't answer more times than the ssh backend tries a host for a
// job, so that a job whose host doesn't answer fails with
// ssh_unreachable, which says why, rather than with placement_unanswered.
func TestSupervisionOutlastsSSH(t *testing.T) {
	if defaultSupervision.tries <= backendssh.Tries {
		t.Errorf("the coordinator asks a backend that couldn't answer %d times, the ssh backend tries a host %d", defaultSupervision.tries, backendssh.Tries)
	}
}

// TestProbeCounted checks how a job's look counts a member's failures to
// be reached (backend.FailedError): its first look sees none it can count,
// as one that comes to a host another job's tries have just given up on,
// which a host that comes back then runs, but for its own try that failed,
// which counts once; each look after counts the failures since the
// previous one's, its own among them, once; and each job and member
// counts for itself.
func TestProbeCounted(t *testing.T) {
	c := open(t, Options{})
	wait := fmt.Errorf("test: %w", backend.ErrNotYet)
	failed := errors.New("test: refused")
	retry := time.Now().Add(time.Minute)
	for i, tc := range []struct {
		key     string
		err     error
		counted int
		notYet  bool
	}{
		{"b", &backend.FailedError{Failed: 4, RetryAt: retry, Err: wait}, 0, true},
		{"b", &backend.FailedError{Failed: 4, Err: wait}, 0, true},
		{"b", &backend.FailedError{Failed: 5, Err: failed}, 1, false},
		{"b", &backend.FailedError{Failed: 7, Err: wait}, 2, false},
		{"b", &backend.FailedError{Failed: 7, Err: failed}, 1, false},
		{"c", &backend.FailedError{Failed: 9, Err: failed}, 1, false},
		{"c", &backend.FailedError{Failed: 2, Err: wait}, 0, true},
		{"c", &backend.FailedError{Failed: 3, Err: wait}, 1, false},
		{"d", wait, 0, true},
		{"d", failed, 1, false},
	} {
		counted, notYet, retryAt := c.probeCounted(tc.key, tc.err)
		fe, isFailed := errors.AsType[*backend.FailedError](tc.err)
		if counted != tc.counted || notYet != tc.notYet || isFailed && !retryAt.Equal(fe.RetryAt) {
			t.Errorf("%d, %s: counted %d, not yet %v, retry at %v; want %d, %v", i, tc.key, counted, notYet, retryAt, tc.counted, tc.notYet)
		}
	}
}
