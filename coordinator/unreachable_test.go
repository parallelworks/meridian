package coordinator

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/parallelworks/meridian/backend"
	backendssh "github.com/parallelworks/meridian/backend/ssh"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/sshtest"
	"github.com/parallelworks/meridian/plan"
)

// stalledHostJobs are as many jobs as the coordinator works at once
// (sharedWorkers), each on the host farm.
var stalledHostJobs = func() string {
	var b strings.Builder
	b.WriteString("jobs:\n")
	for i := range sharedWorkers {
		fmt.Fprintf(&b, "  j%d:\n    ssh:\n      remoteHost: farm\n    steps:\n      - run: \"true\"\n", i)
	}
	return b.String()
}()

// TestStalledHostHoldsNoWorkers checks that a host that doesn't answer,
// as one whose network drops what it is sent, doesn't hold the workers
// that every run's supervisors share while the jobs placed on it are
// probed: another run, on the local backend, is placed and ends at once
// meanwhile, rather than once the host's probes have each tried it in
// full, which, before, held every worker for as long as they took.
func TestStalledHostHoldsNoWorkers(t *testing.T) {
	parallel(t)
	if _, err := osexec.LookPath("ssh"); err != nil {
		t.Skip("no ssh client to read the SSH config with")
	}
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(k)
	if err != nil {
		t.Fatal(err)
	}
	server := sshtest.New(t, sshtest.Options{Authorized: []ssh.PublicKey{signer.PublicKey()}})
	server.Stall()
	t.Cleanup(server.Resume)
	client := t.TempDir()
	block, err := ssh.MarshalPrivateKey(k, "")
	if err != nil {
		t.Fatal(err)
	}
	keyFile, knownHosts, config := filepath.Join(client, "id_ecdsa"), filepath.Join(client, "known_hosts"), filepath.Join(client, "config")
	for path, data := range map[string][]byte{
		keyFile:    pem.EncodeToMemory(block),
		knownHosts: []byte(server.KnownHosts("")),
		config: fmt.Appendf(nil, `Host farm
  HostName %s
  Port %s
  IdentityFile %s
  IdentitiesOnly yes
  IdentityAgent none
  UserKnownHostsFile %s
  GlobalKnownHostsFile /dev/null
  ConnectTimeout 2
`, server.Host(), server.Port(), keyFile, knownHosts),
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	key := make([]byte, backendssh.MinKeyLen)
	_, _ = rand.Read(key)
	sshDriver := backendssh.New(backendssh.Options{
		ConfigFile: config, Home: client, User: "tester", Getenv: func(string) string { return "" },
		Version: "test", Binaries: t.TempDir(), Store: "teststore", Key: key,
	})
	t.Cleanup(func() { _ = sshDriver.Close() })
	c := open(t, Options{Drivers: map[string]backend.Driver{testBackend: newTestDriver(), "ssh": sshDriver}})
	stalled, _ := create(t, c, stalledHostJobs, nil, nil)
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
	if took := time.Since(began); took > 5*time.Second {
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
