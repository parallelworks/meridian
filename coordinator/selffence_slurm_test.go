//go:build unix

package coordinator

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/backend/batch"
	"github.com/parallelworks/meridian/backend/slurm"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/schedtest"
	"github.com/parallelworks/meridian/plan"
)

// slurmLeaseDriver is a leaseDriver whose placements are batch jobs of a
// fake Slurm cluster, which a batch.Driver submits, finds, checks,
// cancels and forgets, as the slurm backend does, while its workers claim
// their attempts and go silent, as the leaseDriver's do.
type slurmLeaseDriver struct {
	*leaseDriver
	b *batch.Driver
}

func (d *slurmLeaseDriver) Place(ctx context.Context, m *backend.Member, req backend.PlaceRequest) (backend.Placement, error) {
	p, err := d.b.Place(ctx, m, req)
	if err != nil {
		return p, err
	}
	// The leaseDriver's worker claims the attempt once it is placed.
	if _, err := d.leaseDriver.Place(ctx, m, req); err != nil {
		return backend.Placement{}, err
	}
	return p, nil
}

func (d *slurmLeaseDriver) Find(ctx context.Context, m *backend.Member, attempt string) (backend.Placement, bool, error) {
	return d.b.Find(ctx, m, attempt)
}

func (d *slurmLeaseDriver) Check(ctx context.Context, m *backend.Member, p backend.Placement) (backend.Status, error) {
	return d.b.Check(ctx, m, p)
}

func (d *slurmLeaseDriver) Cancel(ctx context.Context, m *backend.Member, p backend.Placement) error {
	return d.b.Cancel(ctx, m, p)
}

func (d *slurmLeaseDriver) EndWait() time.Duration { return d.b.EndWait() }

func (d *slurmLeaseDriver) Forget(ctx context.Context, m *backend.Member, p backend.Placement) error {
	return d.b.Forget(ctx, m, p)
}

// TestSelfFenceSlurm checks a self-fencing attempt whose Slurm job its
// scheduler timed out, with the slurm backend's own driver, over a fake
// cluster: the backend's snapshot of the queue, taken before, still says
// the job waits as the attempt's lease and the fence margin pass, so the
// coordinator asks again; once the snapshot is new, it says TIMEOUT, and
// the job fails with timeout, though its retry: lists worker-lost.
func TestSelfFenceSlurm(t *testing.T) {
	parallel(t)
	cluster := schedtest.New(t, func(s *schedtest.State) { s.Hold = true })
	key := make([]byte, batch.MinKeyLen)
	_, _ = rand.Read(key)
	b := &batch.Driver{
		Scheduler: &slurm.Scheduler{Environ: cluster.Env(nil)}, Dir: t.TempDir(), Key: key,
		Binary:  func() (string, string, error) { return "/bin/false", testDigest, nil },
		Environ: cluster.Env(nil),
	}
	clock := newFakeClock()
	c, ld := openLease(t, clock, []bool{true}, []bool{true})
	c.fenceCheck = 5 * time.Second
	d := &slurmLeaseDriver{leaseDriver: ld, b: b}
	c.opts.Drivers[testBackend] = d
	id, first := startJob(t, c, retryJob, store.AttemptRunning)
	// A snapshot of the queue, as the job waits.
	member := &backend.Member{Name: "m1", Backend: testBackend}
	if st, err := b.Check(t.Context(), member, backend.Placement{Attempt: first.ID, ID: first.Placement.ID}); err != nil || st.State != backend.Queued {
		t.Fatalf("the job is %+v, %v", st, err)
	}
	cluster.SetJob(t, 2, func(j *schedtest.Job) {
		j.State, j.Start, j.End = "TIMEOUT", time.Now().Add(-time.Minute), time.Now()
	})
	clock.Advance(c.timing.Lease + c.timing.FenceMargin + time.Second)
	deadline := time.Now().Add(time.Minute)
	for {
		wait := look(t, c, id)
		if a := latestAttempt(t, c, id, "main"); a.State == store.AttemptEnded {
			if a.ID != first.ID || a.End.Reason != plan.ReasonTimeout || wait != 0 {
				t.Fatalf("the attempt ended %+v, and the supervisor waits %v", a.End, wait)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the attempt wasn't lost")
		}
		// The coordinator's clock goes on slower than the snapshot's age,
		// which is real time, within the driver's EndWait.
		clock.Advance(time.Second)
		time.Sleep(time.Second) //nolint:forbidigo // the snapshot ages in real time, which the test waits out
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeFailure || j.Reason != plan.ReasonTimeout {
		t.Errorf("the job is %+v", j)
	}
}
