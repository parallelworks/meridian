package store

import (
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/coordtest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// credential returns a worker's credential for a test: 64 hex digits.
func credential(c byte) string {
	b := make([]byte, 64)
	for i := range b {
		b[i] = c
	}
	return string(b)
}

var testWorker = workerapi.Worker{Version: "v1.2.3", Digest: "sha256:00"}

func TestAttempt(t *testing.T) {
	t.Parallel()
	s := open(t)
	id := createRun(t, s)
	var a *Attempt
	var token string
	inTx(t, s, func(tx *Tx) error {
		var err error
		a, token, err = placedAttempt(tx, NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		return err
	})
	switch {
	case !hexID.MatchString(a.ID) || len(token) != 64:
		t.Errorf("the attempt's ID is %q and its token %q", a.ID, token)
	case a.Run != id || a.Job != "build" || a.Number != 1 || a.Fence != 1 || a.State != AttemptPlacing || !a.Holds:
		t.Errorf("a new attempt is %+v", a)
	case a.SelfFencing || a.LeaseSeconds != 60 || a.HeartbeatSeconds != 10 || a.CreatedAt.IsZero() || !a.ClaimedAt.IsZero() || !a.LeaseLapses().IsZero():
		t.Errorf("a new attempt is %+v", a)
	}

	inTx(t, s, func(tx *Tx) error {
		// The store keeps the token's digest, not the token.
		var stored []byte
		if err := tx.tx.QueryRowContext(tx.ctx, "SELECT token_hash FROM meridian_attempts").Scan(&stored); err != nil {
			return err
		}
		if sum := sha256.Sum256([]byte(token)); string(stored) != string(sum[:]) {
			t.Error("the store doesn't keep the token's SHA-256 digest")
		}
		if _, err := tx.Authorize(credential('a')); !errors.Is(err, ErrNotFound) {
			t.Errorf("Authorize before the claim = %v, want ErrNotFound", err)
		}
		if err := tx.StepStarted(a.ID); !errors.Is(err, ErrState) {
			t.Errorf("StepStarted before the claim = %v, want ErrState", err)
		}
		if _, err := tx.Claim("not a token", credential('a'), testWorker, workerapi.Version); !errors.Is(err, ErrNotFound) {
			t.Errorf("Claim with another token = %v, want ErrNotFound", err)
		}
		if _, err := tx.Claim(token, "short", testWorker, workerapi.Version); err == nil {
			t.Error("Claim with a short credential succeeded")
		}
		if _, err := tx.Claim(token, credential('a'), testWorker, 0); err == nil {
			t.Error("Claim at version 0 of the worker API succeeded")
		}
		if a.APIVersion != 0 {
			t.Errorf("an attempt not claimed is of version %d of the worker API", a.APIVersion)
		}
		// The store keeps whatever version the server claims it at.
		claimed, err := tx.Claim(token, credential('a'), testWorker, 7)
		if err != nil {
			return err
		}
		if claimed.State != AttemptClaimed || claimed.Worker != testWorker || claimed.APIVersion != 7 || claimed.ClaimedAt.IsZero() || !claimed.RenewedAt.Equal(claimed.ClaimedAt) ||
			!claimed.LeaseLapses().Equal(claimed.RenewedAt.Add(time.Minute)) {
			t.Errorf("a claimed attempt is %+v", claimed)
		}
		again, err := tx.Claim(token, credential('a'), workerapi.Worker{Version: "other"}, workerapi.Version)
		if err != nil || again.Worker != testWorker || again.APIVersion != 7 || !again.ClaimedAt.Equal(claimed.ClaimedAt) {
			t.Errorf("Claim repeated = %+v, %v", again, err)
		}
		if _, err := tx.Claim(token, credential('b'), testWorker, workerapi.Version); !errors.Is(err, ErrCredential) {
			t.Errorf("Claim with another credential = %v, want ErrCredential", err)
		}
		if _, err := tx.NewToken(a.ID); !errors.Is(err, ErrState) {
			t.Errorf("NewToken once claimed = %v, want ErrState", err)
		}
		return nil
	})
	inTx(t, s, func(tx *Tx) error {
		// An attempt not claimed doesn't complete.
		b, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "test", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		if err := tx.EndAttempt(b.ID, AttemptEnd{Kind: EndCompleted, Outcome: plan.OutcomeSuccess}); !errors.Is(err, ErrState) {
			t.Errorf("EndAttempt completed before the claim = %v, want ErrState", err)
		}
		return tx.EndAttempt(b.ID, AttemptEnd{Kind: EndSetupFailed, Reason: plan.ReasonSetupFailed})
	})

	inTx(t, s, func(tx *Tx) error {
		renewed, err := tx.Renew(credential('a'))
		if err != nil {
			return err
		}
		if got, err := tx.Authorize(credential('a')); err != nil || !got.RenewedAt.Equal(renewed.RenewedAt) || got.ID != a.ID {
			t.Errorf("Authorize = %+v, %v; renewed at %v", got, err, renewed.RenewedAt)
		}
		if err := tx.StepStarted(a.ID); err != nil {
			return err
		}
		if err := tx.StepStarted(a.ID); err != nil {
			t.Errorf("StepStarted repeated = %v", err)
		}
		got, err := tx.Attempt(a.ID)
		if err != nil {
			return err
		}
		j, err := tx.Job(id, "build")
		if err != nil {
			return err
		}
		if got.State != AttemptRunning || got.StepStartedAt.IsZero() || j.AttemptsStarted != 1 {
			t.Errorf("an attempt whose step started is %+v, its job %+v", got, j)
		}
		for _, end := range []AttemptEnd{
			{Kind: "done"},
			{Kind: EndCompleted, Outcome: "fine"},
			{Kind: EndLost, Report: []byte(`{"seq":0}`)},
		} {
			if err := tx.EndAttempt(a.ID, end); err == nil {
				t.Errorf("EndAttempt(%+v) succeeded", end)
			}
		}
		return tx.EndAttempt(a.ID, AttemptEnd{Kind: EndCompleted, Outcome: plan.OutcomeFailure, Report: []byte(`{"seq":0}`)})
	})

	// An attempt that has ended no longer holds its job: what its worker
	// reports isn't stored, but how it ended can be read.
	inTx(t, s, func(tx *Tx) error {
		for name, err := range map[string]error{
			"Authorize":   second(tx.Authorize(credential('a'))),
			"Renew":       second(tx.Renew(credential('a'))),
			"Claim":       second(tx.Claim(token, credential('a'), testWorker, workerapi.Version)),
			"StepStarted": tx.StepStarted(a.ID),
			"EndAttempt":  tx.EndAttempt(a.ID, AttemptEnd{Kind: EndLost}),
		} {
			if !errors.Is(err, ErrFenced) {
				t.Errorf("%s once ended = %v, want ErrFenced", name, err)
			}
		}
		got, err := tx.AttemptByCredential(credential('a'))
		if err != nil {
			return err
		}
		if got.Holds || got.State != AttemptEnded || got.End.Kind != EndCompleted || got.End.Outcome != plan.OutcomeFailure ||
			string(got.End.Report) != `{"seq":0}` || got.EndedAt.IsZero() {
			t.Errorf("an ended attempt is %+v", got)
		}
		return nil
	})
	checkIntegrity(t, s)
}

// second returns the second of two values, an error.
func second[T any](_ T, err error) error { return err }

// TestFence checks that each attempt of a job has a fence greater than the
// one before's, which becomes its job's, and that an attempt a newer one
// superseded can do nothing more.
func TestFence(t *testing.T) {
	t.Parallel()
	s := open(t)
	id := createRun(t, s)
	type started struct {
		a     *Attempt
		token string
		cred  string
	}
	var attempts []started
	for i := range 4 {
		inTx(t, s, func(tx *Tx) error {
			a, token, err := placedAttempt(tx, NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
			if err != nil {
				return err
			}
			cred := credential(byte('a' + i))
			if i%2 == 0 { // every other attempt is claimed before the next
				if _, err := tx.Claim(token, cred, testWorker, workerapi.Version); err != nil {
					return err
				}
			}
			attempts = append(attempts, started{a, token, cred})
			return nil
		})
	}
	inTx(t, s, func(tx *Tx) error {
		j, err := tx.Job(id, "build")
		if err != nil {
			return err
		}
		if j.Fence != 4 || j.Attempts != 4 {
			t.Errorf("the job's fence is %d after %d attempts, want 4 and 4", j.Fence, j.Attempts)
		}
		for i, st := range attempts {
			a, err := tx.Attempt(st.a.ID)
			if err != nil {
				return err
			}
			if a.Fence != int64(i+1) || a.Number != i+1 {
				t.Errorf("attempt %d has the fence %d and the number %d", i+1, a.Fence, a.Number)
			}
			last := i == len(attempts)-1
			if a.Holds != last || (a.State == AttemptEnded) == last || !last && a.End.Kind != EndSuperseded {
				t.Errorf("attempt %d of %d is %+v", i+1, len(attempts), a)
			}
			if last {
				continue
			}
			if _, err := tx.Claim(st.token, st.cred, testWorker, workerapi.Version); !errors.Is(err, ErrFenced) {
				t.Errorf("Claim by superseded attempt %d = %v, want ErrFenced", i+1, err)
			}
			if i%2 == 0 {
				if _, err := tx.Renew(st.cred); !errors.Is(err, ErrFenced) {
					t.Errorf("Renew by superseded attempt %d = %v, want ErrFenced", i+1, err)
				}
			}
			if _, err := tx.NewToken(st.a.ID); !errors.Is(err, ErrFenced) {
				t.Errorf("NewToken of superseded attempt %d = %v, want ErrFenced", i+1, err)
			}
		}
		return nil
	})

	// A job's fence never goes back, even once the store is opened again.
	path := s.file
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openAt(t, path)
	inTx(t, s, func(tx *Tx) error {
		a, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err == nil && a.Fence != 5 {
			t.Errorf("the attempt after 4 has the fence %d", a.Fence)
		}
		return err
	})
	checkIntegrity(t, s)
}

// TestNewToken checks that an attempt given a new token can't be claimed
// with its old one.
func TestNewToken(t *testing.T) {
	t.Parallel()
	s := open(t)
	id := createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		a, old, err := placedAttempt(tx, NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		token, err := tx.NewToken(a.ID)
		if err != nil {
			return err
		}
		if token == old {
			t.Error("NewToken gave the same token")
		}
		if _, err := tx.Claim(old, credential('a'), testWorker, workerapi.Version); !errors.Is(err, ErrNotFound) {
			t.Errorf("Claim with the old token = %v, want ErrNotFound", err)
		}
		if _, err := tx.Claim(token, credential('a'), testWorker, workerapi.Version); err != nil {
			t.Errorf("Claim with the new token = %v", err)
		}
		if _, err := tx.NewToken("0123"); !errors.Is(err, ErrNotFound) {
			t.Errorf("NewToken of no attempt = %v, want ErrNotFound", err)
		}
		return nil
	})
}

// TestLapse checks that the credential of an attempt whose job fences
// itself lapses with its lease, by the store's clock, and that another's
// doesn't.
func TestLapse(t *testing.T) {
	t.Parallel()
	s := open(t)
	id := createRun(t, s)
	creds := map[bool]string{true: credential('s'), false: credential('n')}
	inTx(t, s, func(tx *Tx) error {
		for _, self := range []bool{true, false} {
			job := map[bool]string{true: "build", false: "test"}[self]
			_, token, err := placedAttempt(tx, NewAttempt{Run: id, Job: job, SelfFencing: self, LeaseSeconds: 60, HeartbeatSeconds: 10})
			if err != nil {
				return err
			}
			if _, err := tx.Claim(token, creds[self], testWorker, workerapi.Version); err != nil {
				return err
			}
		}
		if _, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "build", SelfFencing: true, LeaseSeconds: 60, HeartbeatSeconds: 13}); err == nil {
			t.Error("NewAttempt of a job that fences itself, with heartbeats more than a fifth of its lease apart, succeeded")
		}
		for _, n := range []NewAttempt{
			{Run: id, Job: "build", LeaseSeconds: 0, HeartbeatSeconds: 0},
			{Run: id, Job: "build", LeaseSeconds: 10, HeartbeatSeconds: 20},
		} {
			if _, _, err := tx.NewAttempt(n); err == nil {
				t.Errorf("NewAttempt(%+v) succeeded", n)
			}
		}
		// The leases were renewed a lease ago, and a microsecond.
		_, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_attempts SET renewed_at = renewed_at - 60000001")
		return err
	})
	inTx(t, s, func(tx *Tx) error {
		for name, err := range map[string]error{
			"Authorize": second(tx.Authorize(creds[true])),
			"Renew":     second(tx.Renew(creds[true])),
		} {
			if !errors.Is(err, ErrLapsed) {
				t.Errorf("%s once the lease lapsed = %v, want ErrLapsed", name, err)
			}
		}
		// The credential of a job that doesn't fence itself is good until
		// its attempt ends, and a heartbeat renews its lease.
		a, err := tx.Renew(creds[false])
		if err != nil {
			return err
		}
		now, err := tx.Now()
		if err != nil {
			return err
		}
		if !a.LeaseLapses().After(now) {
			t.Errorf("the lease renewed lapses at %v, by %v", a.LeaseLapses(), now)
		}
		return nil
	})
}

// TestCheckProblem checks what the store keeps of the problems its
// backend's checks of an attempt's worker gave: the time of the first
// since the backend decided, or the attempt's lease was renewed, which a
// decision, a renewal, and a claim forget; and that a check a renewal
// overtook records nothing.
func TestCheckProblem(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Microsecond)
	s := openClock(t, filepath.Join(coordtest.PrivateDir(t), "meridian.db"), func() time.Time { return now })
	id := createRun(t, s)
	var (
		a     *Attempt
		token string
	)
	inTx(t, s, func(tx *Tx) error {
		var err error
		a, token, err = placedAttempt(tx, NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		return err
	})
	since := func(want time.Time) {
		t.Helper()
		inTx(t, s, func(tx *Tx) error {
			cur, err := tx.Attempt(a.ID)
			if err != nil {
				return err
			}
			got, err := tx.CheckProblem(cur)
			if err != nil || !got.Equal(want) {
				t.Errorf("CheckProblem = %v, %v, want %v", got, err, want)
			}
			stored, err := tx.Attempt(a.ID)
			if err == nil && !stored.ProblemSince.Equal(want) {
				t.Errorf("the attempt's ProblemSince is %v, want %v", stored.ProblemSince, want)
			}
			return err
		})
	}
	first := now
	since(first)
	now = now.Add(time.Minute)
	since(first)
	inTx(t, s, func(tx *Tx) error { return tx.CheckDecided(a.ID) })
	since(now)
	// A claim, and a heartbeat, forget it.
	inTx(t, s, func(tx *Tx) error {
		_, err := tx.Claim(token, credential('a'), testWorker, workerapi.Version)
		return err
	})
	now = now.Add(time.Minute)
	since(now)
	var before *Attempt
	inTx(t, s, func(tx *Tx) error {
		var err error
		if before, err = tx.Attempt(a.ID); err != nil {
			return err
		}
		now = now.Add(time.Second)
		_, err = tx.Renew(credential('a'))
		return err
	})
	// A check of the attempt as it was before the heartbeat.
	inTx(t, s, func(tx *Tx) error {
		if _, err := tx.CheckProblem(before); !errors.Is(err, ErrState) {
			t.Errorf("CheckProblem of the attempt before its heartbeat = %v, want ErrState", err)
		}
		stored, err := tx.Attempt(a.ID)
		if err == nil && !stored.ProblemSince.IsZero() {
			t.Errorf("a check a heartbeat overtook recorded %v", stored.ProblemSince)
		}
		return err
	})
	now = now.Add(time.Minute)
	since(now)
	inTx(t, s, func(tx *Tx) error {
		if _, err := tx.CheckProblem(&Attempt{ID: "no-such-attempt"}); !errors.Is(err, ErrNotFound) {
			t.Errorf("CheckProblem of no attempt = %v", err)
		}
		return nil
	})
}
