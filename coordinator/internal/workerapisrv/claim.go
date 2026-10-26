package workerapisrv

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// A Readier gives the job an attempt claims: plan.Plan.Ready run again, as
// the coordinator runs it whenever it needs a ready job, with what it kept
// of how it readied the job, so that every claim of an attempt gives the
// same, byte for byte (PlanReadier).
type Readier interface {
	// ReadyAttempt returns the job of the attempt a, of run, as Ready gives
	// it, with the part of the run's values, and of its needs' results,
	// that the job reads (plan.Job.Reads and ReadsNeeds). An error that
	// wraps plan.ErrVersion is a run this release doesn't read, which the
	// server answers 503; any other, 500, and the worker tries again.
	ReadyAttempt(ctx context.Context, run *store.Run, a *store.Attempt) (*Ready, error)
	// Steps returns how many steps the attempt's job has, as the plan says
	// them, for the logs of its steps.
	Steps(ctx context.Context, run *store.Run, a *store.Attempt) (int, error)
}

// Ready is what a claim holds of an attempt's job (workerapi.Claim).
type Ready struct {
	// Job is the job as Ready evaluated it.
	Job *plan.ReadyJob
	// Run and Needs are the part of the run's values, and of the results of
	// the jobs it needs, by job name, that the job reads.
	Run   plan.RunValues
	Needs map[string]plan.JobResult
	// Env are the variables a platform gives the job's steps, and
	// Container the container they run in, or nil for the host.
	Env       map[string]workerapi.EnvVar
	Container *workerapi.Container
}

// serveClaim claims the attempt the bootstrap token is for, and answers
// with its Claim.
func (s *Server) serveClaim(w http.ResponseWriter, r *http.Request, uid int) {
	ctx := r.Context()
	token := bearer(r)
	if token == "" {
		writeStatus(w, http.StatusUnauthorized)
		return
	}
	// The token is looked up before the body is read: one that is no
	// attempt's has the server read nothing more.
	var (
		a   *store.Attempt
		run *store.Run
	)
	err := s.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if a, err = tx.AttemptByToken(token); err != nil {
			return err
		}
		run, err = tx.Run(a.Run)
		return err
	})
	switch {
	case err != nil:
		s.writeError(w, r, err)
		return
	case run.PlanVersion > s.reads:
		writeStatus(w, http.StatusServiceUnavailable)
		return
	case !s.peers.Allows(ctx, uid, run):
		writeStatus(w, http.StatusUnauthorized)
		return
	}
	body, ok := s.readBody(w, r, MaxClaimBody)
	if !ok {
		return
	}
	var req workerapi.ClaimRequest
	if err := workerapi.Unmarshal(body, &req); err != nil {
		s.badRequest(w, r, "a claim that isn't one")
		return
	}
	if !credentialForm(req.Credential) {
		s.badRequest(w, r, "a credential that isn't 32 bytes as hex")
		return
	}
	// A first claim, of an attempt that holds its job: until its placement
	// is recorded, as when the worker the call that places it started
	// claims before the call returns, it is answered 503, which the
	// worker tries again; then it is refused, before anything of the run
	// is sent, for a worker that doesn't read the run's plan, or that
	// isn't the binary its placement started, which ends the attempt,
	// failed before its first step.
	if a.Holds && a.ClaimedAt.IsZero() {
		if a.Placement.ID == "" {
			notPlaced(w)
			return
		}
		var refusal *problem.Problem
		switch placed := a.Placement.WorkerDigest; {
		case req.PlanVersion < run.PlanVersion:
			refusal = problems.PlanTooNew.Problem(nil)
		case placed != req.Worker.Digest:
			refusal = problems.WorkerDigestMismatch.Problem(problems.Params{workerapi.ParamPlaced: placed, workerapi.ParamClaimed: req.Worker.Digest})
		}
		if refusal != nil {
			s.setupFailed(ctx, a, refusal)
			problem.Write(w, refusal)
			return
		}
	}
	var claimed *store.Attempt
	err = s.store.Tx(ctx, func(tx *store.Tx) error {
		var err error
		claimed, err = tx.Claim(token, req.Credential, req.Worker)
		return err
	})
	switch {
	case errors.Is(err, store.ErrFenced) && a.ClaimedAt.IsZero():
		// Withdrawn, or superseded, before it was claimed: the token
		// claims nothing.
		writeStatus(w, http.StatusUnauthorized)
		return
	case errors.Is(err, store.ErrNotPlaced):
		notPlaced(w)
		return
	case err != nil:
		s.writeError(w, r, err)
		return
	}
	if a.ClaimedAt.IsZero() {
		s.wakeJob(ctx, run.ID, a.Job)
	}
	c, err := s.claim(ctx, run, claimed)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, c)
}

// notPlaced answers a first claim of an attempt whose placement isn't
// recorded yet: 503, which the worker tries again, after a second.
func notPlaced(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	writeStatus(w, http.StatusServiceUnavailable)
}

// credentialForm reports whether a credential is as a worker makes one: 32
// bytes, as hex.
func credentialForm(credential string) bool {
	b, err := hex.DecodeString(credential)
	return err == nil && len(b) == 32
}

// setupFailed ends the attempt a, which failed before its first step with
// p, unless something ended it meanwhile, and wakes its job's supervisor,
// which places the job again.
func (s *Server) setupFailed(ctx context.Context, a *store.Attempt, p *problem.Problem) {
	err := s.store.Tx(ctx, func(tx *store.Tx) error {
		return tx.EndAttempt(a.ID, store.AttemptEnd{Kind: store.EndSetupFailed, Outcome: plan.OutcomeFailure, Reason: plan.ReasonSetupFailed, Problem: p})
	})
	switch {
	case errors.Is(err, store.ErrFenced):
	case err != nil:
		s.log.ErrorContext(ctx, "workerapisrv: end an attempt that failed before its first step", "attempt", a.ID, "error", err)
	default:
		s.wakeJob(ctx, a.Run, a.Job)
	}
}

// claim returns the Claim of the attempt a, of run, once its worker has
// claimed it.
func (s *Server) claim(ctx context.Context, run *store.Run, a *store.Attempt) (*workerapi.Claim, error) {
	ready, err := s.readier.ReadyAttempt(ctx, run, a)
	if err != nil {
		return nil, err
	}
	job, err := plan.EncodeReadyJob(ready.Job)
	if err != nil {
		return nil, err
	}
	var retry int
	if err := s.store.View(ctx, func(tx *store.Tx) error {
		retry, err = tx.StartedBefore(a)
		return err
	}); err != nil {
		return nil, err
	}
	return &workerapi.Claim{
		RunID: run.ID, AttemptID: a.ID, PlanVersion: run.PlanVersion, Job: job,
		Attempt: a.Number, Group: a.Placement.Group, Member: a.Placement.Member,
		LeaseSeconds: a.LeaseSeconds, HeartbeatSeconds: a.HeartbeatSeconds, SelfFencing: a.SelfFencing,
		Run: ready.Run, Needs: ready.Needs, RunDir: run.RunDir, Env: ready.Env, Retry: retry, Container: ready.Container,
	}, nil
}
