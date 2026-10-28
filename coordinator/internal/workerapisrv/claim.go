package workerapisrv

import (
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// A Readier gives the job an attempt claims: plan.Plan.Ready run again, as
// the coordinator runs it whenever it needs a ready job, with what it kept
// of how it readied the job, so that every claim of an attempt gives the
// job its supervisor placed, byte for byte. The coordinator gives the
// one it serves with, from the same code its supervisors ready a job with.
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
	// Needs returns the oldest version of the worker API that has all
	// that the attempt's job uses, which the server refuses a worker of an
	// older one for (workerapi.WorkerAPITooOld): workerapi.Version3 for a
	// job with a step whose uses: names a workflow, or an action the
	// coordinator performs, or of a child run, and 1 otherwise.
	Needs(ctx context.Context, run *store.Run, a *store.Attempt) (int, error)
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
	// Tree is what an attempt of a child run has of its tree, or nil for a
	// run no step called (workerapi.Claim.Tree).
	Tree *workerapi.Tree
	// RunDir is the run's directory where the attempt runs, where its
	// member chose it (store.Placement.RunDir, or what a private
	// placement holds sealed), or "" for the run's; and RunDirOnMember
	// whether its member chose it, apart from the run's directory where
	// the run started (workerapi.Claim.RunDirOnMember).
	RunDir         string
	RunDirOnMember bool
}

// serveClaim claims the attempt the bootstrap token is for, and answers
// with its Claim, at the lower of the worker's version of the API and the
// server's.
func (s *Server) serveClaim(w http.ResponseWriter, r *http.Request, from peer) {
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
	case from.member == "" && !s.peers.Allows(ctx, from.uid, run):
		writeStatus(w, http.StatusUnauthorized)
		return
	case a.Placed() && !s.allows(ctx, from, run, a):
		// Whom the claim came from is held to the member the attempt was
		// placed on, once that is recorded, before anything else is
		// answered of it, as below for one placed meanwhile.
		writeStatus(w, http.StatusUnauthorized)
		return
	case run.PlanVersion > s.reads, a.APIVersion > s.speaks:
		// A run of a plan this replica doesn't read, or an attempt claimed
		// at a version of the API it doesn't speak, is another replica's,
		// but for what needs neither: that the token claims nothing, of an
		// attempt withdrawn or superseded before its claim, and that a
		// claim repeated of one that no longer holds its job is fenced.
		switch {
		case !a.Holds && a.ClaimedAt.IsZero():
			writeStatus(w, http.StatusUnauthorized)
		case !a.Holds:
			problem.Write(w, fenced())
		default:
			writeStatus(w, http.StatusServiceUnavailable)
		}
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
	// The attempt is served at the lower of the worker's version of the
	// API and the server's, which a claim repeated keeps.
	version := min(req.Speaks(), s.speaks)
	// A first claim, of an attempt that holds its job: until its placement
	// is recorded, as when the worker the call that places it started
	// claims before the call returns, it is answered 503, which the
	// worker tries again; then it is refused, before anything of the run
	// is sent, for a worker that doesn't read the run's plan, that doesn't
	// speak a version of the API that has everything the attempt needs, or
	// that isn't the binary its placement started, which ends the attempt,
	// failed before its first step.
	if a.Holds && a.ClaimedAt.IsZero() && !a.Placed() {
		if a, ok = s.awaitPlaced(ctx, token, a); !ok {
			notPlaced(w)
			return
		}
	}
	// Whom the claim came from is held to the member the attempt was
	// placed on, once that is recorded, before the claim can end it.
	if !s.allows(ctx, from, run, a) {
		writeStatus(w, http.StatusUnauthorized)
		return
	}
	if a.Holds && a.ClaimedAt.IsZero() {
		needs, err := s.readier.Needs(ctx, run, a)
		if err != nil {
			s.writeError(w, r, err)
			return
		}
		needs = max(needs, s.needs)
		var refusal *problem.Problem
		switch placed := a.Placement.WorkerDigest; {
		case req.PlanVersion < run.PlanVersion:
			refusal = problems.PlanTooNew.Problem(nil)
		case version < needs:
			refusal = problems.WorkerAPITooOld.Problem(problems.Params{workerapi.ParamVersion: req.Speaks(), workerapi.ParamNeeded: needs})
		case placed != req.Worker.Digest:
			refusal = problems.WorkerDigestMismatch.Problem(problems.Params{workerapi.ParamPlaced: placed, workerapi.ParamClaimed: req.Worker.Digest})
		}
		if refusal != nil {
			s.endRefused(ctx, a, refusal)
			problem.Write(w, refusal)
			return
		}
	}
	var (
		claimed *store.Attempt
		after   func()
	)
	if s.beforeClaim != nil {
		s.beforeClaim()
	}
	err = s.store.Tx(ctx, func(tx *store.Tx) error {
		var err error
		if claimed, err = tx.Claim(token, req.Credential, req.Worker, version); err != nil || !a.ClaimedAt.IsZero() {
			return err
		}
		after, err = s.wakeJob(ctx, tx, run.ID, a.Job)
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
	committed(after)
	if claimed.APIVersion > s.speaks {
		// Claimed meanwhile, through a replica that speaks a later version
		// of the API than this one.
		writeStatus(w, http.StatusServiceUnavailable)
		return
	}
	c, err := s.claim(ctx, run, claimed)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeBody(w, claimed.APIVersion, c)
}

// placedWait is how long a first claim waits for its placement to be
// recorded before it is answered 503: a worker usually claims while the
// call that started it records where, and a 503 has it wait a quarter of
// a second or more before it claims again. placedRecheck is how often it
// reads the store again meanwhile, for a placement that a coordinator in
// another process records, which can't tell it (Server.Placed): the
// server's defaults, which tests change.
const (
	placedWait    = 2 * time.Second
	placedRecheck = 100 * time.Millisecond
)

// placedWaits are the first claims of an attempt that wait for its
// placement to be recorded: ch is closed once it has been (Server.Placed),
// and n counts them.
type placedWaits struct {
	ch chan struct{}
	n  int
}

// awaitPlaced waits for the placement of a, which the bootstrap token is
// for, to be recorded, for s.placedWait at most, and returns the attempt
// as it is then, and whether it has been: woken once the coordinator in
// this process has recorded it (Server.Placed), and reading the store
// every placedRecheck otherwise.
func (s *Server) awaitPlaced(ctx context.Context, token string, a *store.Attempt) (*store.Attempt, bool) {
	woken, done := s.waitPlaced(a.ID)
	defer done()
	deadline := time.NewTimer(s.placedWait)
	defer deadline.Stop()
	tick := time.NewTicker(s.placedRecheck)
	defer tick.Stop()
	// It reads the store once it waits, so that a placement recorded just
	// before isn't missed.
	for {
		var now *store.Attempt
		if err := s.store.View(ctx, func(tx *store.Tx) error {
			var err error
			now, err = tx.AttemptByToken(token)
			return err
		}); err != nil {
			return a, false
		}
		if a = now; !a.Holds || a.Placed() {
			return a, a.Placed()
		}
		select {
		case <-ctx.Done():
			return a, false
		case <-deadline.C:
			return a, false
		case <-woken:
			// Woken once: the ticker reads the store after.
			woken = nil
		case <-tick.C:
		}
	}
}

// waitPlaced returns a channel closed once the placement of the attempt id
// is recorded (Server.Placed), and what to call once it no longer waits.
func (s *Server) waitPlaced(id string) (<-chan struct{}, func()) {
	s.placed.Lock()
	defer s.placed.Unlock()
	if s.placed.waits == nil {
		s.placed.waits = map[string]*placedWaits{}
	}
	w := s.placed.waits[id]
	if w == nil {
		w = &placedWaits{ch: make(chan struct{})}
		s.placed.waits[id] = w
	}
	w.n++
	return w.ch, func() {
		s.placed.Lock()
		defer s.placed.Unlock()
		if w.n--; w.n == 0 && s.placed.waits[id] == w {
			delete(s.placed.waits, id)
		}
	}
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

// endRefused ends the attempt a, whose claim the server refused, as one
// that failed before its first step, with p, unless something ended it
// meanwhile, and wakes its job's supervisor, which places the job again.
func (s *Server) endRefused(ctx context.Context, a *store.Attempt, p *problem.Problem) {
	var after func()
	err := s.store.Tx(ctx, func(tx *store.Tx) error {
		if err := tx.EndAttempt(a.ID, store.AttemptEnd{Kind: store.EndSetupFailed, Outcome: plan.OutcomeFailure, Reason: plan.ReasonSetupFailed, Problem: p}); err != nil {
			return err
		}
		var err error
		after, err = s.wakeJob(ctx, tx, a.Run, a.Job)
		return err
	})
	switch {
	case errors.Is(err, store.ErrFenced):
	case err != nil:
		s.log.ErrorContext(ctx, "workerapisrv: end an attempt that failed before its first step", "attempt", a.ID, "error", err)
	default:
		committed(after)
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
	// An attempt of version 2 or later is given its control as it is now,
	// which a control stored after this read follows at a later version.
	var (
		retry   int
		control *workerapi.Control
	)
	if err := s.store.View(ctx, func(tx *store.Tx) error {
		if retry, err = tx.StartedBefore(a); err != nil || a.APIVersion < workerapi.Version2 {
			return err
		}
		c, err := tx.Control(a.ID)
		control = &c
		return err
	}); err != nil {
		return nil, err
	}
	// What a child run's attempt has of its tree, and whether its run's
	// directory is its member's own, which a step that calls a workflow
	// reads, are given only in an attempt of version 3 or later, which
	// such an attempt needs.
	var (
		tree     *workerapi.Tree
		onMember bool
	)
	if a.APIVersion >= workerapi.Version3 {
		tree, onMember = ready.Tree, ready.RunDirOnMember
	}
	return &workerapi.Claim{
		RunID: run.ID, AttemptID: a.ID, APIVersion: a.APIVersion, PlanVersion: run.PlanVersion, Job: job,
		Attempt: a.Number, Group: a.Placement.Group, Member: a.Placement.Member,
		LeaseSeconds: a.LeaseSeconds, HeartbeatSeconds: a.HeartbeatSeconds, SelfFencing: a.SelfFencing,
		Run: ready.Run, Needs: ready.Needs, RunDir: cmp.Or(ready.RunDir, run.RunDir), Env: ready.Env, Retry: retry, Container: ready.Container,
		Control: control, Tree: tree, RunDirOnMember: onMember,
	}, nil
}
