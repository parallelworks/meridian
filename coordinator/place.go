package coordinator

import (
	"cmp"
	"context"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// privateFields are the fields of a ready job that, evaluated from a
// variable or a password, make where it runs private: the member chosen,
// and why, are then kept only sealed (docs/design.md, Run state).
var privateFields = []string{"target", "resources", "timeoutSeconds"}

// placementPrivate reports whether where rj runs is private.
func placementPrivate(rj *plan.ReadyJob) bool {
	return slices.ContainsFunc(rj.PrivateFields, func(f string) bool { return slices.Contains(privateFields, f) })
}

// A candidate is a member a job can be placed on, as its driver's Probe
// said, of its group.
type candidate struct {
	group  string
	member *backend.Member
	driver backend.Driver
	fit    backend.Fit
}

// placeJob readies the job, ends it as Ready says when Ready skips it or
// fails, and otherwise places its next attempt on the member that would
// start it soonest, keeping the warnings Ready gave with the job, however
// it ends (store.Tx.NoteJob). A job no member can run fails, with
// placement_impossible and each member's reason.
func (c *Coordinator) placeJob(ctx context.Context, run *store.Run, job *store.Job, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	r, err := c.readyJob(ctx, run, job.Key)
	if err != nil {
		return 0, err
	}
	if end, ok := r.end(); ok {
		return 0, c.endJob(ctx, run.ID, job.Key, end, done)
	}
	if r.result.Job == nil {
		return 0, fmt.Errorf("coordinator: Ready expanded run %s's job %q's matrix", run.ID, job.Key)
	}
	if len(r.diags) > 0 {
		err := c.store.Tx(ctx, func(tx *store.Tx) error { return tx.NoteJob(run.ID, job.Key, r.diags) })
		switch {
		case errors.Is(err, store.ErrState):
			return 0, errAgain
		case err != nil:
			return 0, err
		}
	}
	rj := r.result.Job
	private := placementPrivate(rj)
	g, d, err := c.group(ctx, run, rj, private)
	switch {
	case err != nil:
		return c.unanswered(ctx, run, job, private, err)
	case d != nil:
		return 0, c.endJob(ctx, run.ID, job.Key, store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Diagnostics: problems.Diagnostics{*d}}, done)
	}
	demand, err := c.demandOf(ctx, run, rj)
	if err != nil {
		return 0, err
	}
	demand.Tries = job.PlaceTries
	fits, reasons, look := c.rank(ctx, g, demand, private, seenKey(run.ID, job.Key))
	if len(fits) == 0 {
		switch {
		case look.counted > 0:
			return c.unansweredBy(ctx, run, job, private, look.counted, errors.New("a member's backend couldn't say whether it can run the job"))
		case look.notYet:
			return c.notYetWait(look.retryAt), nil
		}
		diags := append(problems.Diagnostics{diagnostic(problems.PlacementImpossible, problems.Params{"job": job.Key})}, reasons...)
		return 0, c.endJob(ctx, run.ID, job.Key, store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Diagnostics: diags}, done)
	}
	chosen, err := c.choose(ctx, run, job.Key, fits)
	if err != nil {
		return 0, err
	}
	var (
		id, token string
		selfFence = rj.Retry != nil && slices.Contains(rj.Retry.Reasons, plan.ReasonWorkerLost)
	)
	err = c.store.Tx(ctx, func(tx *store.Tx) error {
		// Not once the run has stopped since the look, which the stop
		// may not have told: the supervisor looks again, and ends the
		// job, unless it outlives the stop (placesNoMore).
		if run.StoppedAt.IsZero() {
			now, err := tx.Run(run.ID)
			switch {
			case err != nil:
				return err
			case !now.StoppedAt.IsZero():
				return store.ErrState
			}
		}
		// Only of the job as the look saw it: a supervisor rescued from a
		// replica still running it doesn't supersede the attempt the other
		// made meanwhile.
		a, t, err := tx.NewAttemptAfter(c.newAttempt(run.ID, job.Key, selfFence), job.Fence)
		if err != nil {
			return err
		}
		id, token = a.ID, t
		// The place is chosen in the transaction that makes the attempt,
		// so that no attempt is without one. Sealing a private place asks
		// Keys for the run's data key while the transaction holds the
		// store's write lock, as CreateRun does.
		place, err := c.placeOf(ctx, tx, run.ID, a.ID, chosen, private)
		if err != nil {
			return err
		}
		return tx.ChoosePlace(a.ID, place)
	})
	if errors.Is(err, store.ErrState) || errors.Is(err, store.ErrFenced) {
		return 0, errAgain
	}
	if err != nil {
		return 0, err
	}
	c.forgetSeen(run.ID, job.Key)
	return c.placeAttempt(ctx, run, job, id, token, rj, chosen, private)
}

// choose returns the first of fits, members of a group ranked to run the
// run's job key, that none of the job's attempts failed on before a step
// started, as long as one is left; and the first of them otherwise.
func (c *Coordinator) choose(ctx context.Context, run *store.Run, key string, fits []candidate) (*candidate, error) {
	var (
		places  = map[string]store.Place{} // by attempt
		wrapped []byte
	)
	err := c.store.View(ctx, func(tx *store.Tx) error {
		failures, err := tx.SetupFailures(run.ID, key)
		if err != nil {
			return err
		}
		for _, a := range failures {
			p, chosen, err := tx.ChosenPlace(a.ID)
			switch {
			case err != nil:
				return err
			case !chosen:
				// Made for the record of a placement that failed.
				continue
			case p.Private != nil && wrapped == nil:
				if wrapped, err = tx.WrappedKey(run.ID); err != nil {
					return err
				}
			}
			places[a.ID] = p
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	opened, err := c.openPlaces(ctx, run.ID, wrapped, places)
	if err != nil {
		return nil, err
	}
	failed := map[[3]string]bool{}
	for _, at := range opened {
		failed[[3]string{at.Group, at.Member, at.Backend}] = true
	}
	for i := range fits {
		if !failed[[3]string{fits[i].group, fits[i].member.Name, fits[i].member.Backend}] {
			return &fits[i], nil
		}
	}
	return &fits[0], nil
}

// A sealedPlace is what an attempt whose place is private keeps of it,
// sealed under the run's data key (seal.PurposePlacement): where it is to
// be placed, and, once its backend has placed it, the backend's ID for the
// placement, which can say where it is, as a pod's name can.
type sealedPlace struct {
	Group   string `json:"group"`
	Member  string `json:"member"`
	Backend string `json:"backend"`
	ID      string `json:"id,omitzero"`
	// RunDir is the run's directory where its member chose it
	// (backend.Placement.RunDir), or "".
	RunDir string `json:"runDir,omitzero"`
}

// placeOf is where the attempt id of the run runID is placed, on chosen:
// in the clear, or sealed under the data key of the run's record, as tx
// reads it, as the place's first generation, when where its job runs is
// private.
func (c *Coordinator) placeOf(ctx context.Context, tx *store.Tx, runID, id string, chosen *candidate, private bool) (store.Place, error) {
	if !private {
		return store.Place{Group: chosen.group, Member: chosen.member.Name, Backend: chosen.member.Backend}, nil
	}
	sealed, err := c.sealPlace(ctx, tx, runID, id, 1, sealedPlace{Group: chosen.group, Member: chosen.member.Name, Backend: chosen.member.Backend})
	if err != nil {
		return store.Place{}, err
	}
	return store.Place{Private: sealed, Generation: 1}, nil
}

// sealPlace seals sp, the private place of the attempt id of the run
// runID, as generation gen, under the data key of the run's record, as tx
// reads it.
func (c *Coordinator) sealPlace(ctx context.Context, tx *store.Tx, runID, id string, gen uint64, sp sealedPlace) ([]byte, error) {
	data, err := jsonv2.Marshal(sp, jsonv2.Deterministic(true))
	if err != nil {
		return nil, err
	}
	defer clear(data)
	wrapped, err := tx.WrappedKey(runID)
	if err != nil {
		return nil, err
	}
	return c.sealer.SealPart(ctx, runID, wrapped, seal.PartID{Purpose: seal.PurposePlacement, ID: id, Generation: gen}, data)
}

// openPlaces returns places, those of attempts of the run runID by their
// IDs, in the clear: each in the clear as it is, or, for a private one,
// what it holds sealed, with the backend's ID for the placement once that
// is recorded, opened with the run's data key, which Keys wrapped as
// wrapped and unwraps once for all of them.
func (c *Coordinator) openPlaces(ctx context.Context, runID string, wrapped []byte, places map[string]store.Place) (map[string]sealedPlace, error) {
	out := make(map[string]sealedPlace, len(places))
	var sealed []seal.SealedPart
	for id, p := range places {
		if p.Private == nil {
			out[id] = sealedPlace{Group: p.Group, Member: p.Member, Backend: p.Backend}
			continue
		}
		sealed = append(sealed, seal.SealedPart{Purpose: seal.PurposePlacement, ID: id, Generation: p.Generation, Sealed: p.Private})
	}
	opened, err := c.sealer.OpenParts(ctx, runID, wrapped, sealed)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, data := range opened {
			clear(data)
		}
	}()
	for i, part := range sealed {
		var sp sealedPlace
		if err := jsonv2.Unmarshal(opened[i], &sp, jsonv2.RejectUnknownMembers(true)); err != nil {
			// What the decoder says can quote what the place holds.
			return nil, fmt.Errorf("%w: attempt %s's sealed place doesn't decode", plan.ErrMalformed, part.ID)
		}
		out[part.ID] = sp
	}
	return out, nil
}

// placeAttempt asks chosen's driver to place the attempt id of the run's
// job, with its bootstrap token, and records the placement it gives.
// A problem from the driver fails the attempt before its first step, and
// any other error is a backend that couldn't answer. The request gives a
// child run's root, and its directory's path in the root's, for a member
// that chooses run directories itself (backend.PlaceRequest.Path).
func (c *Coordinator) placeAttempt(ctx context.Context, run *store.Run, job *store.Job, id, token string, rj *plan.ReadyJob, chosen *candidate, private bool) (time.Duration, error) {
	if c.placed != nil {
		c.placed(run, id, rj)
	}
	root, path, err := c.placeTree(ctx, run)
	if err != nil {
		return 0, err
	}
	endpoint, err := c.connect(ctx, chosen.driver, chosen.member)
	var key string
	if err == nil {
		key, err = memberKey(chosen.driver, chosen.member)
	}
	var p backend.Placement
	if err == nil {
		p, err = chosen.driver.Place(ctx, chosen.member, backend.PlaceRequest{
			Attempt: id, Run: run.ID, Name: "meridian-" + run.ID + "-" + job.Key, Coordinator: endpoint, BootstrapToken: token,
			TimeoutSeconds: cmp.Or(rj.TimeoutSeconds, chosen.fit.DefaultTimeoutSeconds),
			Resources:      backend.Resources(rj.Resources), RunDir: run.RunDir, Root: root, Path: path,
		})
	}
	if prob, ok := errors.AsType[*problem.Problem](err); ok {
		return 0, c.attemptSetupFailed(ctx, id, prob)
	}
	if err != nil {
		return c.unanswered(ctx, run, job, private, err)
	}
	return c.recordPlacement(ctx, run.ID, id, chosen, p, key, private)
}

// recordPlacement records the placement p of the attempt id of the run
// runID, on chosen, whose key is key where its backend reaches its workers
// through a listener on it, and returns how long to wait for its worker.
// Of a private placement, the store holds the worker's digest, and the
// member's key, in the clear, and the backend's ID for it and the run's
// directory sealed, with the rest of its place, as the place's next
// generation, which the same transaction stores.
func (c *Coordinator) recordPlacement(ctx context.Context, runID, id string, chosen *candidate, p backend.Placement, key string, private bool) (time.Duration, error) {
	err := c.store.Tx(ctx, func(tx *store.Tx) error {
		if !private {
			return tx.RecordPlacement(id, store.Placement{
				Group: chosen.group, Member: chosen.member.Name, Backend: chosen.member.Backend, ID: p.ID, WorkerDigest: p.WorkerDigest,
				MemberKey: key, RunDir: p.RunDir,
			})
		}
		place, _, err := tx.ChosenPlace(id)
		if err != nil {
			return err
		}
		sealed, err := c.sealPlace(ctx, tx, runID, id, place.Generation+1,
			sealedPlace{Group: chosen.group, Member: chosen.member.Name, Backend: chosen.member.Backend, ID: p.ID, RunDir: p.RunDir})
		if err != nil {
			return err
		}
		if err := tx.ResealPlace(id, place.Generation+1, sealed); err != nil {
			return err
		}
		return tx.RecordPlacement(id, store.Placement{WorkerDigest: p.WorkerDigest, MemberKey: key})
	})
	if err == nil || errors.Is(err, store.ErrState) {
		c.remember(id, chosen, p)
	}
	switch {
	case errors.Is(err, store.ErrFenced):
		// Superseded meanwhile, as by another attempt of the supervisor:
		// its worker's claim is refused.
		return 0, errAgain
	case errors.Is(err, store.ErrState):
		// Claimed already, as when its worker was quicker than the record,
		// or recorded by another supervisor.
		return c.supervision.wait, nil
	case err != nil:
		return 0, err
	}
	return c.supervision.wait, nil
}

// attemptSetupFailed ends the attempt id as one that failed before its
// first step, with p, unless another supervisor placed it meanwhile, and
// has the supervisor look again, to place the job again or fail it.
func (c *Coordinator) attemptSetupFailed(ctx context.Context, id string, p *problem.Problem) error {
	err := c.store.Tx(ctx, func(tx *store.Tx) error {
		if err := tx.Unplaced(id); err != nil {
			return err
		}
		return tx.EndAttempt(id, store.AttemptEnd{Kind: store.EndSetupFailed, Outcome: plan.OutcomeFailure, Reason: plan.ReasonSetupFailed, Problem: p})
	})
	if err != nil && !errors.Is(err, store.ErrFenced) && !errors.Is(err, store.ErrState) {
		return err
	}
	return errAgain
}

// resumePlace places again the attempt a of the job, which a supervisor
// placed, or was placing, but ended before it recorded the placement: it
// finds the placement on the member it chose, as the driver's Find says,
// and records it, or places the attempt there with a new bootstrap token,
// so that no attempt is placed twice. A member that is no longer in its
// group fails the attempt before its first step, as does a placement not
// found once the stop ends the job (stopEnds).
func (c *Coordinator) resumePlace(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt) (time.Duration, error) {
	private := a.Private
	cand, _, rj, err := c.placedOn(ctx, run, job, a)
	groups, unanswered := errors.AsType[*groupsError](err)
	switch {
	case unanswered:
		return c.unanswered(ctx, run, job, private, groups.err)
	case errors.Is(err, errMemberGone):
		return 0, c.attemptSetupFailed(ctx, a.ID, nil)
	case err != nil:
		return 0, err
	}
	m := cand.member
	p, found, err := cand.driver.Find(ctx, m, a.ID)
	if prob, ok := errors.AsType[*problem.Problem](err); ok {
		return 0, c.attemptSetupFailed(ctx, a.ID, prob)
	}
	if err != nil {
		return c.unanswered(ctx, run, job, private, err)
	}
	if found {
		// Its workers reach the coordinator as a new one's would.
		var key string
		if _, err = c.connect(ctx, cand.driver, m); err == nil {
			key, err = memberKey(cand.driver, m)
		}
		if prob, ok := errors.AsType[*problem.Problem](err); ok {
			return 0, c.attemptSetupFailed(ctx, a.ID, prob)
		}
		if err != nil {
			return c.unanswered(ctx, run, job, private, err)
		}
		return c.recordPlacement(ctx, run.ID, a.ID, cand, p, key, private)
	}
	// Nothing was placed: once the run has stopped, the job is as one
	// between attempts, which the stop may end (stopEnds). Then the attempt
	// fails before its first step, and the job ends as the stop says
	// (placesNoMore), rather than be placed after the stop.
	if ends, err := c.stopEnds(ctx, run, job); err != nil || ends {
		if err != nil {
			return 0, err
		}
		return 0, c.attemptSetupFailed(ctx, a.ID, nil)
	}
	demand, err := c.demandOf(ctx, run, rj)
	if err != nil {
		return 0, err
	}
	demand.Tries = job.PlaceTries
	if cand.fit, err = cand.driver.Probe(ctx, m, demand); err == nil && cand.fit.Problem != nil {
		err = cand.fit.Problem
	}
	if prob, ok := errors.AsType[*problem.Problem](err); ok {
		return 0, c.attemptSetupFailed(ctx, a.ID, prob)
	}
	if err != nil {
		switch counted, notYet, retryAt := c.probeCounted(seenKey(run.ID, job.Key)+m.Name, err); {
		case counted > 0:
			return c.unansweredBy(ctx, run, job, private, counted, err)
		case notYet:
			return c.notYetWait(retryAt), nil
		}
	}
	if err != nil {
		return c.unanswered(ctx, run, job, private, err)
	}
	var token string
	err = c.store.Tx(ctx, func(tx *store.Tx) error {
		// Not one another supervisor placed meanwhile, whose worker
		// claims it with its token.
		if err := tx.Unplaced(a.ID); err != nil {
			return err
		}
		var err error
		token, err = tx.NewToken(a.ID)
		return err
	})
	if errors.Is(err, store.ErrFenced) || errors.Is(err, store.ErrState) {
		return 0, errAgain
	}
	if err != nil {
		return 0, err
	}
	return c.placeAttempt(ctx, run, job, a.ID, token, rj, cand, private)
}

// errMemberGone is a member an attempt was to be placed on that is no
// longer in its group.
var errMemberGone = errors.New("coordinator: the member an attempt was placed on is no longer in its group")

// A groupsError is Groups' error, which couldn't answer for the group a job
// runs in.
type groupsError struct{ err error }

func (e *groupsError) Error() string { return e.err.Error() }
func (e *groupsError) Unwrap() error { return e.err }

// placedOn returns the member the attempt a of the run's job was to be
// placed on, as its supervisor chose it, with its driver; its place,
// opened, with the backend's ID for the placement once that is recorded of
// a private one; and the job, as Ready gives it. errMemberGone is a member
// no longer in its group, and a *groupsError Groups' error.
func (c *Coordinator) placedOn(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt) (*candidate, sealedPlace, *plan.ReadyJob, error) {
	var (
		place   store.Place
		chosen  bool
		wrapped []byte
	)
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if place, chosen, err = tx.ChosenPlace(a.ID); err != nil || !a.Private {
			return err
		}
		wrapped, err = tx.WrappedKey(run.ID)
		return err
	})
	switch {
	case err != nil:
		return nil, sealedPlace{}, nil, err
	case !chosen:
		return nil, sealedPlace{}, nil, fmt.Errorf("coordinator: attempt %s has no place", a.ID)
	}
	opened, err := c.openPlaces(ctx, run.ID, wrapped, map[string]store.Place{a.ID: place})
	if err != nil {
		return nil, sealedPlace{}, nil, err
	}
	at := opened[a.ID]
	r, err := c.readyJob(ctx, run, job.Key)
	switch {
	case err != nil:
		return nil, at, nil, err
	case r.result.Job == nil:
		return nil, at, nil, fmt.Errorf("coordinator: run %s's job %q has an attempt, but Ready gives no job", run.ID, job.Key)
	}
	rj := r.result.Job
	g, d, err := c.group(ctx, run, rj, a.Private)
	if err != nil {
		return nil, at, rj, &groupsError{err}
	}
	var m *backend.Member
	if d == nil {
		if i := slices.IndexFunc(g.Members, func(m backend.Member) bool { return m.Name == at.Member && m.Backend == at.Backend }); i >= 0 {
			m = &g.Members[i]
		}
	}
	if m == nil || g.Name != at.Group {
		return nil, at, rj, errMemberGone
	}
	return &candidate{group: g.Name, member: m, driver: c.driver(m)}, at, rj, nil
}

// A withdrawal is the placement of an attempt being withdrawn, which its
// driver cancels once the transaction that withdraws the attempt commits:
// its member, with its driver, and the placement, whose ID is "" when it
// wasn't recorded, which the driver then finds by the attempt.
type withdrawal struct {
	cand      *candidate
	placement backend.Placement
	private   bool
}

// withdrawals returns the placements of the attempts that haven't ended of
// the run's jobs keys that haven't, by attempt, which failJob withdraws:
// each found now, while the job readies again and the run's secret record
// opens, as neither does once the job, or the run, has ended. One whose
// member can't be found is left out, with the reason in the log, unless
// where it runs is private: its worker's reports are refused all the same.
func (c *Coordinator) withdrawals(ctx context.Context, run *store.Run, keys []string) (map[string]*withdrawal, error) {
	type live struct {
		job *store.Job
		a   *store.Attempt
	}
	var lives []live
	err := c.store.View(ctx, func(tx *store.Tx) error {
		for _, key := range keys {
			j, err := tx.Job(run.ID, key)
			if err != nil {
				return err
			}
			if j.Outcome != "" {
				continue
			}
			switch a, err := tx.LatestAttempt(run.ID, key); {
			case errors.Is(err, store.ErrNotFound):
			case err != nil:
				return err
			case a.State != store.AttemptEnded:
				lives = append(lives, live{j, a})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make(map[string]*withdrawal, len(lives))
	for _, l := range lives {
		cand, at, _, err := c.placedOn(ctx, run, l.job, l.a)
		if err != nil {
			if !l.a.Private {
				c.log.WarnContext(ctx, "coordinator: couldn't find where an attempt it withdraws was placed, to cancel it; its worker's reports are refused", "run", run.ID, "job", l.job.Key, "error", err)
			}
			continue
		}
		p := backend.Placement{Attempt: l.a.ID, ID: l.a.Placement.ID, WorkerDigest: l.a.Placement.WorkerDigest}
		if l.a.Private {
			p.ID = at.ID
		}
		out[l.a.ID] = &withdrawal{cand: cand, placement: p, private: l.a.Private}
	}
	return out, nil
}

// cancelPlacements has the drivers of ws, placements by attempt, cancel
// those of the attempts withdrawn, as far as they can: a placement that
// wasn't recorded is found by its attempt first. One a driver couldn't
// cancel now, as one on a host it can't reach for a while, it has the
// driver cancel again in the background (cancelLater); its worker's
// reports are refused all the same.
func (c *Coordinator) cancelPlacements(ctx context.Context, run string, ws map[string]*withdrawal, withdrawn []string) {
	for _, id := range withdrawn {
		if w := ws[id]; w != nil {
			if err := c.cancelPlacement(ctx, id, w); err != nil {
				c.cancelLater(ctx, run, id, w, err)
			}
		}
	}
}

// cancelPlacement has w's driver cancel the placement of the attempt id,
// which it finds first if it wasn't recorded, and then forget it.
func (c *Coordinator) cancelPlacement(ctx context.Context, id string, w *withdrawal) error {
	m, d, p := w.cand.member, w.cand.driver, w.placement
	if p.ID == "" {
		var (
			found bool
			err   error
		)
		if p, found, err = d.Find(ctx, m, id); err != nil || !found {
			return err
		}
	}
	if err := d.Cancel(ctx, m, p); err != nil {
		return err
	}
	c.mu.Lock()
	c.unremember(id)
	c.mu.Unlock()
	c.forget(w.cand, p)
	return nil
}

// How a placement a driver couldn't cancel is cancelled again
// (cancelLater): after cancelFirst, twice as long each time after, up to
// cancelMost, for cancelFor in all.
const (
	cancelFirst = time.Second
	cancelMost  = 30 * time.Second
	cancelFor   = 5 * time.Minute
)

// cancelLater has w's driver cancel the placement of the attempt id of the
// run again, in the background, while Serve runs, as forget has a driver
// forget one: a driver that couldn't, as it answered err, as one whose
// host is being reached again, can a moment later. Once it has tried for
// cancelFor, the log says it couldn't, unless where the attempt runs is
// private.
func (c *Coordinator) cancelLater(ctx context.Context, run, id string, w *withdrawal, err error) {
	failed := func(ctx context.Context, err error) {
		if !w.private {
			c.log.WarnContext(ctx, "coordinator: couldn't cancel the placement of an attempt it withdrew; its worker's reports are refused", "run", run, "attempt", id, "error", err)
		}
	}
	c.mu.Lock()
	later := c.forgetCtx
	if later != nil {
		c.forgetting.Add(1)
	}
	c.mu.Unlock()
	if later == nil {
		failed(ctx, err)
		return
	}
	ctx = later
	go func() {
		defer c.forgetting.Done()
		deadline := time.Now().Add(cancelFor)
		for wait := cancelFirst; time.Now().Add(wait).Before(deadline); wait = min(2*wait, cancelMost) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			if err = c.cancelPlacement(ctx, id, w); err == nil {
				return
			}
		}
		failed(ctx, err)
	}()
}

// unanswered handles cause, a backend's, or Groups', that couldn't answer
// as the run's job, as the look saw it, was placed: it is asked again,
// after a while, until it has been as many times in a row as it may be,
// which then counts as a failure before the job's first step, with
// placement_unanswered, of its attempt that wasn't placed, or of one made
// for the record. The log says what cause says, unless where the job runs
// is private, when what it says can name the member.
func (c *Coordinator) unanswered(ctx context.Context, run *store.Run, job *store.Job, private bool, cause error) (time.Duration, error) {
	return c.unansweredBy(ctx, run, job, private, 1, cause)
}

// unansweredBy is unanswered, which counts the look as by of the times a
// backend couldn't answer, as it does each failure a driver saw to reach a
// member since the job's previous look (backend.FailedError).
func (c *Coordinator) unansweredBy(ctx context.Context, run *store.Run, job *store.Job, private bool, by int, cause error) (time.Duration, error) {
	key := job.Key
	if private {
		c.log.WarnContext(ctx, "coordinator: a backend couldn't answer as a job was placed, and is asked again later", "run", run.ID, "job", key)
	} else {
		c.log.WarnContext(ctx, "coordinator: a backend couldn't answer as a job was placed, and is asked again later", "run", run.ID, "job", key, "error", cause)
	}
	var tries int
	if err := c.store.Tx(ctx, func(tx *store.Tx) error {
		var err error
		tries, err = tx.PlaceTried(run.ID, key, by)
		return err
	}); err != nil {
		if errors.Is(err, store.ErrState) {
			return 0, errSettled
		}
		return 0, err
	}
	if tries < c.supervision.tries {
		return c.supervision.backoff(tries), nil
	}
	p := problems.PlacementUnanswered.Problem(problems.Params{"job": key, "tries": tries})
	err := c.store.Tx(ctx, func(tx *store.Tx) error {
		_, err := tx.PlaceFailed(c.newAttempt(run.ID, key, false), job.Fence, p)
		return err
	})
	switch {
	case errors.Is(err, store.ErrState) || errors.Is(err, store.ErrFenced):
		// Claimed, or ended, meanwhile.
		return 0, errAgain
	case err != nil:
		return 0, err
	}
	return 0, errAgain
}

// newAttempt is a new attempt of the run's job key, with the coordinator's
// lease, which fences itself when selfFencing is set.
func (c *Coordinator) newAttempt(runID, key string, selfFencing bool) store.NewAttempt {
	return store.NewAttempt{
		Run: runID, Job: key, SelfFencing: selfFencing,
		LeaseSeconds: int64(c.timing.Lease / time.Second), HeartbeatSeconds: int64(c.timing.Heartbeat / time.Second),
	}
}

// group returns the execution group rj runs in: the one its runs-on:
// names, or the run's default, or the one its format 1 ssh: makes; a
// problem of the job's for one Groups doesn't have, with its values masked
// when where the job runs is private; and an error for one Groups couldn't
// answer for.
func (c *Coordinator) group(ctx context.Context, run *store.Run, rj *plan.ReadyJob, private bool) (*backend.Group, *problems.Diagnostic, error) {
	t := rj.Target
	if t.Inline != nil {
		settings, err := jsonv2.Marshal(t.Inline.SSH, jsonv2.Deterministic(true))
		if err != nil {
			return nil, nil, err
		}
		return &backend.Group{Name: t.Inline.Name, Members: []backend.Member{{
			Name: t.Inline.Name, Backend: t.Inline.Backend, Isolation: t.Inline.Isolation, Settings: settings,
		}}}, nil, nil
	}
	at := rj.Path + "/runs-on"
	if c.opts.Groups == nil {
		d := problems.ResolverMissing.At("", problems.Position{}, at, problems.Params{"key": "runs-on", "interface": "Groups"})
		return nil, &d, nil
	}
	var (
		g        *backend.Group
		err      error
		notFound *problems.Kind
		params   problems.Params
	)
	switch {
	case t.Environment != nil:
		g, err = c.opts.Groups.Environment(ctx, *t.Environment)
		notFound, params = problems.EnvironmentUnknown, problems.Params{"cluster": t.Environment.Cluster, "name": t.Environment.Name}
	case t.ComputeTarget != nil:
		g, err = c.opts.Groups.ComputeTarget(ctx, *t.ComputeTarget)
		notFound, params = problems.ComputeTargetUnknown, problems.Params{"mode": t.ComputeTarget.Mode, "target": t.ComputeTarget.TargetID}
	default:
		name := cmp.Or(t.Group, run.DefaultGroup)
		g, err = c.opts.Groups.Group(ctx, name)
		notFound, params = problems.GroupUnknown, problems.Params{"group": name}
	}
	if p, ok := errors.AsType[*problem.Problem](err); ok {
		d := maskedIf(private, problemDiagnostic(p))
		return nil, &d, nil
	}
	if errors.Is(err, compile.ErrNotFound) {
		d := maskedIf(private, notFound.At("", problems.Position{}, at, params))
		return nil, &d, nil
	}
	if err != nil {
		return nil, nil, err
	}
	return g, nil, nil
}

// demandOf is what rj, a job of run, asks of a member, with the attempts
// that are parked for it (backend.Demand.Parked): its run's caller chain,
// the attempt whose step called the run, and that attempt's run's caller,
// up to the root, each waiting for the child run below it, which can't end
// before rj's run does. A driver that counts what its member runs, as
// local does, leaves what they asked for out, so that a caller's room goes
// to its own descendants alone, which end before it goes on, and never to
// another job, which would still hold it when the caller did. A run has
// as many callers as it is deep, a few at most (compile.CallLimits),
// each read by its ID; a chain longer than its link's depth says is an
// error, rather than a walk that never ends.
func (c *Coordinator) demandOf(ctx context.Context, run *store.Run, rj *plan.ReadyJob) (backend.Demand, error) {
	var parked []string
	err := c.store.View(ctx, func(tx *store.Tx) error {
		for r := run; r.Parent != nil; {
			if len(parked) == run.Parent.Depth {
				return fmt.Errorf("coordinator: run %s's callers go deeper than its depth, %d", run.ID, run.Parent.Depth)
			}
			parked = append(parked, r.Parent.Attempt)
			var err error
			if r, err = tx.Run(r.Parent.Run); err != nil {
				return err
			}
		}
		return nil
	})
	return backend.Demand{Resources: backend.Resources(rj.Resources), TimeoutSeconds: rj.TimeoutSeconds, Parked: parked}, err
}

// rank returns the members of g that can run a job that asks for demand,
// as their drivers' Probe say, the soonest to start it first, and in the
// group's order among those as soon; for each member that can't, the
// problem its Probe gave, masked when where the job runs is private; and,
// of those whose Probe couldn't answer, what the look counts for the job
// (probeLook), which seen, with a member's name after it, keys
// (probeCounted).
func (c *Coordinator) rank(ctx context.Context, g *backend.Group, demand backend.Demand, private bool, seen string) (fits []candidate, reasons problems.Diagnostics, look probeLook) {
	for i := range g.Members {
		m := &g.Members[i]
		d := c.driver(m)
		fit, err := d.Probe(ctx, m, demand)
		if err == nil && fit.Problem != nil {
			err = fit.Problem
		}
		if p, ok := errors.AsType[*problem.Problem](err); ok {
			reasons = append(reasons, maskedIf(private, problemDiagnostic(p)))
			continue
		}
		if err != nil {
			counted, notYet, retryAt := c.probeCounted(seen+m.Name, err)
			if counted > 0 && !private {
				c.log.WarnContext(ctx, "coordinator: a member's backend couldn't say whether it can run a job", "member", m.Name, "error", err)
			}
			look.add(counted, notYet, retryAt)
			continue
		}
		fits = append(fits, candidate{group: g.Name, member: m, driver: d, fit: fit})
	}
	slices.SortStableFunc(fits, func(a, b candidate) int { return cmp.Compare(a.fit.Availability, b.fit.Availability) })
	return fits, reasons, look
}

// A probeLook is what a look's probes said of the members that couldn't
// say whether they can run the job: the times the look counts as a
// backend's that couldn't answer, the most any member's counted
// (probeCounted); whether one hasn't tried the job yet, but will
// (backend.ErrNotYet), counting none; and the soonest such a member is
// tried again, if its driver says.
type probeLook struct {
	counted int
	notYet  bool
	retryAt time.Time
}

// add adds a member's probe to l.
func (l *probeLook) add(counted int, notYet bool, retryAt time.Time) {
	l.counted = max(l.counted, counted)
	l.notYet = l.notYet || notYet
	if !retryAt.IsZero() && (l.retryAt.IsZero() || retryAt.Before(l.retryAt)) {
		l.retryAt = retryAt
	}
}

// seenKey is what keys the job key of the run in Coordinator.seenFailed,
// before a member's name.
func seenKey(run, key string) string { return run + "\x00" + key + "\x00" }

// probeCounted returns how many times err, of a member's Probe for a job,
// counts as the member's backend's that couldn't answer, whether it says
// the member hasn't been tried for the job yet, and when it will be, if
// it says. A backend.FailedError counts the failures to reach the member
// since the count the job's previous look saw, which the coordinator keeps,
// in memory, by key, the job's and the member's: so that every job waiting
// on a member that doesn't answer gives up after as many failures as it
// has seen, its own tries' and those for others alike. A job's first look,
// as after a restart, sees none it can count, but for its own try, which
// counts once. ErrNotYet alone counts none, and any other error once.
func (c *Coordinator) probeCounted(key string, err error) (counted int, notYet bool, retryAt time.Time) {
	fe, ok := errors.AsType[*backend.FailedError](err)
	switch {
	case ok:
		own := !errors.Is(fe.Err, backend.ErrNotYet)
		c.mu.Lock()
		last, seen := c.seenFailed[key]
		if !seen && len(c.seenFailed) >= maxMembers {
			clear(c.seenFailed)
		}
		c.seenFailed[key] = fe.Failed
		c.mu.Unlock()
		if seen && fe.Failed > last {
			counted = fe.Failed - last
		}
		if own {
			counted = max(counted, 1)
		}
		return counted, counted == 0, fe.RetryAt
	case errors.Is(err, backend.ErrNotYet):
		return 0, true, time.Time{}
	}
	return 1, false, time.Time{}
}

// forgetSeen forgets what the coordinator saw of failures to reach members
// as it placed the run's job key (probeCounted), once it has placed it.
func (c *Coordinator) forgetSeen(run, key string) {
	prefix := seenKey(run, key)
	c.mu.Lock()
	defer c.mu.Unlock()
	maps.DeleteFunc(c.seenFailed, func(k string, _ int) bool { return strings.HasPrefix(k, prefix) })
}

// notYetWait is how long a supervisor waits before it looks again at its
// job, which a member's backend hasn't tried yet, but will: until the
// member is tried again, retryAt, if a driver said, or else supervision's
// first wait, the least.
func (c *Coordinator) notYetWait(retryAt time.Time) time.Duration {
	return min(max(time.Until(retryAt), c.supervision.first), c.supervision.most)
}

// driver returns the driver of m's backend: the coordinator's, or
// backend.NotBuilt for a backend it has none of, whose Probe says so.
func (c *Coordinator) driver(m *backend.Member) backend.Driver {
	if d, ok := c.opts.Drivers[m.Backend]; ok {
		return d
	}
	return backend.NotBuilt{}
}

// A connection is where a member's workers reach the coordinator, as its
// driver's Connect gave it, and what closes what Connect set up.
type connection struct {
	endpoint backend.Endpoint
	closer   io.Closer
}

// connect returns where m's workers reach the coordinator, as d's Connect
// gives it, which the coordinator keeps until it closes. The store's lease
// on a member's connection, which one replica at a time holds, comes with
// the replicas that serve the worker API.
//
// A Forwarder's member's workers reach the coordinator through a listener
// on the member, which ServeMembers holds (forward), and Connect isn't
// called.
func (c *Coordinator) connect(ctx context.Context, d backend.Driver, m *backend.Member) (backend.Endpoint, error) {
	if fw, ok := d.(backend.Forwarder); ok {
		return c.forward(ctx, fw, m)
	}
	key := m.Backend + "\x00" + m.Name + "\x00" + string(m.Settings)
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.connections[key]; ok {
		return conn.endpoint, nil
	}
	endpoint, closer, err := d.Connect(ctx, m, c.opts.Endpoint)
	if err != nil {
		return backend.Endpoint{}, err
	}
	c.connections[key] = connection{endpoint: endpoint, closer: closer}
	return endpoint, nil
}

// memberKey returns the key of m, d's member, where d is a Forwarder,
// which the store records with the attempts placed on it
// (store.Placement.MemberKey), or "".
func memberKey(d backend.Driver, m *backend.Member) (string, error) {
	if fw, ok := d.(backend.Forwarder); ok {
		return fw.MemberKey(m)
	}
	return "", nil
}

// maskedIf returns d, with what its params show of a value masked when
// masked is set (problems.MaskDiagnostics), as when where a job runs is
// private: a member's or a group's name can come from a variable or a
// password. A problem an attempt ends with the store masks itself
// (store.Tx.EndAttempt).
func maskedIf(masked bool, d problems.Diagnostic) problems.Diagnostic {
	if !masked {
		return d
	}
	return problems.MaskDiagnostics(problems.Diagnostics{d})[0]
}
