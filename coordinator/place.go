package coordinator

import (
	"cmp"
	"context"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"slices"
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
// start it soonest. A job no member can run fails, with
// placement_impossible and each member's reason.
func (c *Coordinator) placeJob(ctx context.Context, run *store.Run, job *store.Job, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	r, err := c.readyJob(ctx, run, job.Key)
	switch {
	case err != nil:
		return 0, err
	case r.diags.HasErrors():
		return 0, c.endJob(ctx, run.ID, job.Key, store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Diagnostics: r.diags}, done)
	case r.result.Skipped:
		return 0, c.endJob(ctx, run.ID, job.Key, store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeSkipped, NeedsFailed: r.result.NeedsFailed}}, done)
	case r.result.Job == nil:
		return 0, fmt.Errorf("coordinator: Ready expanded run %s's job %q's matrix", run.ID, job.Key)
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
	fits, reasons, unanswered := c.rank(ctx, g, demandOf(rj), private)
	if len(fits) == 0 {
		if unanswered {
			return c.unanswered(ctx, run, job, private, errors.New("a member's backend couldn't say whether it can run the job"))
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
// any other error is a backend that couldn't answer.
func (c *Coordinator) placeAttempt(ctx context.Context, run *store.Run, job *store.Job, id, token string, rj *plan.ReadyJob, chosen *candidate, private bool) (time.Duration, error) {
	endpoint, err := c.connect(ctx, chosen.driver, chosen.member)
	var p backend.Placement
	if err == nil {
		p, err = chosen.driver.Place(ctx, chosen.member, backend.PlaceRequest{
			Attempt: id, Name: "meridian-" + run.ID + "-" + job.Key, Coordinator: endpoint, BootstrapToken: token,
			TimeoutSeconds: cmp.Or(rj.TimeoutSeconds, chosen.fit.DefaultTimeoutSeconds),
			Resources:      backend.Resources(rj.Resources), RunDir: run.RunDir,
		})
	}
	if prob, ok := errors.AsType[*problem.Problem](err); ok {
		return 0, c.attemptSetupFailed(ctx, id, prob)
	}
	if err != nil {
		return c.unanswered(ctx, run, job, private, err)
	}
	return c.recordPlacement(ctx, run.ID, id, chosen, p, private)
}

// recordPlacement records the placement p of the attempt id of the run
// runID, on chosen, and returns how long to wait for its worker. Of a
// private placement, the store holds the worker's digest in the clear, and
// the backend's ID for it sealed, with the rest of its place, as the
// place's next generation, which the same transaction stores.
func (c *Coordinator) recordPlacement(ctx context.Context, runID, id string, chosen *candidate, p backend.Placement, private bool) (time.Duration, error) {
	err := c.store.Tx(ctx, func(tx *store.Tx) error {
		if !private {
			return tx.RecordPlacement(id, store.Placement{Group: chosen.group, Member: chosen.member.Name, Backend: chosen.member.Backend, ID: p.ID, WorkerDigest: p.WorkerDigest})
		}
		place, _, err := tx.ChosenPlace(id)
		if err != nil {
			return err
		}
		sealed, err := c.sealPlace(ctx, tx, runID, id, place.Generation+1,
			sealedPlace{Group: chosen.group, Member: chosen.member.Name, Backend: chosen.member.Backend, ID: p.ID})
		if err != nil {
			return err
		}
		if err := tx.ResealPlace(id, place.Generation+1, sealed); err != nil {
			return err
		}
		return tx.RecordPlacement(id, store.Placement{WorkerDigest: p.WorkerDigest})
	})
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
// group fails the attempt before its first step.
func (c *Coordinator) resumePlace(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt) (time.Duration, error) {
	var (
		place   store.Place
		chosen  bool
		wrapped []byte
		private = a.Private
	)
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if place, chosen, err = tx.ChosenPlace(a.ID); err != nil || !private {
			return err
		}
		wrapped, err = tx.WrappedKey(run.ID)
		return err
	})
	switch {
	case err != nil:
		return 0, err
	case !chosen:
		return 0, fmt.Errorf("coordinator: attempt %s has no place", a.ID)
	}
	opened, err := c.openPlaces(ctx, run.ID, wrapped, map[string]store.Place{a.ID: place})
	if err != nil {
		return 0, err
	}
	at := opened[a.ID]
	r, err := c.readyJob(ctx, run, job.Key)
	switch {
	case err != nil:
		return 0, err
	case r.result.Job == nil:
		return 0, fmt.Errorf("coordinator: run %s's job %q has an attempt, but Ready gives no job", run.ID, job.Key)
	}
	rj := r.result.Job
	g, d, err := c.group(ctx, run, rj, private)
	if err != nil {
		return c.unanswered(ctx, run, job, private, err)
	}
	var m *backend.Member
	if d == nil {
		if i := slices.IndexFunc(g.Members, func(m backend.Member) bool { return m.Name == at.Member && m.Backend == at.Backend }); i >= 0 {
			m = &g.Members[i]
		}
	}
	if m == nil || g.Name != at.Group {
		return 0, c.attemptSetupFailed(ctx, a.ID, nil)
	}
	cand := &candidate{group: g.Name, member: m, driver: c.driver(m)}
	p, found, err := cand.driver.Find(ctx, m, a.ID)
	if prob, ok := errors.AsType[*problem.Problem](err); ok {
		return 0, c.attemptSetupFailed(ctx, a.ID, prob)
	}
	if err != nil {
		return c.unanswered(ctx, run, job, private, err)
	}
	if found {
		return c.recordPlacement(ctx, run.ID, a.ID, cand, p, private)
	}
	if cand.fit, err = cand.driver.Probe(ctx, m, demandOf(rj)); err == nil && cand.fit.Problem != nil {
		err = cand.fit.Problem
	}
	if prob, ok := errors.AsType[*problem.Problem](err); ok {
		return 0, c.attemptSetupFailed(ctx, a.ID, prob)
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

// unanswered handles cause, a backend's, or Groups', that couldn't answer
// as the run's job, as the look saw it, was placed: it is asked again,
// after a while, until it has been as many times in a row as it may be,
// which then counts as a failure before the job's first step, with
// placement_unanswered, of its attempt that wasn't placed, or of one made
// for the record. The log says what cause says, unless where the job runs
// is private, when what it says can name the member.
func (c *Coordinator) unanswered(ctx context.Context, run *store.Run, job *store.Job, private bool, cause error) (time.Duration, error) {
	key := job.Key
	if private {
		c.log.WarnContext(ctx, "coordinator: a backend couldn't answer as a job was placed, and is asked again later", "run", run.ID, "job", key)
	} else {
		c.log.WarnContext(ctx, "coordinator: a backend couldn't answer as a job was placed, and is asked again later", "run", run.ID, "job", key, "error", cause)
	}
	var tries int
	if err := c.store.Tx(ctx, func(tx *store.Tx) error {
		var err error
		tries, err = tx.PlaceTried(run.ID, key)
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

// demandOf is what rj asks of a member.
func demandOf(rj *plan.ReadyJob) backend.Demand {
	return backend.Demand{Resources: backend.Resources(rj.Resources), TimeoutSeconds: rj.TimeoutSeconds}
}

// rank returns the members of g that can run a job that asks for demand,
// as their drivers' Probe say, the soonest to start it first, and in the
// group's order among those as soon; and, for each member that can't, the
// problem its Probe gave, masked when where the job runs is private.
// unanswered reports that a Probe couldn't answer, so that a job no member
// can run now may be placed later.
func (c *Coordinator) rank(ctx context.Context, g *backend.Group, demand backend.Demand, private bool) (fits []candidate, reasons problems.Diagnostics, unanswered bool) {
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
			if !private {
				c.log.WarnContext(ctx, "coordinator: a member's backend couldn't say whether it can run a job", "member", m.Name, "error", err)
			}
			unanswered = true
			continue
		}
		fits = append(fits, candidate{group: g.Name, member: m, driver: d, fit: fit})
	}
	slices.SortStableFunc(fits, func(a, b candidate) int { return cmp.Compare(a.fit.Availability, b.fit.Availability) })
	return fits, reasons, unanswered
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
func (c *Coordinator) connect(ctx context.Context, d backend.Driver, m *backend.Member) (backend.Endpoint, error) {
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
