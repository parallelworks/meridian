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
		return c.unanswered(ctx, run, job.Key, private, err)
	case d != nil:
		return 0, c.endJob(ctx, run.ID, job.Key, store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Diagnostics: problems.Diagnostics{*d}}, done)
	}
	fits, reasons, unanswered := c.rank(ctx, g, demandOf(rj), private)
	if len(fits) == 0 {
		if unanswered {
			return c.unanswered(ctx, run, job.Key, private, errors.New("a member's backend couldn't say whether it can run the job"))
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
		a, t, err := tx.NewAttempt(c.newAttempt(run.ID, job.Key, selfFence))
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
	if errors.Is(err, store.ErrState) {
		return 0, errAgain
	}
	if err != nil {
		return 0, err
	}
	return c.placeAttempt(ctx, run, job.Key, id, token, rj, chosen, private)
}

// choose returns the first of fits, members of a group ranked to run the
// run's job key, that none of the job's attempts failed on before a step
// started, as long as one is left; and the first of them otherwise.
func (c *Coordinator) choose(ctx context.Context, run *store.Run, key string, fits []candidate) (*candidate, error) {
	var (
		places = map[string]store.Place{} // by attempt
		stored *store.Record
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
			case p.Private != nil && stored == nil:
				if stored, err = tx.Record(run.ID); err != nil {
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
	failed := map[[3]string]bool{}
	for id, p := range places {
		if p, err = c.openPlace(ctx, run.ID, id, stored, p); err != nil {
			return nil, err
		}
		failed[[3]string{p.Group, p.Member, p.Backend}] = true
	}
	for i := range fits {
		if !failed[[3]string{fits[i].group, fits[i].member.Name, fits[i].member.Backend}] {
			return &fits[i], nil
		}
	}
	return &fits[0], nil
}

// A sealedPlace is what an attempt whose place is private keeps of it,
// sealed under the run's data key.
type sealedPlace struct {
	Group   string `json:"group"`
	Member  string `json:"member"`
	Backend string `json:"backend"`
}

// placeOf is where the attempt id of the run runID is placed, on chosen:
// in the clear, or sealed under the data key of the run's record, as tx
// reads it, when where its job runs is private.
func (c *Coordinator) placeOf(ctx context.Context, tx *store.Tx, runID, id string, chosen *candidate, private bool) (store.Place, error) {
	if !private {
		return store.Place{Group: chosen.group, Member: chosen.member.Name, Backend: chosen.member.Backend}, nil
	}
	data, err := jsonv2.Marshal(sealedPlace{Group: chosen.group, Member: chosen.member.Name, Backend: chosen.member.Backend}, jsonv2.Deterministic(true))
	if err != nil {
		return store.Place{}, err
	}
	stored, err := tx.Record(runID)
	if err != nil {
		return store.Place{}, err
	}
	sealed, err := c.sealer.SealPlacement(ctx, runID, id, stored.WrappedKey, data)
	if err != nil {
		return store.Place{}, err
	}
	return store.Place{Private: sealed}, nil
}

// openPlace returns the place of the attempt id of the run runID in the
// clear: p, or what it holds sealed, opened with the data key of stored,
// the run's record.
func (c *Coordinator) openPlace(ctx context.Context, runID, id string, stored *store.Record, p store.Place) (store.Place, error) {
	if p.Private == nil {
		return p, nil
	}
	data, err := c.sealer.OpenPlacement(ctx, runID, id, stored.WrappedKey, p.Private)
	if err != nil {
		return store.Place{}, err
	}
	var sp sealedPlace
	if err := jsonv2.Unmarshal(data, &sp, jsonv2.RejectUnknownMembers(true)); err != nil {
		return store.Place{}, fmt.Errorf("coordinator: attempt %s's sealed place: %w", id, err)
	}
	return store.Place{Group: sp.Group, Member: sp.Member, Backend: sp.Backend}, nil
}

// placeAttempt asks chosen's driver to place the attempt id of the run's
// job key, with its bootstrap token, and records the placement it gives.
// A problem from the driver fails the attempt before its first step, and
// any other error is a backend that couldn't answer.
func (c *Coordinator) placeAttempt(ctx context.Context, run *store.Run, key, id, token string, rj *plan.ReadyJob, chosen *candidate, private bool) (time.Duration, error) {
	endpoint, err := c.connect(ctx, chosen.driver, chosen.member)
	var p backend.Placement
	if err == nil {
		p, err = chosen.driver.Place(ctx, chosen.member, backend.PlaceRequest{
			Attempt: id, Name: "meridian-" + run.ID + "-" + key, Coordinator: endpoint, BootstrapToken: token,
			TimeoutSeconds: cmp.Or(rj.TimeoutSeconds, chosen.fit.DefaultTimeoutSeconds),
			Resources:      backend.Resources(rj.Resources), RunDir: run.RunDir,
		})
	}
	if prob, ok := errors.AsType[*problem.Problem](err); ok {
		return 0, c.attemptSetupFailed(ctx, id, maskedProblemIf(private, prob))
	}
	if err != nil {
		return c.unanswered(ctx, run, key, private, err)
	}
	return c.recordPlacement(ctx, id, chosen, p, private)
}

// recordPlacement records the attempt id's placement p, on chosen, and
// returns how long to wait for its worker.
func (c *Coordinator) recordPlacement(ctx context.Context, id string, chosen *candidate, p backend.Placement, private bool) (time.Duration, error) {
	placement := store.Placement{Group: chosen.group, Member: chosen.member.Name, Backend: chosen.member.Backend, ID: p.ID, WorkerDigest: p.WorkerDigest}
	if private {
		// The placement's ID is its backend's own name for it, which names
		// no member; the rest is sealed (store.Place).
		placement.Group, placement.Member, placement.Backend = "", "", ""
	}
	err := c.store.Tx(ctx, func(tx *store.Tx) error { return tx.RecordPlacement(id, placement) })
	switch {
	case errors.Is(err, store.ErrFenced):
		// Superseded meanwhile, as by another attempt of the supervisor:
		// its worker's claim is refused.
		return 0, errAgain
	case errors.Is(err, store.ErrState):
		// Claimed already: its worker was quicker than the record.
		return c.supervision.wait, nil
	case err != nil:
		return 0, err
	}
	return c.supervision.wait, nil
}

// attemptSetupFailed ends the attempt id as one that failed before its
// first step, with p, and has the supervisor look again, to place the job
// again or fail it.
func (c *Coordinator) attemptSetupFailed(ctx context.Context, id string, p *problem.Problem) error {
	err := c.store.Tx(ctx, func(tx *store.Tx) error {
		return tx.EndAttempt(id, store.AttemptEnd{Kind: store.EndSetupFailed, Outcome: plan.OutcomeFailure, Reason: plan.ReasonSetupFailed, Problem: p})
	})
	if err != nil && !errors.Is(err, store.ErrFenced) {
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
		place  store.Place
		chosen bool
		stored *store.Record
	)
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if place, chosen, err = tx.ChosenPlace(a.ID); err != nil {
			return err
		}
		stored, err = tx.Record(run.ID)
		return err
	})
	switch {
	case err != nil:
		return 0, err
	case !chosen:
		return 0, fmt.Errorf("coordinator: attempt %s has no place", a.ID)
	}
	private := place.Private != nil
	if place, err = c.openPlace(ctx, run.ID, a.ID, stored, place); err != nil {
		return 0, err
	}
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
		return c.unanswered(ctx, run, job.Key, private, err)
	}
	var m *backend.Member
	if d == nil {
		if i := slices.IndexFunc(g.Members, func(m backend.Member) bool { return m.Name == place.Member && m.Backend == place.Backend }); i >= 0 {
			m = &g.Members[i]
		}
	}
	if m == nil || g.Name != place.Group {
		return 0, c.attemptSetupFailed(ctx, a.ID, nil)
	}
	cand := &candidate{group: g.Name, member: m, driver: c.driver(m)}
	p, found, err := cand.driver.Find(ctx, m, a.ID)
	if prob, ok := errors.AsType[*problem.Problem](err); ok {
		return 0, c.attemptSetupFailed(ctx, a.ID, maskedProblemIf(private, prob))
	}
	if err != nil {
		return c.unanswered(ctx, run, job.Key, private, err)
	}
	if found {
		return c.recordPlacement(ctx, a.ID, cand, p, private)
	}
	if cand.fit, err = cand.driver.Probe(ctx, m, demandOf(rj)); err == nil && cand.fit.Problem != nil {
		err = cand.fit.Problem
	}
	if prob, ok := errors.AsType[*problem.Problem](err); ok {
		return 0, c.attemptSetupFailed(ctx, a.ID, maskedProblemIf(private, prob))
	}
	if err != nil {
		return c.unanswered(ctx, run, job.Key, private, err)
	}
	var token string
	err = c.store.Tx(ctx, func(tx *store.Tx) error {
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
	return c.placeAttempt(ctx, run, job.Key, a.ID, token, rj, cand, private)
}

// unanswered handles cause, a backend's, or Groups', that couldn't answer
// as the run's job key was placed: it is asked again, after a while, until
// it has been as many times in a row as it may be, which then counts as a
// failure before the job's first step, with placement_unanswered, of its
// attempt that wasn't placed, or of one made for the record. The log says
// what cause says, unless where the job runs is private, when what it
// says can name the member.
func (c *Coordinator) unanswered(ctx context.Context, run *store.Run, key string, private bool, cause error) (time.Duration, error) {
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
		_, err := tx.PlaceFailed(c.newAttempt(run.ID, key, false), p)
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
// masked is set, as when where a job runs is private: a member's or a
// group's name can come from a variable or a password.
func maskedIf(masked bool, d problems.Diagnostic) problems.Diagnostic {
	if !masked {
		return d
	}
	params := problems.MaskValues(d.Params)
	// Where in a file it is, At gives again.
	for _, name := range []string{problems.ParamFile, problems.ParamLine, problems.ParamColumn} {
		delete(params, name)
	}
	if k, ok := problems.Lookup(d.Code); ok {
		return k.At(d.File(), d.Position(), d.Path(), params)
	}
	return problems.Diagnostic{Type: d.Type, Code: d.Code, Pointer: d.Pointer, Params: params}
}

// maskedProblemIf returns p, with what its params show of a value masked
// when masked is set, as maskedIf does.
func maskedProblemIf(masked bool, p *problem.Problem) *problem.Problem {
	if !masked {
		return p
	}
	params := problems.MaskValues(p.Params)
	if k, ok := problems.Lookup(p.Code); ok {
		return k.Problem(params)
	}
	q := *p
	q.Params, q.Detail = params, ""
	return &q
}
