package store

import (
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// An AttemptState is where an attempt is in its life.
type AttemptState string

// The states of an attempt.
const (
	// AttemptPlacing is an attempt whose worker is being placed, with its
	// bootstrap token, and hasn't claimed it.
	AttemptPlacing AttemptState = "placing"
	// AttemptClaimed is an attempt its worker claimed, with its own
	// credential: its lease runs from the claim.
	AttemptClaimed AttemptState = "claimed"
	// AttemptRunning is an attempt one of whose steps started.
	AttemptRunning AttemptState = "running"
	// AttemptEnded is an attempt that has ended (AttemptEnd).
	AttemptEnded AttemptState = "ended"
)

// An EndKind is how an attempt ended.
type EndKind string

// The ways an attempt ends.
const (
	// EndCompleted is an attempt its worker completed.
	EndCompleted EndKind = "completed"
	// EndSetupFailed is an attempt that failed before a step started.
	EndSetupFailed EndKind = "setup-failed"
	// EndLost is an attempt whose worker was lost.
	EndLost EndKind = "lost"
	// EndSuperseded is an attempt a newer attempt of its job replaced.
	EndSuperseded EndKind = "superseded"
	// EndWithdrawn is an attempt whose placement was withdrawn.
	EndWithdrawn EndKind = "withdrawn"
)

// An Attempt is an attempt of a job, as the store holds it: the record of
// where its job ran, and the lease, the fence and the credentials it
// holds its job with.
type Attempt struct {
	// ID is the attempt's: 32 hex digits, random.
	ID string
	// Run and Job are its job's run and key; Number is its number, from 1,
	// among its job's attempts, and Fence its fence, which is its job's
	// while it holds the job.
	Run    string
	Job    string
	Number int
	Fence  int64
	State  AttemptState
	// SelfFencing is set for an attempt whose job fences itself: its
	// credential lapses with its lease. LeaseSeconds is how long its lease
	// lasts after its claim or a heartbeat, and HeartbeatSeconds how often
	// its worker sends one.
	SelfFencing      bool
	LeaseSeconds     int64
	HeartbeatSeconds int64
	// Worker is the worker that claimed it, and APIVersion the version of
	// the worker API it was claimed at (workerapi.Claim.APIVersion), or 0
	// before its claim.
	Worker     workerapi.Worker
	APIVersion int
	// Placement is where it was placed, once it has been (RecordPlacement,
	// Placed).
	Placement Placement
	// Private reports whether where it is placed is private, as where its
	// job runs is when that reads a variable or a password: its group,
	// member and backend are then sealed, and never held in the clear, and
	// a problem it ends with is stored masked (EndAttempt).
	Private   bool
	CreatedAt time.Time
	// ClaimedAt is when its worker claimed it, RenewedAt when its lease was
	// last renewed, by the claim or a heartbeat, and StepStartedAt when a
	// step of it first started: each the zero time until then.
	ClaimedAt     time.Time
	RenewedAt     time.Time
	StepStartedAt time.Time
	// ProblemSince is when its backend's check of its worker first gave a
	// problem since the backend last decided, or its lease was last
	// renewed (CheckProblem), or the zero time.
	ProblemSince time.Time
	// End is how it ended, once it has.
	End     AttemptEnd
	EndedAt time.Time
	// Holds reports whether the attempt held its job when the transaction
	// read it: whether neither it nor its job had ended, and no newer
	// attempt had superseded it. Only an attempt that holds its job is
	// stored what it reports.
	Holds bool

	tokenHash, credentialHash []byte
}

// Placed reports whether the attempt's placement is recorded
// (RecordPlacement): whether it has the digest of the worker binary it
// started, which every placement recorded has, a private one's too.
func (a *Attempt) Placed() bool { return a.Placement.WorkerDigest != "" }

// LeaseLapses returns when the attempt's lease lapses unless it is renewed,
// by the store's clock: LeaseSeconds after RenewedAt, or the zero time
// before its claim.
func (a *Attempt) LeaseLapses() time.Time {
	if a.RenewedAt.IsZero() {
		return time.Time{}
	}
	return a.RenewedAt.Add(time.Duration(a.LeaseSeconds) * time.Second)
}

// An AttemptEnd is how an attempt ended.
type AttemptEnd struct {
	Kind EndKind
	// Outcome is how the attempt's job ended, for one that completed, and
	// Reason why it ended other than as its steps did, or "".
	Outcome plan.Outcome
	Reason  plan.Reason
	// Report is the worker's report that completed it, a
	// workerapi.Complete as the worker API encodes it, with only the part
	// of its result that run state holds (plan.JobResult.Split), or nil
	// for one that didn't complete.
	Report []byte
	// Problem is what failed before a step started, for one that ended
	// so; why its worker was lost, for one lost after, where that is
	// known, as a problem its backend's check gave for long enough; or nil.
	Problem *problem.Problem
}

// A NewAttempt is an attempt to start.
type NewAttempt struct {
	Run string
	Job string
	// SelfFencing is set for a job that fences itself. HeartbeatSeconds is
	// then a fifth of LeaseSeconds at most, as the worker API asks.
	SelfFencing      bool
	LeaseSeconds     int64
	HeartbeatSeconds int64
}

// NewAttempt starts a new attempt of a job that hasn't ended, of a run that
// hasn't, and returns it and its bootstrap token, which the store keeps
// only the SHA-256 digest of. The attempt's fence is one more than its
// job's last, and its job's from now: an attempt of the job that hadn't
// ended ends, superseded. A job that has ended, or whose run has, is
// ErrState.
func (tx *Tx) NewAttempt(n NewAttempt) (*Attempt, string, error) {
	switch {
	case n.LeaseSeconds <= 0 || n.HeartbeatSeconds <= 0 || n.HeartbeatSeconds > n.LeaseSeconds:
		return nil, "", fmt.Errorf("store: a lease of %ds with heartbeats every %ds", n.LeaseSeconds, n.HeartbeatSeconds)
	case n.SelfFencing && 5*n.HeartbeatSeconds > n.LeaseSeconds:
		return nil, "", fmt.Errorf("store: heartbeats every %ds are more than a fifth of a lease of %ds, which a job that fences itself needs", n.HeartbeatSeconds, n.LeaseSeconds)
	}
	run, err := tx.liveRun(n.Run)
	if err != nil {
		return nil, "", err
	}
	j, err := tx.Job(n.Run, n.Job)
	if err != nil {
		return nil, "", err
	}
	if j.Outcome != "" {
		return nil, "", fmt.Errorf("%w: run %s's job %q has ended", ErrState, n.Run, n.Job)
	}
	now, err := tx.now()
	if err != nil {
		return nil, "", err
	}
	superseded, err := tx.query("UPDATE meridian_attempts SET state = ?, end_kind = ?, ended_at = ? WHERE run_id = ? AND job_key = ? AND state <> ? RETURNING id",
		AttemptEnded, EndSuperseded, now, run, n.Job, AttemptEnded)
	if err != nil {
		return nil, "", err
	}
	var ended []string
	for superseded.Next() {
		var id string
		if err := superseded.Scan(&id); err != nil {
			superseded.Close()
			return nil, "", err
		}
		ended = append(ended, id)
	}
	if err := superseded.Close(); err != nil {
		return nil, "", err
	}
	if err := superseded.Err(); err != nil {
		return nil, "", err
	}
	for _, id := range ended {
		if err := tx.addEvent(run, EventAttemptEnded, n.Job, id, 0); err != nil {
			return nil, "", err
		}
	}
	var fence, number int64
	if err := tx.queryRow("UPDATE meridian_jobs SET fence = fence + 1, attempts = attempts + 1 WHERE run_id = ? AND key = ? RETURNING fence, attempts",
		run, n.Job).Scan(&fence, &number); err != nil {
		return nil, "", err
	}
	id, token := newID(16), newID(32)
	if _, err := tx.exec(`INSERT INTO meridian_attempts (id, run_id, job_key, number, fence, state, token_hash,
  self_fencing, lease_seconds, heartbeat_seconds, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, run, n.Job, number, fence, AttemptPlacing, digest(token), n.SelfFencing, n.LeaseSeconds, n.HeartbeatSeconds, now); err != nil {
		return nil, "", err
	}
	// What every attempt of the run tells its worker, this one does from
	// the start.
	if err := tx.StartControl(id); err != nil {
		return nil, "", err
	}
	a, err := tx.Attempt(id)
	return a, token, err
}

// NewToken gives an attempt that hasn't been claimed a new bootstrap token
// in place of its last, which no longer claims it, as for an attempt
// placed again after the coordinator lost what it placed it with. An
// attempt that has been claimed is ErrState, and one that no longer holds
// its job ErrFenced.
func (tx *Tx) NewToken(id string) (string, error) {
	a, err := tx.Attempt(id)
	if err != nil {
		return "", err
	}
	switch {
	case !a.Holds:
		return "", ErrFenced
	case a.State != AttemptPlacing:
		return "", fmt.Errorf("%w: attempt %s has been claimed", ErrState, id)
	}
	token := newID(32)
	_, err = tx.exec("UPDATE meridian_attempts SET token_hash = ? WHERE id = ?", digest(token), id)
	return token, err
}

const attemptColumns = `a.id, a.run_id, a.job_key, a.number, a.fence, a.state, a.token_hash, a.credential_hash,
  a.self_fencing, a.lease_seconds, a.heartbeat_seconds, a.worker_version, a.worker_digest, a.api_version,
  a.grp, a.member, a.backend, a.placement_id, a.placed_digest, a.private_placement IS NOT NULL,
  a.created_at, a.claimed_at, a.renewed_at, a.step_started_at, a.problem_since,
  a.end_kind, a.outcome, a.reason, a.report, a.problem, a.ended_at,
  a.state <> 'ended' AND a.fence = j.fence AND j.outcome IS NULL`

// attemptWhere returns the attempt where is true of: ErrNotFound if it is
// true of none.
func (tx *Tx) attemptWhere(where string, args ...any) (*Attempt, error) {
	var (
		a                                       Attempt
		run, created                            int64
		state                                   string
		version, wdigest, kind, outcome, reason sql.NullString
		group, member, backend, pid, placed     sql.NullString
		claimed, renewed, started, ended        sql.NullInt64
		problemSince                            sql.NullInt64
		api                                     sql.NullInt64
		prob                                    []byte
	)
	err := tx.queryRow("SELECT "+attemptColumns+`
FROM meridian_attempts a JOIN meridian_jobs j ON j.run_id = a.run_id AND j.key = a.job_key
WHERE `+where, args...).Scan(
		&a.ID, &run, &a.Job, &a.Number, &a.Fence, &state, &a.tokenHash, &a.credentialHash,
		&a.SelfFencing, &a.LeaseSeconds, &a.HeartbeatSeconds, &version, &wdigest, &api,
		&group, &member, &backend, &pid, &placed, &a.Private,
		&created, &claimed, &renewed, &started, &problemSince,
		&kind, &outcome, &reason, &a.End.Report, &prob, &ended, &a.Holds)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	a.Run, a.State = runID(run), AttemptState(state)
	a.Worker, a.APIVersion = workerapi.Worker{Version: version.String, Digest: wdigest.String}, int(api.Int64)
	a.Placement = Placement{Group: group.String, Member: member.String, Backend: backend.String, ID: pid.String, WorkerDigest: placed.String}
	a.CreatedAt, a.ClaimedAt, a.RenewedAt, a.StepStartedAt = time.UnixMicro(created).UTC(), timeOf(claimed), timeOf(renewed), timeOf(started)
	a.ProblemSince = timeOf(problemSince)
	a.End.Kind, a.End.Outcome, a.End.Reason = EndKind(kind.String), plan.Outcome(outcome.String), plan.Reason(reason.String)
	a.EndedAt = timeOf(ended)
	if prob != nil {
		a.End.Problem = new(problem.Problem)
		if err := jsonv2.Unmarshal(prob, a.End.Problem, jsonv2.RejectUnknownMembers(true)); err != nil {
			return nil, fmt.Errorf("%w: the problem attempt %s ended with: %w", ErrMalformed, a.ID, err)
		}
	}
	return &a, nil
}

// AttemptByToken returns the attempt whose bootstrap token is token,
// whatever its state: ErrNotFound if it is no attempt's, as a token
// replaced by NewToken is no longer.
func (tx *Tx) AttemptByToken(token string) (*Attempt, error) {
	return tx.attemptWhere("a.token_hash = ?", digest(token))
}

// Attempt returns the attempt with the ID id.
func (tx *Tx) Attempt(id string) (*Attempt, error) {
	return tx.attemptWhere("a.id = ?", id)
}

// AttemptByCredential returns the attempt claimed with credential,
// whatever its state: ErrNotFound if none was.
func (tx *Tx) AttemptByCredential(credential string) (*Attempt, error) {
	return tx.attemptWhere("a.credential_hash = ?", digest(credential))
}

// Claim records that a worker, w, claimed the attempt whose bootstrap
// token is token, with credential, which its later calls give, at the
// version of the worker API apiVersion, and returns the attempt, its lease
// running from now. A claim repeated with the same credential changes
// nothing: the attempt keeps the version of its first. A token that is no
// attempt's is ErrNotFound, one of an attempt claimed with another
// credential ErrCredential, one of an attempt that no longer holds its job
// ErrFenced, and a first claim of one whose placement isn't recorded yet
// ErrNotPlaced.
func (tx *Tx) Claim(token, credential string, w workerapi.Worker, apiVersion int) (*Attempt, error) {
	if len(credential) < 32 {
		return nil, errors.New("store: a credential shorter than 32 bytes")
	}
	a, err := tx.attemptWhere("a.token_hash = ?", digest(token))
	if err != nil {
		return nil, err
	}
	cred := digest(credential)
	now, err := tx.now()
	if err != nil {
		return nil, err
	}
	if a.credentialHash != nil {
		if subtle.ConstantTimeCompare(a.credentialHash, cred) != 1 {
			return nil, ErrCredential
		}
		if err := usable(a, now); err != nil {
			return nil, err
		}
		return a, nil
	}
	switch {
	case !a.Holds:
		return nil, ErrFenced
	case !a.Placed():
		return nil, ErrNotPlaced
	case apiVersion < 1:
		return nil, fmt.Errorf("store: a claim at version %d of the worker API, which has none before 1", apiVersion)
	}
	var taken bool
	if err := tx.queryRow("SELECT EXISTS (SELECT 1 FROM meridian_attempts WHERE credential_hash = ?)", cred).Scan(&taken); err != nil {
		return nil, err
	}
	if taken {
		return nil, ErrCredential
	}
	if _, err := tx.exec(`UPDATE meridian_attempts SET state = ?, credential_hash = ?, worker_version = ?, worker_digest = ?, api_version = ?,
  claimed_at = ?, renewed_at = ?, problem_since = NULL WHERE id = ?`, AttemptClaimed, cred, w.Version, w.Digest, apiVersion, now, now, a.ID); err != nil {
		return nil, err
	}
	n, _ := runNumber(a.Run)
	if err := tx.addEvent(n, EventJobRunning, a.Job, a.ID, 0); err != nil {
		return nil, err
	}
	return tx.Attempt(a.ID)
}

// usable returns nil if the attempt a can report, at now by the store's
// clock: ErrFenced if it no longer holds its job, and ErrLapsed if its job
// fences itself and its lease has lapsed.
func usable(a *Attempt, now int64) error {
	switch {
	case !a.Holds:
		return ErrFenced
	case a.SelfFencing && !a.RenewedAt.IsZero() && now >= a.LeaseLapses().UnixMicro():
		return ErrLapsed
	}
	return nil
}

// Authorize returns the attempt claimed with credential, if it may report
// now: ErrNotFound if no attempt was claimed with it, ErrFenced if the
// attempt no longer holds its job, and ErrLapsed if its job fences itself
// and its lease has lapsed. Check it in the transaction that stores what
// the attempt reports.
func (tx *Tx) Authorize(credential string) (*Attempt, error) {
	a, err := tx.AttemptByCredential(credential)
	if err != nil {
		return nil, err
	}
	now, err := tx.now()
	if err != nil {
		return nil, err
	}
	if err := usable(a, now); err != nil {
		return nil, err
	}
	return a, nil
}

// Renew renews the lease of the attempt claimed with credential, from now,
// as a heartbeat does, and returns the attempt: Authorize's errors if it
// may not report.
func (tx *Tx) Renew(credential string) (*Attempt, error) {
	a, err := tx.Authorize(credential)
	if err != nil {
		return nil, err
	}
	now, err := tx.now()
	if err != nil {
		return nil, err
	}
	if _, err := tx.exec("UPDATE meridian_attempts SET renewed_at = ?, problem_since = NULL WHERE id = ?", now, a.ID); err != nil {
		return nil, err
	}
	a.RenewedAt, a.ProblemSince = time.UnixMicro(now).UTC(), time.Time{}
	return a, nil
}

// CheckProblem records that its backend's check of the attempt a's worker
// gave a problem, a cause that won't pass by itself, and returns when the
// first such check was since its backend last decided (CheckDecided), or
// its lease was renewed, by its claim or a heartbeat: now, if this is the
// first. A check of a as it was before its lease was renewed since, as one
// a heartbeat overtook, records nothing, and is ErrState: the renewal says
// more of the worker than the check did. ErrNotFound if there is no
// attempt a.
func (tx *Tx) CheckProblem(a *Attempt) (time.Time, error) {
	now, err := tx.now()
	if err != nil {
		return time.Time{}, err
	}
	var renewed sql.NullInt64
	if !a.RenewedAt.IsZero() {
		renewed = sql.NullInt64{Int64: a.RenewedAt.UnixMicro(), Valid: true}
	}
	var since int64
	err = tx.queryRow("UPDATE meridian_attempts SET problem_since = coalesce(problem_since, ?) WHERE id = ? AND renewed_at IS ? RETURNING problem_since",
		now, a.ID, renewed).Scan(&since)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.Attempt(a.ID); err != nil {
			return time.Time{}, err
		}
		return time.Time{}, fmt.Errorf("%w: attempt %s's lease was renewed since it was checked", ErrState, a.ID)
	}
	return time.UnixMicro(since).UTC(), err
}

// CheckDecided records that its backend's check of the attempt's worker
// decided, which ends what CheckProblem counts.
func (tx *Tx) CheckDecided(id string) error {
	_, err := tx.exec("UPDATE meridian_attempts SET problem_since = NULL WHERE id = ? AND problem_since IS NOT NULL", id)
	return err
}

// StepStarted records that a step of the attempt started: the attempt is
// running, and counts among those of its job that started a step. Once is
// enough: a repeat changes nothing. An attempt that hasn't been claimed is
// ErrState, and one that no longer holds its job ErrFenced.
func (tx *Tx) StepStarted(id string) error {
	a, err := tx.Attempt(id)
	if err != nil {
		return err
	}
	switch {
	case !a.Holds:
		return ErrFenced
	case a.State == AttemptRunning:
		return nil
	case a.State != AttemptClaimed:
		return fmt.Errorf("%w: attempt %s hasn't been claimed", ErrState, id)
	}
	now, err := tx.now()
	if err != nil {
		return err
	}
	if _, err := tx.exec("UPDATE meridian_attempts SET state = ?, step_started_at = ? WHERE id = ?", AttemptRunning, now, id); err != nil {
		return err
	}
	n, _ := runNumber(a.Run)
	_, err = tx.exec("UPDATE meridian_jobs SET attempts_started = attempts_started + 1 WHERE run_id = ? AND key = ?", n, a.Job)
	return err
}

// EndAttempt records how the attempt ended. The problem of an attempt
// whose placement is private (Attempt.Private) is stored masked
// (problems.MaskProblem), since it can name the member; its report is the
// caller's to mask. An attempt that no longer holds its job, as one that
// has ended doesn't, is ErrFenced.
func (tx *Tx) EndAttempt(id string, end AttemptEnd) error {
	switch end.Kind {
	case EndCompleted:
		switch end.Outcome {
		case plan.OutcomeSuccess, plan.OutcomeFailure, plan.OutcomeCancelled, plan.OutcomeSkipped:
		default:
			return fmt.Errorf("store: an attempt completed with the outcome %q", end.Outcome)
		}
	case EndSetupFailed, EndLost, EndSuperseded, EndWithdrawn:
		if end.Report != nil {
			return fmt.Errorf("store: an attempt that ended %s with a report", end.Kind)
		}
	default:
		return fmt.Errorf("store: %q isn't how an attempt ends", end.Kind)
	}
	if end.Problem != nil && end.Kind != EndCompleted && end.Kind != EndSetupFailed && end.Kind != EndLost {
		return fmt.Errorf("store: an attempt that ended %s with a problem", end.Kind)
	}
	a, err := tx.Attempt(id)
	if err != nil {
		return err
	}
	switch {
	case !a.Holds:
		return ErrFenced
	case end.Kind == EndCompleted && a.State == AttemptPlacing:
		return fmt.Errorf("%w: attempt %s completed before it was claimed", ErrState, id)
	}
	var prob []byte
	if end.Problem != nil {
		p := end.Problem
		if a.Private {
			p = problems.MaskProblem(p)
		}
		if prob, err = jsonv2.Marshal(p, jsonv2.Deterministic(true)); err != nil {
			return err
		}
	}
	now, err := tx.now()
	if err != nil {
		return err
	}
	_, err = tx.exec(`UPDATE meridian_attempts SET state = ?, end_kind = ?, outcome = ?, reason = ?, report = ?,
  problem = ?, ended_at = ? WHERE id = ?`,
		AttemptEnded, end.Kind, nullString(string(end.Outcome)), nullString(string(end.Reason)),
		end.Report, prob, now, id)
	if err != nil {
		return err
	}
	n, _ := runNumber(a.Run)
	return tx.addEvent(n, EventAttemptEnded, a.Job, id, 0)
}

// digest returns the SHA-256 digest of a secret, as the store keeps a
// bootstrap token or a credential.
func digest(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}
