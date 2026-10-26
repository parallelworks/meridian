package workerapisrv

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// serveHeartbeat renews the attempt's lease.
func (s *Server) serveHeartbeat(w http.ResponseWriter, r *http.Request) {
	err := s.store.Tx(r.Context(), func(tx *store.Tx) error {
		_, err := tx.Renew(bearer(r))
		return err
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// serveControl answers with what the coordinator asks of the attempt's
// worker once it has a newer version than the one the worker saw, or, at
// the latest, after the server's hold, with what it has.
func (s *Server) serveControl(w http.ResponseWriter, r *http.Request, a *store.Attempt) {
	ctx := r.Context()
	seen, err := strconv.ParseInt(r.URL.Query().Get(workerapi.QueryVersion), 10, 64)
	if err != nil {
		s.badRequest(w, r, "a control poll without a version")
		return
	}
	hold := time.NewTimer(s.hold)
	defer hold.Stop()
	tick := time.NewTicker(recheck)
	defer tick.Stop()
	for {
		// The broadcast is taken before the store is read, so that a
		// Notify after the read wakes the wait below.
		changed := s.changes()
		var c workerapi.Control
		if err := s.store.View(ctx, func(tx *store.Tx) error {
			c, err = tx.Control(a.ID)
			return err
		}); err != nil {
			s.writeError(w, r, err)
			return
		}
		if c.Version > seen {
			writeJSON(w, &c)
			return
		}
		select {
		case <-changed:
		case <-tick.C:
		case <-hold.C:
			writeJSON(w, &c)
			return
		case <-ctx.Done():
			return
		}
	}
}

// serveEvents stores the attempt's events it hasn't yet, in order, and
// answers with the sequence number of the last it has.
func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request, a *store.Attempt) {
	ctx := r.Context()
	body, ok := readBody(w, r, MaxEventsBody)
	if !ok {
		return
	}
	var events workerapi.Events
	if err := workerapi.Unmarshal(body, &events); err != nil {
		s.badRequest(w, r, "events that aren't")
		return
	}
	var last int64
	err := s.store.Tx(ctx, func(tx *store.Tx) error {
		if _, err := tx.Authorize(bearer(r)); err != nil {
			return err
		}
		var err error
		last, err = tx.AppendEvents(a.ID, events.Events)
		return err
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	// A step's first start makes the attempt running, which its job's
	// supervisor counts.
	if a.State == store.AttemptClaimed && slices.ContainsFunc(events.Events, func(e workerapi.Event) bool { return !e.Finished }) {
		s.wakeJob(ctx, a.Run, a.Job)
	}
	writeJSON(w, &workerapi.EventsAck{Seq: last})
}

// logParts are the logs an attempt has.
var logParts = []workerapi.LogPart{workerapi.LogSetup, workerapi.LogStep, workerapi.LogCleanup}

// serveLogs stores part of one of the attempt's logs, and answers with the
// log's length and whether it has ended.
func (s *Server) serveLogs(w http.ResponseWriter, r *http.Request, a *store.Attempt, run *store.Run) {
	ctx := r.Context()
	q := r.URL.Query()
	key := LogKey{Run: a.Run, Job: a.Job, Attempt: a.Number, Part: workerapi.LogPart(q.Get(workerapi.QueryPart))}
	offset, err := strconv.ParseInt(q.Get(workerapi.QueryOffset), 10, 64)
	if err != nil || offset < 0 || !slices.Contains(logParts, key.Part) {
		s.badRequest(w, r, "a log upload whose log or offset isn't one")
		return
	}
	end := false
	if v := q.Get(workerapi.QueryEnd); v != "" {
		if end, err = strconv.ParseBool(v); err != nil {
			s.badRequest(w, r, "a log upload whose end isn't true or false")
			return
		}
	}
	if key.Part != workerapi.LogSetup {
		step, errStep := strconv.Atoi(q.Get(workerapi.QueryStep))
		try, errTry := strconv.Atoi(q.Get(workerapi.QueryTry))
		if errStep != nil || errTry != nil || step < 0 || try < 0 {
			s.badRequest(w, r, "a log upload whose step or try isn't one")
			return
		}
		steps, err := s.readier.Steps(ctx, run, a)
		if err != nil {
			s.writeError(w, r, err)
			return
		}
		if step >= steps {
			s.badRequest(w, r, "a log of a step the job doesn't have")
			return
		}
		key.Step, key.Try = step, try
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, workerapi.MaxLogUpload+1))
	if err != nil {
		writeStatus(w, http.StatusBadRequest)
		return
	}
	if len(data) > workerapi.MaxLogUpload {
		writeStatus(w, http.StatusRequestEntityTooLarge)
		return
	}
	// Logs aren't in the store: the attempt is checked to hold its job
	// first. One superseded between the check and the append can add to
	// its own logs, which are its attempt's alone.
	if err := s.store.View(ctx, func(tx *store.Tx) error {
		_, err := tx.Authorize(bearer(r))
		return err
	}); err != nil {
		s.writeError(w, r, err)
		return
	}
	length, ended, err := s.logs.Append(ctx, key, offset, data, end)
	switch {
	case errors.Is(err, ErrLogGap):
		problem.Write(w, outOfOrder(length))
		return
	case err != nil:
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, &workerapi.LogAck{Length: length, Ended: ended})
}

// resealTries is how many times serveComplete seals the run's record again
// when another change to it raced its own.
const resealTries = 8

// serveComplete stores the attempt's end, once its events are stored: its
// result's private part in the run's secret record, sealed again, and the
// rest with the attempt.
func (s *Server) serveComplete(w http.ResponseWriter, r *http.Request, a *store.Attempt, run *store.Run) {
	ctx := r.Context()
	body, ok := readBody(w, r, MaxCompleteBody)
	if !ok {
		return
	}
	end := new(workerapi.Complete)
	if err := workerapi.Unmarshal(body, end); err != nil {
		s.badRequest(w, r, "an end that isn't one")
		return
	}
	// The end as the API writes it, which a repeat must match.
	canonical, err := workerapi.Marshal(end)
	if err != nil {
		writeStatus(w, http.StatusBadRequest)
		return
	}
	sum := sha256.Sum256(canonical)
	if !a.Holds {
		s.repeatedEnd(w, a, sum[:])
		return
	}
	result := end.Result
	switch {
	case result.NeedsFailed || result.Cancelled:
		s.badRequest(w, r, "an end that says what only the coordinator says")
		return
	case end.Problem != nil && (result.Outcome != plan.OutcomeFailure || end.Reason != plan.ReasonSetupFailed):
		s.badRequest(w, r, "an end with a problem, but not one before its first step")
		return
	}
	if ok := s.checkSeq(w, r, a, end.Seq); !ok {
		return
	}
	private, state := result.Split()
	report := *end
	report.Result = state
	reportBody, err := workerapi.Marshal(&report)
	if err != nil {
		writeStatus(w, http.StatusInternalServerError)
		return
	}
	stored := store.AttemptEnd{
		Kind: store.EndCompleted, Outcome: result.Outcome, Reason: end.Reason, CompleteDigest: sum[:], Report: reportBody, Problem: end.Problem,
	}
	hasPrivate := len(private.Outputs) > 0 || len(private.Steps) > 0
	for try := 0; ; try++ {
		var resealed *sealedRecord
		if hasPrivate {
			if resealed, err = s.withJob(ctx, run.ID, a.Job, private); err != nil {
				s.writeError(w, r, err)
				return
			}
		}
		err = s.store.Tx(ctx, func(tx *store.Tx) error {
			if _, err := tx.Authorize(bearer(r)); err != nil {
				return err
			}
			if last, err := tx.LastEvent(a.ID); err != nil {
				return err
			} else if last != end.Seq {
				return errSeqChanged
			}
			if resealed != nil {
				if err := tx.Reseal(run.ID, resealed.gen, resealed.sealed); err != nil {
					return err
				}
			}
			return tx.EndAttempt(a.ID, stored)
		})
		if errors.Is(err, store.ErrState) && resealed != nil && try < resealTries {
			// Another change sealed the record again meanwhile.
			continue
		}
		break
	}
	switch {
	case errors.Is(err, errSeqChanged):
		// Events arrived in between, as from another connection of the
		// same worker: what it has now is checked again, and if it would
		// do, the worker sends the end again.
		if s.checkSeq(w, r, a, end.Seq) {
			writeStatus(w, http.StatusServiceUnavailable)
		}
		return
	case errors.Is(err, store.ErrFenced):
		// The attempt ended meanwhile: as this end, or otherwise.
		var now *store.Attempt
		if err := s.store.View(ctx, func(tx *store.Tx) error {
			var err error
			now, err = tx.Attempt(a.ID)
			return err
		}); err != nil {
			s.writeError(w, r, err)
			return
		}
		s.repeatedEnd(w, now, sum[:])
		return
	case err != nil:
		s.writeError(w, r, err)
		return
	}
	s.wakeJob(ctx, a.Run, a.Job)
	w.WriteHeader(http.StatusNoContent)
}

// errSeqChanged is the attempt's last event changing between
// serveComplete's check and its store.
var errSeqChanged = errors.New("workerapisrv: the attempt's events changed")

// checkSeq checks an end's Seq against the attempt's last event stored,
// once it has answered one that isn't it: OutOfOrder for one past it,
// whose events the coordinator doesn't have, and 400 for one behind it.
func (s *Server) checkSeq(w http.ResponseWriter, r *http.Request, a *store.Attempt, seq int64) bool {
	var last int64
	if err := s.store.View(r.Context(), func(tx *store.Tx) error {
		var err error
		last, err = tx.LastEvent(a.ID)
		return err
	}); err != nil {
		s.writeError(w, r, err)
		return false
	}
	switch {
	case seq > last:
		problem.Write(w, outOfOrder(last+1))
		return false
	case seq < last:
		s.badRequest(w, r, "an end behind the events stored")
		return false
	}
	return true
}

// repeatedEnd answers an end of the attempt a, which no longer holds its
// job, whose report's digest is sum: as the first time, for the end that
// completed it, and Fenced for any other.
func (s *Server) repeatedEnd(w http.ResponseWriter, a *store.Attempt, sum []byte) {
	if a.End.Kind == store.EndCompleted && bytes.Equal(a.End.CompleteDigest, sum) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	problem.Write(w, fenced())
}

// A sealedRecord is a run's secret record sealed again, with the next
// generation, to store in place of the one it was made from.
type sealedRecord struct {
	gen    uint64
	sealed []byte
}

// withJob returns the run's secret record sealed again with job's private
// result, rec, which End gave: its private outputs, and its steps' that
// hold a secret (plan.JobResult.Split). The record is read, opened and
// sealed outside the transaction that stores it, which Keys may take a
// while for, and stored only if no other change sealed it again meanwhile
// (store.Tx.Reseal).
func (s *Server) withJob(ctx context.Context, run, job string, rec plan.JobRecord) (*sealedRecord, error) {
	var stored *store.Record
	if err := s.store.View(ctx, func(tx *store.Tx) error {
		var err error
		stored, err = tx.Record(run)
		return err
	}); err != nil {
		return nil, err
	}
	sealed := seal.Sealed{Key: stored.WrappedKey, Record: stored.Sealed}
	record, err := s.sealer.Open(ctx, run, stored.Generation, sealed)
	if err != nil {
		return nil, err
	}
	if record.Jobs == nil {
		record.Jobs = map[string]plan.JobRecord{}
	}
	record.Jobs[job] = rec
	resealed, gen, err := s.sealer.Reseal(ctx, run, stored.Generation, sealed, record)
	if err != nil {
		return nil, err
	}
	return &sealedRecord{gen: gen, sealed: resealed.Record}, nil
}
