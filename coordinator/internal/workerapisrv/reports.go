package workerapisrv

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/coordinator/internal/runlog"
	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
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
	for {
		// The poll waits before the store is read, so that a control
		// stored after the read wakes it.
		changed, done := s.polls.watch(ctx, a.ID, seen)
		var c workerapi.Control
		err := s.store.View(ctx, func(tx *store.Tx) error {
			var err error
			c, err = tx.Control(a.ID)
			return err
		})
		if err != nil || c.Version > seen {
			done()
			if err != nil {
				s.writeError(w, r, err)
			} else {
				writeJSON(w, &c)
			}
			return
		}
		select {
		case <-changed:
			done()
		case <-hold.C:
			done()
			writeJSON(w, &c)
			return
		case <-ctx.Done():
			done()
			return
		}
	}
}

// serveEvents stores the attempt's events it hasn't yet, in order, and
// answers with the sequence number of the last it has.
func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request, a *store.Attempt) {
	ctx := r.Context()
	body, ok := s.readBody(w, r, MaxEventsBody)
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
		count, size, err := tx.EventsStored(a.ID)
		if err != nil {
			return err
		}
		if count+int64(len(events.Events)) > s.maxEvents || size+int64(len(body)) > s.maxEventsBytes {
			return errTooMuch
		}
		last, err = tx.AppendEvents(a.ID, events.Events)
		return err
	})
	switch {
	case errors.Is(err, errTooMuch):
		writeStatus(w, http.StatusRequestEntityTooLarge)
		return
	case err != nil:
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

// errTooMuch is an attempt's reports past what the server keeps of them.
var errTooMuch = errors.New("workerapisrv: more of an attempt's reports than the server keeps")

// logParts are the logs an attempt has.
var logParts = []workerapi.LogPart{workerapi.LogSetup, workerapi.LogStep, workerapi.LogCleanup}

// serveLogs stores part of one of the attempt's logs, and answers with the
// log's length and whether it has ended. A log keeps its first bytes, to
// the server's limit, then a line that says the rest wasn't kept, and the
// worker is told the rest is stored, so that it goes on.
func (s *Server) serveLogs(w http.ResponseWriter, r *http.Request, a *store.Attempt, run *store.Run) {
	ctx := r.Context()
	q := r.URL.Query()
	key := runlog.Key{Run: a.Run, Job: a.Job, Attempt: a.Number, Part: workerapi.LogPart(q.Get(workerapi.QueryPart))}
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
	data, ok := s.readBody(w, r, workerapi.MaxLogUpload)
	if !ok {
		return
	}
	authorized := func() error {
		return s.store.View(ctx, func(tx *store.Tx) error {
			_, err := tx.Authorize(bearer(r))
			return err
		})
	}
	if err := authorized(); err != nil {
		s.writeError(w, r, err)
		return
	}
	length, ended, err := s.appendLog(ctx, key, offset, data, end)
	switch {
	case errors.Is(err, runlog.ErrGap):
		problem.Write(w, outOfOrder(length))
		return
	case err != nil:
		s.writeError(w, r, err)
		return
	}
	// Logs aren't in the store: an attempt superseded as its upload was
	// appended has added to its own logs, by its own number, which no other
	// attempt's are; it is told it no longer holds its job.
	if err := authorized(); err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, &workerapi.LogAck{Length: length, Ended: ended})
}

// appendLog appends data, the part of the log key that starts at offset,
// as Logs.Append does, but for the part past the server's limit on a log:
// the log keeps its first bytes to the limit, and then a line that says the
// rest wasn't kept, and ends there; the rest is answered as stored.
func (s *Server) appendLog(ctx context.Context, key runlog.Key, offset int64, data []byte, end bool) (int64, bool, error) {
	limit := s.maxLogBytes
	past := offset+int64(len(data)) > limit
	if !past {
		return s.logs.Append(ctx, key, offset, data, end)
	}
	if offset < limit {
		if length, ended, err := s.logs.Append(ctx, key, offset, data[:limit-offset], false); err != nil {
			return length, ended, err
		}
	}
	line := []byte("\n" + problems.LogTruncated.Message(problems.Params{"limit": limit}) + "\n")
	if length, ended, err := s.logs.Append(ctx, key, limit, line, true); err != nil {
		return length, ended, err
	}
	return offset + int64(len(data)), end, nil
}

// resealTries is how many times serveComplete stores an end whose run's
// secret record another store sealed again as it did, as one on another
// database than SQLite, whose transactions run side by side, can.
const resealTries = 8

// serveComplete stores the attempt's end, once its events are stored: its
// result's private part in the run's secret record, sealed again, and the
// rest with the attempt, in one transaction.
func (s *Server) serveComplete(w http.ResponseWriter, r *http.Request, a *store.Attempt, run *store.Run) {
	ctx := r.Context()
	body, ok := s.readBody(w, r, MaxCompleteBody)
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
	if s.beforeEnd != nil {
		s.beforeEnd()
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
	for try := 1; ; try++ {
		err = s.store.Tx(ctx, func(tx *store.Tx) error {
			if _, err := tx.Authorize(bearer(r)); err != nil {
				return err
			}
			if last, err := tx.LastEvent(a.ID); err != nil {
				return err
			} else if last != end.Seq {
				return errSeqChanged
			}
			// The record is opened and sealed again in the transaction
			// that stores it, which holds the database's lock, so that
			// ends of a run's jobs at once each add to the last.
			if hasPrivate {
				if err := s.addJob(ctx, tx, run.ID, a.Job, private); err != nil {
					return err
				}
			}
			return tx.EndAttempt(a.ID, stored)
		})
		if !errors.Is(err, store.ErrRecordChanged) || try == resealTries {
			break
		}
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

// addJob adds job's private result, rec, as End gave it, to the run's
// secret record, in tx: its private outputs, and its steps' that hold a
// secret (plan.JobResult.Split). It opens the record and seals it again
// under the run's data key, which Keys unwraps, with the next generation.
func (s *Server) addJob(ctx context.Context, tx *store.Tx, run, job string, rec plan.JobRecord) error {
	stored, err := tx.Record(run)
	if err != nil {
		return err
	}
	sealed := seal.Sealed{Key: stored.WrappedKey, Record: stored.Sealed}
	record, err := s.sealer.Open(ctx, run, stored.Generation, sealed)
	if err != nil {
		return err
	}
	if record.Jobs == nil {
		record.Jobs = map[string]plan.JobRecord{}
	}
	record.Jobs[job] = rec
	resealed, gen, err := s.sealer.Reseal(ctx, run, stored.Generation, sealed, record)
	if err != nil {
		return err
	}
	return tx.Reseal(run, gen, resealed.Record)
}
