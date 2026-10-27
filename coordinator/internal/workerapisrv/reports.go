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
// answers with the sequence number of the last it has. Of an attempt whose
// placement is private, the diagnostics of each event, which can name the
// member, are masked before they are stored, as its end's are. Storing
// them can change other attempts' controls, as a step that failed changes
// its run's other attempts', whose polls the store's
// Options.OnControlsChanged answers once they are stored (Notify).
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
	if a.Private {
		for i := range events.Events {
			events.Events[i].Diagnostics = problems.MaskDiagnostics(events.Events[i].Diagnostics)
		}
	}
	// A step's first start makes the attempt running, which its job's
	// supervisor counts.
	started := a.State == store.AttemptClaimed && slices.ContainsFunc(events.Events, func(e workerapi.Event) bool { return !e.Finished })
	var (
		last  int64
		after func()
	)
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
		if last, err = tx.AppendEvents(a.ID, events.Events); err != nil || !started {
			return err
		}
		after, err = s.wakeJob(ctx, tx, a.Run, a.Job)
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
	committed(after)
	writeJSON(w, &workerapi.EventsAck{Seq: last})
}

// errTooMuch is an attempt's reports past what the server keeps of them.
var errTooMuch = errors.New("workerapisrv: more of an attempt's reports than the server keeps")

// logParts are the logs an attempt has.
var logParts = []workerapi.LogPart{workerapi.LogSetup, workerapi.LogStep, workerapi.LogCleanup}

// serveLogs stores part of one of the attempt's logs, and answers with the
// log's length and whether it has ended. The attempt's logs keep their
// bytes to the server's limit on them all (appendLog), and those of a
// step's try past MaxLogTry keep none; the worker is told the rest is
// stored, so that it goes on.
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
	if key.Try > MaxLogTry {
		writeJSON(w, &workerapi.LogAck{Length: offset + int64(len(data)), Ended: end})
		return
	}
	length, ended, err := s.appendLog(ctx, a.ID, key, offset, data, end)
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

// appendLog appends data, the part of the log key of the attempt that
// starts at offset, as Logs.Append does, within the server's limit on the
// bytes of the attempt's logs, all of them together. The bytes an upload
// adds to its log are counted against the limit
// (store.Tx.AddLogBytes) before they are appended. The log that reaches
// the limit keeps its bytes to there, then a line that says the rest wasn't
// kept, and ends; each of the attempt's logs after it keeps only that line;
// and the rest of each is answered as stored. An upload counts the bytes
// past its log's length as it read it, so uploads of the same bytes at once
// each count them: the count errs only high, and the attempt's logs never
// keep more than the limit, beside the line that ends each.
func (s *Server) appendLog(ctx context.Context, attempt string, key runlog.Key, offset int64, data []byte, end bool) (int64, bool, error) {
	// The log's length: an append of nothing changes nothing.
	length, ended, err := s.logs.Append(ctx, key, 0, nil, false)
	if err != nil {
		return length, ended, err
	}
	var counted int64
	if err := s.store.View(ctx, func(tx *store.Tx) error {
		counted, err = tx.LogBytes(attempt)
		return err
	}); err != nil {
		return 0, false, err
	}
	if counted >= s.maxLogBytes {
		return s.truncateLog(ctx, key, length, ended, offset+int64(len(data)), end)
	}
	fresh := offset + int64(len(data)) - length
	if offset > length || ended || fresh <= 0 {
		// Past the log's end, after it, or nothing the log hasn't: as
		// Logs.Append answers it.
		return s.logs.Append(ctx, key, offset, data, end)
	}
	if err := s.store.Tx(ctx, func(tx *store.Tx) error {
		counted, err = tx.AddLogBytes(attempt, fresh)
		return err
	}); err != nil {
		return 0, false, err
	}
	keep := min(fresh, max(0, s.maxLogBytes-(counted-fresh)))
	if keep == fresh {
		return s.logs.Append(ctx, key, offset, data, end)
	}
	if keep > 0 {
		if length, ended, err = s.logs.Append(ctx, key, length, data[length-offset:][:keep], false); err != nil {
			return length, ended, err
		}
	}
	return s.truncateLog(ctx, key, length, false, offset+int64(len(data)), end)
}

// truncateLog ends the log key, whose length is length, and which has ended
// if ended, once the attempt's logs have reached the server's limit on
// them: with a line that says the rest of it wasn't kept, unless it has
// ended. It answers the worker's upload, which ends at upTo, and ends the
// log if end, as stored.
func (s *Server) truncateLog(ctx context.Context, key runlog.Key, length int64, ended bool, upTo int64, end bool) (int64, bool, error) {
	if !ended {
		line := problems.LogTruncated.Message(problems.Params{"limit": s.maxLogBytes}) + "\n"
		if length > 0 {
			line = "\n" + line
		}
		if length, ended, err := s.logs.Append(ctx, key, length, []byte(line), true); err != nil {
			return length, ended, err
		}
	}
	return upTo, end, nil
}

// partTries is how many times serveComplete stores an end whose job's part
// of the run's secret record another end stored as it sealed it, as an end
// of the job's attempt that raced another's can.
const partTries = 8

// serveComplete stores the attempt's end, once its events are stored: its
// result's private part as its job's part of the run's secret record,
// sealed (seal.Sealer.SealJob) before the transaction that stores it,
// and the rest with the attempt, in that one transaction. Of an attempt
// whose placement is private (store.Attempt.Private), the problem and the
// diagnostics it reports, which can name the member, are masked before
// anything of it is stored, as the store masks the problem it keeps.
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
	if a.Private {
		end.Problem = problems.MaskProblem(end.Problem)
		end.Diagnostics = problems.MaskDiagnostics(end.Diagnostics)
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
	// An end that fails the job, rather than one before its first step,
	// which is placed again, or one for a reason the job's retry: lists,
	// which another attempt may follow, marks the run as one a job of
	// which failed, for its other attempts' steps whose early-cancel: is
	// any-job-failed, as format 1 tells them as the job fails. The job
	// marks it as it ends otherwise.
	jobFailed := result.Outcome == plan.OutcomeFailure && end.Reason != plan.ReasonSetupFailed
	var after func()
	for try := 1; ; try++ {
		// The part is sealed outside the transaction that stores it, which
		// holds the database's lock only for its writes, and stores it only
		// at the generation it was sealed with.
		var (
			gen  uint64
			part []byte
		)
		if hasPrivate {
			if gen, part, err = s.sealPart(ctx, run.ID, a.Job, private); err != nil {
				break
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
			if hasPrivate {
				if err := tx.PutJobRecord(run.ID, a.Job, gen, part); err != nil {
					return err
				}
			}
			if err := tx.EndAttempt(a.ID, stored); err != nil {
				return err
			}
			if jobFailed && !tx.RetriesFor(a.ID, end.Reason) {
				if _, err := tx.MarkJobFailed(run.ID); err != nil {
					return err
				}
			}
			var err error
			after, err = s.wakeJob(ctx, tx, a.Run, a.Job)
			return err
		})
		if !errors.Is(err, store.ErrRecordChanged) || try == partTries {
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
	committed(after)
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

// sealPart seals rec, job's private result, as End gave it (its private
// outputs, and its steps' that hold a secret: plan.JobResult.Split), as
// job's part of the run's secret record (seal.Sealer.SealJob), under a key
// that the run's data key derives, with the generation after the part's, which it returns:
// what a transaction stores (store.Tx.PutJobRecord) only at that
// generation. Neither the run's record nor another job's part is opened.
func (s *Server) sealPart(ctx context.Context, run, job string, rec plan.JobRecord) (uint64, []byte, error) {
	var (
		key   []byte
		parts map[string]store.JobRecord
	)
	if err := s.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if key, err = tx.WrappedKey(run); err != nil {
			return err
		}
		parts, err = tx.JobRecords(run, []string{job})
		return err
	}); err != nil {
		return 0, nil, err
	}
	gen := parts[job].Generation + 1
	sealed, err := s.sealer.SealJob(ctx, run, job, gen, key, rec)
	return gen, sealed, err
}
