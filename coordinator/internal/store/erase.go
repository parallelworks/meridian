package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strconv"
	"sync"
	"time"
)

// A walEraser is the state of a Store's erasure of the secret records its
// transactions delete from the write-ahead log (Store.eraseWAL), which the
// Store embeds; its methods are the Store's, which reads its database and
// logs to its logger as everything else of it does.
//
// pending is set while the write-ahead log may hold a secret record a
// transaction deleted, which a reader or a writer kept a truncation from
// erasing (eraseWAL), and erasedHere while one of them is a record a
// deletion counted in the store (walEraseMarked) that no finished
// truncation covered, this process's own, or one a process that ended
// before it could truncate the log left, which the store warns of, and only
// then: another process, as the meridian run that a meridian session url
// runs beside, keeps truncations from finishing all the time, of a log that
// Open truncates for a process before it whatever it holds, so that a short
// command that deleted nothing would warn each time; erasing while one
// goroutine, which erasers counts, tries it again, until it succeeds, or
// the store closes: Close cancels closing, which ends the goroutine and its
// try, waits for it, and tries once more, if one is pending, which
// lastTried then says (eraseLast). heldUp counts the tries held up, so that
// a try that succeeds clears pending only if none was held up as it ran,
// which may have been of a record deleted after the log it truncated.
// warnAfter is how long the goroutine tries before it warns, once, where
// erasedHere says the log may hold such a record (eraseWarnAfter, which a
// test shortens, under walMu once the goroutine runs). erased, if a test
// sets it, is called as the goroutine ends; truncated, as a try has
// truncated the log, before heldUp is read; and checkpointing with the
// connection of a try that waits for no one, as its checkpoint is about to
// run (truncateWALNow).
type walEraser struct {
	warnAfter     time.Duration
	walMu         sync.Mutex
	pending       bool
	erasedHere    bool
	heldUp        uint64
	truncated     func()
	checkpointing func(*sql.Conn)
	erasing       bool
	lastTried     bool
	closing       context.Context //nolint:containedctx // the store's own, which Close cancels
	close         context.CancelFunc
	erasers       sync.WaitGroup
	erased        func()
}

// initEraser readies the store's erasure of its write-ahead log until the
// store closes, which ctx, without its cancel, outlives.
func (s *Store) initEraser(ctx context.Context) {
	s.closing, s.close = context.WithCancel(context.WithoutCancel(ctx))
	s.warnAfter = eraseWarnAfter
}

// stopErasing ends the goroutine that tries a truncation again, and its
// try, and waits for it. closing is cancelled under walMu, which a new
// goroutine starts under, so that none starts after the wait.
func (s *Store) stopErasing() {
	s.walMu.Lock()
	s.close()
	s.walMu.Unlock()
	s.erasers.Wait()
}

// eraseLast tries once more, as the store closes, a truncation of the
// write-ahead log that a reader or a writer held up, once the goroutine
// that tries it again has ended (stopErasing), and warns if it is held up
// too; one held up after this one warns itself (eraseWAL).
func (s *Store) eraseLast() {
	s.stopErasing()
	s.walMu.Lock()
	pending, here := s.pending, s.erasedHere
	s.lastTried = true
	s.walMu.Unlock()
	if pending && !s.truncateMarked(context.WithoutCancel(s.closing), s.truncateWAL) && here {
		s.warnPending()
	}
}

// The keys of meridian_store that say whether the write-ahead log may
// hold a secret record a process deleted: walEraseMarked counts the
// transactions that deleted one, or digests (eraseDigests), and
// walEraseDone is the count a truncation read before it began, once it
// finished, which covered every deletion counted by then. Each only ever
// rises, so that a deletion counted as another truncation finishes is
// never taken for one it covered; the log may hold one while marked is
// past done. Neither holds anything of what was deleted. They outlive a
// process that ended before it could truncate the log, so that one whose
// Open's truncation is held up warns of that record too.
const (
	walEraseMarked = "wal_erase_marked"
	walEraseDone   = "wal_erase_done"
)

// markErased marks the transaction as one that deleted a secret record,
// which the write-ahead log is truncated of once it commits (erased), and
// in the store, until a truncation finishes (markErasePending).
func (tx *Tx) markErased() error {
	tx.erased = true
	return tx.markErasePending()
}

// markErasePending counts a deletion of a secret record (walEraseMarked),
// in the transaction that deletes it.
func (tx *Tx) markErasePending() error {
	_, err := tx.exec(`INSERT INTO meridian_store (key, value) VALUES ('` + walEraseMarked + `', '1')
ON CONFLICT (key) DO UPDATE SET value = CAST(CAST(meridian_store.value AS INTEGER) + 1 AS TEXT)`)
	return err
}

// eraseCounts returns the deletions counted (walEraseMarked) and those a
// finished truncation covered (walEraseDone), 0 for none.
func (tx *Tx) eraseCounts() (marked, done int64, err error) {
	err = tx.queryRow(`SELECT
  coalesce((SELECT CAST(value AS INTEGER) FROM meridian_store WHERE key = '`+walEraseMarked+`'), 0),
  coalesce((SELECT CAST(value AS INTEGER) FROM meridian_store WHERE key = '`+walEraseDone+`'), 0)`).Scan(&marked, &done)
	return marked, done, err
}

// erasePending reports whether the write-ahead log may hold a secret
// record a process deleted: one counted that no finished truncation
// covered.
func (tx *Tx) erasePending() (bool, error) {
	marked, done, err := tx.eraseCounts()
	return marked > done, err
}

// erasedMark returns the deletions counted (walEraseMarked) as a
// truncation reads them before it begins, which it covers once it
// finishes (eraseCovered); or 0 where none is past those covered.
func (s *Store) erasedMark(ctx context.Context) int64 {
	var marked, done int64
	err := s.View(ctx, func(tx *Tx) error {
		var err error
		marked, done, err = tx.eraseCounts()
		return err
	})
	if err != nil {
		s.log.DebugContext(ctx, "store: read whether the write-ahead log may hold a secret record", "error", err)
		return 0
	}
	if marked <= done {
		return 0
	}
	return marked
}

// eraseCovered raises the deletions a finished truncation covered
// (walEraseDone) to marked, as it read them before it began, and never
// lowers them; and reports whether it raised them.
func (s *Store) eraseCovered(ctx context.Context, marked int64) bool {
	var raised bool
	if err := s.Tx(ctx, func(tx *Tx) error {
		res, err := tx.exec(`INSERT INTO meridian_store (key, value) VALUES ('`+walEraseDone+`', ?)
ON CONFLICT (key) DO UPDATE SET value = excluded.value WHERE CAST(meridian_store.value AS INTEGER) < CAST(excluded.value AS INTEGER)`,
			strconv.FormatInt(marked, 10))
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		raised = n > 0
		return err
	}); err != nil {
		s.log.DebugContext(ctx, "store: count the secret records a truncation of the write-ahead log covered", "error", err)
	}
	return raised
}

// truncateMarked truncates the write-ahead log with truncate, and reports
// whether it did; once it did, it counts the deletions it read before it
// began as covered (eraseCovered), and truncates the log of that write
// too, which holds nothing secret, if it can.
func (s *Store) truncateMarked(ctx context.Context, truncate func(context.Context) bool) bool {
	marked := s.erasedMark(ctx)
	if !truncate(ctx) {
		return false
	}
	if marked > 0 && s.eraseCovered(ctx, marked) {
		truncate(ctx)
	}
	return true
}

// eraseWAL truncates the write-ahead log once a transaction has deleted a
// secret record (Tx.erased), so that the record, which secure_delete
// overwrote with zeros in the pages the checkpoint writes, is in neither
// the database nor the log: at once, and, while a reader or a writer
// keeps the checkpoint from finishing, as a follower's transaction can for
// longer than the busy timeout the checkpoint waits, again in the
// background, by one goroutine for the store, which the ends of runs
// meanwhile start no other of, from eraseRetryFirst apart, doubling up to
// eraseRetryMost, until it is truncated, or the store closes, which tries
// once more (Close). Open erases the log so too, for a process that ended
// first. hopper's maintenance truncates the log only once it is past
// 8 MiB, which a log that holds a record needn't be, so the store counts
// on itself alone. here says whether this process's transaction deleted
// the record, which the store warns of where it can't truncate the log
// before it closes, or for a while (erasedHere).
func (s *Store) eraseWAL(ctx context.Context, here bool) {
	s.walMu.Lock()
	heldUp := s.heldUp
	s.walMu.Unlock()
	if s.truncateMarked(ctx, s.truncateWAL) {
		if s.truncated != nil {
			s.truncated()
		}
		s.walMu.Lock()
		if s.heldUp == heldUp {
			s.pending, s.erasedHere = false, false
		}
		s.walMu.Unlock()
		return
	}
	s.walMu.Lock()
	defer s.walMu.Unlock()
	s.heldUp++
	if here {
		s.erasedHere = true
	}
	switch {
	case s.lastTried:
		// The store has closed, past its last try.
		if here {
			s.warnPending()
		}
		return
	case s.closing.Err() != nil:
		// Close's last try, which hasn't been made, covers it.
		s.pending = true
		return
	}
	s.pending = true
	if !s.erasing {
		s.erasing = true
		s.erasers.Go(s.eraseLater)
	}
}

// The waits between the tries of a truncation of the write-ahead log that
// a reader or a writer held up (eraseWAL), and how long they go on before
// the store warns, once, that the log may still hold a secret record.
const (
	eraseRetryFirst = 10 * time.Millisecond
	eraseRetryMost  = time.Second
	eraseWarnAfter  = time.Minute
)

// eraseLater tries the truncation of the write-ahead log again until it
// succeeds, or the store closes (eraseWAL), each try without waiting for
// a reader or a writer (truncateWALNow), and warns once if it has tried
// for warnAfter, then says so again only below its warnings.
func (s *Store) eraseLater() {
	if s.erased != nil {
		defer s.erased()
	}
	began, warned := time.Now(), false
	for wait := eraseRetryFirst; ; wait = min(2*wait, eraseRetryMost) {
		t := time.NewTimer(wait)
		select {
		case <-s.closing.Done():
			t.Stop()
			s.walMu.Lock()
			s.erasing = false
			s.walMu.Unlock()
			return
		case <-t.C:
		}
		s.walMu.Lock()
		heldUp := s.heldUp
		s.walMu.Unlock()
		if s.truncateMarked(s.closing, s.truncateWALNow) {
			if s.truncated != nil {
				s.truncated()
			}
			s.walMu.Lock()
			if s.heldUp == heldUp {
				s.erasing, s.pending, s.erasedHere = false, false, false
				s.walMu.Unlock()
				return
			}
			// A try held up as this one ran may be of a record deleted
			// after the log it truncated: tried again, from the first wait.
			s.walMu.Unlock()
			wait = eraseRetryFirst / 2
			continue
		}
		s.walMu.Lock()
		s.heldUp++
		here, warnAfter := s.erasedHere, s.warnAfter
		s.walMu.Unlock()
		if !warned && here && time.Since(began) >= warnAfter {
			warned = true
			s.log.Warn("store: a reader or a writer has kept the write-ahead log from being truncated for a while, so it may still hold a secret record a process deleted; it is tried again", "for", time.Since(began).Round(time.Second))
		}
	}
}

// warnPending warns that the log may hold a secret record past the
// store's close.
func (s *Store) warnPending() {
	s.log.Warn("store: a reader or a writer kept the write-ahead log from being truncated as the store closed, so it may still hold a secret record a process deleted; the next Open truncates it")
}

// A queryer is a database or one of its connections, which a checkpoint
// runs on.
type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// truncateWAL checkpoints the write-ahead log into the database and
// truncates it, and reports whether it did: a reader that still reads
// from the log, or a writer, keeps the checkpoint from finishing once the
// busy timeout has passed, which the store says in its log only below its
// warnings, as it tries again (eraseWAL). An error it warns of.
func (s *Store) truncateWAL(ctx context.Context) bool {
	return s.checkpoint(ctx, s.db)
}

// truncateWALNow is truncateWAL without waiting for a reader or a writer:
// on a connection of its own whose busy timeout is 0 while it tries, so
// that a checkpoint held up fails at once, rather than hold back writers,
// as a truncation that waits for readers does, for the busy timeout. A
// connection whose busy timeout it can't set back isn't used again.
func (s *Store) truncateWALNow(ctx context.Context) bool {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.log.WarnContext(ctx, "store: checkpoint the write-ahead log", "error", err)
		}
		return false
	}
	defer conn.Close()
	var timeout int64
	if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&timeout); err != nil {
		if ctx.Err() == nil {
			s.log.WarnContext(ctx, "store: checkpoint the write-ahead log", "error", err)
		}
		return false
	}
	// Set back however the try goes, the setting to 0 included, which can
	// take effect and still return an error.
	defer func() {
		if _, err := conn.ExecContext(context.WithoutCancel(ctx), "PRAGMA busy_timeout = "+strconv.FormatInt(timeout, 10)); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	if _, err := conn.ExecContext(ctx, "PRAGMA busy_timeout = 0"); err != nil {
		if ctx.Err() == nil {
			s.log.WarnContext(ctx, "store: checkpoint the write-ahead log", "error", err)
		}
		return false
	}
	if s.checkpointing != nil {
		s.checkpointing(conn)
	}
	return s.checkpoint(ctx, conn)
}

// checkpoint runs a truncating checkpoint of the write-ahead log on q, as
// truncateWAL says.
func (s *Store) checkpoint(ctx context.Context, q queryer) bool {
	var busy, logPages, checkpointed int64
	err := q.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logPages, &checkpointed)
	switch {
	case err != nil && ctx.Err() != nil:
		// The store is closing.
		return false
	case err != nil:
		s.log.WarnContext(ctx, "store: checkpoint the write-ahead log", "error", err)
		return false
	case busy != 0:
		s.log.DebugContext(ctx, "store: a reader or a writer kept the write-ahead log from being checkpointed and truncated, which is tried again", "pages", logPages, "checkpointed", checkpointed)
		return false
	}
	return true
}
