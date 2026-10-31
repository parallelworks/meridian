package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"log/slog"
	"strconv"
	"sync"
	"time"
)

// A walEraser erases from the store's write-ahead log the secret records
// its transactions delete (eraseWAL), for the Store it is part of, whose
// database db is, and which logs to log.
//
// pending is set while the write-ahead log may hold a secret record a
// transaction deleted, which a reader or a writer kept a truncation from
// erasing (eraseWAL); erasing while one goroutine, which erasers counts,
// tries it again, until it succeeds, or the store closes: Close cancels
// closing, which ends the goroutine and its try, waits for it, and tries
// once more, if one is pending, which lastTried then says (eraseLast).
// heldUp counts the tries held up, so that a try that succeeds clears
// pending only if none was held up as it ran, which may have been of a
// record deleted after the log it truncated. warnAfter is how long the
// goroutine tries before it warns, once (eraseWarnAfter, which a test
// shortens). erased, if a test sets it, is called as the goroutine ends;
// truncated, as a try has truncated the log, before heldUp is read; and
// checkpointing with the connection of a try that waits for no one, as
// its checkpoint is about to run (truncateWALNow).
type walEraser struct {
	db  *sql.DB
	log *slog.Logger

	warnAfter     time.Duration
	walMu         sync.Mutex
	pending       bool
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

// initEraser readies e for the store whose database db is, which logs to
// log, until its close, which ctx, without its cancel, outlives.
func (e *walEraser) initEraser(ctx context.Context, db *sql.DB, log *slog.Logger) {
	e.db, e.log = db, log
	e.closing, e.close = context.WithCancel(context.WithoutCancel(ctx))
	e.warnAfter = eraseWarnAfter
}

// stopErasing ends the goroutine that tries a truncation again, and its
// try, and waits for it. closing is cancelled under walMu, which a new
// goroutine starts under, so that none starts after the wait.
func (e *walEraser) stopErasing() {
	e.walMu.Lock()
	e.close()
	e.walMu.Unlock()
	e.erasers.Wait()
}

// eraseLast tries once more, as the store closes, a truncation of the
// write-ahead log that a reader or a writer held up, once the goroutine
// that tries it again has ended (stopErasing), and warns if it is held up
// too; one held up after this one warns itself (eraseWAL).
func (e *walEraser) eraseLast() {
	e.stopErasing()
	e.walMu.Lock()
	pending := e.pending
	e.lastTried = true
	e.walMu.Unlock()
	if pending && !e.truncateWAL(context.WithoutCancel(e.closing)) {
		e.warnPending()
	}
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
// on itself alone.
func (e *walEraser) eraseWAL(ctx context.Context) {
	e.walMu.Lock()
	heldUp := e.heldUp
	e.walMu.Unlock()
	if e.truncateWAL(ctx) {
		if e.truncated != nil {
			e.truncated()
		}
		e.walMu.Lock()
		if e.heldUp == heldUp {
			e.pending = false
		}
		e.walMu.Unlock()
		return
	}
	e.walMu.Lock()
	defer e.walMu.Unlock()
	e.heldUp++
	switch {
	case e.lastTried:
		// The store has closed, past its last try.
		e.warnPending()
		return
	case e.closing.Err() != nil:
		// Close's last try, which hasn't been made, covers it.
		e.pending = true
		return
	}
	e.pending = true
	if !e.erasing {
		e.erasing = true
		e.erasers.Go(e.eraseLater)
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
func (e *walEraser) eraseLater() {
	if e.erased != nil {
		defer e.erased()
	}
	began, warned := time.Now(), false
	for wait := eraseRetryFirst; ; wait = min(2*wait, eraseRetryMost) {
		t := time.NewTimer(wait)
		select {
		case <-e.closing.Done():
			t.Stop()
			e.walMu.Lock()
			e.erasing = false
			e.walMu.Unlock()
			return
		case <-t.C:
		}
		e.walMu.Lock()
		heldUp := e.heldUp
		e.walMu.Unlock()
		if e.truncateWALNow(e.closing) {
			if e.truncated != nil {
				e.truncated()
			}
			e.walMu.Lock()
			if e.heldUp == heldUp {
				e.erasing, e.pending = false, false
				e.walMu.Unlock()
				return
			}
			// A try held up as this one ran may be of a record deleted
			// after the log it truncated: tried again, from the first wait.
			e.walMu.Unlock()
			wait = eraseRetryFirst / 2
			continue
		}
		e.walMu.Lock()
		e.heldUp++
		e.walMu.Unlock()
		if !warned && time.Since(began) >= e.warnAfter {
			warned = true
			e.log.Warn("store: a reader or a writer has kept the write-ahead log from being truncated for a while, so it may still hold a secret record deleted since; it is tried again", "for", time.Since(began).Round(time.Second))
		}
	}
}

// warnPending warns that the log may hold a secret record past the
// store's close.
func (e *walEraser) warnPending() {
	e.log.Warn("store: a reader or a writer kept the write-ahead log from being truncated as the store closed, so it may still hold a secret record deleted since; the next Open truncates it")
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
func (e *walEraser) truncateWAL(ctx context.Context) bool {
	return e.checkpoint(ctx, e.db)
}

// truncateWALNow is truncateWAL without waiting for a reader or a writer:
// on a connection of its own whose busy timeout is 0 while it tries, so
// that a checkpoint held up fails at once, rather than hold back writers,
// as a truncation that waits for readers does, for the busy timeout. A
// connection whose busy timeout it can't set back isn't used again.
func (e *walEraser) truncateWALNow(ctx context.Context) bool {
	conn, err := e.db.Conn(ctx)
	if err != nil {
		if ctx.Err() == nil {
			e.log.WarnContext(ctx, "store: checkpoint the write-ahead log", "error", err)
		}
		return false
	}
	defer conn.Close()
	var timeout int64
	if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&timeout); err != nil {
		if ctx.Err() == nil {
			e.log.WarnContext(ctx, "store: checkpoint the write-ahead log", "error", err)
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
			e.log.WarnContext(ctx, "store: checkpoint the write-ahead log", "error", err)
		}
		return false
	}
	if e.checkpointing != nil {
		e.checkpointing(conn)
	}
	return e.checkpoint(ctx, conn)
}

// checkpoint runs a truncating checkpoint of the write-ahead log on q, as
// truncateWAL says.
func (e *walEraser) checkpoint(ctx context.Context, q queryer) bool {
	var busy, logPages, checkpointed int64
	err := q.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logPages, &checkpointed)
	switch {
	case err != nil && ctx.Err() != nil:
		// The store is closing.
		return false
	case err != nil:
		e.log.WarnContext(ctx, "store: checkpoint the write-ahead log", "error", err)
		return false
	case busy != 0:
		e.log.DebugContext(ctx, "store: a reader or a writer kept the write-ahead log from being checkpointed and truncated, which is tried again", "pages", logPages, "checkpointed", checkpointed)
		return false
	}
	return true
}
