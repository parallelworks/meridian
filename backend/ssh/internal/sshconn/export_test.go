package sshconn

import (
	"context"
	"time"
)

// SetDialed has p call f once each of its dials has ended, and what it
// made, or how it failed, is recorded.
func SetDialed(p *Pool, f func()) { p.dialed = f }

// SetClock has p back off by the clock now.
func SetClock(p *Pool, now func() time.Time) { p.now = now }

// Dialing reports whether p is dialing t.
func Dialing(p *Pool, t Target) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.entries[t.key()]
	return e != nil && e.dialing != nil
}

// NewSession opens a session on c, as a script does (Conn.Script).
func NewSession(ctx context.Context, c *Conn) (*Session, error) { return c.newSession(ctx) }
