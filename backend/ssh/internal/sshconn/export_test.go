package sshconn

import "context"

// SetDialed has p call f once each of its dials has ended, and what it
// made, or how it failed, is recorded.
func SetDialed(p *Pool, f func()) { p.dialed = f }

// NewSession opens a session on c, as a script does (Conn.Script).
func NewSession(ctx context.Context, c *Conn) (*Session, error) { return c.newSession(ctx) }
