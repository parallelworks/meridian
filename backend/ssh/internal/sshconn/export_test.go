package sshconn

// SetDialed has p call f once each of its dials has ended, and what it
// made, or how it failed, is recorded.
func SetDialed(p *Pool, f func()) { p.dialed = f }
