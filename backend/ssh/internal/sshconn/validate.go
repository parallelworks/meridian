package sshconn

import (
	"strings"

	"github.com/parallelworks/meridian/problems"
)

// check refuses a target whose host or user could be read as more than a
// name, before anything runs with it: ssh -G, ssh-keygen, or a
// ProxyCommand, in which %h, %n and %r stand for them, unquoted, in a
// shell. Its values come from a workflow's inputs. OpenSSH 9.6 and later
// refuses the same on its command line, but an older ssh doesn't, so the
// check is Meridian's own, as OpenSSH's valid_hostname and valid_ruser
// make it, whichever ssh is installed. A value it refuses is
// problems.SSHTargetUnsafe, of format 1's key for it.
func (t Target) check() error {
	for _, v := range []struct {
		key, value string
		valid      func(string) bool
	}{
		{"remoteHost", t.Host, validHostname},
		{"remoteUser", t.User, validUser},
		{"jumpNodeHost", t.JumpHost, validHostname},
		{"jumpNodeUser", t.JumpUser, validUser},
	} {
		if !v.valid(v.value) {
			return problems.SSHTargetUnsafe.Problem(problems.Params{"key": v.key, "value": v.value})
		}
	}
	return nil
}

// isSpace and isControl are C's isspace and iscntrl, in the C locale,
// for a byte: OpenSSH checks bytes, not characters.
func isSpace(b byte) bool   { return b == ' ' || b >= '\t' && b <= '\r' }
func isControl(b byte) bool { return b < 0x20 || b == 0x7f }

// validHostname reports whether a host is safe to give ssh, as OpenSSH's
// valid_hostname says: it doesn't start with -, and holds no white space,
// control character, or any of '`"$\;&<>|(){}.
func validHostname(s string) bool {
	if strings.HasPrefix(s, "-") {
		return false
	}
	for i := range len(s) {
		if strings.IndexByte("'`\"$\\;&<>|(){}", s[i]) >= 0 || isSpace(s[i]) || isControl(s[i]) {
			return false
		}
	}
	return true
}

// validUser reports whether a user is safe to give ssh, as OpenSSH's
// valid_ruser says: "", or one that doesn't start with -, holds none of
// '`";&<>|(){}, has no - after white space, and no \ last.
func validUser(s string) bool {
	if s == "" {
		return true
	}
	if strings.HasPrefix(s, "-") {
		return false
	}
	for i := range len(s) {
		switch {
		case strings.IndexByte("'`\";&<>|(){}", s[i]) >= 0:
			return false
		case isSpace(s[i]) && i+1 < len(s) && s[i+1] == '-':
			return false
		case s[i] == '\\' && i+1 == len(s):
			return false
		}
	}
	return true
}
