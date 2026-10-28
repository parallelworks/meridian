// Package sshname says whether a host or a user is safe to give ssh, as
// OpenSSH 10.1 and later says of what its own command line is given: a
// name that holds none of what a shell reads as more than a name, since
// ssh puts a host and a user, %h, %n and %r, unquoted, in a ProxyCommand
// or a Match exec, which a shell runs. An older ssh refuses less, or none
// of it, so Meridian checks names itself, as OpenSSH 10.1's valid_hostname
// and valid_ruser do, whichever ssh is installed: a host or a user a
// workflow gives, of an ssh: or of a repository to check out.
package sshname

import "strings"

// isSpace and isControl are C's isspace and iscntrl, in the C locale,
// for a byte: OpenSSH checks bytes, not characters.
func isSpace(b byte) bool   { return b == ' ' || b >= '\t' && b <= '\r' }
func isControl(b byte) bool { return b < 0x20 || b == 0x7f }

// ValidHost reports whether a host is safe to give ssh, as OpenSSH's
// valid_hostname says: it doesn't start with -, and holds no white space,
// control character, or any of '`"$\;&<>|(){}. A port after a colon, and
// an address in brackets, are in it as they are.
func ValidHost(s string) bool {
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

// ValidUser reports whether a user is safe to give ssh, as OpenSSH 10.1's
// valid_ruser says: "", or one that doesn't start with -, holds no control
// character and none of '`";&<>|(){}, has no - after white space, and no \
// last.
func ValidUser(s string) bool {
	if s == "" {
		return true
	}
	if strings.HasPrefix(s, "-") {
		return false
	}
	for i := range len(s) {
		switch {
		case isControl(s[i]) || strings.IndexByte("'`\";&<>|(){}", s[i]) >= 0:
			return false
		case isSpace(s[i]) && i+1 < len(s) && s[i+1] == '-':
			return false
		case s[i] == '\\' && i+1 == len(s):
			return false
		}
	}
	return true
}
