package sshconn

import (
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/internal/sshname"
	"github.com/parallelworks/meridian/problems"
)

// check refuses a target whose host or user could be read as more than a
// name, before anything runs with it: ssh -G, ssh-keygen, or a
// ProxyCommand, in which %h, %n and %r stand for them, unquoted, in a
// shell. Its values come from a workflow's inputs. OpenSSH 10.1 and later
// refuses the same on its command line, but an older ssh doesn't, or not
// all of it: before 10.1, a user's control characters, as a newline, which
// starts a line of ssh -G's output of its own (CVE-2025-61984). So the
// check is Meridian's own, as OpenSSH 10.1's valid_hostname and
// valid_ruser make it, whichever ssh is installed (sshname). A value it refuses is
// problems.SSHTargetUnsafe, of format 1's key for it, with the value
// escaped (escape).
//
// It then refuses a host in a form Dial doesn't take
// (problems.SSHHostFormUnsupported): one that names a user, or is a URL,
// or a list of hosts (hostForm).
func (t Target) check() error {
	for _, v := range []struct {
		key, value string
		valid      func(string) bool
	}{
		{"remoteHost", t.Host, sshname.ValidHost},
		{"remoteUser", t.User, sshname.ValidUser},
		{"jumpNodeHost", t.JumpHost, sshname.ValidHost},
		{"jumpNodeUser", t.JumpUser, sshname.ValidUser},
	} {
		if !v.valid(v.value) {
			return problems.SSHTargetUnsafe.Problem(problems.Params{"key": v.key, "value": escape(v.value)})
		}
	}
	for _, v := range []struct{ key, value string }{{"remoteHost", t.Host}, {"jumpNodeHost", t.JumpHost}} {
		if !hostForm(v.value) {
			return problems.SSHHostFormUnsupported.Problem(problems.Params{"key": v.key, "value": escape(v.value)})
		}
	}
	return nil
}

// hostForm reports whether a host is in a form Dial takes: a name or an
// address, or a host and its port, host:port. ssh's command line takes
// more, which a format 1 ssh: doesn't mean, and which Dial would get wrong:
// a user in the host, user@host, which ssh -G takes out, so that it prints
// another host than it was asked for; a URL, ssh://user@host:port; and,
// for a jump host, a list of hosts, a,b, which ssh -J connects through one
// after another, but Dial would dial as one host's name.
func hostForm(s string) bool {
	return !strings.ContainsAny(s, "@,") && !strings.Contains(s, "://")
}

// escape writes s as a Go string literal does, without its quotes: a
// control character, as an escape sequence's ESC, as \x1b, so that none
// reaches a terminal from a message.
func escape(s string) string {
	q := strconv.Quote(s)
	return q[1 : len(q)-1]
}
