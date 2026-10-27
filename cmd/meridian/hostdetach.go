package main

import (
	"context"
	"os"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/backend/hostproto"
	"github.com/parallelworks/meridian/problems"
)

// decideDetach decides how meridian host starts a worker so that it
// outlives the session that placed it, from what logind says through
// loginctl:
//
//   - Where it doesn't kill a user's processes as their session ends
//     (KillUserProcesses=no, the default of most systems), or there is no
//     logind, as on macOS, FreeBSD, in a container, or with sshd's UsePAM
//     no: in a session of its own (hostproto.DetachSession), as the local
//     backend starts one, which keeps the session's environment.
//   - Where it does, and the user's service manager keeps running after
//     their last session (Linger=yes): in a scope of that manager's
//     (hostproto.DetachScope), outside the session, which keeps the
//     session's environment too, as a service of the manager wouldn't.
//   - Where it does, and the user doesn't linger, or there is no
//     systemd-run: nowhere, ssh_worker_would_be_killed, rather than start
//     a worker that the end of the session would kill.
func decideDetach(ctx context.Context, sys hostSystem) (string, error) {
	// With no session named, loginctl shows the manager's properties.
	// A system where loginctl can't say, as one without logind, kills
	// nothing.
	out, err := sys.loginctl(ctx, "show-session", "-p", "KillUserProcesses")
	if err != nil || property(out, "KillUserProcesses") != "yes" {
		return hostproto.DetachSession, nil //nolint:nilerr // no logind, which kills nothing
	}
	out, err = sys.loginctl(ctx, "show-user", strconv.Itoa(os.Getuid()), "-p", "Linger")
	if err == nil && property(out, "Linger") == "yes" && sys.scope() {
		return hostproto.DetachScope, nil
	}
	return "", problems.SSHWorkerWouldBeKilled.Problem(problems.Params{"host": hostName()})
}

// property returns the value of the property name in what loginctl show-*
// wrote, a property a line as name=value, or "" for one it didn't.
func property(out, name string) string {
	for line := range strings.Lines(out) {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), name+"="); ok {
			return value
		}
	}
	return ""
}
