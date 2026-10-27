package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend/hostproto"
	"github.com/parallelworks/meridian/problems"
)

// decideDetach decides how meridian host starts a worker so that it
// outlives the session that placed it, from what logind says through
// loginctl, and returns the mode, with the systemd-run that starts the
// worker for hostproto.DetachScope:
//
//   - Where logind doesn't run, as on macOS, FreeBSD or in a container
//     (sys.logind), or there is no loginctl to ask it, or it doesn't kill
//     a user's processes as their session ends (KillUserProcesses=no, the
//     default of most systems): in a session of its own
//     (hostproto.DetachSession), as the local backend starts one, which
//     keeps the session's environment.
//   - Where it does, and the user's service manager keeps running after
//     their last session (Linger=yes): in a scope of that manager's
//     (hostproto.DetachScope), outside the session, which keeps the
//     session's environment too, as a service of the manager wouldn't.
//   - Where it does, and the user doesn't linger, or there is no
//     systemd-run: nowhere, ssh_worker_would_be_killed, rather than start
//     a worker that the end of the session would kill.
//
// It reads logind's setting, not whether the session meridian host runs
// in is one of logind's: where sshd doesn't register its sessions with
// logind (UsePAM no), logind ends none of their processes, but meridian
// host decides as if it did, which costs a scope, or a refusal, that the
// host didn't need. A loginctl that fails otherwise than by not being
// there, as one that times out, or can't reach logind over D-Bus, is an
// error, for the coordinator to try again: what logind would do isn't
// known.
func decideDetach(ctx context.Context, sys hostSystem) (mode, systemdRun string, err error) {
	if !sys.logind() {
		return hostproto.DetachSession, "", nil
	}
	// With no session named, loginctl shows the manager's properties.
	out, err := sys.loginctl(ctx, "show-session", "-p", "KillUserProcesses")
	switch {
	case errors.Is(err, exec.ErrNotFound):
		return hostproto.DetachSession, "", nil
	case err != nil:
		return "", "", loginctlFailed(err)
	case property(out, "KillUserProcesses") != "yes":
		return hostproto.DetachSession, "", nil
	}
	// A user who neither lingers nor has a session logind knows of is
	// one loginctl fails for: they don't linger.
	out, err = sys.loginctl(ctx, "show-user", strconv.Itoa(os.Getuid()), "-p", "Linger")
	if _, ok := errors.AsType[*problem.Problem](err); ok {
		return "", "", err
	}
	if err == nil && property(out, "Linger") == "yes" {
		path, err := sys.command("systemd-run")
		if err == nil {
			return hostproto.DetachScope, path, nil
		}
		if !errors.Is(err, exec.ErrNotFound) {
			return "", "", err
		}
	}
	return "", "", problems.SSHWorkerWouldBeKilled.Problem(problems.Params{"host": hostName()})
}

// loginctlFailed is the error of a loginctl that failed with err: a
// problem as it is, and otherwise err, with what loginctl wrote on its
// standard error.
func loginctlFailed(err error) error {
	if _, ok := errors.AsType[*problem.Problem](err); ok {
		return err
	}
	if exit, ok := errors.AsType[*exec.ExitError](err); ok && len(exit.Stderr) > 0 {
		return fmt.Errorf("host: loginctl: %w: %s", err, strings.TrimSpace(string(exit.Stderr)))
	}
	return fmt.Errorf("host: loginctl: %w", err)
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
