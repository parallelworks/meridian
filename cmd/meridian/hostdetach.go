package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
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
//     (sys.logind), or it doesn't kill a user's processes as their session
//     ends (KillUserProcesses=no, the default of most systems): in a
//     session of its own
//     (hostproto.DetachSession), as the local backend starts one, which
//     keeps the session's environment.
//   - Where it does, and the user's service manager keeps running after
//     their last session (Linger=yes): in a scope of that manager's
//     (hostproto.DetachScope), outside the session, which keeps the
//     session's environment too, as a service of the manager wouldn't.
//   - Where it does, and the user doesn't linger, or there is no
//     systemd-run: nowhere, ssh_worker_would_be_killed, rather than start
//     a worker that the end of the session would kill; and where its
//     systemd-run is older than systemdCollect, which a scope's --collect
//     needs, ssh_systemd_too_old.
//
// Where logind runs, but there is no loginctl to ask it,
// ssh_loginctl_missing: what logind would do isn't known.
//
// It reads logind's setting, not whether the session meridian host runs
// in is one of logind's: where sshd doesn't register its sessions with
// logind (UsePAM no), logind ends none of their processes, but meridian
// host decides as if it did, which costs a scope, or a refusal, that the
// host didn't need. A loginctl that fails otherwise, as one that times
// out, or can't reach logind over D-Bus, is an error, for the coordinator
// to try again: what logind would do isn't known.
func decideDetach(ctx context.Context, sys hostSystem) (mode, systemdRun string, err error) {
	if !sys.logind() {
		return hostproto.DetachSession, "", nil
	}
	// With no session named, loginctl shows the manager's properties.
	out, err := sys.loginctl(ctx, "show-session", "-p", "KillUserProcesses")
	switch {
	case errors.Is(err, exec.ErrNotFound):
		return "", "", problems.SSHLoginctlMissing.Problem(problems.Params{"host": hostName()})
	case err != nil:
		return "", "", loginctlFailed(err)
	case property(out, "KillUserProcesses") != "yes":
		return hostproto.DetachSession, "", nil
	}
	out, err = sys.loginctl(ctx, "show-user", strconv.Itoa(os.Getuid()), "-p", "Linger")
	switch {
	case notLingering(err):
		// A user who neither lingers nor has a session logind knows of
		// is one loginctl fails for so: they don't linger.
	case err != nil:
		return "", "", loginctlFailed(err)
	case property(out, "Linger") == "yes":
		path, err := sys.command("systemd-run")
		switch {
		case errors.Is(err, exec.ErrNotFound):
		case err != nil:
			return "", "", err
		default:
			return hostproto.DetachScope, path, checkSystemd(ctx, sys, path)
		}
	}
	return "", "", problems.SSHWorkerWouldBeKilled.Problem(problems.Params{"host": hostName()})
}

// systemdCollect is the first systemd whose systemd-run takes --collect,
// with which a scope that failed goes once it has ended, rather than stay
// loaded, its name taken, until someone resets it.
const systemdCollect = 236

// systemdVersion is the line systemd-run --version begins with.
var systemdVersion = regexp.MustCompile(`^systemd (\d+)\b`)

// checkSystemd checks that the systemd-run at path takes --collect, as
// its version says: one that doesn't is ssh_systemd_too_old, rather than a
// worker that never starts, and one whose version can't be read, an error.
func checkSystemd(ctx context.Context, sys hostSystem, path string) error {
	out, err := sys.version(ctx, path)
	if err != nil {
		return fmt.Errorf("host: %s --version: %w", path, err)
	}
	m := systemdVersion.FindStringSubmatch(out)
	if m == nil {
		return fmt.Errorf("host: %s --version says no systemd version", path)
	}
	if v, err := strconv.Atoi(m[1]); err != nil || v < systemdCollect {
		return problems.SSHSystemdTooOld.Problem(problems.Params{"host": hostName(), "version": m[1], "minimum": systemdCollect})
	}
	return nil
}

// notLingering says whether err is loginctl show-user's failure for a user
// who is neither logged in, in a session logind knows of, nor lingers.
func notLingering(err error) bool {
	exit, ok := errors.AsType[*exec.ExitError](err)
	return ok && strings.Contains(string(exit.Stderr), "is not logged in or lingering")
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
