package main

import (
	"context"
	"flag"
	"fmt"
	"slices"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/problems"
)

// sessionAbout is what meridian session's usage says of it besides its
// summary.
const sessionAbout = `meridian session url <run> <key> prints a link to the session key of the
run's sessions:, as the meridian run or resume that serves it does on a
person's terminal: at the session's host name on this machine,
<label>.localhost, and its port, with a one-time code, which works once,
within ten minutes, and has the browser that opens it keep the session's
cookie. <run> is the run whose sessions: has the session, as meridian run says
when it serves it; a workflow a step calls has its own, which every try,
attempt and matrix instance of the call shares, and any of their runs
names. A link session's url it prints as it is.

meridian session reset <run> <key> gives the session a new secret, which
signs out every browser that opened it, and makes the links printed before
it worthless.

It exits 0 once it has, and 2 for a run the store hasn't (run_not_found),
or without such a session (session_key_unknown), for a session that isn't
served yet (session_not_ready), or that no process serves, as after the
meridian run that served it ended, which meridian resume serves again
(session_not_served), and for any other problem of its own.`

// runSession prints a link to a session of a run, or resets it.
func runSession(ctx context.Context, c *cli, fs *flag.FlagSet, args []string) int {
	positional, err := parseFlags(fs, args)
	if err != nil {
		return flagError(c, fs, err)
	}
	if len(positional) != 3 || !slices.Contains([]string{"url", "reset"}, positional[0]) {
		return c.failf(problems.ArgumentsInvalid, problems.Params{"command": "session"})
	}
	op, id, key := positional[0], positional[1], positional[2]
	dirs, err := local.Locate(c.getenv)
	if err != nil {
		return c.fail(err)
	}
	cfg, err := local.LoadConfig(dirs.Config, c.lookupEnv)
	if err != nil {
		return c.fail(err)
	}
	db, err := storeOf(dirs)
	if err != nil {
		return c.fail(runNotFound(id, err))
	}
	co, err := coordinate(ctx, c, dirs, cfg, db)
	if err != nil {
		return c.fail(err)
	}
	defer func() { _ = co.close() }()
	coord := co.coord
	s, err := runSessionOf(ctx, coord, id, key)
	if err != nil {
		return c.fail(err)
	}
	if op == "reset" {
		if err := coord.ResetSession(ctx, s.ID); err != nil {
			return c.fail(err)
		}
		if _, err := fmt.Fprintf(c.stdout, "session %s of run %s has a new secret: every browser that opened it is signed out\n", key, id); err != nil {
			return c.fail(err)
		}
		return exitOK
	}
	link, err := co.sessions.Link(ctx, s)
	if err != nil {
		return c.fail(err)
	}
	if _, err := fmt.Fprintln(c.stdout, problems.EscapeControl(link)); err != nil {
		return c.fail(err)
	}
	return exitOK
}

// runSessionOf returns the session key of the run id's sessions:, which
// hasn't ended: the one its call made, which every try of the call shares,
// so that any of their runs finds it. A run the store hasn't is
// problems.RunNotFound, and one without such a session
// problems.SessionKeyUnknown, which lists those it has.
func runSessionOf(ctx context.Context, coord *coordinator.Coordinator, id, key string) (coordinator.Session, error) {
	all, err := coord.CallSessions(ctx, id)
	if err != nil {
		return coordinator.Session{}, runNotFound(id, err)
	}
	keys := make([]string, 0, len(all))
	for _, s := range all {
		if s.Key == key {
			return s, nil
		}
		keys = append(keys, s.Key)
	}
	return coordinator.Session{}, problems.SessionKeyUnknown.Problem(problems.Listed(problems.Params{"run": id, "key": key}, "keys", keys))
}
