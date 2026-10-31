package sessionproxy

import (
	"context"
	"errors"
	"net"
	"strconv"
	"syscall"

	"github.com/parallelworks/meridian/problems"
)

// Listen listens on port of 127.0.0.1, this machine's loopback address,
// for a session, or on a port the system picks for 0, and never on any
// other address: a session is this machine's user's, whom another
// machine reaches only through ssh -L. A port in use is
// problems.SessionLocalPortTaken, and one the system won't let the
// process listen on, as a privileged port, problems.SessionLocalPortRefused,
// each of which a session's step reports; Meridian doesn't take another,
// since the workflow can tell the user to open that one.
func Listen(ctx context.Context, port int) (net.Listener, error) {
	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	switch {
	case errors.Is(err, syscall.EADDRINUSE):
		return nil, problems.SessionLocalPortTaken.Problem(problems.Params{"port": strconv.Itoa(port)})
	case errors.Is(err, syscall.EACCES):
		return nil, problems.SessionLocalPortRefused.Problem(problems.Params{"port": strconv.Itoa(port)})
	}
	return l, err
}
