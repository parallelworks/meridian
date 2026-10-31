package sessionproxy

import (
	"context"
	"errors"
	"net"
	"strconv"
	"syscall"

	"github.com/parallelworks/meridian/problems"
)

// A Listener is where a session's port listens: one port of this
// machine's loopback addresses, 127.0.0.1's and, where the system has
// IPv6, [::1]'s, the same port of both, and no other address. A browser
// resolves a session's host name, <label>.localhost, to both, and can try
// [::1] first, as curl and Chrome do: listening on both, the proxy leaves
// none of them to another program, which could otherwise take the
// session's first request, its code and its cookie with it.
type Listener struct {
	// v4 listens on 127.0.0.1, and v6 on [::1] at the same port, or is nil
	// where the system has no IPv6 loopback.
	v4, v6 net.Listener
}

// Port returns the port the listener listens on.
func (l *Listener) Port() int {
	a, _ := l.v4.Addr().(*net.TCPAddr)
	if a == nil {
		return 0
	}
	return a.Port
}

// Close closes the listener, on both addresses.
func (l *Listener) Close() error {
	err := l.v4.Close()
	if l.v6 != nil {
		err = errors.Join(err, l.v6.Close())
	}
	return err
}

// listeners returns what the listener listens with.
func (l *Listener) listeners() []net.Listener {
	if l.v6 == nil {
		return []net.Listener{l.v4}
	}
	return []net.Listener{l.v4, l.v6}
}

// portTries is how many ports Listen tries, for a port the system picks,
// before it gives up: one the system picks on 127.0.0.1 can be another
// program's on [::1].
const portTries = 16

// listen is how Listen listens, which a test replaces.
var listen = func(ctx context.Context, network, address string) (net.Listener, error) {
	return (&net.ListenConfig{}).Listen(ctx, network, address)
}

// Listen listens on port of this machine's loopback addresses, 127.0.0.1
// and, where the system has IPv6, [::1], the same port of both, for a
// session, or, for 0, on one the system picks on 127.0.0.1 that is free on
// [::1] too; and never on any other address: a session is this machine's
// user's, whom another machine reaches only through ssh -L.
//
// A port in use, on either address, is problems.SessionLocalPortTaken, as
// one a session was served at before, which another program took, and one
// the system won't let the process listen on, as a privileged port,
// problems.SessionLocalPortRefused; for 0, each says the system picked it,
// and so does SessionLocalPortTaken where every port the system picked, in
// portTries tries, was another program's on [::1]. Its caller, which
// serves a session, takes another, or says why it can't. [::1] is left out
// only where the system has no IPv6 loopback, as it says by refusing it
// as an address it doesn't have, or a family it doesn't support: then no
// program can listen on it.
func Listen(ctx context.Context, port int) (*Listener, error) {
	for try := 1; ; try++ {
		v4, err := listen(ctx, "tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err != nil {
			return nil, portProblem(port, err)
		}
		got, _ := v4.Addr().(*net.TCPAddr)
		v6, err := listen(ctx, "tcp6", net.JoinHostPort("::1", strconv.Itoa(got.Port)))
		switch {
		case err == nil:
			return &Listener{v4: v4, v6: v6}, nil
		case errors.Is(err, syscall.EADDRNOTAVAIL) || errors.Is(err, syscall.EAFNOSUPPORT):
			return &Listener{v4: v4}, nil
		}
		_ = v4.Close()
		if port != 0 || !errors.Is(err, syscall.EADDRINUSE) {
			return nil, portProblem(got.Port, err)
		}
		if try == portTries {
			return nil, portProblem(0, err)
		}
	}
}

// portProblem returns err, an error listening on port, as the problem it
// is: one in use, or one the system won't let the process listen on; for
// 0, one the system picked.
func portProblem(port int, err error) error {
	params := problems.Params{"port": strconv.Itoa(port), "picked": strconv.FormatBool(port == 0)}
	switch {
	case errors.Is(err, syscall.EADDRINUSE):
		return problems.SessionLocalPortTaken.Problem(params)
	case errors.Is(err, syscall.EACCES):
		return problems.SessionLocalPortRefused.Problem(params)
	}
	return err
}
