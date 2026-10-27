package main

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"os"
	"strconv"
	"syscall"

	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/problems"
)

// socketEndpoint is the name the store records the coordinator's socket
// under, which one meridian run of a state directory at a time holds.
const socketEndpoint = "socket"

// maxSocketPath is the most bytes a Unix socket's path can have on this
// system: the size of the address that holds it, less the NUL that ends
// it.
var maxSocketPath = len(syscall.RawSockaddrUnix{}.Path) - 1

// A socket is the coordinator's socket, which workers on this machine
// connect to, as one meridian run holds it (holdSocket).
type socket struct {
	// errs has the problem that stopped the socket, if one did, and done
	// is closed once the socket is closed and released.
	errs chan error
	done chan struct{}
}

// holdSocket takes the coordinator's socket at path, for coord's store,
// once no other process holds it, as every meridian run of the store waits
// to, and serves the worker API on it (coordinator.ServeWorkers), storing
// the logs workers upload with logs, until ctx is done: then it closes it,
// and releases it, so that one that waits takes it over. Only the process
// that holds the socket's endpoint in the store listens at path, so a
// socket at path is one that a process that held it before left, which it
// replaces.
func holdSocket(ctx context.Context, coord *coordinator.Coordinator, path string, logs coordinator.Logs) *socket {
	s := &socket{errs: make(chan error, 1), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		if err := serveSocket(ctx, coord, path, logs); err != nil && ctx.Err() == nil {
			s.errs <- err
		}
	}()
	return s
}

// checkSocketPath checks that a Unix socket can be at path, whose length
// the system limits.
func checkSocketPath(path string) error {
	if len(path) > maxSocketPath {
		return problems.SocketPathTooLong.Problem(problems.Params{"path": path, "limit": maxSocketPath})
	}
	return nil
}

func serveSocket(ctx context.Context, coord *coordinator.Coordinator, path string, logs coordinator.Logs) error {
	if err := checkSocketPath(path); err != nil {
		return err
	}
	hold, err := coord.HoldEndpoint(ctx, socketEndpoint, "meridian run, process "+strconv.Itoa(os.Getpid()))
	if err != nil {
		return err
	}
	// The lock is released even if the record of who holds it can't be
	// deleted, which the next holder replaces.
	defer func() { _ = hold.Release(context.WithoutCancel(ctx)) }()
	ln, err := listenSocket(ctx, path)
	if err != nil {
		return err
	}
	// Once ctx is done, the server closes its listener, which removes the
	// socket.
	err = coord.ServeWorkers(ctx, ln, logs)
	if ctx.Err() != nil {
		return nil
	}
	if err == nil {
		err = errors.New("the worker API's server stopped")
	}
	return problems.SocketUnusable.Problem(problems.Params{"path": path, "reason": err.Error()})
}

// listenSocket listens on a Unix socket at path, once it has removed a
// socket there, which a process that held it before left. Anything else at
// path is left for its user to remove.
func listenSocket(ctx context.Context, path string) (net.Listener, error) {
	unusable := func(err error) error {
		return problems.SocketUnusable.Problem(problems.Params{"path": path, "reason": err.Error()})
	}
	if info, err := os.Lstat(path); err == nil && info.Mode()&fs.ModeSocket != 0 {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, unusable(err)
		}
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "unix", path)
	if err != nil {
		return nil, unusable(err)
	}
	// Only the user's processes connect to it, as the state directory,
	// which only they can open, already sees to: this is in case it
	// doesn't.
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		return nil, unusable(err)
	}
	return ln, nil
}
