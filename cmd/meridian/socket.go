package main

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"

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
// to, and serves workers on it with handler, until ctx is done: then it
// closes it, and releases it, so that one that waits takes it over. Only
// the process that holds the socket's endpoint in the store listens at
// path, so a socket at path is one that a process that held it before
// left, which it replaces.
func holdSocket(ctx context.Context, coord *coordinator.Coordinator, path string, handler http.Handler) *socket {
	s := &socket{errs: make(chan error, 1), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		if err := serveSocket(ctx, coord, path, handler); err != nil && ctx.Err() == nil {
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

func serveSocket(ctx context.Context, coord *coordinator.Coordinator, path string, handler http.Handler) error {
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
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	select {
	case <-ctx.Done():
		// Closing the server closes its listener, which removes the
		// socket.
		_ = srv.Close()
		<-served
		return nil
	case err := <-served:
		return problems.SocketUnusable.Problem(problems.Params{"path": path, "reason": err.Error()})
	}
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
	return ln, nil
}

// workerAPIUnavailable answers every request 503 Service Unavailable, to
// be tried again a second later, as a coordinator does that can't serve it
// yet: a worker tries again. The worker API's server takes its place once
// the coordinator has one.
func workerAPIUnavailable() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusServiceUnavailable)
	})
}
