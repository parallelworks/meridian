//go:build linux

package worker

import (
	"fmt"
	"net"
	"syscall"
)

// checkPeer checks that the process at the other end of a Unix socket
// runs as the worker's user, or as root, before the worker sends it a
// token or a credential.
func checkPeer(conn *net.UnixConn, path string) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var cred *syscall.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return err
	}
	if credErr != nil {
		return credErr
	}
	if !peerAllowed(int(cred.Uid)) {
		return fmt.Errorf("worker: the coordinator's socket %s is served by user %d, not this one or root", path, cred.Uid)
	}
	return nil
}

// harden keeps the user's other processes, a job's steps among them, from
// reading the worker's memory, where its credential is, or attaching to
// it: it makes the worker not dumpable. A process it starts is dumpable
// again once it runs its program.
func harden() error {
	if _, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, syscall.PR_SET_DUMPABLE, 0, 0); errno != 0 {
		return fmt.Errorf("worker: making itself not dumpable: %w", errno)
	}
	return nil
}
