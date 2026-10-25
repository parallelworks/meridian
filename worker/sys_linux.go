//go:build linux

package worker

import (
	"net"
	"syscall"

	"github.com/parallelworks/meridian/problems"
)

// checkPeer checks that the process at the other end of a Unix socket
// runs as the worker's user, or as root, before the worker sends it a
// token or a credential: SO_PEERCRED says who it ran as when it connected
// or listened.
func checkPeer(conn *net.UnixConn, path string) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return peerUnknown(path, err)
	}
	var cred *syscall.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return peerUnknown(path, err)
	}
	if credErr != nil {
		return peerUnknown(path, credErr)
	}
	return checkPeerUID(path, int(cred.Uid))
}

// harden keeps the user's other processes, a job's steps among them, from
// reading the worker's memory, where its credential is, or attaching to
// it: it makes the worker not dumpable, which also keeps it from dumping
// core. A process it starts is dumpable again once it runs its program.
func harden() error {
	if _, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, syscall.PR_SET_DUMPABLE, 0, 0); errno != 0 {
		return problems.WorkerHardenFailed.Problem(problems.Params{"reason": errno.Error()})
	}
	return nil
}
