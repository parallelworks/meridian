package worker

import (
	"net"
	"syscall"

	"github.com/parallelworks/meridian/problems"
)

// checkPeer checks that the process at the other end of a Unix socket
// runs as the worker's user, or as root, before the worker sends it a
// token or a credential: SO_PEERCRED says who it ran as when it bound the
// socket. Its struct sockpeercred starts with the user's ID, a uid_t of 32
// bits, and getsockopt gives as much of an option as it is asked for, so
// the worker asks for that.
func checkPeer(conn *net.UnixConn, path string) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return peerUnknown(path, err)
	}
	var uid int
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		uid, credErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return peerUnknown(path, err)
	}
	if credErr != nil {
		return peerUnknown(path, credErr)
	}
	return checkPeerUID(path, int(uint32(uid))) //nolint:gosec // a uid_t's 32 bits, which GetsockoptInt reads as a C int
}

// harden checks that the system keeps the user's other processes, a job's
// steps among them, from attaching to the worker, to read its memory,
// where its credential is. OpenBSD has no /proc, and lets a process trace
// only its own descendants unless kern.global_ptrace is set, and gives a
// process no way to refuse tracing itself, so the worker refuses to run
// where it is set.
func harden() error {
	global, err := syscall.SysctlUint32("kern.global_ptrace")
	if err != nil {
		return problems.WorkerHardenFailed.Problem(problems.Params{"reason": err.Error()})
	}
	if global != 0 {
		return problems.GlobalPtraceEnabled.Problem(nil)
	}
	return nil
}
