package worker

import (
	"errors"
	"net"
	"runtime"

	"github.com/parallelworks/meridian/internal/peercred"
	"github.com/parallelworks/meridian/problems"
)

// checkPeer checks that the process at the other end of a Unix socket
// runs as the worker's user, or as root, before the worker sends it a
// token or a credential (peercred). Where the system can't say, as on any
// system the worker doesn't run on, it refuses the socket.
func checkPeer(conn *net.UnixConn, path string) error {
	uid, err := peercred.UID(conn)
	switch {
	case errors.Is(err, peercred.ErrUnsupported):
		return problems.WorkerSystemUnsupported.Problem(problems.Params{"os": runtime.GOOS})
	case err != nil:
		return peerUnknown(path, err)
	}
	return checkPeerUID(path, uid)
}

// checkPeerUID checks that uid, the user the system says serves the
// coordinator's socket at path, is one the worker sends a token or a
// credential to: its own user, or root.
func checkPeerUID(path string, uid int) error {
	if !peercred.Trusted(uid) {
		return problems.CoordinatorPeerRefused.Problem(problems.Params{"path": path, "uid": uid})
	}
	return nil
}

// peerUnknown is the problem of a coordinator's socket at path whose user
// the system couldn't say, for the reason err gives.
func peerUnknown(path string, err error) error {
	return problems.CoordinatorPeerUnknown.Problem(problems.Params{"path": path, "reason": err.Error()})
}
