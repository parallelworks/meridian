//go:build linux || darwin || freebsd

package worker

import (
	"os"

	"github.com/parallelworks/meridian/problems"
)

// checkPeerUID checks that uid, the user the system says serves the
// coordinator's socket at path, is one the worker sends a token or a
// credential to: its own user, or root.
func checkPeerUID(path string, uid int) error {
	if uid != os.Getuid() && uid != 0 {
		return problems.CoordinatorPeerRefused.Problem(problems.Params{"path": path, "uid": uid})
	}
	return nil
}

// peerUnknown is the problem of a coordinator's socket at path whose user
// the system couldn't say, for the reason err gives.
func peerUnknown(path string, err error) error {
	return problems.CoordinatorPeerUnknown.Problem(problems.Params{"path": path, "reason": err.Error()})
}
