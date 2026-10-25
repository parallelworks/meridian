//go:build unix && !linux && !darwin && !freebsd && !openbsd

package worker

import (
	"testing"

	"github.com/parallelworks/meridian/problems"
)

// TestSystemUnsupported checks that, on a system where it can't keep the
// user's other processes out of its memory, the worker refuses to run,
// and to send a token to a socket, saying why.
func TestSystemUnsupported(t *testing.T) {
	t.Parallel()
	if err := harden(); problemOf(err, problems.WorkerSystemUnsupported) == nil {
		t.Errorf("harden: %v", err)
	}
	if err := checkPeer(nil, "/c.sock"); problemOf(err, problems.WorkerSystemUnsupported) == nil {
		t.Errorf("checkPeer: %v", err)
	}
}
