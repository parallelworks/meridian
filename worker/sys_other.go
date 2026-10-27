//go:build !unix

package worker

import (
	"io"
	"os/exec"
	"runtime"
	"syscall"

	"github.com/parallelworks/meridian/problems"
)

// A worker runs only on a Unix host, where it can stop a step and
// everything it started as one process group, check who it talks to, and
// keep the user's other processes out of its memory.
var errNotUnix error = problems.WorkerSystemUnsupported.Problem(problems.Params{"os": runtime.GOOS})

func runProcess(*exec.Cmd, io.Writer, <-chan stopRequest, func(int)) (int, stopCause, error) {
	return -1, noStop, errNotUnix
}

func signalGroup(int, syscall.Signal) {}

func liveGroupsOf([]int) map[int]bool { return nil }

const holdsLeaders = false

const (
	openNoFollow = 0
	openNonBlock = 0
)
