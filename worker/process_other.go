//go:build !unix

package worker

import (
	"errors"
	"io"
	"os/exec"
	"time"
)

// runProcess fails: a worker runs steps only on a Unix host, where it can
// stop a step and everything it started as one process group.
func runProcess(*exec.Cmd, io.Writer, <-chan stopCause, func(stopCause) time.Duration, time.Duration) (int, stopCause, error) {
	return -1, noStop, errors.New("worker: steps run only on a Unix host")
}
