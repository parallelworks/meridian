//go:build !unix

package worker

import (
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"syscall"
)

// A worker runs only on a Unix host, where it can stop a step and
// everything it started as one process group, and check who it talks to.
var errNotUnix = errors.New("worker: a worker runs only on a Unix host")

func runProcess(*exec.Cmd, io.Writer, <-chan stopRequest, func(int)) (int, stopCause, error) {
	return -1, noStop, errNotUnix
}

func signalGroup(int, syscall.Signal) {}

func groupAlive(int) bool { return false }

func checkPeer(*net.UnixConn, string) error { return errNotUnix }

func harden() error { return errNotUnix }

func ownedByUser(os.FileInfo) bool { return false }

const (
	openNoFollow = 0
	openNonBlock = 0
)
