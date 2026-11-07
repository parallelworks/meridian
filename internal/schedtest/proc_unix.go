//go:build unix

package schedtest

import (
	"os/exec"
	"syscall"
)

// detach starts cmd in a session of its own, as slurmstepd is apart from
// what submitted its job.
func detach(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} }

// group starts cmd in a process group of its own, which a job's signals
// go to.
func group(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }

// killGroup sends the process group pgid SIGKILL, or else SIGTERM.
func killGroup(pgid int, kill bool) {
	sig := syscall.SIGTERM
	if kill {
		sig = syscall.SIGKILL
	}
	_ = syscall.Kill(-pgid, sig)
}

// terminate sends the process pid SIGTERM.
func terminate(pid int) { _ = syscall.Kill(pid, syscall.SIGTERM) }

// signalNumber is the number of the signal that ended a process, or 0.
func signalNumber(state *exec.ExitError) int {
	if ws, ok := state.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return int(ws.Signal())
	}
	return 0
}
