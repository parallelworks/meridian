//go:build unix

package worker

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// runProcess runs cmd, in a process group of its own, which it reports to
// started, with what it prints on its standard output and error written to
// out, until it has exited and closed its output. When stop asks it to,
// it sends the group SIGTERM, and SIGKILL when the latest request says,
// which a later request can make sooner, never later; then it stops
// reading the output afterKill later, should something the step started
// keep it open. It returns the command's exit status, -1 if a signal ended
// it, and why it was first asked to stop, or an error if it didn't start.
//
// A step ends when its output closes, as in format 1, not when its shell
// exits: what it starts in the background and leaves writing to it is
// still the step's.
func runProcess(cmd *exec.Cmd, out io.Writer, stop <-chan stopRequest, started func(group int)) (int, stopCause, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return -1, noStop, err
	}
	defer r.Close()
	cmd.Stdin = nil
	cmd.Stdout, cmd.Stderr = w, w
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	err = cmd.Start()
	w.Close()
	if err != nil {
		return -1, noStop, err
	}
	group := cmd.Process.Pid
	started(group)

	copied := make(chan struct{})
	go func() {
		_, _ = io.Copy(out, r)
		close(copied)
	}()
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()

	var (
		cause     = noStop
		waitErr   error
		kill      *time.Timer
		killC     <-chan time.Time
		killAt    time.Time
		afterKill time.Duration
		abandon   <-chan time.Time
		running   = true
		printing  = true
	)
	defer func() {
		if kill != nil {
			kill.Stop()
		}
	}()
	for running || printing {
		select {
		case waitErr = <-exited:
			running, exited = false, nil
		case <-copied:
			printing, copied = false, nil
		case req := <-stop:
			if cause == noStop {
				cause = req.cause
				signalGroup(group, syscall.SIGTERM)
			}
			if killAt.IsZero() || req.kill.Before(killAt) {
				killAt, afterKill = req.kill, req.afterKill
				if kill == nil {
					kill = time.NewTimer(time.Until(killAt))
				} else {
					kill.Reset(time.Until(killAt))
				}
				killC = kill.C
			}
		case <-killC:
			// SIGKILL is never put off: no request after it matters.
			killC, stop = nil, nil
			signalGroup(group, syscall.SIGKILL)
			abandon = time.After(afterKill)
		case <-abandon:
			// Something the step started outside its process group still
			// holds its output: stop waiting for it.
			abandon = nil
			r.Close()
		}
	}
	if waitErr != nil {
		if exit, ok := errors.AsType[*exec.ExitError](waitErr); ok {
			return exit.ExitCode(), cause, nil
		}
		return -1, cause, nil
	}
	return 0, cause, nil
}

// signalGroup sends sig to every process in a process group that is left.
func signalGroup(group int, sig syscall.Signal) {
	_ = syscall.Kill(-group, sig)
}

// groupAlive reports whether a process group has a process left that the
// worker can signal.
func groupAlive(group int) bool {
	return syscall.Kill(-group, 0) == nil
}
