//go:build unix

package local

import (
	"os/exec"
	"syscall"
)

// killGroup makes cmd run in a session of its own, without a controlling
// terminal, and makes cancelling it kill the session's process group:
// git's helpers, such as git-remote-https, as well as git, which would
// otherwise hold its output open after it was killed. With no terminal,
// an ssh command that would ask for a passphrase fails at once, rather
// than be stopped in the background waiting for one.
func killGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
}
