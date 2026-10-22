//go:build unix

package local

import (
	"os/exec"
	"syscall"
)

// killGroup makes cmd run in a process group of its own, and makes
// cancelling it kill the group: git's helpers, such as git-remote-https,
// as well as git, which would otherwise hold its output open after it was
// killed.
func killGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
}
