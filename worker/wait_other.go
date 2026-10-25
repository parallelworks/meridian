//go:build unix && !(linux && !mips && !mipsle && !mips64 && !mips64le)

package worker

import (
	"os/exec"
	"syscall"
)

// holdsLeaders is unset where the worker can't wait for a process without
// reaping it, as on darwin, whose system calls go through libSystem: there
// a step's group leader is reaped as it exits, and once its group has no
// process left, its number can be given to another process group before
// the worker forgets it, so a halt could signal that group, of the same
// user. The worker forgets a group as soon as it finds it empty.
const holdsLeaders = false

// waitExit waits for cmd's process to exit, reaps it, and returns its exit
// status, or -1 if a signal ended it.
func waitExit(cmd *exec.Cmd) int {
	return exitOf(cmd.Wait())
}

// liveGroupsOf returns which of groups have a process left.
func liveGroupsOf(groups []int) map[int]bool {
	live := map[int]bool{}
	for _, g := range groups {
		if syscall.Kill(-g, 0) == nil {
			live[g] = true
		}
	}
	return live
}
