//go:build linux

package harden

import "syscall"

// Process keeps the user's other processes from reading the process's
// memory, or attaching to it: it makes the process not dumpable, which
// also keeps it from dumping core. A process it starts is dumpable again
// once it runs its program.
func Process() error {
	if _, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, syscall.PR_SET_DUMPABLE, 0, 0); errno != 0 {
		return errno
	}
	return nil
}
