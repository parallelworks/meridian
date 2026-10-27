package harden

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// Process keeps the user's other processes from reading the process's
// memory: it refuses debuggers, which would attach to it (PT_DENY_ATTACH),
// and sets the size of a core file of it to 0, as macOS's default is, so
// that nothing makes it dump core. Reading another process's memory any
// other way takes root, or a debugger the user has authorised. A process
// it starts starts with that core file size too, and can raise it.
func Process() error {
	if err := unix.PtraceDenyAttach(); err != nil {
		return err
	}
	var core syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_CORE, &core); err != nil {
		return err
	}
	core.Cur = 0
	return syscall.Setrlimit(syscall.RLIMIT_CORE, &core)
}
