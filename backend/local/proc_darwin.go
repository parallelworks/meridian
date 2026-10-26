//go:build darwin

package local

import (
	"errors"
	"fmt"
	"strconv"

	"golang.org/x/sys/unix"
)

// szomb is SZOMB, from <sys/proc.h>: a process that has ended and waits to
// be reaped.
const szomb = 5

// processStart returns when the process pid started, by the wall clock, to
// the microsecond, as kern.proc.pid says, or errNoProcess for one that
// isn't there or has ended and waits to be reaped.
func processStart(pid int) (string, error) {
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	// kern.proc.pid gives nothing for a process that isn't there, which
	// SysctlKinfoProc says is EIO.
	if errors.Is(err, unix.EIO) || errors.Is(err, unix.ESRCH) {
		return "", errNoProcess
	} else if err != nil {
		return "", err
	}
	if int(kp.Proc.P_pid) != pid || kp.Proc.P_stat == szomb {
		return "", errNoProcess
	}
	tv := kp.Proc.P_starttime
	return fmt.Sprintf("%d.%06d", tv.Sec, tv.Usec), nil
}

// bootID returns when the system booted, to the second, as kern.boottime
// says.
func bootID() (string, error) {
	tv, err := unix.SysctlTimeval("kern.boottime")
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(tv.Sec, 10), nil
}

// hostMemory returns the host's memory, in bytes, as hw.memsize says.
func hostMemory() (int64, error) {
	n, err := unix.SysctlUint64("hw.memsize")
	return int64(n), err //nolint:gosec // a host's memory fits in an int64
}
