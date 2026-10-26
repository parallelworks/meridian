//go:build freebsd

package local

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"unsafe"

	"golang.org/x/sys/unix"
)

// The offsets in FreeBSD's struct kinfo_proc, from <sys/user.h>, of
// ki_pid, a 32-bit pid_t, and of ki_start, the struct timeval of when the
// process started, and the bytes of ki_start that hold it: its seconds,
// which are 32 bits only on i386, and its microseconds, without the
// padding after them. Before ki_pid are two ints and eight pointers; up
// to ki_start, the struct is the same on every architecture but for the
// size of its pointers and longs.
var kinfoPID, kinfoStart, kinfoStartLen = func() (int, int, int) {
	if unsafe.Sizeof(uintptr(0)) == 8 {
		return 72, 336, 16
	}
	if runtime.GOARCH == "386" {
		return 40, 280, 8
	}
	return 40, 280, 12
}()

// processStart returns when the process pid started, as the bytes of
// kinfo_proc's ki_start, the wall clock's time to the microsecond, that
// kern.proc.pid gives, or errNoProcess for one that isn't there. The
// ki_pid it gives must be pid, which checks the layout it reads.
func processStart(pid int) (string, error) {
	raw, err := unix.SysctlRaw("kern.proc.pid", pid)
	if errors.Is(err, unix.ESRCH) || err == nil && len(raw) == 0 {
		return "", errNoProcess
	} else if err != nil {
		return "", err
	}
	if len(raw) < kinfoStart+kinfoStartLen {
		return "", fmt.Errorf("local: kern.proc.pid gave %d bytes, fewer than a kinfo_proc", len(raw))
	}
	if got := int32(binary.NativeEndian.Uint32(raw[kinfoPID:])); int(got) != pid { //nolint:gosec // a pid_t is an int32
		return "", fmt.Errorf("local: kern.proc.pid gave process %d for %d: a kinfo_proc of another layout", got, pid)
	}
	return hex.EncodeToString(raw[kinfoStart : kinfoStart+kinfoStartLen]), nil
}

// bootID returns when the system booted, to the second, as kern.boottime
// says.
func bootID() (string, error) {
	tv, err := unix.SysctlTimeval("kern.boottime")
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(int64(tv.Sec), 10), nil //nolint:unconvert // 32 bits on i386
}

// hostMemory returns the host's memory, in bytes, as hw.physmem says.
func hostMemory() (int64, error) {
	n, err := unix.SysctlUint64("hw.physmem")
	return int64(n), err //nolint:gosec // a host's memory fits in an int64
}
