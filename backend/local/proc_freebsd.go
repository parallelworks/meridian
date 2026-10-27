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

// kinfoLayout is where processStart reads FreeBSD's struct kinfo_proc,
// from <sys/user.h>: its size, ki_structsize, which pins its layout; the
// offsets of ki_pid, a 32-bit pid_t, of ki_start, the struct timeval of
// when the process started, and of ki_stat, its state; and the sizes of
// ki_start's seconds, which are 32 bits only on i386, and microseconds, a
// long. Up to ki_stat the struct is the same on every architecture but for
// the size of its pointers, longs and time_t.
type kinfoLayout struct {
	size, pid, start, stat int
	secSize, usecSize      int
}

var kinfo = func() kinfoLayout {
	switch {
	case unsafe.Sizeof(uintptr(0)) == 8:
		return kinfoLayout{size: 1088, pid: 72, start: 336, stat: 388, secSize: 8, usecSize: 8}
	case runtime.GOARCH == "386":
		return kinfoLayout{size: 768, pid: 40, start: 280, stat: 308, secSize: 4, usecSize: 4}
	default:
		return kinfoLayout{size: 816, pid: 40, start: 280, stat: 324, secSize: 8, usecSize: 4}
	}
}()

// szomb is SZOMB, from <sys/proc.h>: a process that has ended and waits to
// be reaped.
const szomb = 5

// readInt reads a signed integer of size bytes at off.
func readInt(raw []byte, off, size int) int64 {
	if size == 4 {
		return int64(int32(binary.NativeEndian.Uint32(raw[off:]))) //nolint:gosec // a C int's bits
	}
	return int64(binary.NativeEndian.Uint64(raw[off:])) //nolint:gosec // a C long's bits
}

// processStart returns when the process pid started, in microseconds since
// the boot: kern.proc.pid's ki_start, which is when it started since the
// boot plus kern.boottime, less kern.boottime, which a change to the clock
// moves, as it moves ki_start. It reads both again if kern.boottime moved
// as it read them. It returns errNoProcess for a process that isn't there,
// or has ended and waits to be reaped. The kinfo_proc must be of the
// layout kinfo says, by its size and the process ID in it.
func processStart(pid int) (string, error) {
	for range 3 {
		before, err := unix.SysctlTimeval("kern.boottime")
		if err != nil {
			return "", err
		}
		raw, err := unix.SysctlRaw("kern.proc.pid", pid)
		if errors.Is(err, unix.ESRCH) || err == nil && len(raw) == 0 {
			return "", errNoProcess
		} else if err != nil {
			return "", err
		}
		after, err := unix.SysctlTimeval("kern.boottime")
		if err != nil {
			return "", err
		}
		if *before != *after {
			continue
		}
		switch {
		case len(raw) < kinfo.size || readInt(raw, 0, 4) != int64(kinfo.size):
			return "", fmt.Errorf("local: kern.proc.pid gave a kinfo_proc of %d bytes, not %d", len(raw), kinfo.size)
		case readInt(raw, kinfo.pid, 4) != int64(pid):
			return "", fmt.Errorf("local: kern.proc.pid gave process %d for %d: a kinfo_proc of another layout", readInt(raw, kinfo.pid, 4), pid)
		case raw[kinfo.stat] == szomb:
			return "", errNoProcess
		}
		sec := readInt(raw, kinfo.start, kinfo.secSize)
		usec := readInt(raw, kinfo.start+kinfo.secSize, kinfo.usecSize)
		since := (sec-int64(after.Sec))*1_000_000 + usec - int64(after.Usec) //nolint:unconvert // 32 bits on i386
		return strconv.FormatInt(since, 10), nil
	}
	return "", errors.New("local: kern.boottime kept moving")
}

// readBootID returns the boot's ID, kern.boot_id, which FreeBSD makes at
// random each boot, from 13.5 and 14.
func readBootID() (string, error) {
	id, err := unix.SysctlRaw("kern.boot_id")
	if err == nil && len(id) == 0 {
		err = errors.New("local: kern.boot_id is empty")
	}
	return hex.EncodeToString(id), err
}

// hostMemory returns the host's memory, in bytes, as hw.physmem says.
func hostMemory() (int64, error) {
	n, err := unix.SysctlUint64("hw.physmem")
	return int64(n), err //nolint:gosec // a host's memory fits in an int64
}
