//go:build linux

package local

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// processStart returns when the process pid started, in clock ticks since
// the boot, as field 22 of /proc/<pid>/stat says, or errNoProcess for one
// that isn't there or has ended and waits to be reaped.
func processStart(pid int) (string, error) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
		return "", errNoProcess
	} else if err != nil {
		return "", err
	}
	// The command's name, field 2, is in parentheses, and can hold spaces
	// and parentheses of its own: the fields after it follow the last.
	_, after, ok := bytes.CutLast(data, []byte(")"))
	if !ok {
		return "", fmt.Errorf("local: /proc/%d/stat isn't one: %q", pid, data)
	}
	fields := strings.Fields(string(after))
	if len(fields) < 20 {
		return "", fmt.Errorf("local: /proc/%d/stat isn't one: %q", pid, data)
	}
	// Field 3 is its state: a zombie, or one being reaped, has ended.
	if fields[0] == "Z" || fields[0] == "X" {
		return "", errNoProcess
	}
	return fields[19], nil
}

// bootID returns the boot's ID, which the kernel makes at random each
// boot, so that a start time, counted from the boot, can't be another
// boot's.
func bootID() (string, error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	return strings.TrimSpace(string(data)), err
}

// hostMemory returns the host's memory, in bytes.
func hostMemory() (int64, error) {
	var info syscall.Sysinfo_t
	if err := syscall.Sysinfo(&info); err != nil {
		return 0, err
	}
	return int64(info.Totalram) * int64(info.Unit), nil //nolint:gosec,unconvert // a host's memory fits in an int64, and the types differ by architecture
}

// runningExecutable returns the path that starts the file this process
// runs: /proc/self/exe, which is that file in the process that execs it,
// a child of this one, even if another file has taken exe's path since, as
// an upgrade in place does.
func runningExecutable(string) (string, error) { return "/proc/self/exe", nil }

// signalWorker sends sig to r's worker, through a pidfd that names the
// process, taken before the driver checks that it is r's: a process that
// has its ID after it ends, as ID are reused, is never signalled.
func signalWorker(r *record, sig syscall.Signal) error {
	p, err := os.FindProcess(r.PID)
	if err != nil {
		return err
	}
	defer func() { _ = p.Release() }()
	if alive, err := r.alive(); err != nil {
		return err
	} else if !alive {
		return errNoProcess
	}
	if err := p.Signal(sig); errors.Is(err, os.ErrProcessDone) {
		return errNoProcess
	} else if err != nil {
		return err
	}
	return nil
}
