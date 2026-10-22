//go:build linux && !mips && !mipsle && !mips64 && !mips64le

package worker

import (
	"bytes"
	"encoding/binary"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"unsafe"
)

// holdsLeaders is set where the worker keeps each step's process group
// leader unreaped, a zombie, until the attempt ends or its group has no
// process left: while it is, its group's number stays the group's, and no
// other process group can be given it, for a halt to signal.
const holdsLeaders = true

// What waitid takes, from <sys/wait.h>, which package syscall doesn't
// have all of.
const (
	pPID      = 1
	wExited   = 0x4
	wNoWait   = 0x1000000
	cldExited = 1
)

// waitExit waits for cmd's process to exit, and returns its exit status,
// or -1 if a signal ended it, as exec.ExitError's ExitCode does, without
// reaping it: waitid with WNOWAIT, a raw system call, since package
// syscall has none. The attempt reaps it, with cmd.Wait, once its group
// has no process left, or at its end.
func waitExit(cmd *exec.Cmd) int {
	// A siginfo_t: si_signo, si_errno and si_code, then, aligned as a
	// pointer is, si_pid, si_uid and si_status.
	var info [128]byte
	for {
		_, _, errno := syscall.Syscall6(syscall.SYS_WAITID, pPID, uintptr(cmd.Process.Pid),
			uintptr(unsafe.Pointer(&info[0])), wExited|wNoWait, 0, 0) //nolint:gosec // waitid writes the siginfo_t it is given
		if errno == syscall.EINTR {
			continue
		}
		if errno != 0 {
			return exitOf(cmd.Wait())
		}
		break
	}
	fields := 12
	if unsafe.Sizeof(uintptr(0)) == 8 {
		fields = 16
	}
	code := int32(binary.NativeEndian.Uint32(info[8:12]))        //nolint:gosec // the bits of a C int
	status := int32(binary.NativeEndian.Uint32(info[fields+8:])) //nolint:gosec // the bits of a C int
	if code == cldExited {
		return int(status)
	}
	return -1
}

// liveGroupsOf returns which of groups have a process left that isn't a
// zombie, from /proc: a zombie leader, which the worker holds, answers a
// signal as a live process does.
func liveGroupsOf(groups []int) map[int]bool {
	live := map[int]bool{}
	want := map[int]bool{}
	for _, g := range groups {
		want[g] = true
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		// What can't be told is taken to be live.
		for _, g := range groups {
			live[g] = true
		}
		return live
	}
	uid := os.Getuid()
	for _, e := range entries {
		if _, err := strconv.Atoi(e.Name()); err != nil {
			continue
		}
		// A process the worker can signal is its user's: reading only
		// theirs makes the scan as cheap as the user's processes are few.
		if info, err := e.Info(); err != nil || !ownedBy(info, uid) {
			continue
		}
		data, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		// pid (comm) state ppid pgrp …, where comm can hold anything.
		_, after, ok := bytes.CutLast(data, []byte(")"))
		if !ok {
			continue
		}
		fields := strings.Fields(string(after))
		if len(fields) < 3 || fields[0] == "Z" || fields[0] == "X" {
			continue
		}
		if g, err := strconv.Atoi(fields[2]); err == nil && want[g] {
			live[g] = true
		}
	}
	return live
}

// ownedBy reports whether info, of a file in /proc, belongs to uid.
func ownedBy(info os.FileInfo, uid int) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(st.Uid) == uid
}
