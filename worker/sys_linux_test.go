//go:build linux

package worker

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"

	"github.com/parallelworks/meridian/internal/safepath"
)

// TestHarden checks that a worker makes itself not dumpable, so that the
// user's other processes can't read its memory, where its credential is.
// It runs in a process of its own, since it would change the tests'.
func TestHarden(t *testing.T) {
	if os.Getenv("MERIDIAN_TEST_HARDEN") == "1" {
		if err := harden(); err != nil {
			t.Fatal(err)
		}
		if dumpable, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, syscall.PR_GET_DUMPABLE, 0, 0); errno != 0 || dumpable != 0 {
			t.Fatalf("dumpable is %d (%v)", dumpable, errno)
		}
		return
	}
	t.Parallel()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHarden$", "-test.count=1") //nolint:gosec // the test's own binary
	cmd.Env = append(os.Environ(), "MERIDIAN_TEST_HARDEN=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
}

// TestLiveGroupNotDumpable checks that a process of a step's group that
// isn't dumpable, as a program that changes its user, or makes itself so,
// is, which /proc shows as root's, keeps its group live, so that a halt
// still signals it.
func TestLiveGroupNotDumpable(t *testing.T) {
	if os.Getenv("MERIDIAN_TEST_UNDUMPABLE") == "1" {
		if err := harden(); err != nil {
			t.Fatal(err)
		}
		os.Stdout.WriteString("ready\n")
		_, _ = io.Copy(io.Discard, os.Stdin)
		return
	}
	t.Parallel()
	cmd := exec.Command(os.Args[0], "-test.run=^TestLiveGroupNotDumpable$", "-test.count=1") //nolint:gosec // the test's own binary
	cmd.Env = append(os.Environ(), "MERIDIAN_TEST_UNDUMPABLE=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stdin.Close()
		_ = cmd.Wait()
	}()
	if line, err := bufio.NewReader(stdout).ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("the process said %q (%v)", line, err)
	}
	info, err := os.Stat("/proc/" + strconv.Itoa(cmd.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	if owner, _ := safepath.Owner(info); owner == os.Getuid() {
		t.Logf("/proc shows the process as the user's, as some sandboxes do")
	}
	if live := liveGroupsOf([]int{cmd.Process.Pid}); !live[cmd.Process.Pid] {
		t.Error("a group whose process isn't dumpable isn't live")
	}
}
