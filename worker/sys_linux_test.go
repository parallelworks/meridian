//go:build linux

package worker

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"

	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/workerapi"
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

// hardenedChild starts the test test in a process of its own, with env
// added to its environment, which hardens itself and waits until the test
// ends (runHardenedChild), once it has hardened itself.
func hardenedChild(t *testing.T, test string, env ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+test+"$", "-test.count=1") //nolint:gosec // the test's own binary
	cmd.Env = append(append(os.Environ(), "MERIDIAN_TEST_UNDUMPABLE=1"), env...)
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
	t.Cleanup(func() {
		stdin.Close()
		_ = cmd.Wait()
	})
	if line, err := bufio.NewReader(stdout).ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("the process said %q (%v)", line, err)
	}
	return cmd
}

// runHardenedChild reports whether this process is a hardenedChild, and if
// it is, hardens it, says so, and waits until its test ends.
func runHardenedChild(t *testing.T) bool {
	if os.Getenv("MERIDIAN_TEST_UNDUMPABLE") != "1" {
		return false
	}
	if err := harden(); err != nil {
		t.Fatal(err)
	}
	os.Stdout.WriteString("ready\n")
	_, _ = io.Copy(io.Discard, os.Stdin)
	return true
}

// TestLiveGroupNotDumpable checks that a process of a step's group that
// isn't dumpable, as a program that changes its user, or makes itself so,
// is, which /proc shows as root's, keeps its group live, so that a halt
// still signals it.
func TestLiveGroupNotDumpable(t *testing.T) {
	if runHardenedChild(t) {
		return
	}
	t.Parallel()
	cmd := hardenedChild(t, "TestLiveGroupNotDumpable")
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

// TestHardenedEnviron checks that the user's other processes can't read a
// hardened worker's environment, where a bootstrap token can be, from
// /proc, which follows the dumpable flag harden clears.
func TestHardenedEnviron(t *testing.T) {
	if runHardenedChild(t) {
		return
	}
	if os.Getuid() == 0 {
		t.Skip("root reads every process's environment")
	}
	t.Parallel()
	const token = "bootstrap-token-sentinel"
	cmd := hardenedChild(t, "TestHardenedEnviron", workerapi.EnvBootstrapToken+"="+token)
	data, err := os.ReadFile("/proc/" + strconv.Itoa(cmd.Process.Pid) + "/environ")
	if err == nil || bytes.Contains(data, []byte(token)) {
		t.Errorf("read %d bytes of the hardened process's environment (%v)", len(data), err)
	}
}
