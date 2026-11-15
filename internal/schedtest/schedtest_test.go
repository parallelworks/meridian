//go:build unix

package schedtest

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	Main()
	os.Exit(m.Run())
}

// run runs the cluster's command name with args, and stdin as its standard
// input, as a test's code would, and returns what it printed and its exit
// status.
func run(t *testing.T, c *Cluster, stdin, name string, args ...string) (string, string, int) {
	t.Helper()
	cmd := exec.Command(filepath.Join(c.Bin, name), args...)
	cmd.Env = c.Env(nil)
	cmd.Stdin = strings.NewReader(stdin)
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	code := 0
	if ee, ok := errors.AsType[*exec.ExitError](err); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return stdout.String(), stderr.String(), code
}

// waitState waits until the job id is in state.
func waitState(t *testing.T, c *Cluster, id int, state string) *Job {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		j := c.Job(t, id)
		if j != nil && j.State == state {
			return j
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %d is %+v, not %s", id, j, state)
		}
		_, _, _ = run(t, c, "", "squeue", "-h", "-t", "all", "-j", strconv.Itoa(id), "-o", "%T")
	}
}

// TestJobs checks that sbatch runs a job's script, with Slurm's variables
// and its output where it asks, and that squeue, scancel and the time
// limit move it through Slurm's states, as the captured releases print
// them.
func TestJobs(t *testing.T) {
	t.Parallel()
	c := New(t, func(s *State) { s.Minute, s.KillWait = 0.5, 0.2 })
	dir := t.TempDir()
	out, errOut, code := run(t, c, "#!/bin/sh\nenv > env\necho done\n", "sbatch", "--parsable", "--job-name=meridian-a1-1", "--output="+dir+"/out", "--chdir="+dir)
	if out != "2\n" || errOut != "" || code != 0 {
		t.Fatalf("sbatch: %q, %q, %d", out, errOut, code)
	}
	waitState(t, c, 2, "COMPLETED")
	if data, err := os.ReadFile(filepath.Join(dir, "out")); err != nil || string(data) != "done\n" {
		t.Errorf("the job's output: %q, %v", data, err)
	}
	env, err := os.ReadFile(filepath.Join(dir, "env"))
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range []string{"SLURM_JOB_ID=2", "SLURM_JOB_NAME=meridian-a1-1", "SLURM_CLUSTER_NAME=fixture"} {
		if !slices.Contains(strings.Split(string(env), "\n"), kv) {
			t.Errorf("the job's environment lacks %s", kv)
		}
	}
	if strings.Contains(string(env), envStepd) {
		t.Errorf("the job's environment has %s", envStepd)
	}

	run(t, c, "#!/bin/sh\nsleep 60\n", "sbatch", "--parsable", "--job-name=b")
	run(t, c, "#!/bin/sh\nsleep 60\n", "sbatch", "--parsable", "--job-name=c", "--time=1")
	run(t, c, "#!/bin/sh\nexit 3\n", "sbatch", "--parsable", "--job-name=d")
	run(t, c, "#!/bin/sh\nsleep 60\n", "sbatch", "--parsable", "--job-name=e", "--hold")
	waitState(t, c, 3, "RUNNING")
	if _, _, code := run(t, c, "", "scancel", "3"); code != 0 {
		t.Errorf("scancel exited %d", code)
	}
	waitState(t, c, 3, "CANCELLED")
	waitState(t, c, 4, "TIMEOUT")
	waitState(t, c, 5, "FAILED")
	out, errOut, code = run(t, c, "", "squeue", "-h", "-t", "all", "-j", "2,3,4,5,6", "-o", "%i|%T|%r")
	want := "2|COMPLETED|None\n3|CANCELLED|None\n4|TIMEOUT|TimeLimit\n5|FAILED|NonZeroExitCode\n6|PENDING|JobHeldUser\n"
	if out != want || errOut != "" || code != 0 {
		t.Errorf("squeue: %q, %q, %d; want %q", out, errOut, code, want)
	}
	if _, _, code := run(t, c, "", "scancel", "2", "999"); code != 0 {
		t.Errorf("scancel of an ended job and an unknown one exited %d", code)
	}
}

func TestSqueue(t *testing.T) {
	t.Parallel()
	c := New(t, func(s *State) {
		s.MinJobAge = 1
		s.Jobs = []*Job{
			{ID: 2, Name: "meridian-a1-1", UID: os.Getuid(), State: "COMPLETED", Reason: "None", End: time.Now().Add(-time.Hour)},
			{ID: 3, Name: "meridian-a2-1", UID: os.Getuid(), State: "PENDING", Reason: "Priority"},
			{ID: 4, Name: "other", UID: os.Getuid() + 1, State: "RUNNING", Reason: "None", Start: time.Now()},
		}
	})
	for _, tc := range []struct {
		args        []string
		out, errOut string
		code        int
	}{
		{[]string{"-h", "-t", "all", "-j", "2", "-o", "%i|%T|%r"}, "", "slurm_load_jobs error: Invalid job id specified\n", 1},
		{[]string{"-h", "-t", "all", "-j", "2,3", "-o", "%i|%T|%r"}, "3|PENDING|Priority\n", "", 0},
		{[]string{"-h", "-u", strconv.Itoa(os.Getuid()), "-t", "all", "-o", "%i|%j"}, "3|meridian-a2-1\n", "", 0},
		{[]string{"-h", "-j", "3", "-o", "%e"}, "N/A\n", "", 0},
	} {
		if out, errOut, code := run(t, c, "", "squeue", tc.args...); out != tc.out || errOut != tc.errOut || code != tc.code {
			t.Errorf("squeue %q: %q, %q, %d", tc.args, out, errOut, code)
		}
	}
}

func TestSacct(t *testing.T) {
	t.Parallel()
	c := New(t)
	if out, errOut, code := run(t, c, "", "sacct", "-X", "-n", "-P", "-j", "2", "-o", "JobIDRaw,State"); out != "" || errOut != "Slurm accounting storage is disabled\n" || code != 1 {
		t.Errorf("sacct without accounting: %q, %q, %d", out, errOut, code)
	}
	c.Update(t, func(s *State) {
		s.Accounting = true
		s.Jobs = []*Job{{ID: 2, Name: "x", UID: 7, State: "CANCELLED"}, {ID: 3, State: "COMPLETED"}}
	})
	if out, _, code := run(t, c, "", "sacct", "-X", "-n", "-P", "-j", "2", "-o", "JobIDRaw,State,ExitCode"); out != "2|CANCELLED by 7|0:0\n" || code != 0 {
		t.Errorf("sacct: %q, %d", out, code)
	}
}

func TestScontrol(t *testing.T) {
	t.Parallel()
	c := New(t, func(s *State) { s.Version = "21.08.5" })
	out, _, code := run(t, c, "", "scontrol", "show", "config")
	if code != 0 || !strings.Contains(out, "AccountingStorageType   = accounting_storage/none\n") || !strings.Contains(out, "SLURM_VERSION           = 21.08.5\n") {
		t.Errorf("scontrol show config: %q, %d", out, code)
	}
	if out, errOut, code := run(t, c, "", "scontrol", "show", "partition", "-o", "nosuch"); out != "" || errOut != "Partition nosuch not found\n" || code != 1 {
		t.Errorf("scontrol show partition of none: %q, %q, %d", out, errOut, code)
	}
	if out, _, _ := run(t, c, "", "sinfo", "-h", "-p", "debug", "-o", "%P|%a|%l"); out != "debug*|up|unlimited\n" {
		t.Errorf("sinfo: %q", out)
	}
}

// TestScripted checks that a command fails, and waits, as a test says.
func TestScripted(t *testing.T) {
	t.Parallel()
	c := New(t, func(s *State) {
		s.Fail = map[string][]Failure{"sbatch": {{Stderr: "sbatch: error: Batch job submission failed: Socket timed out on send/recv operation\n", Exit: 1}}}
		s.Hold = true
	})
	if out, errOut, code := run(t, c, "#!/bin/sh\n", "sbatch", "--parsable"); out != "" || code != 1 || !strings.Contains(errOut, "Socket timed out") {
		t.Errorf("the failing sbatch: %q, %q, %d", out, errOut, code)
	}
	if out, _, code := run(t, c, "#!/bin/sh\n", "sbatch", "--parsable"); out != "2\n" || code != 0 {
		t.Errorf("the next sbatch: %q, %d", out, code)
	}
	if j := c.Job(t, 2); j.State != "PENDING" || j.Reason != "Priority" {
		t.Errorf("the held job is %+v", j)
	}
	if calls := c.Calls(t, "sbatch"); len(calls) != 2 {
		t.Errorf("sbatch was called %d times", len(calls))
	}
}

// TestOptions checks the commands' long options, as the backend gives
// them, and what the fake models of hidden partitions, federations,
// --export and a job's nodes.
func TestOptions(t *testing.T) {
	t.Parallel()
	c := New(t, func(s *State) {
		s.Hold = true
		s.Partitions = append(s.Partitions, Partition{Name: "hidden", State: "UP", MaxTime: "UNLIMITED", DefaultTime: "NONE", Hidden: true})
	})
	run(t, c, "#!/bin/sh\n", "sbatch", "--parsable", "--job-name=a", "--nodes=2", "--ntasks=1", "--export=ALL,SECRET=")
	run(t, c, "#!/bin/sh\n", "sbatch", "--parsable", "--job-name=b", "--nodes=2", "--ntasks-per-node=1", "--partition=hidden")
	// Any --export but ALL also asks for the user's login environment.
	if j := c.Job(t, 2); j.Nodes != 1 || !slices.Contains(j.Env, "SECRET=") || !slices.Contains(j.Env, "SLURM_GET_USER_ENV=1") {
		t.Errorf("a job of one task on two nodes: %d nodes, environment %q", j.Nodes, j.Env)
	}
	if j := c.Job(t, 3); j.Nodes != 2 {
		t.Errorf("a job of a task on each of two nodes has %d nodes", j.Nodes)
	}
	uid := strconv.Itoa(os.Getuid())
	if out, _, _ := run(t, c, "", "squeue", "--noheader", "--user="+uid, "--states=all", "--format=%i|%j"); out != "2|a\n" {
		t.Errorf("squeue without --all: %q", out)
	}
	if out, _, _ := run(t, c, "", "squeue", "--noheader", "--all", "--user="+uid, "--states=all", "--format=%i|%j"); out != "2|a\n3|b\n" {
		t.Errorf("squeue --all: %q", out)
	}
	if out, _, _ := run(t, c, "", "squeue", "--noheader", "--jobs=2", "--format=%S|%e"); out != "N/A|N/A\n" {
		t.Errorf("a pending job's start and end: %q", out)
	}
	if out, _, _ := run(t, c, "", "scontrol", "--all", "--oneliner", "show", "partition"); strings.Count(out, "PartitionName=") != 2 {
		t.Errorf("scontrol --all: %q", out)
	}
	if out, _, code := run(t, c, "", "scontrol", "show", "federation"); out != "" || code != 0 {
		t.Errorf("a cluster of no federation: %q, %d", out, code)
	}
	c.Update(t, func(s *State) { s.Federation = "fed1" })
	if out, _, _ := run(t, c, "", "scontrol", "show", "federation"); !strings.HasPrefix(out, "Federation: fed1\n") {
		t.Errorf("a federation: %q", out)
	}
}
