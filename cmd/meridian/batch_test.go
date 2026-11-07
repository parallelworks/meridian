//go:build linux || darwin || freebsd

package main

import (
	"crypto/rand"
	jsonv2 "encoding/json/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/backend/batch"
	localbackend "github.com/parallelworks/meridian/backend/local"
	"github.com/parallelworks/meridian/backend/slurm"
	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/internal/porttest"
	"github.com/parallelworks/meridian/internal/schedtest"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// A batchTest is a submission's directory, and what meridian batch run in
// it is given.
type batchTest struct {
	dir    string
	spec   *batch.Spec
	sys    batchSystem
	vars   map[string]string
	report string
	// events is the job's cgroup's memory.events.
	events  string
	signals chan os.Signal
	end     time.Time
}

// newBatchTest makes a submission's directory, of an attempt whose spec
// names a coordinator's socket, whose token is in its file, of a job whose
// worker is the tests' stub worker, which reports what it was given to
// report and does as mode says (hostStub).
func newBatchTest(t *testing.T, mode string) *batchTest {
	t.Helper()
	root := t.TempDir()
	_, digest, err := localbackend.Executable()
	if err != nil {
		t.Fatal(err)
	}
	b := &batchTest{
		dir:  filepath.Join(root, "sub"),
		spec: &batch.Spec{Attempt: "a1", Submission: 1, Scheduler: "slurm", Digest: digest, Coordinator: batch.Coordinator{Address: "/run/c.sock"}, MarginSeconds: 300},
		end:  time.Now().Add(time.Hour).Truncate(time.Second), report: filepath.Join(root, "report"), signals: make(chan os.Signal, 4),
	}
	if err := os.Mkdir(b.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(b.dir, batch.TokenFile), []byte("the-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	// The job's memory is limited at the job's cgroup, which its steps'
	// are under.
	cgroups := filepath.Join(root, "cgroup")
	b.events = filepath.Join(cgroups, "job_12", "memory.events")
	if err := os.MkdirAll(filepath.Join(cgroups, "job_12", "step_batch"), 0o700); err != nil {
		t.Fatal(err)
	}
	// Other processes of the job's were killed for their memory before.
	if err := os.WriteFile(b.events, []byte("low 0\nhigh 0\nmax 3\noom 2\noom_kill 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	environ := []string{
		"PATH=" + os.Getenv("PATH"), "GODEBUG=fips140=off", "MERIDIAN_STATE_DIR=/x", "KEEP=1",
		hostStubEnv + "=" + b.report, hostStubModeEnv + "=" + mode,
	}
	b.sys = batchSystem{
		harden: func() error { return nil }, executable: localbackend.Runnable,
		environ: func() []string { return environ }, hostname: func() (string, error) { return "n1", nil },
		cgroup:     func() (string, error) { return "0::/job_12/step_batch\n", nil },
		cgroupRoot: cgroups, relayWait: 300 * time.Millisecond, relayFresh: 3 * time.Minute, tokenWait: time.Minute,
	}
	b.vars = map[string]string{"SLURM_JOB_ID": "12", "SLURM_JOB_END_TIME": strconv.FormatInt(b.end.Unix(), 10)}
	return b
}

// start runs meridian batch in the directory, once its spec is written.
func (b *batchTest) start(t *testing.T) *commandRun {
	t.Helper()
	if err := batch.WriteRecord(b.dir, batch.SpecFile, b.spec); err != nil {
		t.Fatal(err)
	}
	return startCommand(t, b.vars, func(c *cli) { c.batch, c.signals = &b.sys, b.signals }, "batch", b.dir)
}

// stub waits for the stub worker's report.
func (b *batchTest) stub(t *testing.T) hostStubReport {
	t.Helper()
	var r hostStubReport
	waitUntil(t, "the worker to report", func() bool {
		data, err := os.ReadFile(b.report)
		return err == nil && jsonv2.Unmarshal(data, &r) == nil
	})
	return r
}

// records reads the directory's records.
func (b *batchTest) records(t *testing.T) (*batch.Started, *batch.Term, *batch.Exit) {
	t.Helper()
	read := func(name string, v any) bool {
		data, err := os.ReadFile(filepath.Join(b.dir, name))
		if err != nil {
			return false
		}
		if err := jsonv2.Unmarshal(data, v); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return true
	}
	started, term, exit := new(batch.Started), new(batch.Term), new(batch.Exit)
	if !read(batch.StartedFile, started) {
		started = nil
	}
	if !read(batch.TermFile, term) {
		term = nil
	}
	if !read(batch.ExitFile, exit) {
		exit = nil
	}
	return started, term, exit
}

func TestBatch(t *testing.T) {
	parallel(t)
	b := newBatchTest(t, "")
	r := endedWithin(t, b.start(t))
	if r.code != 0 {
		t.Fatalf("meridian batch: %+v", r)
	}
	stub := b.stub(t)
	if stub.Token != "the-token" || stub.Coordinator != "/run/c.sock" || len(stub.leaks("the-token")) > 0 || !slices.Equal(stub.Args, []string{"worker"}) {
		t.Errorf("the worker was given %+v", stub)
	}
	// Half the margin before the end: the first half started the worker.
	deadline := strconv.FormatInt(b.end.Add(-150*time.Second).Unix(), 10)
	for _, kv := range []string{"KEEP=1", workerapi.EnvDeadline + "=" + deadline, workerapi.EnvBootstrapTokenFD + "=3"} {
		if !slices.Contains(stub.Env, kv) {
			t.Errorf("the worker's environment lacks %s", kv)
		}
	}
	for _, name := range []string{"GODEBUG", "MERIDIAN_STATE_DIR", workerapi.EnvKeySHA256, workerapi.EnvCoordinatorFile} {
		if slices.ContainsFunc(stub.Env, func(kv string) bool { return strings.HasPrefix(kv, name+"=") }) {
			t.Errorf("the worker's environment has %s", name)
		}
	}
	if _, err := os.Lstat(filepath.Join(b.dir, batch.TokenFile)); err == nil {
		t.Error("the token's file is there")
	}
	started, term, exit := b.records(t)
	if started == nil || started.Job != "12" || started.Node != "n1" || !started.End.Equal(b.end) || started.PID != os.Getpid() {
		t.Errorf("started.json: %+v", started)
	}
	if term != nil || exit == nil || exit.Status != 0 || exit.Signal != 0 || exit.OOMKills != 0 || exit.Problem != nil {
		t.Errorf("term.json %+v, exit.json %+v", term, exit)
	}
}

// TestBatchTerm checks that the scheduler's SIGTERM is passed on to the
// worker, and recorded.
func TestBatchTerm(t *testing.T) {
	parallel(t)
	b := newBatchTest(t, "wait")
	cmd := b.start(t)
	b.stub(t)
	sent := time.Now()
	b.signals <- syscall.SIGTERM
	r := endedWithin(t, cmd)
	_, term, exit := b.records(t)
	if r.code != 0 || term == nil || term.Signal != int(syscall.SIGTERM) || term.At.Before(sent.Add(-time.Second)) || exit == nil || exit.Status != 0 {
		t.Errorf("%+v: term.json %+v, exit.json %+v", r, term, exit)
	}
}

// TestBatchKilled checks that a worker the kernel killed for its memory is
// recorded as one killed, with how many processes the kernel killed for
// memory while it ran.
func TestBatchKilled(t *testing.T) {
	parallel(t)
	b := newBatchTest(t, "wait")
	cmd := b.start(t)
	stub := b.stub(t)
	if err := os.WriteFile(b.events, []byte("low 0\nhigh 0\nmax 9\noom 4\noom_kill 3\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(stub.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	r := endedWithin(t, cmd)
	_, _, exit := b.records(t)
	if r.code != 128+int(syscall.SIGKILL) || exit == nil || exit.Status != -1 || exit.Signal != int(syscall.SIGKILL) || exit.OOMKills != 1 {
		t.Errorf("%+v: exit.json %+v", r, exit)
	}
}

// TestBatchNotStarted checks that meridian batch records why it didn't
// start the worker, and removes the token, which no worker takes now.
func TestBatchNotStarted(t *testing.T) {
	parallel(t)
	for _, tc := range []struct {
		name  string
		setUp func(*testing.T, *batchTest)
		// want is the problem recorded, or nil for none.
		want *problems.Kind
	}{
		{
			name: "unsafe binary", want: problems.WorkerBinaryUnsafe,
			setUp: func(t *testing.T, b *batchTest) {
				b.sys.executable = func() (string, string, string, error) {
					return "", "", "", problems.WorkerBinaryUnsafe.Problem(problems.Params{"path": "/x", "code": "file_changeable"})
				}
			},
		},
		{
			name: "no hardening", want: problems.WorkerHardenFailed,
			setUp: func(t *testing.T, b *batchTest) { b.sys.harden = func() error { return syscall.EPERM } },
		},
		{
			name: "another binary", want: problems.WorkerDigestMismatch,
			setUp: func(t *testing.T, b *batchTest) { b.spec.Digest = "sha256:" + strings.Repeat("0", 64) },
		},
		{
			name: "withdrawn",
			setUp: func(t *testing.T, b *batchTest) {
				t.Helper()
				if err := batch.WriteRecord(b.dir, batch.WithdrawnFile, &batch.Withdrawn{At: time.Now()}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "token not taken", want: problems.WorkerTokenNotTaken,
			setUp: func(t *testing.T, b *batchTest) {
				b.sys.executable = func() (string, string, string, error) { return "/bin/true", "/bin/true", b.spec.Digest, nil }
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parallel(t)
			b := newBatchTest(t, "")
			tc.setUp(t, b)
			r := endedWithin(t, b.start(t))
			_, _, exit := b.records(t)
			if r.code != exitError || exit == nil || exit.Status != -1 || tc.want == nil && exit.Problem != nil ||
				tc.want != nil && (exit.Problem == nil || exit.Problem.Code != tc.want.Code()) {
				t.Errorf("%+v: exit.json %+v", r, exit)
			}
			if _, err := os.Lstat(filepath.Join(b.dir, batch.TokenFile)); err == nil {
				t.Error("the token's file is there")
			}
			if _, err := os.Lstat(b.report); err == nil && tc.want != problems.WorkerTokenNotTaken {
				t.Error("a worker started")
			}
		})
	}
}

// TestBatchStoppedEarly checks that a signal that comes before the worker
// starts stops the job there: the relay's check ends without saying it
// is unreachable, the token goes, and the worker never starts.
func TestBatchStoppedEarly(t *testing.T) {
	parallel(t)
	b := newBatchTest(t, "")
	file := filepath.Join(filepath.Dir(b.dir), "relay.json")
	refused := "127.0.0.1:" + strconv.Itoa(porttest.Refusing(t))
	data, err := (&workerapi.AddressFile{Addresses: []string{refused}, Name: "r.meridian", Updated: time.Now().Unix()}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, data, 0o600); err != nil {
		t.Fatal(err)
	}
	b.spec.Coordinator = batch.Coordinator{AddressFile: file, KeyHash: "sha256:" + strings.Repeat("0", 64)}
	b.sys.relayWait = time.Minute
	cmd := b.start(t)
	waitUntil(t, "started.json", func() bool {
		_, err := os.Lstat(filepath.Join(b.dir, batch.StartedFile))
		return err == nil
	})
	b.signals <- syscall.SIGTERM
	r := endedWithin(t, cmd)
	_, term, exit := b.records(t)
	if r.code != 128+int(syscall.SIGTERM) || term == nil || exit == nil || exit.Problem != nil || exit.Status != -1 {
		t.Errorf("%+v: term.json %+v, exit.json %+v", r, term, exit)
	}
	if _, err := os.Lstat(filepath.Join(b.dir, batch.TokenFile)); err == nil {
		t.Error("the token's file is there")
	}
	if _, err := os.Lstat(b.report); err == nil {
		t.Error("a worker started")
	}
}

// TestBatchStoppedInHandoff checks that a signal that comes as the worker
// starts, before it takes its token, stops the job, rather than say the
// worker didn't take its token.
func TestBatchStoppedInHandoff(t *testing.T) {
	parallel(t)
	b := newBatchTest(t, "")
	// A worker that starts, says so, and never asks for its token.
	worker := filepath.Join(t.TempDir(), "worker")
	if err := os.WriteFile(worker, []byte("#!/bin/sh\ntouch \"$0.started\"\nexec sleep 60\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	b.sys.executable = func() (string, string, string, error) { return worker, worker, b.spec.Digest, nil }
	cmd := b.start(t)
	waitUntil(t, "the worker to start", func() bool {
		_, err := os.Lstat(worker + ".started")
		return err == nil
	})
	b.signals <- syscall.SIGTERM
	r := endedWithin(t, cmd)
	_, term, exit := b.records(t)
	if r.code != 128+int(syscall.SIGTERM) || term == nil || exit == nil || exit.Problem != nil {
		t.Errorf("%+v: term.json %+v, exit.json %+v", r, term, exit)
	}
}

// TestBatchAllocationEnd checks the deadline where the scheduler says when
// the job started, but not when it ends: its start and its walltime; and
// that there is none, nor an end in started.json, where it says neither.
func TestBatchAllocationEnd(t *testing.T) {
	parallel(t)
	b := newBatchTest(t, "")
	start := time.Now().Add(-time.Minute).Truncate(time.Second)
	b.vars = map[string]string{"SLURM_JOB_ID": "12", "SLURM_JOB_START_TIME": strconv.FormatInt(start.Unix(), 10)}
	b.spec.WalltimeSeconds = 600
	// No squeue: the scheduler can't be asked.
	b.sys.environ = func() []string { return []string{"PATH=" + t.TempDir(), hostStubEnv + "=" + b.report} }
	if r := endedWithin(t, b.start(t)); r.code != 0 {
		t.Fatalf("%+v", r)
	}
	deadline := workerapi.EnvDeadline + "=" + strconv.FormatInt(start.Add(600*time.Second-150*time.Second).Unix(), 10)
	if stub := b.stub(t); !slices.Contains(stub.Env, deadline) {
		t.Errorf("the worker's environment lacks %s: %q", deadline, stub.Env)
	}
	if started, _, _ := b.records(t); !started.End.Equal(start.Add(600 * time.Second)) {
		t.Errorf("started.json's end is %v", started.End)
	}

	b = newBatchTest(t, "")
	b.vars = map[string]string{"SLURM_JOB_ID": "12"}
	b.spec.WalltimeSeconds = 600
	b.sys.environ = func() []string { return []string{"PATH=" + t.TempDir(), hostStubEnv + "=" + b.report} }
	if r := endedWithin(t, b.start(t)); r.code != 0 {
		t.Fatalf("%+v", r)
	}
	if stub := b.stub(t); slices.ContainsFunc(stub.Env, func(kv string) bool { return strings.HasPrefix(kv, workerapi.EnvDeadline+"=") }) {
		t.Errorf("a deadline of an allocation whose end can't be told: %q", stub.Env)
	}
	if started, _, _ := b.records(t); !started.End.IsZero() {
		t.Errorf("started.json's end is %v", started.End)
	}
}

// TestBatchRelay checks that a job that can't reach a relay that is up
// stops before its worker starts, with scheduler_relay_unreachable; and
// that one that can, or whose relay is down, starts it, with the address
// file.
func TestBatchRelay(t *testing.T) {
	parallel(t)
	refused := "127.0.0.1:" + strconv.Itoa(porttest.Refusing(t))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	for _, tc := range []struct {
		name    string
		address string
		age     time.Duration
		started bool
	}{
		{"unreachable", refused, 0, false},
		{"reachable", listener.Addr().String(), 0, true},
		{"down", refused, 10 * time.Minute, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parallel(t)
			b := newBatchTest(t, "")
			file := filepath.Join(filepath.Dir(b.dir), "relay.json")
			data, err := (&workerapi.AddressFile{Addresses: []string{tc.address}, Name: "r.meridian", Updated: time.Now().Unix()}).Marshal()
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(file, data, 0o600); err != nil {
				t.Fatal(err)
			}
			old := time.Now().Add(-tc.age)
			if err := os.Chtimes(file, old, old); err != nil {
				t.Fatal(err)
			}
			b.spec.Coordinator = batch.Coordinator{AddressFile: file, KeyHash: "sha256:" + strings.Repeat("0", 64)}
			r := endedWithin(t, b.start(t))
			_, _, exit := b.records(t)
			if !tc.started {
				if r.code != exitError || exit == nil || exit.Problem == nil || exit.Problem.Code != problems.SchedulerRelayUnreachable.Code() ||
					exit.Problem.Params["node"] != "n1" || exit.Problem.Params["addresses"] != `"`+refused+`"` {
					t.Errorf("%+v: exit.json %+v", r, exit)
				}
				return
			}
			stub := b.stub(t)
			if r.code != 0 || !slices.Contains(stub.Env, workerapi.EnvCoordinatorFile+"="+file) || stub.Coordinator != "" {
				t.Errorf("%+v: the worker's environment is %q", r, stub.Env)
			}
		})
	}
}

func TestBatchArguments(t *testing.T) {
	parallel(t)
	for _, args := range [][]string{{"batch"}, {"batch", "relative"}, {"batch", "/a", "/b"}} {
		if r := commandTest(t, nil, args...); r.code != exitError || !strings.Contains(r.stderr, "(arguments_invalid)") {
			t.Errorf("meridian %q: %+v", args, r)
		}
	}
	// A directory other users can read isn't the driver's: nothing is
	// written to it.
	b := newBatchTest(t, "")
	if err := os.Chmod(b.dir, 0o755); err != nil {
		t.Fatal(err)
	}
	r := endedWithin(t, b.start(t))
	if _, _, exit := b.records(t); r.code != exitError || exit != nil || !strings.Contains(r.stderr, "(directory_not_private)") {
		t.Errorf("%+v, exit.json %+v", r, exit)
	}
}

func TestOOMKills(t *testing.T) {
	parallel(t)
	root := t.TempDir()
	write := func(rel, data string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Join(root, filepath.Dir(rel)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, rel), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("system.slice/n1_slurmstepd.scope/job_2/memory.events", "oom 2\noom_kill 2\noom_group_kill 0\n")
	write("memory/slurm/uid_1001/job_3/memory.oom_control", "oom_kill_disable 0\nunder_oom 0\noom_kill 4\n")
	// Above a job's cgroup, which another job's are under, nothing counts.
	write("system.slice/memory.events", "oom_kill 9\n")
	write("memory/slurm/memory.oom_control", "oom_kill 9\n")
	// What each release's batch step said of its cgroup, as its job 2's:
	// 21.08's, without cgroups, none (backend/slurm/testdata).
	for release, want := range map[string]int64{"21.08.5": 0, "23.11.4": 2, "23.11.4-accounting": 2, "24.11.5": 2} {
		cgroup, err := os.ReadFile(filepath.Join("../../backend/slurm/testdata", release, "05-job-cgroup.txt"))
		if err != nil {
			t.Fatal(err)
		}
		if got := oomKills(string(cgroup), root, "2"); got != want {
			t.Errorf("%s's cgroup: %d, want %d", release, got, want)
		}
	}
	for _, tc := range []struct {
		cgroup, job string
		want        int64
	}{
		{"12:memory:/slurm/uid_1001/job_3/step_batch\n11:cpu,cpuacct:/slurm\n", "3", 4},
		{"0::/system.slice/n1_slurmstepd.scope/job_4/step_batch\n", "4", 0},
		{"12:memory:/slurm/uid_1001/job_5/step_batch\n", "5", 0},
		{"0::/system.slice/n1_slurmstepd.scope/job_2/step_batch\n", "", 0},
		{"0::/\n", "2", 0},
		{"0::/elsewhere\n", "2", 0},
		{"", "2", 0},
		{"0::/system.slice/job_2/../../x\n", "2", 0},
	} {
		if got := oomKills(tc.cgroup, root, tc.job); got != tc.want {
			t.Errorf("oomKills(%q, job %s) = %d, want %d", tc.cgroup, tc.job, got, tc.want)
		}
	}
	// The cgroup filesystem's root is a path, not a prefix.
	if got := oomKills("0::/system.slice/n1_slurmstepd.scope/job_2/step_batch\n", root+"X", "2"); got != 0 {
		t.Errorf("another root's: %d", got)
	}
}

// batchRun sets up meridian run with a group, hpc, of one member of the
// slurm backend, cluster, of a fake Slurm cluster that opts set up, and a
// group, hpc-here, of that member and then one of the local backend,
// here, which offers a job one CPU; and returns its environment, the
// cluster, what sets meridian run up to drive it, and the directory of
// its submissions.
func batchRun(t *testing.T, opts ...func(*schedtest.State)) (map[string]string, *schedtest.Cluster, func(*cli), string) {
	t.Helper()
	env := runEnv(t)
	c := schedtest.New(t, opts...)
	config := env[local.EnvConfigDir]
	if err := os.MkdirAll(config, 0o700); err != nil {
		t.Fatal(err)
	}
	cluster := "    - {name: cluster, backend: slurm, isolation: host, settings: {partition: debug}}\n"
	groups := "hpc:\n  members:\n" + cluster + "hpc-here:\n  members:\n" + cluster + "    - {name: here, backend: local, isolation: host, resources: {cpu: 1}}\n"
	if err := os.WriteFile(filepath.Join(config, local.GroupsFile), []byte(groups), 0o600); err != nil {
		t.Fatal(err)
	}
	key := make([]byte, batch.MinKeyLen)
	_, _ = rand.Read(key)
	dir := filepath.Join(env["HOME"], "batch")
	d := &batch.Driver{
		Scheduler: &slurm.Scheduler{Environ: c.Env(nil)}, Dir: dir, Key: key,
		Binary: localbackend.Executable, Environ: c.Env(nil),
	}
	return env, c, func(c *cli) { c.drivers = map[string]backend.Driver{"slurm": d} }, dir
}

// jobOutput returns what the cluster's jobs wrote, for a test that fails.
func jobOutput(dir string) string {
	outs, _ := filepath.Glob(filepath.Join(dir, "*", "*", batch.OutFile))
	var b strings.Builder
	for _, o := range outs {
		data, _ := os.ReadFile(o) //nolint:gosec // a test's
		b.Write(data)
	}
	return b.String()
}

// TestBatchEndToEnd checks that meridian run runs a job on a member of the
// slurm backend, of a fake Slurm cluster on this machine: the driver
// submits the job, whose script runs meridian batch, which starts
// meridian worker, which claims the attempt at the coordinator's socket,
// runs its step and completes it.
func TestBatchEndToEnd(t *testing.T) {
	parallel(t)
	env, cluster, setUp, dir := batchRun(t)
	workflow := writeFile(t, "w.yaml", "jobs:\n  main:\n    runs-on: {execution-group: hpc}\n    steps:\n      - run: echo hello from $SLURM_JOB_ID\n")
	r := endedWithin(t, startCommand(t, env, setUp, "run", workflow))
	if r.code != exitOK || !strings.Contains(r.stdout, "hello from 2") {
		t.Fatalf("meridian run: %+v\nthe job's output:\n%s", r, jobOutput(dir))
	}
	if j := cluster.Job(t, 2); j == nil || j.State != "COMPLETED" || !strings.HasPrefix(j.Name, "meridian-") {
		t.Errorf("the cluster's job is %+v\n%s", j, jobOutput(dir))
	}
}

// TestBatchCaller checks that a job that calls a workflow runs off the
// scheduler, on the next member of its group, here, since it would hold
// its allocation while the run it calls ran; while the job of the run it
// calls, which calls none, and asks for more CPUs than here offers, runs
// on the scheduler's member, in the caller's group. A caller whose group
// has only the scheduler's member fails as it is placed, before any of its
// code runs, and nothing is submitted.
func TestBatchCaller(t *testing.T) {
	parallel(t)
	env, cluster, setUp, dir := batchRun(t)
	sources := &conformancetest.MapSources{Texts: map[string]string{"github/t/leaf": "jobs:\n  leaf:\n    resources: {cpu: 2}\n    steps:\n      - run: echo leaf in job $SLURM_JOB_ID\n"}}
	withSources := func(c *cli) { setUp(c); c.sources = sources }
	caller := func(group string) string {
		return writeFile(t, group+".yaml", "jobs:\n  main:\n    runs-on: {execution-group: "+group+"}\n    steps:\n"+
			"      - run: echo main in job ${SLURM_JOB_ID:-none}\n      - uses: github/t/leaf\n        with: {$yaml: w.yaml}\n")
	}
	r := endedWithin(t, startCommand(t, env, withSources, "run", caller("hpc-here")))
	if r.code != exitOK || !strings.Contains(r.stdout, "main step 0| main in job none\n") || !strings.Contains(r.stdout, "main step 1 › leaf step 0| leaf in job 2\n") {
		t.Fatalf("meridian run of a caller in hpc-here: %+v\nthe jobs' output:\n%s", r, jobOutput(dir))
	}
	if j := cluster.Job(t, 2); j == nil || j.State != "COMPLETED" || cluster.Job(t, 3) != nil {
		t.Errorf("the cluster's jobs are %+v", cluster.State(t).Jobs)
	}
	r = endedWithin(t, startCommand(t, env, withSources, "run", caller("hpc")))
	if r.code != exitInvalid || !strings.Contains(r.stdout+r.stderr, string(problems.SchedulerCallerRefused.Code())) || strings.Contains(r.stdout, "main step 0") {
		t.Errorf("meridian run of a caller in hpc: %+v", r)
	}
	if cluster.Job(t, 3) != nil {
		t.Errorf("the caller was submitted: %+v", cluster.Job(t, 3))
	}
}

// TestBatchScancel checks that a job its scheduler cancels, which signals
// every process of it, meridian batch and its worker among them, is
// stopped as the worker stops a job: its step gets SIGTERM, and what its
// handler writes reaches the coordinator; and the worker ends of itself,
// though meridian batch passes it the signal again, which would kill a
// command that caught only the first.
func TestBatchScancel(t *testing.T) {
	parallel(t)
	env, cluster, setUp, dir := batchRun(t, func(s *schedtest.State) { s.KillWait = 60 })
	workflow := writeFile(t, "w.yaml", `jobs:
  main:
    runs-on: {execution-group: hpc}
    steps:
      - run: |
          trap 'echo got TERM; exit 0' TERM
          echo ready
          sleep 60 & wait
`)
	cmd := startCommand(t, env, setUp, "run", workflow)
	waitUntil(t, "the step to start", func() bool { return strings.Contains(cmd.stdout.String(), "main step 0| ready") },
		func() string { return cmd.stdout.String() + cmd.stderr.String() + jobOutput(dir) })
	scancel := exec.CommandContext(t.Context(), filepath.Join(cluster.Bin, "scancel"), "2")
	scancel.Env = cluster.Env(nil)
	if out, err := scancel.CombinedOutput(); err != nil {
		t.Fatalf("scancel: %v: %s", err, out)
	}
	waitUntil(t, "the step's handler's line", func() bool { return strings.Contains(cmd.stdout.String(), "main step 0| got TERM") },
		func() string { return cmd.stdout.String() + cmd.stderr.String() + jobOutput(dir) })
	var exit batch.Exit
	waitUntil(t, "meridian batch's exit record", func() bool {
		files, _ := filepath.Glob(filepath.Join(dir, "*", "1", batch.ExitFile))
		if len(files) != 1 {
			return false
		}
		data, err := os.ReadFile(files[0])
		return err == nil && jsonv2.Unmarshal(data, &exit) == nil
	})
	if exit.Signal != 0 {
		t.Errorf("the worker was ended by signal %d\n%s", exit.Signal, jobOutput(dir))
	}
	if r := commandTest(t, env, "cancel", "1"); r.code != exitOK {
		t.Errorf("meridian cancel: %+v", r)
	}
	endedWithin(t, cmd)
}
