//go:build linux || darwin || freebsd

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend/batch"
	localbackend "github.com/parallelworks/meridian/backend/local"
	"github.com/parallelworks/meridian/backend/slurm"
	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/internal/porttest"
	"github.com/parallelworks/meridian/internal/safepath"
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
	t.Parallel()
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

// TestBatchRelay checks that a job that can't reach the coordinator
// through a relay that is up, as at an address that refuses it, or one at
// which a server holds another key, stops before its worker starts, with
// scheduler_relay_unreachable and the addresses the file said last; and
// that one that completes a TLS handshake with the coordinator's key
// there, or at the addresses the file says once it is written again, as
// by a listener another process took over, or whose relay is down,
// starts it, with the address file.
func TestBatchRelay(t *testing.T) {
	t.Parallel()
	refused := "127.0.0.1:" + strconv.Itoa(porttest.Refusing(t))
	refusedToo := "127.0.0.1:" + strconv.Itoa(porttest.Refusing(t))
	coordinator, hash := tlsServer(t)
	other, _ := tlsServer(t)
	for _, tc := range []struct {
		name    string
		address string
		age     time.Duration
		// moveTo, if it is set, is where the file says the relay listens
		// once it is written again, as the job waits for it.
		moveTo  string
		started bool
	}{
		{name: "unreachable", address: refused},
		{name: "another key", address: other},
		{name: "reachable", address: coordinator, started: true},
		{name: "down", address: refused, age: 10 * time.Minute, started: true},
		{name: "moved", address: refused, moveTo: coordinator, started: true},
		{name: "moved, unreachable", address: refused, moveTo: refusedToo},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parallel(t)
			b := newBatchTest(t, "")
			file := filepath.Join(filepath.Dir(b.dir), "relay.json")
			write := func(address string) {
				data, err := (&workerapi.AddressFile{Addresses: []string{address}, Name: "r.meridian", Updated: time.Now().Unix()}).Marshal()
				if err == nil {
					err = safepath.WriteFile(file, data)
				}
				if err != nil {
					t.Error(err)
				}
			}
			write(tc.address)
			old := time.Now().Add(-tc.age)
			if err := os.Chtimes(file, old, old); err != nil {
				t.Fatal(err)
			}
			last := tc.address
			if tc.moveTo != "" {
				b.sys.relayWait, last = 3*time.Second, tc.moveTo
				time.AfterFunc(300*time.Millisecond, func() { write(tc.moveTo) })
			}
			b.spec.Coordinator = batch.Coordinator{AddressFile: file, KeyHash: hash}
			r := endedWithin(t, b.start(t))
			_, _, exit := b.records(t)
			if !tc.started {
				if r.code != exitError || exit == nil || exit.Problem == nil || exit.Problem.Code != problems.SchedulerRelayUnreachable.Code() ||
					exit.Problem.Params["node"] != "n1" || exit.Problem.Params["addresses"] != `"`+last+`"` {
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
	// A spec whose key hash isn't one starts no worker.
	b := newBatchTest(t, "")
	b.spec.Coordinator = batch.Coordinator{AddressFile: filepath.Join(filepath.Dir(b.dir), "relay.json"), KeyHash: "sha256:00"}
	if r := endedWithin(t, b.start(t)); r.code != exitError {
		t.Errorf("meridian batch with a spec whose key hash isn't one: %+v", r)
	}
}

// tlsServer serves TLS 1.3 on the loopback with a key of its own, as the
// coordinator does, completing each handshake and ending the connection;
// and returns its address, and its key's hash, as a spec says it.
func tlsServer(t *testing.T) (address, hash string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
	l, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			_ = c.(*tls.Conn).Handshake()
			c.Close()
		}
	}()
	return l.Addr().String(), "sha256:" + hex.EncodeToString(sum[:])
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
// its submissions. The slurm backend's driver is meridian run's own, but for
// the cluster's commands, which it runs and submits jobs with. Its
// workers reach the coordinator at this machine's private addresses: a
// test skips without one, but in CI.
func batchRun(t *testing.T, opts ...func(*schedtest.State)) (map[string]string, *schedtest.Cluster, func(*cli), string) {
	t.Helper()
	needPrivateAddress(t)
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
	setUp := func(cl *cli) {
		cl.schedulers = func(d *batch.Driver) {
			d.Scheduler = &slurm.Scheduler{Environ: c.Env(nil)}
			d.Environ = c.Env(nil)
		}
	}
	return env, c, setUp, filepath.Join(env["HOME"], ".local", "state", "meridian", "batch")
}

// needPrivateAddress skips a test whose scheduler's workers reach the
// coordinator at this machine's private addresses where it has none, but
// in CI, which must run it.
func needPrivateAddress(t *testing.T) {
	t.Helper()
	addrs, err := batch.InterfaceAddresses()
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(addrs, func(p netip.Prefix) bool { return p.Addr().IsPrivate() }) {
		return
	}
	if os.Getenv("CI") != "" {
		t.Fatal("this machine has no private address for a scheduler's workers to reach the coordinator at")
	}
	t.Skip("this machine has no private address for a scheduler's workers to reach the coordinator at")
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
// slurm backend, of a fake Slurm cluster on this machine, its login node:
// the driver copies meridian into the cache in the home the cluster's
// nodes share, listens for the member's workers on this machine's
// private addresses, and writes where to the member's address file there;
// it submits the job, whose script runs the copy's meridian batch, which
// starts meridian worker, which claims the attempt over TLS, at the
// addresses the file names, pinned to the coordinator's key, runs its
// step in the run's directory in that home, and completes it. The driver
// forgets the job's submission, and the address file goes with meridian
// run.
func TestBatchEndToEnd(t *testing.T) {
	parallel(t)
	env, cluster, setUp, dir := batchRun(t)
	release := filepath.Join(t.TempDir(), "release")
	workflow := writeFile(t, "w.yaml", fmt.Sprintf(`jobs:
  main:
    runs-on: {execution-group: hpc}
    steps:
      - run: |
          echo hello from $SLURM_JOB_ID in $PWD
          while [ ! -e %s ]; do sleep 0.05; done
`, quoted(t, release)))
	cmd := startCommand(t, env, setUp, "run", workflow)
	detail := func() string { return cmd.stdout.String() + cmd.stderr.String() + jobOutput(dir) }
	waitUntil(t, "the step to start", func() bool { return strings.Contains(cmd.stdout.String(), "main step 0| hello from 2") }, detail)
	specs, _ := filepath.Glob(filepath.Join(dir, "*", "1", batch.SpecFile))
	if len(specs) != 1 {
		t.Fatalf("the submissions' specs are %v", specs)
	}
	spec, err := batch.ReadSpec(filepath.Dir(specs[0]))
	if err != nil {
		t.Fatal(err)
	}
	co := spec.Coordinator
	if co.Address != "" || filepath.Dir(co.AddressFile) != filepath.Join(dir, batch.RelaysDir) || !strings.HasPrefix(co.KeyHash, "sha256:") {
		t.Errorf("the job reaches the coordinator at %+v", co)
	}
	data, err := os.ReadFile(co.AddressFile)
	if err != nil {
		t.Fatal(err)
	}
	f, err := workerapi.UnmarshalAddressFile(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range f.Addresses {
		if ap, err := netip.ParseAddrPort(a); err != nil || !ap.Addr().IsPrivate() {
			t.Errorf("the coordinator listens at %s", a)
		}
	}
	runs := filepath.Join(env["HOME"], ".local", "state", "meridian", "runs")
	if !strings.HasPrefix(spec.RunDir, runs+string(filepath.Separator)) || !strings.Contains(cmd.stdout.String(), "in "+spec.RunDir+"\n") {
		t.Errorf("the run's directory is %s, not in %s\n%s", spec.RunDir, runs, cmd.stdout.String())
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	r := endedWithin(t, cmd)
	if r.code != exitOK || !strings.HasSuffix(r.stdout, "run 1 finished: success\n") {
		t.Fatalf("meridian run: %+v\nthe job's output:\n%s", r, jobOutput(dir))
	}
	// The job ends as its worker does, which can be after the run's end.
	waitUntil(t, "the cluster's job to complete", func() bool { j := cluster.Job(t, 2); return j != nil && j.State == "COMPLETED" })
	j := cluster.Job(t, 2)
	binaries := filepath.Join(env["HOME"], ".cache", "meridian", "bin") + string(filepath.Separator)
	if !strings.HasPrefix(j.Name, "meridian-") || !strings.Contains(j.Script, "exec '"+binaries) {
		t.Errorf("the cluster's job is %s, of the script %q", j.Name, j.Script)
	}
	gone := func(path string) func() bool {
		return func() bool {
			_, err := os.Lstat(path)
			return errors.Is(err, fs.ErrNotExist)
		}
	}
	waitUntil(t, "the attempt's submissions to be forgotten", gone(filepath.Dir(filepath.Dir(specs[0]))))
	waitUntil(t, "the address file to go with meridian run", gone(co.AddressFile))
}

// addressFile reads the address file of the slurm backend's members of
// this machine in the home home, and returns what it says, or nil while
// there is none.
func addressFile(t *testing.T, home string) *workerapi.AddressFile {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(home, ".local", "state", "meridian", "batch", batch.RelaysDir, "*.json"))
	if len(files) == 0 {
		return nil
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		return nil
	}
	f, err := workerapi.UnmarshalAddressFile(data)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// TestBatchResumeKilled checks that a job on a slurm member goes on as its
// meridian run is killed, its step running, and that meridian resume takes
// it up: it takes the member's listener over, at another port, since the
// test holds the old one, as another user could, where its connections
// end at once; writes it to the address file; and the job's worker, which
// reads the file again before each connection it makes, reaches it there,
// delivers what its step wrote meanwhile, and completes the job, whose one
// attempt and one batch job run it once.
func TestBatchResumeKilled(t *testing.T) {
	parallel(t)
	needPrivateAddress(t)
	env := runEnv(t)
	cluster := schedtest.New(t)
	for _, kv := range cluster.Env(nil) {
		if name, value, _ := strings.Cut(kv, "="); name == "PATH" || name == schedtest.EnvState {
			env[name] = value
		}
	}
	if err := os.MkdirAll(env[local.EnvConfigDir], 0o700); err != nil {
		t.Fatal(err)
	}
	groups := "hpc:\n  members:\n    - {name: cluster, backend: slurm, isolation: host, settings: {partition: debug}}\n"
	if err := os.WriteFile(filepath.Join(env[local.EnvConfigDir], local.GroupsFile), []byte(groups), 0o600); err != nil {
		t.Fatal(err)
	}
	release := filepath.Join(t.TempDir(), "release")
	workflow := writeFile(t, "w.yaml", fmt.Sprintf(`jobs:
  main:
    runs-on: {execution-group: hpc}
    steps:
      - run: |
          echo one
          while [ ! -e %s ]; do sleep 0.05; done
          echo two
`, quoted(t, release)))
	killed := meridianProcess(t, env, "run", "--format", "json", workflow)
	dir := filepath.Join(env["HOME"], ".local", "state", "meridian", "batch")
	waitUntil(t, "the step to start", func() bool { return strings.Contains(killed.stdout.String(), `"kind":"step.started"`) },
		func() string { return killed.stdout.String() + killed.stderr.String() + jobOutput(dir) })
	before := addressFile(t, env["HOME"])
	if before == nil {
		t.Fatal("no address file as the job runs")
	}
	if err := killed.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = killed.cmd.Wait()
	// The old ports are another's now, which ends each connection.
	for _, a := range before.Addresses {
		l, err := net.Listen("tcp", a)
		if err != nil {
			continue
		}
		t.Cleanup(func() { l.Close() })
		go func() {
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				c.Close()
			}
		}()
	}
	resumed := meridianProcess(t, env, "resume", "1")
	waitUntil(t, "the address file to name another port", func() bool {
		f := addressFile(t, env["HOME"])
		return f != nil && f.Updated >= before.Updated && !slices.Equal(f.Addresses, before.Addresses)
	}, func() string { return resumed.stdout.String() + resumed.stderr.String() + jobOutput(dir) })
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := resumed.cmd.Wait(); err != nil {
		t.Fatalf("meridian resume 1: %v\n%s%s\n%s", err, resumed.stdout.String(), resumed.stderr.String(), jobOutput(dir))
	}
	if out := resumed.stdout.String(); !strings.Contains(out, "main step 0| two\n") || !strings.HasSuffix(out, "run 1 finished: success\n") {
		t.Errorf("meridian resume showed\n%s%s", out, resumed.stderr.String())
	}
	if log := stepLog(t, env, "1"); log != "one\ntwo\n" {
		t.Errorf("the step's log is %q", log)
	}
	waitUntil(t, "the cluster's job to complete", func() bool { j := cluster.Job(t, 2); return j != nil && j.State == "COMPLETED" })
	if cluster.Job(t, 3) != nil {
		t.Errorf("the cluster's jobs are %+v", cluster.State(t).Jobs)
	}
}

// TestBatchPrivate checks a job whose group a variable names, which makes
// where it runs private, on a slurm member: the group and the member are
// never in the clear, in what meridian run shows, or meridian status or
// logs, or in the state directory, and the run's directory the member
// chose isn't in the store's; and a job that no member can run, as one
// whose time limit is longer than the member allows, fails with the
// member's reason, masked.
func TestBatchPrivate(t *testing.T) {
	parallel(t)
	env, _, setUp, dir := batchRun(t)
	const group, member = "h1dd3n", "m3mb3r"
	config := env[local.EnvConfigDir]
	groups := group + ":\n  members:\n    - {name: " + member + ", backend: slurm, isolation: host, max-walltime: 1h, settings: {partition: debug}}\n"
	if err := os.WriteFile(filepath.Join(config, local.GroupsFile), []byte(groups), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config, local.VariablesFile), []byte("vars:\n  GROUP: "+group+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	job := "jobs:\n  main:\n    runs-on: {execution-group: \"${{ vars.GROUP }}\"}\n%s    steps:\n      - run: echo in $PWD\n"
	hides := func(what, out string, secret ...string) {
		t.Helper()
		for _, s := range append(secret, group, member) {
			if strings.Contains(out, s) {
				t.Errorf("%s shows %q:\n%s", what, s, out)
			}
		}
	}
	r := endedWithin(t, startCommand(t, env, setUp, "run", writeFile(t, "w.yaml", fmt.Sprintf(job, ""))))
	if r.code != exitOK || !strings.HasSuffix(r.stdout, "run 1 finished: success\n") {
		t.Fatalf("meridian run: %+v\n%s", r, jobOutput(dir))
	}
	hides("meridian run", r.stdout+r.stderr)
	r = endedWithin(t, startCommand(t, env, setUp, "run", writeFile(t, "long.yaml", fmt.Sprintf(job, "    timeout: 1h\n"))))
	if r.code != exitInvalid || !strings.Contains(r.stdout+r.stderr, string(problems.WalltimeExceedsMember.Code())) {
		t.Fatalf("meridian run of a job longer than its member allows: %+v", r)
	}
	hides("meridian run of a job longer than its member allows", r.stdout+r.stderr)
	for _, args := range [][]string{{"logs", "1"}, {"status", "-all"}, {"status", "--format", "json", "1"}, {"status", "--format", "json", "2"}} {
		r := commandTest(t, env, args...)
		hides("meridian "+strings.Join(args, " "), r.stdout+r.stderr)
	}
	if logs := commandTest(t, env, "logs", "1"); !strings.Contains(logs.stdout, "| in "+env["HOME"]) {
		t.Errorf("the step didn't run in the home: %+v", logs)
	}
	if err := filepath.WalkDir(env[local.EnvStateDir], func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Type()&fs.ModeSocket != 0 {
			return err
		}
		data, err := os.ReadFile(path) //nolint:gosec // a test's
		if err != nil {
			return err
		}
		if strings.HasPrefix(d.Name(), local.DatabaseFile) {
			hides(path, string(data), filepath.Join(".local", "state", "meridian", "runs"))
		} else {
			hides(path, string(data))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
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
	waitUntil(t, "the cluster's job to complete", func() bool { j := cluster.Job(t, 2); return j != nil && j.State == "COMPLETED" })
	if cluster.Job(t, 3) != nil {
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
