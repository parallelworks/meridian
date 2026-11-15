//go:build unix

package slurm

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/backend/batch"
	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/internal/schedtest"
	"github.com/parallelworks/meridian/problems"
)

// envArgs has TestMain, run as meridian batch by a job's script, write its
// arguments to the file it names.
const envArgs = "SLURMTEST_ARGS"

func TestMain(m *testing.M) {
	schedtest.Main()
	if file := os.Getenv(envArgs); file != "" && len(os.Args) > 1 && os.Args[1] == "batch" {
		data, _ := jsonv2.Marshal(os.Args[1:])
		_ = os.WriteFile(file, data, 0o600)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// newScheduler returns a Scheduler of a fake cluster that opts set up.
func newScheduler(t *testing.T, opts ...func(*schedtest.State)) (*Scheduler, *schedtest.Cluster) {
	t.Helper()
	c := schedtest.New(t, opts...)
	return &Scheduler{Environ: c.Env(nil)}, c
}

func TestInfo(t *testing.T) {
	t.Parallel()
	s, _ := newScheduler(t)
	info, err := s.Info(t.Context())
	if err != nil || *info != (batch.Info{Version: "23.11.4", Cluster: "fixture"}) {
		t.Errorf("Info: %+v, %v", info, err)
	}
	for _, tc := range []struct {
		name      string
		set       func(*schedtest.State)
		version   string
		federated string
	}{
		{"old controller", func(st *schedtest.State) { st.Version = "20.11.9" }, "20.11.9", "false"},
		{"old commands", func(st *schedtest.State) { st.ClientVersion = "20.02.7" }, "20.02.7", "false"},
		{"unknown release", func(st *schedtest.State) { st.Version = "" }, "unknown", "false"},
		{"federation", func(st *schedtest.State) { st.Federation = "fed1" }, "23.11.4", "true"},
	} {
		s, _ := newScheduler(t, tc.set)
		_, err := s.Info(t.Context())
		if got := kindtest.Want(t, err, problems.SchedulerUnsupported); got.Params["version"] != tc.version || got.Params["minimum"] != Minimum || got.Params["federated"] != tc.federated {
			t.Errorf("%s: params %v", tc.name, got.Params)
		}
	}
	// A variable that sends the commands to another cluster is refused;
	// one that names their own is not.
	c := schedtest.New(t)
	s = &Scheduler{Environ: append(c.Env(nil), "SBATCH_CLUSTERS=other")}
	_, err = s.Info(t.Context())
	if got := kindtest.Want(t, err, problems.SchedulerClusterVariable); got.Params["variable"] != "SBATCH_CLUSTERS" || got.Params["value"] != "other" || got.Params["cluster"] != "fixture" {
		t.Errorf("params %v", got.Params)
	}
	s = &Scheduler{Environ: append(c.Env(nil), "SLURM_CLUSTERS=fixture")}
	if _, err := s.Info(t.Context()); err != nil {
		t.Errorf("a variable that names the cluster's own: %v", err)
	}
	// With accounting, sacct is needed too.
	s, c = newScheduler(t, func(st *schedtest.State) { st.Accounting = true })
	if err := os.Remove(filepath.Join(c.Bin, "sacct")); err != nil {
		t.Fatal(err)
	}
	_, err = s.Info(t.Context())
	if got := kindtest.Want(t, err, problems.SchedulerCommandMissing); got.Params["command"] != "sacct" {
		t.Errorf("the missing command's params are %v", got.Params)
	}
}

// TestCommands checks that a command is found only on the PATH, at an
// absolute path, and refused where another user could change it.
func TestCommands(t *testing.T) {
	t.Parallel()
	s := &Scheduler{Environ: []string{"PATH=relative/bin:" + t.TempDir()}}
	_, err := s.Info(t.Context())
	if got := kindtest.Want(t, err, problems.SchedulerCommandMissing); got.Params["command"] != "sbatch" {
		t.Errorf("params %v", got.Params)
	}
	open := filepath.Join(t.TempDir(), "open")
	if err := os.Mkdir(open, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(open, "sbatch"), []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	s = &Scheduler{Environ: []string{"PATH=" + open}}
	_, err = s.Info(t.Context())
	if got := kindtest.Want(t, err, problems.SchedulerCommandUnsafe); got.Params["code"] != string(problems.PathUnsafe.Code()) {
		t.Errorf("params %v", got.Params)
	}
}

func TestPartition(t *testing.T) {
	t.Parallel()
	s, _ := newScheduler(t, func(st *schedtest.State) {
		st.Partitions = append(st.Partitions, schedtest.Partition{Name: "long", State: "DRAIN", MaxTime: "2-00:00:00", DefaultTime: "01:00:00", Hidden: true})
	})
	for name, want := range map[string]batch.Partition{
		"":     {Name: "debug", State: "UP", Up: true},
		"long": {Name: "long", State: "DRAIN", MaxSeconds: 2 * 86400, DefaultSeconds: 3600},
	} {
		if p, found, err := s.Partition(t.Context(), name); err != nil || !found || p != want {
			t.Errorf("Partition(%q): %+v, %v, %v", name, p, found, err)
		}
	}
	if _, found, err := s.Partition(t.Context(), "gpu"); err != nil || found {
		t.Errorf("Partition(gpu): %v, %v", found, err)
	}
}

func TestSettings(t *testing.T) {
	t.Parallel()
	s := new(Scheduler)
	got, err := s.Settings("m", jsontext.Value(`{"partition":"gpu","account":"proj","qos":"high","reservation":"r1","nodes":2,"exclusive":true,"gpu-type":"a100","walltime":"90m","host":"login1"}`))
	want := batch.Settings{Host: "login1", Partition: "gpu", Account: "proj", QOS: "high", Reservation: "r1", Nodes: 2, Exclusive: true, GPUType: "a100", WalltimeSeconds: 5400}
	if err != nil || *got != want {
		t.Errorf("Settings: %+v, %v", got, err)
	}
	for raw, want := range map[string][2]string{
		// The first key, as they sort, is the one said.
		`{"zzz":1,"partiton":"x"}`: {"partiton", "unknown"},
		`{"account":"a,b"}`:        {"account", "value"},
		`{"gpu-type":"a100:2"}`:    {"gpu-type", "value"},
		`{"walltime":true}`:        {"walltime", "type"},
		`{"walltime":1e30}`:        {"walltime", "value"},
		`{"walltime":99999999999}`: {"walltime", "value"},
		`{"nodes":"2"}`:            {"nodes", "type"},
		`{"nodes":0}`:              {"nodes", "value"},
		`{"account":"a\nb"}`:       {"account", "value"},
		`{"account":""}`:           {"account", "value"},
		`{"exclusive":"yes"}`:      {"exclusive", "type"},
		`{"walltime":"1y"}`:        {"walltime", "value"},
		`{"walltime":0}`:           {"walltime", "value"},
		`["partition"]`:            {"settings", "type"},
		`{"directives":["-N"]}`:    {"directives", "unknown"},
	} {
		_, err := s.Settings("m", jsontext.Value(raw))
		got := kindtest.Want(t, err, problems.MemberSettingInvalid)
		if got.Params["key"] != want[0] || got.Params["reason"] != want[1] || got.Params["member"] != "m" {
			t.Errorf("%s: params %v, want %v", raw, got.Params, want)
		}
	}
	if got, err := s.Settings("m", nil); err != nil || *got != (batch.Settings{}) {
		t.Errorf("no settings: %+v, %v", got, err)
	}
}

// TestSubmit checks sbatch's arguments, one for each option, and the
// environment it submits with, which leaves out what changes what sbatch
// does, and which cluster it reaches.
func TestSubmit(t *testing.T) {
	t.Parallel()
	s, c := newScheduler(t, func(st *schedtest.State) { st.Hold = true })
	dir := t.TempDir()
	job := &batch.Job{
		Name: "meridian-a1-1", Dir: dir, Script: []byte("#!/bin/sh\n"),
		Environ: c.Env([]string{
			"SBATCH_ARRAY_INX=0-9", "SQUEUE_FORMAT=%i", "SLURM_JOB_ID=7", "POSIXLY_CORRECT=1",
			"SLURM_CONF=/etc/slurm/slurm.conf", "SLURM_JWT=the-token", "KEEP=1",
		}),
		Cluster:         "fixture",
		WalltimeSeconds: 3601,
		Resources:       backend.Resources{MilliCPU: 1500, Memory: 3<<30 + 1, GPU: 2},
		Settings:        &batch.Settings{Partition: "debug", Account: "a", QOS: "q", Reservation: "r", Nodes: 2, Exclusive: true, GPUType: "a100"},
	}
	id, err := s.Submit(t.Context(), job)
	if err != nil || id != "2" {
		t.Fatalf("Submit: %q, %v", id, err)
	}
	want := []string{
		"--parsable", "--no-requeue", "--export=ALL", "--job-name=meridian-a1-1", "--output=" + dir + "/out", "--chdir=" + dir,
		"--partition=debug", "--account=a", "--qos=q", "--reservation=r", "--nodes=2", "--exclusive",
		"--time=61", "--ntasks-per-node=1", "--cpus-per-task=2", "--mem=3073M", "--gres=gpu:a100:2",
	}
	if got := c.Calls(t, "sbatch"); len(got) != 1 || !slices.Equal(got[0], want) {
		t.Errorf("sbatch's arguments are %q, want %q", got, want)
	}
	// One task on each node keeps the job's two nodes, where --ntasks=1
	// would have made it one of a node (testdata's 06l and 06m).
	j := c.Job(t, 2)
	if j.Nodes != 2 {
		t.Errorf("the job has %d nodes", j.Nodes)
	}
	// sbatch runs without the token, which it would copy into the job's
	// environment, which the scheduler keeps.
	for _, kv := range []string{"SBATCH_ARRAY_INX=0-9", "SQUEUE_FORMAT=%i", "SLURM_JOB_ID=7", "POSIXLY_CORRECT=1"} {
		if slices.Contains(j.Env, kv) {
			t.Errorf("the job's environment has %s", kv)
		}
	}
	if slices.ContainsFunc(j.Env, func(kv string) bool { return strings.HasPrefix(kv, "SLURM_JWT=") }) {
		t.Error("the job's environment has SLURM_JWT")
	}
	for _, kv := range []string{"SLURM_CONF=/etc/slurm/slurm.conf", "KEEP=1"} {
		if !slices.Contains(j.Env, kv) {
			t.Errorf("the job's environment lacks %s", kv)
		}
	}
	// A job that asks for nothing gets the scheduler's defaults.
	job = &batch.Job{Name: "meridian-a2-1", Dir: dir, Script: []byte("#!/bin/sh\n"), Environ: c.Env(nil), Settings: &batch.Settings{}, Cluster: "fixture"}
	if _, err := s.Submit(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	if got := c.Calls(t, "sbatch")[1]; len(got) != 6 || got[2] != "--export=ALL" {
		t.Errorf("sbatch's arguments for a job that asks for nothing: %q", got)
	}
	// One that the environment would send to another cluster isn't sent.
	job.Environ = append(c.Env(nil), "SLURM_CLUSTERS=other")
	_, err = s.Submit(t.Context(), job)
	kindtest.Want(t, err, problems.SchedulerClusterVariable)
	// Where sbatch can't authenticate without the token it isn't given, as
	// without munge, the job isn't submitted, and says why.
	noMunge, err := os.ReadFile("testdata/24.11.5-auth/06-sbatch-no-munge.stderr")
	if err != nil {
		t.Fatal(err)
	}
	c.Update(t, func(st *schedtest.State) {
		st.Fail = map[string][]schedtest.Failure{"sbatch": {{Stderr: string(noMunge), Exit: 1}, {Stderr: string(noMunge), Exit: 1}}}
	})
	job.Environ = append(c.Env(nil), "SLURM_JWT=the-token")
	_, err = s.Submit(t.Context(), job)
	kindtest.Want(t, err, problems.SchedulerTokenRequired)
	// Without a token, munge is restarting: an error to try again.
	job.Environ = c.Env(nil)
	if _, err := s.Submit(t.Context(), job); err == nil || problems.IsProblem(err) {
		t.Errorf("sbatch whose munge doesn't answer: %v", err)
	}
}

// TestNoShell checks that a path a shell would read more into reaches
// sbatch, and the job's script, and meridian batch, as it is: each is an
// argument of its own, and the script quotes it.
func TestNoShell(t *testing.T) {
	t.Parallel()
	s, c := newScheduler(t)
	parent := t.TempDir()
	dir := filepath.Join(parent, `it's "a" $(touch pwned); 100%j dir`+"\n`touch pwned`")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	args := filepath.Join(parent, "args")
	job := &batch.Job{
		Name: "meridian-a1-1", Dir: dir, Script: batch.Script(exe, dir),
		Environ: c.Env([]string{envArgs + "=" + args}), Settings: &batch.Settings{}, Cluster: "fixture",
	}
	if _, err := s.Submit(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	var data []byte
	for data, err = os.ReadFile(args); err != nil; data, err = os.ReadFile(args) {
		if time.Now().After(deadline) {
			t.Fatalf("meridian batch didn't run: %v, %+v", err, c.Job(t, 2))
		}
		_, _ = s.Statuses(t.Context(), []string{"2"}, false)
	}
	var got []string
	if err := jsonv2.Unmarshal(data, &got); err != nil || !slices.Equal(got, []string{"batch", dir}) {
		t.Errorf("meridian batch's arguments are %q, %v", got, err)
	}
	call := c.Calls(t, "sbatch")[0]
	if !slices.Contains(call, "--chdir="+dir) || !slices.Contains(call, "--output="+strings.ReplaceAll(dir, "%", "%%")+"/out") {
		t.Errorf("sbatch's arguments are %q", call)
	}
	// sbatch reads %% in the output's path as %.
	if j := c.Job(t, 2); j.Output != filepath.Join(dir, "out") {
		t.Errorf("the job's output is %q", j.Output)
	}
	for _, d := range []string{parent, dir, "/"} {
		if _, err := os.Lstat(filepath.Join(d, "pwned")); err == nil {
			t.Errorf("a shell ran what %s held", d)
		}
	}
}

func TestSubmitFailed(t *testing.T) {
	t.Parallel()
	refused := "sbatch: error: Batch job submission failed: Invalid account or account/partition combination specified\n"
	busy := "sbatch: error: Batch job submission failed: Socket timed out on send/recv operation\n"
	s, c := newScheduler(t, func(st *schedtest.State) {
		st.Fail = map[string][]schedtest.Failure{"sbatch": {{Stderr: refused, Exit: 1}, {Stderr: busy, Exit: 1}}}
	})
	job := &batch.Job{Name: "x", Dir: t.TempDir(), Script: []byte("#!/bin/sh\n"), Environ: c.Env(nil), Settings: &batch.Settings{}, Cluster: "fixture"}
	_, err := s.Submit(t.Context(), job)
	if r, ok := errors.AsType[*batch.Rejected](err); !ok || r.Said != "Invalid account or account/partition combination specified" {
		t.Errorf("a refusal: %v", err)
	}
	_, err = s.Submit(t.Context(), job)
	if _, ok := errors.AsType[*batch.Rejected](err); ok || err == nil || problems.IsProblem(err) {
		t.Errorf("a busy controller: %v", err)
	}
	// A command that doesn't answer in time.
	s, c = newScheduler(t, func(st *schedtest.State) { st.Delay = map[string]float64{"squeue": 5} })
	s.Timeout = 200 * time.Millisecond
	if _, err := s.Statuses(t.Context(), []string{"2"}, false); err == nil || problems.IsProblem(err) {
		t.Errorf("a slow squeue: %v", err)
	}
	_ = c
}

func TestStatuses(t *testing.T) {
	t.Parallel()
	now := time.Now()
	s, c := newScheduler(t, func(st *schedtest.State) {
		st.Jobs = []*schedtest.Job{
			{ID: 2, Name: "meridian-a1-1", UID: os.Getuid(), State: "PENDING", Reason: "PartitionTimeLimit"},
			{ID: 3, Name: "meridian-a2-1", UID: os.Getuid(), State: "OUT_OF_MEMORY", Reason: "OutOfMemory", End: now},
			{ID: 4, Name: "meridian-a3-1", UID: os.Getuid(), State: "NODE_FAIL", End: now.Add(-time.Hour)},
		}
	})
	got, err := s.Statuses(t.Context(), []string{"2", "3", "4"}, false)
	want := map[string]batch.JobStatus{
		"2": {State: backend.Queued, Reason: "PartitionTimeLimit", Stuck: true},
		"3": {State: backend.Dead, End: backend.EndOutOfMemory},
	}
	if err != nil || len(got) != 2 || got["2"] != want["2"] || got["3"] != want["3"] {
		t.Errorf("Statuses: %+v, %v", got, err)
	}
	if got, err := s.Statuses(t.Context(), []string{"4"}, false); err != nil || len(got) != 0 {
		t.Errorf("Statuses of a job squeue no longer lists: %+v, %v", got, err)
	}
	// Long options, each with its value after =, which POSIXLY_CORRECT
	// leaves as they are (testdata's 11c and 11d).
	if got := c.Calls(t, "squeue")[0]; !slices.Equal(got, []string{"--noheader", "--states=all", "--jobs=2,3,4", "--format=%i|%T|%r"}) {
		t.Errorf("squeue's arguments are %q", got)
	}
	c.Update(t, func(st *schedtest.State) {
		st.Accounting = true
		st.Partitions = append(st.Partitions, schedtest.Partition{Name: "hidden", State: "UP", MaxTime: "UNLIMITED", DefaultTime: "NONE", Hidden: true})
		st.Jobs = append(st.Jobs, &schedtest.Job{ID: 5, Name: "meridian-a4-1", UID: os.Getuid(), Partition: "hidden", State: "PENDING", Reason: "JobHeldUser"})
	})
	if got, err := s.Accounting(t.Context(), []string{"4"}); err != nil || got["4"] != (batch.JobStatus{State: backend.Dead, End: backend.EndNodeFailed}) {
		t.Errorf("Accounting: %+v, %v", got, err)
	}
	// A record accounting has as running, which the queue no longer lists,
	// is a runaway: missing.
	c.Update(t, func(st *schedtest.State) {
		st.Jobs = append(st.Jobs, &schedtest.Job{ID: 6, Name: "meridian-a5-1", UID: os.Getuid(), State: "RUNNING", Start: now, Runaway: true})
	})
	if got, err := s.Statuses(t.Context(), []string{"6"}, true); err != nil || len(got) != 0 {
		t.Errorf("Statuses of a runaway: %+v, %v", got, err)
	}
	if got, err := s.Accounting(t.Context(), []string{"4", "6"}); err != nil || len(got) != 1 {
		t.Errorf("Accounting of a runaway: %+v, %v", got, err)
	}
	if got := c.Calls(t, "sacct")[0]; !slices.Equal(got, []string{"--allocations", "--noheader", "--parsable2", "--jobs=4", "--format=JobIDRaw,State"}) {
		t.Errorf("sacct's arguments are %q", got)
	}
	// squeue --all lists a hidden partition's job; sacct has the one squeue
	// no longer lists.
	named, err := s.Named(t.Context(), []string{"meridian-a1-1", "meridian-a3-1", "meridian-a4-1", "meridian-zz-1"}, true)
	if err != nil || !slices.Equal(named["meridian-a1-1"], []string{"2"}) || !slices.Equal(named["meridian-a3-1"], []string{"4"}) ||
		!slices.Equal(named["meridian-a4-1"], []string{"5"}) || len(named) != 3 {
		t.Errorf("Named: %v, %v", named, err)
	}
	if err := s.Cancel(t.Context(), "999"); err != nil {
		t.Errorf("Cancel of a job the cluster doesn't know: %v", err)
	}
}

func TestAllocation(t *testing.T) {
	t.Parallel()
	start := time.Now().Truncate(time.Second)
	s, c := newScheduler(t, func(st *schedtest.State) {
		st.Jobs = []*schedtest.Job{{ID: 2, State: "RUNNING", Start: start, TimeLimit: 10}}
	})
	env := map[string]string{"SLURM_JOB_ID": "2", "SLURM_JOB_START_TIME": "1799999400", "SLURM_JOB_END_TIME": "1800000000"}
	a, err := s.Allocation(t.Context(), func(k string) string { return env[k] })
	if err != nil || a != (batch.Allocation{Job: "2", Start: time.Unix(1_799_999_400, 0), End: time.Unix(1_800_000_000, 0)}) {
		t.Errorf("with SLURM_JOB_START_TIME and SLURM_JOB_END_TIME: %+v, %v", a, err)
	}
	if calls := len(c.Calls(t, "squeue")); calls != 0 {
		t.Errorf("squeue was asked %d times", calls)
	}
	// Before 23.02, squeue says.
	delete(env, "SLURM_JOB_START_TIME")
	delete(env, "SLURM_JOB_END_TIME")
	a, err = s.Allocation(t.Context(), func(k string) string { return env[k] })
	if err != nil || !a.Start.Equal(start) || !a.End.Equal(start.Add(10*time.Minute)) {
		t.Errorf("from squeue: %+v, %v", a, err)
	}
	if _, err := s.Allocation(t.Context(), func(string) string { return "" }); err == nil {
		t.Error("Allocation outside a job")
	}
	if strings.Contains(strings.Join(s.Environ, " "), "SLURM_TIME_FORMAT") {
		t.Error("the scheduler's environment was changed")
	}
}

// TestTruncated checks that a command that writes more than the backend
// reads fails, rather than giving part of what it wrote.
func TestTruncated(t *testing.T) {
	t.Parallel()
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "squeue"), []byte("#!/bin/sh\nhead -c 17000000 /dev/zero\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	s := &Scheduler{Environ: []string{"PATH=" + bin + ":/usr/bin:/bin"}}
	if _, err := s.query(t.Context(), "squeue", nil); !errors.Is(err, errTruncated) {
		t.Errorf("a command that wrote 17 MB: %v", err)
	}
}

// TestCommandEnviron checks what Slurm's commands run with: the token
// stays, for squeue and the rest, as does SLURM_CONF; what changes what
// they do goes.
func TestCommandEnviron(t *testing.T) {
	t.Parallel()
	got := commandEnviron([]string{
		"SLURM_JWT=t", "SLURM_CONF=/etc/slurm.conf", "SLURM_JOB_ID=1", "SQUEUE_FORMAT=%i", "SBATCH_ACCOUNT=a",
		"POSIXLY_CORRECT=1", "HOME=/home/u",
	}, "SLURM_TIME_FORMAT=%s")
	if want := []string{"SLURM_JWT=t", "SLURM_CONF=/etc/slurm.conf", "HOME=/home/u", "SLURM_TIME_FORMAT=%s"}; !slices.Equal(got, want) {
		t.Errorf("commandEnviron = %q, want %q", got, want)
	}
}
