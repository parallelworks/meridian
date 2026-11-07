// Package schedtest is a fake Slurm cluster for tests: the test binary,
// run again under the names of Slurm's commands (sbatch, squeue, sacct,
// scancel, scontrol and sinfo) through symbolic links in a directory a
// test puts first on the PATH it gives them, acts on a cluster kept in a
// JSON file, under a lock. Its sbatch really runs a job's script, detached,
// as Slurm's slurmstepd would, with SLURM_JOB_ID and the rest set; and a
// test scripts the rest: a job's state and pending reason, MinJobAge,
// whether the cluster keeps accounting, and commands that are slow or
// fail.
//
// What the commands print, and their exit statuses, are those of the
// releases in backend/slurm/testdata, captured from real clusters.
//
// A package whose tests use it calls Main first in its TestMain.
package schedtest

import (
	"context"
	jsonv2 "encoding/json/v2"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/internal/flock"
)

// EnvState names the cluster's file, which has the test binary act as a
// command of the cluster's when it runs under one's name.
const EnvState = "SCHEDTEST_STATE"

// envStepd has the test binary run a job, as slurmstepd does, by its ID.
const envStepd = "SCHEDTEST_STEPD"

// Commands are the commands the fake has.
var Commands = []string{"sbatch", "squeue", "sacct", "scancel", "scontrol", "sinfo"}

// A State is a cluster.
type State struct {
	// Version is Slurm's release, as 23.11.4: SLURM_JOB_END_TIME is set
	// from 23.02, and scontrol says accounting is off as (null) from 23.
	Version string `json:"version"`
	// ClientVersion is the commands' own release, which sbatch --version
	// says, or Version's for "".
	ClientVersion string `json:"clientVersion,omitzero"`
	Cluster       string `json:"cluster"`
	// Accounting is whether the cluster keeps accounting, which sacct
	// reads.
	Accounting bool `json:"accounting"`
	// MinJobAge is how long, in seconds, squeue lists a job after it
	// ends.
	MinJobAge float64 `json:"minJobAge"`
	// KillWait is how long, in seconds, a job has between SIGTERM and
	// SIGKILL.
	KillWait float64 `json:"killWait"`
	// Minute is how many seconds a minute of a job's time limit lasts.
	Minute     float64     `json:"minute"`
	Partitions []Partition `json:"partitions"`
	// Hold keeps every job submitted pending, with HoldReason, until the
	// test starts it.
	Hold       bool   `json:"hold,omitzero"`
	HoldReason string `json:"holdReason,omitzero"`
	NextID     int    `json:"nextID"`
	Jobs       []*Job `json:"jobs"`
	// Calls are the commands run, in order.
	Calls []Call `json:"calls"`
	// Fail are failures each command gives, one a call, in order.
	Fail map[string][]Failure `json:"fail,omitempty"`
	// Delay is how long, in seconds, each command waits before it acts.
	Delay map[string]float64 `json:"delay,omitempty"`
	// Federation is the federation the cluster is one of, or "".
	Federation string `json:"federation,omitzero"`
	// Controller is SlurmctldHost's value, as n1 or ctl(10.0.0.2), and
	// SrunPortRange the ports srun listens at, or 0-0 for any.
	Controller    string `json:"controller,omitzero"`
	SrunPortRange string `json:"srunPortRange,omitzero"`
}

// A Partition is a partition. MaxTime and DefaultTime are as scontrol
// shows them: UNLIMITED, NONE, or [days-]hours:minutes:seconds.
type Partition struct {
	Name        string `json:"name"`
	State       string `json:"state"`
	Default     bool   `json:"default,omitzero"`
	Hidden      bool   `json:"hidden,omitzero"`
	MaxTime     string `json:"maxTime"`
	DefaultTime string `json:"defaultTime"`
}

// A Job is a job of the cluster's.
type Job struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	UID       int    `json:"uid"`
	Partition string `json:"partition"`
	// State is Slurm's: PENDING, RUNNING, COMPLETED, FAILED, CANCELLED,
	// TIMEOUT, OUT_OF_MEMORY, NODE_FAIL, PREEMPTED, and the rest; Reason
	// what squeue's %r shows.
	State  string `json:"state"`
	Reason string `json:"reason"`
	// Argv is sbatch's arguments.
	Argv   []string `json:"argv"`
	Dir    string   `json:"dir"`
	Output string   `json:"output"`
	Env    []string `json:"env"`
	Script string   `json:"script"`
	// Runaway is a job the controller forgot that accounting still has
	// as it was: squeue doesn't list it, and sacct does.
	Runaway bool `json:"runaway,omitzero"`
	// Nodes is how many nodes it was given.
	Nodes int `json:"nodes,omitzero"`
	// TimeLimit is in minutes, or 0 for the partition's default.
	TimeLimit int       `json:"timeLimit"`
	Submit    time.Time `json:"submit"`
	Start     time.Time `json:"start,omitzero"`
	End       time.Time `json:"end,omitzero"`
	ExitCode  int       `json:"exitCode"`
	// PID is the process that runs it, as slurmstepd, and PGID the process
	// group of its script, and what its script starts.
	PID  int `json:"pid,omitzero"`
	PGID int `json:"pgid,omitzero"`
}

// A Call is a command run, and its arguments.
type Call struct {
	Command string   `json:"command"`
	Args    []string `json:"args"`
}

// A Failure is what a command that fails prints on its standard error,
// and its exit status.
type Failure struct {
	Stderr string `json:"stderr"`
	Exit   int    `json:"exit"`
}

// live are the states of a job that hasn't ended.
var live = []string{
	"PENDING", "CONFIGURING", "REQUEUE_HOLD", "RESV_DEL_HOLD", "REQUEUED", "REQUEUE_FED",
	"RUNNING", "COMPLETING", "SUSPENDED", "STAGE_OUT", "SIGNALING", "RESIZING", "STOPPED",
}

// terminal reports whether a job's state is one it has ended in.
func (j *Job) terminal() bool { return !slices.Contains(live, j.State) }

// A Cluster is a fake cluster of a test's.
type Cluster struct {
	// Bin holds the commands, which the cluster's Env puts first on the
	// PATH.
	Bin   string
	state string
}

// New makes a cluster for the test t, as the State Slurm 23.11.4 with one
// partition, debug, up and the default, set has it, and then as each of
// opts changes it. Its jobs are killed as the test ends.
func New(t testing.TB, opts ...func(*State)) *Cluster {
	t.Helper()
	dir := t.TempDir()
	c := &Cluster{Bin: filepath.Join(dir, "bin"), state: filepath.Join(dir, "cluster.json")}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(c.Bin, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range Commands {
		if err := os.Symlink(exe, filepath.Join(c.Bin, name)); err != nil {
			t.Fatal(err)
		}
	}
	s := &State{
		Version: "23.11.4", Cluster: "fixture", MinJobAge: 300, KillWait: 5, Minute: 60, NextID: 2,
		Partitions: []Partition{{Name: "debug", State: "UP", Default: true, MaxTime: "UNLIMITED", DefaultTime: "NONE"}},
	}
	for _, o := range opts {
		o(s)
	}
	if err := save(c.state, s); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.kill() })
	return c
}

// Env returns the environment the cluster's commands run with: environ,
// os.Environ() for nil, with the commands first on its PATH, and the
// cluster's file named.
func (c *Cluster) Env(environ []string) []string {
	if environ == nil {
		environ = os.Environ()
	}
	path := os.Getenv("PATH")
	out := []string{EnvState + "=" + c.state}
	for _, kv := range environ {
		name, value, _ := strings.Cut(kv, "=")
		switch name {
		case "PATH":
			path = value
		case EnvState:
		default:
			out = append(out, kv)
		}
	}
	return append(out, "PATH="+c.Bin+string(os.PathListSeparator)+path)
}

// Update changes the cluster as f does.
func (c *Cluster) Update(t testing.TB, f func(*State)) {
	t.Helper()
	if err := update(t.Context(), c.state, func(s *State) error { f(s); return nil }); err != nil {
		t.Fatal(err)
	}
}

// State returns the cluster as it is.
func (c *Cluster) State(t testing.TB) *State {
	t.Helper()
	var s *State
	c.Update(t, func(st *State) { s = st })
	return s
}

// Job returns the job id, or nil.
func (c *Cluster) Job(t testing.TB, id int) *Job {
	t.Helper()
	return c.State(t).job(id)
}

// SetJob changes the job id as f does.
func (c *Cluster) SetJob(t testing.TB, id int, f func(*Job)) {
	t.Helper()
	c.Update(t, func(s *State) {
		j := s.job(id)
		if j == nil {
			t.Fatalf("schedtest: no job %d", id)
		}
		f(j)
	})
}

// Calls returns the arguments of each call of the command name.
func (c *Cluster) Calls(t testing.TB, name string) [][]string {
	t.Helper()
	var out [][]string
	for _, call := range c.State(t).Calls {
		if call.Command == name {
			out = append(out, call.Args)
		}
	}
	return out
}

// Start starts the pending job id, as the scheduler does once it can.
func (c *Cluster) Start(t testing.TB, id int) {
	t.Helper()
	err := update(t.Context(), c.state, func(s *State) error {
		j := s.job(id)
		if j == nil || j.State != "PENDING" {
			return fmt.Errorf("schedtest: job %d isn't pending", id)
		}
		return s.start(c.state, j)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// kill kills every job that runs, as the test ends: its stepd's session,
// and its script's process group, which what the script starts, as
// meridian batch and its worker, is in.
func (c *Cluster) kill() {
	s, err := load(c.state)
	if err != nil {
		return
	}
	for _, j := range s.Jobs {
		// A group long gone may have another's ID now.
		if j.terminal() && time.Since(j.End) > time.Minute {
			continue
		}
		for _, pid := range []int{j.PID, j.PGID} {
			if pid > 0 {
				killGroup(pid, true)
			}
		}
	}
}

// job returns the job id, or nil.
func (s *State) job(id int) *Job {
	for _, j := range s.Jobs {
		if j.ID == id {
			return j
		}
	}
	return nil
}

// partition returns the partition named, or the default for "", or nil.
func (s *State) partition(name string) *Partition {
	for i := range s.Partitions {
		if p := &s.Partitions[i]; p.Name == name || name == "" && p.Default {
			return p
		}
	}
	return nil
}

// start starts j, whose state the caller saves: it runs its script, as
// slurmstepd, in a process of its own, detached.
func (s *State) start(state string, j *Job) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe) //nolint:noctx,gosec // the test binary, as a job that outlives the command that starts it
	cmd.Env = append(os.Environ(), EnvState+"="+state, envStepd+"="+strconv.Itoa(j.ID))
	detach(cmd)
	if err := cmd.Start(); err != nil {
		return err
	}
	j.State, j.Reason, j.Start, j.PID = "RUNNING", "None", time.Now(), cmd.Process.Pid
	return cmd.Process.Release()
}

// update changes the cluster in the file state as f does, under its lock.
func update(ctx context.Context, state string, f func(*State) error) error {
	unlock, err := flock.Lock(ctx, state+".lock")
	if err != nil {
		return err
	}
	defer unlock()
	s, err := load(state)
	if err != nil {
		return err
	}
	if err := f(s); err != nil {
		return err
	}
	return save(state, s)
}

func load(state string) (*State, error) {
	data, err := os.ReadFile(state) //nolint:gosec // a test's cluster
	if err != nil {
		return nil, err
	}
	s := new(State)
	return s, jsonv2.Unmarshal(data, s)
}

func save(state string, s *State) error {
	data, err := jsonv2.Marshal(s)
	if err != nil {
		return err
	}
	if err := os.WriteFile(state+".new", data, 0o600); err != nil { //nolint:gosec // a test's cluster
		return err
	}
	return os.Rename(state+".new", state) //nolint:gosec // a test's cluster
}

// atLeast reports whether release is version or later, each as 23.11.
func atLeast(release, version string) bool {
	parse := func(v string) (int, int) {
		parts := strings.SplitN(v, ".", 3)
		if len(parts) < 2 {
			return 0, 0
		}
		major, _ := strconv.Atoi(parts[0])
		minor, _ := strconv.Atoi(parts[1])
		return major, minor
	}
	a, b := parse(release)
	c, d := parse(version)
	return a > c || a == c && b >= d
}
