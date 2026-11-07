package slurm

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/backend/batch"
	"github.com/parallelworks/meridian/problems"
)

// releases are the directories of testdata, each a release's output.
var releases = []string{"21.08.5", "23.11.4", "23.11.4-accounting", "24.11.5"}

// accounting reports whether a release's capture is of a cluster that
// keeps accounting.
func accounting(release string) bool { return strings.HasSuffix(release, "-accounting") }

// captured returns what a capture of a release printed, and its exit
// status: a file that isn't there is empty.
func captured(t *testing.T, release, name string) (stdout, stderr string, code int) {
	t.Helper()
	read := func(ext string) string {
		data, err := os.ReadFile(filepath.Join("testdata", release, name+ext))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
		return string(data)
	}
	code, err := strconv.Atoi(strings.TrimSpace(read(".exit")))
	if err != nil {
		t.Fatalf("%s %s: %v", release, name, err)
	}
	return read(".stdout"), read(".stderr"), code
}

// jobs returns a release's jobs' IDs, as JOBS.txt names them: a1=2 …
func jobs(t *testing.T, release string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", release, "JOBS.txt"))
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for f := range strings.FieldsSeq(string(data)) {
		if k, v, ok := strings.Cut(strings.TrimSuffix(f, ";"), "="); ok {
			ids[k] = v
		}
	}
	return ids
}

func TestFixtureConfig(t *testing.T) {
	t.Parallel()
	for _, release := range releases {
		out, _, _ := captured(t, release, "02-scontrol-show-config")
		info := parseConfig(out)
		version, _, _ := strings.Cut(release, "-")
		// The controller is n1, and srun's ports aren't fixed.
		if info.Version != version || info.Cluster != "fixture" || info.Accounting != accounting(release) || !supported(info.Version) ||
			!slices.Equal(info.Controllers, []string{"n1"}) || info.FirstPort != 0 || info.LastPort != 0 {
			t.Errorf("%s: %+v", release, info)
		}
		out, _, _ = captured(t, release, "01-sbatch-version")
		if v := clientVersion(out); v != version {
			t.Errorf("%s: sbatch --version %q gives %q", release, out, v)
		}
		if out, _, code := captured(t, release, "04-scontrol-show-federation"); code != 0 || federated(out) {
			t.Errorf("%s: a cluster of no federation: %q, %d", release, out, code)
		}
	}
	for v, want := range map[string]bool{"21.08.0": true, "20.11.9": false, "24.05": true, "3": false, "": false, "21.8.0": true, "21.07.1": false} {
		if supported(v) != want {
			t.Errorf("supported(%q) = %v", v, !want)
		}
	}
	// A controller's address, where its host's line gives one, a backup's
	// too, and srun's ports where the site fixes them.
	info := parseConfig("SlurmctldHost[0]        = ctl1(10.1.0.2)\nSlurmctldHost[1]        = ctl2\nSrunPortRange           = 60001-63000\n")
	if !slices.Equal(info.Controllers, []string{"10.1.0.2", "ctl2"}) || info.FirstPort != 60001 || info.LastPort != 63000 {
		t.Errorf("a config with controllers and srun's ports: %+v", info)
	}
	if !federated("Federation: fed1\nSelf:       c1:n1:6817 ID:1 FedState:ACTIVE Features:\nSibling:    c2:n2:6817 ID:2\n") {
		t.Error("a federation isn't one")
	}
}

func TestFixturePartitions(t *testing.T) {
	t.Parallel()
	for _, release := range releases {
		out, _, _ := captured(t, release, "03-scontrol-show-partition")
		ps, err := parsePartitions(out)
		if err != nil {
			t.Fatal(err)
		}
		want := []batch.Partition{
			{Name: "debug", State: "UP", Up: true, MaxSeconds: 600, DefaultSeconds: 300},
			{Name: "down", State: "DOWN"},
			{Name: "hidden", State: "UP", Up: true},
		}
		var got []batch.Partition
		for _, p := range ps {
			got = append(got, p.Partition)
		}
		if !slices.Equal(got, want) || !ps[0].isDefault || ps[1].isDefault {
			t.Errorf("%s: %+v", release, ps)
		}
		// Without --all, a hidden partition isn't shown.
		out, _, _ = captured(t, release, "03b-scontrol-show-partition-not-all")
		if ps, _ := parsePartitions(out); len(ps) != 2 {
			t.Errorf("%s: without --all, %+v", release, ps)
		}
	}
	for s, want := range map[string]int64{"UNLIMITED": 0, "NONE": 0, "00:10:00": 600, "1-00:00:00": 86400, "2-03:04:05": 2*86400 + 3*3600 + 4*60 + 5, "30:00": 1800} {
		if got, ok := duration(s); got != want || !ok {
			t.Errorf("duration(%q) = %d, %v, want %d", s, got, ok, want)
		}
	}
	for _, s := range []string{"x", "", "1:2:3:4", "-1:00:00", "INFINITE"} {
		if _, ok := duration(s); ok {
			t.Errorf("duration(%q) reads", s)
		}
	}
	if _, err := parsePartitions("PartitionName=p MaxTime=forever DefaultTime=NONE State=UP\n"); err == nil {
		t.Error("a MaxTime that can't be read is taken for none")
	}
}

// TestFixtureStatuses checks squeue's output of jobs in each state the
// releases put them in: those pending for a reason that can't clear by
// itself are stuck; finished jobs drop out after MinJobAge.
func TestFixtureStatuses(t *testing.T) {
	t.Parallel()
	for _, release := range releases {
		id := jobs(t, release)
		ids := []string{id["a1"], id["a2"], id["a3"], id["a4"], id["a5"], id["a6"], id["a7"], id["a8"], id["a9"]}
		want := map[string]batch.JobStatus{
			id["a2"]: {State: backend.Queued, Reason: "JobHeldUser"},
			id["a3"]: {State: backend.Queued, Reason: "PartitionTimeLimit", Stuck: true},
			id["a4"]: {State: backend.Queued, Reason: "PartitionDown"},
			id["a5"]: {State: backend.Dead, End: backend.EndExited},
			id["a6"]: {State: backend.Dead, End: backend.EndExited},
			id["a7"]: {State: backend.Dead, End: backend.EndExited, Cancelled: true},
			id["a8"]: {State: backend.Dead, End: backend.EndTimedOut},
		}
		for _, name := range []string{"07a0-squeue-all-before-timeout", "07a-squeue-all"} {
			out, _, code := captured(t, release, name)
			got := parseStatuses(out, ids, accounting(release))
			for jid, w := range want {
				// Before its time limit, the job that times out runs.
				if name == "07a0-squeue-all-before-timeout" && jid == id["a8"] {
					w = batch.JobStatus{State: backend.Running}
				}
				st, ok := got[jid]
				// A capture can come after MinJobAge has passed for a job
				// that ended first, as 23.11.4's did.
				if code != 0 || ok && st != w || !ok && w.State != backend.Dead {
					t.Errorf("%s %s: job %s is %+v, %v, want %+v", release, name, jid, st, ok, w)
				}
			}
			// 23.11 without accounting can say InvalidAccount of the job
			// that waits for CPUs, which then runs.
			if st := got[id["a9"]]; st.State != backend.Queued || st.Stuck {
				t.Errorf("%s %s: the job waiting for CPUs is %+v", release, name, st)
			}
		}
		out, _, _ := captured(t, release, "07b-squeue-all-after-minjobage")
		got := parseStatuses(out, ids, false)
		for _, a := range []string{"a1", "a5", "a6", "a7", "a8"} {
			if _, ok := got[id[a]]; ok {
				t.Errorf("%s: job %s is listed after MinJobAge", release, id[a])
			}
		}
		// One job squeue doesn't know is an error; of several, it lists the
		// rest.
		for _, name := range []string{"08-squeue-unknown", "09c-squeue-purged-alone"} {
			if _, stderr, code := captured(t, release, name); code != 1 || !notListed(&exitError{stderr: stderr}) {
				t.Errorf("%s %s: exited %d: %q", release, name, code, stderr)
			}
		}
		if out, _, code := captured(t, release, "09-squeue-real-and-unknown"); code != 0 || len(parseStatuses(out, []string{id["a1"], "999999"}, false)) > 1 {
			t.Errorf("%s: a known job and an unknown one: %d, %q", release, code, out)
		}
		if out, _, code := captured(t, release, "09b-squeue-purged-and-unknown"); code != 0 || len(parseStatuses(out, ids, false)) != 0 {
			t.Errorf("%s: two jobs it doesn't know: %d, %q", release, code, out)
		}
		// A drained node's job waits for it, which clears, not for
		// PartitionNodeLimit.
		out, _, _ = captured(t, release, "16b-squeue-drained")
		drained, _, _ := strings.Cut(out, "|")
		if st, ok := parseStatuses(out, []string{drained}, accounting(release))[drained]; !ok || st.State != backend.Queued || st.Stuck || !strings.HasPrefix(st.Reason, "ReqNodeNotAvail") {
			t.Errorf("%s: the job of a drained node is %+v, %v", release, st, ok)
		}
	}
}

func TestFixtureAccounting(t *testing.T) {
	t.Parallel()
	const release = "23.11.4-accounting"
	id := jobs(t, release)
	ids := []string{id["a1"], id["a2"], id["a5"], id["a6"], id["a7"], id["a8"]}
	out, _, code := captured(t, release, "12b-sacct-jobs-after-minjobage")
	got := parseAccounting(out, ids)
	for a, w := range map[string]batch.JobStatus{
		"a1": {State: backend.Dead, End: backend.EndTimedOut}, "a5": {State: backend.Dead, End: backend.EndExited},
		"a6": {State: backend.Dead, End: backend.EndExited}, "a7": {State: backend.Dead, End: backend.EndExited, Cancelled: true},
		"a8": {State: backend.Dead, End: backend.EndTimedOut}, "a2": {State: backend.Queued},
	} {
		if code != 0 || got[id[a]] != w {
			t.Errorf("sacct: job %s is %+v, want %+v", id[a], got[id[a]], w)
		}
	}
	// Before MinJobAge, accounting has the jobs as they run too.
	out, _, _ = captured(t, release, "12-sacct-jobs")
	got = parseAccounting(out, ids)
	for a, w := range map[string]batch.JobStatus{
		"a1": {State: backend.Running}, "a7": {State: backend.Dead, End: backend.EndExited, Cancelled: true},
		"a8": {State: backend.Dead, End: backend.EndTimedOut},
	} {
		if got[id[a]] != w {
			t.Errorf("sacct before MinJobAge: job %s is %+v, want %+v", id[a], got[id[a]], w)
		}
	}
	for _, release := range releases {
		if accounting(release) {
			continue
		}
		if _, stderr, code := captured(t, release, "12-sacct-jobs"); code != 1 || stderr != "Slurm accounting storage is disabled\n" {
			t.Errorf("%s: sacct without accounting: %d, %q", release, code, stderr)
		}
	}
}

func TestFixtureNamed(t *testing.T) {
	t.Parallel()
	for _, release := range releases {
		id := jobs(t, release)
		out, _, _ := captured(t, release, "10-squeue-user")
		got := parseNamed(out, []string{"meridian-a2-1", "meridian-a11-1", "meridian-zz-1"}, 2)
		// --all lists the job of a hidden partition, which squeue leaves
		// out otherwise.
		if len(got) != 2 || got["meridian-a2-1"][0] != id["a2"] || got["meridian-a11-1"][0] != id["a11"] {
			t.Errorf("%s: %v", release, got)
		}
		out, _, _ = captured(t, release, "10b-squeue-user-not-all")
		if got := parseNamed(out, []string{"meridian-a11-1"}, 2); len(got) != 0 {
			t.Errorf("%s: without --all, %v", release, got)
		}
	}
	id := jobs(t, "23.11.4-accounting")
	out, _, _ := captured(t, "23.11.4-accounting", "13-sacct-user-names")
	if got := parseNamed(out, []string{"meridian-a1-1", "meridian-a5-1"}, 3); got["meridian-a1-1"][0] != id["a1"] || got["meridian-a5-1"][0] != id["a5"] {
		t.Errorf("sacct: %v", got)
	}
	// A | in a name, last, doesn't move the ID.
	if got := parseNamed("7|PENDING|a|b\n8|PENDING|a\n", []string{"a|b"}, 3); !slices.Equal(got["a|b"], []string{"7"}) {
		t.Errorf("a name with a |: %v", got)
	}
}

// TestFixtureAllocation checks when a job's allocation starts and ends, as
// its environment says from 23.02, and squeue says before; and that
// squeue takes the long options the backend gives it under
// POSIXLY_CORRECT, as it doesn't its short ones.
func TestFixtureAllocation(t *testing.T) {
	t.Parallel()
	for _, release := range releases {
		data, err := os.ReadFile(filepath.Join("testdata", release, "05-job-env.txt"))
		if err != nil {
			t.Fatal(err)
		}
		env := map[string]string{}
		for line := range strings.Lines(string(data)) {
			if k, v, ok := strings.Cut(strings.TrimSpace(line), "="); ok {
				env[k] = v
			}
		}
		start, sok := epoch(env["SLURM_JOB_START_TIME"])
		end, eok := epoch(env["SLURM_JOB_END_TIME"])
		if newer := release != "21.08.5"; sok != newer || eok != newer || newer && end.Sub(start) != 2*time.Minute || env["SLURM_JOB_ID"] != jobs(t, release)["a1"] {
			t.Errorf("%s: the job's environment says %s to %s", release, env["SLURM_JOB_START_TIME"], env["SLURM_JOB_END_TIME"])
		}
		out, _, _ := captured(t, release, "11a-squeue-start-end-running")
		s, e, _ := strings.Cut(strings.TrimSpace(out), "|")
		st, ok1 := epoch(s)
		en, ok2 := epoch(e)
		if !ok1 || !ok2 || en.Sub(st) != 2*time.Minute {
			t.Errorf("%s: squeue says %q", release, out)
		}
		if out, _, _ := captured(t, release, "11b-squeue-start-end-pending"); strings.TrimSpace(out) != "N/A|N/A" {
			t.Errorf("%s: a pending job's are %q", release, out)
		}
		if _, _, code := captured(t, release, "11c-posixly-correct-short"); code == 0 {
			t.Errorf("%s: squeue's short options under POSIXLY_CORRECT", release)
		}
		if out, _, code := captured(t, release, "11d-posixly-correct-long"); code != 0 || strings.TrimSpace(out) != jobs(t, release)["a1"] {
			t.Errorf("%s: squeue's long options under POSIXLY_CORRECT: %d, %q", release, code, out)
		}
	}
}

// TestFixtureSubmitted checks that sbatch --parsable gives a job's ID; that
// a job of fewer tasks than nodes gets fewer nodes, which is why a job's
// cores are one task on each node; and that scancel says nothing of a job
// that runs, has ended, or that the cluster no longer knows.
func TestFixtureSubmitted(t *testing.T) {
	t.Parallel()
	for _, release := range releases {
		names, err := filepath.Glob(filepath.Join("testdata", release, "*-sbatch-*.exit"))
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range names {
			name := strings.TrimSuffix(filepath.Base(file), ".exit")
			if name == "01-sbatch-version" {
				continue
			}
			out, _, code := captured(t, release, name)
			if id, _, _ := strings.Cut(strings.TrimSpace(out), ";"); code == 0 && !jobID(id) {
				t.Errorf("%s %s: %q isn't a job's ID", release, name, out)
			}
		}
		for name, want := range map[string]string{
			"06l-scontrol-show-job-nodes-ntasks": "NumNodes=1-1", "06m-scontrol-show-job-nodes-ntasks-per-node": "NumNodes=2-2",
		} {
			if out, _, _ := captured(t, release, name); !strings.Contains(out, " "+want+" ") {
				t.Errorf("%s %s: no %s in %q", release, name, want, out)
			}
		}
		for _, name := range []string{"06g-scancel-running", "14a-scancel-completed", "14b-scancel-unknown", "14c-scancel-purged"} {
			if out, stderr, code := captured(t, release, name); code != 0 || out != "" || stderr != "" {
				t.Errorf("%s %s: %d, %q, %q", release, name, code, out, stderr)
			}
		}
	}
}

// TestFixtureRefusals checks that each of sbatch's refusals, in every
// release, is a rejection with what it said; that a busy controller, or a
// submit limit, is an error to try again; and that any other limit, or a
// job_submit plugin's line that says to try again, is a refusal.
func TestFixtureRefusals(t *testing.T) {
	t.Parallel()
	// A rejection says what sbatch said besides, as why it refused.
	said := map[string]string{
		"15a-sbatch-partition-nosuch": "Invalid partition name specified (invalid partition specified: nosuch)",
		"15b-sbatch-time-bogus":       "Invalid --time specification",
		"15c-sbatch-nodes-5":          "Requested node configuration is not available",
		"15d-sbatch-mem-too-big":      "Requested node configuration is not available (Memory specification can not be satisfied)",
		"15e-sbatch-account-nosuch":   "Invalid account or account/partition combination specified",
	}
	seen := 0
	for _, release := range releases {
		for name, want := range said {
			if _, err := os.Stat(filepath.Join("testdata", release, name+".exit")); err != nil {
				continue
			}
			_, stderr, code := captured(t, release, name)
			if code == 0 {
				// 21.08 and 23.11 queue a job of more nodes than the
				// partition has, as EnforcePartLimits=NO lets them.
				continue
			}
			seen++
			err := classify(&exitError{command: "sbatch", code: code, stderr: stderr}, false)
			if r, ok := errors.AsType[*batch.Rejected](err); !ok || r.Said != want {
				t.Errorf("%s %s: %v, want a rejection saying %q", release, name, err, want)
			}
		}
	}
	if seen < 14 {
		t.Errorf("only %d refusals were captured", seen)
	}
	for _, stderr := range []string{
		"sbatch: error: Batch job submission failed: Socket timed out on send/recv operation\n",
		"sbatch: error: Batch job submission failed: Unable to contact slurm controller (connect failure)\n",
		"sbatch: error: Slurm temporarily unable to accept job, sleeping and retrying\nsbatch: error: Batch job submission failed: Resource temporarily unavailable\n",
		"sbatch: error: QOSMaxSubmitJobPerUserLimit\nsbatch: error: Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)\n",
		"sbatch: error: AssocGrpSubmitJobsLimit\nsbatch: error: Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)\n",
		"sbatch: error: Batch job submission failed: Controller is in standby mode, try a different controller\n",
		"sbatch: error: Batch job submission failed: Unable to connect to database\n",
		"sbatch: error: Batch job submission failed: Message send failure\n",
		"sbatch: error: Batch job submission failed: Message receive failure\n",
		// What ends sbatch's own loop while the controller's queue is full.
		"sbatch: error: Batch job submission failed: Unable to create job record, try again\n",
		"sbatch: error: Batch job submission failed: Requested nodes are busy\n",
		// 23.02's rate limit.
		"sbatch: error: Batch job submission failed: Rate limit exceeded, please retry momentarily\n",
		"sbatch: error: Batch job submission failed: Rate limit exceeded, please retry later\n",
		"", "Killed\n",
	} {
		err := classify(&exitError{command: "sbatch", code: 1, stderr: stderr}, false)
		if _, ok := errors.AsType[*batch.Rejected](err); ok || err == nil {
			t.Errorf("%q: %v, want an error to try again", stderr, err)
		}
	}
	for stderr, want := range map[string]string{
		"sbatch: error: QOSMaxWallDurationPerJobLimit\nsbatch: error: Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)\n": "Job violates accounting/QOS policy (job submit limit, user's size and/or time limits) (QOSMaxWallDurationPerJobLimit)",
		"sbatch: error: site policy: try again tomorrow\nsbatch: error: Batch job submission failed: Invalid qos specification\n":                                                           "Invalid qos specification (site policy: try again tomorrow)",
		// A job_submit plugin's refusal says why only in its own line.
		"sbatch: error: GPU jobs go to the gpu partition\nsbatch: error: Batch job submission failed: Unspecified error\n": "Unspecified error (GPU jobs go to the gpu partition)",
	} {
		err := classify(&exitError{command: "sbatch", code: 1, stderr: stderr}, false)
		if r, ok := errors.AsType[*batch.Rejected](err); !ok || r.Said != want {
			t.Errorf("%q: %v, want a rejection saying %q", stderr, err, want)
		}
	}
}

// TestStates checks every state Slurm names, and that a state it doesn't
// know, or REVOKED, a federation's job another cluster runs, is Unknown.
func TestStates(t *testing.T) {
	t.Parallel()
	q, r := backend.Queued, backend.Running
	dead := func(end backend.End) batch.JobStatus { return batch.JobStatus{State: backend.Dead, End: end} }
	for state, want := range map[string]batch.JobStatus{
		"PENDING": {State: q}, "CONFIGURING": {State: q}, "REQUEUE_HOLD": {State: q}, "RESV_DEL_HOLD": {State: q},
		"REQUEUED": {State: q}, "REQUEUE_FED": {State: q},
		"RUNNING": {State: r}, "COMPLETING": {State: r}, "SUSPENDED": {State: r}, "STAGE_OUT": {State: r},
		"SIGNALING": {State: r}, "RESIZING": {State: r}, "STOPPED": {State: r},
		"COMPLETED": dead(backend.EndExited), "FAILED": dead(backend.EndExited), "SPECIAL_EXIT": dead(backend.EndExited),
		"TIMEOUT": dead(backend.EndTimedOut), "DEADLINE": dead(backend.EndTimedOut),
		"OUT_OF_MEMORY": dead(backend.EndOutOfMemory), "NODE_FAIL": dead(backend.EndNodeFailed), "BOOT_FAIL": dead(backend.EndNodeFailed),
		"PREEMPTED": dead(backend.EndPreempted), "CANCELLED": {State: backend.Dead, End: backend.EndExited, Cancelled: true},
		"CANCELLED by 1001": {State: backend.Dead, End: backend.EndExited, Cancelled: true},
		"REVOKED":           {State: backend.Unknown, Reason: "REVOKED"},
		// job_state_string never gives it.
		"LAUNCH_FAILED": {State: backend.Unknown, Reason: "LAUNCH_FAILED"},
		"NEW_STATE":     {State: backend.Unknown, Reason: "NEW_STATE"},
	} {
		if got := status(state, "", true); got != want {
			t.Errorf("%s: %+v, want %+v", state, got, want)
		}
	}
	if len(states) != 23 {
		t.Errorf("the map has %d states, and the test 23", len(states))
	}
}

// TestStuck checks each family of reasons a job pends for that never
// clear for it, and some that do.
func TestStuck(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{
		"PartitionTimeLimit", "BadConstraints", "DependencyNeverSatisfied", "MaxMemPerLimit",
		"AccountNotAllowed", "QOSNotAllowed", "ReservationDeleted",
		"QOSMaxWallDurationPerJobLimit", "AssocMaxWallDurationPerJobLimit", "QOSMaxCpuPerJobLimit", "AssocMaxCpuPerJobLimit",
		"QOSMaxNodePerJobLimit", "AssocMaxNodePerJobLimit", "QOSMaxGRESPerJob", "AssocMaxGRESPerJob", "QOSMaxMemoryPerJob",
		"AssocMaxMemPerJob", "QOSMaxBillingPerJob", "QOSMaxBBPerJob", "QOSMaxLicensePerJob", "QOSMaxEnergyPerJob",
		"QOSMaxUnknownPerJob", "QOSMaxCpuMinutesPerJobLimit", "AssocMaxCpuMinutesPerJobLimit", "QOSMaxGRESMinutesPerJob",
		"QOSMaxGRESPerNode", "QOSMaxCpuPerNode", "QOSMaxMemoryPerNode",
		"QOSMinCpuNotSatisfied", "QOSMinGRES", "QOSMinMemory",
	} {
		if !status("PENDING", reason, false).Stuck {
			t.Errorf("a job pending for %s isn't stuck", reason)
		}
	}
	for _, reason := range []string{"InvalidAccount", "InvalidQOS"} {
		if !status("PENDING", reason, true).Stuck || status("PENDING", reason, false).Stuck {
			t.Errorf("a job pending for %s is stuck where accounting isn't kept, or not where it is", reason)
		}
	}
	for _, reason := range []string{
		"JobHeldAdmin", "JobHeldUser", "Priority", "Resources", "ReqNodeNotAvail, UnavailableNodes:n1", "PartitionNodeLimit",
		"PartitionDown", "QOSMaxCpuPerUserLimit", "AssocGrpCpuLimit", "QOSGrpNodeLimit", "Dependency", "BeginTime",
	} {
		if status("PENDING", reason, true).Stuck {
			t.Errorf("a job pending for %s is stuck", reason)
		}
	}
}

// jobEnv reads a job's environment as an auth capture's job printed it,
// its token's value as <token>.
func jobEnv(t *testing.T, release, name string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", release, name+"-job-env.txt"))
	if err != nil {
		t.Fatal(err)
	}
	env := map[string]string{}
	for line := range strings.Lines(string(data)) {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), "="); ok {
			env[k] = v
		}
	}
	return env
}

// TestFixtureAuth checks, on 21.08, whose packages have auth/jwt, and
// 24.11: that --export=ALL gives a job sbatch's environment, and that any
// --export with a variable after ALL copies sbatch's SLURM_ variables back
// over it, a token among them, and has the job read the user's login
// environment, so no --export keeps a token out of a job; that squeue
// takes a token; and that, without munge, sbatch can't authenticate, but
// for the token.
func TestFixtureAuth(t *testing.T) {
	t.Parallel()
	for _, release := range []string{"21.08.5-auth", "24.11.5-auth"} {
		if env := jobEnv(t, release, "01-sbatch-export-all"); env["SLURM_FOO"] != "x" || env["SLURM_GET_USER_ENV"] != "" || env["FROM_LOGIN_PROFILE"] != "" {
			t.Errorf("%s: --export=ALL gave a job SLURM_FOO=%q, SLURM_GET_USER_ENV=%q, FROM_LOGIN_PROFILE=%q", release, env["SLURM_FOO"], env["SLURM_GET_USER_ENV"], env["FROM_LOGIN_PROFILE"])
		}
		if env := jobEnv(t, release, "02-sbatch-export-all-slurm-foo-empty"); env["SLURM_FOO"] != "x" || env["SLURM_GET_USER_ENV"] != "1" || env["FROM_LOGIN_PROFILE"] != "yes" {
			t.Errorf("%s: --export=ALL,SLURM_FOO= gave a job SLURM_FOO=%q, SLURM_GET_USER_ENV=%q, FROM_LOGIN_PROFILE=%q", release, env["SLURM_FOO"], env["SLURM_GET_USER_ENV"], env["FROM_LOGIN_PROFILE"])
		}
		_, stderr, code := captured(t, release, "06-sbatch-no-munge")
		e := &exitError{command: "sbatch", code: code, stderr: stderr}
		if err := classify(e, false); !errors.Is(err, e) {
			t.Errorf("%s: sbatch without munge, with no token withheld: %v, want an error to try again", release, err)
		}
		if p, ok := errors.AsType[*problem.Problem](classify(e, true)); !ok || p.Code != problems.SchedulerTokenRequired.Code() {
			t.Errorf("%s: sbatch without munge, with a token withheld: %v", release, p)
		}
		if _, stderr, code := captured(t, release, "07-squeue-no-munge"); code != 1 || notListed(&exitError{stderr: stderr}) {
			t.Errorf("%s: squeue without munge: %d, %q", release, code, stderr)
		}
	}
	const release = "21.08.5-auth"
	if env := jobEnv(t, release, "03-sbatch-jwt-export-all"); env["SLURM_JWT"] != "<token>" {
		t.Errorf("--export=ALL gave a job SLURM_JWT=%q", env["SLURM_JWT"])
	}
	if env := jobEnv(t, release, "04-sbatch-jwt-export-all-jwt-empty"); env["SLURM_JWT"] != "<token>" {
		t.Errorf("--export=ALL,SLURM_JWT= gave a job SLURM_JWT=%q", env["SLURM_JWT"])
	}
	for _, name := range []string{"05-squeue-jwt", "08-sbatch-no-munge-jwt"} {
		if out, _, code := captured(t, release, name); code != 0 || out == "" {
			t.Errorf("%s: %d, %q", name, code, out)
		}
	}
}
