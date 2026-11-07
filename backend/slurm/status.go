package slurm

import (
	"context"
	"errors"
	"maps"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/backend/batch"
)

// states are Slurm's job states, as squeue's %T and sacct's State show
// them, as the driver reads them (docs/design.md's table). REVOKED isn't
// one: it is a federation's job that a sibling cluster runs, which
// Info refuses, and which reads as Unknown.
var states = map[string]batch.JobStatus{
	"PENDING": {State: backend.Queued}, "CONFIGURING": {State: backend.Queued}, "REQUEUE_HOLD": {State: backend.Queued},
	"RESV_DEL_HOLD": {State: backend.Queued}, "REQUEUED": {State: backend.Queued}, "REQUEUE_FED": {State: backend.Queued},
	"RUNNING": {State: backend.Running}, "COMPLETING": {State: backend.Running}, "SUSPENDED": {State: backend.Running},
	"STAGE_OUT": {State: backend.Running}, "SIGNALING": {State: backend.Running}, "RESIZING": {State: backend.Running},
	"STOPPED":   {State: backend.Running},
	"COMPLETED": {State: backend.Dead, End: backend.EndExited}, "FAILED": {State: backend.Dead, End: backend.EndExited},
	"SPECIAL_EXIT": {State: backend.Dead, End: backend.EndExited},
	"TIMEOUT":      {State: backend.Dead, End: backend.EndTimedOut}, "DEADLINE": {State: backend.Dead, End: backend.EndTimedOut},
	"OUT_OF_MEMORY": {State: backend.Dead, End: backend.EndOutOfMemory},
	"NODE_FAIL":     {State: backend.Dead, End: backend.EndNodeFailed}, "BOOT_FAIL": {State: backend.Dead, End: backend.EndNodeFailed},
	"PREEMPTED": {State: backend.Dead, End: backend.EndPreempted},
	"CANCELLED": {State: backend.Dead, End: backend.EndExited, Cancelled: true},
}

// stuck are the reasons a job pends for that never clear for it, as
// squeue's man page names them: a limit its QoS or its association sets
// on one job, or on one node, or a minimum of its QoS, which the job asks
// more, or less, than; the most memory a partition allows; a time limit
// longer than its partition allows; a constraint no node has; a
// dependency that can't be met; and an account, a QoS or a reservation it
// may not use, or that is gone.
//
// JobHeldAdmin isn't one: an administrator holds a job on purpose, and
// may release it. Nor is PartitionNodeLimit, which a partition whose nodes
// are down or drained can give too: withdrawing a job such a node would
// have run is worse than leaving one queued that never runs.
var stuck = []*regexp.Regexp{
	regexp.MustCompile(`^(Assoc|QOS)Max[A-Za-z]*Per(Job|Node)`),
	regexp.MustCompile(`^QOSMin`),
	regexp.MustCompile(`^(MaxMemPerLimit|PartitionTimeLimit|BadConstraints|DependencyNeverSatisfied|AccountNotAllowed|QOSNotAllowed|ReservationDeleted)$`),
}

// stuckAccounting are the reasons that never clear only where the cluster
// keeps accounting: without it, 23.11 says InvalidAccount of a job that
// waits for nodes, and then runs it (testdata/23.11.4).
var stuckAccounting = []string{"InvalidAccount", "InvalidQOS"}

// status returns the JobStatus of a job in state, pending for reason, on
// a cluster that keeps accounting or not: a state the driver doesn't know
// is Unknown, with it as its reason.
func status(state, reason string, accounting bool) batch.JobStatus {
	// sacct shows CANCELLED by <uid>.
	state, _, _ = strings.Cut(strings.TrimSpace(state), " ")
	st, ok := states[state]
	if !ok {
		return batch.JobStatus{State: backend.Unknown, Reason: state}
	}
	if st.State == backend.Queued {
		st.Reason = reason
		st.Stuck = slices.ContainsFunc(stuck, func(re *regexp.Regexp) bool { return re.MatchString(reason) }) ||
			accounting && slices.Contains(stuckAccounting, reason)
	}
	return st
}

// Statuses asks squeue about the jobs ids, in every state. A job it no
// longer lists is missing.
func (s *Scheduler) Statuses(ctx context.Context, ids []string, accounting bool) (map[string]batch.JobStatus, error) {
	out, err := s.query(ctx, "squeue", []string{"--noheader", "--states=all", "--jobs=" + strings.Join(ids, ","), "--format=%i|%T|%r"})
	if ee, ok := errors.AsType[*exitError](err); ok && notListed(ee) {
		// One job it doesn't know is an error; of several, it lists those
		// it knows.
		return map[string]batch.JobStatus{}, nil
	}
	if err != nil {
		return nil, err
	}
	return parseStatuses(out, ids, accounting), nil
}

// notListed reports whether squeue failed for a job it doesn't know.
func notListed(e *exitError) bool {
	return strings.Contains(e.stderr, "Invalid job id specified")
}

// parseStatuses reads squeue --format='%i|%T|%r', for the jobs ids.
func parseStatuses(out string, ids []string, accounting bool) map[string]batch.JobStatus {
	jobs := map[string]batch.JobStatus{}
	for line := range strings.Lines(out) {
		fields := strings.SplitN(strings.TrimSpace(line), "|", 3)
		if len(fields) != 3 || !slices.Contains(ids, fields[0]) {
			continue
		}
		jobs[fields[0]] = status(fields[1], fields[2], accounting)
	}
	return jobs
}

// Accounting asks sacct about the jobs ids' allocations, which the queue
// no longer lists: those it has ended. One it has as pending or running
// is a record a controller that stopped never closed, a runaway, and
// missing.
func (s *Scheduler) Accounting(ctx context.Context, ids []string) (map[string]batch.JobStatus, error) {
	out, err := s.query(ctx, "sacct", []string{"--allocations", "--noheader", "--parsable2", "--jobs=" + strings.Join(ids, ","), "--format=JobIDRaw,State"})
	if err != nil {
		return nil, err
	}
	jobs := parseAccounting(out, ids)
	maps.DeleteFunc(jobs, func(_ string, st batch.JobStatus) bool { return st.State != backend.Dead })
	return jobs, nil
}

// parseAccounting reads sacct --parsable2 --format=JobIDRaw,State, for the
// jobs ids: the last line of a job is its latest.
func parseAccounting(out string, ids []string) map[string]batch.JobStatus {
	jobs := map[string]batch.JobStatus{}
	for line := range strings.Lines(out) {
		fields := strings.Split(strings.TrimSpace(line), "|")
		if len(fields) == 2 && slices.Contains(ids, fields[0]) {
			jobs[fields[0]] = status(fields[1], "", true)
		}
	}
	return jobs
}

// Named returns the IDs of the user's jobs of each name, as squeue lists
// them, of every partition, hidden ones among them, and then, for names
// it doesn't, as sacct has them from the last 30 days, where accounting
// is on.
func (s *Scheduler) Named(ctx context.Context, names []string, accounting bool) (map[string][]string, error) {
	uid := strconv.Itoa(os.Getuid())
	out, err := s.query(ctx, "squeue", []string{"--noheader", "--all", "--user=" + uid, "--states=all", "--format=%i|%j"})
	if err != nil {
		return nil, err
	}
	found := parseNamed(out, names, 2)
	var rest []string
	for _, name := range names {
		if len(found[name]) == 0 {
			rest = append(rest, name)
		}
	}
	if !accounting || len(rest) == 0 {
		return found, nil
	}
	out, err = s.query(ctx, "sacct", []string{
		"--allocations", "--noheader", "--parsable2", "--user=" + uid, "--starttime=now-30days",
		"--name=" + strings.Join(rest, ","), "--format=JobIDRaw,State,JobName",
	})
	if err != nil {
		return nil, err
	}
	for name, ids := range parseNamed(out, rest, 3) {
		found[name] = append(found[name], ids...)
	}
	return found, nil
}

// parseNamed reads lines of fields fields, a job's ID first and its name
// last, where a | in a name can't move it, and returns the IDs of each of
// names.
func parseNamed(out string, names []string, fields int) map[string][]string {
	found := map[string][]string{}
	for line := range strings.Lines(out) {
		f := strings.SplitN(strings.TrimRight(line, "\r\n"), "|", fields)
		if len(f) == fields && jobID(f[0]) && slices.Contains(names, f[fields-1]) {
			found[f[fields-1]] = append(found[f[fields-1]], f[0])
		}
	}
	return found
}
