package slurm

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/backend/batch"
	"github.com/parallelworks/meridian/problems"
)

// Submit submits job with sbatch --parsable, its script on standard
// input, each option an argument of its own. It is never requeued; its
// output and working directory are its submission's. A refusal that won't
// pass is a *batch.Rejected; a busy controller, or a limit on how many
// jobs the user may submit, an error to try again.
//
// sbatch runs without SLURM_JWT, with --export=ALL: it copies its
// environment into the job's, which the scheduler keeps, and any --export
// but ALL, NIL or NONE copies every SLURM_ variable it has back into the
// job's, a token among them (testdata's *-auth). Where sbatch can't
// authenticate without the token, as on a login node without munge,
// submitting is scheduler_token_required, rather than the token stored in
// a job.
func (s *Scheduler) Submit(ctx context.Context, job *batch.Job) (string, error) {
	if err := otherCluster(job.Environ, job.Cluster); err != nil {
		return "", err
	}
	environ := commandEnviron(job.Environ)
	withheld := getenv(environ, "SLURM_JWT") != ""
	environ = slices.DeleteFunc(environ, func(kv string) bool { return strings.HasPrefix(kv, "SLURM_JWT=") })
	out, err := s.run(ctx, "sbatch", submitArgs(job), environ, job.Script)
	if ee, ok := errors.AsType[*exitError](err); ok {
		return "", classify(ee, withheld)
	}
	if err != nil {
		return "", err
	}
	id, _, _ := strings.Cut(strings.TrimSpace(out), ";")
	if !jobID(id) {
		return "", fmt.Errorf("slurm: sbatch printed %q, not a job's ID", batch.Said(out))
	}
	return id, nil
}

// submitArgs returns sbatch's arguments for job.
func submitArgs(job *batch.Job) []string {
	args := []string{
		"--parsable", "--no-requeue", "--export=ALL", "--job-name=" + job.Name,
		// sbatch reads % in an output's path as a pattern, as %j.
		"--output=" + strings.ReplaceAll(filepath.Join(job.Dir, batch.OutFile), "%", "%%"), "--chdir=" + job.Dir,
	}
	set := job.Settings
	for _, o := range []struct{ name, value string }{
		{"partition", set.Partition}, {"account", set.Account}, {"qos", set.QOS}, {"reservation", set.Reservation},
	} {
		if o.value != "" {
			args = append(args, "--"+o.name+"="+o.value)
		}
	}
	if set.Nodes > 0 {
		args = append(args, "--nodes="+strconv.Itoa(set.Nodes))
	}
	if set.Exclusive {
		args = append(args, "--exclusive")
	}
	if job.WalltimeSeconds > 0 {
		// Slurm keeps time limits in whole minutes.
		args = append(args, "--time="+strconv.FormatInt((job.WalltimeSeconds+59)/60, 10))
	}
	r := job.Resources
	if r.MilliCPU > 0 {
		// One task on each node, which holds the job's cores, as --mem
		// and --gres are per node: with --ntasks=1, sbatch would make a
		// job of more nodes one of a node.
		args = append(args, "--ntasks-per-node=1", "--cpus-per-task="+strconv.FormatInt((r.MilliCPU+999)/1000, 10))
	}
	if r.Memory > 0 {
		args = append(args, "--mem="+strconv.FormatInt((r.Memory+(1<<20)-1)>>20, 10)+"M")
	}
	if r.GPU > 0 {
		gres := "gpu:"
		if set.GPUType != "" {
			gres += set.GPUType + ":"
		}
		args = append(args, "--gres="+gres+strconv.Itoa(r.GPU))
	}
	return args
}

// jobID reports whether id is a job's ID as sbatch --parsable gives it.
func jobID(id string) bool {
	return id != "" && strings.Trim(id, "0123456789") == ""
}

// transient are slurm_strerror's texts for a controller that is busy, or
// can't be reached, or can't reach its database, or a queue that is full,
// which pass; and an authentication that failed, as while munged restarts
// (testdata's *-auth/06).
var transient = []string{
	"Socket timed out on send/recv operation",
	"Zero Bytes were transmitted or received",
	"Communication connection failure",
	"Message send failure",
	"Message receive failure",
	"Resource temporarily unavailable",
	"Slurm backup controller in standby mode",
	"Controller is in standby mode, try a different controller",
	"Unable to connect to database",
	"Unable to create job record, try again",
	"Requested nodes are busy",
	authFailed,
}

// transientPrefixes are those of slurm_strerror's texts that end
// otherwise in each release: "(connect failure)", and 23.02's "please
// retry momentarily", or "later".
var transientPrefixes = []string{"Unable to contact slurm controller", "Rate limit exceeded"}

// authFailed is slurm_strerror's text for an authentication that failed.
const authFailed = "Protocol authentication error"

// policy is slurm_strerror's text for a job a limit refuses, which passes
// only for a limit on how many jobs the user may submit, as the reason
// sbatch says before it names, as QOSMaxSubmitJobPerUserLimit.
const policy = "Job violates accounting/QOS policy"

// classify returns the error of a failed sbatch, whose environment had a
// token withheld or not: scheduler_token_required where the token was,
// and sbatch couldn't authenticate without it; a *batch.Rejected with what
// it said, but for what passes, which is an error to try again, as is a
// failure that says nothing. Only the text after "Batch job submission
// failed: " says what the controller said, and only all of it: a
// job_submit plugin's own lines can say anything. A rejection says the
// other lines too, as the limit a policy names, or the plugin's message.
func classify(e *exitError, withheld bool) error {
	var failed string
	var said []string
	submitLimit := false
	for line := range strings.Lines(e.stderr) {
		line = strings.TrimSpace(line)
		rest, ok := strings.CutPrefix(line, "sbatch: error: ")
		if !ok {
			continue
		}
		if f, ok := strings.CutPrefix(rest, "Batch job submission failed: "); ok {
			failed = f
			continue
		}
		said = append(said, rest)
		submitLimit = submitLimit || strings.Contains(rest, "SubmitJob")
	}
	switch {
	case failed == authFailed && withheld:
		return problems.SchedulerTokenRequired.Problem(problems.Params{"host": hostName()})
	case failed != "":
		if slices.Contains(transient, failed) || strings.HasPrefix(failed, policy) && submitLimit ||
			slices.ContainsFunc(transientPrefixes, func(p string) bool { return strings.HasPrefix(failed, p) }) {
			return e
		}
		if len(said) > 0 {
			failed += " (" + strings.Join(said, "; ") + ")"
		}
		return &batch.Rejected{Said: batch.Said(failed)}
	case len(said) > 0:
		// sbatch's own refusal of an option, as of a --time it can't read.
		return &batch.Rejected{Said: batch.Said(strings.Join(said, "; "))}
	}
	return e
}
