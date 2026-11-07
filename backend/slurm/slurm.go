// Package slurm is the slurm backend's batch.Scheduler: Slurm's commands
// on the login node, of release 21.08 or later. It runs each with its
// arguments, each option in its long form with its value after =, never
// through a shell, by the absolute path the PATH it is given finds, once it
// has checked it as the worker's binary is (safepath.CheckExecutable). What
// each prints it reads as the releases in testdata print it.
package slurm

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend/batch"
	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
)

// Minimum is the oldest release the backend supports: Ubuntu 22.04's.
const Minimum = "21.08"

// DefaultTimeout bounds each command.
const DefaultTimeout = time.Minute

// maxOutput is the most bytes of a command's output read: a command that
// writes more fails.
const maxOutput = 16 << 20

// A Scheduler is Slurm's commands on this machine.
type Scheduler struct {
	// Environ is the environment the commands run in, and whose PATH finds
	// them: os.Environ() for nil.
	Environ []string
	// Timeout bounds each command: DefaultTimeout for 0.
	Timeout time.Duration
}

var _ batch.Scheduler = (*Scheduler)(nil)

// Name is the backend's name.
func (*Scheduler) Name() string { return "slurm" }

// WalltimeUnit is a minute: Slurm keeps time limits in minutes, and
// Submit rounds a walltime up to one.
func (*Scheduler) WalltimeUnit() int64 { return 60 }

// defaultConf is where Slurm's commands read their config without
// SLURM_CONF, as Debian's and Ubuntu's packages and Slurm's own builds
// put it.
const defaultConf = "/etc/slurm/slurm.conf"

// Config is the path of the config file the commands read, SLURM_CONF's
// or the default, with its symbolic links followed where it is there; one
// that isn't, as a configless site's, is as it is named.
func (s *Scheduler) Config() string {
	conf := getenv(s.environ(), "SLURM_CONF")
	if conf == "" {
		conf = defaultConf
	}
	if resolved, err := filepath.EvalSymlinks(conf); err == nil {
		return resolved
	}
	return conf
}

// The commands the backend runs: sacct only where the cluster keeps
// accounting.
var commands = []string{"sbatch", "squeue", "scancel", "scontrol"}

// hostName is this machine's name, for its problems.
func hostName() string {
	name, err := os.Hostname()
	if err != nil {
		return "this host"
	}
	return name
}

// command returns the absolute path of the command name, without symbolic
// links, as the PATH of s's environment finds it, once it has checked that
// no other user can change it or the way to it: scheduler_command_unsafe
// for one that isn't safe, and scheduler_command_missing for none. A PATH
// entry that isn't absolute is passed by.
func (s *Scheduler) command(name string) (string, error) {
	for _, dir := range filepath.SplitList(getenv(s.environ(), "PATH")) {
		if !filepath.IsAbs(dir) {
			continue
		}
		candidate := filepath.Join(dir, name)
		if info, err := os.Stat(candidate); err != nil || info.IsDir() || info.Mode()&0o111 == 0 {
			continue
		}
		path, err := safepath.LookExecutable(candidate)
		if p, ok := errors.AsType[*problem.Problem](err); ok {
			return "", problems.SchedulerCommandUnsafe.Problem(problems.Params{"host": hostName(), "path": path, "code": string(p.Code)})
		}
		if err != nil {
			continue
		}
		return path, nil
	}
	return "", problems.SchedulerCommandMissing.Problem(problems.Params{"host": hostName(), "command": name})
}

func (s *Scheduler) environ() []string {
	if s.Environ != nil {
		return s.Environ
	}
	return os.Environ()
}

func getenv(environ []string, name string) string {
	for _, kv := range slices.Backward(environ) {
		if n, v, _ := strings.Cut(kv, "="); n == name {
			return v
		}
	}
	return ""
}

// inputPrefixes are the prefixes of the variables that change what Slurm's
// commands do: each command's input variables.
var inputPrefixes = []string{"SBATCH_", "SQUEUE_", "SACCT_", "SCANCEL_", "SCONTROL_", "SINFO_"}

// commandEnviron returns base, but the variables that change what Slurm's
// commands do or which cluster they reach: their input variables, SLURM_
// variables, as a job inherits them within an allocation, but SLURM_CONF,
// and SLURM_JWT, with which a site that takes tokens authenticates, and
// which Submit takes out for sbatch; and POSIXLY_CORRECT; and then extra.
func commandEnviron(base []string, extra ...string) []string {
	out := make([]string, 0, len(base)+len(extra))
	for _, kv := range base {
		name, _, _ := strings.Cut(kv, "=")
		switch {
		case name == "SLURM_CONF", name == "SLURM_JWT":
		case strings.HasPrefix(name, "SLURM_"), name == "POSIXLY_CORRECT",
			slices.ContainsFunc(inputPrefixes, func(p string) bool { return strings.HasPrefix(name, p) }):
			continue
		}
		out = append(out, kv)
	}
	return append(out, extra...)
}

// clusterVariables are the variables that send Slurm's commands to
// another cluster than their own.
var clusterVariables = []string{"SLURM_CLUSTERS", "SBATCH_CLUSTERS", "SQUEUE_CLUSTERS", "SACCT_CLUSTERS", "SCANCEL_CLUSTERS"}

// otherCluster returns scheduler_cluster_variable where environ sends
// Slurm's commands to another cluster than cluster, the one they reach
// themselves, which the backend leaves out of their environment, rather
// than submit to another than the user meant.
func otherCluster(environ []string, cluster string) error {
	for _, name := range clusterVariables {
		v := getenv(environ, name)
		if v != "" && v != cluster {
			return problems.SchedulerClusterVariable.Problem(problems.Params{"host": hostName(), "variable": name, "value": batch.Said(v), "cluster": cluster})
		}
	}
	return nil
}

// An exitError is a command that exited with a status other than 0.
type exitError struct {
	command string
	code    int
	stderr  string
}

func (e *exitError) Error() string {
	return fmt.Sprintf("slurm: %s exited with status %d: %s", e.command, e.code, batch.Said(e.stderr))
}

// errTruncated is a command's output that is larger than maxOutput.
var errTruncated = fmt.Errorf("slurm: a command wrote more than %d bytes", maxOutput)

// run runs the command name with args, in the environment environ, and
// stdin as its standard input, and returns what it wrote on its standard
// output: an *exitError for a status other than 0.
func (s *Scheduler) run(ctx context.Context, name string, args []string, environ []string, stdin []byte) (string, error) {
	path, err := s.command(name)
	if err != nil {
		return "", err
	}
	timeout := s.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, args...) //nolint:gosec // a command whose path command checked, with its arguments, never through a shell
	cmd.Args[0] = name
	cmd.Env = environ
	cmd.Dir = "/"
	cmd.WaitDelay = time.Second
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr limited
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	switch ee, ok := errors.AsType[*exec.ExitError](err); {
	case stdout.over || stderr.over:
		return "", fmt.Errorf("%w: %s", errTruncated, name)
	case ok && ctx.Err() == nil:
		return stdout.String(), &exitError{command: name, code: ee.ExitCode(), stderr: stderr.String()}
	case err != nil:
		return "", fmt.Errorf("slurm: %s: %w", name, err)
	}
	return stdout.String(), nil
}

// A limited keeps the first maxOutput bytes written to it, and says
// whether there were more. Its buffer isn't embedded, whose ReadFrom
// io.Copy would take in place of Write.
type limited struct {
	buf  bytes.Buffer
	over bool
}

func (l *limited) Write(p []byte) (int, error) {
	if room := maxOutput - l.buf.Len(); len(p) > room {
		l.over = true
		p = p[:max(room, 0)]
	}
	return l.buf.Write(p)
}

func (l *limited) String() string { return l.buf.String() }

// query runs a command that asks the cluster something, in s's
// environment, but what changes what it prints, and extra.
func (s *Scheduler) query(ctx context.Context, name string, args []string, extra ...string) (string, error) {
	return s.run(ctx, name, args, commandEnviron(s.environ(), extra...), nil)
}

// Info asks scontrol show config for the release, the cluster's name and
// its accounting, once it has found every command it needs, and checked
// that the commands' own release, as sbatch --version says it, is one the
// backend supports too; and refuses a cluster of a federation, whose jobs
// can run on another, and one its commands' environment would send them to
// another cluster than.
func (s *Scheduler) Info(ctx context.Context) (*batch.Info, error) {
	for _, name := range commands {
		if _, err := s.command(name); err != nil {
			return nil, err
		}
	}
	out, err := s.query(ctx, "scontrol", []string{"show", "config"})
	if err != nil {
		return nil, err
	}
	info := parseConfig(out)
	unsupported := func(version string, federated bool) error {
		if version == "" {
			version = "unknown"
		}
		return problems.SchedulerUnsupported.Problem(problems.Params{
			"host": hostName(), "scheduler": "slurm", "version": batch.Said(version), "minimum": Minimum, "federated": federated,
		})
	}
	if !supported(info.Version) {
		return nil, unsupported(info.Version, false)
	}
	out, err = s.query(ctx, "sbatch", []string{"--version"})
	if err != nil {
		return nil, err
	}
	if client := clientVersion(out); !supported(client) {
		return nil, unsupported(client, false)
	}
	out, err = s.query(ctx, "scontrol", []string{"show", "federation"})
	if err != nil {
		return nil, err
	}
	if federated(out) {
		return nil, unsupported(info.Version, true)
	}
	if err := otherCluster(s.environ(), info.Cluster); err != nil {
		return nil, err
	}
	if info.Accounting {
		if _, err := s.command("sacct"); err != nil {
			return nil, err
		}
	}
	return info, nil
}

// parseConfig reads scontrol show config's output. Accounting is on
// unless its storage is none, which 23.02 and later show as (null). A
// controller is SlurmctldHost[n]'s address, where it gives one in
// parentheses, or its name; and SrunPortRange the ports compute nodes
// connect to a login node at, or 0-0 for any.
func parseConfig(out string) *batch.Info {
	info := new(batch.Info)
	for line := range strings.Lines(out) {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		switch key {
		case "SLURM_VERSION":
			info.Version = value
		case "ClusterName":
			info.Cluster = value
		case "AccountingStorageType":
			info.Accounting = strings.HasPrefix(value, "accounting_storage/") && value != "accounting_storage/none"
		case "SrunPortRange":
			if first, last, ok := PortRange(value); ok {
				info.FirstPort, info.LastPort = first, last
			}
		default:
			if strings.HasPrefix(key, "SlurmctldHost[") {
				name, addr, _ := strings.Cut(strings.TrimSuffix(value, ")"), "(")
				if host := cmp.Or(addr, name); host != "" {
					info.Controllers = append(info.Controllers, host)
				}
			}
		}
	}
	return info
}

// clientVersion reads sbatch --version's release: its last word, as
// slurm-wlm 21.08.5 or slurm 24.05.1 give it.
func clientVersion(out string) string {
	fields := strings.Fields(out)
	if len(fields) == 0 {
		return ""
	}
	return fields[len(fields)-1]
}

// federated reports whether scontrol show federation says the cluster is
// one of a federation.
func federated(out string) bool {
	for line := range strings.Lines(out) {
		if strings.HasPrefix(strings.TrimSpace(line), "Federation:") {
			return true
		}
	}
	return false
}

// supported reports whether version, as 23.11.4, is Minimum or later.
func supported(version string) bool {
	major, rest, _ := strings.Cut(version, ".")
	minor, _, _ := strings.Cut(rest, ".")
	ma, err1 := strconv.Atoi(major)
	mi, err2 := strconv.Atoi(minor)
	return err1 == nil && err2 == nil && (ma > 21 || ma == 21 && mi >= 8)
}

// Partition asks scontrol show partition, of every partition, hidden ones
// among them, for the partition named, or the default one for "".
func (s *Scheduler) Partition(ctx context.Context, name string) (batch.Partition, bool, error) {
	out, err := s.query(ctx, "scontrol", []string{"--all", "--oneliner", "show", "partition"})
	if err != nil {
		return batch.Partition{}, false, err
	}
	ps, err := parsePartitions(out)
	if err != nil {
		return batch.Partition{}, false, err
	}
	for _, p := range ps {
		if p.Name == name || name == "" && p.isDefault {
			return p.Partition, true, nil
		}
	}
	return batch.Partition{}, false, nil
}

type partition struct {
	batch.Partition
	isDefault bool
}

// parsePartitions reads scontrol --oneliner show partition's output: a
// line of Key=Value fields for each. A time it can't read is an error,
// never taken for none.
func parsePartitions(out string) ([]partition, error) {
	var ps []partition
	for line := range strings.Lines(out) {
		fields := map[string]string{}
		for f := range strings.FieldsSeq(line) {
			if k, v, ok := strings.Cut(f, "="); ok {
				fields[k] = v
			}
		}
		if fields["PartitionName"] == "" {
			continue
		}
		var p partition
		p.Name, p.State, p.isDefault = fields["PartitionName"], fields["State"], fields["Default"] == "YES"
		p.Up = p.State == "UP"
		var ok1, ok2 bool
		p.MaxSeconds, ok1 = duration(fields["MaxTime"])
		p.DefaultSeconds, ok2 = duration(fields["DefaultTime"])
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("slurm: the partition %s has a MaxTime %q or a DefaultTime %q that can't be read", p.Name, fields["MaxTime"], fields["DefaultTime"])
		}
		ps = append(ps, p)
	}
	return ps, nil
}

// duration reads a time as Slurm shows it, [days-]hours:minutes:seconds,
// or minutes:seconds, in seconds, or 0 for UNLIMITED or NONE; and false
// for one it can't read.
func duration(s string) (int64, bool) {
	if s == "UNLIMITED" || s == "NONE" {
		return 0, true
	}
	var days int64
	if d, rest, ok := strings.Cut(s, "-"); ok {
		n, err := strconv.ParseInt(d, 10, 64)
		if err != nil || n < 0 {
			return 0, false
		}
		days, s = n, rest
	}
	parts := strings.Split(s, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, false
	}
	var total int64
	for _, p := range parts {
		n, err := strconv.ParseInt(p, 10, 64)
		if err != nil || n < 0 {
			return 0, false
		}
		total = total*60 + n
	}
	return days*86400 + total, true
}

// Allocation says which job the process runs in, SLURM_JOB_ID, and when
// its allocation ends, or else when it started: SLURM_JOB_END_TIME and
// SLURM_JOB_START_TIME, which 23.02 and later set, or else what squeue
// says. Each is zero where neither says.
func (s *Scheduler) Allocation(ctx context.Context, getenv func(string) string) (batch.Allocation, error) {
	a := batch.Allocation{Job: getenv("SLURM_JOB_ID")}
	if !jobID(a.Job) {
		return batch.Allocation{}, errors.New("slurm: the process isn't a Slurm job's: SLURM_JOB_ID isn't set")
	}
	a.Start, _ = epoch(getenv("SLURM_JOB_START_TIME"))
	a.End, _ = epoch(getenv("SLURM_JOB_END_TIME"))
	// The start counts only for an end that isn't told.
	if !a.End.IsZero() {
		return a, nil
	}
	out, err := s.query(ctx, "squeue", []string{"--noheader", "--jobs=" + a.Job, "--format=%S|%e"}, "SLURM_TIME_FORMAT=%s")
	if err != nil {
		return a, err
	}
	start, end, _ := strings.Cut(strings.TrimSpace(out), "|")
	if t, ok := epoch(start); ok && a.Start.IsZero() {
		a.Start = t
	}
	if t, ok := epoch(end); ok && a.End.IsZero() {
		a.End = t
	}
	return a, nil
}

// epoch reads Unix seconds, as SLURM_JOB_END_TIME, or squeue's %e with
// SLURM_TIME_FORMAT=%s, gives them, and false for anything else, as N/A.
func epoch(s string) (time.Time, bool) {
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || n <= 0 {
		return time.Time{}, false
	}
	return time.Unix(n, 0), true
}

// Cancel cancels the job id with scancel, which signals it, SIGTERM and
// then SIGKILL. scancel says nothing, and exits 0, of a job that has
// ended, or that the cluster doesn't know (testdata's 14a, 14b and 14c).
func (s *Scheduler) Cancel(ctx context.Context, id string) error {
	_, err := s.query(ctx, "scancel", []string{id})
	return err
}
