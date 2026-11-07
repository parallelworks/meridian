package schedtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Main runs the test binary as one of the cluster's commands, or as the
// process that runs a job, and exits, when it was started as one; and
// otherwise returns. A package whose tests use the fake calls it first in
// its TestMain.
func Main() {
	state := os.Getenv(EnvState)
	if state == "" {
		return
	}
	ctx := context.Background()
	if id := os.Getenv(envStepd); id != "" {
		os.Exit(stepd(ctx, state, id))
	}
	name := filepath.Base(os.Args[0])
	if slices.Contains(Commands, name) {
		os.Exit(command(ctx, state, name, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
	}
}

// errUsage is a command's arguments that the fake doesn't take, which ends
// it as a real command would, with an error the test sees.
type errUsage struct{ arg string }

func (e errUsage) Error() string { return "schedtest: an argument the fake doesn't take: " + e.arg }

// command runs the command name of the cluster in the file state, and
// returns its exit status.
func command(ctx context.Context, state, name string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var delay float64
	var fail *Failure
	err := update(ctx, state, func(s *State) error {
		s.Calls = append(s.Calls, Call{Command: name, Args: args})
		delay = s.Delay[name]
		if fs := s.Fail[name]; len(fs) > 0 {
			fail = &fs[0]
			s.Fail[name] = fs[1:]
		}
		return nil
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	time.Sleep(time.Duration(delay * float64(time.Second)))
	if fail != nil {
		fmt.Fprint(stderr, fail.Stderr)
		return fail.Exit
	}
	var script []byte
	if name == "sbatch" {
		if script, err = io.ReadAll(stdin); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
	}
	var out, errOut strings.Builder
	code := 0
	err = update(ctx, state, func(s *State) error {
		out.Reset()
		errOut.Reset()
		var err error
		switch name {
		case "sbatch":
			code, err = s.sbatch(state, args, script, &out, &errOut)
		case "squeue":
			code, err = s.squeue(args, &out, &errOut)
		case "sacct":
			code, err = s.sacct(args, &out, &errOut)
		case "scancel":
			code, err = s.scancel(args)
		case "scontrol":
			code, err = s.scontrol(args, &out, &errOut)
		case "sinfo":
			code, err = s.sinfo(args, &out)
		}
		return err
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	fmt.Fprint(stdout, out.String())
	fmt.Fprint(stderr, errOut.String())
	return code
}

// An argReader reads a command's arguments, an option's value after it,
// or after = in its long form.
type argReader struct {
	args []string
	i    int
}

// next returns the next option, and its value, for one in values, which
// take one: name=value, or name value.
func (r *argReader) next(values ...string) (name, value string, ok bool) {
	if r.i >= len(r.args) {
		return "", "", false
	}
	a := r.args[r.i]
	r.i++
	if n, v, found := strings.Cut(a, "="); found && strings.HasPrefix(a, "--") {
		return n, v, true
	}
	if slices.Contains(values, a) && r.i < len(r.args) {
		r.i++
		return a, r.args[r.i-1], true
	}
	return a, "", true
}

// sbatch submits a job, as sbatch --parsable does, and starts it unless
// the cluster holds it.
func (s *State) sbatch(state string, args []string, script []byte, out, errOut io.Writer) (int, error) {
	j := &Job{UID: os.Getuid(), Argv: args, Script: string(script), Env: os.Environ(), Submit: time.Now()}
	hold, parsable := false, false
	ntasks := 0
	for _, a := range args {
		name, value, _ := strings.Cut(a, "=")
		switch name {
		case "--version":
			fmt.Fprintf(out, "slurm-wlm %s\n", cmpOr(s.ClientVersion, s.Version))
			return 0, nil
		case "--parsable":
			parsable = true
		case "--hold":
			hold = true
		case "--export":
			// ALL, and each NAME=value after it, which the job's
			// environment gets in place of sbatch's. As Slurm's
			// env_merge_filter does for any but ALL, NIL or NONE, every
			// SLURM_ variable of sbatch's is then copied back over them,
			// and the job asks for the user's login environment
			// (testdata's *-auth/02 and 04).
			for kv := range strings.SplitSeq(value, ",") {
				if name, _, ok := strings.Cut(kv, "="); ok {
					j.Env = append(slices.DeleteFunc(j.Env, func(e string) bool { return strings.HasPrefix(e, name+"=") }), kv)
				} else if kv != "ALL" {
					return 0, errUsage{a}
				}
			}
			if value != "ALL" {
				for _, kv := range os.Environ() {
					if name, _, _ := strings.Cut(kv, "="); strings.HasPrefix(name, "SLURM_") {
						j.Env = append(slices.DeleteFunc(j.Env, func(e string) bool { return strings.HasPrefix(e, name+"=") }), kv)
					}
				}
				j.Env = append(j.Env, "SLURM_GET_USER_ENV=1")
			}
		case "--job-name":
			j.Name = value
		case "--output":
			// sbatch reads %j and the rest in it, and %% as %.
			j.Output = strings.ReplaceAll(value, "%%", "%")
		case "--chdir":
			j.Dir = value
		case "--partition":
			j.Partition = value
		case "--time":
			n, err := strconv.Atoi(value)
			if err != nil {
				fmt.Fprintln(errOut, "sbatch: error: Invalid --time specification")
				return 255, nil //nolint:nilerr // sbatch's own refusal, which it prints
			}
			j.TimeLimit = n
		case "--nodes":
			j.Nodes, _ = strconv.Atoi(value)
		case "--ntasks":
			ntasks, _ = strconv.Atoi(value)
		case "--no-requeue", "--account", "--qos", "--reservation", "--exclusive",
			"--ntasks-per-node", "--cpus-per-task", "--mem", "--gres":
		default:
			return 0, errUsage{a}
		}
	}
	// As sbatch's option checks do, a job of fewer tasks than nodes gets
	// as many nodes as tasks.
	if j.Nodes == 0 {
		j.Nodes = 1
	}
	if ntasks > 0 && ntasks < j.Nodes {
		j.Nodes = ntasks
	}
	p := s.partition(j.Partition)
	if p == nil {
		fmt.Fprintf(errOut, "sbatch: error: invalid partition specified: %s\nsbatch: error: Batch job submission failed: Invalid partition name specified\n", j.Partition)
		return 1, nil
	}
	j.Partition = p.Name
	j.ID, s.NextID = s.NextID, s.NextID+1
	if j.Name == "" {
		j.Name = "sbatch"
	}
	s.Jobs = append(s.Jobs, j)
	j.State = "PENDING"
	switch maxTime, _ := minutes(p.MaxTime); {
	case p.State != "UP":
		j.Reason = "PartitionDown"
	case hold:
		j.Reason = "JobHeldUser"
	case maxTime > 0 && j.TimeLimit > maxTime:
		j.Reason = "PartitionTimeLimit"
	case s.Hold:
		j.Reason = cmpOr(s.HoldReason, "Priority")
	default:
		if err := s.start(state, j); err != nil {
			return 0, err
		}
	}
	if parsable {
		fmt.Fprintf(out, "%d\n", j.ID)
	} else {
		fmt.Fprintf(out, "Submitted batch job %d\n", j.ID)
	}
	return 0, nil
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// purged reports whether squeue no longer lists j, which ended over
// MinJobAge ago.
func (s *State) purged(j *Job) bool {
	return j.terminal() && time.Since(j.End).Seconds() > s.MinJobAge
}

// squeue lists jobs as squeue --noheader --format does, for --states=all,
// --jobs, --user and --all, in their short forms too. Without --all, a
// hidden partition's jobs aren't listed.
func (s *State) squeue(args []string, out, errOut io.Writer) (int, error) {
	var ids []int
	single, all, every := false, false, false
	uid, format := -1, ""
	r := &argReader{args: args}
	for {
		name, value, ok := r.next("-t", "-u", "-o", "-j")
		if !ok {
			break
		}
		switch {
		case name == "-h" || name == "--noheader":
		case name == "-a" || name == "--all":
			every = true
		case name == "-t" || name == "--states":
			all = value == "all"
		case name == "--jobs" || strings.HasPrefix(name, "-j"):
			list := value
			if name != "--jobs" && name != "-j" {
				// squeue's -j takes its list joined to it, as -j1008.
				list = strings.TrimPrefix(name, "-j")
			}
			ids = nil
			for id := range strings.SplitSeq(list, ",") {
				n, err := strconv.Atoi(id)
				if err != nil {
					return 0, errUsage{id}
				}
				ids = append(ids, n)
			}
			single = len(ids) == 1
		case name == "-u" || name == "--user":
			uid, _ = strconv.Atoi(value)
		case name == "-o" || name == "--format":
			format = value
		default:
			return 0, errUsage{name}
		}
	}
	var listed []*Job
	for _, j := range s.Jobs {
		hidden := false
		if p := s.partition(j.Partition); p != nil {
			hidden = p.Hidden
		}
		switch {
		case s.purged(j), j.Runaway, ids != nil && !slices.Contains(ids, j.ID), uid >= 0 && j.UID != uid,
			!all && ids == nil && j.terminal(), hidden && !every && ids == nil:
		default:
			listed = append(listed, j)
		}
	}
	if single && len(listed) == 0 {
		fmt.Fprintln(errOut, "slurm_load_jobs error: Invalid job id specified")
		return 1, nil
	}
	for _, j := range listed {
		line, err := s.format(j, format)
		if err != nil {
			return 0, err
		}
		fmt.Fprintln(out, line)
	}
	return 0, nil
}

// format writes j as squeue's format does, for %i, %j, %T, %r and %e.
func (s *State) format(j *Job, format string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(format); i++ {
		if format[i] != '%' || i+1 == len(format) {
			b.WriteByte(format[i])
			continue
		}
		i++
		switch format[i] {
		case 'i':
			b.WriteString(strconv.Itoa(j.ID))
		case 'j':
			b.WriteString(j.Name)
		case 'T':
			b.WriteString(j.State)
		case 'r':
			b.WriteString(j.Reason)
		case 'S':
			if j.Start.IsZero() {
				b.WriteString("N/A")
			} else {
				b.WriteString(timeFormat(j.Start))
			}
		case 'e':
			if end := s.endTime(j); end.IsZero() {
				b.WriteString("N/A")
			} else {
				b.WriteString(timeFormat(end))
			}
		default:
			return "", errUsage{format}
		}
	}
	return b.String(), nil
}

// timeFormat writes t as squeue does, in SLURM_TIME_FORMAT's %s or else
// its default.
func timeFormat(t time.Time) string {
	if os.Getenv("SLURM_TIME_FORMAT") == "%s" {
		return strconv.FormatInt(t.Unix(), 10)
	}
	return t.Format("2006-01-02T15:04:05")
}

// endTime is when j's allocation ends, or ended, or zero for a pending
// job.
func (s *State) endTime(j *Job) time.Time {
	switch {
	case j.terminal():
		return j.End
	case j.Start.IsZero():
		return time.Time{}
	}
	return j.Start.Add(s.timeLimit(j))
}

// timeLimit is how long j may run, as the fake's minutes last, or
// a year for a job without one.
func (s *State) timeLimit(j *Job) time.Duration {
	n := j.TimeLimit
	if n == 0 {
		if p := s.partition(j.Partition); p != nil {
			n, _ = minutes(p.DefaultTime)
		}
	}
	if n == 0 {
		return 365 * 24 * time.Hour
	}
	return time.Duration(float64(n) * s.Minute * float64(time.Second))
}

// minutes reads a time as scontrol shows it, in whole minutes, or 0 for
// UNLIMITED or NONE.
func minutes(t string) (int, error) {
	if t == "UNLIMITED" || t == "NONE" {
		return 0, nil
	}
	days := 0
	if d, rest, ok := strings.Cut(t, "-"); ok {
		var err error
		if days, err = strconv.Atoi(d); err != nil {
			return 0, err
		}
		t = rest
	}
	var h, m, sec int
	if _, err := fmt.Sscanf(t, "%d:%d:%d", &h, &m, &sec); err != nil {
		return 0, err
	}
	return days*24*60 + h*60 + m, nil
}

// sacct lists jobs as sacct --allocations --noheader --parsable2 does,
// for --jobs, --user and --name, with JobIDRaw, JobName, State and
// ExitCode, in their short forms too.
func (s *State) sacct(args []string, out, errOut io.Writer) (int, error) {
	if !s.Accounting {
		fmt.Fprintln(errOut, "Slurm accounting storage is disabled")
		return 1, nil
	}
	var ids []int
	var names, fields []string
	uid := -1
	r := &argReader{args: args}
	for {
		name, value, ok := r.next("-j", "-u", "-S", "-o")
		if !ok {
			break
		}
		switch name {
		case "-X", "--allocations", "-n", "--noheader", "-P", "--parsable2":
		case "-j", "--jobs":
			for id := range strings.SplitSeq(value, ",") {
				n, err := strconv.Atoi(id)
				if err != nil {
					return 0, errUsage{id}
				}
				ids = append(ids, n)
			}
		case "-u", "--user":
			uid, _ = strconv.Atoi(value)
		case "-S", "--starttime":
		case "--name":
			names = strings.Split(value, ",")
		case "-o", "--format":
			fields = strings.Split(value, ",")
		default:
			return 0, errUsage{name}
		}
	}
	for _, j := range s.Jobs {
		if ids != nil && !slices.Contains(ids, j.ID) || names != nil && !slices.Contains(names, j.Name) || uid >= 0 && j.UID != uid {
			continue
		}
		var cols []string
		for _, f := range fields {
			switch f {
			case "JobIDRaw":
				cols = append(cols, strconv.Itoa(j.ID))
			case "JobName":
				cols = append(cols, j.Name)
			case "State":
				st := j.State
				if st == "CANCELLED" {
					st += " by " + strconv.Itoa(j.UID)
				}
				cols = append(cols, st)
			case "ExitCode":
				cols = append(cols, strconv.Itoa(j.ExitCode)+":0")
			default:
				return 0, errUsage{f}
			}
		}
		fmt.Fprintln(out, strings.Join(cols, "|"))
	}
	return 0, nil
}

// scancel cancels jobs, as Slurm's does: one that has ended, or that the
// cluster doesn't know, quietly.
func (s *State) scancel(args []string) (int, error) {
	for _, a := range args {
		id, err := strconv.Atoi(a)
		if err != nil {
			return 0, errUsage{a}
		}
		j := s.job(id)
		switch {
		case j == nil || j.terminal():
		case j.State == "PENDING":
			j.State, j.Reason, j.End = "CANCELLED", "None", time.Now()
		default:
			j.State, j.End = "CANCELLED", time.Now()
			// Its stepd signals the job, and waits KillWait for it.
			terminate(j.PID)
		}
	}
	return 0, nil
}

// scontrol shows the config, the partitions, the federation and a job,
// as scontrol show config, --all --oneliner show partition, show
// federation and --oneliner show job do. Without --all, a hidden partition
// isn't shown.
func (s *State) scontrol(args []string, out, errOut io.Writer) (int, error) {
	every := false
	var words []string
	for _, a := range args {
		switch a {
		case "-a", "--all":
			every = true
		case "-o", "--oneliner":
		default:
			words = append(words, a)
		}
	}
	switch {
	case slices.Equal(words, []string{"show", "config"}):
		accounting := "(null)"
		if !atLeast(s.Version, "23.02") {
			accounting = "accounting_storage/none"
		}
		if s.Accounting {
			accounting = "accounting_storage/slurmdbd"
		}
		fmt.Fprintf(out, "Configuration data as of %s\n", time.Now().Format("2006-01-02T15:04:05"))
		for _, kv := range [][2]string{
			{"AccountingStorageType", accounting},
			{"ClusterName", s.Cluster},
			{"KillWait", strconv.FormatFloat(s.KillWait, 'f', -1, 64) + " sec"},
			{"MinJobAge", strconv.FormatFloat(s.MinJobAge, 'f', -1, 64) + " sec"},
			{"SLURM_VERSION", s.Version},
		} {
			fmt.Fprintf(out, "%-23s = %s\n", kv[0], kv[1])
		}
		fmt.Fprint(out, "\nSlurmctld(primary) at n1 is UP\n")
		return 0, nil
	case slices.Equal(words, []string{"show", "federation"}):
		// A cluster of no federation prints nothing.
		if s.Federation != "" {
			fmt.Fprintf(out, "Federation: %s\nSelf:       %s:n1:6817 ID:1 FedState:ACTIVE Features:\nSibling:    other:n2:6817 ID:2 FedState:ACTIVE Features: PersistConnSend/Recv:Yes/Yes Synced:Yes\n", s.Federation, s.Cluster)
		}
		return 0, nil
	case len(words) >= 2 && words[0] == "show" && words[1] == "partition" && len(words) <= 3:
		listed := false
		for _, p := range s.Partitions {
			if len(words) == 3 && p.Name != words[2] || p.Hidden && !every {
				continue
			}
			listed = true
			yes := map[bool]string{true: "YES", false: "NO"}
			fmt.Fprintf(out, "PartitionName=%s AllowGroups=ALL AllowAccounts=ALL AllowQos=ALL AllocNodes=ALL Default=%s QoS=N/A DefaultTime=%s "+
				"DisableRootJobs=NO ExclusiveUser=NO GraceTime=0 Hidden=%s MaxNodes=UNLIMITED MaxTime=%s MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED "+
				"Nodes=n1 PriorityJobFactor=1 PriorityTier=1 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=%s "+
				"TotalCPUs=2 TotalNodes=1 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerNode=UNLIMITED MaxMemPerNode=UNLIMITED\n",
				p.Name, yes[p.Default], p.DefaultTime, yes[p.Hidden], p.MaxTime, p.State)
		}
		if !listed && len(words) == 3 {
			fmt.Fprintf(errOut, "Partition %s not found\n", words[2])
			return 1, nil
		}
		return 0, nil
	}
	return 0, errUsage{strings.Join(args, " ")}
}

// sinfo lists a partition, as sinfo -h -p P -o '%P|%a|%l' does.
func (s *State) sinfo(args []string, out io.Writer) (int, error) {
	if len(args) != 5 || args[0] != "-h" || args[1] != "-p" || args[3] != "-o" || args[4] != "%P|%a|%l" {
		return 0, errUsage{strings.Join(args, " ")}
	}
	p := s.partition(args[2])
	if p == nil {
		return 0, nil
	}
	name := p.Name
	if p.Default {
		name += "*"
	}
	limit := strings.ToLower(p.MaxTime)
	if n, err := minutes(p.MaxTime); err == nil && n > 0 {
		limit = fmt.Sprintf("%d:00", n)
	}
	fmt.Fprintf(out, "%s|%s|%s\n", name, strings.ToLower(p.State), limit)
	return 0, nil
}

// stepd runs the job id, as slurmstepd does: its script, by sh, with what
// Slurm gives a batch job in its environment, its output in its output
// file, in a process group of its own that the job's signals go to: from
// scancel, and at its time limit, each SIGTERM, and SIGKILL KillWait
// later. It records how the job ended.
func stepd(ctx context.Context, state, id string) int {
	// Under the lock, which the sbatch that started this holds until it
	// has saved the job.
	n, _ := strconv.Atoi(id)
	var s *State
	var j *Job
	if err := update(ctx, state, func(st *State) error { s, j = st, st.job(n); return nil }); err != nil || j == nil {
		return 1
	}
	script := state + "." + id + ".sh"
	if err := os.WriteFile(script, []byte(j.Script), 0o600); err != nil { //nolint:gosec // a test's job
		return 1
	}
	end := s.endTime(j)
	cmd := exec.Command("/bin/sh", script) //nolint:noctx,gosec // a test's job, which signals end
	cmd.Dir = cmpOr(j.Dir, "/")
	cmd.Env = slices.DeleteFunc(slices.Clone(j.Env), func(kv string) bool { return strings.HasPrefix(kv, envStepd+"=") })
	cmd.Env = append(cmd.Env,
		"SLURM_JOB_ID="+id, "SLURM_JOBID="+id, "SLURM_JOB_NAME="+j.Name, "SLURM_JOB_PARTITION="+j.Partition,
		"SLURM_CLUSTER_NAME="+s.Cluster, "SLURM_JOB_UID="+strconv.Itoa(j.UID), "SLURM_SUBMIT_DIR=/", "SLURM_NODELIST=n1",
	)
	if atLeast(s.Version, "23.02") {
		cmd.Env = append(cmd.Env, "SLURM_JOB_START_TIME="+strconv.FormatInt(j.Start.Unix(), 10), "SLURM_JOB_END_TIME="+strconv.FormatInt(end.Unix(), 10))
	}
	if j.Output != "" {
		f, err := os.OpenFile(j.Output, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644) //nolint:gosec // the job's output, as Slurm makes it
		if err != nil {
			return 1
		}
		defer f.Close()
		cmd.Stdout, cmd.Stderr = f, f
	}
	group(cmd)
	terms := make(chan os.Signal, 1)
	signal.Notify(terms, syscall.SIGTERM)
	ended := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		ended <- err
	} else {
		_ = update(ctx, state, func(s *State) error {
			s.job(n).PGID = cmd.Process.Pid
			return nil
		})
		go func() { ended <- cmd.Wait() }()
	}
	limit := time.NewTimer(time.Until(end))
	var killAt <-chan time.Time
	timedOut := false
	stop := func() {
		if killAt == nil && cmd.Process != nil {
			killGroup(cmd.Process.Pid, false)
			killAt = time.After(time.Duration(s.KillWait * float64(time.Second)))
		}
	}
	var waitErr error
wait:
	for {
		select {
		case waitErr = <-ended:
			break wait
		case <-terms:
			stop()
		case <-limit.C:
			timedOut = true
			stop()
		case <-killAt:
			killGroup(cmd.Process.Pid, true)
		}
	}
	code := 0
	if ee, ok := errors.AsType[*exec.ExitError](waitErr); ok {
		code = ee.ExitCode()
		if sig := signalNumber(ee); sig != 0 {
			code = 128 + sig
		}
	} else if waitErr != nil {
		code = 1
	}
	_ = update(ctx, state, func(s *State) error {
		j := s.job(n)
		if j.terminal() {
			return nil
		}
		j.End, j.ExitCode = time.Now(), code
		switch {
		case timedOut:
			j.State, j.Reason = "TIMEOUT", "TimeLimit"
		case code == 0:
			j.State, j.Reason = "COMPLETED", "None"
		default:
			j.State, j.Reason = "FAILED", "NonZeroExitCode"
		}
		return nil
	})
	return 0
}
