package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend/batch"
	localbackend "github.com/parallelworks/meridian/backend/local"
	"github.com/parallelworks/meridian/backend/slurm"
	"github.com/parallelworks/meridian/internal/harden"
	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// batchAbout is what meridian batch's usage says of it besides its summary.
const batchAbout = `meridian batch <directory> is the job a scheduler backend submits for an
attempt: it reads what to do from the attempt's submission directory, the
one it is given, records there how its job starts and ends, and runs
meridian worker, which it gives the attempt's bootstrap token on a socket
once both have kept the user's other processes out of their memory.`

// A batchSystem is what meridian batch asks of the system it runs on,
// which a test gives in place of the system's (cli.batch).
type batchSystem struct {
	// harden keeps the user's other processes out of the process's
	// memory.
	harden func() error
	// executable returns the meridian binary, the path that starts it,
	// and its digest, once it has checked it (localbackend.Runnable).
	executable func() (path, run, digest string, err error)
	// environ is the job's environment, which the worker gets.
	environ  func() []string
	hostname func() (string, error)
	// cgroup returns what /proc/self/cgroup says, and cgroupRoot is where
	// the cgroup filesystem is.
	cgroup     func() (string, error)
	cgroupRoot string
	// relayWait is how long the relay's addresses are tried, and
	// relayFresh how recently its file must have been written for a relay
	// to be up.
	relayWait, relayFresh time.Duration
	// tokenWait is how long the worker has to ask for its token.
	tokenWait time.Duration
}

// systemBatch is the system's own.
var systemBatch = batchSystem{
	harden: harden.Process, executable: localbackend.Runnable, environ: os.Environ, hostname: os.Hostname,
	cgroup: func() (string, error) {
		data, err := os.ReadFile("/proc/self/cgroup")
		return string(data), err
	},
	cgroupRoot: "/sys/fs/cgroup",
	// A relay touches its file every 30 seconds, and NFS can show a
	// file's time a minute late.
	relayWait: time.Minute, relayFresh: 3 * time.Minute,
	tokenWait: localbackend.DefaultTokenWait,
}

// runBatch is meridian batch: the main process of a scheduler's job for
// an attempt (backend/batch), which a scheduler starts on a compute node
// with the attempt's submission directory. It hardens itself, checks the
// binary it runs, reads the spec, records when it started and when its
// allocation ends (started.json), checks that a relay that is up can be
// reached, and only then takes the token from its file; starts meridian
// worker, and hands it the token once it says it is hardened; passes on
// SIGTERM and SIGINT, recording when the first came (term.json); and,
// once the worker has ended, records how, with how many processes the
// kernel killed for memory meanwhile (exit.json). It exits as the worker
// did, or 2 when it couldn't start it.
func runBatch(ctx context.Context, c *cli, fs *flag.FlagSet, args []string) int {
	positional, err := parseFlags(fs, args)
	if err != nil {
		return flagError(c, fs, err)
	}
	if len(positional) != 1 || !filepath.IsAbs(positional[0]) {
		return c.failf(problems.ArgumentsInvalid, problems.Params{"command": "batch"})
	}
	sys := systemBatch
	if c.batch != nil {
		sys = *c.batch
	}
	// Signals are caught from the start, and passed on once the worker
	// runs: a second one mustn't end this process, as it would a command
	// a person runs.
	signals := c.signals
	if signals == nil {
		signals = make(chan os.Signal, 4)
		signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
		defer signal.Stop(signals)
	}
	b := &batchJob{c: c, sys: sys, dir: filepath.Clean(positional[0]), signals: signals}
	code, err := b.run(ctx)
	if err != nil {
		return c.fail(err)
	}
	return code
}

// A batchJob is one run of meridian batch.
type batchJob struct {
	c       *cli
	sys     batchSystem
	dir     string
	signals chan os.Signal

	mu sync.Mutex
	// worker is the worker's process, once it has started.
	worker *os.Process
	// termed is the first signal that came, or nil.
	termed os.Signal
	// stop is cancelled as the first signal comes, which stops what comes
	// before the worker starts.
	stop context.CancelFunc
}

// run does what runBatch says, and returns the exit status, or the error
// with which it couldn't start the worker, which it records first where
// it can.
func (b *batchJob) run(ctx context.Context) (int, error) {
	// Nothing is written to a directory another user could change.
	if err := safepath.CheckDir(b.dir, safepath.Private); err != nil {
		return exitError, err
	}
	life, end := context.WithCancel(context.WithoutCancel(ctx))
	defer end()
	stop, cancel := context.WithCancel(ctx)
	defer cancel()
	b.stop = cancel
	go b.passSignals(life)
	// A job its driver withdrew doesn't start, whose token is gone.
	if _, err := os.Lstat(filepath.Join(b.dir, batch.WithdrawnFile)); err == nil {
		return b.fail(errors.New("batch: the job was withdrawn"))
	}
	if err := b.sys.harden(); err != nil {
		if errors.Is(err, harden.ErrUnsupported) {
			return b.fail(problems.WorkerSystemUnsupported.Problem(problems.Params{"os": runtime.GOOS}))
		}
		return b.fail(problems.WorkerHardenFailed.Problem(problems.Params{"reason": err.Error()}))
	}
	path, run, digest, err := b.sys.executable()
	if err != nil {
		return b.fail(err)
	}
	spec, err := batch.ReadSpec(b.dir)
	if err != nil {
		return b.fail(err)
	}
	// The coordinator would refuse a worker of another binary's claim, after
	// it had the token: it doesn't get it.
	if spec.Digest != digest {
		return b.fail(problems.WorkerDigestMismatch.Problem(problems.Params{"placed": spec.Digest, "claimed": digest}))
	}
	var sched batch.Scheduler
	switch spec.Scheduler {
	case "slurm":
		sched = &slurm.Scheduler{Environ: b.sys.environ()}
	default:
		return b.fail(fmt.Errorf("batch: a spec of the scheduler %q", spec.Scheduler))
	}
	alloc, err := sched.Allocation(stop, b.c.getenv)
	if err != nil {
		fmt.Fprintf(b.c.stderr, "meridian batch: when the allocation ends can't be told: %v\n", err)
	}
	// The job's own start and its walltime, where the scheduler doesn't say
	// its end; or nothing, rather than a guess a check would read.
	if alloc.End.IsZero() && !alloc.Start.IsZero() && spec.WalltimeSeconds > 0 {
		alloc.End = alloc.Start.Add(time.Duration(spec.WalltimeSeconds) * time.Second)
	}
	node, _ := b.sys.hostname()
	started := &batch.Started{Job: alloc.Job, Node: node, BootID: bootID(), PID: os.Getpid(), Start: time.Now(), End: alloc.End}
	if err := batch.WriteRecord(b.dir, batch.StartedFile, started); err != nil {
		return b.fail(err)
	}
	if file := spec.Coordinator.AddressFile; file != "" {
		if p := b.relayUnreachable(stop, file, node); p != nil {
			return b.fail(p)
		}
	}
	if sig := b.signalled(); sig != nil {
		// Stopped before the worker started: it doesn't.
		return b.stopped(sig)
	}
	token, err := batch.TakeToken(b.dir)
	if err != nil {
		return b.fail(err)
	}
	cgroup, _ := b.sys.cgroup()
	oomBefore := oomKills(cgroup, b.sys.cgroupRoot, alloc.Job)
	cmd, err := b.startWorker(path, run, spec, alloc.End, token)
	if err != nil {
		// A signal as the worker started stopped it, rather than it failed
		// to take its token.
		if sig := b.signalled(); sig != nil {
			return b.stopped(sig)
		}
		return b.fail(err)
	}
	waitErr := cmd.Wait()
	exit := &batch.Exit{Status: -1, At: time.Now()}
	if st := cmd.ProcessState; st != nil {
		exit.Status = st.ExitCode()
		if ws, ok := st.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			exit.Signal = int(ws.Signal())
		}
	}
	exit.OOMKills = max(oomKills(cgroup, b.sys.cgroupRoot, alloc.Job)-oomBefore, 0)
	if err := batch.WriteRecord(b.dir, batch.ExitFile, exit); err != nil {
		return exitError, err
	}
	switch {
	case exit.Signal != 0:
		return 128 + exit.Signal, nil
	case exit.Status >= 0:
		return exit.Status, nil
	}
	return exitError, waitErr
}

// fail records err, or a stop for nil, as how the job ended, before its
// worker started, with its problem where it has one, and removes the
// token, which no worker will take now; and returns it.
func (b *batchJob) fail(err error) (int, error) {
	exit := &batch.Exit{Status: -1, At: time.Now()}
	if p, ok := errors.AsType[*problem.Problem](err); ok {
		exit.Problem = batch.Record(p)
	}
	if rerr := os.Remove(filepath.Join(b.dir, batch.TokenFile)); rerr != nil && !errors.Is(rerr, fs.ErrNotExist) {
		err = errors.Join(err, rerr)
	}
	if werr := batch.WriteRecord(b.dir, batch.ExitFile, exit); werr != nil {
		err = errors.Join(err, werr)
	}
	return exitError, err
}

// stopped records the job as stopped by sig before its worker ran, as
// fail does, and returns the status of a process sig ended.
func (b *batchJob) stopped(sig os.Signal) (int, error) {
	code, err := b.fail(nil)
	if n, ok := sig.(syscall.Signal); ok && err == nil {
		code = 128 + int(n)
	}
	return code, err
}

// signalled returns the first signal that came, or nil.
func (b *batchJob) signalled() os.Signal {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.termed
}

// startWorker starts meridian worker, from the file this process runs,
// with the job's environment and what tells it where the coordinator is,
// and when to stop, and hands it its token once it says it is hardened.
// A worker that doesn't ask for it in time is killed.
func (b *batchJob) startWorker(path, run string, spec *batch.Spec, end time.Time, token string) (*exec.Cmd, error) {
	worker, ours, err := localbackend.TokenSocket()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(run, "worker") //nolint:gosec,noctx // the file this process runs, which executable checked, and a worker that the job's signals stop
	cmd.Args[0] = path
	cmd.Dir = "/"
	cmd.Env = workerEnviron(b.sys.environ(), spec, end)
	cmd.Stdout, cmd.Stderr = b.c.stdout, b.c.stderr
	cmd.ExtraFiles = []*os.File{worker}
	err = cmd.Start()
	worker.Close()
	if err != nil {
		ours.Close()
		return nil, err
	}
	b.mu.Lock()
	b.worker = cmd.Process
	termed := b.termed
	b.mu.Unlock()
	if termed != nil {
		_ = cmd.Process.Signal(termed)
	}
	if err := localbackend.GiveToken(ours, token, b.sys.tokenWait); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, problems.WorkerTokenNotTaken.Problem(problems.Params{
			"ended": !errors.Is(err, os.ErrDeadlineExceeded), "exit": cmd.ProcessState.ExitCode(), "log": filepath.Join(b.dir, batch.OutFile),
		})
	}
	return cmd, nil
}

// passSignals passes each signal on to the worker, once it runs, as
// `scancel --batch` signals only the job's first process; records when the
// first came, and which; and stops what comes before the worker starts. It
// ends with life.
func (b *batchJob) passSignals(life context.Context) {
	for {
		var sig os.Signal
		select {
		case sig = <-b.signals:
		case <-life.Done():
			return
		}
		b.mu.Lock()
		first := b.termed == nil
		if first {
			b.termed = sig
		}
		worker := b.worker
		b.mu.Unlock()
		if first {
			n := 0
			if s, ok := sig.(syscall.Signal); ok {
				n = int(s)
			}
			_ = batch.WriteRecord(b.dir, batch.TermFile, &batch.Term{At: time.Now(), Signal: n})
			b.stop()
		}
		if worker != nil {
			_ = worker.Signal(sig)
		}
	}
}

// workerEnviron returns the worker's environment: the job's, but GODEBUG
// and MERIDIAN_ variables, and where the coordinator is, the token's
// socket, and, where the allocation's end is known, the deadline, half
// the spec's margin before it: the margin's first half started the
// worker, so a deadline the whole margin before the end would come before
// the job's own timeout:, and its second half is the cleanups' and the
// reports'.
func workerEnviron(base []string, spec *batch.Spec, end time.Time) []string {
	out := slices.DeleteFunc(slices.Clone(base), func(kv string) bool {
		name, _, _ := strings.Cut(kv, "=")
		return name == "GODEBUG" || strings.HasPrefix(name, "MERIDIAN_")
	})
	co := spec.Coordinator
	if co.AddressFile != "" {
		out = append(out, workerapi.EnvCoordinatorFile+"="+co.AddressFile)
	} else {
		out = append(out, workerapi.EnvCoordinator+"="+co.Address)
	}
	if co.KeyHash != "" {
		out = append(out, workerapi.EnvKeySHA256+"="+co.KeyHash)
	}
	if !end.IsZero() {
		deadline := end.Add(-time.Duration(spec.MarginSeconds) * time.Second / 2)
		out = append(out, workerapi.EnvDeadline+"="+strconv.FormatInt(deadline.Unix(), 10))
	}
	return append(out, workerapi.EnvBootstrapTokenFD+"=3")
}

// relayUnreachable returns scheduler_relay_unreachable where the address
// file says a relay is up, as one written within relayFresh is, and none
// of its addresses takes a connection within relayWait; and otherwise
// nil, as for a relay that is down, while the coordinator is away, which
// the worker waits for.
func (b *batchJob) relayUnreachable(ctx context.Context, file, node string) *problem.Problem {
	// A file that can't be read, or isn't one, the worker says why of.
	f, fresh := b.readRelay(file)
	if !fresh {
		return nil
	}
	deadline := time.Now().Add(b.sys.relayWait)
	var dialer net.Dialer
	for {
		for _, a := range f.Addresses {
			dctx, cancel := context.WithTimeout(ctx, min(5*time.Second, max(time.Until(deadline), time.Millisecond)))
			conn, err := dialer.DialContext(dctx, "tcp", a)
			cancel()
			if err == nil {
				conn.Close()
				return nil
			}
		}
		if ctx.Err() != nil {
			return nil //nolint:nilerr // stopped, as by a signal: not unreachable
		}
		if time.Now().After(deadline) {
			return problems.SchedulerRelayUnreachable.Problem(problems.Params{"node": node, "addresses": problems.QuoteAll(f.Addresses)})
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
		}
	}
}

// readRelay reads the address file, and reports whether it was written
// within relayFresh.
func (b *batchJob) readRelay(file string) (*workerapi.AddressFile, bool) {
	data, err := safepath.ReadFile(file, safepath.Private, workerapi.MaxAddressFile)
	if err != nil {
		return nil, false
	}
	f, err := workerapi.UnmarshalAddressFile(data)
	if err != nil {
		return nil, false
	}
	info, err := os.Stat(file)
	return f, err == nil && time.Since(info.ModTime()) <= b.sys.relayFresh
}

// bootID returns the boot's ID, where the system says one, or "".
func bootID() string {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// oomKills returns how many processes of the process's cgroup, and those
// under it, the kernel has killed for their memory, as cgroup, what
// /proc/self/cgroup says, and the files under root, the cgroup
// filesystem, say: cgroup v2's memory.events, or v1's memory.oom_control,
// of the nearest cgroup that has one, up to the job's own, job_<job>, as
// Slurm names it, and never above it; or 0 where none says.
func oomKills(cgroup, root, job string) int64 {
	if job == "" {
		return 0
	}
	for line := range strings.Lines(cgroup) {
		parts := strings.SplitN(strings.TrimSpace(line), ":", 3)
		if len(parts) != 3 {
			continue
		}
		base, file := root, "memory.events"
		switch {
		case parts[0] == "0" && parts[1] == "":
		case slices.Contains(strings.Split(parts[1], ","), "memory"):
			base, file = filepath.Join(root, "memory"), "memory.oom_control"
		default:
			continue
		}
		names := strings.Split(strings.Trim(parts[2], "/"), "/")
		top := slices.Index(names, "job_"+job)
		if top < 0 || slices.Contains(names, "..") {
			continue
		}
		for i := len(names); i > top; i-- {
			n, err := oomKill(filepath.Join(append([]string{base}, names[:i]...)...), file)
			if err == nil {
				return n
			}
			if !errors.Is(err, fs.ErrNotExist) {
				break
			}
		}
	}
	return 0
}

// oomKill reads the oom_kill line of a cgroup's memory file.
func oomKill(dir, file string) (int64, error) {
	f, err := os.Open(filepath.Join(dir, file)) //nolint:gosec // a cgroup's file
	if err != nil {
		return 0, err
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		if v, ok := strings.CutPrefix(scan.Text(), "oom_kill "); ok {
			return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		}
	}
	return 0, scan.Err()
}
