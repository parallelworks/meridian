// Package local is the local backend: it starts meridian worker for a
// job's attempt as a process of the user's on this host, detached from the
// process that placed it, which reaches the coordinator at the
// coordinator's own Unix socket (docs/design.md, The local backend).
//
// Its jobs run on the host, as the user: a member names isolation host.
// Any other isolation, the default container among them, is
// isolation_not_built, and never the host instead.
//
// A worker gets its bootstrap token on a socket it inherits, once it has
// said there that it has kept the user's other processes out of its
// memory (workerapi.EnvBootstrapTokenFD): no other process of the user's,
// another job's step among them, can read it first, as one could a file.
//
// The driver keeps its files in the state directory, in directories only
// the user can use: the worker's standard error in workers/; that it may
// still run in live/, which Probe reads; and its placement in placements/,
// which says which process it is by its ID, its start time and the boot it
// started in, so that any process of the user's, a coordinator that
// restarted among them, can find, check and cancel it, and none takes
// another process that has the same ID later for it. A clock set back or
// forward changes none of them.
//
// It runs where the worker does: Linux, macOS and FreeBSD. Elsewhere its
// Probe gives every member worker_system_unsupported, and every other call
// fails with it.
package local

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/internal/flock"
	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// DefaultKillAfter is how long Cancel waits after it sends a worker
// SIGTERM before it sends SIGKILL: time for the worker to stop its steps,
// with their grace, and send what it has.
const DefaultKillAfter = 60 * time.Second

// DefaultTokenWait is how long Place's process waits for a worker to say
// it is ready for its token, before it gives up, and the worker gets none.
const DefaultTokenWait = time.Minute

// A Driver is the local backend's backend.Driver.
type Driver struct {
	// State is the directory the driver keeps its files in: the meridian
	// command's state directory, never a run's directory.
	State string
	// Environ is the environment a worker starts with, but its MERIDIAN_
	// variables, which the driver sets itself, and those Withhold names:
	// os.Environ() for nil. The worker gives it to its job's steps.
	Environ []string
	// Withhold names the environment variables that a worker's
	// environment, and so its steps', leaves out: those that hold the
	// values of the user's secret variables, which a job reads, if it
	// does, only as the run gives them, masked in its logs. A step that
	// printed its environment would show them in the clear.
	Withhold []string
	// KillAfter is how long Cancel waits after SIGTERM before SIGKILL:
	// DefaultKillAfter for 0.
	KillAfter time.Duration
	// Launcher is a command that starts the worker, which it runs as the
	// process the placement records, as systemd-run --user --scope does:
	// the worker's arguments follow it. Its first is the command's
	// absolute path, which whoever sets it has checked
	// (safepath.CheckExecutable): it
	// gets the worker's token's socket. It is empty to start the worker
	// itself, as on the coordinator's own host.
	Launcher []string
	// AwaitToken has Place return only once the worker has taken its
	// bootstrap token, rather than hand it over once it has returned: for
	// a process that ends once it has placed a worker, as meridian host
	// does on another host, which would otherwise end before the worker
	// had its token. A worker that ends first, or doesn't ask for it in
	// time, is stopped, and its placement is worker_token_not_taken.
	AwaitToken bool
	// TokenWait is how long a worker has to say it is ready for its
	// token: DefaultTokenWait for 0.
	TokenWait time.Duration

	// swept is done once the driver has looked through what State holds,
	// the first time it places a worker or probes a member (sweep).
	swept sync.Once
	// reaping are the workers this process placed that it hasn't reaped
	// yet, and recorded the exit status of, by attempt: each closed once
	// it has, which Forget waits for, so that the status isn't written
	// after the placement is gone.
	reapMu  sync.Mutex
	reaping map[string]chan struct{}
}

var _ backend.Driver = (*Driver)(nil)

// The directories in State that the driver keeps its files in: a worker's
// standard error in workers/; its placement in placements/; and, in live/,
// a file named for each attempt whose worker may still run, which is what
// Probe looks through, rather than every placement ever kept.
const (
	workersDir    = "workers"
	placementsDir = "placements"
	liveDir       = "live"
)

// staleAfter is how long the placement of a worker that has ended is kept,
// unless the coordinator has the driver forget it sooner (Forget): one
// whose coordinator stopped before it did is removed once it is this old,
// by the next process that places a worker or probes a member.
const staleAfter = 24 * time.Hour

// bootID returns the boot's ID, which a process reads once.
var bootID = sync.OnceValues(readBootID)

// attemptPattern is an attempt's ID, which names its files.
var attemptPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// A record is what the driver keeps of a placement, in its file: which
// process the worker is, what it asked for, and which binary it runs.
type record struct {
	PID int `json:"pid"`
	// Start is when the process started, counted from the boot, and Boot
	// the boot's ID, as the system says them, so that another process that
	// has the same ID later isn't taken for it, and a clock set since
	// changes neither.
	Start      string            `json:"start"`
	Boot       string            `json:"boot"`
	Resources  backend.Resources `json:"resources,omitzero"`
	Executable string            `json:"executable"`
	// Digest is sha256: and the hex of the SHA-256 digest of the file the
	// worker runs, as the worker reports its own (workerapi.Worker), which
	// the coordinator compares with it.
	Digest string `json:"digest"`
	// RunDir is the run's directory the worker was placed with, which
	// Find gives; "" in a placement an earlier release recorded.
	RunDir string `json:"runDir,omitempty"`
}

// placement is the placement of attempt that r records.
func (r *record) placement(attempt string) backend.Placement {
	return backend.Placement{Attempt: attempt, ID: r.id(), WorkerDigest: r.Digest, RunDir: r.RunDir}
}

// id is the placement's ID: the process's, and its start time.
func (r *record) id() string { return strconv.Itoa(r.PID) + ":" + r.Start }

// alive reports whether the worker r records still runs: a process of its
// ID that started when it did, in the same boot, and hasn't ended.
func (r *record) alive() (bool, error) {
	boot, err := bootID()
	if err != nil || boot != r.Boot {
		return false, err
	}
	start, err := processStart(r.PID)
	if errors.Is(err, errNoProcess) {
		return false, nil
	}
	return err == nil && start == r.Start, err
}

// errNoProcess is processStart's error for a process that isn't there, or
// has ended and waits to be reaped.
var errNoProcess = errors.New("local: no such process")

// isolationProblem returns why m's isolation is one the driver doesn't
// give, or nil for host.
func isolationProblem(m *backend.Member) *problem.Problem {
	if m.Isolation == "host" {
		return nil
	}
	return problems.IsolationNotBuilt.Problem(problems.Params{"member": m.Name, "isolation": m.Isolation, "has_isolation": strconv.FormatBool(m.Isolation != "")})
}

// Probe says whether m can run a job that asks for demand: what it offers
// is its Resources, or, for one it doesn't declare, the host's CPUs and
// memory, and no GPU; and it starts the job now if what the live workers
// it placed asked for leaves room, but those of the attempts demand says
// are parked (backend.Demand.Parked), which wait for a child run that
// needs the room. It starts a job that doesn't fit at once too, past what
// the member offers: a member that would start one now ranks first. The
// local backend has no walltime of its own: the member's limits are
// passed on.
func (d *Driver) Probe(ctx context.Context, m *backend.Member, demand backend.Demand) (backend.Fit, error) {
	fit := backend.Fit{MaxWalltimeSeconds: m.MaxWalltimeSeconds, MarginSeconds: m.MarginSeconds, DefaultTimeoutSeconds: m.DefaultTimeoutSeconds}
	if fit.Problem = unsupported(); fit.Problem == nil {
		fit.Problem = isolationProblem(m)
	}
	if fit.Problem != nil {
		return fit, nil //nolint:nilerr // what is wrong with the member is the Fit's, not the call's
	}
	offer := m.Resources
	if offer.MilliCPU == 0 {
		offer.MilliCPU = hostMilliCPU()
	}
	if offer.Memory == 0 {
		mem, err := hostMemory()
		if err != nil {
			return backend.Fit{}, err
		}
		offer.Memory = mem
	}
	for _, r := range []struct {
		name        string
		asks, offer int64
	}{
		{"cpu", demand.Resources.MilliCPU, offer.MilliCPU},
		{"memory", demand.Resources.Memory, offer.Memory},
		{"gpu", int64(demand.Resources.GPU), int64(offer.GPU)},
	} {
		if r.asks > r.offer {
			fit.Problem = problems.ResourcesExceedMember.Problem(problems.Params{"member": m.Name, "resource": r.name, "asks": r.asks, "offers": r.offer})
			return fit, nil
		}
	}
	d.sweep()
	used, err := d.used(ctx, demand.Parked)
	if err != nil {
		return backend.Fit{}, err
	}
	fit.Availability = backend.StartsNow
	if used.MilliCPU+demand.Resources.MilliCPU > offer.MilliCPU ||
		used.Memory+demand.Resources.Memory > offer.Memory ||
		used.GPU+demand.Resources.GPU > offer.GPU {
		fit.Availability = backend.HasCapacity
	}
	return fit, nil
}

// used returns what the live workers the driver placed asked for, together:
// those live/ names, but those of the attempts parked names, whose worker
// waits for a child run. One that has ended, or whose placement is gone,
// it takes out of live/, under the attempt's lock, which Place holds as it
// places it again.
func (d *Driver) used(ctx context.Context, parked []string) (backend.Resources, error) {
	var used backend.Resources
	entries, err := os.ReadDir(filepath.Join(d.State, liveDir))
	if errors.Is(err, fs.ErrNotExist) {
		return used, nil
	} else if err != nil {
		return used, err
	}
	for _, e := range entries {
		attempt := e.Name()
		if !attemptPattern.MatchString(attempt) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return used, err
		}
		r, live, err := d.live(attempt)
		if err != nil {
			return used, err
		}
		if !live {
			if err := d.unlive(ctx, attempt); err != nil {
				return used, err
			}
			continue
		}
		if slices.Contains(parked, attempt) {
			continue
		}
		used.MilliCPU += r.Resources.MilliCPU
		used.Memory += r.Resources.Memory
		used.GPU += r.Resources.GPU
	}
	return used, nil
}

// live returns the attempt's record, and whether its worker still runs.
func (d *Driver) live(attempt string) (*record, bool, error) {
	r, ok, err := d.read(attempt)
	if err != nil || !ok {
		return nil, false, err
	}
	alive, err := r.alive()
	return r, alive, err
}

// unlive takes the attempt out of live/ once its worker has ended, or its
// placement is gone: under the attempt's lock, so that a Place of it again
// meanwhile, which marks it live once it has started its worker, isn't
// undone.
func (d *Driver) unlive(ctx context.Context, attempt string) error {
	unlock, err := flock.Lock(ctx, d.path(attempt, ".lock"))
	if err != nil {
		return err
	}
	defer unlock()
	if _, live, err := d.live(attempt); err != nil || live {
		return err
	}
	if err := os.Remove(d.livePath(attempt)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// livePath returns the path of the attempt's file in live/.
func (d *Driver) livePath(attempt string) string {
	return filepath.Join(d.State, liveDir, attempt)
}

// sweep looks through placements/, once a driver, the first time it places
// a worker or probes a member: a placement whose worker runs, as one that a
// release before live/ placed, it marks live; one whose worker ended more
// than staleAfter ago, which no coordinator had the driver forget, it
// removes, with its worker's log. What it can't do, the next process does.
func (d *Driver) sweep() {
	d.swept.Do(func() {
		if safepath.EnsureDir(filepath.Join(d.State, liveDir), safepath.Private) != nil {
			return
		}
		entries, err := os.ReadDir(filepath.Join(d.State, placementsDir))
		if err != nil {
			return
		}
		for _, e := range entries {
			attempt, ok := strings.CutSuffix(e.Name(), ".json")
			if !ok || !attemptPattern.MatchString(attempt) {
				continue
			}
			_, live, err := d.live(attempt)
			switch {
			case err != nil:
			case live:
				_ = d.markLive(attempt)
			default:
				if info, err := e.Info(); err == nil && time.Since(info.ModTime()) > staleAfter {
					d.forget(attempt)
				}
			}
		}
	})
}

// markLive puts the attempt in live/, as one whose worker runs. Nothing
// is synced: a worker doesn't outlive the host's crash, and a later boot
// takes none for live.
func (d *Driver) markLive(attempt string) error {
	f, err := os.OpenFile(d.livePath(attempt), os.O_WRONLY|os.O_CREATE|openNoFollow, 0o600)
	if err != nil {
		return err
	}
	return f.Close()
}

// Connect returns coordinator itself: a local worker reaches the
// coordinator at its own socket. There is nothing to close.
func (d *Driver) Connect(_ context.Context, _ *backend.Member, coordinator backend.Endpoint) (backend.Endpoint, io.Closer, error) {
	if p := unsupported(); p != nil {
		return backend.Endpoint{}, nil, p
	}
	return coordinator, nopCloser{}, nil
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

// Place starts meridian worker for req's attempt: the file this process
// runs, once it has checked that no other user can change it, with the
// user's environment but its MERIDIAN_ variables, in a session of its own,
// so that a terminal's signals don't reach it, with its standard error in
// workers/, and its token on a socket it inherits, which this process
// writes once the worker says it is ready. It records the placement, and
// returns once the worker has started. An attempt whose worker runs
// already is found, and not placed again, however many processes place it
// at once.
func (d *Driver) Place(ctx context.Context, m *backend.Member, req backend.PlaceRequest) (backend.Placement, error) {
	if p := unsupported(); p != nil {
		return backend.Placement{}, p
	}
	if p := isolationProblem(m); p != nil {
		return backend.Placement{}, p
	}
	if !attemptPattern.MatchString(req.Attempt) {
		return backend.Placement{}, fmt.Errorf("local: an attempt's ID that can't name a file: %q", req.Attempt)
	}
	for _, dir := range []string{workersDir, placementsDir, liveDir} {
		if err := safepath.EnsureDir(filepath.Join(d.State, dir), safepath.Private); err != nil {
			return backend.Placement{}, err
		}
	}
	d.sweep()
	// One process at a time places an attempt, from finding its placement
	// to recording a new one.
	unlock, err := flock.Lock(ctx, d.path(req.Attempt, ".lock"))
	if err != nil {
		return backend.Placement{}, err
	}
	defer unlock()
	if r, ok, err := d.read(req.Attempt); err != nil {
		return backend.Placement{}, err
	} else if ok {
		alive, err := r.alive()
		if err != nil {
			return backend.Placement{}, err
		}
		if alive {
			return r.placement(req.Attempt), nil
		}
		d.remove(req.Attempt)
	}
	path, exe, digest, err := executable()
	if err != nil {
		return backend.Placement{}, err
	}
	if len(d.Launcher) > 0 {
		// The launcher runs the worker, as a process of its own, in which
		// the file this one runs has no name but its path: one that took
		// its path since was checked as this one was, and the worker's
		// digest, which the coordinator compares with this one's, tells it
		// apart.
		exe = path
	}
	worker, ours, err := socketPair()
	if err != nil {
		return backend.Placement{}, err
	}
	stderr, err := os.OpenFile(filepath.Join(d.State, workersDir, req.Attempt+".log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND|openNoFollow, 0o600)
	if err != nil {
		worker.Close()
		ours.Close()
		return backend.Placement{}, err
	}
	// The worker outlives this call, and the process that placed it: it
	// isn't started with the call's context.
	args := append(slices.Clone(d.Launcher), exe, "worker")
	cmd := exec.Command(args[0], args[1:]...) //nolint:gosec,noctx // the file this process runs, which Executable checked, behind the driver's launcher, and a worker that outlives the call
	cmd.Dir = "/"
	cmd.Env = d.environ(req.Coordinator)
	cmd.Stderr = stderr
	cmd.ExtraFiles = []*os.File{worker}
	detach(cmd)
	err = cmd.Start()
	worker.Close()
	if err != nil {
		stderr.Close()
		ours.Close()
		return backend.Placement{}, err
	}
	r := &record{PID: cmd.Process.Pid, Resources: req.Resources, Executable: exe, Digest: digest, RunDir: req.RunDir}
	r.Start, err = processStart(r.PID)
	if err == nil {
		r.Boot, err = bootID()
	}
	if err == nil {
		err = safepath.WriteFile(d.path(req.Attempt, ".json"), mustMarshal(r))
	}
	if err == nil {
		err = d.markLive(req.Attempt)
	}
	if err != nil {
		// Without its record, the worker couldn't be found again: it
		// doesn't run, and gets no token.
		ours.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		stderr.Close()
		return backend.Placement{}, err
	}
	if !d.AwaitToken {
		go func() { _ = giveToken(ours, req.BootstrapToken, d.tokenWait()) }()
	} else if err := giveToken(ours, req.BootstrapToken, d.tokenWait()); err != nil {
		// A worker that didn't take its token runs no job: one that
		// hasn't ended is stopped, and the placement goes, but for what
		// the worker wrote, which says why.
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		stderr.Close()
		for _, path := range []string{d.livePath(req.Attempt), d.path(req.Attempt, ".json")} {
			_ = os.Remove(path)
		}
		exit := -1
		if cmd.ProcessState != nil {
			exit = cmd.ProcessState.ExitCode()
		}
		return backend.Placement{}, problems.WorkerTokenNotTaken.Problem(problems.Params{
			"ended": !errors.Is(err, os.ErrDeadlineExceeded), "exit": exit, "log": filepath.Join(d.State, workersDir, req.Attempt+".log"),
		})
	}
	// The worker is reaped while this process runs, and its exit status
	// kept beside its placement; after, the system reaps it.
	reaped := make(chan struct{})
	d.reapMu.Lock()
	if d.reaping == nil {
		d.reaping = map[string]chan struct{}{}
	}
	d.reaping[req.Attempt] = reaped
	d.reapMu.Unlock()
	go func() {
		defer func() {
			d.reapMu.Lock()
			if d.reaping[req.Attempt] == reaped {
				delete(d.reaping, req.Attempt)
			}
			d.reapMu.Unlock()
			close(reaped)
		}()
		_ = cmd.Wait()
		stderr.Close()
		if cmd.ProcessState != nil {
			exit := []byte(strconv.Itoa(cmd.ProcessState.ExitCode()) + "\n")
			_ = safepath.WriteFile(d.path(req.Attempt, ".exit"), exit)
		}
	}()
	return r.placement(req.Attempt), nil
}

// tokenWait is how long a worker has to say it is ready for its token.
func (d *Driver) tokenWait() time.Duration {
	if d.TokenWait > 0 {
		return d.TokenWait
	}
	return DefaultTokenWait
}

// socketPair returns the two ends of a new stream Unix socket: the
// worker's, which it inherits, and this process's, neither of which any
// other process gets.
func socketPair() (worker, ours *os.File, err error) {
	fds, err := socketpair()
	if err != nil {
		return nil, nil, err
	}
	return os.NewFile(uintptr(fds[0]), "worker's token"), os.NewFile(uintptr(fds[1]), "token"), nil
}

// giveToken writes token to ours, once the worker has written that it is
// ready on its end, and closes it. A worker that doesn't say so within
// wait, which is os.ErrDeadlineExceeded, or ends first, gets no
// token.
func giveToken(ours *os.File, token string, wait time.Duration) error {
	conn, err := net.FileConn(ours)
	ours.Close()
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(wait)); err != nil {
		return err
	}
	ready := make([]byte, 1)
	if _, err := io.ReadFull(conn, ready); err != nil {
		return err
	}
	if ready[0] != '1' {
		return errors.New("local: the worker said other than that it was ready")
	}
	_, err = conn.Write([]byte(token))
	return err
}

// Executable returns the path of the file this process runs, a worker the
// driver places runs, and its digest, once it has checked that the file is
// a regular file of the user's or root's that no other user can change, nor
// any directory on the way to it. Otherwise it is worker_binary_unsafe.
func Executable() (string, string, error) {
	path, _, digest, err := executable()
	return path, digest, err
}

// executable is Executable, with the path to start the worker with, the
// file this process runs, even if another has taken its path since, where
// the system names it (runningExecutable).
func executable() (path, run, digest string, err error) {
	if path, err = os.Executable(); err != nil {
		return "", "", "", err
	}
	if path, err = filepath.EvalSymlinks(path); err != nil {
		return "", "", "", err
	}
	if run, err = runningExecutable(path); err != nil {
		return "", "", "", err
	}
	digest, err = checkExecutable(path, run)
	return path, run, digest, err
}

// A fileIdentity is which file a file is, and what it holds, as far as
// fstat says: its device and inode, its size, and when its data and its
// status last changed, in nanoseconds.
type fileIdentity struct {
	dev, ino           uint64
	size, mtime, ctime int64
}

// digests keeps the digest of the file this process runs, the last time
// checkExecutable hashed it, by the file's identity, so that a placement
// doesn't hash the binary, tens of megabytes, again unless it changed.
var digests struct {
	sync.Mutex
	id     fileIdentity
	digest string
}

// settled is how long ago a file's status must have last changed for its
// digest to be kept (digests): a variable a test sets.
var settled = time.Second

// hashFile returns the digest of what r holds, as sha256: and the hex of
// its SHA-256 digest: a variable a test counts calls of.
var hashFile = func(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// checkExecutable checks that no other user can change the way to path,
// and that the file open opens, the file at path or the one the process
// runs, is a regular file of the user's or root's that no other user can
// change, and returns its digest.
//
// The checks run every time. On Linux, where open is /proc/self/exe, the
// file this process runs, which nothing can write while it runs, the
// digest is kept, and given again, while that file has the identity it had
// when it was hashed: the same device, inode and size, and the same times
// its data and its status last changed. Writing a file changes both times,
// and a process can set its data's back (utimes), but not its status's,
// which any change sets to the time it happens, utimes among them; and a
// digest is kept only once its file's status last changed a second or more
// before it was hashed, so a change after it gives a status time that
// differs. Elsewhere the file at path is hashed at each placement
// (identity).
func checkExecutable(path, open string) (string, error) {
	f, info, err := safepath.OpenExecutable(path, open)
	if p, ok := errors.AsType[*problem.Problem](err); ok {
		return "", problems.WorkerBinaryUnsafe.Problem(problems.Params{"path": path, "code": string(p.Code)})
	}
	if err != nil {
		return "", err
	}
	defer f.Close()
	id, known := identity(info)
	if known {
		digests.Lock()
		digest := digests.digest
		hit := digest != "" && digests.id == id
		digests.Unlock()
		if hit {
			return digest, nil
		}
	}
	digest, err := hashFile(f)
	if err != nil {
		return "", err
	}
	if known && time.Since(time.Unix(0, id.ctime)) >= settled {
		digests.Lock()
		digests.id, digests.digest = id, digest
		digests.Unlock()
	}
	return digest, nil
}

// environ returns a worker's environment: d's, but its MERIDIAN_
// variables and those d withholds, and where the coordinator is and the
// token's socket, which the worker inherits as its descriptor 3.
func (d *Driver) environ(coordinator backend.Endpoint) []string {
	base := d.Environ
	if base == nil {
		base = os.Environ()
	}
	out := make([]string, 0, len(base)+3)
	for _, kv := range base {
		name, _, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(name, "MERIDIAN_") && !slices.Contains(d.Withhold, name) {
			out = append(out, kv)
		}
	}
	out = append(out, workerapi.EnvCoordinator+"="+coordinator.Address)
	if coordinator.KeyHash != "" {
		out = append(out, workerapi.EnvKeySHA256+"="+coordinator.KeyHash)
	}
	return append(out, workerapi.EnvBootstrapTokenFD+"=3")
}

// Find returns the placement of an attempt, from its file in placements/.
func (d *Driver) Find(_ context.Context, _ *backend.Member, attempt string) (backend.Placement, bool, error) {
	if p := unsupported(); p != nil {
		return backend.Placement{}, false, p
	}
	r, ok, err := d.read(attempt)
	if err != nil || !ok {
		return backend.Placement{}, false, err
	}
	return r.placement(attempt), true, nil
}

// Check says whether a placement's worker runs: whether a process of its
// ID that started when it did, in the same boot, hasn't ended. One that
// has is Dead, withdrawn if Cancel stopped it, and so is a placement
// without its file.
func (d *Driver) Check(_ context.Context, _ *backend.Member, p backend.Placement) (backend.Status, error) {
	if pr := unsupported(); pr != nil {
		return backend.Status{}, pr
	}
	r, ok, err := d.read(p.Attempt)
	if err != nil {
		return backend.Status{State: backend.Unknown}, err
	}
	if !ok || r.id() != p.ID {
		return backend.Status{State: backend.Dead}, nil
	}
	alive, err := r.alive()
	switch {
	case err != nil:
		return backend.Status{State: backend.Unknown}, err
	case alive:
		return backend.Status{State: backend.Running}, nil
	}
	end := backend.EndExited
	if _, err := os.Lstat(d.path(p.Attempt, ".term")); err == nil {
		end = backend.EndWithdrawn
	}
	return backend.Status{State: backend.Dead, End: end}, nil
}

// Cancel stops a placement's worker: it sends it SIGTERM, which stops its
// steps with their grace and sends what it has, and SIGKILL if it still
// runs KillAfter later, counted from the first Cancel, by any process. It
// returns once the worker has ended, or ctx is done. A worker SIGKILL ends
// can't stop its steps' process groups. It signals only the process the
// placement records, never another that has its ID since; a placement
// that has ended, or that isn't there, is no error, and isn't withdrawn.
func (d *Driver) Cancel(ctx context.Context, _ *backend.Member, p backend.Placement) error {
	if pr := unsupported(); pr != nil {
		return pr
	}
	r, ok, err := d.read(p.Attempt)
	if err != nil || !ok || r.id() != p.ID {
		return err
	}
	if alive, err := r.alive(); err != nil || !alive {
		return err
	}
	termed, err := d.terminate(p.Attempt, r)
	if err != nil {
		return err
	}
	killAt := termed.Add(d.killAfter())
	var killedAt time.Time
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		alive, err := r.alive()
		if err != nil {
			return err
		}
		switch {
		case !alive:
			return nil
		case killedAt.IsZero() && !time.Now().Before(killAt):
			if err := signalWorker(r, sigKill); err != nil && !errors.Is(err, errNoProcess) {
				return err
			}
			killedAt = time.Now()
		case !killedAt.IsZero() && time.Since(killedAt) > 10*time.Second:
			// Killed, and not yet reaped by whoever its parent is.
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// terminate sends r's worker SIGTERM, the first time an attempt is
// cancelled, and returns when that was, which a file beside its placement
// keeps for a later Cancel.
func (d *Driver) terminate(attempt string, r *record) (time.Time, error) {
	path := d.path(attempt, ".term")
	if data, err := safepath.ReadFile(path, safepath.Private, 64); err == nil {
		if t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(data))); err == nil {
			return t, nil
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return time.Time{}, err
	}
	now := time.Now()
	if err := safepath.WriteFile(path, []byte(now.Format(time.RFC3339Nano)+"\n")); err != nil {
		return time.Time{}, err
	}
	if err := signalWorker(r, sigTerm); err != nil && !errors.Is(err, errNoProcess) {
		return now, err
	}
	return now, nil
}

// Forget removes what the driver keeps of a placement whose worker has
// ended, once the coordinator has recorded how it ended: its file, its
// exit status, when Cancel stopped it, and the worker's log. A placement
// whose worker still runs is kept, and an error. One whose worker this
// process placed it waits to have reaped, as it does at once once the
// worker has ended.
func (d *Driver) Forget(ctx context.Context, _ *backend.Member, p backend.Placement) error {
	if pr := unsupported(); pr != nil {
		return pr
	}
	r, ok, err := d.read(p.Attempt)
	if err != nil || !ok || r.id() != p.ID {
		return err
	}
	if alive, err := r.alive(); err != nil {
		return err
	} else if alive {
		return fmt.Errorf("local: the worker of %s still runs", p.Attempt)
	}
	d.reapMu.Lock()
	reaped := d.reaping[p.Attempt]
	d.reapMu.Unlock()
	if reaped != nil {
		select {
		case <-reaped:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	d.forget(p.Attempt)
	return nil
}

// forget removes the attempt's files, its lock among them.
func (d *Driver) forget(attempt string) {
	d.remove(attempt)
	_ = os.Remove(d.path(attempt, ".lock"))
}

func (d *Driver) killAfter() time.Duration {
	if d.KillAfter == 0 {
		return DefaultKillAfter
	}
	return d.KillAfter
}

// path returns the path of an attempt's file in placements/ that ends in
// suffix.
func (d *Driver) path(attempt, suffix string) string {
	return filepath.Join(d.State, placementsDir, attempt+suffix)
}

// read reads an attempt's record, and reports false if it has none.
func (d *Driver) read(attempt string) (*record, bool, error) {
	if !attemptPattern.MatchString(attempt) {
		return nil, false, nil
	}
	data, err := safepath.ReadFile(d.path(attempt, ".json"), safepath.Private, 64<<10)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	r := new(record)
	if err := jsonv2.Unmarshal(data, r, jsonv2.RejectUnknownMembers(true)); err != nil {
		return nil, false, fmt.Errorf("local: the placement of %s: %w", attempt, err)
	}
	return r, true, nil
}

// mustMarshal encodes a record, which can't fail.
func mustMarshal(r *record) []byte {
	data, err := jsonv2.Marshal(r, jsonv2.Deterministic(true))
	if err != nil {
		panic(err) //nolint:forbidigo // a record of strings and numbers always encodes
	}
	return data
}

// remove removes an attempt's files, but its lock.
func (d *Driver) remove(attempt string) {
	for _, path := range []string{
		d.livePath(attempt), d.path(attempt, ".term"), d.path(attempt, ".exit"), d.path(attempt, ".json"),
		filepath.Join(d.State, workersDir, attempt+".log"),
	} {
		_ = os.Remove(path)
	}
}
