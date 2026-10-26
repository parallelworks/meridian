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
// the user can use: the worker's standard error in workers/, and its
// placement in placements/, which says which process it is by its ID, its
// start time and the boot it started in, so that any process of the
// user's, a coordinator that restarted among them, can find, check and
// cancel it, and none takes another process that has the same ID later for
// it. A clock set back or forward changes none of them.
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
	"strconv"
	"strings"
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

// tokenWait is how long Place's process waits for a worker to say it is
// ready for its token, before it gives up, and the worker gets none.
const tokenWait = time.Minute

// A Driver is the local backend's backend.Driver.
type Driver struct {
	// State is the directory the driver keeps its files in: the meridian
	// command's state directory, never a run's directory.
	State string
	// Environ is the environment a worker starts with, but its MERIDIAN_
	// variables, which the driver sets itself: os.Environ() for nil.
	Environ []string
	// KillAfter is how long Cancel waits after SIGTERM before SIGKILL:
	// DefaultKillAfter for 0.
	KillAfter time.Duration
}

var _ backend.Driver = (*Driver)(nil)

// The directories in State that the driver keeps its files in.
const (
	workersDir    = "workers"
	placementsDir = "placements"
)

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
	return problems.IsolationNotBuilt.Problem(problems.Params{"member": m.Name, "isolation": m.Isolation, "has_isolation": m.Isolation != ""})
}

// Probe says whether m can run a job that asks for demand: what it offers
// is its Resources, or, for one it doesn't declare, the host's CPUs and
// memory, and no GPU; and it starts the job now if what the live workers
// it placed asked for leaves room. The local backend has no walltime of
// its own: the member's limits are passed on.
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
	used, err := d.used(ctx)
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

// used returns what the live workers the driver placed asked for, together.
func (d *Driver) used(ctx context.Context) (backend.Resources, error) {
	var used backend.Resources
	entries, err := os.ReadDir(filepath.Join(d.State, placementsDir))
	if errors.Is(err, fs.ErrNotExist) {
		return used, nil
	} else if err != nil {
		return used, err
	}
	for _, e := range entries {
		attempt, ok := strings.CutSuffix(e.Name(), ".json")
		if !ok || !attemptPattern.MatchString(attempt) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return used, err
		}
		r, ok, err := d.read(attempt)
		if err != nil {
			return used, err
		}
		if !ok {
			continue
		}
		if alive, err := r.alive(); err != nil {
			return used, err
		} else if alive {
			used.MilliCPU += r.Resources.MilliCPU
			used.Memory += r.Resources.Memory
			used.GPU += r.Resources.GPU
		}
	}
	return used, nil
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
	for _, dir := range []string{workersDir, placementsDir} {
		if err := safepath.EnsureDir(filepath.Join(d.State, dir), safepath.Private); err != nil {
			return backend.Placement{}, err
		}
	}
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
			return backend.Placement{Attempt: req.Attempt, ID: r.id()}, nil
		}
		d.remove(req.Attempt)
	}
	exe, digest, err := executable()
	if err != nil {
		return backend.Placement{}, err
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
	cmd := exec.Command(exe, "worker") //nolint:gosec,noctx // the file this process runs, which executable checked, and a worker that outlives the call
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
	r := &record{PID: cmd.Process.Pid, Resources: req.Resources, Executable: exe, Digest: digest}
	r.Start, err = processStart(r.PID)
	if err == nil {
		r.Boot, err = bootID()
	}
	if err == nil {
		err = d.writeFile(d.path(req.Attempt, ".json"), mustMarshal(r))
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
	go giveToken(ours, req.BootstrapToken)
	// The worker is reaped while this process runs, and its exit status
	// kept beside its placement; after, the system reaps it.
	go func() {
		_ = cmd.Wait()
		stderr.Close()
		if cmd.ProcessState != nil {
			exit := []byte(strconv.Itoa(cmd.ProcessState.ExitCode()) + "\n")
			_ = d.writeFile(d.path(req.Attempt, ".exit"), exit)
		}
	}()
	return backend.Placement{Attempt: req.Attempt, ID: r.id()}, nil
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
// tokenWait, or ends first, gets no token.
func giveToken(ours *os.File, token string) {
	conn, err := net.FileConn(ours)
	ours.Close()
	if err != nil {
		return
	}
	defer conn.Close()
	if conn.SetDeadline(time.Now().Add(tokenWait)) != nil {
		return
	}
	ready := make([]byte, 1)
	if _, err := io.ReadFull(conn, ready); err != nil || ready[0] != '1' {
		return
	}
	_, _ = conn.Write([]byte(token))
}

// executable returns the path to start the worker with, the file this
// process runs, and its digest, once it has checked that the file is a
// regular file of the user's or root's that no other user can change, nor
// any directory on the way to it. Otherwise it is worker_binary_unsafe.
func executable() (string, string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", "", err
	}
	if exe, err = filepath.EvalSymlinks(exe); err != nil {
		return "", "", err
	}
	run, err := runningExecutable(exe)
	if err != nil {
		return "", "", err
	}
	digest, err := checkExecutable(exe, run)
	return run, digest, err
}

// checkExecutable checks that no other user can change the way to path,
// and that the file open opens, the file at path or the one the process
// runs, is a regular file of the user's or root's that no other user can
// change, and returns its digest.
func checkExecutable(path, open string) (string, error) {
	unsafe := func(code problem.Code) error {
		return problems.WorkerBinaryUnsafe.Problem(problems.Params{"path": path, "code": string(code)})
	}
	if err := safepath.Check(path, false); err != nil {
		if p, ok := errors.AsType[*problem.Problem](err); ok {
			return "", unsafe(p.Code)
		}
		return "", err
	}
	f, err := os.Open(open) //nolint:gosec // this binary, whose way safepath checked
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	owner, known := safepath.Owner(info)
	switch {
	case !info.Mode().IsRegular():
		return "", unsafe(problems.FileNotRegular.Code())
	case !known:
		return "", unsafe(problems.OwnerUnknown.Code())
	case owner != os.Getuid() && owner != 0:
		return "", unsafe(problems.FileNotOwned.Code())
	case info.Mode().Perm()&0o022 != 0:
		return "", unsafe(problems.FileChangeable.Code())
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// environ returns a worker's environment: d's, but its MERIDIAN_
// variables, and where the coordinator is and the token's socket, which
// the worker inherits as its descriptor 3.
func (d *Driver) environ(coordinator backend.Endpoint) []string {
	base := d.Environ
	if base == nil {
		base = os.Environ()
	}
	out := make([]string, 0, len(base)+3)
	for _, kv := range base {
		if !strings.HasPrefix(kv, "MERIDIAN_") {
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
	return backend.Placement{Attempt: attempt, ID: r.id()}, true, nil
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
	if err := d.writeFile(path, []byte(now.Format(time.RFC3339Nano)+"\n")); err != nil {
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
// whose worker still runs is kept, and an error.
func (d *Driver) Forget(_ context.Context, _ *backend.Member, p backend.Placement) error {
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
	d.remove(p.Attempt)
	_ = os.Remove(d.path(p.Attempt, ".lock"))
	return nil
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

// writeFile writes a file in placements/ whole or not at all, readable
// only by the user: to a new file beside it, synced, then renamed over it,
// and the directory synced.
func (d *Driver) writeFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".new-*")
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		os.Remove(f.Name())
		return err
	}
	return syncDir(dir)
}

// syncDir syncs the directory dir, so that what was renamed in it lasts.
func syncDir(dir string) error {
	f, err := os.Open(dir) //nolint:gosec // placements/, in the state directory
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// remove removes an attempt's files, but its lock.
func (d *Driver) remove(attempt string) {
	for _, path := range []string{
		d.path(attempt, ".term"), d.path(attempt, ".exit"), d.path(attempt, ".json"),
		filepath.Join(d.State, workersDir, attempt+".log"),
	} {
		_ = os.Remove(path)
	}
}
