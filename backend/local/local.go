// Package local is the local backend: it starts meridian worker for a
// job's attempt as a process of the user's on this host, detached from the
// process that placed it, which reaches the coordinator at the
// coordinator's own Unix socket (docs/design.md, The local backend).
//
// Its jobs run on the host, as the user: a member names isolation host.
// Any other isolation, the default container among them, is
// isolation_not_built, and never the host instead.
//
// It keeps its files in the state directory, each in a directory only the
// user can use: an attempt's bootstrap token in tokens/, until the worker
// reads and removes it; the worker's standard error in workers/; and its
// placement in placements/, which says which process it is by its ID, its
// start time and the boot it started in, so that any process of the
// user's, a coordinator that restarted among them, can find, check and
// cancel it, and none takes another process that has the same ID later for
// it.
//
// It runs where the worker does: Linux, macOS and FreeBSD. Elsewhere its
// Probe gives every member worker_system_unsupported, and every other call
// fails with it.
package local

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// DefaultKillAfter is how long Cancel waits after it sends a worker
// SIGTERM before it sends SIGKILL: time for the worker to stop its steps,
// with their grace, and send what it has.
const DefaultKillAfter = 60 * time.Second

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
	tokensDir     = "tokens"
	workersDir    = "workers"
	placementsDir = "placements"
)

// attemptPattern is an attempt's ID, which names its files.
var attemptPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// A record is what the driver keeps of a placement, in its file: which
// process the worker is, the token file it was started with, what it
// asked for, and which binary it runs.
type record struct {
	PID int `json:"pid"`
	// Start is the process's start time, as the system says it, and Boot
	// the boot it started in, so that another process that has the same
	// ID later isn't taken for it.
	Start string `json:"start"`
	Boot  string `json:"boot"`
	// Token is the path of the file the worker was given its bootstrap
	// token in, which it removes once it has read it.
	Token      string            `json:"token"`
	Resources  backend.Resources `json:"resources,omitzero"`
	Executable string            `json:"executable"`
	// Digest is sha256: and the hex of the SHA-256 digest of Executable,
	// as the worker reports its own.
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

// Place starts meridian worker for req's attempt: this binary, once it has
// checked that no other user can change it, with the token in a file only
// the user can read, the user's environment but its MERIDIAN_ variables,
// in a session of its own, so that a terminal's signals don't reach it,
// and its standard error in workers/. It records the placement, and
// returns once the worker has started. An attempt whose worker runs
// already is found, and not placed again.
func (d *Driver) Place(_ context.Context, m *backend.Member, req backend.PlaceRequest) (backend.Placement, error) {
	if p := unsupported(); p != nil {
		return backend.Placement{}, p
	}
	if p := isolationProblem(m); p != nil {
		return backend.Placement{}, p
	}
	if !attemptPattern.MatchString(req.Attempt) {
		return backend.Placement{}, fmt.Errorf("local: an attempt's ID that can't name a file: %q", req.Attempt)
	}
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
		d.forget(req.Attempt, r)
	}
	exe, digest, err := executable()
	if err != nil {
		return backend.Placement{}, err
	}
	for _, dir := range []string{tokensDir, workersDir, placementsDir} {
		if err := safepath.EnsureDir(filepath.Join(d.State, dir), safepath.Private); err != nil {
			return backend.Placement{}, err
		}
	}
	token, err := d.writeToken(req.BootstrapToken)
	if err != nil {
		return backend.Placement{}, err
	}
	stderr, err := os.OpenFile(filepath.Join(d.State, workersDir, req.Attempt+".log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND|openNoFollow, 0o600)
	if err != nil {
		os.Remove(token)
		return backend.Placement{}, err
	}
	// The worker outlives this call, and the process that placed it: it
	// isn't started with the call's context.
	cmd := exec.Command(exe, "worker") //nolint:gosec,noctx // this binary, which executable checked, and a worker that outlives the call
	cmd.Dir = "/"
	cmd.Env = d.environ(req.Coordinator, token)
	cmd.Stderr = stderr
	detach(cmd)
	if err := cmd.Start(); err != nil {
		stderr.Close()
		os.Remove(token)
		return backend.Placement{}, err
	}
	r := &record{PID: cmd.Process.Pid, Token: token, Resources: req.Resources, Executable: exe, Digest: digest}
	r.Start, err = processStart(r.PID)
	if err == nil {
		r.Boot, err = bootID()
	}
	if err == nil {
		err = d.writeRecord(req.Attempt, r)
	}
	if err != nil {
		// Without its record, the worker couldn't be found again: it
		// doesn't run.
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		stderr.Close()
		os.Remove(token)
		return backend.Placement{}, err
	}
	// The worker is reaped while this process runs, and its exit status
	// kept beside its placement; after, the system reaps it.
	go func() {
		_ = cmd.Wait()
		stderr.Close()
		if cmd.ProcessState != nil {
			exit := []byte(strconv.Itoa(cmd.ProcessState.ExitCode()) + "\n")
			_ = d.writeFile(filepath.Join(d.State, placementsDir, req.Attempt+".exit"), exit)
		}
	}()
	return backend.Placement{Attempt: req.Attempt, ID: r.id()}, nil
}

// executable returns the path of this binary, with its symbolic links
// resolved, once it has checked that it is a regular file of the user's or
// root's that no other user can change, nor the directories on the way to
// it, and its digest. Otherwise it is worker_binary_unsafe.
func executable() (string, string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", "", err
	}
	if exe, err = filepath.EvalSymlinks(exe); err != nil {
		return "", "", err
	}
	return checkExecutable(exe)
}

// checkExecutable checks exe as executable does, and returns it and its
// digest.
func checkExecutable(exe string) (string, string, error) {
	unsafe := func(k *problems.Kind) error {
		return problems.WorkerBinaryUnsafe.Problem(problems.Params{"path": exe, "code": string(k.Code())})
	}
	if err := safepath.Check(exe, false); err != nil {
		if p, ok := errors.AsType[*problem.Problem](err); ok {
			return "", "", problems.WorkerBinaryUnsafe.Problem(problems.Params{"path": exe, "code": string(p.Code)})
		}
		return "", "", err
	}
	f, err := os.OpenFile(exe, os.O_RDONLY|openNoFollow, 0) //nolint:gosec // this binary, whose way safepath checked
	if err != nil {
		return "", "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", "", err
	}
	owner, known := safepath.Owner(info)
	switch {
	case !info.Mode().IsRegular():
		return "", "", unsafe(problems.FileNotRegular)
	case !known:
		return "", "", unsafe(problems.OwnerUnknown)
	case owner != os.Getuid() && owner != 0:
		return "", "", unsafe(problems.FileNotOwned)
	case info.Mode().Perm()&0o022 != 0:
		return "", "", unsafe(problems.FileChangeable)
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", "", err
	}
	return exe, "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// writeToken writes token to a new file in tokens/, which only the user
// can read, never through a symbolic link nor over a file that is there,
// and returns its path.
func (d *Driver) writeToken(token string) (string, error) {
	name := make([]byte, 16)
	if _, err := rand.Read(name); err != nil {
		return "", err
	}
	path := filepath.Join(d.State, tokensDir, hex.EncodeToString(name)+".token")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|openNoFollow, 0o600) //nolint:gosec // a new file of a random name, in tokens/
	if err != nil {
		return "", err
	}
	_, err = f.WriteString(token)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(path)
		return "", err
	}
	return path, nil
}

// environ returns a worker's environment: d's, but its MERIDIAN_
// variables, and where the coordinator is and the token's file.
func (d *Driver) environ(coordinator backend.Endpoint, token string) []string {
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
	return append(out, workerapi.EnvBootstrapTokenFile+"="+token)
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
// without its file. The token file of a worker that ended without reading
// it is removed.
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
	d.removeToken(r)
	end := backend.EndExited
	if _, err := os.Lstat(d.termPath(p.Attempt)); err == nil {
		end = backend.EndWithdrawn
	}
	return backend.Status{State: backend.Dead, End: end}, nil
}

// Cancel stops a placement's worker: it sends it SIGTERM, which stops its
// steps with their grace and sends what it has, and SIGKILL if it still
// runs KillAfter later, counted from the first Cancel, by any process. It
// returns once the worker has ended, or ctx is done. A worker SIGKILL ends
// can't stop its steps' process groups. A placement that has ended, or
// that isn't there, is no error.
func (d *Driver) Cancel(ctx context.Context, _ *backend.Member, p backend.Placement) error {
	if pr := unsupported(); pr != nil {
		return pr
	}
	r, ok, err := d.read(p.Attempt)
	if err != nil || !ok || r.id() != p.ID {
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
			d.removeToken(r)
			return nil
		case killedAt.IsZero() && !time.Now().Before(killAt):
			if err := signalProcess(r.PID, sigKill); err != nil && !errors.Is(err, errNoProcess) {
				return err
			}
			killedAt = time.Now()
		case !killedAt.IsZero() && time.Since(killedAt) > 10*time.Second:
			// Killed, and not yet reaped by whoever its parent is.
			d.removeToken(r)
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
	path := d.termPath(attempt)
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
	if alive, err := r.alive(); err != nil || !alive {
		return now, err
	}
	if err := signalProcess(r.PID, sigTerm); err != nil && !errors.Is(err, errNoProcess) {
		return now, err
	}
	return now, nil
}

func (d *Driver) killAfter() time.Duration {
	if d.KillAfter == 0 {
		return DefaultKillAfter
	}
	return d.KillAfter
}

func (d *Driver) recordPath(attempt string) string {
	return filepath.Join(d.State, placementsDir, attempt+".json")
}

func (d *Driver) termPath(attempt string) string {
	return filepath.Join(d.State, placementsDir, attempt+".term")
}

// read reads an attempt's record, and reports false if it has none.
func (d *Driver) read(attempt string) (*record, bool, error) {
	if !attemptPattern.MatchString(attempt) {
		return nil, false, nil
	}
	data, err := safepath.ReadFile(d.recordPath(attempt), safepath.Private, 64<<10)
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

// writeRecord writes an attempt's record, whole or not at all.
func (d *Driver) writeRecord(attempt string, r *record) error {
	data, err := jsonv2.Marshal(r, jsonv2.Deterministic(true))
	if err != nil {
		return err
	}
	return d.writeFile(d.recordPath(attempt), data)
}

// writeFile writes a file in placements/ whole or not at all, readable
// only by the user: to a new file beside it, synced, then renamed over it.
func (d *Driver) writeFile(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".new-*")
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
	}
	return err
}

// removeToken removes the token file of r's worker, if the worker didn't:
// one in tokens/, and no other file.
func (d *Driver) removeToken(r *record) {
	if filepath.Dir(r.Token) == filepath.Join(d.State, tokensDir) && strings.HasSuffix(r.Token, ".token") {
		_ = os.Remove(r.Token)
	}
}

// forget removes what is left of an attempt's placement whose worker has
// ended, before the attempt is placed again.
func (d *Driver) forget(attempt string, r *record) {
	d.removeToken(r)
	for _, path := range []string{d.termPath(attempt), filepath.Join(d.State, placementsDir, attempt+".exit"), d.recordPath(attempt)} {
		_ = os.Remove(path)
	}
}
