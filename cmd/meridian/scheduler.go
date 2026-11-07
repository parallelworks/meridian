package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/backend/batch"
	localbackend "github.com/parallelworks/meridian/backend/local"
	"github.com/parallelworks/meridian/backend/slurm"
	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
)

// slurmDriver returns the slurm backend's driver (schedulerDriver): a
// batch.Driver over Slurm's commands on this machine, for a member whose
// login node it is, which keeps what the compute nodes read in the home
// the cluster's nodes share (schedulerHome): its submissions in batch/,
// the address files of its members' listeners in batch/relays/, runs'
// directories in runs/, as meridian host chooses them, and the coordinator's
// binary in the cache's bin/<hex digest>/meridian (sharedBinary). Jobs are
// submitted with the environment of the command, but the variables the
// user's secret variables are read from, withhold, and placements are
// named by a key derived from the key file. A test changes the driver as
// c.schedulers does. Its runs' directories are named by the store's ID,
// which runs.store is set to once the coordinator has opened it.
func slurmDriver(c *cli, keys *local.Keys, withhold []string) (*schedulerDriver, *schedulerRuns, error) {
	key, err := keys.Derive("slurm names", batch.MinKeyLen)
	if err != nil {
		return nil, nil, err
	}
	sched := &slurm.Scheduler{}
	s := &schedulerDriver{settings: sched.Settings}
	runs := new(schedulerRuns)
	state, cache, err := schedulerHome(c.getenv)
	if err != nil {
		// Its members can't run a job, which its Probe says.
		s.unusable = err
		return s, runs, nil //nolint:nilerr // what is wrong is the members', not the command's
	}
	runs.dir = filepath.Join(state, "runs")
	bin := &sharedBinary{dir: filepath.Join(cache, "bin"), self: localbackend.Runnable}
	s.local = &batch.Driver{
		Scheduler: sched, Dir: filepath.Join(state, "batch"), Key: key,
		Binary: bin.get, Withhold: withhold, RunDir: runs.runDir,
	}
	if c.schedulers != nil {
		c.schedulers(s.local)
	}
	return s, runs, nil
}

// schedulerHome returns where the slurm backend keeps what the compute
// nodes read, and its cache, in the home the cluster's nodes share:
// ${XDG_STATE_HOME:-$HOME/.local/state}/meridian and
// ${XDG_CACHE_HOME:-$HOME/.cache}/meridian, whatever MERIDIAN_STATE_DIR
// and MERIDIAN_CACHE_DIR say, which name this machine's own, as its
// database, which can't be on a network filesystem, needs. A relative
// XDG directory is ignored, as XDG says.
func schedulerHome(getenv func(string) string) (state, cache string, err error) {
	home := getenv("HOME")
	dir := func(xdg, fallback string) (string, error) {
		if d := getenv(xdg); filepath.IsAbs(d) {
			return filepath.Join(d, "meridian"), nil
		}
		if !filepath.IsAbs(home) {
			return "", problems.HomeUnknown.Problem(problems.Params{"variable": xdg})
		}
		return filepath.Join(home, fallback, "meridian"), nil
	}
	if state, err = dir("XDG_STATE_HOME", filepath.Join(".local", "state")); err != nil {
		return "", "", err
	}
	if cache, err = dir("XDG_CACHE_HOME", ".cache"); err != nil {
		return "", "", err
	}
	return state, cache, nil
}

// schedulerRuns chooses runs' directories for the slurm backend's
// members, in dir, named as meridian host names them on a host
// (hostRunDir), by the run's and the store's IDs, but in the home the
// cluster's nodes share whatever MERIDIAN_STATE_DIR says, which meridian
// host's honour.
type schedulerRuns struct {
	dir, store string
}

// runDir is the directory of req's run, which compute nodes see: in the
// runs' directory, which it makes, the user's alone; or, for a child run,
// in its root's there, which it makes if none of its root's jobs ran on
// the cluster, or checks (makeRootDir), and the worker makes the child's
// in it. What is wrong is the directory_* kinds, of this machine's own
// directories, not a host's.
func (r *schedulerRuns) runDir(req backend.PlaceRequest) (string, error) {
	if r.store == "" {
		return "", errors.New("meridian: the slurm backend's runs' directories have no store yet")
	}
	if (req.Root == "") != (req.Path == "") || req.Path != "" && !filepath.IsLocal(req.Path) {
		return "", errors.New("meridian: a child run's placement without its root, or with a path outside the root's directory")
	}
	if err := safepath.EnsureDir(r.dir, safepath.Private); err != nil {
		return "", err
	}
	if req.Root == "" {
		return hostRunDir(r.dir, req.Run, r.store), nil
	}
	root := hostRunDir(r.dir, req.Root, r.store)
	if err := makeRootDir(root); err != nil {
		if !problems.IsProblem(err) {
			err = problems.DirectoryUnavailable.Problem(problems.Params{"path": root, "reason": err.Error()})
		}
		return "", err
	}
	return filepath.Join(root, req.Path), nil
}

// A sharedBinary is the binary a scheduler's job runs: the file this
// process runs, copied into dir, in the home the cluster's nodes share, as
// <hex digest>/meridian.
type sharedBinary struct {
	dir string
	// self returns the file this process runs, as the local backend
	// checks it (localbackend.Runnable).
	self func() (path, run, digest string, err error)

	mu sync.Mutex
	// digest and copied are the copy's last checked, and what it was then,
	// which the next check of the same binary takes it to be while its
	// file is the same, and unchanged (sameFile).
	digest string
	copied fs.FileInfo
}

// settled is how long ago a copy's status must have last changed for its
// check to be kept, as the local backend keeps its digest of the file it
// runs: a change within the second after could leave its time as it was.
const settled = time.Second

// get returns the copy's path and digest, once it has copied the file this
// process runs there, if it isn't there, and checked the copy, and the way
// to it, as the local backend checks the binary it starts
// (worker_binary_unsafe), hashing what it opened as it checked it. A copy
// that holds another digest is removed, and copied again. The copy is
// written to a new file of its own there, checked against the digest, and
// only then linked into place, so that a copy that stops halfway leaves
// none that isn't whole, and of two at once, the second checks the
// first's. A copy it checked before isn't hashed again while it is the
// same file, of the same size, its data and its status changed at the
// same times, where the system says them: no other user can change it,
// nor the way to it, which is checked each time, and meridian batch
// checks its own digest before it takes a token.
func (b *sharedBinary) get() (string, string, error) {
	_, run, digest, err := b.self()
	if err != nil {
		return "", "", err
	}
	hexDigest, ok := strings.CutPrefix(digest, "sha256:")
	if !ok || hexDigest == "" || strings.Trim(hexDigest, "0123456789abcdef") != "" {
		return "", "", fmt.Errorf("meridian: %q isn't a digest", digest)
	}
	dir := filepath.Join(b.dir, hexDigest)
	path := filepath.Join(dir, "meridian")
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := safepath.EnsureDir(dir, safepath.Private); err != nil {
		return "", "", err
	}
	if b.digest == digest {
		if info, err := checkBinary(path, nil); err == nil && sameFile(info, b.copied) {
			return path, digest, nil
		}
	}
	var found string
	info, err := checkBinary(path, &found)
	if err == nil && found != digest {
		// Another binary, which the user's own processes could have left:
		// it goes, and a copy of this one takes its place.
		if err := os.Remove(path); err != nil {
			return "", "", err
		}
		info, err = nil, fs.ErrNotExist
	}
	if errors.Is(err, fs.ErrNotExist) {
		info, err = copyBinary(run, dir, digest)
	}
	if err != nil {
		return "", "", err
	}
	if ctime, ok := changeTime(info); ok && time.Since(time.Unix(0, ctime)) >= settled {
		b.digest, b.copied = digest, info
	}
	return path, digest, nil
}

// sameFile reports whether a and b are the same file, of the same size,
// its data and its status last changed at the same times; never where the
// system doesn't say when its status changed, which a process can't set
// back, as it can its data's.
func sameFile(a, b fs.FileInfo) bool {
	ac, ok := changeTime(a)
	if !ok || b == nil {
		return false
	}
	bc, _ := changeTime(b)
	return os.SameFile(a, b) && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime()) && ac == bc
}

// checkBinary checks the binary at path, and the way to it, as the local
// backend checks the binary it starts: a regular file of the user's or
// root's that no other user can change (worker_binary_unsafe); and, where
// digest isn't nil, sets it to the digest of what it opened, sha256: and
// hex.
func checkBinary(path string, digest *string) (fs.FileInfo, error) {
	f, info, err := safepath.OpenExecutable(path, path)
	if p, ok := errors.AsType[*problem.Problem](err); ok {
		return nil, problems.WorkerBinaryUnsafe.Problem(problems.Params{"path": path, "code": string(p.Code)})
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if digest != nil {
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			return nil, err
		}
		*digest = "sha256:" + hex.EncodeToString(h.Sum(nil))
	}
	return info, nil
}

// copyBinary copies the file run, whose digest is digest, into dir, as
// meridian: to a new file there, readable by the user alone, which it
// checks against digest before it links it into place. Where another has
// linked one there meanwhile, it checks that one instead.
func copyBinary(run, dir, digest string) (fs.FileInfo, error) {
	src, err := os.Open(run) //nolint:gosec // the file this process runs
	if err != nil {
		return nil, err
	}
	defer src.Close()
	tmp, err := os.CreateTemp(dir, ".meridian-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name()) //nolint:errcheck // linked into place, or not wanted
	h := sha256.New()
	_, err = io.Copy(io.MultiWriter(tmp, h), src)
	if err == nil {
		err = tmp.Chmod(0o700)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	if got := "sha256:" + hex.EncodeToString(h.Sum(nil)); got != digest {
		return nil, fmt.Errorf("meridian: the copy of %s has the digest %s, not %s", run, got, digest)
	}
	path := filepath.Join(dir, "meridian")
	if err := os.Link(tmp.Name(), path); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, err
	}
	var found string
	info, err := checkBinary(path, &found)
	if err == nil && found != digest {
		return nil, fmt.Errorf("meridian: %s holds the digest %s, not %s", path, found, digest)
	}
	return info, err
}

// A schedulerDriver is the slurm backend's driver, which routes each
// member by where its login node is: this machine, for a member without
// a host, whose driver is local; or another, which a later release reaches
// over SSH, whose backend isn't built yet (backend_not_built).
type schedulerDriver struct {
	settings func(member string, raw jsontext.Value) (*batch.Settings, error)
	local    *batch.Driver
	// unusable is why local can't be used, as a home that can't be told,
	// or nil.
	unusable error
}

var _ backend.Forwarder = (*schedulerDriver)(nil)

// of returns the driver of m's login node, or why there is none. A member
// whose settings don't decode is local's, whose calls say so.
func (s *schedulerDriver) of(m *backend.Member) (backend.Forwarder, error) {
	if set, err := s.settings(m.Name, m.Settings); err == nil && set.Host != "" {
		return nil, problems.BackendNotBuilt.Problem(problems.Params{"member": m.Name, "backend": m.Backend})
	}
	if s.unusable != nil {
		return nil, s.unusable
	}
	return s.local, nil
}

// Probe refuses a job that calls a workflow, which would hold its
// allocation while the run it calls ran, here, wherever m's login node is,
// so that no driver it routes to can miss it; and is the Probe of m's
// login node's driver otherwise.
func (s *schedulerDriver) Probe(ctx context.Context, m *backend.Member, demand backend.Demand) (backend.Fit, error) {
	if demand.CallsWorkflow {
		return backend.Fit{Problem: problems.SchedulerCallerRefused.Problem(problems.Params{"member": m.Name})}, nil
	}
	d, err := s.of(m)
	if p, ok := errors.AsType[*problem.Problem](err); ok {
		return backend.Fit{Problem: p}, nil
	}
	if err != nil {
		return backend.Fit{}, err
	}
	return d.Probe(ctx, m, demand)
}

// EndWait is the local driver's: how long its checks can take to see how
// a worker ended.
func (s *schedulerDriver) EndWait() time.Duration {
	if s.local == nil {
		return 0
	}
	return s.local.EndWait()
}

var _ backend.EndWaiter = (*schedulerDriver)(nil)

func (s *schedulerDriver) Connect(ctx context.Context, m *backend.Member, coordinator backend.Endpoint) (backend.Endpoint, io.Closer, error) {
	d, err := s.of(m)
	if err != nil {
		return backend.Endpoint{}, nil, err
	}
	return d.Connect(ctx, m, coordinator)
}

func (s *schedulerDriver) Place(ctx context.Context, m *backend.Member, req backend.PlaceRequest) (backend.Placement, error) {
	d, err := s.of(m)
	if err != nil {
		return backend.Placement{}, err
	}
	return d.Place(ctx, m, req)
}

func (s *schedulerDriver) Find(ctx context.Context, m *backend.Member, attempt string) (backend.Placement, bool, error) {
	d, err := s.of(m)
	if err != nil {
		return backend.Placement{}, false, err
	}
	return d.Find(ctx, m, attempt)
}

func (s *schedulerDriver) Check(ctx context.Context, m *backend.Member, p backend.Placement) (backend.Status, error) {
	d, err := s.of(m)
	if err != nil {
		return backend.Status{State: backend.Unknown}, err
	}
	return d.Check(ctx, m, p)
}

func (s *schedulerDriver) Cancel(ctx context.Context, m *backend.Member, p backend.Placement) error {
	d, err := s.of(m)
	if err != nil {
		return err
	}
	return d.Cancel(ctx, m, p)
}

func (s *schedulerDriver) Forget(ctx context.Context, m *backend.Member, p backend.Placement) error {
	d, err := s.of(m)
	if err != nil {
		return err
	}
	return d.Forget(ctx, m, p)
}

func (s *schedulerDriver) MemberKey(m *backend.Member) (string, error) {
	d, err := s.of(m)
	if err != nil {
		return "", err
	}
	return d.MemberKey(m)
}

func (s *schedulerDriver) Endpoint(ctx context.Context, m *backend.Member) (backend.Endpoint, error) {
	d, err := s.of(m)
	if err != nil {
		return backend.Endpoint{}, err
	}
	return d.Endpoint(ctx, m)
}

func (s *schedulerDriver) Forward(ctx context.Context, m *backend.Member) (net.Listener, error) {
	d, err := s.of(m)
	if err != nil {
		return nil, err
	}
	return d.Forward(ctx, m)
}
