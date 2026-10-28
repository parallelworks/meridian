package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/backend/hostproto"
	localbackend "github.com/parallelworks/meridian/backend/local"
	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/internal/flock"
	"github.com/parallelworks/meridian/internal/harden"
	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
)

// hostAbout is what meridian host's usage says of it besides its summary.
const hostAbout = `meridian host <operation> is the ssh backend's helper on a host: one of
hello, probe, place, find, check, cancel or forget, which it does with the
local backend, as the coordinator would on its own host. Once it has kept
the user's other processes out of its memory, it says it is ready on
standard output; only then does it read the request on standard input,
which for place carries the attempt's bootstrap token; and it writes what
it did on standard output, one JSON object a line, as backend/hostproto
documents.`

// A hostSystem is what meridian host asks of the system it runs on, which
// a test gives in place of the system's (cli.host).
type hostSystem struct {
	// harden keeps the user's other processes out of the process's
	// memory.
	harden func() error
	// machineID returns the host's own ID, or "" for a host without one.
	machineID func(context.Context) (string, error)
	// hostname returns the host's name.
	hostname func() (string, error)
	// logind says whether logind runs.
	logind func() bool
	// loginctl runs loginctl with args, and returns what it wrote: an
	// error that is exec.ErrNotFound where there is none.
	loginctl func(ctx context.Context, args ...string) (string, error)
	// command returns the path of a command, as hostCommand does.
	command func(name string) (string, error)
	// version runs the command at path with --version, and returns what
	// it wrote.
	version func(ctx context.Context, path string) (string, error)
	// tmp is the directory a sockets' directory of a random name is made
	// in, where the user's runtime directory won't do: /tmp, whose short
	// path leaves room in a socket's for its name.
	tmp string
}

// systemHost is the system's own.
var systemHost = hostSystem{
	harden:    harden.Process,
	machineID: machineID,
	hostname:  os.Hostname,
	// As systemd's own tools tell, logind runs where it has made its
	// directory of seats; where what is there can't be told, it is
	// taken to run, and asked.
	logind: func() bool {
		_, err := os.Stat("/run/systemd/seats/")
		return !errors.Is(err, fs.ErrNotExist)
	},
	loginctl: func(ctx context.Context, args ...string) (string, error) {
		path, err := hostCommand("loginctl")
		if err != nil {
			return "", err
		}
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, path, args...).Output() //nolint:gosec // loginctl, which hostCommand checked, with the properties it shows
		return string(out), err
	},
	command: hostCommand,
	version: func(ctx context.Context, path string) (string, error) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, path, "--version").Output() //nolint:gosec // a command hostCommand checked, asked its version
		return string(out), err
	},
	tmp: "/tmp",
}

// hostCommand returns the path of the command name, as the PATH finds it,
// absolute and without symbolic links, once it has checked it as the
// worker's binary is checked (safepath.CheckExecutable): a regular file of
// the user's or root's that no other user can change, nor any directory on
// the way to it. meridian host runs it with what the user's other
// processes mustn't have, systemd-run a worker's token's socket, and a
// directory another user can change can come first on a PATH, as a shared
// bin/ that a cluster's startup files add. One that isn't safe is
// ssh_host_command_unsafe; one the PATH doesn't find, an error that is
// exec.ErrNotFound. It is safepath.LookExecutable, as the coordinator
// finds ssh.
func hostCommand(name string) (string, error) {
	path, err := safepath.LookExecutable(name)
	if p, ok := errors.AsType[*problem.Problem](err); ok {
		return "", problems.SSHHostCommandUnsafe.Problem(problems.Params{"host": hostName(), "path": path, "code": string(p.Code)})
	}
	return path, err
}

// machineIDPattern is what a host's ID may be.
var machineIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,127}$`)

// hostKeyName is what of the host's name its key keeps: what a host's name
// has, but in lower case, which DNS ignores; anything else is a hyphen.
var hostKeyName = regexp.MustCompile(`[^a-z0-9.-]`)

// runHost is meridian host: it does one operation of the protocol
// hostproto documents, which a coordinator drives it with over a channel
// to the host it runs on, with the local backend's Driver, whose state is
// the host's own, and writes what it did. It exits 0 once it has, and 2
// when what it writes is a problem or an error.
func runHost(ctx context.Context, c *cli, fs *flag.FlagSet, args []string) int {
	positional, err := parseFlags(fs, args)
	if err != nil {
		return flagError(c, fs, err)
	}
	c.json = true
	sys := systemHost
	if c.host != nil {
		sys = *c.host
	}
	h := &hostOp{c: c, sys: sys}
	// An operation this helper doesn't have, as a newer coordinator's, is
	// refused in the protocol, of the helper's version, which says why.
	if len(positional) != 1 || !slices.Contains(hostproto.Ops, positional[0]) {
		return h.write(hostproto.Message{Problem: problems.ArgumentsInvalid.Problem(problems.Params{"command": "host"})})
	}
	// Nothing is read until the process is hardened: a request can carry
	// a bootstrap token.
	if err := sys.harden(); err != nil {
		var p *problem.Problem
		if errors.Is(err, harden.ErrUnsupported) {
			p = problems.WorkerSystemUnsupported.Problem(problems.Params{"os": runtime.GOOS})
		} else {
			p = problems.WorkerHardenFailed.Problem(problems.Params{"reason": err.Error()})
		}
		return h.write(hostproto.Message{Problem: p})
	}
	if code := h.write(hostproto.Message{Ready: true}); code != exitOK {
		return code
	}
	var req hostproto.Request
	if err := readRequest(c.stdin, &req); err != nil {
		return h.write(hostproto.Message{Error: err.Error()})
	}
	return h.write(h.do(ctx, positional[0], &req))
}

// readRequest reads one request from r, at most hostproto.MaxRequest bytes
// of it, refusing members it doesn't know.
func readRequest(r io.Reader, req *hostproto.Request) error {
	value, err := jsontext.NewDecoder(io.LimitReader(r, hostproto.MaxRequest)).ReadValue()
	if err != nil {
		return fmt.Errorf("host: read the request: %w", err)
	}
	if err := jsonv2.Unmarshal(value, req, jsonv2.RejectUnknownMembers(true)); err != nil {
		return fmt.Errorf("host: a request that isn't one: %w", err)
	}
	return nil
}

// A hostOp is one operation of meridian host's.
type hostOp struct {
	c   *cli
	sys hostSystem
}

// write writes m, of the protocol's version, as a line of its own, and
// returns the command's exit status: 2 for a problem or an error.
func (h *hostOp) write(m hostproto.Message) int {
	m.Protocol = hostproto.Version
	data, err := jsonv2.Marshal(m, jsonv2.Deterministic(true))
	if err != nil {
		return h.c.fail(err)
	}
	if _, err := h.c.stdout.Write(append(data, '\n')); err != nil {
		return h.c.fail(err)
	}
	if m.Problem != nil || m.Error != "" {
		return exitError
	}
	return exitOK
}

// failed is the message of an operation that failed with err: its problem,
// as a Driver's, or else that the helper couldn't answer.
func failed(err error) hostproto.Message {
	if p, ok := errors.AsType[*problem.Problem](err); ok {
		return hostproto.Message{Problem: p}
	}
	return hostproto.Message{Error: err.Error()}
}

// do does op, as req asks.
func (h *hostOp) do(ctx context.Context, op string, req *hostproto.Request) hostproto.Message {
	if op == hostproto.OpHello {
		hello, err := h.hello(ctx)
		if err != nil {
			return failed(err)
		}
		return hostproto.Message{Hello: hello}
	}
	if req.Member == nil {
		return hostproto.Message{Error: "host: a request without its member"}
	}
	switch op {
	case hostproto.OpProbe:
		return h.probe(ctx, req)
	case hostproto.OpPlace:
		return h.place(ctx, req)
	case hostproto.OpFind:
		d, err := h.driver(ctx)
		if err != nil {
			return failed(err)
		}
		p, found, err := d.Find(ctx, req.Member, req.Attempt)
		if err != nil {
			return failed(err)
		}
		if !found {
			return hostproto.Message{}
		}
		return hostproto.Message{Found: true, Placement: &p}
	}
	if req.Placement == nil {
		return hostproto.Message{Error: "host: a request without its placement"}
	}
	d, err := h.driver(ctx)
	if err != nil {
		return failed(err)
	}
	switch op {
	case hostproto.OpCheck:
		s, err := d.Check(ctx, req.Member, *req.Placement)
		m := hostproto.Message{Status: hostproto.StatusOf(s)}
		if err != nil {
			m = failed(err)
			m.Status = hostproto.StatusOf(s)
		}
		return m
	case hostproto.OpCancel:
		if err := d.Cancel(ctx, req.Member, *req.Placement); err != nil {
			return failed(err)
		}
	case hostproto.OpForget:
		if err := d.Forget(ctx, req.Member, *req.Placement); err != nil {
			return failed(err)
		}
	}
	return hostproto.Message{}
}

// hostName is the host's name, as it says it, for its problems.
func hostName() string {
	name, err := os.Hostname()
	if err != nil {
		return "this host"
	}
	return name
}

// hello says what the host is: its system, its user's home, its ID, how a
// worker is started detached, the binary that runs, and the directories,
// which it makes the user's own, if they aren't there, and checks.
func (h *hostOp) hello(ctx context.Context) (*hostproto.Hello, error) {
	paths, home, id, err := h.paths(ctx)
	if err != nil {
		return nil, err
	}
	detach, _, err := decideDetach(ctx, h.sys)
	if err != nil {
		return nil, err
	}
	exe, digest, err := localbackend.Executable()
	if err != nil {
		return nil, err
	}
	for _, dir := range []string{paths.State, paths.Cache, paths.Runs} {
		if err := ensureHostDir(dir); err != nil {
			return nil, err
		}
	}
	if paths.Sockets, err = h.sockets(ctx, paths.State); err != nil {
		return nil, err
	}
	version, _ := buildVersion()
	return &hostproto.Hello{
		Version: version, OS: runtime.GOOS, Arch: runtime.GOARCH, Host: hostName(), Home: home, MachineID: id,
		Detach: detach, Binary: hostproto.Binary{Path: exe, Digest: digest}, Paths: paths,
	}, nil
}

// paths returns the directories meridian host keeps what it keeps in, but
// the sockets' (sockets), the user's home, and the host's ID: the state
// directory's hosts/<key>, which is this host's own where the home is
// shared, as a cluster's nodes' homes are (hostKey); the cache directory's
// bin; and the state directory's runs, which the hosts that share it share
// (hostRunsDir).
func (h *hostOp) paths(ctx context.Context) (hostproto.Paths, string, string, error) {
	home := h.c.getenv("HOME")
	if !filepath.IsAbs(home) {
		return hostproto.Paths{}, "", "", problems.HomeUnknown.Problem(problems.Params{"variable": "HOME"})
	}
	dirs, err := local.Locate(h.c.getenv)
	if err != nil {
		return hostproto.Paths{}, "", "", err
	}
	id, key, err := h.hostKey(ctx)
	if err != nil {
		return hostproto.Paths{}, "", "", err
	}
	return hostproto.Paths{
		State: filepath.Join(dirs.State, "hosts", key),
		Cache: filepath.Join(dirs.Cache, "bin"),
		Runs:  hostRunsDir(h.c.getenv, home),
	}, filepath.Clean(home), id, nil
}

// hostKey returns the host's machine ID, and its key, which names its own
// directory under the state directory's hosts/: the ID, a hyphen, and the
// host's name (hostKeyName), at most 64 bytes of it. The ID alone tells
// hosts apart but where they were made from one image that held it, as a
// cluster's stateless or cloned nodes can be, which share a home too; their
// names tell them apart. A host whose name changes takes a key of its
// own, in which it doesn't find the workers it placed before. A host
// without an ID, or a name, is ssh_host_id_unknown.
func (h *hostOp) hostKey(ctx context.Context) (id, key string, err error) {
	unknown := problems.SSHHostIDUnknown.Problem(problems.Params{"host": hostName()})
	if id, err = h.sys.machineID(ctx); err != nil {
		if _, ok := errors.AsType[*problem.Problem](err); ok {
			return "", "", err
		}
		return "", "", unknown
	}
	id = strings.ToLower(strings.TrimSpace(id))
	name, err := h.sys.hostname()
	name = strings.Trim(hostKeyName.ReplaceAllString(strings.ToLower(name), "-"), ".-")
	if err != nil || name == "" || !machineIDPattern.MatchString(id) {
		return "", "", unknown
	}
	return id, id + "-" + name[:min(len(name), 64)], nil
}

// sockets returns the directory of the sockets a coordinator listens on
// for the host's workers, which only the user can use, and no other user
// can take from them: their runtime directory's meridian/, where
// XDG_RUNTIME_DIR names one that is only theirs, and that is short enough
// (hostproto.MaxSocketsDir); otherwise a directory of a random name in tmp,
// which the host's state, under state, records: made afresh where it
// records none, or one that is gone, or isn't the user's own. A name of
// tmp's that another user could know, as /tmp/meridian-<uid>, is one they
// could make first, as a directory of theirs or a link, which would keep
// the user out of it.
func (h *hostOp) sockets(ctx context.Context, state string) (string, error) {
	if runtimeDir := h.c.getenv("XDG_RUNTIME_DIR"); filepath.IsAbs(runtimeDir) {
		dir := filepath.Join(runtimeDir, "meridian")
		if len(dir) <= hostproto.MaxSocketsDir && safepath.CheckDir(runtimeDir, safepath.Private) == nil && safepath.EnsureDir(dir, safepath.Private) == nil {
			return dir, nil
		}
	}
	// One process at a time chooses it, so that two hellos at once give
	// the same directory.
	unlock, err := flock.Lock(ctx, filepath.Join(state, "sockets.lock"))
	if err != nil {
		return "", err
	}
	defer unlock()
	record := filepath.Join(state, "sockets")
	data, err := safepath.ReadFile(record, safepath.Private, 4096)
	switch {
	case err == nil:
		dir := string(data)
		if filepath.Dir(dir) == h.sys.tmp && strings.HasPrefix(filepath.Base(dir), "meridian-") && safepath.CheckDir(dir, safepath.Private) == nil {
			return dir, nil
		}
	case !errors.Is(err, fs.ErrNotExist):
		return "", hostDirUnusable(state, err)
	}
	dir, err := os.MkdirTemp(h.sys.tmp, "meridian-")
	if err != nil {
		return "", hostDirUnusable(h.sys.tmp, err)
	}
	if err := safepath.CheckDir(dir, safepath.Private); err != nil {
		return "", hostDirUnusable(dir, err)
	}
	if err := safepath.WriteFile(record, []byte(dir)); err != nil {
		return "", hostDirUnusable(state, err)
	}
	return dir, nil
}

// hostRunsDir is where runs' directories are on a host whose environment
// getenv reads, and whose user's home is home, an absolute path: the state
// directory's runs/ where MERIDIAN_STATE_DIR names it, which local.Locate
// checked, and otherwise ${XDG_STATE_HOME:-$HOME/.local/state}/meridian/runs
// on every system, outside any one host's hosts/<key>. macOS's own state
// directory, ~/Library/Application Support/Meridian, holds a space, which
// make, autotools and an unquoted $PWD break on, as a run's steps would
// in its directory. On a cluster, it is in the home its nodes share, so a
// run's directory is shared with the compute nodes its steps submit work
// to, as steps that call sbatch, qsub or srun from it need, and with the
// cluster's other login nodes, which a connection to the same host name
// can reach after a reconnect. The system's temporary directory, where a
// run's directory is on the coordinator's own host, is each node's own. It
// is Meridian's own directory, apart from where anything else keeps its
// runs. Each run's is hostRunDir.
func hostRunsDir(getenv func(string) string, home string) string {
	if state := getenv(local.EnvStateDir); state != "" {
		return filepath.Join(filepath.Clean(state), "runs")
	}
	// The XDG specification says a relative path is invalid, and is
	// ignored.
	if state := getenv("XDG_STATE_HOME"); filepath.IsAbs(state) {
		return filepath.Join(state, "meridian", "runs")
	}
	return filepath.Join(home, ".local", "state", "meridian", "runs")
}

// hostRunDir is the directory of the run run of the store store, in runs,
// a host's hostRunsDir: the jobs of a run on one host, and on hosts that
// share a home, share it.
func hostRunDir(runs, run, store string) string {
	return filepath.Join(runs, local.RunDirName(store, run))
}

// ensureHostDir makes dir the user's own, if it isn't there, which only
// they can use, and checks it, and the way to it: what is wrong is
// ssh_host_dir_unusable.
func ensureHostDir(dir string) error {
	if err := safepath.EnsureDir(dir, safepath.Private); err != nil {
		return hostDirUnusable(dir, err)
	}
	return nil
}

// ensureRootDir makes dir, the directory of a child run's root in the runs
// directory, which place has checked (ensureHostDir), if none of the
// root's jobs ran on the host, readable by the user alone, as a root's
// worker makes its own; and checks one that is there, as it is, never
// following a symbolic link: a directory of the user's that no other user
// can change, though others may read it, as a workflow may have let them,
// which ensureHostDir would refuse. A symbolic link there, which
// makeDirIn's os.Root would follow, a file, or a directory another user
// can change is ssh_host_dir_unusable.
func ensureRootDir(dir string) error {
	err := os.Mkdir(dir, 0o700)
	if err == nil {
		return nil
	}
	if !errors.Is(err, fs.ErrExist) {
		return hostDirUnusable(dir, err)
	}
	info, err := os.Lstat(dir)
	if err == nil {
		err = safepath.CheckDirInfo(dir, info, safepath.Unchangeable)
	}
	if err != nil {
		return hostDirUnusable(dir, err)
	}
	return nil
}

// hostDirUnusable is the ssh_host_dir_unusable of the directory dir, which
// failed its check with err: at the directory that err's problem names,
// dir or one on the way to it, as path_unsafe names the one another user
// can change, with the problem's code as its reason; or at dir, with the
// system's error.
func hostDirUnusable(dir string, err error) error {
	at, reason := dir, err.Error()
	if p, ok := errors.AsType[*problem.Problem](err); ok {
		reason = string(p.Code)
		for _, name := range []string{"dir", "path"} {
			if path, ok := p.Params[name].(string); ok && path != "" {
				at = path
				break
			}
		}
	}
	return problems.SSHHostDirUnusable.Problem(problems.Params{"host": hostName(), "path": dir, "at": at, "reason": reason})
}

// driver returns the local backend's Driver of the host's own state
// directory, which it makes, and checks: a worker it starts gets the
// process's environment, but GODEBUG, which could turn off what the worker
// requires of the process's cryptography.
func (h *hostOp) driver(ctx context.Context) (*localbackend.Driver, error) {
	paths, _, _, err := h.paths(ctx)
	if err != nil {
		return nil, err
	}
	if err := ensureHostDir(paths.State); err != nil {
		return nil, err
	}
	environ := slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "GODEBUG=") })
	return &localbackend.Driver{State: paths.State, Environ: environ}, nil
}

// probe says whether the member can run a job, as the local backend does:
// on a host where a worker wouldn't outlive the session that started it,
// it can't.
func (h *hostOp) probe(ctx context.Context, req *hostproto.Request) hostproto.Message {
	if req.Demand == nil {
		return hostproto.Message{Error: "host: a probe without its demand"}
	}
	if _, _, err := decideDetach(ctx, h.sys); err != nil {
		p, ok := errors.AsType[*problem.Problem](err)
		if !ok {
			return failed(err)
		}
		return hostproto.Message{Fit: hostproto.FitOf(backend.Fit{Problem: p})}
	}
	d, err := h.driver(ctx)
	if err != nil {
		if p, ok := errors.AsType[*problem.Problem](err); ok {
			return hostproto.Message{Fit: hostproto.FitOf(backend.Fit{Problem: p})}
		}
		return failed(err)
	}
	fit, err := d.Probe(ctx, req.Member, req.Demand.Demand())
	if err != nil {
		return failed(err)
	}
	return hostproto.Message{Fit: hostproto.FitOf(fit)}
}

// place starts the attempt's worker, detached as the host requires, in the
// run's directory on the host, which it chooses (hostRunDir) and returns,
// and hands the worker its bootstrap token before it returns. A child
// run's is its root's, with the child's path in it, as format 1 lays a
// tree out the same on each host: the root's is made here, if none of its
// jobs ran on the host, or checked (ensureRootDir), and the worker makes
// the child's in it.
func (h *hostOp) place(ctx context.Context, req *hostproto.Request) hostproto.Message {
	pl := req.Place
	if pl == nil || pl.Run == "" || pl.Store == "" {
		return hostproto.Message{Error: "host: a place without its attempt, run and store"}
	}
	if (pl.Root == "") != (pl.Path == "") || pl.Path != "" && !filepath.IsLocal(pl.Path) {
		return hostproto.Message{Error: "host: a place of a child run without its root, or with a path outside the root's directory"}
	}
	detach, systemdRun, err := decideDetach(ctx, h.sys)
	if err != nil {
		return failed(err)
	}
	d, err := h.driver(ctx)
	if err != nil {
		return failed(err)
	}
	paths, _, _, err := h.paths(ctx)
	if err != nil {
		return failed(err)
	}
	if err := ensureHostDir(paths.Runs); err != nil {
		return failed(err)
	}
	if detach == hostproto.DetachScope {
		// The scope's name is new each time, so that one an earlier
		// worker of the attempt still holds doesn't keep this one from
		// starting.
		suffix := make([]byte, 4)
		_, _ = rand.Read(suffix)
		unit := "--unit=meridian-worker-" + pl.Attempt + "-" + hex.EncodeToString(suffix)
		d.Launcher = []string{systemdRun, "--user", "--scope", "--collect", "--quiet", unit}
	}
	d.AwaitToken = true
	r := pl.Request()
	r.RunDir = hostRunDir(paths.Runs, pl.Run, pl.Store)
	if pl.Root != "" {
		root := hostRunDir(paths.Runs, pl.Root, pl.Store)
		if err := ensureRootDir(root); err != nil {
			return failed(err)
		}
		r.RunDir = filepath.Join(root, pl.Path)
	}
	p, err := d.Place(ctx, req.Member, r)
	if err != nil {
		return failed(err)
	}
	return hostproto.Message{Placement: &p, RunDir: r.RunDir}
}
