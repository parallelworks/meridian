package main

import (
	"context"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/backend/hostproto"
	localbackend "github.com/parallelworks/meridian/backend/local"
	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
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
	// machineID returns the host's own ID.
	machineID func(context.Context) (string, error)
	// loginctl runs loginctl with args, and returns what it wrote.
	loginctl func(ctx context.Context, args ...string) (string, error)
	// scope says whether systemd-run is there to start a worker in a scope
	// of the user's service manager.
	scope func() bool
	// tmp is the directory the sockets' directory is made in: /tmp, whose
	// short path leaves room in a socket's for its name.
	tmp string
}

// systemHost is the system's own.
var systemHost = hostSystem{
	harden:    harden.Process,
	machineID: machineID,
	loginctl: func(ctx context.Context, args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "loginctl", args...).Output() //nolint:gosec // loginctl, with the properties it shows
		return string(out), err
	},
	scope: func() bool {
		_, err := exec.LookPath("systemd-run")
		return err == nil
	},
	tmp: "/tmp",
}

// machineIDPattern is what a host's ID may be, which names a directory.
var machineIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,127}$`)

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
	if len(positional) != 1 || !slices.Contains(hostproto.Ops, positional[0]) {
		return c.failf(problems.ArgumentsInvalid, problems.Params{"command": "host"})
	}
	sys := systemHost
	if c.host != nil {
		sys = *c.host
	}
	h := &hostOp{c: c, sys: sys}
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
	detach, err := decideDetach(ctx, h.sys)
	if err != nil {
		return nil, err
	}
	exe, digest, err := localbackend.Executable()
	if err != nil {
		return nil, err
	}
	for _, dir := range []string{paths.State, paths.Cache, paths.Sockets, paths.Runs} {
		if err := ensureHostDir(dir); err != nil {
			return nil, err
		}
	}
	version, _ := buildVersion()
	return &hostproto.Hello{
		Version: version, OS: runtime.GOOS, Arch: runtime.GOARCH, Host: hostName(), Home: home, MachineID: id,
		Detach: detach, Binary: hostproto.Binary{Path: exe, Digest: digest}, Paths: paths,
	}, nil
}

// paths returns the directories meridian host keeps what it keeps in, the
// user's home, and the host's ID: the state directory's hosts/<ID>, which
// is this host's own where the home is shared, as a cluster's nodes'
// homes are; the cache directory's bin; the user's own directory under
// tmp, for the sockets; and the state directory's runs, which the hosts
// that share it share (hostRunsDir).
func (h *hostOp) paths(ctx context.Context) (hostproto.Paths, string, string, error) {
	home := h.c.getenv("HOME")
	if !filepath.IsAbs(home) {
		return hostproto.Paths{}, "", "", problems.HomeUnknown.Problem(problems.Params{"variable": "HOME"})
	}
	dirs, err := local.Locate(h.c.getenv)
	if err != nil {
		return hostproto.Paths{}, "", "", err
	}
	id, err := h.sys.machineID(ctx)
	if err != nil {
		return hostproto.Paths{}, "", "", err
	}
	id = strings.ToLower(strings.TrimSpace(id))
	if !machineIDPattern.MatchString(id) {
		return hostproto.Paths{}, "", "", fmt.Errorf("host: the host's ID %q can't name a directory", id)
	}
	return hostproto.Paths{
		State:   filepath.Join(dirs.State, "hosts", id),
		Cache:   filepath.Join(dirs.Cache, "bin"),
		Sockets: filepath.Join(h.sys.tmp, "meridian-"+strconv.Itoa(os.Getuid())),
		Runs:    hostRunsDir(dirs.State),
	}, filepath.Clean(home), id, nil
}

// hostRunsDir is where runs' directories are on a host whose state
// directory is state, as meridian finds its own (local.Locate), as
// ${XDG_STATE_HOME:-$HOME/.local/state}/meridian: its runs/, outside any
// one host's hosts/<ID>. On a cluster, that is in the home its nodes
// share, so a run's directory is shared with the compute nodes its steps
// submit work to, as steps that call sbatch, qsub or srun from it need,
// and with the cluster's other login nodes, which a connection to the same
// host name can reach after a reconnect. The system's temporary directory,
// where a run's directory is on the coordinator's own host, is each node's
// own. It is Meridian's own directory, apart from where anything else
// keeps its runs. Each run's is hostRunDir.
func hostRunsDir(state string) string {
	return filepath.Join(state, "runs")
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
		reason := err.Error()
		if p, ok := errors.AsType[*problem.Problem](err); ok {
			reason = string(p.Code)
		}
		return problems.SSHHostDirUnusable.Problem(problems.Params{"host": hostName(), "path": dir, "reason": reason})
	}
	return nil
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
	if _, err := decideDetach(ctx, h.sys); err != nil {
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
// and hands the worker its bootstrap token before it returns.
func (h *hostOp) place(ctx context.Context, req *hostproto.Request) hostproto.Message {
	pl := req.Place
	if pl == nil || pl.Run == "" || pl.Store == "" {
		return hostproto.Message{Error: "host: a place without its attempt, run and store"}
	}
	detach, err := decideDetach(ctx, h.sys)
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
		d.Launcher = []string{"systemd-run", "--user", "--scope", "--collect", "--quiet", "--unit=meridian-worker-" + pl.Attempt}
	}
	d.AwaitToken = true
	r := pl.Request()
	r.RunDir = hostRunDir(paths.Runs, pl.Run, pl.Store)
	p, err := d.Place(ctx, req.Member, r)
	if err != nil {
		return failed(err)
	}
	return hostproto.Message{Placement: &p, RunDir: r.RunDir}
}
