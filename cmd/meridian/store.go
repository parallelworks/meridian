package main

import (
	"context"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/parallelworks/meridian/backend"
	localbackend "github.com/parallelworks/meridian/backend/local"
	backendssh "github.com/parallelworks/meridian/backend/ssh"
	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/internal/harden"
	"github.com/parallelworks/meridian/internal/sessionproxy"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// startQueue names the hopper queue the Start of the run id goes in: one
// of the run's own, which the meridian run that created it works, or a
// meridian resume of it, so that its Start reads its variables from that
// process's environment, the shell that ran it.
func startQueue(id string) string { return "meridian.start." + id }

// coordinate opens a coordinator of the store at db, in the state
// directory dirs names, that works its runs as meridian run and resume do:
// with the user's variables and groups, cfg; the local backend, which
// runs each job's worker as a process of the user's, which reaches the
// coordinator on its socket, with the environment of the command but the
// variables that hold secret variables' values (local.Config.SecretEnv);
// and the ssh backend, which reaches hosts as the user's ssh would, and
// names them by a key derived from the state directory's key file. It logs
// what no answer says to c's standard error. Its session host serves the
// store's sessions on this machine, once ServeSessions has it serve them,
// reaching a cluster's services through the ssh backend's connections,
// under a key derived from the key file too, at the path of the login name
// that PW_USER gives its runs' steps, which it reads once for both. The
// caller closes the two, the host first, once the coordinator stops
// serving (coordination.close).
//
// First it keeps the user's other processes, the steps of its runs' jobs
// among them, out of the process's memory, where it holds the runs' secret
// records, as a worker does (internal/harden): it refuses to coordinate
// where it can't.
func coordinate(ctx context.Context, c *cli, dirs local.Dirs, cfg *local.Config, db string) (*coordination, error) {
	if err := c.hardenProcess(); err != nil {
		return nil, err
	}
	keys := local.NewKeys(dirs.State)
	sshKey, err := keys.Derive("ssh member names", backendssh.MinKeyLen)
	if err != nil {
		return nil, err
	}
	sessionKey, err := keys.Derive(sessionproxy.KeyLabel, sessionproxy.KeyLen)
	if err != nil {
		return nil, err
	}
	members, err := clusterMembers(cfg)
	if err != nil {
		return nil, err
	}
	version, _ := buildVersion()
	opts := backendssh.Options{
		Home: c.getenv("HOME"), Getenv: c.getenv,
		Version: version, Binaries: filepath.Join(dirs.Cache, "binaries"), Key: sshKey,
	}
	if c.ssh != nil {
		c.ssh(&opts)
	}
	ssh := backendssh.New(opts)
	log := slog.New(slog.NewTextHandler(c.stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	// A session that points at a cluster's service is reached through
	// the cluster's host, as its jobs' workers are.
	dialCluster := func(ctx context.Context, clusterHost, host string, port int) (net.Conn, error) {
		m := members[clusterHost]
		if m == nil {
			return nil, fmt.Errorf("meridian: no cluster of the clusters file has the host %q", clusterHost)
		}
		return ssh.DialTCP(ctx, m, host, port)
	}
	user := local.UserLogin()
	host, err := local.NewSessionHost(ctx, cfg, user, sessionKey, dialCluster, log)
	if err != nil {
		return nil, errors.Join(err, ssh.Close())
	}
	var trees coordinator.Trees = &cacheSources{dirs: dirs, servers: cfg.GitLabServers(), workflows: cfg.Workflows()}
	if c.trees != nil {
		trees = c.trees
	}
	coord, err := coordinator.Open(ctx, coordinator.Options{
		Database: db, Keys: keys,
		Variables: cfg, Inputs: cfg, Groups: cfg, Env: platformEnv(user),
		Trees: trees,
		Drivers: map[string]backend.Driver{
			"local": &localbackend.Driver{State: dirs.State, Withhold: cfg.SecretEnv()},
			"ssh":   ssh,
		},
		Endpoint:    backend.Endpoint{Address: dirs.Socket()},
		StartQueue:  startQueue,
		SessionHost: host,
		Log:         log,
	})
	if err != nil {
		host.Close()
		return nil, errors.Join(err, ssh.Close())
	}
	// The store names runs' directories on hosts, with their runs, and
	// its sessions' secrets.
	ssh.Store = coord.StoreID()
	host.Attach(coord.StoreID(), coord)
	return &coordination{coord: coord, sessions: host}, nil
}

// A coordination is what coordinate opens: a coordinator of the store,
// and its session host.
type coordination struct {
	coord    *coordinator.Coordinator
	sessions *local.SessionHost
}

// close closes the session host, and then the coordinator, once it has
// stopped serving.
func (co *coordination) close() error {
	co.sessions.Close()
	return co.coord.Close()
}

// progress returns the command's progress display (local.Progress), on
// which the session host says each session it serves, with a link where
// c's output shows one (cli.showsSessionLinks).
func (co *coordination) progress(c *cli) *local.Progress {
	p := local.NewProgress(c.stdout, c.stderr, c.json)
	co.sessions.AnnounceTo(p, c.showsSessionLinks())
	return p
}

// clusterMembers returns the member of the ssh backend that reaches each
// cluster of cfg's clusters file, by its host, through which a session
// that points at the cluster's service is reached.
func clusterMembers(cfg *local.Config) (map[string]*backend.Member, error) {
	members := map[string]*backend.Member{}
	for _, name := range cfg.ClusterNames() {
		host, _ := cfg.ClusterHost(name)
		settings, err := jsonv2.Marshal(map[string]any{"remoteHost": host})
		if err != nil {
			return nil, err
		}
		members[host] = &backend.Member{Name: host, Backend: "ssh", Settings: settings}
	}
	return members, nil
}

// platformEnv returns the variables a platform gives every job's steps
// that meridian gives them too (coordinator.Options.Env): PW_USER, format
// 1's name of the person who runs the workflow, as user, the login name of
// the user who runs meridian, which a session's path holds too, or none
// where there is none. Every run of the store is theirs.
func platformEnv(user local.Login) map[string]workerapi.EnvVar {
	if user.Err != nil {
		return nil
	}
	return map[string]workerapi.EnvVar{"PW_USER": {Value: user.Name}}
}

// hardenProcess keeps the user's other processes out of the process's
// memory (internal/harden), as c.harden does, which a test gives:
// coordinator_harden_failed when the system refuses, and
// coordinator_system_unsupported on a system that gives a process no way
// to.
func (c *cli) hardenProcess() error {
	h := c.harden
	if h == nil {
		h = harden.Process
	}
	switch err := h(); {
	case errors.Is(err, harden.ErrUnsupported):
		return problems.CoordinatorSystemUnsupported.Problem(problems.Params{"os": runtime.GOOS})
	case err != nil:
		return problems.CoordinatorHardenFailed.Problem(problems.Params{"reason": err.Error()})
	}
	return nil
}

// errNoStore is what storeOf returns for a state directory without a
// store, which has no runs.
var errNoStore = errors.New("no store")

// storeOf returns the database of the store in the state directory dirs
// names, which a run before made: errNoStore, without making it, if there
// is none.
func storeOf(dirs local.Dirs) (string, error) {
	if _, err := os.Lstat(filepath.Join(dirs.State, local.DatabaseFile)); errors.Is(err, fs.ErrNotExist) {
		return "", errNoStore
	}
	return dirs.Database()
}

// readStore opens a coordinator of the store in the state directory dirs
// names only to read it, as status and logs do, which change nothing in it,
// not even its schema, so as not to change a store that an earlier
// release, still running a workflow, uses (coordinator.Options.ReadOnly):
// errNoStore if there is none.
func readStore(ctx context.Context, c *cli, dirs local.Dirs) (*coordinator.Coordinator, error) {
	db, err := storeOf(dirs)
	if err != nil {
		return nil, err
	}
	return coordinator.Open(ctx, coordinator.Options{
		Database: db, ReadOnly: true,
		Log: slog.New(slog.NewTextHandler(c.stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
}

// runNotFound returns the problem of the run id, which the store doesn't
// have, for err, ErrRunNotFound or errNoStore, and otherwise err.
func runNotFound(id string, err error) error {
	if errors.Is(err, coordinator.ErrRunNotFound) || errors.Is(err, errNoStore) {
		return problems.RunNotFound.Problem(problems.Params{"run": id})
	}
	return err
}

// lockWriters has c write its standard output and its standard error under
// one lock, so that what goroutines write, as a run's progress as it goes,
// and the command's problems, doesn't break another's line.
func lockWriters(c *cli) {
	var mu sync.Mutex
	c.stdout, c.stderr = &lockedWriter{mu: &mu, w: c.stdout}, &lockedWriter{mu: &mu, w: c.stderr}
}

// showsSessionLinks reports whether c's run's output shows a session's
// link, with its one-time code: only as text, to a person's terminal,
// under its lock (lockWriters), as no log keeps it; never as JSON, which
// a program reads, and keeps, even on a terminal, as an editor's task
// runner does.
func (c *cli) showsSessionLinks() bool {
	if c.json {
		return false
	}
	if c.terminal != nil {
		return *c.terminal
	}
	var w any = c.stdout
	if l, ok := w.(*lockedWriter); ok {
		w = l.w
	}
	return isTerminal(w)
}

// A lockedWriter writes to w under mu, which other lockedWriters share, so
// that a write to one doesn't break another's line.
type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
