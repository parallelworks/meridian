package worker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// Config is how a worker runs. How it reaches the coordinator, and the
// attempt it claims, come from its environment, as its backend starts it,
// and never from its command line, where other users could read the
// bootstrap token.
type Config struct {
	// Worker is the meridian binary that runs the attempt, for the
	// attempt's record. Run fills in its digest, from the running
	// executable, when it is "".
	Worker workerapi.Worker
	// Environ is the worker's environment, as os.Environ gives it, or nil
	// for os.Environ(). It holds the coordinator's address, its key's hash
	// and the bootstrap token, or the file that holds it
	// (workerapi.EnvCoordinator and the rest), and the job's steps get the
	// rest of it.
	Environ []string
	// Dir is where the worker makes the directory it keeps its own files
	// in as it runs, which only its user can use: its spooled logs, and
	// its steps' scripts and $OUTPUTS files. It must be a directory of the
	// user's that no other user can change, on a path no other user can
	// change, or a symbolic link to one. When it is "",
	// or not such a directory, or goes, the worker uses meridian/worker in
	// the user's cache directory, which it makes, on disk; else
	// XDG_RUNTIME_DIR, if the environment sets it to such a directory,
	// which is in memory; and else os.TempDir(), with a warning in the
	// log, since another user could take the name of a directory there
	// that is removed. None is in the run's directory, which a step can
	// remove. Whatever removes the directory, the worker makes another,
	// with another name, rather than trust what is at the old path. Run
	// removes them before it returns.
	Dir string
	// SpoolLimit is the most bytes of logs the worker keeps that the
	// coordinator hasn't stored, or 0 for DefaultSpoolLimit, or
	// memorySpoolLimit while the worker's directory is in XDG_RUNTIME_DIR,
	// which is in memory. Past it, a
	// log loses what its step prints until the coordinator has stored
	// more, and the step's event warns of it.
	SpoolLimit int64
	// Log is where the worker logs what it does, for whoever runs it, or
	// nil for nowhere. What it logs as it sets its job up goes to the job's
	// setup log too.
	Log *slog.Logger

	// stepEnded, if it is set, is called as each step, or cleanup, has
	// ended, with the attempt: a test's seam.
	stepEnded func(a *attempt, step int, cleanup bool)
}

// DefaultSpoolLimit is how many bytes of logs a worker keeps on disk for
// the coordinator by default.
const DefaultSpoolLimit = 1 << 30

// memorySpoolLimit is how many it keeps by default in XDG_RUNTIME_DIR, a
// tmpfs, in memory, to which systemd gives a tenth of it.
const memorySpoolLimit = 64 << 20

// Run is meridian worker: it claims the attempt its environment names,
// runs the job's steps on this host, as package plan's Step phase readies
// each one, and reports on them over the worker API, until it has reported
// the attempt's end (docs/design.md, Workers).
//
// It returns nil once the coordinator has stored the attempt's end,
// whatever the job's outcome. It returns an error when it couldn't claim
// the attempt; when the attempt ended without it, because another attempt
// superseded it, the coordinator no longer accepts its credential, or, for
// a job that fences itself, its lease was about to lapse, once it has sent
// what it had until the lease lapsed; and when ctx is done before the
// job's steps have ended, once it has stopped them and, for its flush
// period after that, sent what it had. Then the coordinator decides how
// the attempt ended. When ctx is done once they have ended, it sends the
// attempt's end in that period.
//
// A job whose steps are to run in a container fails before its first step,
// with a problem that says so: this worker runs steps only on the host, and
// never runs a job with less isolation than it asks for. Before it claims
// the attempt, Run keeps the user's other processes, the job's steps among
// them, from reading the attempt's credential from its memory, as the
// system allows (on Linux, it makes the process not dumpable), and refuses
// to run on a system where it can't; and it sends a token over a Unix
// socket only once the system has said that its own user, or root, serves
// it.
func Run(ctx context.Context, cfg Config) error {
	return run(ctx, cfg, defaultTiming, harden)
}

// run is Run, with the timing it uses, and protect, which hardens the
// process before it reads the bootstrap token.
func run(ctx context.Context, cfg Config, tm timing, protect func() error) error {
	if err := protect(); err != nil {
		return err
	}
	environ := cfg.Environ
	if environ == nil {
		environ = os.Environ()
	}
	ep, stepsEnv, err := readEnv(environ, environReadable)
	if err != nil {
		return err
	}
	log := cfg.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	worker := cfg.Worker
	if worker.Digest == "" {
		if worker.Digest, err = executableDigest(); err != nil {
			return err
		}
	}
	if ep.keptTokenFile != nil {
		log.Warn("couldn't remove the bootstrap token's file, which is good for no more than the one claim", "error", ep.keptTokenFile)
	}
	// The worker's directory is made before it claims the attempt, so that
	// a worker that can't make one never runs it.
	files, err := makePrivateDir(privateParents(cfg.Dir, stepsEnv), log)
	if err != nil {
		return err
	}
	defer files.removeAll()

	c := newClient(ep, tm, log)
	defer c.close()
	credential := newCredential()
	claim, claimed, err := c.claim(ctx, ep.token, workerapi.ClaimRequest{Credential: credential, PlanVersion: plan.Version, Worker: worker})
	if err != nil {
		return err
	}
	c.credential = credential
	log.Info("claimed an attempt", "run", claim.RunID, "attempt", claim.AttemptID, "number", claim.Attempt)
	a := &attempt{
		claim:   claim,
		claimed: claimed,
		client:  c,
		tm:      tm,
		log:     log,
		files:   files,
		environ: stepsEnv,
		worker:  worker,
		events:  newEventQueue(),
		spool:   newSpool(files, cfg.SpoolLimit),

		stepEnded: cfg.stepEnded,
	}
	return a.run(ctx)
}

// An endpoint is how a worker reaches its coordinator, and the bootstrap
// token it claims its attempt with.
type endpoint struct {
	// address is a Unix socket's path, or a host and port for TLS 1.3.
	address string
	// keyHash is the SHA-256 digest of the coordinator's TLS public key,
	// for TLS.
	keyHash []byte
	token   string
	// keptTokenFile is why the worker couldn't remove the token's file,
	// if it couldn't.
	keptTokenFile error
}

func (e endpoint) socket() bool { return strings.HasPrefix(e.address, "/") }

// readEnv reads how to reach the coordinator, and the bootstrap token, from
// environ, and returns the rest of it, which the job's steps get. It reads
// the token from the file EnvBootstrapTokenFile names, if it does, and
// removes the file; it refuses one that other users can read. Where the
// user's other processes can read the worker's environment, as readable
// says (environReadable), it refuses a token in EnvBootstrapToken.
func readEnv(environ []string, readable bool) (endpoint, []string, error) {
	vars := map[string]string{}
	var rest []string
	for _, kv := range environ {
		name, value, _ := strings.Cut(kv, "=")
		switch name {
		case workerapi.EnvCoordinator, workerapi.EnvKeySHA256, workerapi.EnvBootstrapToken, workerapi.EnvBootstrapTokenFile:
			vars[name] = value
		default:
			rest = append(rest, kv)
		}
	}
	ep := endpoint{address: vars[workerapi.EnvCoordinator]}
	key := vars[workerapi.EnvKeySHA256]
	missing := func(variable string) error {
		return problems.WorkerEnvironmentMissing.Problem(problems.Params{"variable": variable})
	}
	invalid := func(variable string) error {
		return problems.WorkerEnvironmentInvalid.Problem(problems.Params{"variable": variable})
	}
	switch {
	case ep.address == "":
		return ep, nil, missing(workerapi.EnvCoordinator)
	case ep.socket() && key != "":
		// A Unix socket needs no key: one set means the backend meant TLS.
		return ep, nil, invalid(workerapi.EnvKeySHA256)
	case !ep.socket() && !hostPort(ep.address):
		return ep, nil, invalid(workerapi.EnvCoordinator)
	case !ep.socket():
		hexKey, ok := strings.CutPrefix(key, "sha256:")
		hash, err := hex.DecodeString(hexKey)
		if !ok || err != nil || len(hash) != sha256.Size {
			return ep, nil, invalid(workerapi.EnvKeySHA256)
		}
		ep.keyHash = hash
	}
	token, file := vars[workerapi.EnvBootstrapToken], vars[workerapi.EnvBootstrapTokenFile]
	switch {
	case token != "" && file != "":
		return ep, nil, invalid(workerapi.EnvBootstrapTokenFile)
	case token != "" && readable:
		return ep, nil, problems.BootstrapTokenInEnvironment.Problem(nil)
	case file != "":
		var err error
		if token, err = readTokenFile(file); err != nil {
			return ep, nil, err
		}
		// The token is good for one claim, which the worker makes at once.
		ep.keptTokenFile = os.Remove(file)
	}
	if token == "" {
		return ep, nil, missing(workerapi.EnvBootstrapToken)
	}
	ep.token = token
	return ep, rest, nil
}

// maxTokenFile is the most bytes the worker reads of a bootstrap token's
// file.
const maxTokenFile = 4096

// hostPort reports whether address is a host and port, or a host, that a
// URL can hold as it is, as the coordinator's address over TLS must be.
func hostPort(address string) bool {
	u, err := url.Parse("https://" + address)
	return err == nil && u.Host == address && u.Path == "" && u.RawQuery == "" && u.Fragment == ""
}

// readTokenFile reads a bootstrap token from a file of the worker's user
// that only the user can read, not through a symbolic link, on a path no
// other user can change (safepath.ReadFile).
func readTokenFile(path string) (string, error) {
	data, err := safepath.ReadFile(path, safepath.Private, maxTokenFile)
	if err != nil {
		if _, ok := errors.AsType[*problem.Problem](err); ok {
			return "", err
		}
		return "", problems.FileUnreadable.Problem(problems.Params{"path": path, "reason": err.Error()})
	}
	return strings.TrimSpace(string(data)), nil
}

// newCredential makes the attempt's credential: 32 bytes from crypto/rand,
// as hex.
func newCredential() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b) // crypto/rand.Read never fails; it ends the program first
	return hex.EncodeToString(b)
}

// executableDigest returns sha256: and the hex of the running executable's
// SHA-256 digest.
func executableDigest() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", problems.FileUnreadable.Problem(problems.Params{"path": os.Args[0], "reason": err.Error()})
	}
	f, err := os.Open(path) //nolint:gosec // the running executable
	if err != nil {
		return "", problems.FileUnreadable.Problem(problems.Params{"path": path, "reason": err.Error()})
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", problems.FileUnreadable.Problem(problems.Params{"path": path, "reason": err.Error()})
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}
