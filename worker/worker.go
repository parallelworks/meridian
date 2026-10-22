package worker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/parallelworks/meridian/plan"
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
	// in as it runs, which only its user can read: its spooled logs, and
	// its steps' scripts and $OUTPUTS files. "" is os.TempDir(). Run
	// removes the directory before it returns.
	Dir string
	// SpoolLimit is the most bytes of logs the worker keeps on disk that
	// the coordinator hasn't stored, or 0 for DefaultSpoolLimit. Past it,
	// a log loses what its step prints until the coordinator has stored
	// more, and the step's event warns of it.
	SpoolLimit int64
	// Log is where the worker logs what it does, for whoever runs it, or
	// nil for nowhere. What it logs as it sets its job up goes to the job's
	// setup log too.
	Log *slog.Logger
}

// DefaultSpoolLimit is how many bytes of logs a worker keeps on disk for
// the coordinator by default.
const DefaultSpoolLimit = 1 << 30

// Run is meridian worker: it claims the attempt its environment names,
// runs the job's steps on this host, as package plan's Step phase readies
// each one, and reports on them over the worker API, until it has reported
// the attempt's end (docs/design.md, Workers).
//
// It returns nil once the coordinator has stored the attempt's end,
// whatever the job's outcome. It returns an error when it couldn't claim
// the attempt; when the attempt ended without it, because another attempt
// superseded it, the coordinator no longer accepts its credential, or, for
// a job that fences itself, its lease was about to lapse; and when ctx is
// done before the job's steps have ended, once it has stopped them and,
// for its flush period, sent what it had. Then the coordinator decides how
// the attempt ended. When ctx is done once they have ended, it sends the
// attempt's end in that period.
//
// A job whose steps are to run in a container fails before its first step,
// with a problem that says so: this worker runs steps only on the host, and
// never runs a job with less isolation than it asks for. On Linux, Run
// makes the process not dumpable, so that the user's other processes, the
// job's steps among them, can't read the attempt's credential from its
// memory.
func Run(ctx context.Context, cfg Config) error {
	if err := harden(); err != nil {
		return err
	}
	return run(ctx, cfg, planPhases, defaultTiming)
}

func run(ctx context.Context, cfg Config, ph phases, tm timing) error {
	environ := cfg.Environ
	if environ == nil {
		environ = os.Environ()
	}
	ep, stepsEnv, err := readEnv(environ)
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
	limit := cfg.SpoolLimit
	if limit <= 0 {
		limit = DefaultSpoolLimit
	}
	dir, err := os.MkdirTemp(cfg.Dir, "meridian-worker-")
	if err != nil {
		return fmt.Errorf("worker: making its directory: %w", err)
	}
	defer os.RemoveAll(dir)

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
		ph:      ph,
		tm:      tm,
		log:     log,
		dir:     dir,
		environ: stepsEnv,
		worker:  worker,
		events:  newEventQueue(),
		spool:   newSpool(dir, limit),
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
}

func (e endpoint) socket() bool { return strings.HasPrefix(e.address, "/") }

// readEnv reads how to reach the coordinator, and the bootstrap token, from
// environ, and returns the rest of it, which the job's steps get. It reads
// the token from the file EnvBootstrapTokenFile names, if it does, and
// removes the file; it refuses one that other users can read.
func readEnv(environ []string) (endpoint, []string, error) {
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
	switch {
	case ep.address == "":
		return ep, nil, fmt.Errorf("worker: %s isn't set", workerapi.EnvCoordinator)
	case ep.socket() && key != "":
		return ep, nil, fmt.Errorf("worker: %s is set for a coordinator reached through the Unix socket %s", workerapi.EnvKeySHA256, ep.address)
	case !ep.socket():
		hexKey, ok := strings.CutPrefix(key, "sha256:")
		hash, err := hex.DecodeString(hexKey)
		if !ok || err != nil || len(hash) != sha256.Size {
			return ep, nil, fmt.Errorf("worker: %s must be sha256: and 64 hex digits to reach %s over TLS", workerapi.EnvKeySHA256, ep.address)
		}
		ep.keyHash = hash
	}
	token, file := vars[workerapi.EnvBootstrapToken], vars[workerapi.EnvBootstrapTokenFile]
	switch {
	case token != "" && file != "":
		return ep, nil, fmt.Errorf("worker: both %s and %s are set", workerapi.EnvBootstrapToken, workerapi.EnvBootstrapTokenFile)
	case file != "":
		var err error
		if token, err = readTokenFile(file); err != nil {
			return ep, nil, err
		}
	}
	if token == "" {
		return ep, nil, fmt.Errorf("worker: neither %s nor %s holds a bootstrap token", workerapi.EnvBootstrapToken, workerapi.EnvBootstrapTokenFile)
	}
	ep.token = token
	return ep, rest, nil
}

// readTokenFile reads a bootstrap token from a file of the worker's user
// that only the user can read, not through a symbolic link, and removes
// it.
func readTokenFile(path string) (string, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|openNoFollow|openNonBlock, 0) //nolint:gosec // the file the backend put the token in
	if err != nil {
		return "", fmt.Errorf("worker: reading the bootstrap token: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("worker: reading the bootstrap token: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || !ownedByUser(info) {
		return "", fmt.Errorf("worker: the bootstrap token's file %s must be a file of this user's that only it can read", path)
	}
	data, err := io.ReadAll(io.LimitReader(f, 4096))
	if err != nil {
		return "", fmt.Errorf("worker: reading the bootstrap token: %w", err)
	}
	if err := os.Remove(path); err != nil {
		return "", fmt.Errorf("worker: removing the bootstrap token's file: %w", err)
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
		return "", fmt.Errorf("worker: finding its executable: %w", err)
	}
	f, err := os.Open(path) //nolint:gosec // the running executable
	if err != nil {
		return "", fmt.Errorf("worker: reading its executable: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("worker: reading its executable: %w", err)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}
