package coordinator

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/coordinator/internal/coordtest"
	"github.com/parallelworks/meridian/coordinator/internal/runlog/runlogtest"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/worker"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// newTLSKey returns a new ECDSA key on P-256, as a coordinator's TLS key.
func newTLSKey(t testing.TB) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// keyHashOf returns the hash a worker knows key by: sha256: and the hex of
// the SHA-256 digest of its public key's DER SubjectPublicKeyInfo.
func keyHashOf(t testing.TB, key *ecdsa.PrivateKey) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(der)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// The coordinator's certificate is one its key signs itself, for TLS
// servers, whose key has the hash a worker is placed with; another
// process's of the same key is another certificate of the same hash. A key
// that isn't an ECDSA key on P-256 isn't taken.
func TestTLSIdentity(t *testing.T) {
	key := newTLSKey(t)
	a, err := newTLSIdentity(key)
	if err != nil {
		t.Fatal(err)
	}
	leaf := a.cert.Leaf
	if a.keyHash != keyHashOf(t, key) {
		t.Errorf("the identity's hash is %s, want %s", a.keyHash, keyHashOf(t, key))
	}
	if err := leaf.CheckSignature(leaf.SignatureAlgorithm, leaf.RawTBSCertificate, leaf.Signature); err != nil {
		t.Errorf("the certificate isn't its key's own: %v", err)
	}
	if leaf.SignatureAlgorithm != x509.ECDSAWithSHA256 || len(leaf.ExtKeyUsage) != 1 || leaf.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth || leaf.IsCA {
		t.Errorf("the certificate is %v, for %v, CA %t", leaf.SignatureAlgorithm, leaf.ExtKeyUsage, leaf.IsCA)
	}
	b, err := newTLSIdentity(key)
	if err != nil {
		t.Fatal(err)
	}
	if b.keyHash != a.keyHash || leaf.SerialNumber.Cmp(b.cert.Leaf.SerialNumber) == 0 {
		t.Errorf("two identities of one key have the hashes %s and %s, and the serials %v and %v", a.keyHash, b.keyHash, leaf.SerialNumber, b.cert.Leaf.SerialNumber)
	}

	p384, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, ed, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for name, key := range map[string]crypto.Signer{"P-384": p384, "Ed25519": ed} {
		c, err := Open(t.Context(), Options{Database: filepath.Join(coordtest.PrivateDir(t), "meridian.db"), Keys: coordtest.NewKeys(t), TLSKey: key})
		if err == nil {
			c.Close()
			t.Errorf("Open took an %s TLS key", name)
		}
	}
}

// relayMember is the key of a relayDriver's every member.
const relayMember = "batch-test-member"

// A relayDriver is a workerDriver whose members' workers reach the
// coordinator over TLS through a relay of the test's: a port of the
// loopback interface, which passes each connection on to the Unix socket
// Forward last listened on, without reading it, as a relay on a login node
// passes them to a member's socket forwarded over SSH. It records the
// requests it places workers with.
type relayDriver struct {
	*workerDriver
	relay net.Listener

	rmu      sync.Mutex
	socket   string
	forwards int
	requests []backend.PlaceRequest
	conns    map[net.Conn]bool
	relays   sync.WaitGroup
}

var _ backend.Forwarder = (*relayDriver)(nil)

// newRelayDriver returns a relayDriver whose workers run in dir, as the
// test's, until it ends.
func newRelayDriver(t *testing.T, dir string) *relayDriver {
	t.Helper()
	l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	d := &relayDriver{
		workerDriver: &workerDriver{dir: dir, log: t.Logf, placements: map[string]backend.Placement{}, cancels: map[string]context.CancelFunc{}},
		relay:        l, conns: map[net.Conn]bool{},
	}
	ctx := context.WithoutCancel(t.Context())
	d.relays.Go(func() { d.serveRelay(ctx) })
	t.Cleanup(func() {
		l.Close()
		d.rmu.Lock()
		for c := range d.conns {
			c.Close()
		}
		d.rmu.Unlock()
		d.relays.Wait()
	})
	return d
}

// serveRelay passes each connection the relay takes on to the socket
// Forward last listened on, until the relay closes.
func (d *relayDriver) serveRelay(ctx context.Context) {
	for {
		c, err := d.relay.Accept()
		if err != nil {
			return
		}
		d.rmu.Lock()
		socket := d.socket
		d.conns[c] = true
		d.rmu.Unlock()
		d.relays.Go(func() {
			defer func() {
				c.Close()
				d.rmu.Lock()
				delete(d.conns, c)
				d.rmu.Unlock()
			}()
			s, err := new(net.Dialer).DialContext(ctx, "unix", socket)
			if err != nil {
				return
			}
			defer s.Close()
			done := make(chan struct{})
			go func() {
				_, _ = io.Copy(s, c)
				_ = s.(*net.UnixConn).CloseWrite()
				close(done)
			}()
			_, _ = io.Copy(c, s)
			c.Close()
			<-done
		})
	}
}

func (d *relayDriver) MemberKey(*backend.Member) (string, error) { return relayMember, nil }

func (d *relayDriver) Endpoint(context.Context, *backend.Member) (backend.Endpoint, error) {
	return backend.Endpoint{Address: d.relay.Addr().String()}, nil
}

func (d *relayDriver) Forward(ctx context.Context, _ *backend.Member) (net.Listener, error) {
	d.rmu.Lock()
	defer d.rmu.Unlock()
	path := filepath.Join(d.dir, fmt.Sprintf("m%d.sock", d.forwards+1))
	l, err := new(net.ListenConfig).Listen(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	// Counted once the relay passes connections to it.
	d.forwards++
	d.socket = path
	return l, nil
}

func (d *relayDriver) Place(ctx context.Context, m *backend.Member, req backend.PlaceRequest) (backend.Placement, error) {
	d.rmu.Lock()
	d.requests = append(d.requests, req)
	d.rmu.Unlock()
	return d.workerDriver.Place(ctx, m, req)
}

// placed returns the requests d placed workers with, and how many times it
// forwarded its member's listener.
func (d *relayDriver) placed() ([]backend.PlaceRequest, int) {
	d.rmu.Lock()
	defer d.rmu.Unlock()
	return append([]backend.PlaceRequest(nil), d.requests...), d.forwards
}

// relayDir returns a directory of the test's own, whose path is short
// enough for a Unix socket's.
func relayDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "meridian-tls-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// serveRelayMembers has c serve its members' listeners, storing logs in
// logs, until the test ends, or the function it returns is called, which
// returns what ServeMembers did. d's workers end first.
func serveRelayMembers(t *testing.T, c *Coordinator, d *relayDriver, logs Logs) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	served := make(chan error, 1)
	go func() { served <- c.ServeMembers(ctx, logs) }()
	stop := sync.OnceValue(func() error {
		cancel()
		return <-served
	})
	t.Cleanup(func() { _ = stop() })
	// The workers end before the server they report to stops.
	t.Cleanup(func() {
		d.mu.Lock()
		for _, cancel := range d.cancels {
			cancel()
		}
		d.mu.Unlock()
		d.workers.Wait()
	})
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.forwarding != nil
	})
	return stop
}

// tlsJob is a workflow of one job whose step says it ran.
const tlsJob = `
jobs:
  main:
    steps:
      - run: echo over tls
`

// A member whose workers reach the coordinator over TLS, through a relay,
// is served over TLS with the coordinator's key, which its workers are
// placed with the hash of, and which meridian's worker checks: its job
// runs to its end, in FIPS 140-3 mode, as the tests run.
func TestServeMembersTLS(t *testing.T) {
	parallel(t)
	dir := relayDir(t)
	d := newRelayDriver(t, dir)
	key := newTLSKey(t)
	c := open(t, Options{Drivers: map[string]backend.Driver{testBackend: d}, TLSKey: key})
	logs := &runlogtest.Logs{}
	serveRelayMembers(t, c, d, logs)
	r := &workerRig{c: c, d: d.workerDriver, logs: logs, runs: filepath.Join(dir, "runs")}
	id := r.create(t, []byte(tlsJob), nil, nil)
	serve(t, c)
	run := waitEnded(t, c, id)
	if run.Outcome != plan.OutcomeSuccess {
		t.Fatalf("the run ended %s: %v", run.Outcome, storedJob(t, c, id, "main").Diagnostics)
	}
	if log := logs.Of(id, "main", 1)[workerapitest.LogKey{Part: workerapi.LogStep}]; string(log.Data) != "over tls\n" {
		t.Errorf("the step's log is %q", log.Data)
	}
	requests, _ := d.placed()
	if len(requests) != 1 || requests[0].Coordinator != (backend.Endpoint{Address: d.relay.Addr().String(), KeyHash: keyHashOf(t, key)}) {
		t.Errorf("the workers were placed with %+v", requests)
	}
	if a := latestAttempt(t, c, id, "main"); a.Placement.MemberKey != relayMember {
		t.Errorf("the attempt was placed on the member %q", a.Placement.MemberKey)
	}
}

// runWorker runs meridian's worker for the attempt req placed, pinned to
// keyHash, in a directory of its own under dir, and returns how it ended.
func runWorker(ctx context.Context, t *testing.T, dir string, req backend.PlaceRequest, keyHash string) error {
	t.Helper()
	wd, err := os.MkdirTemp(dir, "worker-")
	if err != nil {
		t.Fatal(err)
	}
	token := filepath.Join(wd, "token")
	if err := os.WriteFile(token, []byte(req.BootstrapToken), 0o600); err != nil {
		t.Fatal(err)
	}
	var environ []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "MERIDIAN_") {
			environ = append(environ, kv)
		}
	}
	environ = append(environ, workerapi.EnvCoordinator+"="+req.Coordinator.Address, workerapi.EnvKeySHA256+"="+keyHash, workerapi.EnvBootstrapTokenFile+"="+token)
	return worker.Run(ctx, worker.Config{Worker: workerapi.Worker{Version: "test", Digest: testDigest}, Environ: environ, Dir: wd})
}

// A worker pinned to another key than the coordinator's refuses it before
// it sends its token, which then claims the attempt for the worker pinned
// to the coordinator's. A coordinator that takes the member's listener over
// from one that stopped serves it with the same key, as each of meridian's
// processes reads the same key from the state directory
// (local.CoordinatorKey, which its own tests check): the worker placed by
// the first reaches the second.
func TestServeMembersTLSPinned(t *testing.T) {
	parallel(t)
	dir := relayDir(t)
	d := newRelayDriver(t, dir)
	d.idle = func(backend.PlaceRequest) bool { return true }
	key := newTLSKey(t)
	db := filepath.Join(coordtest.PrivateDir(t), "meridian.db")
	keys := coordtest.NewKeys(t)
	first := open(t, Options{Database: db, Keys: keys, Drivers: map[string]backend.Driver{testBackend: d}, TLSKey: key})
	second := open(t, Options{Database: db, Keys: keys, Drivers: map[string]backend.Driver{testBackend: d}, TLSKey: key})
	logs := &runlogtest.Logs{}
	stopFirst := serveRelayMembers(t, first, d, logs)
	r := &workerRig{c: first, d: d.workerDriver, logs: logs, runs: filepath.Join(dir, "runs")}
	id := r.create(t, []byte(tlsJob), nil, nil)
	serve(t, first)
	var req backend.PlaceRequest
	waitFor(t, func() bool {
		requests, _ := d.placed()
		if len(requests) == 0 {
			return false
		}
		req = requests[0]
		return true
	})

	other := keyHashOf(t, newTLSKey(t))
	err := runWorker(t.Context(), t, dir, req, other)
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.CoordinatorKeyMismatch.Code() {
		t.Fatalf("a worker pinned to another key ended with %v, want coordinator_key_mismatch", err)
	}
	if a := latestAttempt(t, first, id, "main"); a.State != store.AttemptPlacing || !a.ClaimedAt.IsZero() {
		t.Fatalf("the attempt is %s, claimed at %v, once the worker pinned to another key refused the coordinator", a.State, a.ClaimedAt)
	}

	if err := stopFirst(); err != nil {
		t.Fatal(err)
	}
	serveRelayMembers(t, second, d, logs)
	waitFor(t, func() bool { _, forwards := d.placed(); return forwards == 2 })
	if err := runWorker(t.Context(), t, dir, req, req.Coordinator.KeyHash); err != nil {
		t.Fatalf("the worker pinned to the coordinator's key, through the coordinator that took over: %v", err)
	}
	if run := waitEnded(t, first, id); run.Outcome != plan.OutcomeSuccess {
		t.Errorf("the run ended %s", run.Outcome)
	}
	if req.Coordinator.KeyHash != keyHashOf(t, key) {
		t.Errorf("the worker was placed with the hash %s, want %s", req.Coordinator.KeyHash, keyHashOf(t, key))
	}
}

// A coordinator that may write the store doesn't open without a TLS key,
// since it may come to hold any member's listener, and nothing is made of
// the store; one opened only to read the store needs none.
func TestOpenRequiresTLSKey(t *testing.T) {
	parallel(t)
	db := filepath.Join(coordtest.PrivateDir(t), "meridian.db")
	if c, err := Open(t.Context(), Options{Database: db, Keys: coordtest.NewKeys(t)}); err == nil {
		c.Close()
		t.Fatal("a coordinator opened without a TLS key")
	}
	if _, err := os.Lstat(db); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the refused coordinator left its store: %v", err)
	}
	open(t, Options{Database: db})
	r, err := Open(t.Context(), Options{Database: db, ReadOnly: true})
	if err != nil {
		t.Fatalf("a coordinator opened only to read the store: %v", err)
	}
	r.Close()
}
