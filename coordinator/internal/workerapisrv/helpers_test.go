package workerapisrv_test

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

func TestMain(m *testing.M) {
	// A test that changes the store from another process starts this
	// binary again, which does what envChild names (TestControlAcrossProcesses).
	if role := os.Getenv(envChild); role != "" {
		os.Exit(runChild(role))
	}
	os.Exit(m.Run())
}

// testWorkflow is the workflow of the tests' runs: build, of three steps,
// which reads an input and an output of prep, which it needs.
const testWorkflow = `
on:
  execute:
    inputs:
      note:
        type: string
        default: hello
jobs:
  prep:
    steps:
      - id: make
        run: echo "x=1" >> $OUTPUTS
    outputs:
      x: ${{ steps.make.outputs.x }}
  build:
    needs: [prep]
    steps:
      - run: echo ${{ inputs.note }} ${{ needs.prep.outputs.x }}
      - run: echo two
      - run: echo three
`

// created returns what Create gives of testWorkflow.
func created(t testing.TB) *compile.Created {
	t.Helper()
	c, diags, err := compile.Create(t.Context(), "w.yaml", []byte(testWorkflow), nil, compile.Options{Source: plan.Source{Location: "w.yaml"}})
	if err != nil {
		t.Fatal(err)
	}
	if diags.HasErrors() {
		t.Fatalf("the test workflow has errors: %v", diags)
	}
	return c
}

// privateDir returns a directory of the test's that only its user can use.
func privateDir(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

// openStore opens a new store, which the test closes, and returns it and
// its database's path.
func openStore(t testing.TB) (*store.Store, string) {
	t.Helper()
	path := filepath.Join(privateDir(t), "meridian.db")
	s, err := store.Open(t.Context(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

// jobIDs makes hopper job IDs for runs that insert no hopper job.
var jobIDs struct {
	sync.Mutex
	next int
}

func fakeJobID() hopper.JobID {
	jobIDs.Lock()
	defer jobIDs.Unlock()
	jobIDs.next++
	var id hopper.JobID
	id[15], id[14], id[13] = byte(jobIDs.next), byte(jobIDs.next>>8), 0xfe
	return id
}

// testKeys wraps data keys with AES-256-GCM under a key of its own, bound
// to the run, as the local Keys does.
type testKeys struct{ aead cipher.AEAD }

func newKeys(t testing.TB) *testKeys {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		t.Fatal(err)
	}
	return &testKeys{aead}
}

func (k *testKeys) Wrap(_ context.Context, runID string, key []byte) ([]byte, error) {
	return k.aead.Seal(nil, nil, key, []byte(runID)), nil
}

func (k *testKeys) Unwrap(_ context.Context, runID string, wrapped []byte) ([]byte, error) {
	return k.aead.Open(nil, nil, wrapped, []byte(runID))
}

// memLogs keeps logs in memory, as coordinator.Logs says.
type memLogs struct {
	mu   sync.Mutex
	logs map[workerapisrv.LogKey]*workerapitest.Log
}

func (l *memLogs) Append(_ context.Context, key workerapisrv.LogKey, offset int64, data []byte, end bool) (int64, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.logs == nil {
		l.logs = map[workerapisrv.LogKey]*workerapitest.Log{}
	}
	log := l.logs[key]
	if log == nil {
		log = &workerapitest.Log{}
		l.logs[key] = log
	}
	length := int64(len(log.Data))
	switch {
	case offset > length, log.Ended && offset+int64(len(data)) > length:
		return length, log.Ended, workerapisrv.ErrLogGap
	case offset+int64(len(data)) > length:
		log.Data = append(log.Data, data[length-offset:]...)
	}
	if end {
		log.Ended = true
	}
	return int64(len(log.Data)), log.Ended, nil
}

// of returns the logs of the attempt number of run's job.
func (l *memLogs) of(run, job string, number int) map[workerapitest.LogKey]workerapitest.Log {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[workerapitest.LogKey]workerapitest.Log{}
	for k, v := range l.logs {
		if k.Run == run && k.Job == job && k.Attempt == number {
			out[workerapitest.LogKey{Part: k.Part, Step: k.Step, Try: k.Try}] = workerapitest.Log{Data: bytes.Clone(v.Data), Ended: v.Ended}
		}
	}
	return out
}

// fakeReadier gives each run's attempts the job, the run's values and the
// needs' results a test set for the run.
type fakeReadier struct {
	mu   sync.Mutex
	runs map[string]*workerapisrv.Ready
}

func (f *fakeReadier) set(run string, r *workerapisrv.Ready) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.runs == nil {
		f.runs = map[string]*workerapisrv.Ready{}
	}
	f.runs[run] = r
}

func (f *fakeReadier) get(run string) (*workerapisrv.Ready, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.runs[run]
	if r == nil {
		return nil, fmt.Errorf("no job for run %s", run)
	}
	return r, nil
}

func (f *fakeReadier) ReadyAttempt(_ context.Context, run *store.Run, _ *store.Attempt) (*workerapisrv.Ready, error) {
	return f.get(run.ID)
}

func (f *fakeReadier) Steps(_ context.Context, run *store.Run, _ *store.Attempt) (int, error) {
	r, err := f.get(run.ID)
	if err != nil {
		return 0, err
	}
	return len(r.Job.Steps), nil
}

// A rig is a store, and a server of it on a socket of its own.
type rig struct {
	t       *testing.T
	store   *store.Store
	path    string
	sealer  *seal.Sealer
	readier *fakeReadier
	logs    *memLogs
	server  *workerapisrv.Server
	socket  string

	mu    sync.Mutex
	woken []string
}

// rigOptions change the rig's server.
type rigOptions struct {
	peers workerapisrv.Peers
	// older makes the server a replica of an older release, which reads
	// only the plan versions before this one's.
	older bool
	hold  time.Duration
}

func newRig(t *testing.T, opts rigOptions) *rig {
	t.Helper()
	s, path := openStore(t)
	sealer, err := seal.New(newKeys(t), s.ID())
	if err != nil {
		t.Fatal(err)
	}
	r := &rig{t: t, store: s, path: path, sealer: sealer, readier: &fakeReadier{}, logs: &memLogs{}}
	r.server = r.serve(opts)
	return r
}

// serve starts another server of the rig's store, as another replica, and
// returns it, with its socket in r.socket if it is the first.
func (r *rig) serve(opts rigOptions) *workerapisrv.Server {
	r.t.Helper()
	srv, err := workerapisrv.New(workerapisrv.Options{
		Store: r.store, Logs: r.logs, Sealer: r.sealer, Readier: r.readier, Peers: opts.peers,
		Wake: func(_ context.Context, run, job string) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.woken = append(r.woken, run+"/"+job)
		},
	})
	if err != nil {
		r.t.Fatal(err)
	}
	if opts.older {
		workerapisrv.SetReads(srv, plan.Version-1)
	}
	hold := opts.hold
	if hold == 0 {
		hold = 2 * time.Second
	}
	workerapisrv.SetHold(srv, hold)
	socket := listen(r.t, srv)
	if r.socket == "" {
		r.socket = socket
	}
	return srv
}

// listen serves srv on a new Unix socket until the test ends, and returns
// the socket's path.
func listen(t *testing.T, srv *workerapisrv.Server) string {
	t.Helper()
	// A socket's path is short: t.TempDir's can be too long.
	dir, err := os.MkdirTemp("", "ws")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "c.sock")
	l, err := new(net.ListenConfig).Listen(t.Context(), "unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, l) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	return socket
}

// newRun stores a run of testWorkflow, with its jobs and a secret record,
// and returns its ID.
func (r *rig) newRun(rec *plan.Record) string {
	r.t.Helper()
	c := created(r.t)
	if rec == nil {
		rec = &c.Record
	}
	var id string
	err := r.store.Tx(r.t.Context(), func(tx *store.Tx) error {
		var err error
		if id, err = tx.NewRunID(); err != nil {
			return err
		}
		if err := tx.InsertRun(store.NewRun{ID: id, Plan: c.Plan, DefaultGroup: "local", RunDir: "/runs/" + id, WorkflowID: fakeJobID()}); err != nil {
			return err
		}
		for _, j := range c.Plan.Jobs {
			if err := tx.InsertJob(store.NewJob{Run: id, Name: j.Name, Key: j.Name, SupervisorID: fakeJobID()}); err != nil {
				return err
			}
		}
		sealed, gen, err := r.sealer.Seal(r.t.Context(), id, rec)
		if err != nil {
			return err
		}
		return tx.InsertRecord(id, gen, sealed.Key, sealed.Record)
	})
	if err != nil {
		r.t.Fatal(err)
	}
	return id
}

// placed are the terms of an attempt to start.
type placed struct {
	selfFencing           bool
	lease, heartbeat      int64
	digest, group, member string
}

// newAttempt starts an attempt of run's job, placed with p, and returns it
// and its bootstrap token.
func (r *rig) newAttempt(run, job string, p placed) (*store.Attempt, string) {
	r.t.Helper()
	if p.lease == 0 {
		p.lease, p.heartbeat = 60, 10
	}
	var a *store.Attempt
	var token string
	err := r.store.Tx(r.t.Context(), func(tx *store.Tx) error {
		var err error
		a, token, err = tx.NewAttempt(store.NewAttempt{Run: run, Job: job, SelfFencing: p.selfFencing, LeaseSeconds: p.lease, HeartbeatSeconds: p.heartbeat})
		if err != nil {
			return err
		}
		return tx.RecordPlacement(a.ID, store.Placement{Group: p.group, Member: p.member, Backend: "local", ID: "1:2", WorkerDigest: p.digest})
	})
	if err != nil {
		r.t.Fatal(err)
	}
	return a, token
}

// attempt returns the attempt id as the store holds it.
func (r *rig) attempt(id string) *store.Attempt {
	r.t.Helper()
	var a *store.Attempt
	if err := r.store.View(r.t.Context(), func(tx *store.Tx) error {
		var err error
		a, err = tx.Attempt(id)
		return err
	}); err != nil {
		r.t.Fatal(err)
	}
	return a
}

// inTx runs fn in a transaction of the rig's store.
func (r *rig) inTx(fn func(*store.Tx) error) {
	r.t.Helper()
	if err := r.store.Tx(r.t.Context(), fn); err != nil {
		r.t.Fatal(err)
	}
}

// wokenJobs returns the jobs the server woke, as run/job.
func (r *rig) wokenJobs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.woken...)
}

// conformanceDigest is the digest of the worker Conformance plays.
var conformanceDigest = "sha256:" + strings.Repeat("0", 64)

// A client talks to a server on its socket, as a worker does.
type client struct {
	t    *testing.T
	http *http.Client
}

func newClient(t *testing.T, socket string) *client {
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return new(net.Dialer).DialContext(ctx, "unix", socket)
	}}
	t.Cleanup(tr.CloseIdleConnections)
	return &client{t: t, http: &http.Client{Transport: tr, Timeout: time.Minute}}
}

// send sends a request, with bearer unless it is "", and returns its
// status and body, or the error of a request that got no answer.
func (c *client) send(method, path string, body []byte, bearer string) (int, []byte, error) {
	c.t.Helper()
	req, err := http.NewRequestWithContext(c.t.Context(), method, "http://coordinator"+path, bytes.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	var b bytes.Buffer
	_, err = b.ReadFrom(resp.Body)
	return resp.StatusCode, b.Bytes(), err
}

// claimRequest returns a claim with credential, of the worker Conformance
// plays but with digest.
func claimRequest(t *testing.T, credential, digest string) []byte {
	t.Helper()
	data, err := workerapi.Marshal(workerapi.ClaimRequest{Credential: credential, PlanVersion: plan.Version, Worker: workerapi.Worker{Version: "v0", Digest: digest}})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// newCredential returns a credential as a worker makes one.
func newCredential(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}
