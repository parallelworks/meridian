package workerapisrv_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/coordtest"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/runlog/runlogtest"
	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

func TestMain(m *testing.M) {
	// A test that changes the store from another process starts this
	// binary again, which does what envChild names (TestControlAcrossProcesses).
	if role := os.Getenv(envChild); role != "" {
		os.Exit(runChild(role))
	}
	code := m.Run()
	storeTemplate.Remove()
	os.Exit(code)
}

// parallelTests bounds how many of the package's tests that run in
// parallel run at once, beside go test's -parallel, as the coordinator's
// tests do: each serves the worker API on a store of its own, and many at
// once on a small runner would stretch the waits the tests time.
var parallelTests = make(chan struct{}, 4)

// parallel has the test t run in parallel with the package's other tests
// that do, as many at a time as parallelTests allows. A test that bounds
// how long the server takes, or that changes what the worker API's
// versions have (apiversion), doesn't call it.
func parallel(t *testing.T) {
	t.Helper()
	t.Parallel()
	parallelTests <- struct{}{}
	t.Cleanup(func() { <-parallelTests })
}

// storeTemplate is the store that each test's new store is a copy of
// (coordtest.StoreTemplate).
var storeTemplate = coordtest.StoreTemplate{Make: func(ctx context.Context, path string) error {
	s, err := store.Open(ctx, path, nil)
	if err != nil {
		return err
	}
	return s.Close()
}}

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

// openStore opens a new store, a copy of the store template, with opts,
// which the test closes, and returns it and its database's path.
func openStore(t testing.TB, opts *store.Options) (*store.Store, string) {
	t.Helper()
	path := filepath.Join(coordtest.PrivateDir(t), "meridian.db")
	storeTemplate.Copy(t, path)
	s, err := store.Open(t.Context(), path, opts)
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

// fakeReadier gives each run's attempts the job, the run's values and the
// needs' results a test set for the run.
type fakeReadier struct {
	mu   sync.Mutex
	runs map[string]*workerapisrv.Ready
	// needs is the oldest version of the worker API each attempt needs:
	// 1 for 0.
	needs int
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

func (f *fakeReadier) Needs(context.Context, *store.Run, *store.Attempt) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return max(f.needs, 1), nil
}

// rigChildren are a rig's workerapisrv.Children: each child a step asks
// for is a run of the rig's store, linked to the step's try, as the
// coordinator's are, and its stops are kept, by the attempt that asked.
type rigChildren struct {
	r *rig

	mu    sync.Mutex
	stops map[string][]workerapi.ChildStop
}

func (k *rigChildren) Child(ctx context.Context, run *store.Run, a *store.Attempt, req *workerapi.ChildRequest) (*workerapi.Child, error) {
	ready, err := k.r.readier.get(run.ID)
	if err != nil {
		return nil, err
	}
	if req.Step >= len(ready.Job.Steps) {
		return nil, fmt.Errorf("%w: step %d", workerapisrv.ErrNotChild, req.Step)
	}
	c := created(k.r.t)
	var id string
	err = k.r.store.Tx(ctx, func(tx *store.Tx) error {
		var err error
		if id, err = tx.Child(a.ID, req.Step, req.Try); err == nil || !errors.Is(err, store.ErrNotFound) {
			return err
		}
		now, err := tx.Attempt(a.ID)
		if err != nil {
			return err
		}
		if !now.Holds {
			return store.ErrFenced
		}
		if id, err = tx.NewRunID(); err != nil {
			return err
		}
		return tx.InsertRun(store.NewRun{
			ID: id, Plan: c.Plan, DefaultGroup: "local", RunDir: "/runs/" + id, WorkflowID: fakeJobID(),
			Parent: &store.Link{Run: run.ID, Job: a.Job, Attempt: a.ID, Step: req.Step, Try: req.Try, Root: run.ID, Depth: 1},
		})
	})
	if err != nil {
		return nil, err
	}
	return &workerapi.Child{Run: id}, nil
}

// StopChild keeps the stop, by the attempt whose step's child id is.
func (k *rigChildren) StopChild(ctx context.Context, id string, reason plan.Reason) error {
	var r *store.Run
	if err := k.r.store.View(ctx, func(tx *store.Tx) error {
		var err error
		r, err = tx.Run(id)
		return err
	}); err != nil {
		return err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.stops == nil {
		k.stops = map[string][]workerapi.ChildStop{}
	}
	k.stops[r.Parent.Attempt] = append(k.stops[r.Parent.Attempt], workerapi.ChildStop{Step: r.Parent.Step, Try: r.Parent.Try, Reason: reason})
	return nil
}

// end ends the child of the step's try of the attempt, with outcome, and
// tells the rig's servers.
func (k *rigChildren) end(attempt string, step, try int, outcome plan.Outcome) {
	var id string
	k.r.inTx(func(tx *store.Tx) error {
		var err error
		if id, err = tx.Child(attempt, step, try); err != nil {
			return err
		}
		return tx.EndRun(id, outcome, "")
	})
	k.r.mu.Lock()
	servers := slices.Clone(k.r.servers)
	k.r.mu.Unlock()
	for _, s := range servers {
		s.Ended(id)
	}
}

func (f *fakeReadier) Steps(_ context.Context, run *store.Run, _ *store.Attempt) (int, error) {
	r, err := f.get(run.ID)
	if err != nil {
		return 0, err
	}
	return len(r.Job.Steps), nil
}

// fakeActions performs the action of a step of a job the readier gives
// whose uses: names a built-in action, as answer answers it, or with
// success when answer is nil, in the transaction Commit gives it; and
// counts the calls of answer, which it makes in that transaction. Once
// uncommitted is set, it answers success without Commit, as Actions
// mustn't.
type fakeActions struct {
	readier     *fakeReadier
	mu          sync.Mutex
	answer      func(tx *store.Tx, call *workerapisrv.ActionCall) (workerapi.ActionAnswer, error)
	acted       int
	uncommitted bool
	// cancelled are what each call's Cancelled said, before its Commit.
	cancelled []bool
	// treeFault is how Tree fails: before it writes, after its first
	// file, or not at all, for "".
	treeFault string
}

// The ways fakeActions.Tree fails.
const (
	treeFailsFirst = "first"
	treeFailsAfter = "after"
)

// errTree is what fakeActions.Tree fails with.
var errTree = errors.New("the tree couldn't be read")

// Tree writes a tar stream of one file, README, that holds the step's
// index, and of a directory, docs, as treeFault says.
func (f *fakeActions) Tree(_ context.Context, call *workerapisrv.ActionCall, w io.Writer) error {
	f.mu.Lock()
	fault := f.treeFault
	f.mu.Unlock()
	if fault == treeFailsFirst {
		return errTree
	}
	tw := tar.NewWriter(w)
	content := strconv.Itoa(call.Request.Step)
	if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: "README", Mode: 0o644, Size: int64(len(content))}); err != nil {
		return err
	}
	if _, err := io.WriteString(tw, content); err != nil {
		return err
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if fault == treeFailsAfter {
		return errTree
	}
	if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: "docs/", Mode: 0o755}); err != nil {
		return err
	}
	return tw.Close()
}

// setTreeFault makes f's Tree fail as fault says.
func (f *fakeActions) setTreeFault(fault string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.treeFault = fault
}

func (f *fakeActions) Act(ctx context.Context, call *workerapisrv.ActionCall) (workerapi.ActionAnswer, error) {
	ready, err := f.readier.get(call.Run.ID)
	if err != nil {
		return workerapi.ActionAnswer{}, err
	}
	steps := ready.Job.Steps
	if call.Request.Step >= len(steps) || !strings.HasPrefix(steps[call.Request.Step].Uses, "parallelworks/") {
		return workerapi.ActionAnswer{}, fmt.Errorf("%w: step %d", workerapisrv.ErrNotAction, call.Request.Step)
	}
	cancelled, err := call.Cancelled(ctx, &steps[call.Request.Step])
	if err != nil {
		return workerapi.ActionAnswer{}, err
	}
	f.mu.Lock()
	answer, uncommitted := f.answer, f.uncommitted
	f.cancelled = append(f.cancelled, cancelled)
	f.mu.Unlock()
	if uncommitted {
		return workerapi.ActionAnswer{Outcome: plan.OutcomeSuccess}, nil
	}
	return call.Commit(ctx, &steps[call.Request.Step], func(tx *store.Tx) (workerapi.ActionAnswer, error) {
		f.mu.Lock()
		f.acted++
		f.mu.Unlock()
		if answer != nil {
			return answer(tx, call)
		}
		return workerapi.ActionAnswer{Outcome: plan.OutcomeSuccess}, nil
	})
}

// setAnswer makes f answer as answer does.
func (f *fakeActions) setAnswer(answer func(tx *store.Tx, call *workerapisrv.ActionCall) (workerapi.ActionAnswer, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.answer = answer
}

// setUncommitted makes f answer without Commit, or not.
func (f *fakeActions) setUncommitted(uncommitted bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.uncommitted = uncommitted
}

// sawCancelled returns what each call's Cancelled said, in order.
func (f *fakeActions) sawCancelled() []bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.cancelled)
}

// times returns how many times f has performed an action.
func (f *fakeActions) times() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.acted
}

// A rig is a store, and a server of it on a socket of its own.
type rig struct {
	t        testing.TB
	store    *store.Store
	path     string
	sealer   *seal.Sealer
	readier  *fakeReadier
	actions  *fakeActions
	children *rigChildren
	logs     *runlogtest.Logs
	server   *workerapisrv.Server
	socket   string

	mu    sync.Mutex
	woken []string
	// servers are the rig's servers, the first and each other replica.
	servers []*workerapisrv.Server
}

// rigOptions change the rig's server.
type rigOptions struct {
	peers workerapisrv.Peers
	// older makes the server a replica of an older release, which reads
	// only the plan versions before this one's.
	older    bool
	hold     time.Duration
	maxConns int
	// configure, if it is set, changes the server before it serves.
	configure func(*workerapisrv.Server)
	// store, if it is set, is how the rig's store opens.
	store *store.Options
}

func newRig(t testing.TB, opts rigOptions) *rig {
	t.Helper()
	// A change to attempts' controls answers the polls the rig's first
	// server holds of them, as a coordinator has its own server's answered
	// (store.Options.OnControlsChanged); another replica's read them again
	// at their recheck, as another process's do.
	var storeOpts store.Options
	if opts.store != nil {
		storeOpts = *opts.store
	}
	var first atomic.Pointer[workerapisrv.Server]
	storeOpts.OnControlsChanged = func(_ context.Context, ids []string) {
		if srv := first.Load(); srv != nil {
			srv.Notify(ids...)
		}
	}
	s, path := openStore(t, &storeOpts)
	sealer, err := seal.New(coordtest.NewKeys(t), s.ID())
	if err != nil {
		t.Fatal(err)
	}
	readier := &fakeReadier{}
	r := &rig{t: t, store: s, path: path, sealer: sealer, readier: readier, actions: &fakeActions{readier: readier}, logs: &runlogtest.Logs{}}
	r.children = &rigChildren{r: r}
	r.server = r.serve(opts)
	first.Store(r.server)
	return r
}

// serve starts another server of the rig's store, as another replica, and
// returns it, with its socket in r.socket if it is the first.
func (r *rig) serve(opts rigOptions) *workerapisrv.Server {
	r.t.Helper()
	srv, err := workerapisrv.New(workerapisrv.Options{
		Store: r.store, Logs: r.logs, Sealer: r.sealer, Readier: r.readier, Actions: r.actions, Children: r.children,
		Peers: opts.peers, MaxConnections: opts.maxConns,
		Wake: func(_ context.Context, _ *store.Tx, run, job string) (func(), error) {
			return func() {
				r.mu.Lock()
				defer r.mu.Unlock()
				r.woken = append(r.woken, run+"/"+job)
			}, nil
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
	if opts.configure != nil {
		opts.configure(srv)
	}
	r.mu.Lock()
	r.servers = append(r.servers, srv)
	r.mu.Unlock()
	socket := listen(r.t, srv)
	if r.socket == "" {
		r.socket = socket
	}
	return srv
}

// listen serves srv on a new Unix socket until the test ends, and returns
// the socket's path.
func listen(t testing.TB, srv *workerapisrv.Server) string {
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

// newestRun stores a run of testWorkflow as newRun does, but of this
// release's newest plan version (plan.Version), which a replica of an
// older release doesn't read, and returns its ID.
func (r *rig) newestRun() string {
	r.t.Helper()
	c := created(r.t)
	c.Plan.Version, c.Record.Version = plan.Version, plan.Version
	return r.newRunOf(c)
}

// newRun stores a run of testWorkflow, with its jobs and a secret record,
// and returns its ID.
func (r *rig) newRun(rec *plan.Record) string {
	r.t.Helper()
	c := created(r.t)
	if rec != nil {
		c.Record = *rec
	}
	return r.newRunOf(c)
}

// newRunOf stores a run of what Create gave, c, with its jobs and its
// secret record, and returns its ID.
func (r *rig) newRunOf(c *compile.Created) string {
	r.t.Helper()
	rec := &c.Record
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
	// unplaced leaves the attempt without a placement recorded, and
	// digest "" is conformanceDigest.
	unplaced bool
	// private places the attempt where its job runs is private: its
	// group, member and backend sealed, as the coordinator records them.
	private bool
	// memberKey places it on a member whose workers reach the server
	// through a listener on it (ServeMember), and runDir is the run's
	// directory there.
	memberKey, runDir string
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
		if err != nil || p.unplaced {
			return err
		}
		if p.digest == "" {
			p.digest = conformanceDigest
		}
		if p.private {
			if _, err := tx.SQL().ExecContext(r.t.Context(), "UPDATE meridian_attempts SET private_placement = x'01' WHERE id = ?", a.ID); err != nil {
				return err
			}
			return tx.RecordPlacement(a.ID, store.Placement{WorkerDigest: p.digest, MemberKey: p.memberKey})
		}
		return tx.RecordPlacement(a.ID, store.Placement{
			Group: p.group, Member: p.member, Backend: "local", ID: "1:2", WorkerDigest: p.digest, MemberKey: p.memberKey, RunDir: p.runDir,
		})
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

// askControl asks c of the attempt id's worker in place of what it asked,
// at its control's next version.
func askControl(tx *store.Tx, id string, c workerapi.Control) error {
	_, err := tx.UpdateControl(id, func(cur *workerapi.Control) bool {
		*cur = c
		return true
	})
	return err
}

// conformanceDigest is the digest of the worker Conformance plays.
var conformanceDigest = "sha256:" + strings.Repeat("0", 64)

// A client talks to a server on its socket, as a worker does.
type client struct {
	t    testing.TB
	http *http.Client
}

func newClient(t testing.TB, socket string) *client {
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
func claimRequest(t testing.TB, credential, digest string) []byte {
	t.Helper()
	return claimRequestAt(t, credential, digest, workerapi.Version)
}

// claimRequestAt returns claimRequest's claim, of a worker that speaks
// apiVersion of the worker API.
func claimRequestAt(t testing.TB, credential, digest string, apiVersion int) []byte {
	t.Helper()
	data, err := workerapi.Marshal(workerapi.ClaimRequest{Credential: credential, APIVersion: apiVersion, PlanVersion: plan.Version, Worker: workerapi.Worker{Version: "v0", Digest: digest}})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
