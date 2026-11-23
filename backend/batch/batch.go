// Package batch is what the slurm and pbs backends share: a
// backend.Driver over a batch scheduler's commands on the login node that
// Meridian runs on (Scheduler). For each attempt it submits a job whose
// script runs meridian batch, from a directory of its own in the home the
// cluster's nodes share, and it learns how each job went from the
// scheduler and from the records meridian batch keeps there.
//
// # Submissions
//
// An attempt's submissions are Driver.Dir's <attempt>/<n>/, n counting
// them from 1, each made with mkdir, which is atomic on NFS too. Each
// holds what Place writes before it submits (spec.json, and the bootstrap
// token in token, which only the user can read), the job's ID once the
// scheduler has given it (job), whether Cancel withdrew it (withdrawn),
// what meridian batch records on the compute node (started.json,
// term.json, exit.json), and the job's output (out). The token is never in
// the job's script or its environment, which the scheduler keeps.
//
// # Placements
//
// A placement's ID is <kind>:<scope tag>/<id>: the kind job for a batch
// job; the tag a keyed hash of the cluster the job is on, whose IDs are
// the same from each of its login nodes; and the scheduler's job ID. Later
// kinds, as a worker on the login node itself, route by the kind.
//
// # Asking the scheduler
//
// The coordinator checks every unclaimed or lapsed attempt each heartbeat
// period. So Check reads a snapshot of the cluster at most 15 seconds old
// (Driver.poll), which one command, made by one caller at a time, gives
// for every job the driver watches, in chunks of 500; then the records of
// those the scheduler no longer lists; then the scheduler's accounting,
// where the cluster keeps it, for those whose records don't say how they
// ended. A job that is gone, whose records don't say how it ended, is
// Unknown for two minutes, the time NFS can take to show a new file.
package batch

import (
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/problems"
)

// DefaultMarginSeconds is a scheduler member's margin unless it sets one:
// time to start the worker, run cleanups and report, before the
// allocation ends.
const DefaultMarginSeconds = 300

// MinKeyLen is the shortest Driver.Key, in bytes.
const MinKeyLen = 32

// A Scheduler is a batch scheduler's commands on one cluster's login node.
// An error that is a problem says what is wrong with the cluster or the
// member; any other error is one that passes, as a busy controller, and
// is tried again.
type Scheduler interface {
	// Name is the backend's name: slurm.
	Name() string
	// Settings decodes a member's settings, refusing a key or a value it
	// doesn't take (problems.MemberSettingInvalid).
	Settings(member string, raw jsontext.Value) (*Settings, error)
	// Info says which release and cluster the commands reach. A command
	// that is missing, isn't safe to run, or is of a release it doesn't
	// support is a problem.
	Info(ctx context.Context) (*Info, error)
	// Partition returns the partition named, or the cluster's default for
	// "", and false where there is none.
	Partition(ctx context.Context, name string) (Partition, bool, error)
	// Submit submits job, and returns its ID. A refusal that submitting
	// again won't change is a *Rejected.
	Submit(ctx context.Context, job *Job) (string, error)
	// Statuses says what the scheduler lists of the jobs ids, on a cluster
	// that keeps accounting or not: one it no longer lists is missing.
	Statuses(ctx context.Context, ids []string, accounting bool) (map[string]JobStatus, error)
	// Accounting says what the cluster's accounting has of the jobs ids,
	// where Info says it keeps it: one it doesn't have is missing.
	Accounting(ctx context.Context, ids []string) (map[string]JobStatus, error)
	// Named returns the IDs of the user's jobs of each name, as the queue
	// lists them, and as accounting has them where accounting is on.
	Named(ctx context.Context, names []string, accounting bool) (map[string][]string, error)
	// Cancel cancels a job. One that has ended, or that the scheduler no
	// longer knows, is no error.
	Cancel(ctx context.Context, id string) error
	// Allocation says which job the process runs in, and when its
	// allocation ends, as the job's environment, getenv, says or the
	// scheduler does: End is zero where neither says.
	Allocation(ctx context.Context, getenv func(string) string) (Allocation, error)
}

// Settings are a member's settings, as its scheduler decodes them.
type Settings struct {
	// Host is the login node, an ssh alias or a host name, which a later
	// release reaches over SSH; "" for this machine.
	Host string
	// Partition is the partition, or a PBS queue, or "" for the cluster's
	// default.
	Partition string
	// Account, QOS and Reservation are submitted with, where set.
	Account, QOS, Reservation string
	// Nodes is how many nodes a job asks for, or 0 for the scheduler's
	// default.
	Nodes int
	// Exclusive asks for whole nodes.
	Exclusive bool
	// GPUType is the type of GPU a job's gpu resource asks for, or "".
	GPUType string
	// WalltimeSeconds fixes the walltime asked for, or is 0.
	WalltimeSeconds int64
}

// Info is what a cluster's scheduler says of itself.
type Info struct {
	// Version is its release, as 23.11.4.
	Version string
	// Cluster names the cluster, whose job IDs are its own.
	Cluster string
	// Accounting says whether the cluster keeps accounting, which knows
	// jobs that the queue has forgotten.
	Accounting bool
}

// A Partition is a partition, or a PBS queue.
type Partition struct {
	Name string
	// State is the scheduler's word for it, and Up whether it takes jobs.
	State string
	Up    bool
	// MaxSeconds and DefaultSeconds are its longest walltime and the one a
	// job that asks for none gets, or 0 for none.
	MaxSeconds, DefaultSeconds int64
}

// A Job is what the driver submits.
type Job struct {
	// Name is meridian-<attempt>-<n>, by which Find finds it.
	Name string
	// Dir is its submission's directory: its working directory, where its
	// output goes.
	Dir string
	// Script is its batch script, which runs meridian batch.
	Script []byte
	// Environ is the environment it is submitted with.
	Environ []string
	// WalltimeSeconds is the walltime it asks for, or 0 for the
	// partition's default.
	WalltimeSeconds int64
	// Resources are what its job's resources: asks for.
	Resources backend.Resources
	Settings  *Settings
	// Cluster is the cluster the scheduler's commands reach, as Info said,
	// to which the job goes, and to no other.
	Cluster string
}

// A Rejected is a job the scheduler refused, for a cause that submitting
// it again won't change. Said is what it said.
type Rejected struct{ Said string }

func (r *Rejected) Error() string { return "batch: the scheduler refused the job: " + r.Said }

// A JobStatus is what a scheduler says of a job.
type JobStatus struct {
	// State is backend.Queued, Running or Dead.
	State backend.State
	// End is why a Dead job ended: backend.EndExited for one that exited,
	// or that was cancelled (Cancelled).
	End       backend.End
	Cancelled bool
	// Reason is why a Queued job waits, as the scheduler says it, and
	// Stuck whether it is one that won't change by itself.
	Reason string
	Stuck  bool
}

// An Allocation is the job a process runs in: when it started and when
// it ends, each zero where the scheduler doesn't say.
type Allocation struct {
	Job        string
	Start, End time.Time
}

// A Driver is a backend.Driver over a Scheduler, for members whose login
// node is this machine.
type Driver struct {
	Scheduler Scheduler
	// Dir is where submissions are kept: in the home the cluster's nodes
	// share, as ${XDG_STATE_HOME:-$HOME/.local/state}/meridian/batch.
	Dir string
	// Binary returns the meridian binary a job runs, at a path the compute
	// nodes see, once it has checked it, and its digest.
	Binary func() (path, digest string, err error)
	// Environ is the environment jobs are submitted with, but GODEBUG,
	// every MERIDIAN_ variable and those Withhold names: os.Environ() for
	// nil. The scheduler keeps a job's environment, and may archive it.
	Environ  []string
	Withhold []string
	// Key keys the hash that names a cluster in a placement's ID: at least
	// MinKeyLen bytes, the same for every replica, and kept.
	Key []byte

	once    sync.Once
	timing  timing
	poll    *poller
	infos   cached[*Info]
	partsMu sync.Mutex
	parts   map[string]*cached[partitionFact]
	submits chan struct{}
	swept   sync.Once
}

var _ backend.Driver = (*Driver)(nil)

// timing is how long the driver waits for what.
type timing struct {
	// poll is the oldest snapshot Check reads (the package's Asking the
	// scheduler).
	poll time.Duration
	// grace is how long a job that is gone, whose records don't say how
	// it ended, is Unknown: NFS's attribute cache holds a directory up to
	// acdirmax, 60 seconds by default, besides the time to write.
	grace time.Duration
	// stuck is how long a job pends for a reason that won't clear before
	// it is withdrawn: a scheduler can give one for a while of a job it
	// then runs.
	stuck time.Duration
	// facts is how long Info and partitions are kept, and failed how long
	// a failure to get them is.
	facts, failed time.Duration
	// idle is how long a job nobody asks about stays watched.
	idle time.Duration
	// refresh bounds one snapshot's commands.
	refresh time.Duration
	// now is the clock grace is counted by.
	now func() time.Time
}

var defaultTiming = timing{
	poll: 15 * time.Second, grace: 2 * time.Minute, stuck: 2 * time.Minute, facts: time.Minute, failed: 15 * time.Second,
	idle: 10 * time.Minute, refresh: 2 * time.Minute, now: time.Now,
}

// chunk is the most job IDs one status command names.
const chunk = 500

// maxSubmits is how many submissions to a cluster run at once.
const maxSubmits = 4

func (d *Driver) init() {
	d.once.Do(func() {
		if d.timing.poll == 0 {
			d.timing = defaultTiming
		}
		d.poll = &poller{d: d, live: map[string]*watch{}}
		d.parts = map[string]*cached[partitionFact]{}
		d.submits = make(chan struct{}, maxSubmits)
	})
}

// attemptPattern is an attempt's ID, which names its directory and its
// jobs.
var attemptPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// kindJob is the kind of a placement that is a batch job.
const kindJob = "job"

// placementID returns the ID of the job id on the cluster tagged tag.
func placementID(tag, id string) string { return kindJob + ":" + tag + "/" + id }

// parseID returns the cluster's tag and the job's ID of a placement's ID,
// and false for one that isn't a batch job's.
func parseID(s string) (tag, id string, ok bool) {
	kind, rest, ok := strings.Cut(s, ":")
	if !ok || kind != kindJob {
		return "", "", false
	}
	tag, id, ok = strings.Cut(rest, "/")
	if !ok || tag == "" || id == "" {
		return "", "", false
	}
	return tag, id, true
}

// tag returns the keyed hash that names the cluster info describes in a
// placement's ID, so that a placement's ID says which cluster it is on
// without naming it.
func (d *Driver) tag(info *Info) (string, error) {
	if len(d.Key) < MinKeyLen {
		return "", fmt.Errorf("batch: the driver's key has %d bytes, fewer than %d", len(d.Key), MinKeyLen)
	}
	mac := hmac.New(sha256.New, d.Key)
	mac.Write([]byte("cluster\x00" + d.Scheduler.Name() + "\x00" + info.Cluster))
	return hex.EncodeToString(mac.Sum(nil)[:16]), nil
}

// A cached is what the scheduler said of something, kept for a while: its
// last good value, and a failure to get it for timing.failed, so that a
// controller that doesn't answer is asked at most that often. One caller
// at a time asks, and the others wait for its answer, none of them
// holding the lock while the command runs.
type cached[T any] struct {
	mu   sync.Mutex
	val  T
	good time.Time
	err  error
	// failed is when the last try failed, with err.
	failed time.Time
	// running is closed once the try in flight ends, or nil.
	running chan struct{}
}

// get returns the value, kept for keep, or the failure, kept for retry,
// or else what fetch, which one caller at a time runs, gives. While fetch
// runs, a caller gets the last good value at once, if there is one, rather
// than wait for a fetch that may be failing; and a failure that isn't a
// problem, as a busy controller, gives the last good value, however old.
func (c *cached[T]) get(ctx context.Context, keep, retry, bound time.Duration, fetch func(context.Context) (T, error)) (T, error) {
	c.mu.Lock()
	switch {
	case !c.good.IsZero() && time.Since(c.good) < keep:
		defer c.mu.Unlock()
		return c.val, nil
	case !c.failed.IsZero() && c.failed.After(c.good) && time.Since(c.failed) < retry:
		defer c.mu.Unlock()
		return c.result()
	}
	wait := c.running
	if wait == nil {
		wait = make(chan struct{})
		c.running = wait
		go c.fetch(ctx, bound, wait, fetch)
	}
	if !c.good.IsZero() {
		defer c.mu.Unlock()
		return c.val, nil
	}
	c.mu.Unlock()
	select {
	case <-wait:
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.result()
}

// fetch runs fetch, which a caller that stops waiting doesn't cancel, and
// keeps what it gives.
func (c *cached[T]) fetch(ctx context.Context, bound time.Duration, done chan struct{}, fetch func(context.Context) (T, error)) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bound)
	defer cancel()
	v, err := fetch(ctx)
	c.mu.Lock()
	if err == nil {
		c.val, c.good, c.err = v, time.Now(), nil
	} else {
		c.err, c.failed = err, time.Now()
	}
	c.running = nil
	c.mu.Unlock()
	close(done)
}

// result is the last try's answer: its value, or its failure, but for the
// last good value for a failure that isn't a problem. Its caller holds
// mu.
func (c *cached[T]) result() (T, error) {
	if c.err == nil || c.good.After(c.failed) {
		return c.val, nil
	}
	if _, ok := errors.AsType[*problem.Problem](c.err); !ok && !c.good.IsZero() {
		return c.val, nil
	}
	var zero T
	return zero, c.err
}

type partitionFact struct {
	p     Partition
	found bool
}

// info returns the scheduler's Info, kept for a while.
func (d *Driver) info(ctx context.Context) (*Info, error) {
	d.init()
	return d.infos.get(ctx, d.timing.facts, d.timing.failed, d.timing.refresh, d.Scheduler.Info)
}

// partition returns a partition, kept for a while.
func (d *Driver) partition(ctx context.Context, name string) (Partition, bool, error) {
	d.init()
	d.partsMu.Lock()
	c := d.parts[name]
	if c == nil {
		c = new(cached[partitionFact])
		d.parts[name] = c
	}
	d.partsMu.Unlock()
	f, err := c.get(ctx, d.timing.facts, d.timing.failed, d.timing.refresh, func(ctx context.Context) (partitionFact, error) {
		p, found, err := d.Scheduler.Partition(ctx, name)
		return partitionFact{p, found}, err
	})
	return f.p, f.found, err
}

// settings decodes m's settings, and returns why m can't run a job, as a
// problem: a host, which only a later release reaches, or isolation other
// than the host's.
func (d *Driver) settings(m *backend.Member) (*Settings, error) {
	s, err := d.Scheduler.Settings(m.Name, m.Settings)
	if err != nil {
		return nil, err
	}
	if s.Host != "" {
		return nil, problems.BackendNotBuilt.Problem(problems.Params{"member": m.Name, "backend": m.Backend})
	}
	// A scheduler's members run jobs on the host, as the user, by default.
	if m.Isolation != "" && m.Isolation != "host" {
		return nil, problems.IsolationNotBuilt.Problem(problems.Params{"member": m.Name, "isolation": m.Isolation, "has_isolation": true})
	}
	return s, nil
}

// margin is m's margin.
func margin(m *backend.Member) int64 {
	if m.MarginSeconds > 0 {
		return m.MarginSeconds
	}
	return DefaultMarginSeconds
}

// limits returns m's Fit but for its availability: its longest walltime,
// its margin and its default timeout, from m and from its partition p; or
// the problem with them, a member that allows more than its partition.
func limits(m *backend.Member, s *Settings, p Partition) (backend.Fit, *problem.Problem) {
	fit := backend.Fit{MaxWalltimeSeconds: p.MaxSeconds, MarginSeconds: margin(m), DefaultTimeoutSeconds: m.DefaultTimeoutSeconds}
	over := func(key string) *problem.Problem {
		return problems.MemberSettingInvalid.Problem(problems.Params{"member": m.Name, "key": key, "reason": "partition"})
	}
	if m.MaxWalltimeSeconds > 0 {
		if p.MaxSeconds > 0 && m.MaxWalltimeSeconds > p.MaxSeconds {
			return fit, over("max-walltime")
		}
		fit.MaxWalltimeSeconds = m.MaxWalltimeSeconds
	}
	if s.WalltimeSeconds > 0 && fit.MaxWalltimeSeconds > 0 && s.WalltimeSeconds > fit.MaxWalltimeSeconds {
		return fit, over("walltime")
	}
	// A job without a timeout: gets the allocation less the margin.
	if alloc := cmp.Or(s.WalltimeSeconds, p.DefaultSeconds); fit.DefaultTimeoutSeconds == 0 && alloc > fit.MarginSeconds {
		fit.DefaultTimeoutSeconds = alloc - fit.MarginSeconds
	}
	return fit, nil
}

// walltime is what a job of m asks for, for a timeout of timeout seconds:
// the member's fixed walltime, or the timeout and the margin, or 0 for
// the partition's default.
func walltime(m *backend.Member, s *Settings, timeout int64) int64 {
	switch {
	case s.WalltimeSeconds > 0:
		return s.WalltimeSeconds
	case timeout > 0:
		return timeout + margin(m)
	}
	return 0
}

// member returns m's settings and its partition, or the problem that
// keeps m from running a job.
func (d *Driver) member(ctx context.Context, m *backend.Member) (*Settings, Partition, error) {
	s, err := d.settings(m)
	if err != nil {
		return nil, Partition{}, err
	}
	if _, err := d.info(ctx); err != nil {
		return nil, Partition{}, err
	}
	p, found, err := d.partition(ctx, s.Partition)
	switch {
	case err != nil:
		return nil, Partition{}, err
	case !found && s.Partition == "":
		return nil, Partition{}, problems.MemberSettingInvalid.Problem(problems.Params{"member": m.Name, "key": "partition", "reason": "required"})
	case !found:
		return nil, Partition{}, problems.SchedulerPartitionUnknown.Problem(problems.Params{"member": m.Name, "partition": s.Partition})
	case !p.Up:
		return nil, Partition{}, problems.SchedulerPartitionDown.Problem(problems.Params{"member": m.Name, "partition": p.Name, "state": p.State})
	}
	return s, p, nil
}

// Probe says whether m can run a job that asks for demand: whether the
// job calls no workflow, whose run it would hold its allocation for; and
// whether its scheduler's commands are there, safe to run and of a release
// the driver supports, its partition is there and up, and its walltimes
// fit the partition's. A job that fits would queue: the scheduler decides
// when it starts. Demand.Parked is ignored: an allocation can't lend what
// it holds.
func (d *Driver) Probe(ctx context.Context, m *backend.Member, demand backend.Demand) (backend.Fit, error) {
	d.init()
	if demand.CallsWorkflow {
		return backend.Fit{Problem: problems.SchedulerCallerRefused.Problem(problems.Params{"member": m.Name})}, nil
	}
	s, p, err := d.member(ctx, m)
	if err != nil {
		return fitOf(err)
	}
	fit, prob := limits(m, s, p)
	if prob != nil {
		return backend.Fit{Problem: prob}, nil //nolint:nilerr // what is wrong with the member is the Fit's, not the call's
	}
	for _, r := range []struct {
		name        string
		asks, offer int64
	}{
		{"cpu", demand.Resources.MilliCPU, m.Resources.MilliCPU},
		{"memory", demand.Resources.Memory, m.Resources.Memory},
		{"gpu", int64(demand.Resources.GPU), int64(m.Resources.GPU)},
	} {
		// What a member doesn't declare, its scheduler decides.
		if r.offer > 0 && r.asks > r.offer {
			fit.Problem = problems.ResourcesExceedMember.Problem(problems.Params{"member": m.Name, "resource": r.name, "asks": r.asks, "offers": r.offer})
			return fit, nil
		}
	}
	if err := ensureDir(d.Dir); err != nil {
		return fitOf(err)
	}
	if _, _, err := d.Binary(); err != nil {
		return fitOf(err)
	}
	fit.Availability = backend.WouldQueue
	return fit, nil
}

// fitOf returns the Fit of a member that err keeps from running a job, for
// a problem, or err.
func fitOf(err error) (backend.Fit, error) {
	if p, ok := errors.AsType[*problem.Problem](err); ok {
		return backend.Fit{Problem: p}, nil
	}
	return backend.Fit{}, err
}

// Connect returns coordinator itself, for compute nodes that reach it
// there, as this machine's own do. A listener on the login node, and the
// address file workers find it by, are a later release's.
func (d *Driver) Connect(_ context.Context, _ *backend.Member, coordinator backend.Endpoint) (backend.Endpoint, io.Closer, error) {
	return coordinator, nopCloser{}, nil
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

// Place submits a job that runs meridian batch for req's attempt, in a new
// submission's directory, which holds the attempt's spec and its token,
// and returns once the scheduler has the job.
func (d *Driver) Place(ctx context.Context, m *backend.Member, req backend.PlaceRequest) (backend.Placement, error) {
	d.init()
	if !attemptPattern.MatchString(req.Attempt) {
		return backend.Placement{}, fmt.Errorf("batch: an attempt's ID that can't name a job: %q", req.Attempt)
	}
	s, _, err := d.member(ctx, m)
	if err != nil {
		return backend.Placement{}, err
	}
	info, err := d.info(ctx)
	if err != nil {
		return backend.Placement{}, err
	}
	tag, err := d.tag(info)
	if err != nil {
		return backend.Placement{}, err
	}
	binary, digest, err := d.Binary()
	if err != nil {
		return backend.Placement{}, err
	}
	d.sweep()
	sub, n, err := d.newSubmission(req.Attempt)
	if err != nil {
		return backend.Placement{}, err
	}
	spec := &Spec{
		Attempt: req.Attempt, Submission: n, Scheduler: d.Scheduler.Name(), Digest: digest,
		Coordinator:   Coordinator{Address: req.Coordinator.Address, KeyHash: req.Coordinator.KeyHash},
		MarginSeconds: margin(m), WalltimeSeconds: walltime(m, s, req.TimeoutSeconds), RunDir: req.RunDir,
	}
	if err := writeJSON(sub, SpecFile, spec); err != nil {
		return backend.Placement{}, err
	}
	if err := writeFile(sub, TokenFile, []byte(req.BootstrapToken)); err != nil {
		return backend.Placement{}, err
	}
	job := &Job{
		Name: jobName(req.Attempt, n), Dir: sub, Script: Script(binary, sub), Environ: d.environ(),
		WalltimeSeconds: spec.WalltimeSeconds, Resources: req.Resources, Settings: s, Cluster: info.Cluster,
	}
	id, err := d.submit(ctx, job)
	if r, ok := errors.AsType[*Rejected](err); ok {
		// No job will read the token: the submission goes, with it.
		_ = d.removeSubmission(req.Attempt, n)
		return backend.Placement{}, problems.SchedulerRejected.Problem(problems.Params{"member": m.Name, "reason": r.Said})
	}
	if err != nil {
		// The job may be there all the same, as one whose command timed
		// out after the scheduler took it: Find finds it by its name.
		return backend.Placement{}, err
	}
	if err := writeFile(sub, JobFile, []byte(id+"\n")); err != nil {
		return backend.Placement{}, err
	}
	d.poll.watch(id, sub)
	return backend.Placement{Attempt: req.Attempt, ID: placementID(tag, id), WorkerDigest: digest, RunDir: req.RunDir}, nil
}

// submit submits job, maxSubmits at a time.
func (d *Driver) submit(ctx context.Context, job *Job) (string, error) {
	select {
	case d.submits <- struct{}{}:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	defer func() { <-d.submits }()
	return d.Scheduler.Submit(ctx, job)
}

// jobName is the name of the attempt's nth job.
func jobName(attempt string, n int) string {
	return "meridian-" + attempt + "-" + strconv.Itoa(n)
}

// Script returns the batch script that runs meridian batch, the binary at
// path, for the submission dir: two lines, the paths quoted for the shell.
// Every option of the job is an argument of the submit command, never a
// line here.
func Script(path, dir string) []byte {
	return []byte("#!/bin/sh\nexec " + shellQuote(path) + " batch " + shellQuote(dir) + "\n")
}

// shellQuote quotes s for sh: in single quotes, each one of its own
// ending them, escaped, and starting them again.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// environ returns the environment jobs are submitted with.
func (d *Driver) environ() []string {
	return jobEnviron(d.Environ, d.Withhold)
}

// Find returns the attempt's placement: its newest submission whose job
// is known, by its records, by the queue, or by accounting. Older ones
// whose jobs are there, as one submitted again after a coordinator
// stopped between submitting it and recording its ID, are cancelled.
func (d *Driver) Find(ctx context.Context, m *backend.Member, attempt string) (backend.Placement, bool, error) {
	d.init()
	if !attemptPattern.MatchString(attempt) {
		return backend.Placement{}, false, nil
	}
	if _, err := d.settings(m); err != nil {
		return backend.Placement{}, false, err
	}
	info, err := d.info(ctx)
	if err != nil {
		return backend.Placement{}, false, err
	}
	tag, err := d.tag(info)
	if err != nil {
		return backend.Placement{}, false, err
	}
	subs, err := d.submissions(attempt)
	if err != nil {
		return backend.Placement{}, false, err
	}
	type found struct {
		n   int
		sub string
		ids []string
	}
	var known []found
	unnamed := map[string]found{}
	for _, n := range subs {
		sub := d.submissionDir(attempt, n)
		if id := recordedJob(sub); id != "" {
			known = append(known, found{n, sub, []string{id}})
		} else {
			unnamed[jobName(attempt, n)] = found{n: n, sub: sub}
		}
	}
	if len(unnamed) > 0 {
		names := make([]string, 0, len(unnamed))
		for name := range unnamed {
			names = append(names, name)
		}
		byName, err := d.Scheduler.Named(ctx, names, info.Accounting)
		if err != nil {
			return backend.Placement{}, false, err
		}
		for name, f := range unnamed {
			if ids := byName[name]; len(ids) > 0 {
				f.ids = ids
				known = append(known, f)
			}
		}
	}
	if len(known) == 0 {
		return backend.Placement{}, false, nil
	}
	newest := known[0]
	for _, f := range known[1:] {
		if f.n > newest.n {
			newest = f
		}
	}
	// The newest job of the newest submission is the placement; any other
	// holds a token that is no longer the attempt's.
	id := newestID(newest.ids)
	for _, f := range known {
		for _, other := range f.ids {
			if f.n == newest.n && other == id {
				continue
			}
			if err := d.withdraw(ctx, f.sub, other, ""); err != nil {
				return backend.Placement{}, false, err
			}
		}
	}
	if recordedJob(newest.sub) == "" {
		if err := writeFile(newest.sub, JobFile, []byte(id+"\n")); err != nil {
			return backend.Placement{}, false, err
		}
	}
	spec, err := ReadSpec(newest.sub)
	if err != nil {
		return backend.Placement{}, false, err
	}
	d.poll.watch(id, newest.sub)
	return backend.Placement{Attempt: attempt, ID: placementID(tag, id), WorkerDigest: spec.Digest, RunDir: spec.RunDir}, true, nil
}

// newestID returns the job ID of ids that the scheduler gave last: the
// longest, then the greatest, as a scheduler counts them up.
func newestID(ids []string) string {
	newest := ids[0]
	for _, id := range ids[1:] {
		if len(id) > len(newest) || len(id) == len(newest) && id > newest {
			newest = id
		}
	}
	return newest
}

// placement returns the job's ID and the directory of the submission of
// p, a placement on this driver's cluster.
func (d *Driver) placement(ctx context.Context, p backend.Placement) (id, sub string, err error) {
	tag, id, ok := parseID(p.ID)
	if !ok {
		return "", "", fmt.Errorf("batch: %q isn't the ID of a batch job's placement", p.ID)
	}
	info, err := d.info(ctx)
	if err != nil {
		return "", "", err
	}
	ours, err := d.tag(info)
	if err != nil {
		return "", "", err
	}
	if tag != ours {
		return "", "", fmt.Errorf("batch: the placement of %s is on another cluster than the one the scheduler's commands reach", p.Attempt)
	}
	// The poller keeps a job's submission once it is known, so that a
	// check doesn't look through the attempt's each time.
	if sub = d.poll.subOf(id); sub == "" {
		sub, err = d.submissionOf(p.Attempt, id)
	}
	return id, sub, err
}

// Check says what has become of a placement's job: as the scheduler says,
// with what the records say where it says the job exited; or, for a job it
// no longer lists, as accounting says, or the records. A job pending for a
// reason that can't clear, for timing.grace, is withdrawn, and Dead with
// scheduler_job_cannot_start.
func (d *Driver) Check(ctx context.Context, m *backend.Member, p backend.Placement) (backend.Status, error) {
	d.init()
	unknown := backend.Status{State: backend.Unknown}
	if _, err := d.settings(m); err != nil {
		return unknown, err
	}
	id, sub, err := d.placement(ctx, p)
	if err != nil {
		return unknown, err
	}
	st, listed, err := d.poll.status(ctx, id, sub)
	if err != nil {
		return unknown, err
	}
	var out backend.Status
	switch {
	case listed && st.State == backend.Unknown:
		return unknown, fmt.Errorf("batch: the scheduler says job %s is %s, which the driver doesn't know", id, Said(st.Reason))
	case listed && st.State == backend.Queued && d.poll.stuckFor(id, st.Stuck):
		if err := d.withdraw(ctx, sub, id, st.Reason); err != nil {
			return unknown, err
		}
		out = cannotStart(id, st.Reason)
	case listed && st.State != backend.Dead:
		return backend.Status{State: st.State}, nil
	case listed:
		// Records are read only of a job that has ended, or gone.
		out = d.ended(id, sub, st, readRecords(sub))
	default:
		out = d.gone(id, sub, readRecords(sub))
	}
	switch out.State {
	case backend.Dead:
		d.poll.forget(id)
	case backend.Unknown:
		return out, fmt.Errorf("batch: job %s has ended, and its records aren't there yet", id)
	}
	return out, nil
}

// termNearEnd is how close to its allocation's end a job's signal says the
// scheduler ended it at its time limit.
const termNearEnd = 60 * time.Second

// ended returns the Status of a job the scheduler says has ended, as st:
// its own reason wins, but for a job that exited, of which the records
// tell more.
func (d *Driver) ended(id, sub string, st JobStatus, r records) backend.Status {
	dead := backend.Status{State: backend.Dead, End: st.End}
	switch {
	case st.Cancelled:
		return withdrawnOr(id, r, backend.Status{State: backend.Dead, End: backend.EndExited})
	case st.End != backend.EndExited:
		return dead
	case r.exit != nil:
		return r.status(id)
	case !d.poll.graceOver(id):
		return backend.Status{State: backend.Unknown}
	case r.started == nil:
		return notStarted(id, sub)
	}
	return dead
}

// gone returns the Status of a job that neither the queue nor accounting
// lists, from its records, once they have had the time to show.
func (d *Driver) gone(id, sub string, r records) backend.Status {
	switch {
	case r.exit != nil:
		return r.status(id)
	case r.withdrawn != nil:
		return withdrawnOr(id, r, backend.Status{})
	case !d.poll.graceOver(id):
		return backend.Status{State: backend.Unknown}
	case r.started == nil:
		return notStarted(id, sub)
	case r.timedOut():
		return backend.Status{State: backend.Dead, End: backend.EndTimedOut}
	}
	return backend.Status{State: backend.Dead, Problem: problems.SchedulerJobVanished.Problem(problems.Params{"id": id})}
}

// withdrawnOr returns the Status of a job that was withdrawn, as its
// records say: by Cancel, or as one that could never start; or otherwise
// other.
func withdrawnOr(id string, r records, other backend.Status) backend.Status {
	switch {
	case r.withdrawn == nil:
		return other
	case r.withdrawn.CannotStart != "":
		return cannotStart(id, r.withdrawn.CannotStart)
	}
	return backend.Status{State: backend.Dead, End: backend.EndWithdrawn}
}

// cannotStart is the Status of the job id, withdrawn as one pending for
// reason, which can't clear by itself.
func cannotStart(id, reason string) backend.Status {
	return backend.Status{State: backend.Dead, Problem: problems.SchedulerJobCannotStart.Problem(problems.Params{"id": id, "reason": Said(reason)})}
}

// notStarted is the Status of a job that ended without running meridian
// batch, with the last line of its output.
func notStarted(id, sub string) backend.Status {
	return backend.Status{State: backend.Dead, Problem: problems.SchedulerWorkerNotStarted.Problem(problems.Params{"id": id, "said": lastLine(sub)})}
}

// withdraw cancels the job id of the submission sub, once its records say
// it was withdrawn, and why, if it could never start, so that a check of it
// after says so, and its token, which no worker may take now, is gone.
func (d *Driver) withdraw(ctx context.Context, sub, id, cannotStart string) error {
	if sub != "" {
		if err := markWithdrawn(sub, cannotStart); err != nil {
			return err
		}
		if err := removeToken(sub); err != nil {
			return err
		}
	}
	return d.Scheduler.Cancel(ctx, id)
}

// Cancel withdraws a placement: its records say so first, so that its
// job's end reads as withdrawn; the scheduler cancels it, signalling it
// first; and its token goes.
func (d *Driver) Cancel(ctx context.Context, m *backend.Member, p backend.Placement) error {
	d.init()
	if _, err := d.settings(m); err != nil {
		return err
	}
	id, sub, err := d.placement(ctx, p)
	if err != nil {
		return err
	}
	return d.withdraw(ctx, sub, id, "")
}

// Forget removes the attempt's submissions, its token among them, once its
// job has ended: the scheduler lists it as ended, or no longer lists it.
// One whose job may still run, or that the scheduler can't say of, is
// kept, and an error.
func (d *Driver) Forget(ctx context.Context, m *backend.Member, p backend.Placement) error {
	d.init()
	if _, err := d.settings(m); err != nil {
		return err
	}
	id, sub, err := d.placement(ctx, p)
	if err != nil {
		return err
	}
	st, listed, err := d.poll.status(ctx, id, sub)
	if err != nil {
		return err
	}
	if listed && st.State != backend.Dead {
		return fmt.Errorf("batch: the job of %s hasn't ended", p.Attempt)
	}
	if sub != "" {
		if err := removeToken(sub); err != nil {
			return err
		}
	}
	d.poll.forget(id)
	return d.removeAttempt(p.Attempt)
}
