// Package ssh is the ssh backend: it runs meridian worker for a job's
// attempt on another host, which it reaches over SSH as the user would
// (internal/sshconn). It puts the binary for the host's system there first
// (internal/hostbin), and then drives the local backend there through
// meridian host, over exec sessions, in the protocol backend/hostproto
// documents: the host's local backend probes, places, finds, checks,
// cancels and forgets the host's workers, as it does on the coordinator's
// own host (docs/design.md, The ssh backend).
//
// # Members
//
// A member's settings are format 1's ssh: keys. remoteHost, remoteUser,
// jumpNodeHost and jumpNodeUser say which host, and as whom, each as text,
// as format 1 reads them; the driver keeps one connection for each four,
// which is the member's identity, whatever else the settings say, as
// disconnect-timeout. A host can come from a variable or a password, so the
// driver names a member, or a host it reached, only by a keyed hash
// (Driver.Key), never in the clear, and gives meridian host none of the
// settings. A problem that names the host, the coordinator masks, as it
// masks a private placement's.
//
// # Which host
//
// A connection made again can reach another host than the last did, as a
// cluster's login nodes behind one name do, whose state is its own:
// meridian host keeps each host's placements apart, under its machine ID
// and its name. So a placement's ID holds a keyed hash of the host it was
// made on, and the driver checks, cancels or forgets it only on that host:
// on another, Check gives Unknown, with problems.SSHPlacementElsewhere,
// which the coordinator takes for a worker that runs for a while, and the
// others fail, to be tried again. Find trusts that a host has no
// placement of an attempt only if it isn't another than the one this
// driver last placed or found it on, which it keeps in memory: what it
// doesn't know of, as a placement made before the process started, it
// trusts.
package ssh

import (
	"container/list"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/backend/hostproto"
	"github.com/parallelworks/meridian/backend/ssh/internal/hostbin"
	"github.com/parallelworks/meridian/backend/ssh/internal/sshconn"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/problems"
)

// Tries is how many failures to connect to a host a job sees, its own
// tries and those for jobs that wait on the host with it, before Probe
// says the member can't run the job (problems.SSHUnreachable), as format
// 1 gives each step four tries. Each Probe makes one try at most, so that
// the tries are spread over the looks of the jobs' supervisors (reach),
// and the pool waits longer before each, as it does before it dials any
// host again that it failed to reach.
const Tries = 4

// connectBudget is how long a call of the driver's waits for a connection
// to a host it has none to: the dial goes on without it, bounded by the
// host's ConnectTimeout, and what it makes, or how it fails, is the pool's,
// for the next call. So a host that doesn't answer holds a supervisor's
// look, and the workers the coordinator's looks share, for no longer.
const connectBudget = 5 * time.Second

// MinKeyLen is the shortest Driver.Key, in bytes.
const MinKeyLen = 32

// A Driver is the ssh backend's backend.Driver.
type Driver struct {
	// Pool holds the driver's connections to hosts, which its Dialer
	// makes.
	Pool *sshconn.Pool
	// Binaries are the binaries the driver copies to hosts, for each
	// host's system.
	Binaries *hostbin.Binaries
	// Store is the ID of the coordinator's store, which, with a run's,
	// names the run's directory on a host.
	Store string
	// Key is the key of the keyed hashes, HMAC-SHA256, by which the driver
	// names a member (Name), and a host it reached, in a placement's ID:
	// at least MinKeyLen bytes, the same for every replica of the
	// coordinator, and kept, as the placements it names hosts in are.
	Key []byte

	// hello, which a test sets, changes each hello the driver gets.
	hello func(*hostproto.Hello)

	mu sync.Mutex
	// hosts are what the driver learned of the host each connection
	// reaches.
	hosts map[*sshconn.Conn]*host
	// kept are the attempts the driver placed or found, each with the
	// tag of the host it is on (Driver.tag), until it forgets it, the
	// least recently placed or found first.
	kept placedAttempts
}

var _ backend.Driver = (*Driver)(nil)

// maxPlaced is how many attempts' hosts a Driver keeps (Driver.kept):
// past it, it forgets the one it placed or found longest ago.
const maxPlaced = 4096

// placedAttempts are attempts, each with the tag of the host it is on,
// in the order they were last kept.
type placedAttempts struct {
	order *list.List
	by    map[string]*list.Element
}

// A placedAttempt is one of placedAttempts.
type placedAttempt struct{ attempt, tag string }

// keep keeps the attempt on the host tagged tag, as the last kept, and
// forgets the one kept longest ago if there are more than most.
func (pa *placedAttempts) keep(attempt, tag string, most int) {
	if pa.by == nil {
		pa.order, pa.by = list.New(), map[string]*list.Element{}
	}
	if e, ok := pa.by[attempt]; ok {
		e.Value = placedAttempt{attempt, tag}
		pa.order.MoveToBack(e)
		return
	}
	pa.by[attempt] = pa.order.PushBack(placedAttempt{attempt, tag})
	for pa.order.Len() > most {
		oldest := pa.order.Front()
		delete(pa.by, pa.order.Remove(oldest).(placedAttempt).attempt) //nolint:forcetypeassert // the list holds only placedAttempts
	}
}

// tag returns the tag of the host the attempt is on, and false for one it
// doesn't keep.
func (pa *placedAttempts) tag(attempt string) (string, bool) {
	if e, ok := pa.by[attempt]; ok {
		return e.Value.(placedAttempt).tag, true //nolint:forcetypeassert // the list holds only placedAttempts
	}
	return "", false
}

// forget forgets the attempt.
func (pa *placedAttempts) forget(attempt string) {
	if e, ok := pa.by[attempt]; ok {
		pa.order.Remove(e)
		delete(pa.by, attempt)
	}
}

// targetOf returns the host a member's settings, format 1's ssh: keys,
// name, and the logins: each as text, as format 1 reads them, null and a
// key that isn't there "". A member that names no host can't run a job
// (problems.SSHHostMissing).
func targetOf(m *backend.Member) (sshconn.Target, error) {
	var keys map[string]any
	if err := jsonv2.Unmarshal(m.Settings, &keys); err != nil {
		return sshconn.Target{}, fmt.Errorf("ssh: the member's settings aren't an ssh: mapping: %w", err)
	}
	t := sshconn.Target{
		Host: expr.Text(keys["remoteHost"]), User: expr.Text(keys["remoteUser"]),
		JumpHost: expr.Text(keys["jumpNodeHost"]), JumpUser: expr.Text(keys["jumpNodeUser"]),
	}
	if t.Host == "" {
		return sshconn.Target{}, problems.SSHHostMissing.Problem(problems.Params{"member": m.Name})
	}
	return t, nil
}

// identity is a target's identity, which the driver keys a member by:
// remoteHost|remoteUser|jumpNodeHost|jumpNodeUser. None of them holds a |,
// which sshconn refuses in a host or a user.
func identity(t sshconn.Target) string {
	return strings.Join([]string{t.Host, t.User, t.JumpHost, t.JumpUser}, "|")
}

// hash returns the keyed hash of what, of the kind kind, in hex: the first
// 128 bits of its HMAC-SHA256.
func (d *Driver) hash(kind, what string) (string, error) {
	if len(d.Key) < MinKeyLen {
		return "", fmt.Errorf("ssh: the driver's key has %d bytes, fewer than %d", len(d.Key), MinKeyLen)
	}
	mac := hmac.New(sha256.New, d.Key)
	mac.Write([]byte(kind + "\x00" + what))
	return hex.EncodeToString(mac.Sum(nil)[:16]), nil
}

// Name returns the name of the member m, which says nothing of its host or
// logins in the clear: ssh- and the keyed hash of its identity, which the
// same host and logins always give, whatever the member is called.
func (d *Driver) Name(m *backend.Member) (string, error) {
	t, err := targetOf(m)
	if err != nil {
		return "", err
	}
	h, err := d.hash("member", identity(t))
	if err != nil {
		return "", err
	}
	return "ssh-" + h, nil
}

// tag returns the tag of the host a hello is from, which a placement's ID
// begins with: the keyed hash of its machine ID and its name, by which
// meridian host keeps its placements apart from those of the hosts that
// share its home.
func (d *Driver) tag(h *hostproto.Hello) (string, error) {
	return d.hash("host", h.MachineID+"\x00"+h.Host)
}

// hostMember is what meridian host is given of m: all but its settings,
// which name the host and the logins, and which the host's local backend
// doesn't read.
func hostMember(m *backend.Member) *backend.Member {
	return &backend.Member{
		Name: m.Name, Backend: m.Backend, Isolation: m.Isolation, Resources: m.Resources,
		MaxWalltimeSeconds: m.MaxWalltimeSeconds, MarginSeconds: m.MarginSeconds, DefaultTimeoutSeconds: m.DefaultTimeoutSeconds,
	}
}

// connect returns the pool's connection to t, waiting connectBudget at
// most for one it has to dial (sshconn.Pool.Get): a host that is being
// dialed, or that the pool waits to dial again, is answered at once.
func (d *Driver) connect(ctx context.Context, t sshconn.Target) (*sshconn.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, connectBudget)
	defer cancel()
	return d.Pool.Get(ctx, t)
}

// reach connects to t for Probe of a job that has seen tries failures to
// reach a member, in all, since it was last placed, as the coordinator
// counts them (backend.Demand.Tries), with one try at most: a problem, as
// a host key known_hosts doesn't have, is the member's. Otherwise, a job
// that has seen Tries failures, or whose own try is the Tries-th, can't
// run on the member (problems.SSHUnreachable). Any other answer is a
// *backend.FailedError, with the pool's failures to reach t in all, by
// which the coordinator counts the failures since the job's previous
// look, so that every job waiting on a host that doesn't answer gives up
// after as many as it has seen: the job's own try that failed, or one
// that hasn't answered within connectBudget, which counts once it fails;
// or, wrapping backend.ErrNotYet, a host the pool is dialing for another,
// or waits to dial again until RetryAt, which the job hasn't tried.
func (d *Driver) reach(ctx context.Context, t sshconn.Target, tries int) (*sshconn.Conn, error) {
	conn, err := d.connect(ctx, t)
	if err == nil || asProblem(err) != nil || ctx.Err() != nil {
		return conn, err
	}
	de, dialed := errors.AsType[*sshconn.DialError](err)
	own := dialed && !de.Waiting
	reason := err
	switch {
	case dialed:
		reason = de.Err
	case errors.Is(err, context.DeadlineExceeded):
		reason = fmt.Errorf("ssh: %s: %w", t.Host, errNoAnswer)
	}
	if tries >= Tries || own && tries+1 >= Tries {
		return nil, problems.SSHUnreachable.Problem(problems.Params{"host": t.Host, "reason": reason.Error()})
	}
	fe := &backend.FailedError{Failed: d.Pool.Failed(t), Err: err}
	switch {
	case own:
		fe.Failed = de.Failed
	case dialed:
		fe.Failed, fe.RetryAt, fe.Err = de.Failed, de.RetryAt, fmt.Errorf("%w: %w", backend.ErrNotYet, err)
	default:
		// The job's own try, which goes on: it counts once it fails.
		fe.Err = fmt.Errorf("%w: %w", backend.ErrNotYet, reason)
	}
	return nil, fe
}

// errNoAnswer is a try to connect to a host that hasn't answered within
// connectBudget, which goes on, for the pool's next Get.
var errNoAnswer = fmt.Errorf("no answer within %v", connectBudget)

// run runs meridian host op on the host t names, over conn, or else the
// pool's connection to it, with the request req makes for the host, and
// returns what it said, and what the driver knows of the host. A
// connection that turns out to have closed before the request was sent,
// as one the host dropped before the pool knew, is made again, once:
// nothing was asked of the host over it.
func (d *Driver) run(ctx context.Context, t sshconn.Target, conn *sshconn.Conn, op string, req func(*host) (*hostproto.Request, error)) (*hostproto.Message, *host, error) {
	for try := 1; ; try++ {
		if conn == nil {
			var err error
			if conn, err = d.connect(ctx, t); err != nil {
				return nil, nil, err
			}
		}
		m, h, err := d.runOn(ctx, conn, t, op, req)
		if _, unsent := err.(*unsentError); !unsent || try > 1 || ctx.Err() != nil || !closed(conn, err) { //nolint:errorlint // runOn's own error
			return m, h, sentOrNot(err)
		}
		conn = nil
	}
}

// closedWait is how long closed waits for a connection to say it has
// closed, once something on it has failed.
const closedWait = time.Second

// closed reports whether conn has closed: it has said so, or it does
// within closedWait of err, which isn't a problem, as a session that
// failed on a connection whose host has gone, before the connection
// noticed.
func closed(conn *sshconn.Conn, err error) bool {
	if conn.Err() != nil {
		return true
	}
	if asProblem(err) != nil {
		return false
	}
	timer := time.NewTimer(closedWait)
	defer timer.Stop()
	select {
	case <-conn.Done():
		return true
	case <-timer.C:
		return false
	}
}

// runOn is run, over conn. An error before the request was sent, learning
// the host among it, is an *unsentError.
func (d *Driver) runOn(ctx context.Context, conn *sshconn.Conn, t sshconn.Target, op string, req func(*host) (*hostproto.Request, error)) (*hostproto.Message, *host, error) {
	h, err := d.host(ctx, conn, t)
	if err != nil {
		return nil, nil, &unsentError{err}
	}
	r, err := req(h)
	if err != nil {
		return nil, h, err
	}
	m, err := d.call(ctx, conn, h, t, op, r)
	return m, h, err
}

// Probe says whether m's host can run a job that asks for demand, as the
// host's local backend says, through meridian host, once the driver has
// reached the host, with one try at most (reach), put the
// binary for its system there, and asked it what it is, and how it would
// start a worker (hostproto.OpHello). A problem on the way, as a host key
// known_hosts doesn't have, a system the worker doesn't run on, or a host
// that would end a worker with the session that started it
// (problems.SSHWorkerWouldBeKilled), is the Fit's.
func (d *Driver) Probe(ctx context.Context, m *backend.Member, demand backend.Demand) (backend.Fit, error) {
	fit := func(err error) (backend.Fit, error) {
		if p := asProblem(err); p != nil {
			return backend.Fit{Problem: p}, nil //nolint:nilerr // what is wrong with the member is the Fit's, not the call's
		}
		return backend.Fit{}, err
	}
	t, err := targetOf(m)
	if err != nil {
		return fit(err)
	}
	conn, err := d.reach(ctx, t, demand.Tries)
	if err != nil {
		return fit(err)
	}
	msg, _, err := d.run(ctx, t, conn, hostproto.OpProbe, func(*host) (*hostproto.Request, error) {
		return &hostproto.Request{Member: hostMember(m), Demand: &hostproto.Demand{Resources: demand.Resources, TimeoutSeconds: demand.TimeoutSeconds, Parked: demand.Parked}}, nil
	})
	if err != nil {
		return fit(err)
	}
	if msg.Fit == nil {
		return backend.Fit{}, errors.New("ssh: meridian host's probe gave no fit")
	}
	return msg.Fit.Fit(), nil
}

// Place starts a worker for the attempt on m's host, through meridian host,
// which starts it detached, as the host requires, in the run's directory
// on the host, which it chooses, by the run's and the store's IDs, and
// for a child run by its root's, with the child's path in it; and
// hands the worker its bootstrap token, written on the session's standard
// input once meridian host has hardened itself, never in an argument or
// the environment, before it returns the placement.
func (d *Driver) Place(ctx context.Context, m *backend.Member, req backend.PlaceRequest) (backend.Placement, error) {
	t, err := targetOf(m)
	if err != nil {
		return backend.Placement{}, err
	}
	msg, h, err := d.run(ctx, t, nil, hostproto.OpPlace, func(*host) (*hostproto.Request, error) {
		return &hostproto.Request{Member: hostMember(m), Place: &hostproto.Place{
			Attempt: req.Attempt, Name: req.Name,
			Coordinator:    hostproto.Endpoint{Address: req.Coordinator.Address, KeyHash: req.Coordinator.KeyHash},
			BootstrapToken: req.BootstrapToken, TimeoutSeconds: req.TimeoutSeconds, Resources: req.Resources,
			TraceParent: req.TraceParent, Run: req.Run, Store: d.Store, Root: req.Root, Path: req.Path,
		}}, nil
	})
	if err != nil {
		return backend.Placement{}, err
	}
	if msg.Placement == nil || msg.Placement.Attempt != req.Attempt {
		return backend.Placement{}, errors.New("ssh: meridian host's place gave no placement of the attempt")
	}
	return d.placed(h, *msg.Placement), nil
}

// placed returns the host's placement p as the driver's, its ID led by the
// tag of the host it is on, which it keeps for the attempt.
func (d *Driver) placed(h *host, p backend.Placement) backend.Placement {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.kept.keep(p.Attempt, h.tag, maxPlaced)
	p.ID = h.tag + "/" + p.ID
	return p
}

// Find returns the attempt's placement on m's host, as its local backend
// finds it. That the host has none is trusted only where the attempt
// isn't one the driver placed or found on another host than the one the
// connection reaches now; there, it is an error, to be tried again.
func (d *Driver) Find(ctx context.Context, m *backend.Member, attempt string) (backend.Placement, bool, error) {
	t, err := targetOf(m)
	if err != nil {
		return backend.Placement{}, false, err
	}
	msg, h, err := d.run(ctx, t, nil, hostproto.OpFind, func(*host) (*hostproto.Request, error) {
		return &hostproto.Request{Member: hostMember(m), Attempt: attempt}, nil
	})
	if err != nil {
		return backend.Placement{}, false, err
	}
	if msg.Found {
		if msg.Placement == nil || msg.Placement.Attempt != attempt {
			return backend.Placement{}, false, errors.New("ssh: meridian host's find gave no placement of the attempt")
		}
		return d.placed(h, *msg.Placement), true, nil
	}
	d.mu.Lock()
	on, known := d.kept.tag(attempt)
	d.mu.Unlock()
	if known && on != h.tag {
		return backend.Placement{}, false, d.elsewhere(m, attempt)
	}
	return backend.Placement{}, false, nil
}

// elsewhere is the error of an attempt placed on another host than the
// one m's connection reaches now.
func (d *Driver) elsewhere(m *backend.Member, attempt string) error {
	name, err := d.Name(m)
	if err != nil {
		return err
	}
	return fmt.Errorf("ssh: %s: %s was placed on another host than the one the connection reaches now", name, attempt)
}

// onHost returns the request of op for the placement p, the driver's, on
// the host h, which must be the one it is on: with the host's own
// placement, its ID without the host's tag. A connection to t that
// reaches another host, which knows nothing of p, as another of a
// cluster's login nodes, is problems.SSHPlacementElsewhere: a cause that
// won't pass while the host it is on is out of the name's rotation.
func (d *Driver) onHost(m *backend.Member, t sshconn.Target, p backend.Placement) func(*host) (*hostproto.Request, error) {
	return func(h *host) (*hostproto.Request, error) {
		on, id, ok := strings.Cut(p.ID, "/")
		if !ok {
			return nil, fmt.Errorf("ssh: %q isn't the ID of a placement on a host", p.ID)
		}
		if on != h.tag {
			return nil, problems.SSHPlacementElsewhere.Problem(problems.Params{"host": t.Host})
		}
		// A request made again, over a connection made again, is made
		// from p as it was.
		local := p
		local.ID = id
		return &hostproto.Request{Member: hostMember(m), Placement: &local}, nil
	}
}

// Check says what has become of a placement's worker, as the host's local
// backend says. One it can't ask is Unknown, with the error that says why:
// for a cause that passes, as a connection that is down, stalls, or whose
// sessions sshd refuses, an error that isn't a problem, so that a worker
// on a host is never lost to an outage; and for one that won't pass by
// itself, a problem, which the coordinator bounds (backend.Driver.Check):
// a host key known_hosts doesn't have, or that has changed; a meridian
// host that stops before it answers, or is of another release; or a
// connection that reaches another host than the one the worker is on.
func (d *Driver) Check(ctx context.Context, m *backend.Member, p backend.Placement) (backend.Status, error) {
	unknown := backend.Status{State: backend.Unknown}
	t, err := targetOf(m)
	if err != nil {
		return unknown, err
	}
	msg, _, err := d.run(ctx, t, nil, hostproto.OpCheck, d.onHost(m, t, p))
	switch {
	case err != nil:
		return unknown, err
	case msg.Status == nil:
		return unknown, errors.New("ssh: meridian host's check gave no status")
	}
	return msg.Status.Status(), nil
}

// Cancel withdraws a placement, as the host's local backend does: it sends
// the worker SIGTERM, and SIGKILL if it still runs a minute later, and
// returns once it has ended. One it can't reach, it doesn't withdraw.
func (d *Driver) Cancel(ctx context.Context, m *backend.Member, p backend.Placement) error {
	return d.end(ctx, m, p, hostproto.OpCancel)
}

// Forget removes what the host's local backend keeps of a placement whose
// worker has ended.
func (d *Driver) Forget(ctx context.Context, m *backend.Member, p backend.Placement) error {
	if err := d.end(ctx, m, p, hostproto.OpForget); err != nil {
		return err
	}
	d.mu.Lock()
	d.kept.forget(p.Attempt)
	d.mu.Unlock()
	return nil
}

// end does op, cancel or forget, to the placement p on m's host.
func (d *Driver) end(ctx context.Context, m *backend.Member, p backend.Placement, op string) error {
	t, err := targetOf(m)
	if err != nil {
		return err
	}
	_, _, err = d.run(ctx, t, nil, op, d.onHost(m, t, p))
	return err
}

// asProblem returns err's problem, or nil for an error that isn't one.
func asProblem(err error) *problem.Problem {
	p, _ := errors.AsType[*problem.Problem](err)
	return p
}
