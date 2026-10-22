package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// TestDirReplaced checks that once a step removes the worker's directory
// and puts something else at its path, here a link to a directory another
// user could have made, the worker uses none of it: it makes a new
// directory, with a new name.
func TestDirReplaced(t *testing.T) {
	t.Parallel()
	other := t.TempDir()
	f := newFakeCoordinator(t, newJob(
		command(`d="$(dirname "$OUTPUTS")"; rm -rf "$d"; ln -s "$OTHER" "$d"; echo "$OUTPUTS"`),
		command(`ls -A "$OTHER"; echo "$OUTPUTS"; echo a=1 >> "$OUTPUTS"`),
	))
	if err := f.runWorker(t.Context(), Config{Environ: []string{"OTHER=" + other}}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success 0", "started 1.0", "finished 1.0 success 0")
	before, _ := f.log(logKey{workerapi.LogStep, 0, 0})
	after, _ := f.log(logKey{workerapi.LogStep, 1, 0})
	if entries, _ := os.ReadDir(other); len(entries) != 0 || strings.Count(after, "\n") != 1 {
		t.Errorf("the worker wrote %d files in the directory the link names: %q", len(entries), after)
	}
	if filepath.Dir(after) == filepath.Dir(before) {
		t.Errorf("the second step's $OUTPUTS is in the old directory: %q, then %q", before, after)
	}
	if got := f.finishedEvent(1, 0, false).Outputs; got["a"] != "1" {
		t.Errorf("the second step's outputs are %v", got)
	}
}

// TestPrivateDir checks that a directory the worker makes is its user's,
// and only its user can use it, and that it refuses one that isn't.
func TestPrivateDir(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	d, err := makePrivateDir(parent, false)
	if err != nil {
		t.Fatal(err)
	}
	defer d.removeAll()
	path, _, _, err := d.current()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := checkPrivate(path); err != nil {
		t.Error(err)
	}
	open := filepath.Join(parent, "open")
	if err := os.Mkdir(open, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o750); err != nil { //nolint:gosec // a directory others can use, which the worker refuses
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{open, link} {
		if _, err := checkPrivate(p); err == nil {
			t.Errorf("%s passed as private", p)
		}
	}
	// Removed, and made again elsewhere.
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	again, _, _, err := d.current()
	if err != nil || again == path {
		t.Errorf("after its directory was removed and made again, the worker uses %q (%v)", again, err)
	}
}

// TestHeartbeatStalls checks that one heartbeat that the coordinator never
// answers, as a connection that is half open would hold it, doesn't stop a
// job that fences itself: the worker gives up on it in time to send
// another. It also heartbeats more often than the coordinator asks, when
// that wouldn't leave it time to try.
func TestHeartbeatStalls(t *testing.T) {
	t.Parallel()
	f := newFakeCoordinator(t, newJob(command("sleep 4")))
	f.claim.SelfFencing, f.claim.LeaseSeconds, f.claim.HeartbeatSeconds = true, 5, 2
	var n atomic.Int32
	f.fault = func(r *http.Request) fault {
		if r.URL.Path == workerapi.PathHeartbeat && n.Add(1) == 1 {
			return hang
		}
		return answer
	}
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatalf("one stalled heartbeat stopped the job: %v", err)
	}
	checkEnd(t, f, plan.OutcomeSuccess, "")
}

// TestFenceKeepsSending checks that a job that fences itself goes on
// sending what it has until its lease lapses: its log reaches a
// coordinator that comes back between the job stopping and the lease
// lapsing.
func TestFenceKeepsSending(t *testing.T) {
	t.Parallel()
	f := newFakeCoordinator(t, newJob(command("echo hello; sleep 30")))
	f.claim.SelfFencing, f.claim.LeaseSeconds, f.claim.HeartbeatSeconds = true, 3, 1
	var cutOff atomic.Int64
	f.onEvent = func(e workerapi.Event) { cutOff.CompareAndSwap(0, time.Now().UnixNano()) }
	f.fault = func(r *http.Request) fault {
		c := cutOff.Load()
		if c == 0 {
			return answer
		}
		since := time.Since(time.Unix(0, c))
		switch {
		case r.URL.Path == workerapi.PathHeartbeat:
			return unavailable
		case r.URL.Path == workerapi.PathLogs && since < 2200*time.Millisecond:
			// Back after the job has stopped, at 1.8 s, and before the
			// lease lapses, at 3 s.
			return unavailable
		}
		return answer
	}
	if err := f.runWorker(t.Context(), Config{}); !errors.Is(err, errLeaseLapsed) {
		t.Fatalf("got %v, want %v", err, errLeaseLapsed)
	}
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "hello\n")
}

// TestStoppedKeepsLastWords checks that a worker stopped from outside
// sends what a step prints in its grace period: the flush period starts
// once the steps have stopped.
func TestStoppedKeepsLastWords(t *testing.T) {
	t.Parallel()
	f := newFakeCoordinator(t, newJob(command(`trap 'sleep 1.5; echo last words; exit 0' TERM; echo started; while :; do sleep 0.1; done`)))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var once sync.Once
	f.fault = func(r *http.Request) fault {
		if r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogStep) {
			once.Do(cancel)
		}
		return answer
	}
	if err := f.runWorker(ctx, Config{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if log, ended := f.log(logKey{workerapi.LogStep, 0, 0}); !ended || !strings.HasPrefix(log, "started\n") || !strings.HasSuffix(log, "last words\n") {
		t.Errorf("the log is %q (ended %t), without what the step printed last", log, ended)
	}
}

// TestPlatformEnvSecret checks that a secret variable a platform gives is
// masked wherever the worker reports it: logs, outputs and problems.
func TestPlatformEnvSecret(t *testing.T) {
	t.Parallel()
	step := command(`echo "key=$PW_API_KEY"; echo "k=$PW_API_KEY" >> $OUTPUTS`)
	step.WorkingDirectory = plan.Literal("/proc/sekr1t-api-key/x")
	ok := command(`echo "key=$PW_API_KEY user=$PW_USER"; echo "k=$PW_API_KEY" >> $OUTPUTS`)
	ok.If = plan.Literal("always()")
	f := newFakeCoordinator(t, newJob(step, ok))
	f.claim.Env = map[string]workerapi.EnvVar{"PW_API_KEY": {Value: "sekr1t-api-key", Secret: true}, "PW_USER": {Value: "alice"}}
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkLog(t, f, logKey{workerapi.LogStep, 1, 0}, "key=*** user=alice\n")
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.events {
		if strings.Contains(fmt.Sprint(e.Outputs, e.Diagnostics), "sekr1t") {
			t.Errorf("an event quotes the secret: %v %v", e.Outputs, e.Diagnostics)
		}
	}
	if f.end == nil || strings.Contains(fmt.Sprint(f.end.Outputs), "sekr1t") {
		t.Errorf("the job's end quotes the secret, or there is none: %+v", f.end)
	}
}

// TestEnvPrecedence checks how the environments are laid over each other:
// the worker's, the workflow's over it, the platform's over that, and the
// worker's own over all.
func TestEnvPrecedence(t *testing.T) {
	t.Parallel()
	step := command(`echo "id=$PW_JOB_ID parent=$PW_PARENT_JOB_DIR user=$PW_USER outputs=$([ "$OUTPUTS" = wf ] || [ "$OUTPUTS" = plat ] && echo theirs || echo ours) dir=$PW_JOB_DIR ci=$CI"`)
	step.Env = map[string]plan.Value{
		"PW_JOB_ID": plan.Literal("wf"), "PW_PARENT_JOB_DIR": plan.Literal("wf"), "PW_USER": plan.Literal("wf"),
		"OUTPUTS": plan.Literal("wf"), "PW_JOB_DIR": plan.Literal("wf"), "CI": plan.Literal("wf"),
	}
	f := newFakeCoordinator(t, newJob(step))
	f.claim.Env = map[string]workerapi.EnvVar{"PW_JOB_ID": {Value: "plat"}, "PW_USER": {Value: "plat"}, "OUTPUTS": {Value: "plat"}, "PW_JOB_DIR": {Value: "plat"}}
	if err := f.runWorker(t.Context(), Config{Environ: []string{"PW_USER=worker", "CI=worker"}}); err != nil {
		t.Fatal(err)
	}
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0},
		"id=plat parent="+f.claim.RunDir+" user=plat outputs=ours dir="+f.claim.RunDir+" ci=wf\n")
}

// TestEnvNameInvalid checks that a variable a platform gives that no
// variable can be named fails the attempt before its first step.
func TestEnvNameInvalid(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"", "A=B", "A\x00B"} {
		f := newFakeCoordinator(t, newJob(command("true")))
		f.claim.Env = map[string]workerapi.EnvVar{name: {Value: "x"}}
		if err := f.runWorker(t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		checkEvents(t, f)
		if end := checkEnd(t, f, plan.OutcomeFailure, plan.ReasonSetupFailed); end.Problem == nil || end.Problem.Code != "env_name_invalid" {
			t.Errorf("%q: the attempt's problem is %+v", name, end.Problem)
		}
	}
}

// TestCompactionLostLog checks that a log stays whole, at the right
// offsets, when its file is compacted as its step prints, and the
// coordinator, some way off, loses what it had stored.
func TestCompactionLostLog(t *testing.T) {
	t.Parallel()
	pad := strings.Repeat("x", 200)
	f := newFakeCoordinator(t, newJob(command(`for i in $(seq 1 400); do echo "line $i `+pad+`"; sleep 0.003; done`)))
	var uploads atomic.Int32
	f.fault = func(r *http.Request) fault {
		if r.URL.Path == workerapi.PathLogs && r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogStep) {
			time.Sleep(10 * time.Millisecond) //nolint:forbidigo // a coordinator some way off, so that the step prints faster than it stores
			if uploads.Add(1) == 30 {
				return forget
			}
		}
		return answer
	}
	err := f.runWorker(t.Context(), Config{})
	if err != nil {
		// Lost bytes from before the file was compacted can't be sent
		// again: the attempt ends, rather than send a log with a hole.
		if !strings.Contains(err.Error(), "lost") {
			t.Fatal(err)
		}
		return
	}
	got, _ := f.log(logKey{workerapi.LogStep, 0, 0})
	for i, line := range strings.Split(strings.TrimSuffix(got, "\n"), "\n") {
		if want := fmt.Sprintf("line %d %s", i+1, pad); line != want {
			t.Fatalf("line %d of the log is %.20q…", i+1, line)
		}
	}
	if n := strings.Count(got, "\n"); n != 400 {
		t.Errorf("the log has %d lines, want 400", n)
	}
}

// TestPeerRefused checks that a worker sends no token to a Unix socket
// that another user serves, and doesn't try it again.
func TestPeerRefused(t *testing.T) {
	t.Parallel()
	f := newFakeCoordinator(t, newJob(command("true")))
	socket := f.serve()
	c := newClient(endpoint{address: socket, token: f.token}, testTiming, testLogger(t))
	defer c.close()
	var dials atomic.Int32
	c.peer = func(*net.UnixConn, string) error {
		dials.Add(1)
		return errors.New("served by another user")
	}
	_, _, err := c.claim(t.Context(), f.token, workerapi.ClaimRequest{Credential: strings.Repeat("ab", 32), PlanVersion: plan.Version})
	if _, ok := errors.AsType[*peerError](err); !ok {
		t.Fatalf("got %v, want a refused peer", err)
	}
	if n := dials.Load(); n != 1 {
		t.Errorf("the worker tried the socket %d times", n)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimed {
		t.Error("the claim reached the coordinator")
	}
	if !peerAllowed(os.Getuid()) || !peerAllowed(0) || peerAllowed(os.Getuid()+1) && os.Getuid()+1 != 0 {
		t.Error("peerAllowed allows other users, or not this one or root")
	}
}

// TestSocketDir checks that, where the system doesn't say who serves a
// Unix socket, the worker uses one only in a directory of its user's that
// no one else can write to.
func TestSocketDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, tc := range []struct {
		mode os.FileMode
		ok   bool
	}{{0o700, true}, {0o755, true}, {0o775, false}, {0o777, false}} {
		sub := filepath.Join(dir, tc.mode.String())
		if err := os.Mkdir(sub, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(sub, tc.mode); err != nil {
			t.Fatal(err)
		}
		if err := checkSocketDir(filepath.Join(sub, "c.sock")); (err == nil) != tc.ok {
			t.Errorf("a socket in a directory of mode %v: %v", tc.mode, err)
		}
	}
}
