package ssh

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode"

	"github.com/parallelworks/foundation/problem"
	"golang.org/x/crypto/ssh"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/backend/hostproto"
	"github.com/parallelworks/meridian/backend/ssh/internal/hostbin"
	"github.com/parallelworks/meridian/backend/ssh/internal/sshconn"
	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/internal/sshtest"
	"github.com/parallelworks/meridian/problems"
)

// built is meridian, built from this tree for the system of the test's
// machine, which is the host, as uname says it: so that the host runs a
// real meridian host, and the workers it places a real meridian worker. It
// is built once, by the first test that needs it, in buildDir.
var built struct {
	once sync.Once
	// path is the binary, digest its digest, and version what it says
	// its version is.
	path, digest, version string
	// dir holds it as a binary fetched for the host's system, as
	// hostbin.Binaries.Dir does: for a test binary built for another
	// architecture than the machine's, as 386 on amd64.
	dir string
	// skip says why the tests can't run here, and err why it couldn't be
	// built.
	skip string
	err  error
}

// buildDir holds what the tests build, until they end.
var buildDir string

func TestMain(m *testing.M) {
	var err error
	if buildDir, err = os.MkdirTemp("", "meridian-ssh-test-"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(buildDir)
	os.Exit(code)
}

// build builds meridian into dir, for the system uname says, as the
// release is built: with the FIPS 140-3 module, and without cgo.
func build(dir string) {
	uname := func(flag string) string {
		out, err := exec.Command("uname", flag).Output() //nolint:noctx // uname, once
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(out))
	}
	goos := map[string]string{"Linux": "linux", "Darwin": "darwin", "FreeBSD": "freebsd"}[uname("-s")]
	goarch := map[string]string{"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}[uname("-m")]
	if goos == "" || goarch == "" {
		built.skip = "the test's machine is a system the worker doesn't run on"
		return
	}
	goTool, err := exec.LookPath("go")
	if err != nil {
		built.skip = "no go to build meridian with"
		return
	}
	built.path = filepath.Join(dir, "meridian")
	cmd := exec.Command(goTool, "build", "-trimpath", "-buildvcs=false", "-o", built.path, "github.com/parallelworks/meridian/cmd/meridian") //nolint:noctx // a build, once
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOFIPS140=certified", "GOOS="+goos, "GOARCH="+goarch, "GOFLAGS=")
	if out, err := cmd.CombinedOutput(); err != nil {
		built.err = fmt.Errorf("go build: %w: %s", err, out)
		return
	}
	data, err := os.ReadFile(built.path)
	if err != nil {
		built.err = err
		return
	}
	sum := sha256.Sum256(data)
	built.digest = "sha256:" + hex.EncodeToString(sum[:])
	out, err := exec.Command(built.path, "version").Output() //nolint:noctx // meridian version, once
	if err != nil {
		built.err = fmt.Errorf("meridian version: %w", err)
		return
	}
	built.version, _, _ = strings.Cut(strings.TrimPrefix(string(out), "meridian "), "\n")
	built.dir = filepath.Join(dir, "fetched")
	fetched := filepath.Join(built.dir, built.version, goos+"-"+goarch)
	if err := os.MkdirAll(fetched, 0o700); err != nil {
		built.err = err
		return
	}
	built.err = os.WriteFile(filepath.Join(fetched, "meridian"), data, 0o700) //nolint:gosec // meridian, which runs
}

// A rig is a host, the test's own machine through a test SSH server, whose
// home is a directory of the test's, and a Driver that reaches it as the
// member m.
type rig struct {
	t      *testing.T
	home   string
	server *sshtest.Server
	d      *Driver
	m      *backend.Member
}

// newRig starts a host, and a Driver that reaches it, whose connections
// find a host that stopped answering within a second and a half.
func newRig(t *testing.T, with ...func(*sshtest.Options)) *rig {
	t.Helper()
	built.once.Do(func() { build(buildDir) })
	switch {
	case built.skip != "":
		t.Skip(built.skip)
	case built.err != nil:
		t.Fatal(built.err)
	}
	r := &rig{t: t, home: t.TempDir()}
	// The user's runtime directory, short enough for the host's sockets.
	runtimeDir, err := os.MkdirTemp("", "msh")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(runtimeDir) })
	client := t.TempDir()
	if err := os.Mkdir(filepath.Join(client, ".ssh"), 0o700); err != nil {
		t.Fatal(err)
	}
	keyFile, signer := sshtest.NewKeyFile(t)
	write(t, filepath.Join(client, ".ssh", "id_ecdsa"), keyFile)
	opts := sshtest.Options{Authorized: []ssh.PublicKey{signer.PublicKey()}, Env: []string{
		"HOME=" + r.home, "XDG_RUNTIME_DIR=" + runtimeDir,
		"XDG_CACHE_HOME=", "XDG_STATE_HOME=", "XDG_CONFIG_HOME=", "MERIDIAN_CACHE_DIR=", "MERIDIAN_STATE_DIR=", "MERIDIAN_CONFIG_DIR=",
	}}
	for _, f := range with {
		f(&opts)
	}
	r.server = sshtest.New(t, opts)
	write(t, filepath.Join(client, ".ssh", "known_hosts"), []byte(r.server.KnownHosts("")))
	dialer := &sshconn.Dialer{
		SSH: filepath.Join(client, "no-ssh"), ConfigFile: filepath.Join(client, "no-config"), Home: client, User: "tester",
		Getenv:         func(string) string { return "" },
		ConnectTimeout: 2 * time.Second, KeepAlive: 300 * time.Millisecond, KeepAliveTimeout: time.Second,
	}
	pool := &sshconn.Pool{Dialer: dialer}
	t.Cleanup(func() { _ = pool.Close() })
	key := make([]byte, MinKeyLen)
	_, _ = rand.Read(key)
	r.d = &Driver{
		Pool:     pool,
		Binaries: &hostbin.Binaries{Version: built.version, Dir: built.dir, Self: func() (string, string, error) { return built.path, built.digest, nil }},
		Store:    "teststore", Key: key,
	}
	r.m = member(t, "ssh", map[string]any{"remoteHost": r.server.Addr()})
	return r
}

// member returns a member of the ssh backend, named name, with the
// settings ssh.
func member(t *testing.T, name string, ssh map[string]any) *backend.Member {
	t.Helper()
	settings, err := jsonv2.Marshal(ssh, jsonv2.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	return &backend.Member{Name: name, Backend: "ssh", Isolation: "host", Settings: settings}
}

func write(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// ctx is a context that ends after d, or with the test.
func ctx(t *testing.T, d time.Duration) context.Context {
	c, cancel := context.WithTimeout(t.Context(), d)
	t.Cleanup(cancel)
	return c
}

// environmental are the problems of a test machine that can't be a host
// the tests drive: one whose workers its system would end, or whose ID
// meridian host can't read.
var environmental = []*problems.Kind{
	problems.SSHWorkerWouldBeKilled, problems.SSHLoginctlMissing, problems.SSHSystemdTooOld,
	problems.SSHHostIDUnknown, problems.WorkerSystemUnsupported,
}

// probe probes the member, and skips the test on a machine that can't be
// a host it drives.
func (r *rig) probe() backend.Fit {
	r.t.Helper()
	var fit backend.Fit
	err := askAgain(r.t, func() (err error) {
		fit, err = r.d.Probe(ctx(r.t, time.Minute), r.m, backend.Demand{})
		return err
	})
	if err != nil {
		r.t.Fatal(err)
	}
	if fit.Problem != nil {
		for _, k := range environmental {
			if fit.Problem.Code == k.Code() {
				r.t.Skipf("the test's machine can't be a host: %s", fit.Problem.Detail)
			}
		}
	}
	return fit
}

// askAgain calls call until it gives anything but the pool's answer that
// the host hasn't answered yet, as it is being dialed, or the pool waits
// to dial it again (sshconn.DialError's Waiting), as the coordinator asks
// again later, for a minute at most; and returns what it gave last.
func askAgain(t *testing.T, call func() error) error {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		err := call()
		de, ok := errors.AsType[*sshconn.DialError](err)
		if !ok || !de.Waiting || time.Now().After(deadline) {
			return err
		}
		time.Sleep(50 * time.Millisecond) //nolint:forbidigo // as the coordinator asks again, later
	}
}

// place places an attempt on the member, and stops its worker when the
// test ends.
func (r *rig) place(attempt string) backend.Placement {
	r.t.Helper()
	return r.placeAsking(attempt, backend.Resources{})
}

// placeAsking places an attempt that asks for res on the member, as place
// does.
func (r *rig) placeAsking(attempt string, res backend.Resources) backend.Placement {
	r.t.Helper()
	r.probe()
	p, err := r.d.Place(ctx(r.t, time.Minute), r.m, backend.PlaceRequest{
		Attempt: attempt, Run: "run1", Name: "meridian-run1-job",
		Coordinator:    backend.Endpoint{Address: filepath.Join(r.home, "coordinator.sock")},
		BootstrapToken: "token-" + attempt, TimeoutSeconds: 600, Resources: res,
	})
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() {
		r.server.Resume()
		r.server.RefuseSessions(0)
		r.server.SetEnv()
		c, cancel := context.WithTimeout(context.WithoutCancel(r.t.Context()), time.Minute)
		defer cancel()
		if err := r.d.Cancel(c, r.m, p); err != nil {
			// Not left running, whatever the driver says.
			_, local, _ := strings.Cut(p.ID, "/")
			pid, _, _ := strings.Cut(local, ":")
			if n, err := strconv.Atoi(pid); err == nil {
				if proc, err := os.FindProcess(n); err == nil {
					_ = proc.Kill()
				}
			}
		}
	})
	return p
}

// check checks p, and fails the test unless it is in the state want.
func (r *rig) check(p backend.Placement, want backend.State) backend.Status {
	r.t.Helper()
	var st backend.Status
	err := askAgain(r.t, func() (err error) {
		st, err = r.d.Check(ctx(r.t, time.Minute), r.m, p)
		return err
	})
	if err != nil || st.State != want {
		r.t.Fatalf("Check gives %+v, %v, want state %d", st, err, want)
	}
	return st
}

// count counts the server's scripts that contain s, which every command,
// /bin/sh -s, reads on its standard input.
func (r *rig) count(s string) int {
	n := 0
	for _, c := range r.server.Scripts() {
		if strings.Contains(c, s) {
			n++
		}
	}
	return n
}

// The scripts the server runs: hostbin's upload, its other scripts, and
// meridian host.
const (
	uploadCommand = `cat > "$f"`
	hostbinScript = "hx() {"
	helperCommand = "unset GODEBUG; exec "
)

// TestProbe checks that Probe puts the binary on the host once, asks
// meridian host what the host is once for each connection, and says what
// the host's local backend says: that it can run a job now, or the
// problem it has with the member, relayed.
func TestProbe(t *testing.T) {
	r := newRig(t)
	if fit := r.probe(); fit.Problem != nil || fit.Availability != backend.StartsNow {
		t.Fatalf("the fit is %+v", fit)
	}
	scripts, helpers := r.count(hostbinScript), r.count(helperCommand)
	if r.count(uploadCommand) != 1 || helpers != 2 {
		t.Errorf("the first probe ran %q", r.server.Scripts())
	}
	r.probe()
	if r.count(hostbinScript) != scripts || r.count(helperCommand) != helpers+1 {
		t.Errorf("a probe on the same connection learned the host again: %q", r.server.Scripts())
	}
	// A new connection learns it again, and finds the binary there.
	r.server.Drop()
	r.probe()
	if r.count(uploadCommand) != 1 || r.count(hostbinScript) == scripts || r.count(helperCommand) != helpers+3 {
		t.Errorf("a probe on a new connection ran %q", r.server.Scripts())
	}

	// meridian host runs without the session's GODEBUG, which would turn
	// off its FIPS 140-only mode, and so its every command.
	r.server.SetEnv("GODEBUG=fips140=off")
	r.server.Drop()
	if fit := r.probe(); fit.Problem != nil {
		t.Errorf("with GODEBUG=fips140=off in the session, the fit is %+v", fit)
	}
	// A binary at a path that holds a space and a quote runs: the path is
	// quoted for sh.
	quoted := filepath.Join(t.TempDir(), "cache 'quoted'")
	if err := os.Mkdir(quoted, 0o700); err != nil {
		t.Fatal(err)
	}
	r.server.SetEnv("MERIDIAN_CACHE_DIR=" + quoted)
	r.server.Drop()
	if fit := r.probe(); fit.Problem != nil {
		t.Errorf("with the binary at a path that holds a space and a quote, the fit is %+v", fit)
	}
	// What hello says is wrong is the Fit's.
	r.server.SetEnv("MERIDIAN_STATE_DIR=state")
	r.server.Drop()
	if fit := r.probe(); fit.Problem == nil || fit.Problem.Code != problems.DirectoryVariableRelative.Code() {
		t.Errorf("a relative MERIDIAN_STATE_DIR on the host gives %+v", fit)
	}
	r.server.SetEnv()

	fit, err := r.d.Probe(t.Context(), r.m, backend.Demand{Resources: backend.Resources{GPU: 1}})
	if err != nil || fit.Problem == nil || fit.Problem.Code != problems.ResourcesExceedMember.Code() {
		t.Errorf("a job that asks for a GPU gives %+v, %v", fit, err)
	}
	podman := *r.m
	podman.Isolation = "podman"
	fit, err = r.d.Probe(t.Context(), &podman, backend.Demand{})
	if err != nil || fit.Problem == nil || fit.Problem.Code != problems.IsolationNotBuilt.Code() {
		t.Errorf("a member that isolates with podman gives %+v, %v", fit, err)
	}
}

// TestPlace checks a placement from start to end: Place starts a worker
// on the host, and gives its placement, whose ID is the host's tag and the
// host's own ID, without the bootstrap token in any command; Find finds
// it; Check says it runs; Cancel stops it, and Check then says it was
// withdrawn; and once Forget has forgotten it, Find doesn't find it.
func TestPlace(t *testing.T) {
	r := newRig(t)
	p := r.place("attempt-1")
	tag, local, ok := strings.Cut(p.ID, "/")
	if p.Attempt != "attempt-1" || !ok || len(tag) != 32 || !strings.Contains(local, ":") || p.WorkerDigest != built.digest {
		t.Errorf("the placement is %+v", p)
	}
	for _, c := range slices.Concat(r.server.Commands(), r.server.Scripts()) {
		if strings.Contains(c, "token-attempt-1") {
			t.Errorf("a command or its script holds the bootstrap token: %q", c)
		}
	}
	found, ok, err := r.d.Find(t.Context(), r.m, "attempt-1")
	if err != nil || !ok || found != p {
		t.Errorf("Find gives %+v, %v, %v, want %+v", found, ok, err, p)
	}
	r.check(p, backend.Running)
	if err := r.d.Forget(t.Context(), r.m, p); err == nil {
		t.Error("Forget forgot a placement whose worker runs")
	}
	if err := r.d.Cancel(ctx(t, time.Minute), r.m, p); err != nil {
		t.Fatal(err)
	}
	if st := r.check(p, backend.Dead); st.End != backend.EndWithdrawn {
		t.Errorf("a cancelled worker's end is %q", st.End)
	}
	if err := r.d.Forget(t.Context(), r.m, p); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := r.d.Find(t.Context(), r.m, "attempt-1"); ok || err != nil {
		t.Errorf("Find found a forgotten placement: %v, %v", ok, err)
	}
	if _, ok, err := r.d.Find(t.Context(), r.m, "attempt-2"); ok || err != nil {
		t.Errorf("Find of an attempt never placed gives %v, %v", ok, err)
	}
}

// TestProbeParked checks that the attempts a Probe says are parked reach
// the host's local backend, which leaves what they asked for out of what
// its member runs: a member full only of a parked attempt starts a job
// now.
func TestProbeParked(t *testing.T) {
	r := newRig(t)
	r.m.Resources = backend.Resources{MilliCPU: 1000}
	r.placeAsking("attempt-1", backend.Resources{MilliCPU: 1000})
	for _, tc := range []struct {
		parked []string
		want   backend.Availability
	}{
		{nil, backend.HasCapacity},
		{[]string{"attempt-1"}, backend.StartsNow},
	} {
		fit, err := r.d.Probe(ctx(t, time.Minute), r.m, backend.Demand{Resources: backend.Resources{MilliCPU: 1000}, Parked: tc.parked})
		if err != nil || fit.Problem != nil || fit.Availability != tc.want {
			t.Errorf("a CPU beside a placement of the one there is, parked %v: %+v, %v; want %v", tc.parked, fit, err, tc.want)
		}
	}
}

// A job is a job being placed, as the coordinator places it: each look of
// its supervisor probes a member, given the job's count of the looks in a
// row that couldn't answer (backend.Demand.Tries), which an answer that
// the member hasn't been tried yet (backend.ErrNotYet) doesn't add to.
type job struct{ tries int }

// probe probes m for j, as a look of its supervisor, and counts it.
func (j *job) probe(t *testing.T, r *rig, m *backend.Member) (backend.Fit, error) {
	t.Helper()
	fit, err := r.d.Probe(ctx(t, time.Minute), m, backend.Demand{Tries: j.tries})
	if err != nil && asProblem(err) == nil && !errors.Is(err, backend.ErrNotYet) {
		j.tries++
	}
	return fit, err
}

// A flakyHost is a host whose sshd is down, as one that reboots: it takes
// connections and closes them, counting them, until it is up, when it
// passes them to the rig's server.
type flakyHost struct {
	ln    net.Listener
	up    atomic.Bool
	tries atomic.Int32
}

// newFlakyHost starts a host that is down, which the rig's known_hosts
// knows by the server's key.
func newFlakyHost(t *testing.T, r *rig) *flakyHost {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	h := &flakyHost{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			if !h.up.Load() {
				h.tries.Add(1)
				_ = c.Close()
				continue
			}
			go func() {
				defer c.Close()
				s, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", r.server.Addr())
				if err != nil {
					return
				}
				defer s.Close()
				go func() { _, _ = io.Copy(s, c) }()
				_, _ = io.Copy(c, s)
			}()
		}
	}()
	knownHosts := filepath.Join(r.d.Pool.Dialer.Home, ".ssh", "known_hosts")
	f, err := os.OpenFile(knownHosts, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(r.server.KnownHosts(ln.Addr().String())); err != nil {
		t.Fatal(err)
	}
	return h
}

// TestUnreachable checks a job's tries of a host that doesn't answer, as
// one that reboots: each Probe makes one try at most, and says it hasn't
// answered yet, an error that isn't a problem, which the coordinator
// counts, and asks about again; a Probe while the pool waits to dial the
// host again makes no try, and says so (backend.ErrNotYet), which isn't
// counted; and the job's fourth try that fails is the member's
// ssh_unreachable. A job that comes to the host within the pool's wait
// after another's tries hasn't tried it: once the host is up again, it
// runs there. And a problem with the host, as a key known_hosts doesn't
// have, is the member's at once.
func TestUnreachable(t *testing.T) {
	r := newRig(t)
	r.probe()
	h := newFlakyHost(t, r)
	m := member(t, "flaky", map[string]any{"remoteHost": h.ln.Addr().String()})
	var a job
	var fit backend.Fit
	deadline := time.Now().Add(time.Minute)
	for looks := 1; fit.Problem == nil; looks++ {
		before := h.tries.Load()
		start := time.Now()
		var err error
		fit, err = a.probe(t, r, m)
		took, tried := time.Since(start), h.tries.Load()-before
		switch {
		case tried > 1 || took > 3*time.Second:
			t.Fatalf("look %d tried the host %d times in %v", looks, tried, took)
		case fit.Problem != nil:
		case err == nil:
			t.Fatalf("look %d gave %+v, without an error", looks, fit)
		case asProblem(err) != nil:
			t.Fatalf("look %d gave the problem %v", looks, err)
		case errors.Is(err, backend.ErrNotYet) != (tried == 0):
			t.Fatalf("look %d tried the host %d times, and gave %v", looks, tried, err)
		case time.Now().After(deadline):
			t.Fatalf("the host was tried %d times in a minute", h.tries.Load())
		default:
			// As the coordinator asks again, later.
			time.Sleep(100 * time.Millisecond) //nolint:forbidigo // a supervisor's snooze
		}
	}
	if p := kindtest.Want(t, fit.Problem, problems.SSHUnreachable).Params; p["host"] != h.ln.Addr().String() || p["reason"] == "" {
		t.Errorf("the problem's params are %v", p)
	}
	if n := h.tries.Load(); n != Tries || a.tries != Tries-1 {
		t.Errorf("the job couldn't reach the member after %d tries, %d counted", n, a.tries)
	}

	// Another job, within the pool's wait after the first's last try,
	// hasn't tried the host, which comes back meanwhile.
	var b job
	if _, err := b.probe(t, r, m); !errors.Is(err, backend.ErrNotYet) || h.tries.Load() != Tries {
		t.Fatalf("a job within the wait gave %v, after %d tries", err, h.tries.Load())
	}
	h.up.Store(true)
	deadline = time.Now().Add(time.Minute)
	for {
		fit, err := b.probe(t, r, m)
		if err == nil && fit.Problem == nil {
			break
		}
		if !errors.Is(err, backend.ErrNotYet) || time.Now().After(deadline) {
			t.Fatalf("the job, once the host is up, gave %+v, %v", fit, err)
		}
		time.Sleep(100 * time.Millisecond) //nolint:forbidigo // a supervisor's snooze
	}
	if b.tries != 0 {
		t.Errorf("the job counted %d tries", b.tries)
	}

	unknown := sshtest.New(t, sshtest.Options{})
	start := time.Now()
	fit, err := r.d.Probe(ctx(t, time.Minute), member(t, "unknown", map[string]any{"remoteHost": unknown.Addr()}), backend.Demand{})
	if err != nil || fit.Problem == nil || fit.Problem.Code != problems.SSHHostKeyUnknown.Code() || time.Since(start) > 3*time.Second {
		t.Errorf("a host whose key isn't known gives %+v, %v, after %v", fit, err, time.Since(start))
	}
}

// TestHello checks that a host whose meridian host runs another binary
// than the one put there isn't driven, and is tried again; nor one that
// says it is of another release, which is ssh_host_release_mismatch.
func TestHello(t *testing.T) {
	r := newRig(t)
	r.probe()
	for _, tc := range []struct {
		name   string
		change func(*hostproto.Hello)
	}{
		{"digest", func(h *hostproto.Hello) { h.Binary.Digest = "sha256:" + strings.Repeat("0", 64) }},
		{"release", func(h *hostproto.Hello) { h.Version = "v0.0.1" }},
	} {
		r.d.hello = tc.change
		r.server.Drop()
		fit, err := r.d.Probe(ctx(t, time.Minute), r.m, backend.Demand{})
		switch tc.name {
		case "digest":
			// Maybe a race with another that put a binary there: tried
			// again.
			if err == nil || fit.Problem != nil {
				t.Errorf("a hello of another digest gives %+v, %v", fit, err)
			}
		default:
			if err != nil || fit.Problem == nil || fit.Problem.Code != problems.SSHHostReleaseMismatch.Code() {
				t.Errorf("a hello of another release gives %+v, %v", fit, err)
			}
		}
	}
	r.d.hello = nil
	r.probe()
}

// TestOutage checks that over a connection that drops, stalls, or whose
// sessions are refused, Check says Unknown, never that the worker is lost,
// and Cancel withdraws nothing; and that once the host answers again, the
// worker is found running.
func TestOutage(t *testing.T) {
	r := newRig(t)
	p := r.place("attempt-1")
	// Unknown, for a cause that passes: an error that isn't a problem.
	unknown := func(what string) {
		t.Helper()
		st, err := r.d.Check(ctx(t, 5*time.Second), r.m, p)
		if _, isProblem := errors.AsType[*problem.Problem](err); st.State != backend.Unknown || err == nil || isProblem {
			t.Errorf("%s: Check gives %+v, %v", what, st, err)
		}
	}

	// Stalled: the keepalives find it, and close the connection; a new
	// one stalls too.
	r.server.Stall()
	unknown("stalled")
	if err := r.d.Cancel(ctx(t, 3*time.Second), r.m, p); err == nil {
		t.Error("Cancel over a stalled connection says it withdrew the placement")
	}
	r.server.Resume()
	r.check(p, backend.Running)

	// Dropped, as a host that rebooted its sshd: a new connection.
	r.server.Drop()
	r.check(p, backend.Running)

	// Dropped as Check runs, whether it has begun to ask or not.
	r.server.Stall()
	done := make(chan struct{})
	go func() {
		defer close(done)
		unknown("dropped as it ran")
	}()
	r.server.Drop()
	<-done
	r.server.Resume()

	// Sessions refused for a while, as sshd's MaxSessions does: asked for
	// again.
	r.server.RefuseSessions(3)
	r.check(p, backend.Running)
	// Refused for good: Unknown once Check gives up.
	r.server.RefuseSessions(-1)
	unknown("every session refused")
	r.server.RefuseSessions(0)
	r.check(p, backend.Running)

	// A host key that changed won't pass by itself: Unknown, with the
	// problem, which the coordinator bounds.
	r.server.SetHostKeys(sshtest.NewKey(t))
	r.server.Drop()
	st, err := r.d.Check(ctx(t, 5*time.Second), r.m, p)
	if pe, ok := errors.AsType[*problem.Problem](err); st.State != backend.Unknown || !ok || pe.Code != problems.SSHHostKeyChanged.Code() {
		t.Errorf("with the host's key changed, Check gives %+v, %v", st, err)
	}
}

// TestAnotherHost checks a connection made again that reaches another host
// than the last did, as a cluster's login nodes behind one name do, whose
// placements are its own: there, the placement of an attempt placed on the
// first is Unknown to Check, and Cancel withdraws nothing; and Find doesn't
// say the attempt has no placement, as it would of one never placed.
// Reaching the first again, all is as it was.
func TestAnotherHost(t *testing.T) {
	r := newRig(t)
	var node sync.Mutex
	machine := "node1"
	r.d.hello = func(h *hostproto.Hello) {
		node.Lock()
		defer node.Unlock()
		h.MachineID += "-" + machine
	}
	reach := func(name string, env ...string) {
		node.Lock()
		machine = name
		node.Unlock()
		r.server.SetEnv(env...)
		r.server.Drop()
	}
	p := r.place("attempt-1")

	reach("node2", "MERIDIAN_STATE_DIR="+t.TempDir())
	st, err := r.d.Check(t.Context(), r.m, p)
	if pe, ok := errors.AsType[*problem.Problem](err); st.State != backend.Unknown || !ok || pe.Code != problems.SSHPlacementElsewhere.Code() {
		t.Errorf("on another host, Check gives %+v, %v", st, err)
	}
	if err := r.d.Cancel(t.Context(), r.m, p); err == nil {
		t.Error("on another host, Cancel says it withdrew the placement")
	}
	if _, ok, err := r.d.Find(t.Context(), r.m, "attempt-1"); ok || err == nil {
		t.Errorf("on another host, Find of an attempt placed on the first gives %v, %v", ok, err)
	}
	if _, ok, err := r.d.Find(t.Context(), r.m, "attempt-2"); ok || err != nil {
		t.Errorf("Find of an attempt never placed gives %v, %v", ok, err)
	}

	reach("node1")
	found, ok, err := r.d.Find(t.Context(), r.m, "attempt-1")
	if err != nil || !ok || found != p {
		t.Errorf("back on the first host, Find gives %+v, %v, %v", found, ok, err)
	}
	r.check(p, backend.Running)
}

// say is a script's line that writes the line line.
func say(line []byte) string { return "cat <<'END'\n" + string(line) + "\nEND\n" }

// helper writes a script that stands for meridian host, and returns its
// path.
func helper(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "meridian")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script), 0o700); err != nil { //nolint:gosec // a test's script, which runs
		t.Fatal(err)
	}
	return path
}

// TestCallOutsideProtocol checks what call makes of meridian host that
// doesn't answer in the protocol: what a login script prints first is
// skipped; what meridian says before it runs a command, on its standard
// error, as a binary that isn't in FIPS 140-3 mode does, is
// ssh_host_helper_failed, with its control characters escaped; one that
// ends saying nothing of its own, or speaks another version of the
// protocol, is an error, to be tried again; and a problem it says is the
// problem.
func TestCallOutsideProtocol(t *testing.T) {
	r := newRig(t)
	conn, err := r.d.Pool.Get(t.Context(), sshconn.Target{Host: r.server.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	hello, err := jsonv2.Marshal(hostproto.Message{Protocol: hostproto.Version, Hello: &hostproto.Hello{Version: "v1", OS: "linux"}})
	if err != nil {
		t.Fatal(err)
	}
	unsafe, err := jsonv2.Marshal(hostproto.Message{Protocol: hostproto.Version, Problem: problems.WorkerBinaryUnsafe.Problem(problems.Params{"path": "/x", "code": "file_changeable"})})
	if err != nil {
		t.Fatal(err)
	}
	ready := `printf '%s\n' '{"protocol":1,"ready":true}'; read -r request; `
	for _, tc := range []struct {
		name, script string
		check        func(*testing.T, *hostproto.Message, error)
	}{
		{
			"banner", `echo "Welcome to the cluster"; echo '{"motd": true}'; ` + ready + say(hello),
			func(t *testing.T, m *hostproto.Message, err error) {
				if err != nil || m.Hello == nil || m.Hello.Version != "v1" {
					t.Errorf("after a login script's lines, call gives %+v, %v", m, err)
				}
			},
		},
		{
			"fips", `echo "meridian: error: Meridian runs only with its cryptography in FIPS 140-3 mode $(printf '\033')[2J (fips_not_enforced)" >&2; exit 2`,
			func(t *testing.T, m *hostproto.Message, err error) {
				p := kindtest.Want(t, err, problems.SSHHostHelperFailed).Params
				reason := fmt.Sprint(p["reason"])
				if p["host"] != "farm" || p["op"] != hostproto.OpHello || !strings.HasSuffix(reason, "(fips_not_enforced)") || strings.ContainsFunc(reason, unicode.IsControl) {
					t.Errorf("the problem's params are %v", p)
				}
			},
		},
		{
			"gone", `echo "sh: 1: exec: not found" >&2; exit 127`,
			func(t *testing.T, m *hostproto.Message, err error) {
				if _, ok := errors.AsType[*problem.Problem](err); ok || err == nil || !strings.Contains(err.Error(), "not found") {
					t.Errorf("a helper that isn't there gives %v", err)
				}
			},
		},
		{
			"version", `printf '%s\n' '{"protocol":2,"ready":true}'`,
			func(t *testing.T, m *hostproto.Message, err error) {
				if _, ok := errors.AsType[*problem.Problem](err); ok || err == nil {
					t.Errorf("a helper of another protocol gives %v", err)
				}
			},
		},
		{
			"problem", ready + say(unsafe) + `exit 2`,
			func(t *testing.T, m *hostproto.Message, err error) {
				kindtest.Want(t, err, problems.WorkerBinaryUnsafe)
			},
		},
		// The real meridian, made to refuse before it runs a command: out
		// of FIPS 140-only mode, and with a flag it doesn't have.
		{
			"meridian-fips", `GODEBUG=fips140=off exec '` + built.path + `' "$@"`,
			func(t *testing.T, m *hostproto.Message, err error) {
				p := kindtest.Want(t, err, problems.SSHHostHelperFailed).Params
				if reason := fmt.Sprint(p["reason"]); !strings.HasPrefix(reason, "meridian: ") || !strings.Contains(reason, string(problems.FIPSNotEnforced.Code())) {
					t.Errorf("meridian out of FIPS 140-only mode gives %v", p)
				}
			},
		},
		{
			"meridian-flag", `exec '` + built.path + `' host --no-such-flag "$2"`,
			func(t *testing.T, m *hostproto.Message, err error) {
				p := kindtest.Want(t, err, problems.SSHHostHelperFailed).Params
				if reason := fmt.Sprint(p["reason"]); !strings.Contains(reason, string(problems.FlagUnknown.Code())) {
					t.Errorf("meridian host with a flag it doesn't have gives %v", p)
				}
			},
		},
		// Nothing is written on its standard input before it says it is
		// ready: a helper that finds something there fails.
		{
			"order", `early=$(timeout 1 head -c 1); if [ -n "$early" ]; then echo "meridian: error: a request before ready (early)" >&2; exit 2; fi; ` + ready + say(hello),
			func(t *testing.T, m *hostproto.Message, err error) {
				if err != nil || m.Hello == nil {
					t.Errorf("a helper that checks that nothing comes before ready gives %+v, %v", m, err)
				}
			},
		},
		{
			"error", ready + `printf '%s\n' '{"protocol":1,"error":"host: busy"}'; exit 2`,
			func(t *testing.T, m *hostproto.Message, err error) {
				if _, ok := errors.AsType[*problem.Problem](err); ok || err == nil || !strings.Contains(err.Error(), "host: busy") {
					t.Errorf("an error it says gives %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := call(ctx(t, 30*time.Second), conn, helper(t, tc.script), "farm", hostproto.OpHello, &hostproto.Request{})
			tc.check(t, m, err)
		})
	}
}

// TestNames checks the names the driver gives: a member's, by its host
// and logins, the same for the same, and other for others, which holds
// none of them; and that meridian host is given none of a member's
// settings. A member that names no host can't run a job.
func TestNames(t *testing.T) {
	d := &Driver{Key: bytes.Repeat([]byte{7}, MinKeyLen)}
	name := func(m *backend.Member) string {
		t.Helper()
		n, err := d.Name(m)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	a := name(member(t, "a", map[string]any{"remoteHost": "farm.example.org", "disconnect-timeout": 30}))
	if b := name(member(t, "b", map[string]any{"remoteHost": "farm.example.org", "remoteUser": nil})); b != a {
		t.Errorf("the same host and logins are %s and %s", a, b)
	}
	for _, other := range []map[string]any{
		{"remoteHost": "farm.example.org", "remoteUser": "alice"},
		{"remoteHost": "farm.example.org", "jumpNodeHost": "login"},
		{"remoteHost": "farm2.example.org"},
	} {
		if n := name(member(t, "a", other)); n == a {
			t.Errorf("%v has the name of another host or login", other)
		}
	}
	if !strings.HasPrefix(a, "ssh-") || len(a) != 36 || strings.Contains(a, "farm") {
		t.Errorf("the name is %s", a)
	}
	if _, err := (&Driver{Key: []byte("short")}).Name(member(t, "a", map[string]any{"remoteHost": "farm"})); err == nil {
		t.Error("a key shorter than MinKeyLen was taken")
	}

	m := member(t, "a", map[string]any{"remoteHost": "farm.example.org"})
	m.Resources = backend.Resources{MilliCPU: 4000}
	if hm := hostMember(m); hm.Settings != nil || hm.Name != "a" || hm.Resources != m.Resources || hm.Isolation != "host" {
		t.Errorf("meridian host is given %+v", hm)
	}

	// The request for a placement on its host is the same each time it is
	// made, as over a connection made again, and leaves the placement as
	// it was.
	p := backend.Placement{Attempt: "attempt-1", ID: "tag/123:456", WorkerDigest: "sha256:x"}
	req := d.onHost(m, sshconn.Target{Host: "farm.example.org"}, p)
	for range 2 {
		r, err := req(&host{tag: "tag"})
		if err != nil || r.Placement.ID != "123:456" || r.Placement.Attempt != "attempt-1" || r.Member.Settings != nil {
			t.Errorf("the request is %+v, %v", r, err)
		}
	}
	if _, err := req(&host{tag: "other"}); err == nil {
		t.Error("a request for a placement on another host was made")
	}

	// Past as many attempts as it keeps, the driver forgets the one it
	// kept longest ago, and only that one.
	var pa placedAttempts
	pa.keep("a1", "t1", 2)
	pa.keep("a2", "t2", 2)
	pa.keep("a1", "t1", 2)
	pa.keep("a3", "t3", 2)
	if _, ok := pa.tag("a2"); ok {
		t.Error("the attempt kept longest ago is still kept")
	}
	for _, a := range []string{"a1", "a3"} {
		if _, ok := pa.tag(a); !ok {
			t.Errorf("%s was forgotten", a)
		}
	}
	pa.forget("a1")
	if _, ok := pa.tag("a1"); ok || pa.tags.Len() != 1 {
		t.Error("a forgotten attempt is still kept")
	}

	for _, ssh := range []map[string]any{{"remoteHost": ""}, {"remoteHost": nil}, {"remoteUser": "alice"}} {
		fit, err := d.Probe(t.Context(), member(t, "nohost", ssh), backend.Demand{})
		if err != nil || fit.Problem == nil || fit.Problem.Code != problems.SSHHostMissing.Code() {
			t.Errorf("%v gives %+v, %v", ssh, fit, err)
		}
	}
}
