package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/internal/testtiming"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// helloWorkflow is a workflow of one job, which the built-in local group
// runs on this machine.
const helloWorkflow = "jobs:\n  main:\n    steps:\n      - run: echo hello\n"

// TestMain runs the test binary as meridian worker when the local backend
// starts it so, as it starts the binary that placed a job, for a job of a
// run that a test's meridian run coordinates. Run with
// testtiming.EnvScale, it shortens the timing of the coordinators it opens
// (testtiming.Scale), as only a test does.
func TestMain(m *testing.M) {
	if v, ok := os.LookupEnv(testtiming.EnvScale); ok {
		s, err := strconv.ParseFloat(v, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", testtiming.EnvScale, err)
			os.Exit(exitError)
		}
		testtiming.Scale = s
	}
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		main()
	}
	os.Exit(m.Run())
}

// runEnv is an environment with directories of a test's own: an empty
// config directory, a state directory and a temporary directory.
func runEnv(t *testing.T) map[string]string {
	t.Helper()
	dir := t.TempDir()
	return map[string]string{
		"HOME": dir, local.EnvConfigDir: filepath.Join(dir, "config"), local.EnvStateDir: filepath.Join(dir, "state"),
		local.EnvCacheDir: filepath.Join(dir, "cache"), "TMPDIR": filepath.Join(dir, "tmp"),
	}
}

// A lockedBuffer is a strings.Builder that goroutines can write at once,
// as meridian run's coordinator logs to its standard error from its own,
// some of which, as hopper's, can outlive the command.
type lockedBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// runTest runs meridian run with args in the environment vars, as a
// test sets c up, and returns what it did.
func runTest(t *testing.T, vars map[string]string, setUp func(*cli), args ...string) result {
	t.Helper()
	var stdout, stderr lockedBuffer
	c := &cli{
		stdin: strings.NewReader(""), stdout: &stdout, stderr: &stderr,
		lookupEnv: func(name string) (string, bool) { v, ok := vars[name]; return v, ok },
	}
	if setUp != nil {
		setUp(c)
	}
	code := run(t.Context(), c, append([]string{"run"}, args...))
	return result{stdout.String(), stderr.String(), code}
}

var startedRE = regexp.MustCompile(`^run (\d+) started \(run directory (.+)\)\n`)

// stepLog returns the log of the first step of the job main of the run id,
// in the store of the state directory env names, as its first attempt's
// worker uploaded it.
func stepLog(t *testing.T, env map[string]string, id string) string {
	t.Helper()
	dirs := local.Dirs{State: env[local.EnvStateDir]}
	db, err := dirs.Database()
	if err != nil {
		t.Fatal(err)
	}
	coord, err := coordinator.Open(t.Context(), coordinator.Options{Database: db, Keys: local.NewKeys(dirs.State)})
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()
	logs, err := local.NewLogs(dirs.State, coord.StoreID())
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	data, ended, err := logs.Read(t.Context(), coordinator.LogKey{Run: id, Job: "main", Attempt: 1, Part: workerapi.LogStep}, 0, 1<<10)
	if err != nil || !ended {
		t.Fatalf("the step's log is %q, ended %v: %v", data, ended, err)
	}
	return string(data)
}

// timeOfDay is the time a line of meridian run's progress starts with.
var timeOfDay = regexp.MustCompile(`(?m)^\d\d:\d\d:\d\d `)

// TestRunShows checks what meridian run shows of a run as it goes: a line
// for each of its events, with the time it happened, each line of a step's
// log after its job and step, and how the run ended; without -logs, no
// log; and as -format json, JSON lines, and the command's problems as
// problem details.
func TestRunShows(t *testing.T) {
	env := runEnv(t)
	workflow := writeFile(t, "w.yaml", helloWorkflow)
	r := runTest(t, env, nil, workflow)
	if r.code != exitOK || r.stderr != "" {
		t.Fatalf("meridian run = %+v", r)
	}
	want := regexp.MustCompile(`^run 1 started \(run directory [^)]+\)
main ready
main attempt 1 placing on local/\S+
main attempt 1 running on local/\S+
main step 0 started
main step 0\| hello
main step 0 finished: success \(exit status 0\)
main finished: success
run 1 finished: success
$`)
	if got := timeOfDay.ReplaceAllString(r.stdout, ""); !want.MatchString(got) || strings.Count(r.stdout, ":") < 6 {
		t.Errorf("meridian run showed\n%s", r.stdout)
	}

	r = runTest(t, env, nil, "--logs=false", workflow)
	if r.code != exitOK || strings.Contains(r.stdout, "hello") || !strings.Contains(r.stdout, "main step 0 finished") {
		t.Errorf("meridian run --logs=false = %+v", r)
	}

	r = runTest(t, env, nil, "--format", "yaml", workflow)
	if r.code != exitError || !strings.Contains(r.stderr, "(flag_value_invalid)") || r.stdout != "" {
		t.Errorf("meridian run --format yaml = %+v", r)
	}
	r = runTest(t, env, nil, "--format", "json", "--input", "cluster.size=5", writeFile(t, "w.yaml", groupInputs))
	if r.code != exitInvalid || r.stdout != "" || !strings.HasPrefix(r.stderr, "{") || !strings.Contains(r.stderr, `"code":"input_above_max"`) {
		t.Errorf("meridian run --format json of a workflow with errors = %+v", r)
	}
}

// TestRunEnds checks a run from start to end: meridian run says which it
// is and where its directory is, which it made new and private, named
// after the store's ID as well as the run's, runs its job on this machine,
// with a worker the local backend starts, which stores the step's log,
// and exits 0 once the run has ended, having let the coordinator's socket
// go.
func TestRunEnds(t *testing.T) {
	env := runEnv(t)
	workflow := writeFile(t, "w.yaml", helloWorkflow)
	for _, want := range []string{"1", "2"} {
		r := runTest(t, env, nil, workflow)
		m := startedRE.FindStringSubmatch(r.stdout)
		if r.code != exitOK || m == nil || m[1] != want || !strings.HasSuffix(r.stdout, "run "+want+" finished: success\n") || r.stderr != "" {
			t.Fatalf("meridian run = %+v", r)
		}
		dir := m[2]
		if filepath.Dir(dir) != local.RunsDir(env["TMPDIR"]) || !regexp.MustCompile(`^`+want+`-[0-9a-f]{32}$`).MatchString(filepath.Base(dir)) {
			t.Errorf("the run's directory is %s", dir)
		}
		if info, err := os.Stat(dir); err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Errorf("the run's directory is %v, %v", info, err)
		}
		if log := stepLog(t, env, want); log != "hello\n" {
			t.Errorf("the step's log is %q", log)
		}
	}
	state := env[local.EnvStateDir]
	for _, name := range []string{local.DatabaseFile, local.KeyFile} {
		if _, err := os.Stat(filepath.Join(state, name)); err != nil {
			t.Error(err)
		}
	}
	if _, err := os.Lstat(filepath.Join(state, local.SocketFile)); err == nil {
		t.Error("the coordinator's socket is there once meridian run has exited")
	}
}

// TestRunNamedDir checks -run-dir: a directory not there yet is made, and
// one that is there is refused before the run is stored.
func TestRunNamedDir(t *testing.T) {
	env := runEnv(t)
	workflow := writeFile(t, "w.yaml", helloWorkflow)
	there := t.TempDir()
	r := runTest(t, env, nil, "--run-dir", there, workflow)
	if r.code != exitError || !strings.Contains(r.stderr, "(run_dir_exists)") || r.stdout != "" {
		t.Errorf("meridian run --run-dir with a directory that is there = %+v", r)
	}
	named := filepath.Join(t.TempDir(), "run")
	r = runTest(t, env, nil, "--run-dir", named, workflow)
	// The refused run wasn't stored: this is the store's first.
	if r.code != exitOK || !strings.HasPrefix(r.stdout, "run 1 started (run directory "+named+")\n") {
		t.Errorf("meridian run --run-dir = %+v", r)
	}
	if info, err := os.Stat(named); err != nil || info.Mode().Perm() != 0o700 {
		t.Errorf("the named run directory is %v, %v", info, err)
	}
}

// TestRunDirNotMade checks that a run whose directory can't be made once
// it is stored, as when something is at its path, ends failed with the
// problem, so that nothing starts in what is there, and exits 2.
func TestRunDirNotMade(t *testing.T) {
	env := runEnv(t)
	dirs := local.Dirs{State: env[local.EnvStateDir]}
	db, err := dirs.Database()
	if err != nil {
		t.Fatal(err)
	}
	coord, err := coordinator.Open(t.Context(), coordinator.Options{Database: db, Keys: local.NewKeys(dirs.State)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = coord.Close() })
	// Something is where the store's first run's directory will be.
	parent := local.RunsDir(env["TMPDIR"])
	if err := local.EnsureRunsDir(parent); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(parent, local.RunDirName(coord.StoreID(), "1")), 0o700); err != nil {
		t.Fatal(err)
	}
	r := runTest(t, env, nil, writeFile(t, "w.yaml", helloWorkflow))
	if r.code != exitError || !strings.Contains(r.stderr, "(run_dir_exists)") || r.stdout != "" {
		t.Errorf("meridian run with its directory there already = %+v", r)
	}
	s, err := coord.Status(t.Context(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if s.State != coordinator.RunEnded || s.Outcome != plan.OutcomeFailure || len(s.Diagnostics) != 1 || s.Diagnostics[0].Code != problems.RunDirExists.Code() {
		t.Errorf("the run whose directory couldn't be made is %+v", s)
	}
}

// TestRunWorkflowErrors checks that a workflow with errors, as one given an
// input value that its input refuses through -input, as validate takes
// one, exits 1, with its errors, and stores no run.
func TestRunWorkflowErrors(t *testing.T) {
	env := runEnv(t)
	r := runTest(t, env, nil, "--input", "cluster.size=5", writeFile(t, "w.yaml", groupInputs))
	if r.code != exitInvalid || r.stdout != "" || !strings.Contains(r.stderr, "error:") || !strings.Contains(r.stderr, "(input_above_max)") {
		t.Errorf("meridian run of a workflow with errors = %+v", r)
	}
	if _, err := os.Stat(filepath.Join(env[local.EnvStateDir], local.DatabaseFile)); err == nil {
		t.Error("a workflow with errors opened the store")
	}
}

// TestRunConcurrent checks that meridian runs that share a state directory
// run at once, each a coordinator of the store, one of which at a time
// holds the socket, and that each ends.
func TestRunConcurrent(t *testing.T) {
	env := runEnv(t)
	workflow := writeFile(t, "w.yaml", helloWorkflow)
	const runs = 3
	results := make([]result, runs)
	var wg sync.WaitGroup
	for i := range runs {
		wg.Go(func() { results[i] = runTest(t, env, nil, workflow) })
	}
	wg.Wait()
	seen := map[string]bool{}
	for _, r := range results {
		m := startedRE.FindStringSubmatch(r.stdout)
		if r.code != exitOK || m == nil {
			t.Fatalf("a meridian run of %d at once = %+v", runs, r)
		}
		seen[m[1]] = true
	}
	if len(seen) != runs {
		t.Errorf("the runs are %v", seen)
	}
}

// TestRunDetaches checks that an interrupt that no one can answer, as
// when a script runs meridian run, leaves the run running, and exits 2,
// saying how to pick it up.
func TestRunDetaches(t *testing.T) {
	env := runEnv(t)
	workflow := writeFile(t, "w.yaml", helloWorkflow)
	no := false
	r := runTest(t, env, func(c *cli) {
		c.interactive = &no
		c.signals = make(chan os.Signal, 1)
		c.signals <- os.Interrupt
	}, workflow)
	if r.code != exitError || !strings.Contains(r.stdout, "run 1 left running: meridian resume 1") {
		t.Errorf("an interrupted meridian run = %+v", r)
	}
}

// TestRunAsks checks that an interrupt in a terminal asks what to do: an
// answer to leave the run running detaches, as another interrupt does.
func TestRunAsks(t *testing.T) {
	env := runEnv(t)
	workflow := writeFile(t, "w.yaml", helloWorkflow)
	yes := true
	r := runTest(t, env, func(c *cli) {
		c.interactive = &yes
		c.stdin = strings.NewReader("l\n")
		c.signals = make(chan os.Signal, 1)
		c.signals <- os.Interrupt
	}, workflow)
	if r.code != exitError || !strings.Contains(r.stderr, "Cancel it (c), leave it running (l), or keep watching (w)?") ||
		!strings.Contains(r.stdout, "run 1 left running") {
		t.Errorf("meridian run interrupted and told to leave the run = %+v", r)
	}
}

// TestAsk checks the answers to what to do about an interrupted run.
func TestAsk(t *testing.T) {
	for in, want := range map[string]string{
		"c\n": "cancel", "cancel\n": "cancel", "L\n": "leave", "w\n": "watch", "\n": "watch", "what?\n": "watch",
		"": "leave", "c": "cancel",
	} {
		var stderr strings.Builder
		c := &cli{stderr: &stderr}
		if got := ask(c, bufio.NewReader(strings.NewReader(in)), "4", make(chan os.Signal)); got != want {
			t.Errorf("ask with %q = %s, want %s", in, got, want)
		}
	}
	// Another interrupt as it asks leaves the run running.
	stdin, w := io.Pipe()
	defer w.Close()
	interrupts := make(chan os.Signal, 1)
	interrupts <- os.Interrupt
	if got := ask(&cli{stderr: io.Discard}, bufio.NewReader(stdin), "4", interrupts); got != "leave" {
		t.Errorf("ask interrupted again = %s", got)
	}
}

// TestHoldSocket checks that one coordinator of a store at a time holds
// its socket, which it replaces when one that held it left it, and that
// another takes it over once the first lets it go.
func TestHoldSocket(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	db, path := filepath.Join(dir, "meridian.db"), filepath.Join(dir, "s.sock")
	open := func() *coordinator.Coordinator {
		c, err := coordinator.Open(t.Context(), coordinator.Options{Database: db, Keys: local.NewKeys(dir)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	// A socket a process that held it left.
	left, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", path)
	if err != nil {
		t.Fatal(err)
	}
	left.(*net.UnixListener).SetUnlinkOnClose(false)
	left.Close()

	first, second := open(), open()
	logs, err := local.NewLogs(dir, first.StoreID())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logs.Close() })
	ctx1, stop1 := context.WithCancel(t.Context())
	s1 := holdSocket(ctx1, first, path, logs)
	waitServed(t, path)
	if info, err := os.Lstat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("the socket is %v, %v, not only its user's", info, err)
	}
	ctx2, stop2 := context.WithCancel(t.Context())
	defer stop2()
	s2 := holdSocket(ctx2, second, path, logs)
	select {
	case <-s2.done:
		t.Fatal("a second coordinator took the socket that another holds")
	case <-time.After(200 * time.Millisecond):
	}
	stop1()
	<-s1.done
	waitServed(t, path)
	stop2()
	<-s2.done
	if _, err := os.Lstat(path); err == nil {
		t.Error("the socket is there once its last holder let it go")
	}
	for _, s := range []*socket{s1, s2} {
		select {
		case err := <-s.errs:
			t.Error(err)
		default:
		}
	}
}

// waitServed waits for the socket at path to answer, as the worker API's
// server does a request without a credential: 401.
func waitServed(t *testing.T, path string) {
	t.Helper()
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}}
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := client.Get("http://coordinator/v1/control")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("the socket answered %s", resp.Status)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no one serves the socket: %v", err)
		}
		<-time.After(10 * time.Millisecond)
	}
}

// TestSocketPathTooLong checks that a socket's path the system can't hold
// is refused before a run is stored.
func TestSocketPathTooLong(t *testing.T) {
	env := runEnv(t)
	env[local.EnvStateDir] = filepath.Join(env[local.EnvStateDir], strings.Repeat("s", maxSocketPath))
	r := runTest(t, env, nil, writeFile(t, "w.yaml", helloWorkflow))
	if r.code != exitError || !strings.Contains(r.stderr, "(socket_path_too_long)") || r.stdout != "" {
		t.Errorf("meridian run with a long state directory = %+v", r)
	}
	if err := checkSocketPath(strings.Repeat("s", maxSocketPath)); err != nil {
		t.Errorf("a socket's path of the most bytes is refused: %v", err)
	}
	kindtest.Want(t, checkSocketPath(strings.Repeat("s", maxSocketPath+1)), problems.SocketPathTooLong)
}
