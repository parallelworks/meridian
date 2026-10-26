package store

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
)

// The environment of a test's process of its own: what it does, and the
// store's database.
const (
	envChild = "MERIDIAN_STORE_CHILD"
	envPath  = "MERIDIAN_STORE_PATH"
)

// childAttempts and childRuns are how many attempts and runs a process
// that shares a store with another starts.
const (
	childAttempts = 50
	childRuns     = 5
)

// runChild does what role says in a process of a test's own, on the store
// at $MERIDIAN_STORE_PATH, and returns the process's exit status: 0 once
// it has done it, and 1 if it failed. What it reports, it writes to
// standard output, a line each.
func runChild(role string) int {
	ctx := context.Background() //nolint:forbidigo // a process of a test's own, which has no test to take a context from
	if role == "open" || role == "share" {
		// It starts with the others that runChildren starts, once each
		// has said it is ready, as its standard input closes.
		fmt.Println("ready")
		_, _ = io.Copy(io.Discard, os.Stdin)
	}
	s, err := Open(ctx, os.Getenv(envPath), nil)
	if err == nil {
		err = child(ctx, s, role)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func child(ctx context.Context, s *Store, role string) error {
	switch role {
	case "open":
		fmt.Println(s.ID())
		return nil

	case "share":
		// Attempts of run 1's build, and runs, one after another, while
		// another process does the same.
		p, err := makePlan(ctx)
		if err != nil {
			return err
		}
		for i := range childAttempts {
			err := s.Tx(ctx, func(tx *Tx) error {
				a, _, err := tx.NewAttempt(NewAttempt{Run: "1", Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
				if err == nil {
					fmt.Println("fence", a.Fence)
				}
				return err
			})
			if err != nil {
				return err
			}
			// A process that writes without a pause keeps another out,
			// since SQLite's busy handler polls for the lock: pause, as
			// a coordinator does, so that the processes take turns.
			time.Sleep(2 * time.Millisecond) //nolint:forbidigo // see above
			if i%(childAttempts/childRuns) != 0 {
				continue
			}
			err = s.Tx(ctx, func(tx *Tx) error {
				id, err := tx.NewRunID()
				if err != nil {
					return err
				}
				fmt.Println("run", id)
				return insertRun(tx, id, p, fakeJobID(0), []hopper.JobID{fakeJobID(1), fakeJobID(2)})
			})
			if err != nil {
				return err
			}
		}
		return nil

	case "crash-create":
		// The process ends as it stores a run, before it commits.
		p, err := makePlan(ctx)
		if err != nil {
			return err
		}
		c, err := hopper.NewClient(s.Driver(), nil)
		if err != nil {
			return err
		}
		_, err = createRunWorkflow(ctx, s, c, p, die)
		return err

	case "crash-claim":
		// The process starts an attempt of run 1's build, and ends as it
		// records its claim and its first step, before it commits.
		var token string
		err := s.Tx(ctx, func(tx *Tx) error {
			var err error
			_, token, err = placedAttempt(tx, NewAttempt{Run: "1", Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
			return err
		})
		if err != nil {
			return err
		}
		fmt.Println(token)
		return s.Tx(ctx, func(tx *Tx) error {
			a, err := tx.Claim(token, credential('c'), testWorker)
			if err != nil {
				return err
			}
			if err := tx.StepStarted(a.ID); err != nil {
				return err
			}
			return die()
		})

	case "stress":
		return stressRun(ctx, s)

	case "hold":
		// The process holds the endpoint until its standard input closes,
		// or it is killed.
		h, err := s.HoldEndpoint(ctx, "socket", "child")
		if err != nil {
			return err
		}
		fmt.Println("held")
		_, _ = io.Copy(io.Discard, os.Stdin)
		return h.Release(ctx)
	}
	return fmt.Errorf("no role %q", role)
}

// die ends the process at once, as a crash or SIGKILL does.
func die() error {
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}
	if err := p.Kill(); err != nil {
		return err
	}
	select {}
}

// startChild starts a process of the test's own that does role on the
// store at path.
func startChild(t *testing.T, role, path string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$") //nolint:gosec // the test's own binary
	cmd.Env = append(os.Environ(), envChild+"="+role, envPath+"="+path)
	return cmd
}

// runChildren runs a process of the test's own for each of roles, on the
// store at path, which start at once, once each is ready, and returns what
// each wrote, a line each.
func runChildren(t *testing.T, path string, roles ...string) [][]string {
	t.Helper()
	cmds := make([]*exec.Cmd, len(roles))
	stdins := make([]io.WriteCloser, len(roles))
	stdouts := make([]*bufio.Reader, len(roles))
	for i, role := range roles {
		cmds[i] = startChild(t, role, path)
		cmds[i].Stderr = os.Stderr
		var err error
		if stdins[i], err = cmds[i].StdinPipe(); err != nil {
			t.Fatal(err)
		}
		stdout, err := cmds[i].StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		stdouts[i] = bufio.NewReader(stdout)
		if err := cmds[i].Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = cmds[i].Process.Kill()
			_ = cmds[i].Wait()
		})
	}
	for i, stdout := range stdouts {
		if line, err := stdout.ReadString('\n'); err != nil || line != "ready\n" {
			t.Fatalf("the process that does %s said %q (%v)", roles[i], line, err)
		}
	}
	for _, stdin := range stdins {
		stdin.Close()
	}
	lines := make([][]string, len(roles))
	for i, stdout := range stdouts {
		out, err := io.ReadAll(stdout)
		if err != nil {
			t.Fatal(err)
		}
		if err := cmds[i].Wait(); err != nil {
			t.Fatalf("the process that did %s: %v", roles[i], err)
		}
		lines[i] = strings.Fields(strings.ReplaceAll(string(out), " ", "_"))
	}
	return lines
}

// TestConcurrentOpen checks that processes that open a new store at once
// migrate it once, and agree on its ID.
func TestConcurrentOpen(t *testing.T) {
	path := filepath.Join(privateDir(t), "meridian.db")
	outs := runChildren(t, path, "open", "open", "open")
	for _, out := range outs[1:] {
		if !slices.Equal(out, outs[0]) || len(out) != 1 || !hexID.MatchString(out[0]) {
			t.Errorf("the processes gave the IDs %v", outs)
		}
	}
	s := openAt(t, path)
	if s.ID() != outs[0][0] {
		t.Errorf("the store's ID is %s, and the processes said %s", s.ID(), outs[0][0])
	}
	inTx(t, s, func(tx *Tx) error {
		var n int
		if err := tx.tx.QueryRowContext(tx.ctx, "SELECT count(*) FROM meridian_schema").Scan(&n); err != nil {
			return err
		}
		if n != SchemaVersion() {
			t.Errorf("meridian_schema has %d versions, want %d", n, SchemaVersion())
		}
		return nil
	})
}

// TestTwoProcesses checks that two processes that share a store, starting
// attempts of one job and runs at once, each get a fence and a run number
// of their own, which only grow.
func TestTwoProcesses(t *testing.T) {
	path := filepath.Join(privateDir(t), "meridian.db")
	s := openAt(t, path)
	createRun(t, s)

	outs := runChildren(t, path, "share", "share")
	var fences, runs []int
	owner := map[int]int{}
	for which, out := range outs {
		var mine []int
		for _, line := range out {
			what, n, _ := strings.Cut(line, "_")
			i, err := strconv.Atoi(n)
			if err != nil {
				t.Fatalf("a process wrote %q", line)
			}
			switch what {
			case "fence":
				mine = append(mine, i)
				fences = append(fences, i)
				owner[i] = which
			case "run":
				runs = append(runs, i)
			}
		}
		if !slices.IsSorted(mine) {
			t.Errorf("a process's fences went back: %v", mine)
		}
	}
	slices.Sort(fences)
	slices.Sort(runs)
	for i, f := range fences {
		if f != i+1 {
			t.Fatalf("the fences given are %v, want 1 to %d once each", fences, 2*childAttempts)
		}
	}
	turns := 0
	for f := 2; f <= len(fences); f++ {
		if owner[f] != owner[f-1] {
			turns++
		}
	}
	// Each pauses between its transactions, so they take turns, and the
	// fences, and the numbers, were each given while the other wanted one.
	if turns < 2 {
		t.Errorf("the processes took turns %d times; the test needs them to share the database at once", turns)
	}
	for i, r := range runs {
		if r != i+2 {
			t.Fatalf("the runs made are %v, want 2 to %d once each", runs, 2*childRuns+1)
		}
	}
	inTx(t, s, func(tx *Tx) error {
		j, err := tx.Job("1", "build")
		if err != nil {
			return err
		}
		if j.Fence != 2*childAttempts || j.Attempts != 2*childAttempts {
			t.Errorf("the job's fence is %d after %d attempts, want %d", j.Fence, j.Attempts, 2*childAttempts)
		}
		var live int
		if err := tx.tx.QueryRowContext(tx.ctx, "SELECT count(*) FROM meridian_attempts WHERE state <> 'ended'").Scan(&live); err != nil {
			return err
		}
		if live != 1 {
			t.Errorf("%d attempts of the job haven't ended, want 1", live)
		}
		return nil
	})
	checkIntegrity(t, s)
}

// TestCrashCreatingRun checks that a process that ends as it stores a run,
// before it commits, leaves nothing of it: no run, no hopper job, and the
// run's number to give again.
func TestCrashCreatingRun(t *testing.T) {
	path := filepath.Join(privateDir(t), "meridian.db")
	cmd := startChild(t, "crash-create", path)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); !isKilled(err) {
		t.Fatalf("the process ended with %v, want killed", err)
	}
	s := openAt(t, path)
	checkIntegrity(t, s)
	if n := hopperJobs(t, newClient(t, s, nil)); n != 0 {
		t.Errorf("%d hopper jobs are stored", n)
	}
	inTx(t, s, func(tx *Tx) error {
		var runs, records, jobs int
		err := tx.tx.QueryRowContext(tx.ctx, `SELECT (SELECT count(*) FROM meridian_runs), (SELECT count(*) FROM meridian_records),
  (SELECT count(*) FROM meridian_jobs)`).Scan(&runs, &records, &jobs)
		if err != nil {
			return err
		}
		if runs+records+jobs != 0 {
			t.Errorf("%d runs, %d records and %d jobs are stored", runs, records, jobs)
		}
		return nil
	})
	if id := createRun(t, s); id != "1" {
		t.Errorf("the run after the crash is %s, want 1", id)
	}
}

// TestCrashBetweenSteps checks that a process that ends between two
// transactions, as it records an attempt's claim, leaves the first whole
// and nothing of the second: the attempt is placing, with its fence, and
// can be claimed.
func TestCrashBetweenSteps(t *testing.T) {
	path := filepath.Join(privateDir(t), "meridian.db")
	s := openAt(t, path)
	id := createRun(t, s)
	cmd := startChild(t, "crash-claim", path)
	out := new(strings.Builder)
	cmd.Stdout, cmd.Stderr = out, os.Stderr
	if err := cmd.Run(); !isKilled(err) {
		t.Fatalf("the process ended with %v, want killed", err)
	}
	token := strings.TrimSpace(out.String())
	checkIntegrity(t, s)
	inTx(t, s, func(tx *Tx) error {
		j, err := tx.Job(id, "build")
		if err != nil {
			return err
		}
		if j.Fence != 1 || j.Attempts != 1 || j.AttemptsStarted != 0 {
			t.Errorf("the job is %+v", j)
		}
		if _, err := tx.AttemptByCredential(credential('c')); !errors.Is(err, ErrNotFound) {
			t.Errorf("the claim that didn't commit is there: %v", err)
		}
		a, err := tx.Claim(token, credential('d'), testWorker)
		if err != nil {
			return err
		}
		if a.Fence != 1 || a.State != AttemptClaimed || !a.StepStartedAt.IsZero() {
			t.Errorf("the attempt claimed after the crash is %+v", a)
		}
		return nil
	})
}

// isKilled reports whether err is that of a process that was killed.
func isKilled(err error) bool {
	exit, ok := errors.AsType[*exec.ExitError](err)
	return ok && !exit.Exited()
}

// TestEndpointAcrossProcesses checks that a process holds an endpoint
// until it ends, however it ends, and another then takes it over.
func TestEndpointAcrossProcesses(t *testing.T) {
	skipWithoutFileLocks(t)
	path := filepath.Join(privateDir(t), "meridian.db")
	s := openAt(t, path)
	cmd := startChild(t, "hold", path)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	if line, err := bufio.NewReader(stdout).ReadString('\n'); err != nil || line != "held\n" {
		t.Fatalf("the process said %q (%v)", line, err)
	}
	var theirs *Endpoint
	inTx(t, s, func(tx *Tx) error {
		var err error
		theirs, err = tx.Endpoint("socket")
		return err
	})
	if theirs.Holder != "child" {
		t.Errorf("the endpoint is %+v", theirs)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if _, err := s.HoldEndpoint(ctx, "socket", "parent"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("HoldEndpoint while another process holds it = %v, want context.DeadlineExceeded", err)
	}

	// Killed, the process releases its lock, but leaves its record.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	h, err := s.HoldEndpoint(t.Context(), "socket", "parent")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := h.Release(t.Context()); err != nil {
			t.Error(err)
		}
	}()
	if mine := h.Endpoint(); mine.Holder != "parent" || mine.TakenAt.Before(theirs.TakenAt) || !mine.GraceFrom.Equal(mine.TakenAt) {
		t.Errorf("the endpoint taken over is %+v, was %+v", mine, theirs)
	}
}
