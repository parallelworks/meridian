package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/coordtest"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/plan"
)

// The stress tests run goroutines, and processes, that start, claim,
// renew, step, end and supersede attempts of run 1's jobs, make runs and
// end them, and reseal run 1's record, all at once, each recording what
// it did in stress_log in the same transaction: the log's order is the
// order they committed in. stressCheck then checks the log, and the
// tables, against what the store promises.

// The environment of a process that runs stressLoop: its random seed, and
// how many times it does something, or 0 for until it is killed.
const (
	envSeed  = "MERIDIAN_STORE_SEED"
	envIters = "MERIDIAN_STORE_ITERS"
)

// stressRun runs stressLoop in a process of a test's own (runChild).
func stressRun(ctx context.Context, s *Store) error {
	seed, _ := strconv.ParseUint(os.Getenv(envSeed), 10, 64)
	iters, _ := strconv.Atoi(os.Getenv(envIters))
	p, err := makePlan(ctx)
	if err != nil {
		return err
	}
	return stressLoop(ctx, s, p, seed, iters)
}

// stressLog records, in tx, what the transaction did: kind, of job, and n,
// a fence, a run's number or a generation.
func stressLog(tx *Tx, kind, job string, n int64) error {
	now, err := tx.now()
	if err != nil {
		return err
	}
	_, err = tx.tx.ExecContext(tx.ctx, "INSERT INTO stress_log (kind, job, n, pid, at) VALUES (?, ?, ?, ?, ?)", kind, job, n, os.Getpid(), now)
	return err
}

// A stressAttempt is one that a stressLoop started, and its worker's
// secrets.
type stressAttempt struct {
	id, token, cred, job string
}

var stressJobs = []string{"build", "test"}

// errRollback is what a stressLoop's transaction returns to roll back.
var errRollback = errors.New("rolled back")

// expected reports whether err is one a stressLoop's call can give, as
// another's call won the race.
func expected(err error) bool {
	return errors.Is(err, ErrFenced) || errors.Is(err, ErrCredential) || errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrState) || errors.Is(err, ErrLapsed) || errors.Is(err, errRollback)
}

// stressLoop does what the stress tests do, at random from seed, iters
// times, or until it is killed for 0.
func stressLoop(ctx context.Context, s *Store, p *plan.Plan, seed uint64, iters int) error {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)) //nolint:gosec // a test's random choices
	var mine []stressAttempt
	pick := func() (stressAttempt, bool) {
		if len(mine) == 0 {
			return stressAttempt{}, false
		}
		if rng.IntN(10) < 7 {
			return mine[len(mine)-1], true
		}
		return mine[rng.IntN(len(mine))], true
	}
	for i := 0; iters == 0 || i < iters; i++ {
		job := stressJobs[rng.IntN(len(stressJobs))]
		var err error
		switch op := rng.IntN(16); op {
		case 0:
			self := rng.IntN(2) == 0
			var m stressAttempt
			err = s.Tx(ctx, func(tx *Tx) error {
				a, token, err := placedAttempt(tx, NewAttempt{Run: "1", Job: job, SelfFencing: self, LeaseSeconds: 60, HeartbeatSeconds: 10})
				if err != nil {
					return err
				}
				m = stressAttempt{id: a.ID, token: token, cred: newID(32), job: job}
				return stressLog(tx, "new", job, a.Fence)
			})
			if err == nil {
				mine = append(mine, m)
				if len(mine) > 20 {
					mine = mine[1:]
				}
			}
		case 2, 10, 11:
			m, ok := pick()
			if !ok {
				continue
			}
			err = s.Tx(ctx, func(tx *Tx) error {
				a, err := tx.Claim(m.token, m.cred, testWorker)
				if err != nil {
					return err
				}
				return stressLog(tx, "claim", a.Job, a.Fence)
			})
		case 3, 12, 13:
			m, ok := pick()
			if !ok {
				continue
			}
			err = s.Tx(ctx, func(tx *Tx) error {
				a, err := tx.Renew(m.cred)
				if err != nil {
					return err
				}
				return stressLog(tx, "renew", a.Job, a.Fence)
			})
		case 4, 14:
			m, ok := pick()
			if !ok {
				continue
			}
			err = s.Tx(ctx, func(tx *Tx) error {
				a, err := tx.Authorize(m.cred)
				if err != nil {
					return err
				}
				if err := tx.StepStarted(a.ID); err != nil {
					return err
				}
				return stressLog(tx, "step", a.Job, a.Fence)
			})
		case 5:
			// The worker completes its attempt.
			m, ok := pick()
			if !ok {
				continue
			}
			err = s.Tx(ctx, func(tx *Tx) error {
				a, err := tx.Authorize(m.cred)
				if err != nil {
					return err
				}
				sum := sha256.Sum256([]byte(m.cred))
				if err := tx.EndAttempt(a.ID, AttemptEnd{Kind: EndCompleted, Outcome: plan.OutcomeSuccess, CompleteDigest: sum[:]}); err != nil {
					return err
				}
				return stressLog(tx, "end", a.Job, a.Fence)
			})
		case 6:
			// A supervisor ends an attempt by its ID, as lost.
			m, ok := pick()
			if !ok {
				continue
			}
			err = s.Tx(ctx, func(tx *Tx) error {
				a, err := tx.Attempt(m.id)
				if err != nil {
					return err
				}
				if err := tx.EndAttempt(a.ID, AttemptEnd{Kind: EndLost}); err != nil {
					return err
				}
				return stressLog(tx, "end", a.Job, a.Fence)
			})
		case 7:
			// A run, rolled back half the time, and ended half the time.
			rollback := rng.IntN(2) == 0
			created := ""
			err = s.Tx(ctx, func(tx *Tx) error {
				id, err := tx.NewRunID()
				if err != nil {
					return err
				}
				if err := insertRun(tx, id, p, fakeJobID(0), []hopper.JobID{fakeJobID(1), fakeJobID(2)}); err != nil {
					return err
				}
				n, _ := runNumber(id)
				if err := stressLog(tx, "run", "", n); err != nil {
					return err
				}
				if rollback {
					return errRollback
				}
				created = id
				return nil
			})
			if err == nil && created != "" && rng.IntN(2) == 0 {
				err = s.Tx(ctx, func(tx *Tx) error {
					if err := tx.EndRun(created, plan.OutcomeSuccess, ""); err != nil {
						return err
					}
					n, _ := runNumber(created)
					return stressLog(tx, "endrun", "", n)
				})
			}
		case 8:
			// A reseal from a generation read in a View, which may race
			// another's.
			var gen uint64
			err = s.View(ctx, func(tx *Tx) error {
				r, err := tx.Record("1")
				if err != nil {
					return err
				}
				gen = r.Generation
				// A View may authorize too.
				if m, ok := pick(); ok {
					_, _ = tx.Authorize(m.cred)
				}
				return nil
			})
			if err != nil {
				break
			}
			err = s.Tx(ctx, func(tx *Tx) error {
				if err := tx.Reseal("1", gen+1, fmt.Appendf(nil, "sealed 1 %d", gen+1)); err != nil {
					return err
				}
				return stressLog(tx, "reseal", "", int64(gen+1)) //nolint:gosec // a test's generations are small
			})
		case 1, 9, 15:
			err = s.View(ctx, func(tx *Tx) error {
				_, err := tx.Jobs("1")
				return err
			})
		}
		if err != nil && !expected(err) {
			return fmt.Errorf("iteration %d: %w", i, err)
		}
	}
	return nil
}

// stressSetUp makes a store for the stress tests, with run 1 and the log.
func stressSetUp(t *testing.T) (string, *Store) {
	t.Helper()
	path := filepath.Join(coordtest.PrivateDir(t), "meridian.db")
	s := openAt(t, path)
	createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		_, err := tx.tx.ExecContext(tx.ctx, "CREATE TABLE stress_log (seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT, job TEXT, n INTEGER, pid INTEGER, at INTEGER)")
		return err
	})
	return path, s
}

// stressCheck checks the log, in the order of the commits, and the
// tables, against what the store promises, and returns how many of each
// kind the log holds:
//   - each new attempt of a job has the fence one more than the last;
//   - a claim, a heartbeat, a step and an end are only of the attempt
//     with its job's last fence, which hasn't ended;
//   - a reseal is of the next generation;
//   - the runs are numbered from 1, without a gap, but for numbers rolled
//     back, which are given again, and none that ended has its record;
//   - a job's fence, attempts and attempts that started a step are its
//     attempts', and it has one attempt that hasn't ended at most, with
//     its fence;
//   - a process's clock never goes back;
//   - the database is whole, and its rows refer to rows that are there.
func stressCheck(t *testing.T, s *Store) map[string]int {
	t.Helper()
	checkIntegrity(t, s)
	counts := map[string]int{}
	err := s.View(t.Context(), func(tx *Tx) error {
		rows, err := tx.tx.QueryContext(tx.ctx, "SELECT seq, kind, job, n, pid, at FROM stress_log ORDER BY seq")
		if err != nil {
			return err
		}
		defer rows.Close()
		fence := map[string]int64{}
		ended := map[string]map[int64]bool{}
		gen := int64(1)
		var runIDs []int64
		lastAt := map[int64]int64{}
		for rows.Next() {
			var seq, n, pid, at int64
			var kind, job string
			if err := rows.Scan(&seq, &kind, &job, &n, &pid, &at); err != nil {
				return err
			}
			counts[kind]++
			if at < lastAt[pid] {
				t.Errorf("log %d: a process's clock went back", seq)
			}
			lastAt[pid] = at
			if ended[job] == nil {
				ended[job] = map[int64]bool{}
			}
			switch kind {
			case "new":
				if n != fence[job]+1 {
					t.Errorf("log %d: job %s's new attempt has the fence %d after %d", seq, job, n, fence[job])
				}
				fence[job] = n
			case "claim", "renew", "step", "end":
				if n != fence[job] {
					t.Errorf("log %d: %s by the attempt with the fence %d of job %s, whose fence was %d", seq, kind, n, job, fence[job])
				}
				if ended[job][n] {
					t.Errorf("log %d: %s by the attempt with the fence %d of job %s, which had ended", seq, kind, n, job)
				}
				if kind == "end" {
					ended[job][n] = true
				}
			case "reseal":
				if n != gen+1 {
					t.Errorf("log %d: a reseal to generation %d after %d", seq, n, gen)
				}
				gen = n
			case "run":
				runIDs = append(runIDs, n)
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}

		// Run 1, which stressSetUp made, and those logged, without a gap.
		want := []int64{1}
		slices.Sort(runIDs)
		want = append(want, runIDs...)
		var got []int64
		runs, err := tx.tx.QueryContext(tx.ctx, "SELECT id FROM meridian_runs ORDER BY id")
		if err != nil {
			return err
		}
		defer runs.Close()
		for runs.Next() {
			var id int64
			if err := runs.Scan(&id); err != nil {
				return err
			}
			got = append(got, id)
		}
		if !slices.Equal(got, want) {
			t.Errorf("the runs are %v, and the log has %v", got, want)
		}
		for i, id := range got {
			if id != int64(i+1) {
				t.Errorf("the runs have a gap: %v", got)
				break
			}
		}
		var last string
		if err := tx.tx.QueryRowContext(tx.ctx, "SELECT value FROM meridian_store WHERE key = 'last_run'").Scan(&last); err != nil {
			return err
		}
		if last != strconv.Itoa(len(got)) {
			t.Errorf("the last run given is %s, of %d runs", last, len(got))
		}

		endedRuns, err := tx.tx.QueryContext(tx.ctx, "SELECT n FROM stress_log WHERE kind = 'endrun'")
		if err != nil {
			return err
		}
		defer endedRuns.Close()
		for endedRuns.Next() {
			var n int64
			if err := endedRuns.Scan(&n); err != nil {
				return err
			}
			id := strconv.FormatInt(n, 10)
			if _, err := tx.Record(id); !errors.Is(err, ErrNotFound) {
				t.Errorf("run %s, which ended, still has its record: %v", id, err)
			}
			if r, err := tx.Run(id); err != nil || r.State != RunEnded {
				t.Errorf("run %s, which ended, is %+v (%v)", id, r, err)
			}
		}

		rec, err := tx.Record("1")
		if err != nil {
			return err
		}
		if int64(rec.Generation) != gen || gen != 1 && string(rec.Sealed) != fmt.Sprintf("sealed 1 %d", gen) { //nolint:gosec // a test's generations are small
			t.Errorf("run 1's record is generation %d, %q, and the log has %d", rec.Generation, rec.Sealed, gen)
		}

		for _, job := range stressJobs {
			j, err := tx.Job("1", job)
			if err != nil {
				return err
			}
			var maxFence, count, started, live int64
			if err := tx.tx.QueryRowContext(tx.ctx, `SELECT coalesce(max(fence), 0), count(*), count(step_started_at),
  count(*) FILTER (WHERE state <> 'ended') FROM meridian_attempts WHERE run_id = 1 AND job_key = ?`, job).Scan(&maxFence, &count, &started, &live); err != nil {
				return err
			}
			if j.Fence != maxFence || int64(j.Attempts) != count || int64(j.AttemptsStarted) != started || live > 1 || j.Fence != fence[job] {
				t.Errorf("job %s has the fence %d, %d attempts and %d that started a step; its attempts have the fence %d at most, are %d, %d started a step and %d haven't ended; the log has the fence %d",
					job, j.Fence, j.Attempts, j.AttemptsStarted, maxFence, count, started, live, fence[job])
			}
			if live == 1 {
				var liveFence int64
				if err := tx.tx.QueryRowContext(tx.ctx, "SELECT fence FROM meridian_attempts WHERE run_id = 1 AND job_key = ? AND state <> 'ended'", job).Scan(&liveFence); err != nil {
					return err
				}
				if liveFence != j.Fence {
					t.Errorf("job %s's attempt that hasn't ended has the fence %d, and the job %d", job, liveFence, j.Fence)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("the log has %v", counts)
	return counts
}

// startStress starts a process of the test's own that runs stressLoop on
// the store at path.
func startStress(t *testing.T, path string, seed uint64, iters int) *exec.Cmd {
	t.Helper()
	cmd := startChild(t, "stress", path)
	cmd.Env = append(cmd.Env, envSeed+"="+strconv.FormatUint(seed, 10), envIters+"="+strconv.Itoa(iters))
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd
}

// seed is a random seed for a stress test, which it logs, so that a
// failure can be run again: MERIDIAN_STORE_STRESS_SEED sets it.
func seed(t *testing.T) uint64 {
	t.Helper()
	s := uint64(time.Now().UnixNano()) //nolint:gosec // the time is after 1970
	if v, err := strconv.ParseUint(os.Getenv("MERIDIAN_STORE_STRESS_SEED"), 10, 64); err == nil {
		s = v
	}
	t.Logf("seed %d", s)
	return s
}

// TestStressGoroutines runs stressLoop in goroutines of this process, on
// two stores of one database.
func TestStressGoroutines(t *testing.T) {
	path, s := stressSetUp(t)
	s2 := openAt(t, path)
	p := testPlan(t)
	base := seed(t)
	var wg sync.WaitGroup
	for g := range 8 {
		st := s
		if g%2 == 1 {
			st = s2
		}
		wg.Go(func() {
			if err := stressLoop(t.Context(), st, p, base+uint64(g), 300); err != nil { //nolint:gosec // g is small
				t.Error(err)
			}
		})
	}
	wg.Wait()
	stressCheck(t, s)
}

// TestStressProcesses runs stressLoop in processes of its own, and in
// goroutines of this one, at once.
func TestStressProcesses(t *testing.T) {
	path, s := stressSetUp(t)
	p := testPlan(t)
	base := seed(t)
	var cmds []*exec.Cmd
	for i := range 4 {
		cmds = append(cmds, startStress(t, path, base+uint64(i), 300)) //nolint:gosec // i is small
	}
	var wg sync.WaitGroup
	for g := range 2 {
		wg.Go(func() {
			if err := stressLoop(t.Context(), s, p, base+100+uint64(g), 300); err != nil { //nolint:gosec // g is small
				t.Error(err)
			}
		})
	}
	wg.Wait()
	for _, c := range cmds {
		if err := c.Wait(); err != nil {
			t.Errorf("a process: %v", err)
		}
	}
	stressCheck(t, s)
}

// stressKilledKinds are what TestStressKilled's processes must have done
// before it stops killing them, stressKilledEach times each: each of what
// an attempt goes through, so that stressCheck checks each against the
// kills.
var stressKilledKinds = []string{"new", "claim", "renew", "step", "end"}

const (
	stressKilledEach = 3
	// stressKilledFor is how long TestStressKilled goes on killing
	// processes for want of stressKilledKinds, at most.
	stressKilledFor = 2 * time.Minute
)

// TestStressKilled kills processes that run stressLoop at random moments,
// mid-transaction as often as not, and checks what they leave. It kills
// them 20 times, or MERIDIAN_STORE_STRESS_CYCLES, and then until the log
// has stressKilledEach of each of stressKilledKinds, for stressKilledFor
// at most, as on a slow machine a process may be killed before it does
// much. It fails if the log has none of one of them.
func TestStressKilled(t *testing.T) {
	path, s := stressSetUp(t)
	cycles := 20
	if v, err := strconv.Atoi(os.Getenv("MERIDIAN_STORE_STRESS_CYCLES")); err == nil {
		cycles = v
	}
	start := time.Now()
	until := start.Add(stressKilledFor)
	if d, ok := t.Deadline(); ok && d.Add(-time.Minute).Before(until) {
		until = d.Add(-time.Minute)
	}
	base := seed(t)
	rng := rand.New(rand.NewPCG(base, 1)) //nolint:gosec // a test's random choices
	live := map[int]*exec.Cmd{}
	next := base
	for i := range 3 {
		next++
		live[i] = startStress(t, path, next, 0)
	}
	enough := func() bool {
		return len(fewer(stressCounts(t, s), stressKilledKinds, stressKilledEach)) == 0
	}
	c := 0
	for ; c < cycles || !enough() && time.Now().Before(until); c++ {
		// The moment of the kill is the test.
		time.Sleep(time.Duration(30+rng.IntN(250)) * time.Millisecond) //nolint:forbidigo // see above
		k := rng.IntN(3)
		_ = live[k].Process.Kill()
		if err := live[k].Wait(); !isKilled(err) {
			t.Fatalf("cycle %d: a process ended with %v, want killed", c, err)
		}
		next++
		live[k] = startStress(t, path, next, 0)
	}
	for _, c := range live {
		_ = c.Process.Kill()
		_ = c.Wait()
	}
	t.Logf("%d kills in %v", c, time.Since(start).Round(time.Millisecond))
	counts := stressCheck(t, s)
	if lack := fewer(counts, stressKilledKinds, 1); len(lack) > 0 {
		t.Errorf("after %d kills in %v, the log has no %v: %v", c, time.Since(start).Round(time.Millisecond), lack, counts)
	}
}

// stressCounts returns how many of each kind the stress log holds.
func stressCounts(t *testing.T, s *Store) map[string]int {
	t.Helper()
	counts := map[string]int{}
	err := s.View(t.Context(), func(tx *Tx) error {
		rows, err := tx.tx.QueryContext(tx.ctx, "SELECT kind, count(*) FROM stress_log GROUP BY kind")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var kind string
			var n int
			if err := rows.Scan(&kind, &n); err != nil {
				return err
			}
			counts[kind] = n
		}
		return rows.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	return counts
}

// fewer returns those of kinds that counts has fewer than n of.
func fewer(counts map[string]int, kinds []string, n int) []string {
	var lack []string
	for _, k := range kinds {
		if counts[k] < n {
			lack = append(lack, k)
		}
	}
	return lack
}

// TestWriteLockAfterKill checks that a process killed with a transaction
// open leaves the database to the next writer at once.
func TestWriteLockAfterKill(t *testing.T) {
	path, s := stressSetUp(t)
	cmd := startStress(t, path, seed(t), 0)
	// Long enough for the process to be mid-transaction, as often as not.
	time.Sleep(300 * time.Millisecond) //nolint:forbidigo // see above
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := s.Tx(ctx, func(tx *Tx) error { _, err := tx.NewRunID(); return err }); err != nil {
		t.Fatal(err)
	}
}
