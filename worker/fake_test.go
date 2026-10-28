package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// The worker's tests run against workerapitest's Fake, which every
// coordinator's conformance is checked against too, so that it can't
// drift from what a coordinator does. These are the worker's own helpers
// around it.

// claimedClient returns a client that has claimed f's attempt, served on a
// Unix socket, as a worker's has once it runs it.
func claimedClient(t *testing.T, f *workerapitest.Fake) *client {
	t.Helper()
	return claimedClientTimed(t, f, testTiming)
}

// claimedClientTimed is claimedClient, with the timing tm.
func claimedClientTimed(t *testing.T, f *workerapitest.Fake, tm timing) *client {
	t.Helper()
	c := newClient(endpoint{address: f.Serve(), token: f.Token}, tm, testLogger(t))
	t.Cleanup(c.close)
	credential := newCredential()
	if _, _, err := c.claim(t.Context(), f.Token, workerapi.ClaimRequest{Credential: credential, APIVersion: workerapi.Version, PlanVersion: plan.Version}); err != nil {
		t.Fatal(err)
	}
	c.credential = credential
	return c
}

// sendingAttempt returns an attempt of job, as a worker that has claimed it
// from f has it, the context it sends in, and start, which starts sending
// its events, so that a test can queue some first. The worker stopping, as
// for events it couldn't send, fails the test.
func sendingAttempt(t *testing.T, f *workerapitest.Fake, job *plan.ReadyJob) (*attempt, context.Context, func()) {
	t.Helper()
	c := claimedClient(t, f)
	a := &attempt{client: c, log: testLogger(t), events: newEventQueue(), spool: newSpool(nil, 0), job: job, mask: plan.NewMasker()}
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	stop := func(err error) {
		t.Errorf("the worker stopped: %v", err)
		cancel()
	}
	return a, ctx, func() { wg.Go(func() { a.events.send(ctx, c, stop, a.reportNotEncoded) }) }
}

// testTiming is the worker's timing, shortened for tests.
var testTiming = timing{
	grace:        2 * time.Second,
	afterKill:    time.Second,
	backoffMin:   5 * time.Millisecond,
	backoffMax:   50 * time.Millisecond,
	flush:        time.Second,
	firstControl: 500 * time.Millisecond,
	logPace:      5 * time.Millisecond,
}

// runWorker runs a worker against f, served on a Unix socket unless
// cfg.Environ says where it is, with its directory in a test's own unless
// cfg.Environ gives it a home, and returns what Run returns.
func runWorker(t *testing.T, f *workerapitest.Fake, ctx context.Context, cfg Config) error {
	t.Helper()
	return runWorkerTimed(t, f, ctx, cfg, testTiming)
}

// runWorkerTimed is runWorker, with the worker's timing tm.
func runWorkerTimed(t *testing.T, f *workerapitest.Fake, ctx context.Context, cfg Config, tm timing) error {
	t.Helper()
	if !slices.ContainsFunc(cfg.Environ, func(kv string) bool { return strings.HasPrefix(kv, workerapi.EnvCoordinator+"=") }) {
		cfg.Environ = append(cfg.Environ, workerapi.EnvCoordinator+"="+f.Serve())
	}
	// The token in a file, which the worker takes on every system.
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(f.Token), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.Environ = append(cfg.Environ,
		workerapi.EnvBootstrapTokenFile+"="+tokenFile,
		"PATH="+os.Getenv("PATH"),
		"FROM_WORKER=inherited",
	)
	if cfg.Worker.Digest == "" {
		cfg.Worker = workerapi.Worker{Version: "v0.0.0-test", Digest: "sha256:" + strings.Repeat("0", 64)}
	}
	if cfg.Dir == "" && lookupEnv(cfg.Environ, "HOME") == "" {
		// A test that gives the worker a home lets it choose its own
		// directory, in that home's cache.
		cfg.Dir = t.TempDir()
	}
	if cfg.Log == nil {
		cfg.Log = testLogger(t)
	}
	return run(ctx, cfg, tm, func() error { return nil })
}

// testLogger returns a logger that writes to a test's log.
func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testWriter writes to a test's log.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}

// stepEvents returns the events f stored, as strings that are easy to
// compare: "started 0.1", "finished 0.1 failure 3", "cleanup started 0".
func stepEvents(t *testing.T, f *workerapitest.Fake) []string {
	t.Helper()
	var out []string
	for k, e := range f.Events() {
		if e.Seq != int64(k+1) {
			t.Errorf("event %d has the sequence number %d", k+1, e.Seq)
		}
		s := ""
		if e.Cleanup {
			s = "cleanup "
		}
		if e.Finished {
			s += "finished " + strconv.Itoa(e.Step) + "." + strconv.Itoa(e.Try) + " " + string(e.Outcome) + " " + strconv.Itoa(e.ExitCode)
		} else {
			s += "started " + strconv.Itoa(e.Step) + "." + strconv.Itoa(e.Try)
		}
		out = append(out, s)
	}
	return out
}

// finishedEvent returns the finished event of a step's try that f stored.
func finishedEvent(t *testing.T, f *workerapitest.Fake, step, try int, cleanup bool) workerapi.Event {
	t.Helper()
	for _, e := range f.Events() {
		if e.Finished && e.Step == step && e.Try == try && e.Cleanup == cleanup {
			return e
		}
	}
	t.Fatalf("no finished event for step %d, try %d", step, try)
	return workerapi.Event{}
}

// logOf returns a log f stored, and whether it has ended.
func logOf(f *workerapitest.Fake, key logKey) (string, bool) {
	return f.Log(workerapitest.LogKey{Part: key.part, Step: key.step, Try: key.try})
}

// codes returns the codes of diagnostics.
func codes(ds problems.Diagnostics) []string {
	var out []string
	for _, d := range ds {
		out = append(out, string(d.Code))
	}
	return out
}

// keyHash returns the hash a worker checks a TLS server's key against.
func keyHash(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	sum := sha256.Sum256(srv.Certificate().RawSubjectPublicKeyInfo)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// The Fake's large refusal is larger than the worker reads of one.
func TestLargeRefusal(t *testing.T) {
	if workerapitest.LargeRefusal <= maxRefusal {
		t.Errorf("the Fake's large refusal, %d bytes, isn't more than the %d the worker reads", workerapitest.LargeRefusal, maxRefusal)
	}
}
