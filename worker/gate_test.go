package worker

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/internal/versiontest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// TestGatePinned checks that the worker sends and reads every body of an
// attempt at the attempt's version of the API (workerapi.CheckVersion),
// with members made a later version's than this one (versiontest). In an
// attempt of this version, events and an end that hold one are never
// sent, but are reports it can't encode, and a claim or a control that
// holds one is an answer it can't read; in an attempt of the later
// version, it sends and reads them. It doesn't run in parallel with
// others, whose bodies the members it sets would change.
func TestGatePinned(t *testing.T) {
	later := workerapi.Version + 1
	for _, member := range []string{"Event.cleanup", "Complete.reason", "Control.anyJobFailed", "Claim.retry"} {
		t.Cleanup(versiontest.Set(member, later))
	}
	for _, version := range []int{workerapi.Version, later} {
		body := workerapi.Claim{
			RunID: "run1", AttemptID: "attempt1", APIVersion: version, PlanVersion: plan.Version, Job: []byte(`{}`),
			Attempt: 1, LeaseSeconds: 60, HeartbeatSeconds: 1, Run: plan.RunValues{Format: 1}, RunDir: "/runs/run1", Retry: 2,
		}
		if version >= workerapi.Version2 {
			// Every claim of version 2 or later holds the attempt's
			// control.
			body.Control = &workerapi.Control{Version: 1}
		}
		claim, err := workerapi.Marshal(&body)
		if err != nil {
			t.Fatal(err)
		}
		// A coordinator that answers each path with a body of the later
		// version, and counts the requests it gets.
		var (
			mu       sync.Mutex
			requests = map[string]int{}
		)
		answers := map[string]string{
			workerapi.PathClaim:   string(claim),
			workerapi.PathControl: `{"version":1,"anyJobFailed":true}`,
			workerapi.PathEvents:  `{"seq":1}`,
		}
		socket := serveUnix(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			requests[r.URL.Path]++
			mu.Unlock()
			answer, ok := answers[r.URL.Path]
			if !ok {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(answer))
		}))
		c := newClient(endpoint{address: socket}, testTiming, testLogger(t))
		t.Cleanup(c.close)
		c.peer = func(*net.UnixConn, string) error { return nil }
		c.credential = strings.Repeat("ab", 32)

		_, _, err = c.claim(t.Context(), "token", workerapi.ClaimRequest{Credential: c.credential, APIVersion: version, PlanVersion: plan.Version})
		if version < later {
			if !isProblem(err, problems.CoordinatorAnswerInvalid) {
				t.Errorf("a claim of version %d with a later member: %v", version, err)
			}
			// The rest as though it had been read.
			c.apiVersion = version
		} else if err != nil || c.apiVersion != later {
			t.Errorf("a claim of version %d: %v, and the client speaks %d", version, err, c.apiVersion)
		}

		ctl, err := c.control(t.Context(), 0)
		switch {
		case version < later && !isProblem(err, problems.CoordinatorAnswerInvalid):
			t.Errorf("a control of version %d with a later member: %+v, %v", version, ctl, err)
		case version == later && (err != nil || !ctl.AnyJobFailed):
			t.Errorf("a control of version %d: %+v, %v", version, ctl, err)
		}

		_, err = c.events(t.Context(), []workerapi.Event{{Seq: 1, Time: time.Now(), Finished: true, Cleanup: true, Outcome: plan.OutcomeSuccess}})
		end := c.complete(t.Context(), &workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Reason: plan.ReasonTimeout})
		mu.Lock()
		sent := requests[workerapi.PathEvents] + requests[workerapi.PathComplete]
		mu.Unlock()
		if version < later {
			if _, ok := errors.AsType[*encodeError](err); !ok {
				t.Errorf("events of version %d with a later member: %v", version, err)
			}
			if _, ok := errors.AsType[*encodeError](end); !ok {
				t.Errorf("an end of version %d with a later member: %v", version, end)
			}
			if sent != 0 {
				t.Errorf("reports of version %d with a later member were sent %d times", version, sent)
			}
		} else if err != nil || end != nil || sent != 2 {
			t.Errorf("reports of version %d: %v, %v, sent %d times", version, err, end, sent)
		}
	}
}

// isProblem reports whether err is a problem of the kind k.
func isProblem(err error, k *problems.Kind) bool {
	p, ok := errors.AsType[*problem.Problem](err)
	return ok && p.Code == k.Code()
}

// serveUnix serves h on a new Unix socket until the test ends, and returns
// the socket's path.
func serveUnix(t *testing.T, h http.Handler) string {
	t.Helper()
	// A socket's path is short: t.TempDir's can be too long.
	dir, err := os.MkdirTemp("", "wg")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "c.sock")
	l, err := new(net.ListenConfig).Listen(t.Context(), "unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	srv := &httptest.Server{Listener: l, Config: &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}}
	srv.Start()
	t.Cleanup(srv.Close)
	return socket
}
