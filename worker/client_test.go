package worker

import (
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

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
}

// TestCoordinatorAnswers checks that what the coordinator answers that the
// worker can't go on from ends it with a problem: the coordinator's own
// refusal, as it sent it, but with its text's control characters escaped,
// and an about:blank one with the code its status implies; an answer that
// isn't one it can read, which says why; an acknowledgement of events it
// never sent; and an end refused because the coordinator lost events.
func TestCoordinatorAnswers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		fault fault
		path  string
		kind  *problems.Kind // nil for the coordinator's refusal
		check func(p *problem.Problem) bool
	}{
		{name: "refused", fault: refuse, path: workerapi.PathClaim, check: func(p *problem.Problem) bool {
			return p.Code == refusal && p.Params["path"] == workerapi.PathClaim
		}},
		{name: "refused, about:blank", fault: refuseBlank, path: workerapi.PathClaim, check: func(p *problem.Problem) bool {
			return p.Code == problem.CodeForStatus(http.StatusNotFound) && p.Detail == `no such\x1b[2J attempt`
		}},
		{name: "a 400 without a problem", fault: badRequest, path: workerapi.PathClaim, kind: problems.CoordinatorAnswerInvalid, check: func(p *problem.Problem) bool {
			return p.Params["path"] == workerapi.PathClaim && p.Params["reason"] == "400 Bad Request"
		}},
		{name: "a claim that isn't JSON", fault: garble, path: workerapi.PathClaim, kind: problems.CoordinatorAnswerInvalid, check: func(p *problem.Problem) bool {
			return p.Params["path"] == workerapi.PathClaim && p.Params["reason"] != ""
		}},
		{name: "events refused", fault: refuse, path: workerapi.PathEvents, check: func(p *problem.Problem) bool {
			return p.Code == refusal && p.Params["path"] == workerapi.PathEvents
		}},
		{name: "events stored ahead", fault: ackAhead, path: workerapi.PathEvents, kind: problems.CoordinatorStoredUnsent, check: func(p *problem.Problem) bool {
			return p.Params["seq"] == int64(1<<20)
		}},
		{name: "events lost before the end", fault: forget, path: workerapi.PathComplete, kind: problems.CoordinatorLostData, check: func(p *problem.Problem) bool {
			return p.Params["data"] == "events" && p.Params["from"] == int64(1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFakeCoordinator(t, newJob(command("true")))
			f.fault = func(r *http.Request) fault {
				if r.URL.Path == tc.path {
					return tc.fault
				}
				return answer
			}
			err := f.runWorker(t.Context(), Config{})
			p, ok := errors.AsType[*problem.Problem](err)
			if !ok || tc.kind != nil && p.Code != tc.kind.Code() || !tc.check(p) {
				t.Fatalf("got %v (%+v)", err, p)
			}
		})
	}
}
