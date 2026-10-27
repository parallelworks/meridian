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
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// TestPeerRefused checks that a worker sends no token to a Unix socket
// that another user serves, and doesn't try it again.
func TestPeerRefused(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("true")))
	socket := f.Serve()
	c := newClient(endpoint{address: socket, token: f.Token}, testTiming, testLogger(t))
	defer c.close()
	var dials atomic.Int32
	c.peer = func(*net.UnixConn, string) error {
		dials.Add(1)
		return errors.New("served by another user")
	}
	_, _, err := c.claim(t.Context(), f.Token, workerapi.ClaimRequest{Credential: strings.Repeat("ab", 32), APIVersion: workerapi.Version, PlanVersion: plan.Version})
	if _, ok := errors.AsType[*peerError](err); !ok {
		t.Fatalf("got %v, want a refused peer", err)
	}
	if n := dials.Load(); n != 1 {
		t.Errorf("the worker tried the socket %d times", n)
	}
	if f.Claimed() {
		t.Error("the claim reached the coordinator")
	}
}

// TestRefusalPrintsEscaped checks that what the meridian command prints of
// a coordinator's refusal holds no control character the coordinator sent:
// not in its code, nor in a param, however deep.
func TestRefusalPrintsEscaped(t *testing.T) {
	t.Parallel()
	for _, flt := range []workerapitest.Fault{workerapitest.RefuseHostile, workerapitest.RefuseNested, workerapitest.RefuseBlank} {
		f := workerapitest.NewFake(t, newJob(command("true")))
		f.Fault = func(*http.Request) workerapitest.Fault { return flt }
		err := runWorker(t, f, t.Context(), Config{})
		p, ok := errors.AsType[*problem.Problem](err)
		if !ok {
			t.Fatalf("got %v", err)
		}
		// As cli.fail prints it.
		if line := (problems.Diagnostic{Type: p.Type, Code: p.Code, Detail: p.Detail, Params: p.Params}).String(); strings.ContainsAny(line, "\x1b\x07\n") {
			t.Errorf("the command prints %q", line)
		}
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
		fault workerapitest.Fault
		path  string
		kind  *problems.Kind // nil for the coordinator's refusal
		check func(p *problem.Problem) bool
	}{
		{name: "refused", fault: workerapitest.Refuse, path: workerapi.PathClaim, check: func(p *problem.Problem) bool {
			return p.Code == workerapitest.Refusal && p.Params["path"] == workerapi.PathClaim
		}},
		{name: "refused, about:blank", fault: workerapitest.RefuseBlank, path: workerapi.PathClaim, check: func(p *problem.Problem) bool {
			return p.Code == problem.CodeForStatus(http.StatusNotFound) && p.Detail == `no such\x1b[2J attempt`
		}},
		{name: "a 400 without a problem", fault: workerapitest.BadRequest, path: workerapi.PathClaim, kind: problems.CoordinatorAnswerInvalid, check: func(p *problem.Problem) bool {
			return p.Params["path"] == workerapi.PathClaim && p.Params["status"] == http.StatusBadRequest && p.Params["has_at"] == "false"
		}},
		{name: "a claim that isn't JSON", fault: workerapitest.Garble, path: workerapi.PathClaim, kind: problems.CoordinatorAnswerInvalid, check: func(p *problem.Problem) bool {
			return p.Params["path"] == workerapi.PathClaim && p.Params["status"] == http.StatusOK
		}},
		{name: "a claim with a value of the wrong type", fault: workerapitest.WrongClaim, path: workerapi.PathClaim, kind: problems.CoordinatorAnswerInvalid, check: func(p *problem.Problem) bool {
			return p.Params["has_at"] == "true" && p.Params["at"] == "/runID" && !strings.Contains(p.Detail, "plan:")
		}},
		{name: "a refusal whose code isn't one", fault: workerapitest.RefuseHostile, path: workerapi.PathClaim, kind: problems.CoordinatorAnswerInvalid, check: func(p *problem.Problem) bool {
			return p.Params["at"] == "/code" && p.Params["status"] == http.StatusUnprocessableEntity
		}},
		{name: "a refusal larger than one", fault: workerapitest.RefuseLarge, path: workerapi.PathClaim, kind: problems.CoordinatorAnswerInvalid, check: func(p *problem.Problem) bool {
			return p.Params["status"] == http.StatusUnprocessableEntity
		}},
		{name: "events refused", fault: workerapitest.Refuse, path: workerapi.PathEvents, check: func(p *problem.Problem) bool {
			return p.Code == workerapitest.Refusal && p.Params["path"] == workerapi.PathEvents
		}},
		{name: "events stored ahead", fault: workerapitest.AckAhead, path: workerapi.PathEvents, kind: problems.CoordinatorStoredUnsent, check: func(p *problem.Problem) bool {
			return p.Params["seq"] == int64(1<<20)
		}},
		{name: "events lost before the end", fault: workerapitest.Forget, path: workerapi.PathComplete, kind: problems.CoordinatorLostData, check: func(p *problem.Problem) bool {
			return p.Params["data"] == "events" && p.Params["from"] == int64(1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := workerapitest.NewFake(t, newJob(command("true")))
			f.Fault = func(r *http.Request) workerapitest.Fault {
				if r.URL.Path == tc.path {
					return tc.fault
				}
				return workerapitest.Answer
			}
			err := runWorker(t, f, t.Context(), Config{})
			p, ok := errors.AsType[*problem.Problem](err)
			if !ok || tc.kind != nil && p.Code != tc.kind.Code() || !tc.check(p) {
				t.Fatalf("got %v (%+v)", err, p)
			}
		})
	}
}
