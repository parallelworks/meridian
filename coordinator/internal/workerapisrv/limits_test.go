package workerapisrv_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// A body that arrives slowly is cut off once its time has passed, so that
// a process sending it holds neither a handler nor a connection.
func TestSlowBody(t *testing.T) {
	r := newRig(t, rigOptions{configure: func(s *workerapisrv.Server) {
		workerapisrv.SetTimes(s, time.Second, 500*time.Millisecond)
	}})
	_, token := r.newAttempt(r.newRun(nil), "build", placed{})
	conn, err := new(net.Dialer).DialContext(t.Context(), "unix", r.socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: m\r\nAuthorization: Bearer %s\r\nContent-Length: 1000\r\n\r\n", workerapi.PathClaim, token); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				if _, err := conn.Write([]byte(" ")); err != nil {
					return
				}
			}
		}
	}()
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode < 400 {
			t.Errorf("a slow body was answered %d", resp.StatusCode)
		}
	}
	if took := time.Since(start); took > 4*time.Second {
		t.Errorf("a slow body held the connection %v", took)
	}
}

// The server holds at most MaxConnections connections at once: another
// waits until one closes.
func TestConnectionLimit(t *testing.T) {
	r := newRig(t, rigOptions{maxConns: 2})
	var held []net.Conn
	for range 2 {
		conn, err := new(net.Dialer).DialContext(t.Context(), "unix", r.socket)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, conn)
	}
	c := newClient(t, r.socket)
	c.http.Timeout = 700 * time.Millisecond
	if status, _, err := c.send(http.MethodPost, workerapi.PathHeartbeat, nil, "x"); err == nil {
		t.Errorf("a third connection was served, %d, while two were held", status)
	}
	for _, conn := range held {
		conn.Close()
	}
	c = newClient(t, r.socket)
	if status, body, err := c.send(http.MethodPost, workerapi.PathHeartbeat, nil, "x"); err != nil || status != http.StatusUnauthorized {
		t.Errorf("once the two closed, a connection was answered %d, %v: %s", status, err, body)
	}
}

// A first claim of an attempt whose placement isn't recorded yet, as when
// its worker claims before the call that placed it returns, is answered
// 503, which the worker tries again, and claims nothing, whatever binary
// claims; once the placement is recorded, the binary it started claims it.
func TestNotPlaced(t *testing.T) {
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	run := r.newRun(nil)
	r.readier.set(run, &workerapisrv.Ready{Job: &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{{Run: plan.Literal("true")}}}})
	a, token := r.newAttempt(run, "build", placed{unplaced: true})
	credential := newCredential(t)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://coordinator"+workerapi.PathClaim, strings.NewReader(string(claimRequest(t, credential, conformanceDigest))))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Errorf("a claim before the placement is recorded was answered %d, Retry-After %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if got := r.attempt(a.ID); !got.ClaimedAt.IsZero() || got.State != store.AttemptPlacing {
		t.Errorf("the attempt is %+v", got)
	}
	r.inTx(func(tx *store.Tx) error {
		return tx.RecordPlacement(a.ID, store.Placement{Backend: "local", ID: "1:2", WorkerDigest: conformanceDigest})
	})
	if status, body, _ := c.send(http.MethodPost, workerapi.PathClaim, claimRequest(t, credential, conformanceDigest), token); status != http.StatusOK {
		t.Errorf("the claim once placed was answered %d: %s", status, body)
	}
}

// manyJobs is a workflow of jobs that need nothing, each of one step.
func manyJobs(n int) string {
	var b strings.Builder
	b.WriteString("jobs:\n")
	for i := range n {
		fmt.Fprintf(&b, "  j%02d:\n    steps:\n      - run: echo %d\n", i, i)
	}
	return b.String()
}

// Ends of a run's jobs at once, each with a private output, are each
// stored, and the run's secret record holds every one: each seals the
// record again in the transaction that stores it.
func TestConcurrentEnds(t *testing.T) {
	const n = 16
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	created, diags, err := compile.Create(t.Context(), "w.yaml", []byte(manyJobs(n)), nil, compile.Options{Source: plan.Source{Location: "w.yaml"}})
	if err != nil || diags.HasErrors() {
		t.Fatal(err, diags)
	}
	run := r.newRunOf(created)
	r.readier.set(run, &workerapisrv.Ready{Job: &plan.ReadyJob{Key: "j", Name: "j", Path: "/jobs/j", Steps: []plan.Step{{Run: plan.Literal("true")}}}})
	credentials := make([]string, n)
	for i := range n {
		_, token := r.newAttempt(run, fmt.Sprintf("j%02d", i), placed{})
		credentials[i] = newCredential(t)
		if status, body, _ := c.send(http.MethodPost, workerapi.PathClaim, claimRequest(t, credentials[i], conformanceDigest), token); status != http.StatusOK {
			t.Fatalf("claim %d was answered %d: %s", i, status, body)
		}
	}
	var wg sync.WaitGroup
	statuses := make([]int, n)
	for i := range n {
		wg.Go(func() {
			end, _ := workerapi.Marshal(workerapi.Complete{Result: plan.JobResult{
				Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"token": "secret-" + strconv.Itoa(i)}, Secrets: []string{"token"},
			}})
			statuses[i], _, _ = newClient(t, r.socket).send(http.MethodPost, workerapi.PathComplete, end, credentials[i])
		})
	}
	wg.Wait()
	for i, status := range statuses {
		if status != http.StatusNoContent {
			t.Errorf("end %d was answered %d", i, status)
		}
	}
	var stored *store.Record
	r.inTx(func(tx *store.Tx) error {
		stored, err = tx.Record(run)
		return err
	})
	if stored.Generation != n+1 {
		t.Errorf("the record's generation is %d, not %d", stored.Generation, n+1)
	}
	rec, err := r.sealer.Open(t.Context(), run, stored.Generation, seal.Sealed{Key: stored.WrappedKey, Record: stored.Sealed})
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if got := rec.Jobs[fmt.Sprintf("j%02d", i)].Outputs["token"]; got != "secret-"+strconv.Itoa(i) {
			t.Errorf("the record holds %q of job %d", got, i)
		}
	}
}

// An end whose events change as it is stored, as from another connection
// of the same worker, is checked again: one behind the events stored is
// refused, 400, and stores nothing.
func TestEndRacesEvents(t *testing.T) {
	var r *rig
	var credential string
	var a *store.Attempt
	r = newRig(t, rigOptions{configure: func(s *workerapisrv.Server) {
		workerapisrv.SetBeforeEnd(s, func() {
			r.inTx(func(tx *store.Tx) error {
				_, err := tx.AppendEvents(a.ID, []workerapi.Event{{Seq: 1, Time: time.Unix(1, 0).UTC(), Finished: true, Outcome: plan.OutcomeSuccess}})
				return err
			})
		})
	}})
	c := newClient(t, r.socket)
	a, _, credential = r.claimed(c, placed{})
	status, body, _ := c.send(http.MethodPost, workerapi.PathComplete, []byte(`{"seq":0,"result":{"outcome":"success"}}`), credential)
	if status != http.StatusBadRequest {
		t.Errorf("an end behind events stored as it was stored was answered %d: %s", status, body)
	}
	if got := r.attempt(a.ID); got.State == store.AttemptEnded {
		t.Error("the end was stored")
	}
}

// Serve, once its context is done, ends the control polls it holds, and
// returns once its requests have ended.
func TestShutdown(t *testing.T) {
	r := newRig(t, rigOptions{hold: 20 * time.Second})
	srv, err := workerapisrv.New(workerapisrv.Options{Store: r.store, Logs: r.logs, Sealer: r.sealer, Readier: r.readier})
	if err != nil {
		t.Fatal(err)
	}
	workerapisrv.SetHold(srv, 20*time.Second)
	l, err := new(net.ListenConfig).Listen(t.Context(), "unix", strings.TrimSuffix(r.socket, "c.sock")+"d.sock")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx, l) }()
	c := newClient(t, l.Addr().String())
	_, _, credential := r.claimed(c, placed{})
	polled := make(chan time.Time, 1)
	go func() {
		_, _, _ = c.send(http.MethodGet, workerapi.PathControl+"?"+workerapi.QueryVersion+"=0", nil, credential)
		polled <- time.Now()
	}()
	select {
	case <-polled:
		t.Fatal("the poll was answered at once")
	case <-time.After(500 * time.Millisecond):
	}
	cancel()
	start := time.Now()
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("Serve = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve didn't return")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("Serve took %v to return", took)
	}
	select {
	case <-polled:
	case <-time.After(5 * time.Second):
		t.Error("the poll held didn't end")
	}
}

// A log keeps its first bytes, to the limit, then a line that says the
// rest wasn't kept, and ends there; the worker is told every byte it sends
// is stored, so it goes on.
func TestLogTruncated(t *testing.T) {
	r := newRig(t, rigOptions{configure: func(s *workerapisrv.Server) {
		workerapisrv.SetLimits(s, 10, workerapisrv.MaxEvents, workerapisrv.MaxEventsBytes)
	}})
	c := newClient(t, r.socket)
	a, _, credential := r.claimed(c, placed{})
	upload := func(offset int, data string, end bool) workerapi.LogAck {
		t.Helper()
		path := fmt.Sprintf("%s?%s=setup&%s=%d&%s=%v", workerapi.PathLogs, workerapi.QueryPart, workerapi.QueryOffset, offset, workerapi.QueryEnd, end)
		status, body, err := c.send(http.MethodPost, path, []byte(data), credential)
		if err != nil || status != http.StatusOK {
			t.Fatalf("an upload at %d was answered %d, %v: %s", offset, status, err, body)
		}
		var ack workerapi.LogAck
		if err := workerapi.Unmarshal(body, &ack); err != nil {
			t.Fatal(err)
		}
		return ack
	}
	if ack := upload(0, "012345", false); ack.Length != 6 || ack.Ended {
		t.Errorf("the first upload is acknowledged %+v", ack)
	}
	if ack := upload(6, "6789abcd", false); ack.Length != 14 || ack.Ended {
		t.Errorf("the upload past the limit is acknowledged %+v", ack)
	}
	if ack := upload(6, "6789abcd", false); ack.Length != 14 || ack.Ended {
		t.Errorf("the same upload again is acknowledged %+v", ack)
	}
	if ack := upload(14, "more", true); ack.Length != 18 || !ack.Ended {
		t.Errorf("the last upload is acknowledged %+v", ack)
	}
	logs := r.logs.of(a.Run, a.Job, a.Number)
	want := "0123456789\n" + problems.LogTruncated.Message(problems.Params{"limit": int64(10)}) + "\n"
	if got := logs[workerapitest.LogKey{Part: workerapi.LogSetup}]; string(got.Data) != want || !got.Ended {
		t.Errorf("the log kept is %q, ended %v; want %q", got.Data, got.Ended, want)
	}
}

// Events past what the server keeps of an attempt's are refused, 413, and
// none of them is stored.
func TestEventsQuota(t *testing.T) {
	r := newRig(t, rigOptions{configure: func(s *workerapisrv.Server) {
		workerapisrv.SetLimits(s, workerapisrv.MaxLogBytes, 2, workerapisrv.MaxEventsBytes)
	}})
	c := newClient(t, r.socket)
	a, _, credential := r.claimed(c, placed{})
	events := func(seqs ...int64) []byte {
		var e workerapi.Events
		for _, seq := range seqs {
			e.Events = append(e.Events, workerapi.Event{Seq: seq, Time: time.Unix(seq, 0).UTC(), Finished: true, Outcome: plan.OutcomeSuccess})
		}
		body, _ := workerapi.Marshal(e)
		return body
	}
	if status, body, _ := c.send(http.MethodPost, workerapi.PathEvents, events(1, 2), credential); status != http.StatusOK {
		t.Fatalf("two events were answered %d: %s", status, body)
	}
	if status, body, _ := c.send(http.MethodPost, workerapi.PathEvents, events(3), credential); status != http.StatusRequestEntityTooLarge {
		t.Errorf("a third event was answered %d: %s", status, body)
	}
	var stored []workerapi.Event
	r.inTx(func(tx *store.Tx) error {
		var err error
		stored, err = tx.Events(a.ID)
		return err
	})
	if len(stored) != 2 {
		t.Errorf("%d events are stored, not 2", len(stored))
	}
}
