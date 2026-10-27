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

	"github.com/parallelworks/meridian/coordinator/internal/coordtest"

	"github.com/parallelworks/meridian/compile"
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

// The server holds at most MaxConnections connections at once. Past it, a
// new one closes the one that has waited longest for a request, once it
// has waited a second, so that connections that send nothing, however
// many, keep no worker out for longer, and closes none of the worker's
// before it could send its request; while every one is handling a request,
// it waits until one is done. (On a loaded runner, a poll that had just
// connected was closed in place of a heartbeat's connection the server
// hadn't yet marked idle, before the grace.)
func TestConnectionLimit(t *testing.T) {
	r := newRig(t, rigOptions{maxConns: 4, hold: time.Minute})
	a, _, credential := r.claimed(newClient(t, r.socket), placed{})
	var held []net.Conn
	defer func() {
		for _, conn := range held {
			conn.Close()
		}
	}()
	for range 8 {
		conn, err := new(net.Dialer).DialContext(t.Context(), "unix", r.socket)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, conn)
	}
	start := time.Now()
	if status, body, err := newClient(t, r.socket).send(http.MethodPost, workerapi.PathHeartbeat, nil, credential); err != nil || status != http.StatusNoContent {
		t.Errorf("a heartbeat while connections that send nothing were held was answered %d, %v: %s", status, err, body)
	}
	// Two seconds' grace, as eight connections take the four places, but
	// not the ten seconds a request's headers are given, after which the
	// server would close one that sent nothing.
	if took := time.Since(start); took > 8*time.Second {
		t.Errorf("a heartbeat while connections that send nothing were held took %v", took)
	}
	// The oldest of those were closed.
	if err := held[0].SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := held[0].Read(make([]byte, 1)); err == nil {
		t.Error("the oldest connection that sent nothing is still open")
	}

	// Every connection handling a request: control polls, which the server
	// holds until the test answers them.
	polls := make(chan error, 4)
	for range 4 {
		go func() {
			status, body, err := newClient(t, r.socket).send(http.MethodGet, workerapi.PathControl+"?"+workerapi.QueryVersion+"=0", nil, credential)
			if err == nil && status != http.StatusOK {
				err = fmt.Errorf("answered %d: %s", status, body)
			}
			polls <- err
		}()
	}
	waitHeld(t, r.server, a.ID, 4)
	heartbeat := make(chan int, 1)
	go func() {
		status, _, _ := newClient(t, r.socket).send(http.MethodPost, workerapi.PathHeartbeat, nil, credential)
		heartbeat <- status
	}()
	select {
	case status := <-heartbeat:
		t.Fatalf("a heartbeat was answered %d while every connection handled a poll held", status)
	case <-time.After(500 * time.Millisecond):
	}
	// Answered, the polls' connections wait for a request, and the
	// heartbeat's takes one's place.
	r.inTx(func(tx *store.Tx) error {
		return askControl(tx, a.ID, workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled})
	})
	r.server.Notify(a.ID)
	deadline := time.After(10 * time.Second)
	for range 4 {
		select {
		case err := <-polls:
			if err != nil {
				t.Errorf("a poll: %v", err)
			}
		case <-deadline:
			t.Fatal("the polls weren't answered")
		}
	}
	select {
	case status := <-heartbeat:
		if status != http.StatusNoContent {
			t.Errorf("the heartbeat, once the polls were answered, was answered %d", status)
		}
	case <-deadline:
		t.Fatal("the heartbeat wasn't answered once the polls were")
	}
}

// A first claim of an attempt whose placement isn't recorded yet, as when
// its worker claims before the call that placed it returns, waits a while
// for it; one that isn't recorded by then is answered 503, which the
// worker tries again, and claims nothing, whatever binary claims; once the
// placement is recorded, the binary it started claims it.
func TestNotPlaced(t *testing.T) {
	r := newRig(t, rigOptions{configure: func(s *workerapisrv.Server) { workerapisrv.SetPlacedWait(s, 50*time.Millisecond, 10*time.Millisecond) }})
	c := newClient(t, r.socket)
	run := r.newRun(nil)
	r.readier.set(run, &workerapisrv.Ready{Job: &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{{Run: plan.Literal("true")}}}})
	a, token := r.newAttempt(run, "build", placed{unplaced: true})
	credential := coordtest.Credential(t)
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

// A first claim that comes before its placement is recorded, which is
// recorded while it waits, as the call that placed the worker records it,
// is answered with the claim, rather than 503 and a wait of a quarter of a
// second or more before the worker claims again: woken by the coordinator
// that recorded it (Placed), or, for one another process recorded, as it
// reads the store again.
func TestPlacedWhileClaiming(t *testing.T) {
	for _, woken := range []bool{true, false} {
		recheck := 20 * time.Millisecond
		if woken {
			// Only Placed answers it before the wait ends.
			recheck = time.Hour
		}
		r := newRig(t, rigOptions{configure: func(s *workerapisrv.Server) { workerapisrv.SetPlacedWait(s, time.Minute, recheck) }})
		c := newClient(t, r.socket)
		run := r.newRun(nil)
		r.readier.set(run, &workerapisrv.Ready{Job: &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{{Run: plan.Literal("true")}}}})
		a, token := r.newAttempt(run, "build", placed{unplaced: true})
		answered := make(chan int, 1)
		go func() {
			status, _, _ := c.send(http.MethodPost, workerapi.PathClaim, claimRequest(t, coordtest.Credential(t), conformanceDigest), token)
			answered <- status
		}()
		for deadline := time.Now().Add(10 * time.Second); workerapisrv.HeldClaims(r.server, a.ID) != 1; {
			if time.Now().After(deadline) {
				t.Fatal("the claim was never held")
			}
			time.Sleep(5 * time.Millisecond) //nolint:forbidigo // polls what the server holds, which says nothing
		}
		r.inTx(func(tx *store.Tx) error {
			return tx.RecordPlacement(a.ID, store.Placement{Backend: "local", ID: "1:2", WorkerDigest: conformanceDigest})
		})
		if woken {
			r.server.Placed(a.ID)
		}
		select {
		case status := <-answered:
			if status != http.StatusOK {
				t.Errorf("woken %t: a claim whose placement was recorded as it waited was answered %d", woken, status)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("woken %t: the claim wasn't answered", woken)
		}
		if got := r.attempt(a.ID); got.ClaimedAt.IsZero() {
			t.Errorf("woken %t: the attempt is %+v", woken, got)
		}
		if n := workerapisrv.HeldClaims(r.server, a.ID); n != 0 {
			t.Errorf("woken %t: %d claims are still held", woken, n)
		}
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
// stored, each as its job's part of the run's secret record.
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
		credentials[i] = coordtest.Credential(t)
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
	if gen := r.recordGeneration(run); gen != 1 {
		t.Errorf("the run's record is generation %d, not 1", gen)
	}
	for i := range n {
		if part, gen := r.part(run, fmt.Sprintf("j%02d", i)); gen != 1 || part.Outputs["token"] != "secret-"+strconv.Itoa(i) {
			t.Errorf("job %d's part is generation %d, %+v", i, gen, part)
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
	logs := r.logs.Of(a.Run, a.Job, a.Number)
	want := "0123456789\n" + problems.LogTruncated.Message(problems.Params{"limit": int64(10)}) + "\n"
	if got := logs[workerapitest.LogKey{Part: workerapi.LogSetup}]; string(got.Data) != want || !got.Ended {
		t.Errorf("the log kept is %q, ended %v; want %q", got.Data, got.Ended, want)
	}
}

// The limit on logs is on all of an attempt's together: the log that
// reaches it keeps its bytes to there, each log after keeps only the line
// that says the rest wasn't kept, and a try past MaxLogTry keeps no log;
// each upload is answered as stored. Uploads to a log that ended before
// the limit was all given out are checked as ever: the server reserves
// less of it at a time than it is, as it does of MaxLogBytes, so that the
// first upload doesn't give it all out.
func TestLogQuotaPerAttempt(t *testing.T) {
	r := newRig(t, rigOptions{configure: func(s *workerapisrv.Server) {
		workerapisrv.SetLimits(s, 1024, workerapisrv.MaxEvents, workerapisrv.MaxEventsBytes)
		workerapisrv.SetLogReserve(s, 100)
	}})
	c := newClient(t, r.socket)
	a, _, credential := r.claimed(c, placed{})
	upload := func(part workerapi.LogPart, try, offset int, data string, end bool) (int, workerapi.LogAck) {
		t.Helper()
		path := fmt.Sprintf("%s?%s=%s&%s=0&%s=%d&%s=%d&%s=%v", workerapi.PathLogs, workerapi.QueryPart, part, workerapi.QueryStep,
			workerapi.QueryTry, try, workerapi.QueryOffset, offset, workerapi.QueryEnd, end)
		status, body, err := c.send(http.MethodPost, path, []byte(data), credential)
		if err != nil {
			t.Fatal(err)
		}
		var ack workerapi.LogAck
		if status == http.StatusOK {
			if err := workerapi.Unmarshal(body, &ack); err != nil {
				t.Fatal(err)
			}
		}
		return status, ack
	}
	kilobyte := strings.Repeat("x", 1000)
	if status, ack := upload(workerapi.LogSetup, 0, 0, "ready\n", true); status != http.StatusOK || ack.Length != 6 || !ack.Ended {
		t.Fatalf("the setup log was answered %d, %+v", status, ack)
	}
	for try := range 50 {
		if status, ack := upload(workerapi.LogStep, try, 0, kilobyte, true); status != http.StatusOK || ack.Length != 1000 || !ack.Ended {
			t.Fatalf("try %d's log was answered %d, %+v", try, status, ack)
		}
	}
	for _, try := range []int{workerapisrv.MaxLogTry, workerapisrv.MaxLogTry + 1} {
		if status, ack := upload(workerapi.LogStep, try, 0, kilobyte, false); status != http.StatusOK || ack.Length != 1000 || ack.Ended {
			t.Errorf("try %d's log was answered %d, %+v", try, status, ack)
		}
	}
	// Past the limit, a log that ended keeps what it has, and is answered
	// as stored.
	if status, ack := upload(workerapi.LogStep, 0, 1000, "more", false); status != http.StatusOK || ack.Length != 1004 {
		t.Errorf("more of a log that ended, past the limit, was answered %d, %+v", status, ack)
	}

	logs := r.logs.Of(a.Run, a.Job, a.Number)
	message := problems.LogTruncated.Message(problems.Params{"limit": int64(1024)}) + "\n"
	want := map[workerapitest.LogKey]string{
		{Part: workerapi.LogSetup}:                             "ready\n",
		{Part: workerapi.LogStep, Try: 0}:                      kilobyte,
		{Part: workerapi.LogStep, Try: 1}:                      kilobyte[:18] + "\n" + message,
		{Part: workerapi.LogStep, Try: 2}:                      message,
		{Part: workerapi.LogStep, Try: 49}:                     message,
		{Part: workerapi.LogStep, Try: workerapisrv.MaxLogTry}: message,
	}
	for key, data := range want {
		if got := logs[key]; string(got.Data) != data || !got.Ended {
			t.Errorf("the %+v log kept is %q, ended %v; want %q", key, got.Data, got.Ended, data)
		}
	}
	if got, ok := logs[workerapitest.LogKey{Part: workerapi.LogStep, Try: workerapisrv.MaxLogTry + 1}]; ok {
		t.Errorf("a try past MaxLogTry kept a log: %q", got.Data)
	}
	kept := 0
	for _, l := range logs {
		kept += len(l.Data)
	}
	if most := 1024 + len(logs)*(len(message)+1); kept > most {
		t.Errorf("the attempt's logs kept %d bytes, more than %d", kept, most)
	}

	// Before the limit, an upload after a log's end is out of order.
	_, _, other := r.claimed(c, placed{})
	if status, _ := uploadAs(t, c, other, workerapi.LogSetup, 0, "done", true); status != http.StatusOK {
		t.Fatalf("the setup log was answered %d", status)
	}
	if status, body := uploadAs(t, c, other, workerapi.LogSetup, 4, "!", false); status != http.StatusConflict {
		t.Errorf("an upload after the log's end was answered %d: %s", status, body)
	}
}

// The limit on an attempt's logs holds over every replica that serves its
// uploads, as after a takeover. Each reserves a part of the limit at a time,
// which the store counts, and spends it as uploads come: what one reserved
// is its own to spend, even once another has run the limit out, and the
// others are given only what the limit has left past it. So the attempt's
// logs keep the limit in all, and no more, beside the line that ends each.
// Each replica answers an upload past the end of a log another ended at
// the limit as stored, whether it holds some of the limit still or none:
// the worker was told the bytes before it were stored.
func TestLogLimitAcrossReplicas(t *testing.T) {
	limits := func(s *workerapisrv.Server) {
		workerapisrv.SetLimits(s, 100, workerapisrv.MaxEvents, workerapisrv.MaxEventsBytes)
		workerapisrv.SetLogReserve(s, 30)
	}
	r := newRig(t, rigOptions{configure: limits})
	first := r.socket
	sockets := make([]string, 3)
	for i := range sockets {
		r.socket = ""
		r.serve(rigOptions{configure: limits})
		sockets[i] = r.socket
	}
	r.socket = first
	one, other, third, fourth := newClient(t, first), newClient(t, sockets[0]), newClient(t, sockets[1]), newClient(t, sockets[2])
	a, _, credential := r.claimed(one, placed{})
	uploads := []struct {
		c           *client
		try, offset int
		data        string
		end         bool
	}{
		// One reserves 30, and spends 10.
		{one, 0, 0, strings.Repeat("a", 10), false},
		// The other reserves 50, and spends them; then of the 50 more
		// it asks, it is given the 20 the limit has left past the 80
		// reserved, and ends the log.
		{other, 0, 10, strings.Repeat("b", 50), false},
		{other, 0, 60, strings.Repeat("c", 50), false},
		// More of that log, past what it stored, through the replica
		// that still holds 20.
		{one, 0, 110, strings.Repeat("g", 200), false},
		// One spends 10 of its 20 on the next log; the third, which
		// holds none, is given none, and ends that log; and one is
		// answered more of it, past what it stored, as stored.
		{one, 1, 0, strings.Repeat("d", 10), false},
		{third, 1, 10, strings.Repeat("e", 5), false},
		{one, 1, 15, strings.Repeat("f", 200), false},
		// One spends the 10 it has left, and is given no more.
		{one, 2, 0, strings.Repeat("h", 30), true},
		{one, 3, 0, "i", true},
		{other, 4, 0, "j", true},
		{third, 5, 0, "k", true},
		// More of the first log, past what it stored, through a replica
		// that has served none of the attempt's uploads.
		{fourth, 0, 310, strings.Repeat("l", 10), false},
	}
	for _, u := range uploads {
		if ack := uploadTry(t, u.c, credential, u.try, u.offset, u.data, u.end); ack.Length != int64(u.offset+len(u.data)) || ack.Ended != u.end {
			t.Errorf("an upload of try %d's log at %d was acknowledged %+v", u.try, u.offset, ack)
		}
	}
	logs := r.logs.Of(a.Run, a.Job, a.Number)
	message := problems.LogTruncated.Message(problems.Params{"limit": int64(100)}) + "\n"
	want := []string{
		strings.Repeat("a", 10) + strings.Repeat("b", 50) + strings.Repeat("c", 20) + "\n" + message,
		strings.Repeat("d", 10) + "\n" + message,
		strings.Repeat("h", 10) + "\n" + message,
		message,
		message,
		message,
	}
	for try, data := range want {
		if got := logs[workerapitest.LogKey{Part: workerapi.LogStep, Try: try}]; string(got.Data) != data || !got.Ended {
			t.Errorf("try %d's log kept %q, ended %v; want %q", try, got.Data, got.Ended, data)
		}
	}
}

// A gap in a log that hasn't ended is answered out of order, so that the
// worker sends the bytes again, even once the store has given out all of
// the limit on the attempt's logs, while a replica still holds some of it:
// only a truncation ends a log at the limit.
func TestLogGapBeforeLimitSpent(t *testing.T) {
	limits := func(s *workerapisrv.Server) {
		workerapisrv.SetLimits(s, 100, workerapisrv.MaxEvents, workerapisrv.MaxEventsBytes)
		workerapisrv.SetLogReserve(s, 30)
	}
	r := newRig(t, rigOptions{configure: limits})
	first := r.socket
	sockets := make([]string, 2)
	for i := range sockets {
		r.socket = ""
		r.serve(rigOptions{configure: limits})
		sockets[i] = r.socket
	}
	r.socket = first
	one, other, third := newClient(t, first), newClient(t, sockets[0]), newClient(t, sockets[1])
	a, _, credential := r.claimed(one, placed{})
	// One holds 20 of the 30 it reserved; the other spends the 50 it
	// reserved; and the third is given 20 of the 30 it asks, and holds 15:
	// the store has given out 110.
	uploadTry(t, one, credential, 0, 0, strings.Repeat("a", 10), false)
	uploadTry(t, other, credential, 0, 10, strings.Repeat("b", 50), false)
	uploadTry(t, third, credential, 1, 0, strings.Repeat("c", 5), false)
	status, body, err := one.send(http.MethodPost, tryPath(0, 100, false), []byte(strings.Repeat("x", 10)), credential)
	if err != nil {
		t.Fatal(err)
	}
	p := wantProblem(t, "an upload past a gap", status, body, workerapi.OutOfOrder)
	if got := fmt.Sprint(p.Params[workerapi.ParamExpected]); got != "60" {
		t.Errorf("an upload past a gap was answered out of order from %s, want 60", got)
	}
	// The worker sends the bytes again, from there, and one keeps the 20
	// it holds.
	if ack := uploadTry(t, one, credential, 0, 60, strings.Repeat("d", 20), false); ack.Length != 80 || ack.Ended {
		t.Errorf("the bytes sent again were acknowledged %+v", ack)
	}
	logs := r.logs.Of(a.Run, a.Job, a.Number)
	if got, want := string(logs[workerapitest.LogKey{Part: workerapi.LogStep}].Data), strings.Repeat("a", 10)+strings.Repeat("b", 50)+strings.Repeat("d", 20); got != want {
		t.Errorf("try 0's log kept %q, want %q", got, want)
	}
}

// A replica reserves the limit on an attempt's logs a part at a time, in
// one write, and spends it as uploads come, with none: two hundred uploads
// of 100 bytes, with 4,096 bytes reserved at a time, take five writes.
func TestLogReservations(t *testing.T) {
	r := newRig(t, rigOptions{configure: func(s *workerapisrv.Server) { workerapisrv.SetLogReserve(s, 4096) }})
	c := newClient(t, r.socket)
	a, _, credential := r.claimed(c, placed{})
	line := strings.Repeat("x", 99) + "\n"
	for i := range 200 {
		uploadTry(t, c, credential, 0, i*len(line), line, false)
	}
	var counted int64
	r.inTx(func(tx *store.Tx) error {
		var err error
		counted, err = tx.AddLogBytes(a.ID, 0)
		return err
	})
	if counted != 5*4096 {
		t.Errorf("the store counted %d bytes reserved; want five reservations of 4,096", counted)
	}
	logs := r.logs.Of(a.Run, a.Job, a.Number)
	if got := logs[workerapitest.LogKey{Part: workerapi.LogStep}]; string(got.Data) != strings.Repeat(line, 200) {
		t.Errorf("the log kept %d bytes; want all 20,000", len(got.Data))
	}
}

// BenchmarkLogUploads uploads a step's log as the worker does a chatty
// step's, a line at a time, each once the last is answered, and reports
// the time each upload takes.
func BenchmarkLogUploads(b *testing.B) {
	r := newRig(b, rigOptions{})
	c := newClient(b, r.socket)
	_, _, credential := r.claimed(c, placed{})
	line := "a line of a chatty step's output\n"
	offset := 0
	for b.Loop() {
		uploadTry(b, c, credential, 0, offset, line, false)
		offset += len(line)
	}
}

// uploadTry uploads data to the log of the try try of the first step of
// the attempt whose credential is credential, at offset, through c, and
// returns its acknowledgement.
func uploadTry(t testing.TB, c *client, credential string, try, offset int, data string, end bool) workerapi.LogAck {
	t.Helper()
	status, body, err := c.send(http.MethodPost, tryPath(try, offset, end), []byte(data), credential)
	if err != nil || status != http.StatusOK {
		t.Fatalf("an upload of try %d's log at %d was answered %d, %v: %s", try, offset, status, err, body)
	}
	var ack workerapi.LogAck
	if err := workerapi.Unmarshal(body, &ack); err != nil {
		t.Fatal(err)
	}
	return ack
}

// tryPath is the path of an upload to the log of the try try of the first
// step, at offset, which ends it if end.
func tryPath(try, offset int, end bool) string {
	return fmt.Sprintf("%s?%s=%s&%s=0&%s=%d&%s=%d&%s=%v", workerapi.PathLogs, workerapi.QueryPart, workerapi.LogStep, workerapi.QueryStep,
		workerapi.QueryTry, try, workerapi.QueryOffset, offset, workerapi.QueryEnd, end)
}

// uploadAs uploads data to the log part of the attempt whose credential is
// credential, at offset, and returns the answer.
func uploadAs(t *testing.T, c *client, credential string, part workerapi.LogPart, offset int, data string, end bool) (int, []byte) {
	t.Helper()
	path := fmt.Sprintf("%s?%s=%s&%s=%d&%s=%v", workerapi.PathLogs, workerapi.QueryPart, part, workerapi.QueryOffset, offset, workerapi.QueryEnd, end)
	status, body, err := c.send(http.MethodPost, path, []byte(data), credential)
	if err != nil {
		t.Fatal(err)
	}
	return status, body
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
