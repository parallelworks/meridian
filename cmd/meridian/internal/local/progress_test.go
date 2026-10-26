package local

import (
	"bytes"
	jsonv2 "encoding/json/v2"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

var progressAt = time.Date(2026, 10, 22, 14, 2, 7, 0, time.Local)

// progressEvents are a run's events, as Follow reports them.
var progressEvents = []coordinator.Event{
	{Seq: 1, Time: progressAt, Kind: coordinator.RunStarted, Run: "r1"},
	{Seq: 2, Time: progressAt, Kind: coordinator.JobReady, Run: "r1", Job: "build#2"},
	{Seq: 3, Time: progressAt, Kind: coordinator.JobPlacing, Run: "r1", Job: "build#2", Attempt: 1, Member: "local/host"},
	{Seq: 4, Time: progressAt, Kind: coordinator.AttemptEnded, Run: "r1", Job: "build#2", Attempt: 1, Member: "local/host", End: coordinator.AttemptLost, Reason: plan.ReasonWorkerLost},
	// Placed where its job runs is private, which isn't shown.
	{Seq: 5, Time: progressAt, Kind: coordinator.JobPlacing, Run: "r1", Job: "build#2", Attempt: 2},
	{Seq: 6, Time: progressAt, Kind: coordinator.JobRunning, Run: "r1", Job: "build#2", Attempt: 2},
	{Seq: 7, Time: progressAt, Kind: coordinator.StepStarted, Run: "r1", Job: "build#2", Attempt: 2, Step: 0},
	{
		Seq: 8, Time: progressAt, Kind: coordinator.StepFinished, Run: "r1", Job: "build#2", Attempt: 2, Step: 0, Outcome: plan.OutcomeFailure, ExitCode: 3,
		Diagnostics: problems.Diagnostics{problems.LogTruncated.At("", problems.Position{}, "", problems.Params{"limit": 10})},
	},
	{Seq: 9, Time: progressAt, Kind: coordinator.StepStarted, Run: "r1", Job: "build#2", Attempt: 2, Step: 0, Try: 1},
	{Seq: 10, Time: progressAt, Kind: coordinator.StepFinished, Run: "r1", Job: "build#2", Attempt: 2, Step: 0, Try: 1, Outcome: plan.OutcomeSuccess},
	{Seq: 11, Time: progressAt, Kind: coordinator.StepFinished, Run: "r1", Job: "build#2", Attempt: 2, Step: 1, Cleanup: true, Outcome: plan.OutcomeCancelled, ExitCode: -1, Reason: plan.ReasonTimeout},
	{Seq: 12, Time: progressAt, Kind: coordinator.AttemptEnded, Run: "r1", Job: "build#2", Attempt: 2, End: coordinator.AttemptCompleted, Outcome: plan.OutcomeSuccess},
	{Seq: 13, Time: progressAt, Kind: coordinator.JobFinished, Run: "r1", Job: "build#2", Outcome: plan.OutcomeSuccess},
	{Seq: 14, Time: progressAt, Kind: coordinator.RunFinished, Run: "r1", Outcome: plan.OutcomeSuccess},
}

// TestProgressText checks the lines a person reads of a run: an event a
// line, but the run's start and end, which the command writes, and an
// attempt's end that completed it; a member that is private isn't shown;
// and each problem goes to errs, as compilers write them.
func TestProgressText(t *testing.T) {
	var out, errs strings.Builder
	p := NewProgress(&out, &errs, false)
	if err := p.Created("r1", "/tmp/r1"); err != nil {
		t.Fatal(err)
	}
	for _, e := range progressEvents {
		if err := p.Event(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Left("r1"); err != nil {
		t.Fatal(err)
	}
	want := `run r1 started (run directory /tmp/r1)
14:02:07 build#2 ready
14:02:07 build#2 attempt 1 placing on local/host
14:02:07 build#2 attempt 1 ended: lost (worker-lost)
14:02:07 build#2 attempt 2 placing
14:02:07 build#2 attempt 2 running
14:02:07 build#2 attempt 2 step 0 started
14:02:07 build#2 attempt 2 step 0 finished: failure (exit status 3)
14:02:07 build#2 attempt 2 step 0 retry 1 started
14:02:07 build#2 attempt 2 step 0 retry 1 finished: success (exit status 0)
14:02:07 build#2 attempt 2 step 1 cleanup finished: cancelled (timeout)
14:02:07 build#2 finished: success
run r1 left running: meridian resume r1 picks it up, and meridian cancel r1 cancels it
`
	if out.String() != want {
		t.Errorf("Progress wrote\n%s\nwant\n%s", out.String(), want)
	}
	if want := "meridian: warning: The coordinator keeps at most 10 bytes of an attempt's logs, so it didn't keep the rest of this one. (log_truncated)\n"; errs.String() != want {
		t.Errorf("Progress wrote to errs %q, want %q", errs.String(), want)
	}
}

// logParts are parts of a run's logs, as Follow reports them: a step's,
// in parts that break its lines, then ended; a cleanup's, of a retry, with
// a last line that has no newline, cut short; and a setup log.
func logParts() []coordinator.LogPart {
	step := coordinator.LogKey{Run: "r1", Job: "main", Attempt: 1, Part: workerapi.LogStep, Step: 2}
	cleanup := coordinator.LogKey{Run: "r1", Job: "main", Attempt: 2, Part: workerapi.LogCleanup, Step: 0, Try: 1}
	setup := coordinator.LogKey{Run: "r1", Job: "main", Attempt: 1, Part: workerapi.LogSetup}
	return []coordinator.LogPart{
		{Key: step, Data: []byte("hel")},
		{Key: setup, Data: []byte("claimed\n")},
		{Key: step, Data: []byte("lo\nwor")},
		{Key: cleanup, Data: []byte("one\ntwo")},
		{Key: step, Data: []byte("ld\n")},
		{Key: step, Ended: true},
		{Key: setup, Ended: true},
		{Key: cleanup, Cut: true},
	}
}

// TestProgressLogs checks that a person reads each line of a step's log
// once whole, after which log it is, and that a log cut short is said to
// be; and nothing of a setup log.
func TestProgressLogs(t *testing.T) {
	var out, errs strings.Builder
	p := NewProgress(&out, &errs, false)
	for _, part := range logParts() {
		if err := p.Log(part); err != nil {
			t.Fatal(err)
		}
	}
	want := "main step 2| hello\nmain attempt 2 step 0 cleanup retry 1| one\nmain step 2| world\nmain attempt 2 step 0 cleanup retry 1| two\n"
	if out.String() != want {
		t.Errorf("Progress wrote\n%q\nwant\n%q", out.String(), want)
	}
	if !strings.Contains(errs.String(), "main attempt 2 step 0 cleanup retry 1") || !strings.HasSuffix(errs.String(), "(log_cut)\n") {
		t.Errorf("Progress wrote to errs %q", errs.String())
	}

	// A line longer than Progress holds is written in parts.
	out.Reset()
	key := coordinator.LogKey{Run: "r1", Job: "main", Attempt: 1, Part: workerapi.LogStep}
	if err := p.Log(coordinator.LogPart{Key: key, Data: bytes.Repeat([]byte("x"), maxLine*2+1)}); err != nil {
		t.Fatal(err)
	}
	if err := p.Log(coordinator.LogPart{Key: key, Ended: true}); err != nil {
		t.Fatal(err)
	}
	if lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n"); len(lines) != 3 || len(lines[2]) != len("main step 0| x") {
		t.Errorf("a long line was written as %d lines", len(lines))
	}
}

// TestProgressJSON checks the JSON lines: an object a line, of the
// version, for each event, the run's start and end among them, each with
// what it has; for each line of a log, setup logs too, with the log's end
// on its last; for the run's creation; and for a run left running. A log's
// bytes that aren't UTF-8 are replaced.
func TestProgressJSON(t *testing.T) {
	var out, errs strings.Builder
	p := NewProgress(&out, &errs, true)
	if err := p.Created("r1", "/tmp/r1"); err != nil {
		t.Fatal(err)
	}
	for _, e := range progressEvents {
		if err := p.Event(e); err != nil {
			t.Fatal(err)
		}
	}
	for _, part := range append(logParts(), coordinator.LogPart{Key: coordinator.LogKey{Run: "r1", Job: "bad", Attempt: 1, Part: workerapi.LogStep}, Data: []byte("\xff\n"), Ended: true}) {
		if err := p.Log(part); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Left("r1"); err != nil {
		t.Fatal(err)
	}
	if errs.Len() > 0 {
		t.Errorf("as JSON, Progress wrote to errs %q", errs.String())
	}
	type jsonLine struct {
		Version     int                  `json:"version"`
		Kind        string               `json:"kind"`
		Seq         int64                `json:"seq"`
		Time        time.Time            `json:"time"`
		Run         string               `json:"run"`
		RunDir      string               `json:"runDir"`
		Job         string               `json:"job"`
		Attempt     int                  `json:"attempt"`
		Member      string               `json:"member"`
		Part        string               `json:"part"`
		Step        *int                 `json:"step"`
		Try         int                  `json:"try"`
		Cleanup     bool                 `json:"cleanup"`
		End         string               `json:"end"`
		Outcome     string               `json:"outcome"`
		Reason      string               `json:"reason"`
		ExitCode    *int                 `json:"exitCode"`
		Text        string               `json:"text"`
		Ended       bool                 `json:"ended"`
		Cut         bool                 `json:"cut"`
		Diagnostics problems.Diagnostics `json:"diagnostics"`
	}
	var lines []jsonLine
	for l := range strings.SplitSeq(strings.TrimSuffix(out.String(), "\n"), "\n") {
		var j jsonLine
		if err := jsonv2.Unmarshal([]byte(l), &j, jsonv2.RejectUnknownMembers(true)); err != nil {
			t.Fatalf("the line %s: %v", l, err)
		}
		if j.Version != ProgressVersion || j.Run != "r1" {
			t.Errorf("the line %s", l)
		}
		lines = append(lines, j)
	}
	if n := 1 + len(progressEvents) + 9 + 1; len(lines) != n {
		t.Fatalf("Progress wrote %d lines, want %d:\n%s", len(lines), n, out.String())
	}
	if l := lines[0]; l.Kind != "run.created" || l.RunDir != "/tmp/r1" {
		t.Errorf("the run's creation is %+v", l)
	}
	for i, e := range progressEvents {
		l := lines[1+i]
		if l.Kind != string(e.Kind) || l.Seq != e.Seq || !l.Time.Equal(e.Time) || l.Job != e.Job || l.Attempt != e.Attempt || l.Member != e.Member ||
			l.End != string(e.End) || l.Outcome != string(e.Outcome) || l.Reason != string(e.Reason) || len(l.Diagnostics) != len(e.Diagnostics) {
			t.Errorf("the event %+v is %+v", e, l)
		}
		step := e.Kind == coordinator.StepStarted || e.Kind == coordinator.StepFinished
		if (l.Step != nil) != step || step && (*l.Step != e.Step || l.Try != e.Try || l.Cleanup != e.Cleanup) {
			t.Errorf("the step of the event %+v is %+v", e, l)
		}
		if exit := e.Kind == coordinator.StepFinished && e.ExitCode >= 0; (l.ExitCode != nil) != exit || exit && *l.ExitCode != e.ExitCode {
			t.Errorf("the exit status of the event %+v is %v", e, l.ExitCode)
		}
	}
	logs := map[string]string{}
	ends := map[string]string{}
	for _, l := range lines[1+len(progressEvents) : len(lines)-1] {
		if l.Kind != "log" {
			t.Errorf("the line %+v isn't a log's", l)
			continue
		}
		name := l.Job + " " + l.Part
		if (l.Step == nil) != (l.Part == "setup") {
			t.Errorf("the log line %+v", l)
		}
		logs[name] += l.Text
		if l.Ended {
			ends[name] += "ended"
		}
		if l.Cut {
			ends[name] += "cut"
		}
	}
	wantLogs := map[string]string{"main step": "hello\nworld\n", "main setup": "claimed\n", "main cleanup": "one\ntwo", "bad step": "\uFFFD\n"}
	wantEnds := map[string]string{"main step": "ended", "main setup": "ended", "main cleanup": "cut", "bad step": "ended"}
	for name := range wantLogs {
		if logs[name] != wantLogs[name] || ends[name] != wantEnds[name] {
			t.Errorf("the log %s is %q, %q; want %q, %q", name, logs[name], ends[name], wantLogs[name], wantEnds[name])
		}
	}
	if l := lines[len(lines)-1]; l.Kind != "run.left" {
		t.Errorf("the last line is %+v", l)
	}
}
