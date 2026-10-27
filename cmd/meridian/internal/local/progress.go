package local

import (
	"bytes"
	"cmp"
	jsonv2 "encoding/json/v2"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// ProgressVersion is the version of the JSON lines Progress writes, which
// each line's version member gives: a reader of one version reads its
// members as it documents them, and a later version can add members, and
// kinds of line, but changes what none of them means.
const ProgressVersion = 1

// maxLine is the most bytes of a log's line Progress holds while it waits
// for the line's end: a longer line it writes in parts.
const maxLine = 64 << 10

// Progress is the command's progress display: it writes a run's events,
// and its logs, as coordinator.Follow reports them, as lines a person
// reads, such as
//
//	14:02:07 build#2 attempt 1 running on local/host
//	build#2 step 0| compiling
//
// or as JSON, one object a line (ProgressVersion). A log's lines are each
// written once whole, prefixed with what they are the log of, as the job's
// step 0's, or its retry 1's, or its cleanup's; its setup log, of how its
// worker started, only as JSON. What a run's problems are, and a log cut
// short, a person reads on errs, as compilers write them.
type Progress struct {
	out, errs io.Writer
	json      bool
	// lines are the logs' parts that don't end a line yet, by log.
	lines map[coordinator.LogKey][]byte
}

// NewProgress returns a Progress that writes to out, as JSON when json is
// set, and what a person reads of problems to errs.
func NewProgress(out, errs io.Writer, json bool) *Progress {
	return &Progress{out: out, errs: errs, json: json, lines: map[coordinator.LogKey][]byte{}}
}

// A progressLine is one of the JSON lines Progress writes: an event, as
// coordinator.Event says, the creation of a run, a run left running, or a
// part of a log, with what each has.
type progressLine struct {
	Version  int       `json:"version"`
	Kind     string    `json:"kind"`
	Seq      int64     `json:"seq,omitzero"`
	Time     time.Time `json:"time,omitzero"`
	Run      string    `json:"run"`
	RunDir   string    `json:"runDir,omitempty"`
	Job      string    `json:"job,omitempty"`
	Attempt  int       `json:"attempt,omitzero"`
	Member   string    `json:"member,omitempty"`
	Part     string    `json:"part,omitempty"`
	Step     *int      `json:"step,omitempty"`
	Try      int       `json:"try,omitzero"`
	Cleanup  bool      `json:"cleanup,omitzero"`
	End      string    `json:"end,omitempty"`
	Outcome  string    `json:"outcome,omitempty"`
	Reason   string    `json:"reason,omitempty"`
	ExitCode *int      `json:"exitCode,omitempty"`
	// Text is a log's line, with the newline that ends it, if one does,
	// and its bytes that aren't UTF-8 replaced. Ended is set on a log's
	// last line, once the log has ended, and Cut instead once its attempt
	// ended before the log did: either can have no text.
	Text        string               `json:"text,omitempty"`
	Ended       bool                 `json:"ended,omitzero"`
	Cut         bool                 `json:"cut,omitzero"`
	Diagnostics problems.Diagnostics `json:"diagnostics,omitempty"`
}

// The kinds of JSON line Progress writes besides the events'.
const (
	kindRunCreated = "run.created"
	kindRunResumed = "run.resumed"
	kindRunLeft    = "run.left"
	kindLog        = "log"
)

// Created writes that the run id is stored, with its directory dir.
func (p *Progress) Created(id, dir string) error {
	if p.json {
		return p.writeJSON(progressLine{Kind: kindRunCreated, Run: id, RunDir: dir})
	}
	_, err := fmt.Fprintf(p.out, "run %s started (run directory %s)\n", id, dir)
	return err
}

// Resumed writes that the command has taken up the run id, whose directory
// is dir, as meridian resume does.
func (p *Progress) Resumed(id, dir string) error {
	if p.json {
		return p.writeJSON(progressLine{Kind: kindRunResumed, Run: id, RunDir: dir})
	}
	_, err := fmt.Fprintf(p.out, "run %s resumed (run directory %s)\n", id, dir)
	return err
}

// Flush writes what Progress holds of each log's line that hasn't ended,
// as the last of what meridian logs shows of a log that goes on.
func (p *Progress) Flush() error {
	for _, key := range slices.SortedFunc(maps.Keys(p.lines), compareKeys) {
		if held := p.lines[key]; len(held) > 0 {
			if err := p.writeLog(key, held, false, false); err != nil {
				return err
			}
		}
		delete(p.lines, key)
	}
	return nil
}

// compareKeys orders logs by their job, attempt, which of the attempt's
// logs each is, step and try.
func compareKeys(a, b coordinator.LogKey) int {
	return cmp.Or(cmp.Compare(a.Job, b.Job), cmp.Compare(a.Attempt, b.Attempt), cmp.Compare(a.Part, b.Part),
		cmp.Compare(a.Step, b.Step), cmp.Compare(a.Try, b.Try))
}

// Left writes that the command leaves the run id running, and how to pick
// it up.
func (p *Progress) Left(id string) error {
	if p.json {
		return p.writeJSON(progressLine{Kind: kindRunLeft, Run: id})
	}
	_, err := fmt.Fprintf(p.out, "run %s left running: meridian resume %s picks it up, and meridian cancel %s cancels it\n", id, id, id)
	return err
}

// Event writes e: as JSON, every event; and as text, each but the run's
// start, and its end, which the command writes itself, and an attempt's
// end that completed it, which its job's end says.
func (p *Progress) Event(e coordinator.Event) error {
	if p.json {
		l := progressLine{
			Kind: string(e.Kind), Seq: e.Seq, Time: e.Time, Run: e.Run, Job: e.Job, Attempt: e.Attempt, Member: e.Member,
			End: string(e.End), Outcome: string(e.Outcome), Reason: string(e.Reason), Diagnostics: e.Diagnostics,
		}
		if e.Kind == coordinator.StepStarted || e.Kind == coordinator.StepFinished {
			l.Step, l.Try, l.Cleanup = &e.Step, e.Try, e.Cleanup
		}
		if e.Kind == coordinator.StepFinished && e.ExitCode >= 0 {
			l.ExitCode = &e.ExitCode
		}
		return p.writeJSON(l)
	}
	switch {
	case e.Kind == coordinator.RunStarted || e.Kind == coordinator.RunFinished:
		return nil
	case e.Kind == coordinator.AttemptEnded && e.End == coordinator.AttemptCompleted:
		return p.writeDiagnostics(e.Diagnostics)
	}
	if _, err := fmt.Fprintln(p.out, line(e)); err != nil {
		return err
	}
	return p.writeDiagnostics(e.Diagnostics)
}

// Log writes the lines of part of a log that it ends, and, once the log
// has ended or is cut short, the rest; and that it was cut short, which
// a person reads on errs.
func (p *Progress) Log(part coordinator.LogPart) error {
	key := part.Key
	held := p.lines[key]
	held = append(held, part.Data...)
	for {
		i := bytes.IndexByte(held, '\n')
		if i < 0 && len(held) <= maxLine {
			break
		}
		n := i + 1
		if i < 0 {
			n = maxLine
		}
		if err := p.writeLog(key, held[:n], false, false); err != nil {
			return err
		}
		held = held[n:]
	}
	if !part.Ended && !part.Cut {
		p.lines[key] = bytes.Clone(held)
		return nil
	}
	delete(p.lines, key)
	if len(held) > 0 || p.json {
		if err := p.writeLog(key, held, part.Ended, part.Cut); err != nil {
			return err
		}
	}
	if part.Cut && !p.json {
		d := problems.LogCut.At("", problems.Position{}, "", problems.Params{"log": logName(key)})
		_, err := fmt.Fprintln(p.errs, "meridian: "+d.String())
		return err
	}
	return nil
}

// writeLog writes text, a line of the log key, or its last bytes, which
// ended, or were cut short, when they are.
func (p *Progress) writeLog(key coordinator.LogKey, text []byte, ended, cut bool) error {
	if p.json {
		l := progressLine{
			Kind: kindLog, Run: key.Run, Job: key.Job, Attempt: key.Attempt, Part: string(key.Part),
			Text: strings.ToValidUTF8(string(text), "�"), Ended: ended, Cut: cut,
		}
		if key.Part != workerapi.LogSetup {
			step := key.Step
			l.Step, l.Try = &step, key.Try
		}
		return p.writeJSON(l)
	}
	if key.Part == workerapi.LogSetup {
		return nil
	}
	if !bytes.HasSuffix(text, []byte("\n")) {
		text = append(bytes.Clone(text), '\n')
	}
	_, err := fmt.Fprintf(p.out, "%s| %s", logName(key), text)
	return err
}

// logName names the log key as a person reads it: its job's key, its
// attempt's number after the first, and which of the attempt's logs it is,
// as in build#2 step 0 retry 1.
func logName(key coordinator.LogKey) string {
	var b strings.Builder
	b.WriteString(key.Job)
	if key.Attempt > 1 {
		fmt.Fprintf(&b, " attempt %d", key.Attempt)
	}
	if key.Part == workerapi.LogSetup {
		b.WriteString(" setup")
		return b.String()
	}
	fmt.Fprintf(&b, " step %d", key.Step)
	if key.Part == workerapi.LogCleanup {
		b.WriteString(" cleanup")
	}
	if key.Try > 0 {
		fmt.Fprintf(&b, " retry %d", key.Try)
	}
	return b.String()
}

// writeDiagnostics writes diags, as compilers write them, to errs, each
// about the run as a whole said to be meridian's.
func (p *Progress) writeDiagnostics(diags problems.Diagnostics) error {
	for _, d := range diags {
		line := d.String()
		if d.File() == "" {
			line = "meridian: " + line
		}
		if _, err := fmt.Fprintln(p.errs, line); err != nil {
			return err
		}
	}
	return nil
}

func (p *Progress) writeJSON(l progressLine) error {
	l.Version = ProgressVersion
	data, err := jsonv2.Marshal(l)
	if err != nil {
		return err
	}
	_, err = p.out.Write(append(data, '\n'))
	return err
}

// line is the line Progress shows for e.
func line(e coordinator.Event) string {
	var b strings.Builder
	if !e.Time.IsZero() {
		b.WriteString(e.Time.In(time.Local).Format(time.TimeOnly))
		b.WriteByte(' ')
	}
	attempt := func(what string) {
		fmt.Fprintf(&b, "%s attempt %d %s", e.Job, e.Attempt, what)
	}
	switch e.Kind {
	case coordinator.JobReady:
		fmt.Fprintf(&b, "%s ready", e.Job)
	case coordinator.JobPlacing:
		attempt("placing")
		if e.Member != "" {
			fmt.Fprintf(&b, " on %s", e.Member)
		}
	case coordinator.JobRunning:
		attempt("running")
		if e.Member != "" {
			fmt.Fprintf(&b, " on %s", e.Member)
		}
	case coordinator.AttemptEnded:
		attempt("ended: " + string(e.End))
	case coordinator.JobFinished:
		fmt.Fprintf(&b, "%s finished: %s", e.Job, e.Outcome)
	case coordinator.StepStarted, coordinator.StepFinished:
		b.WriteString(logName(coordinator.LogKey{Job: e.Job, Attempt: e.Attempt, Part: stepPart(e.Cleanup), Step: e.Step, Try: e.Try}))
		if e.Kind == coordinator.StepStarted {
			b.WriteString(" started")
			break
		}
		fmt.Fprintf(&b, " finished: %s", e.Outcome)
		if e.ExitCode >= 0 {
			fmt.Fprintf(&b, " (exit status %d)", e.ExitCode)
		}
	default:
		fmt.Fprintf(&b, "%s %s", e.Kind, e.Job)
	}
	if e.Reason != "" {
		fmt.Fprintf(&b, " (%s)", e.Reason)
	}
	return b.String()
}

// stepPart is the log of a step's event: its cleanup's, or its own.
func stepPart(cleanup bool) workerapi.LogPart {
	if cleanup {
		return workerapi.LogCleanup
	}
	return workerapi.LogStep
}
