package plan

import (
	"math"
	"regexp"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/problems"
)

// What Ready and Step evaluate of a job and a step: each value at its
// pointer, read into what the ready job or step holds. Each reports what
// doesn't fit its key, and gives the zero value for it.

// target evaluates where job runs: its format 1 ssh:, or else its
// runs-on:. An ssh: that gives null is none, as format 1 runs such a job
// without ssh.
func (e *evaluation) target(job *Job) Target {
	if !job.SSH.IsZero() {
		pointer := at(job.Path, "ssh")
		v, _, ok := e.value(job.SSH, pointer)
		if !ok {
			return Target{}
		}
		if v != nil {
			return Target{Inline: e.inline(v, pointer)}
		}
	}
	if job.RunsOn.IsZero() {
		return Target{}
	}
	pointer := at(job.Path, "runs-on")
	v, read, ok := e.value(job.RunsOn, pointer)
	if !ok {
		return Target{}
	}
	t, ok := ReadTarget(v)
	if !ok {
		e.reportInvalid(problems.RunsOnInvalid, pointer, "", v, read)
	}
	return t
}

// ReadTarget reads a runs-on:, v, as Ready evaluates it: an execution
// group's name, a mapping with execution-group:, one with environment: and
// params:, or a compute target. Null and "" are no target. ok is false for
// any other value, which Ready refuses (runs_on_invalid).
func ReadTarget(v any) (t Target, ok bool) {
	switch v := v.(type) {
	case nil:
		return Target{}, true
	case string:
		return Target{Group: v}, true
	case map[string]any:
		if g, ok := v["execution-group"]; ok {
			name, ok := g.(string)
			return Target{Group: name}, ok && len(v) == 1
		}
		if env, ok := v["environment"]; ok {
			return readEnvironment(env, v)
		}
		if _, ok := v["mode"]; ok {
			return readComputeTarget(v)
		}
	}
	return Target{}, false
}

// readEnvironment reads a runs-on: environment:, env, with the mapping it is
// in, m, which holds its params:.
func readEnvironment(env any, m map[string]any) (Target, bool) {
	for k := range m {
		if k != "environment" && k != "params" {
			return Target{}, false
		}
	}
	e, _ := env.(map[string]any)
	cluster, ok1 := e["cluster"].(string)
	name, ok2 := e["name"].(string)
	params, ok3 := mappingData(m["params"])
	if !ok1 || !ok2 || !ok3 || cluster == "" || name == "" || len(e) != 2 {
		return Target{}, false
	}
	return Target{Environment: &Environment{Cluster: cluster, Name: name, Params: params}}, true
}

// readComputeTarget reads a compute target, as a compute-target input
// gives one.
func readComputeTarget(m map[string]any) (Target, bool) {
	var c ComputeTarget
	ok := true
	text := func(v any, to *string) {
		if v == nil {
			return
		}
		s, isString := v.(string)
		*to, ok = s, ok && isString
	}
	for k, v := range m {
		switch k {
		case "mode":
			text(v, &c.Mode)
		case "targetId":
			text(v, &c.TargetID)
		case "environmentId":
			text(v, &c.EnvironmentID)
		case "workerId":
			text(v, &c.WorkerID)
		case "schedulingParams":
			var isMapping bool
			c.SchedulingParams, isMapping = mappingData(v)
			ok = ok && isMapping
		case "newWorker":
			if v != nil {
				b, isBool := v.(bool)
				c.NewWorker, ok = b, ok && isBool
			}
		default:
			ok = false
		}
	}
	switch c.Mode {
	case "environment", "login", "worker":
	default:
		ok = false
	}
	return Target{ComputeTarget: &c}, ok
}

// mappingData returns v, a mapping or null, as Data.
func mappingData(v any) (Data, bool) {
	switch v := v.(type) {
	case nil:
		return Data{}, true
	case map[string]any:
		return DataOf(v), true
	}
	return Data{}, false
}

// inline reads a format 1 ssh:, v, as evaluated at pointer: the group of
// one member it makes, on the host it names.
func (e *evaluation) inline(v any, pointer string) *Inline {
	m, isMapping := v.(map[string]any)
	if !isMapping {
		e.reportType(pointer, v, expr.Read{}, "object")
		return nil
	}
	return &Inline{Name: "ssh", Backend: "ssh", Isolation: "host", SSH: DataOf(m)}
}

// resources evaluates a job's resources:, v, at pointer.
func (e *evaluation) resources(v Value, pointer string) Resources {
	if v.IsZero() {
		return Resources{}
	}
	out, read, ok := e.value(v, pointer)
	if !ok {
		return Resources{}
	}
	m, isMapping := out.(map[string]any)
	if !isMapping {
		e.reportType(pointer, out, read, "object")
		return Resources{}
	}
	var r Resources
	invalid := func(key string) { e.reportInvalid(problems.ResourcesInvalid, at(pointer, key), key, m[key], read) }
	if r.MilliCPU, ok = ParseCPU(m["cpu"]); !ok {
		invalid("cpu")
	}
	if r.Memory, ok = ParseMemory(m["memory"]); !ok {
		invalid("memory")
	}
	if r.GPU, ok = ParseGPU(m["gpu"]); !ok {
		invalid("gpu")
	}
	return r
}

// The defaults of a job's retry:, the design's.
const (
	defaultJobRetries  = 3
	defaultJobInterval = 60
)

// jobRetry evaluates a job's retry:, r, at pointer.
func (e *evaluation) jobRetry(r *JobRetry, pointer string) *ReadyJobRetry {
	if r == nil {
		return nil
	}
	out := &ReadyJobRetry{
		Reasons:         make([]Reason, len(r.Reasons)),
		MaxRetries:      e.count(r.MaxRetries, at(pointer, "max-retries"), "max-retries", defaultJobRetries),
		IntervalSeconds: e.duration(r.Interval, at(pointer, "interval"), "interval", defaultJobInterval),
	}
	for i, reason := range r.Reasons {
		out.Reasons[i] = Reason(reason)
	}
	return out
}

// duration evaluates a duration, v, at pointer, the value of key, in
// seconds, or def when it is unset, or null, "" or 0 without a unit, as
// format 1 reads one it has none of. A job's or a step's timeout: has none
// by default, so 0s is none too, where format 1 times out at once.
func (e *evaluation) duration(v Value, pointer, key string, def int64) int64 {
	seconds, _ := e.durationOf(v, pointer, key, def, false)
	return seconds
}

// RunTimeout evaluates the workflow's timeout:, v, in scope, as Start
// does: in seconds, or 0 for none, read as a job's timeout: is
// (Plan.Ready), with what evaluating it read, and its problems, at
// /timeout, which show no value made from a private one.
func RunTimeout(v Value, scope expr.Scope) (int64, expr.Read, problems.Diagnostics) {
	e := &evaluation{phase: PhaseStart, scope: scope}
	seconds, read := e.durationOf(v, "/timeout", "timeout", 0, false)
	return seconds, read, e.diags
}

// retryDuration evaluates a step's retry: interval: or timeout:, v, as
// duration does, but for a number with a fraction, which it rounds down,
// as format 1 does: one under a second is def.
func (e *evaluation) retryDuration(v Value, pointer, key string, def int64) int64 {
	seconds, _ := e.durationOf(v, pointer, key, def, true)
	return seconds
}

// durationOf is duration, which rounds a number with a fraction down when
// round is set, and gives what evaluating v read too.
func (e *evaluation) durationOf(v Value, pointer, key string, def int64, round bool) (int64, expr.Read) {
	if v.IsZero() {
		return def, expr.Read{}
	}
	out, read, ok := e.value(v, pointer)
	if !ok {
		return 0, read
	}
	if f, ok := out.(float64); ok && round && f >= 0 && f < 1<<63 {
		out = math.Trunc(f)
	}
	if out == nil || out == "" || bareZero(out) {
		return def, read
	}
	seconds, ok := ParseDuration(out)
	if !ok {
		e.reportInvalid(problems.DurationInvalid, pointer, key, out, read)
	}
	return seconds, read
}

// cleanOnRetry evaluates a step's retry: clean-on-retry:, v, at pointer:
// true or false, or text or a number that reads as one (readBool), and
// true, format 1's default, for anything else, such as null, as format 1
// reads it, and when it is unset. The compiler gives true for a retry:
// without one.
func (e *evaluation) cleanOnRetry(v Value, pointer string) bool {
	if v.IsZero() {
		return true
	}
	out, _, ok := e.value(v, pointer)
	return ok && readBool(out, true)
}

// count evaluates how many times something may run again, v, at pointer,
// the value of key, or def when it is unset, or null or "".
func (e *evaluation) count(v Value, pointer, key string, def int) int {
	if v.IsZero() {
		return def
	}
	out, read, ok := e.value(v, pointer)
	if !ok {
		return 0
	}
	if out == nil || out == "" {
		return def
	}
	n, ok := readCount(out)
	if !ok {
		e.reportInvalid(problems.CountInvalid, pointer, key, out, read)
	}
	return n
}

// A workingDirectory is a working-directory: as evaluated: its text, and
// whether it is null, which a step's is to opt out of its job's, with
// where it is and what it read.
type workingDirectory struct {
	dir     string
	null    bool
	pointer string
	read    expr.Read
}

// directory evaluates a working-directory:, v, at pointer.
func (e *evaluation) directory(v Value, pointer string) workingDirectory {
	if v.IsZero() {
		return workingDirectory{}
	}
	out, read, ok := e.value(v, pointer)
	if !ok {
		return workingDirectory{}
	}
	switch out := out.(type) {
	case nil:
		return workingDirectory{null: true}
	case string:
		return workingDirectory{dir: out, pointer: pointer, read: read}
	}
	e.reportType(pointer, out, read, "string", "null")
	return workingDirectory{}
}

// shellVariable is a shell variable as a shell would expand it, in what
// isn't a shell's: $NAME or ${NAME}.
var shellVariable = regexp.MustCompile(`\$\{[^}]*\}?|\$[A-Za-z_][A-Za-z0-9_]*`)

// checkDirectory reports a working directory that keeps a shell variable,
// which would be a directory of that name, as format 1 fails the step for
// one.
func (e *evaluation) checkDirectory(d workingDirectory) {
	if v := shellVariable.FindString(d.dir); v != "" {
		e.report(problems.WorkingDirectoryVar, d.pointer, problems.Params{"variable": v}, d.read)
	}
}
