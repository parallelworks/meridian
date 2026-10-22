package plan

import (
	"github.com/parallelworks/meridian/problems"
)

// What Ready and Step evaluate of a job and a step: each value at its
// pointer, read into what the ready job or step holds. Each reports what
// doesn't fit its key, and gives the zero value for it.

// target evaluates where job runs: its format 1 ssh:, or else its
// runs-on:.
func (e *evaluation) target(job *Job) Target {
	if !job.SSH.IsZero() {
		return Target{Inline: e.inline(job.SSH, at(job.Path, "ssh"))}
	}
	if job.RunsOn.IsZero() {
		return Target{}
	}
	pointer := at(job.Path, "runs-on")
	v, read, ok := e.value(job.RunsOn, pointer)
	if !ok {
		return Target{}
	}
	t, ok := readTarget(v)
	if !ok {
		e.reportInvalid(problems.RunsOnInvalid, pointer, "", v, read)
	}
	return t
}

// readTarget reads a runs-on:: an execution group's name, a mapping with
// execution-group:, one with environment: and params:, or a compute
// target. Null and "" are no target.
func readTarget(v any) (Target, bool) {
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

// inline evaluates a format 1 ssh:, v, at pointer: the group of one member
// it makes, on the host it names.
func (e *evaluation) inline(v Value, pointer string) *Inline {
	out, read, ok := e.value(v, pointer)
	if !ok {
		return nil
	}
	m, isMapping := out.(map[string]any)
	if !isMapping {
		e.reportType(pointer, out, read, "object")
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
	if r.MilliCPU, ok = readCPU(m["cpu"]); !ok {
		invalid("cpu")
	}
	if r.Memory, ok = readMemory(m["memory"]); !ok {
		invalid("memory")
	}
	if r.GPU, ok = readGPU(m["gpu"]); !ok {
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
// seconds, or def when it is unset.
func (e *evaluation) duration(v Value, pointer, key string, def int64) int64 {
	if v.IsZero() {
		return def
	}
	out, read, ok := e.value(v, pointer)
	if !ok {
		return 0
	}
	seconds, ok := readDuration(out)
	if !ok {
		e.reportInvalid(problems.DurationInvalid, pointer, key, out, read)
	}
	return seconds
}

// count evaluates how many times something may run again, v, at pointer,
// the value of key, or def when it is unset.
func (e *evaluation) count(v Value, pointer, key string, def int) int {
	if v.IsZero() {
		return def
	}
	out, read, ok := e.value(v, pointer)
	if !ok {
		return 0
	}
	if out == nil {
		return def
	}
	n, ok := readCount(out)
	if !ok {
		e.reportInvalid(problems.CountInvalid, pointer, key, out, read)
	}
	return n
}

// directory evaluates a working-directory:, v, at pointer: its text, and
// whether it is null, which a step's is to opt out of its job's.
func (e *evaluation) directory(v Value, pointer string) (dir string, null bool) {
	if v.IsZero() {
		return "", false
	}
	out, read, ok := e.value(v, pointer)
	if !ok {
		return "", false
	}
	switch out := out.(type) {
	case nil:
		return "", true
	case string:
		return out, false
	}
	e.reportType(pointer, out, read, "string", "null")
	return "", false
}
