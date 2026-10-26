// Package runlogtest keeps logs in memory, as runlog.Logs says, for the
// tests of the coordinator and of its server of the worker API.
package runlogtest

import (
	"bytes"
	"context"
	"sync"

	"github.com/parallelworks/meridian/coordinator/internal/runlog"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// Logs keeps logs in memory, as runlog.Logs says. Its zero value has none,
// and is ready to use.
type Logs struct {
	mu   sync.Mutex
	logs map[runlog.Key]*workerapitest.Log
}

var _ runlog.Logs = (*Logs)(nil)

// Append stores data as the part of the log at key from offset, as
// runlog.Logs says.
func (l *Logs) Append(_ context.Context, key runlog.Key, offset int64, data []byte, end bool) (int64, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.logs == nil {
		l.logs = map[runlog.Key]*workerapitest.Log{}
	}
	log := l.logs[key]
	if log == nil {
		log = &workerapitest.Log{}
		l.logs[key] = log
	}
	length := int64(len(log.Data))
	switch {
	case offset > length, log.Ended && offset+int64(len(data)) > length:
		return length, log.Ended, runlog.ErrGap
	case offset+int64(len(data)) > length:
		log.Data = append(log.Data, data[length-offset:]...)
	}
	if end {
		log.Ended = true
	}
	return int64(len(log.Data)), log.Ended, nil
}

// Read returns the part of the log at key from offset, as runlog.Logs
// says.
func (l *Logs) Read(_ context.Context, key runlog.Key, offset int64, limit int) ([]byte, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	log := l.logs[key]
	if log == nil {
		return nil, false, nil
	}
	end := min(int64(len(log.Data)), offset+int64(limit))
	if offset >= end {
		return nil, log.Ended, nil
	}
	return bytes.Clone(log.Data[offset:end]), log.Ended && end == int64(len(log.Data)), nil
}

// Of returns the logs of the attempt number of the run's job, by which of
// its logs each is.
func (l *Logs) Of(run, job string, number int) map[workerapitest.LogKey]workerapitest.Log {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[workerapitest.LogKey]workerapitest.Log{}
	for k, v := range l.logs {
		if k.Run == run && k.Job == job && k.Attempt == number {
			out[workerapitest.LogKey{Part: k.Part, Step: k.Step, Try: k.Try}] = workerapitest.Log{Data: bytes.Clone(v.Data), Ended: v.Ended}
		}
	}
	return out
}
