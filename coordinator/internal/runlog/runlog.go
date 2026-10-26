// Package runlog names the coordinator's store of step logs, which the
// coordinator's package gives as coordinator.Logs, coordinator.LogKey and
// coordinator.ErrLogGap: one definition, which the coordinator and the
// server of the worker API, which the coordinator imports, both use.
package runlog

import (
	"context"
	"errors"

	"github.com/parallelworks/meridian/workerapi"
)

// ErrGap is what Logs.Append returns for an offset past a log's end, or
// for bytes past the end of a log that has ended.
var ErrGap = errors.New("coordinator: log write past its end")

// Logs stores logs, which workers upload in parts, and reads them. Each
// attempt of a job, and each try of a step, keeps its own logs. A log
// arrives masked: the worker masks secrets before they leave its host.
type Logs interface {
	// Append stores data as the part of the log at key that starts at
	// offset, and returns the log's length after it, and whether it has
	// ended. It skips the bytes before the log's end, which the log has
	// already, so an upload repeated after a dropped connection is stored
	// once. end says that data ends the log, whose length is final from
	// then on. An offset past the log's end, or bytes past the end of a log
	// that has ended, return the length and ErrGap.
	Append(ctx context.Context, key Key, offset int64, data []byte, end bool) (length int64, ended bool, err error)
	// Read returns the part of the log at key that starts at offset, at
	// most limit bytes, and whether the log has ended. At the log's end it
	// returns no bytes, at once.
	Read(ctx context.Context, key Key, offset int64, limit int) (data []byte, ended bool, err error)
}

// A Key names one log.
type Key struct {
	Run string
	// Job is the job instance's key, as in build#2, and Attempt the
	// attempt's number, from 1.
	Job     string
	Attempt int
	// Part is which of the attempt's logs it is, and Step and Try the
	// step's index and its try, for a step's and a cleanup's.
	Part workerapi.LogPart
	Step int
	Try  int
}
