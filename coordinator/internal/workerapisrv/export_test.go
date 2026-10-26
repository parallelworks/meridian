package workerapisrv

import "time"

// SetReads sets the newest plan version s reads, as a replica of an older
// release reads fewer.
func SetReads(s *Server, version int) { s.reads = version }

// SetHold sets how long s holds a control poll.
func SetHold(s *Server, d time.Duration) { s.hold = d }

// SetLimits sets what s keeps of an attempt's reports at most: the bytes
// of a log, and its events' number and bytes.
func SetLimits(s *Server, logBytes, events, eventsBytes int64) {
	s.maxLogBytes, s.maxEvents, s.maxEventsBytes = logBytes, events, eventsBytes
}

// SetTimes sets how long s gives a request's headers, and its body beside
// what its size takes, to arrive in.
func SetTimes(s *Server, header, body time.Duration) { s.headerTime, s.bodyTime = header, body }

// SetBeforeEnd sets what s calls as it stores an end, between its checks
// and its transaction.
func SetBeforeEnd(s *Server, f func()) { s.beforeEnd = f }
