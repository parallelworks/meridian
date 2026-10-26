package workerapisrv

import "time"

// SetReads sets the newest plan version s reads, as a replica of an older
// release reads fewer.
func SetReads(s *Server, version int) { s.reads = version }

// SetHold sets how long s holds a control poll.
func SetHold(s *Server, d time.Duration) { s.hold = d }
