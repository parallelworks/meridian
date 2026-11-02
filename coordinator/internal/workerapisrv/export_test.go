package workerapisrv

import (
	"time"
)

// SetReads sets the newest plan version s reads, as a replica of an older
// release reads fewer.
func SetReads(s *Server, version int) { s.reads = version }

// SetSpeaks sets the newest version of the worker API s speaks, as a
// replica of another release speaks another; and SetNeeds the oldest an
// attempt needs its worker to speak, as one of a job that uses what a
// later version adds does.
func SetSpeaks(s *Server, version int) { s.speaks = version }
func SetNeeds(s *Server, version int)  { s.needs = version }

// SetBeforeClaim sets what s calls as it stores a claim, between its
// refusals and its transaction.
func SetBeforeClaim(s *Server, f func()) { s.beforeClaim = f }

// SetPlacedWait sets how long a first claim waits for its placement to be
// recorded, and how often it reads the store again meanwhile.
func SetPlacedWait(s *Server, wait, recheck time.Duration) {
	s.placedWait, s.placedRecheck = wait, recheck
}

// HeldClaims returns how many first claims of the attempt id s holds until
// its placement is recorded.
func HeldClaims(s *Server, id string) int {
	s.placed.Lock()
	defer s.placed.Unlock()
	if w := s.placed.waits[id]; w != nil {
		return w.n
	}
	return 0
}

// SetHold sets how long s holds a control poll.
func SetHold(s *Server, d time.Duration) { s.hold = d }

// SetLimits sets what s keeps of an attempt's reports at most: the bytes
// of its logs, and its events' number and bytes.
func SetLimits(s *Server, logBytes, events, eventsBytes int64) {
	s.maxLogBytes, s.maxEvents, s.maxEventsBytes = logBytes, events, eventsBytes
}

// SetLogReserve sets how many bytes of an attempt's limit on its logs s
// reserves at a time.
func SetLogReserve(s *Server, n int64) { s.logReserve = n }

// SetTimes sets how long s gives a request's headers, and its body beside
// what its size takes, to arrive in.
func SetTimes(s *Server, header, body time.Duration) { s.headerTime, s.bodyTime = header, body }

// SetBeforeEnd sets what s calls as it stores an end, between its checks
// and its transaction.
func SetBeforeEnd(s *Server, f func()) { s.beforeEnd = f }

// HeldPolls returns how many control polls of the attempt id s holds.
func HeldPolls(s *Server, id string) int {
	s.polls.mu.Lock()
	defer s.polls.mu.Unlock()
	if a := s.polls.attempts[id]; a != nil {
		return a.waiting
	}
	return 0
}

// SetRecheck sets how often s reads again the controls of the attempts it
// holds polls for.
func SetRecheck(s *Server, d time.Duration) { s.polls.every = d }

// SetChildRecheck sets how often a poll for a child's end reads the store
// again.
func SetChildRecheck(s *Server, d time.Duration) { s.childRecheck = d }

// SetAfterEndRead sets what s calls as a poll for a child's end has read
// the child.
func SetAfterEndRead(s *Server, f func()) { s.afterEndRead = f }
