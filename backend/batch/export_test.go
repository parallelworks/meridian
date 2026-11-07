package batch

import "time"

// Grace is how long a check waits for a job's records, and Stuck for a
// reason it pends for to clear.
var (
	Grace = defaultTiming.grace
	Stuck = defaultTiming.stuck
)

// SetClock has d count its graces by now.
func SetClock(d *Driver, now func() time.Time) {
	d.init()
	d.timing.now = now
}

// Refresh has d's next check ask the scheduler again.
func Refresh(d *Driver) {
	d.init()
	d.poll.mu.Lock()
	defer d.poll.mu.Unlock()
	d.poll.snap = snapshot{}
}
