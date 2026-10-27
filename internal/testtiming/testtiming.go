// Package testtiming lets a test of the meridian command shorten the
// timing of the coordinators its process opens: their attempts' leases,
// and the periods around them (coordinator.Timing), as a test of a lost
// worker needs to see one lost in seconds rather than minutes.
//
// A release never shortens them. Only a test sets Scale: no package
// outside this module can import this one, and no code of the module's
// but a _test.go file sets it. The meridian command's tests set it, before
// they run a test, from the environment variable EnvScale, which only
// their own TestMain reads, so that the command they start as a process
// of its own runs with it too.
package testtiming

// EnvScale is the environment variable a meridian command's test binary,
// and only one, reads Scale from.
const EnvScale = "MERIDIAN_TEST_TIMING_SCALE"

// Scale multiplies, when it isn't 0, each period of the timing that every
// coordinator the process opens from then on runs with: 0.1 makes a lease
// 6 seconds, and its heartbeats every second. It is from 0.1 to 1.
var Scale float64
