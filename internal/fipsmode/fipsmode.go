// Package fipsmode says how the process's cryptography runs, and refuses
// to let Meridian run unless FIPS 140-3 mode is enforced.
//
// The meridian binary is built with Go's validated FIPS 140-3 module, and
// go.mod's godebug line makes fips140=only its default. But a default is
// all it is: the GODEBUG environment variable overrides it, and a worker
// can be started by hand, or over SSH, with any environment. So the
// meridian command, and the worker, check the process's mode before they
// do anything.
package fipsmode

import (
	"crypto/fips140"

	"github.com/parallelworks/meridian/problems"
)

// Mode returns how the process's cryptography runs, as GODEBUG's fips140
// says: only, FIPS 140-3 mode enforced, so that an algorithm the module
// doesn't approve is an error; on, FIPS 140-3 mode without enforcing it;
// or off.
func Mode() string {
	switch {
	case fips140.Enabled() && fips140.Enforced():
		return "only"
	case fips140.Enabled():
		return "on"
	default:
		return "off"
	}
}

// Check returns nil for the mode only, and problems.FIPSNotEnforced for
// any other.
func Check(mode string) error {
	if mode == "only" {
		return nil
	}
	return problems.FIPSNotEnforced.Problem(problems.Params{"mode": mode})
}
