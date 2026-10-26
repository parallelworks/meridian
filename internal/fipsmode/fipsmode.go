// Package fipsmode says how the process's cryptography runs, and refuses
// to let Meridian run unless it runs in FIPS 140-3 mode, enforced, with a
// validated module.
//
// The meridian binary is built with Go's validated FIPS 140-3 module
// (GOFIPS140=certified), and go.mod's godebug line makes fips140=only its
// default. But neither is assured: a plain go build compiles the module
// from Go's source, which isn't the validated one, and the GODEBUG
// environment variable overrides a default, while a worker can be started
// by hand, or over SSH, with any environment. So the meridian command, and
// the worker, check both before they do anything, as any other entry
// point of Meridian's, such as a coordinator's, must.
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

// Module returns the version of the FIPS 140-3 module the binary was built
// with, as fips140.Version says: latest for one compiled from Go's source,
// which isn't a validated module, rather than the frozen one GOFIPS140
// names.
func Module() string { return fips140.Version() }

// Check returns problems.FIPSNotEnforced for any mode but only, and
// problems.FIPSModuleNotValidated for the module latest; nil otherwise.
func Check(mode, module string) error {
	switch {
	case mode != "only":
		return problems.FIPSNotEnforced.Problem(problems.Params{"mode": mode})
	case module == "latest":
		return problems.FIPSModuleNotValidated.Problem(problems.Params{"version": module})
	}
	return nil
}

// CheckProcess checks the process's own mode and module (Check).
func CheckProcess() error { return Check(Mode(), Module()) }
