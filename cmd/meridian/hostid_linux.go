package main

import (
	"context"

	"github.com/parallelworks/meridian/internal/machineid"
)

// machineID returns the host's own ID: systemd's, or D-Bus's where
// systemd's file isn't there, is empty, or says uninitialized, as it does
// until the system has given it one; or "" where neither has one.
func machineID(context.Context) (string, error) {
	return machineid.Of(machineid.Files)
}
