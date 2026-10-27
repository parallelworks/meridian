package main

import (
	"context"

	"golang.org/x/sys/unix"
)

// machineID returns the host's own ID: its UUID, as the kernel keeps it.
func machineID(context.Context) (string, error) {
	return unix.Sysctl("kern.hostuuid")
}
