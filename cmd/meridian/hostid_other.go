//go:build !linux && !darwin && !freebsd

package main

import (
	"context"
	"errors"
)

// machineID returns an error: meridian host runs where the worker does, on
// Linux, macOS and FreeBSD.
func machineID(context.Context) (string, error) {
	return "", errors.New("host: no machine ID on this system")
}
