package main

import (
	"context"
	"errors"
	"io/fs"
	"os"
)

// machineID returns the host's own ID: systemd's, or D-Bus's where it has
// none.
func machineID(context.Context) (string, error) {
	var err error
	for _, path := range []string{"/etc/machine-id", "/var/lib/dbus/machine-id"} {
		var data []byte
		if data, err = os.ReadFile(path); err == nil { //nolint:gosec // the system's machine ID files
			return string(data), nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
	}
	return "", err
}
