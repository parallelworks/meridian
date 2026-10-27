package main

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"strings"
)

// machineIDFiles are the files a host's ID is in, the first that holds one
// first: systemd's, and D-Bus's, which a system without systemd has.
var machineIDFiles = []string{"/etc/machine-id", "/var/lib/dbus/machine-id"}

// machineID returns the host's own ID: systemd's, or D-Bus's where
// systemd's file isn't there, is empty, or says uninitialized, as it does
// until the system has given it one; or "" where neither has one.
func machineID(context.Context) (string, error) {
	return machineIDOf(machineIDFiles)
}

// machineIDOf returns the ID in the first of files that holds one, or "".
func machineIDOf(files []string) (string, error) {
	for _, path := range files {
		data, err := os.ReadFile(path) //nolint:gosec // the system's machine ID files
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		if id := strings.TrimSpace(string(data)); id != "" && id != "uninitialized" {
			return id, nil
		}
	}
	return "", nil
}
