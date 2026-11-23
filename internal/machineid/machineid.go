// Package machineid reads a Linux host's own ID, which systemd, or D-Bus
// on a system without systemd, gives it.
package machineid

import (
	"errors"
	"io/fs"
	"os"
	"strings"
)

// Files are the files a Linux host's ID is in, the first that holds one
// first: systemd's, and D-Bus's, which a system without systemd has.
var Files = []string{"/etc/machine-id", "/var/lib/dbus/machine-id"}

// Of returns the ID in the first of files that holds one, or "" where
// none does: one that isn't there, is empty, or says uninitialized, as
// systemd's does until the system has given it one, holds none.
func Of(files []string) (string, error) {
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
