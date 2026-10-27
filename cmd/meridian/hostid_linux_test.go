package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestMachineID checks which ID a Linux host has: that of the first of its
// files that holds one, systemd's, else D-Bus's, where one that isn't
// there, is empty, or says uninitialized holds none; and none where
// neither does.
func TestMachineID(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	file := func(name, data string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	missing, empty := filepath.Join(dir, "missing"), file("empty", "")
	uninitialized, dbus := file("uninitialized", "uninitialized\n"), file("dbus", "dbus0123\n")
	for _, tc := range []struct {
		files []string
		want  string
	}{
		{[]string{file("systemd", "systemd0123\n"), dbus}, "systemd0123"},
		{[]string{missing, dbus}, "dbus0123"},
		{[]string{empty, dbus}, "dbus0123"},
		{[]string{uninitialized, dbus}, "dbus0123"},
		{[]string{empty, missing}, ""},
	} {
		if got, err := machineIDOf(tc.files); err != nil || got != tc.want {
			t.Errorf("machineIDOf(%q) = %q, %v; want %q", tc.files, got, err, tc.want)
		}
	}
	if len(machineIDFiles) != 2 || machineIDFiles[0] != "/etc/machine-id" {
		t.Errorf("a host's ID is read from %q", machineIDFiles)
	}
}
