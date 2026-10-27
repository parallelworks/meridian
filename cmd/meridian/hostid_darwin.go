package main

import (
	"context"
	"errors"
	"os/exec"
	"regexp"
)

// platformUUID is the line of ioreg's that holds the host's UUID.
var platformUUID = regexp.MustCompile(`"IOPlatformUUID" = "([0-9A-Fa-f-]+)"`)

// machineID returns the host's own ID: its hardware's UUID, as ioreg says
// it.
func machineID(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(ctx, "ioreg", "-rd1", "-c", "IOPlatformExpertDevice").Output()
	if err != nil {
		return "", err
	}
	m := platformUUID.FindSubmatch(out)
	if m == nil {
		return "", errors.New("host: ioreg gave no IOPlatformUUID")
	}
	return string(m[1]), nil
}
