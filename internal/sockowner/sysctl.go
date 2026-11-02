//go:build darwin || freebsd

package sockowner

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// sysctl returns what the sysctl name gives. A list that grew between the
// kernel's saying how long it is and giving it, which it refuses with
// ENOMEM, is asked for again, a few times.
func sysctl(name string) ([]byte, error) {
	var (
		b   []byte
		err error
	)
	for range 4 {
		if b, err = unix.SysctlRaw(name); !errors.Is(err, unix.ENOMEM) {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("sockowner: sysctl %s: %w", name, err)
	}
	return b, nil
}
