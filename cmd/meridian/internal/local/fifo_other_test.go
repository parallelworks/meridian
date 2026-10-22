//go:build !unix || solaris || illumos

package local

import "errors"

func mkfifo(string) error { return errors.New("no FIFOs here") }
