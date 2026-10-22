//go:build !unix

package local

import "errors"

func mkfifo(string) error { return errors.New("no FIFOs here") }
