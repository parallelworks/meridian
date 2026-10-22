//go:build unix && !solaris && !illumos

package local

import "syscall"

func mkfifo(path string) error { return syscall.Mkfifo(path, 0o600) }
