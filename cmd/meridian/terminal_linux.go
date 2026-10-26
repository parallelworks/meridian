//go:build linux

package main

import "golang.org/x/sys/unix"

// getTermios is the ioctl that reads a terminal's settings.
const getTermios = unix.TCGETS
