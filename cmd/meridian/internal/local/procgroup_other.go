//go:build !unix

package local

import "os/exec"

// killGroup leaves cancelling cmd as it is: the system has no process
// groups that package syscall offers, and cmd's WaitDelay bounds the wait
// for the helpers it started.
func killGroup(*exec.Cmd) {}
