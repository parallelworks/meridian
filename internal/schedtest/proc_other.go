//go:build !unix

package schedtest

import "os/exec"

// The fake runs jobs only where Slurm does.

func detach(*exec.Cmd) {}

func group(*exec.Cmd) {}

func killGroup(int, bool) {}

func terminate(int) {}

func signalNumber(*exec.ExitError) int { return 0 }
