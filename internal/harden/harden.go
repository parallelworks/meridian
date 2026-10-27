// Package harden keeps the user's other processes, a job's steps among
// them, out of a Meridian process's memory, where it holds what they
// mustn't read: a worker's attempt credential, or a coordinator's runs'
// secret records.
//
// It can on Linux, macOS and FreeBSD. Other systems, such as NetBSD and
// OpenBSD, give a process no way to keep the user's other processes from
// tracing it, and Process returns ErrUnsupported there: a caller refuses
// to run, rather than run with less protection.
package harden

import "errors"

// ErrUnsupported is what Process returns on a system where a process
// can't keep the user's other processes out of its memory.
var ErrUnsupported = errors.New("harden: the system gives a process no way to keep the user's other processes out of its memory")
