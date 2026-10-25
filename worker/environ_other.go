//go:build !darwin

package worker

// environReadable is set where any process of the user can read another's
// environment, however the worker protects itself. Here it can't once the
// worker has hardened itself: Linux's /proc/<pid>/environ follows the
// dumpable flag harden clears, and FreeBSD's kern.proc.env the tracing it
// turns off. Where the worker can't harden itself, it doesn't run.
const environReadable = false
