package worker

// environReadable is set where any process of the user can read another's
// environment, however the worker protects itself: on macOS, sysctl's
// kern.procargs2, which ps -E reads, gives the arguments and environment
// of every process of the same user, and PT_DENY_ATTACH doesn't stop it. A
// bootstrap token there is one another process could read and claim the
// attempt with, so the worker takes one only from a file, which it removes
// at once (workerapi.EnvBootstrapTokenFile).
const environReadable = true
