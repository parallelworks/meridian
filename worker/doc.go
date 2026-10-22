// Package worker is meridian worker: the process that runs one attempt of
// a job where its backend placed it, and reports on it to the coordinator
// over the worker API (package workerapi; docs/design.md, Workers).
//
// Run claims the attempt with the bootstrap token its backend started it
// with, and a credential it makes, which is the attempt's from then on. It
// runs the job's steps in order, each as package plan's Step phase readies
// it, then their cleanup: commands, in reverse order, and evaluates the
// job's outputs with the End phase. It reports each step's start and end
// as events, numbered, and its output as logs, masked before they leave
// the host, then the attempt's end.
//
// # Steps
//
// A step's run: command runs in its shell, bash by default, in its working
// directory, the run's by default, in a process group of its own, with the
// worker's environment, the workflow's over it, and the variables format 1
// defines over that, such as OUTPUTS and PW_JOB_DIR. The steps without an
// id share the job's $OUTPUTS file, as in format 1, and one with an id has
// its own. A step's retry: runs it again on the same worker, as format 1
// does, and its timeout:, its tries' and the job's are the worker's to
// enforce: it sends a step past one SIGTERM, then SIGKILL after a grace
// period. A cancellation from the coordinator stops the job the same way,
// and a step whose early-cancel: is any-job-failed stops when another job
// of the run fails.
//
// A step that uses: an action or a workflow, or runs on another host with
// ssh:, fails: this worker doesn't run them yet. A job whose steps are to
// run in a container fails before its first step, with a problem that says
// so, rather than run them on the host: a job never gets weaker isolation
// than it asks for.
//
// # The coordinator
//
// Every request is safe to repeat, and the worker repeats one whose answer
// it didn't get, or that the coordinator asks it to, until it gets one.
// While it can't reach the coordinator, it keeps running the job's steps,
// and spools their logs to disk up to a limit, and their events, to send
// in order when it can. Before the first step that runs starts, it waits
// until the coordinator has stored that it is starting, so that whether
// the workflow's code may have run is always known.
//
// The worker renews the attempt's lease with heartbeats, and polls for
// what the coordinator asks of it. When the coordinator says that the
// attempt has ended without it, superseded by another, or no longer
// accepts its credential, it stops the job's steps, without their cleanup:
// commands, and exits. A job whose retry: lists worker-lost can have a new
// attempt while this one still runs, cut off, so its worker fences itself:
// it stops the job once it hasn't renewed the lease for a lease period,
// counted on its monotonic clock.
package worker
