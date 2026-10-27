package sshconn

import (
	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
)

// command returns the path of a command the Dialer runs, ssh, ssh-keygen,
// or the shell a ProxyCommand runs in, as meridian host finds the commands
// it runs (safepath.LookExecutable): name, as PATH finds it where it has
// no /, absolute and without symbolic links, a regular file of the user's
// or root's that no other user can change, nor any directory on the way to
// it. What it held would run with the user's agent, their keys and their
// hosts' connections at hand. One that isn't safe is
// problems.SSHCommandUnsafe, with the code of the problem with it; one
// that isn't there an error that is exec.ErrNotFound or fs.ErrNotExist.
func command(name string) (string, error) {
	path, err := safepath.LookExecutable(name)
	if p := asProblem(err); p != nil {
		return "", problems.SSHCommandUnsafe.Problem(problems.Params{"path": path, "code": string(p.Code)})
	}
	return path, err
}
