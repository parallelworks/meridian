//go:build paramcheck

package problems

import (
	"fmt"
	"os"
	"runtime/debug"

	"github.com/parallelworks/foundation/problem"
)

// checkDeclared is set in a build with the paramcheck tag, as make test
// and make test-race build the tests: Problem and At then end the test
// binary for a problem built without a param its kind declares
// (mustHaveDeclared).
const checkDeclared = true

// undeclared ends the test binary at once, with exit status 2, for a
// problem of the kind code built without its param name: it writes them,
// and the stack of the call site that built it, to standard error. An exit,
// unlike a panic, is caught by no recover: not by a hopper job's, which
// would try the job again and fail it with no sign of why, nor by a
// net/http handler's, whose client would try again until it timed out.
func undeclared(code problem.Code, name string) {
	fmt.Fprintf(os.Stderr, "problems: a %s problem built without its param %q, which foundation would send as invalid\n%s", code, name, debug.Stack())
	os.Exit(2)
}
