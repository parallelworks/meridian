//go:build !paramcheck

package problems

import "github.com/parallelworks/foundation/problem"

// checkDeclared is unset in a build without the paramcheck tag, as every
// binary is: Problem and At check nothing more (paramcheck.go).
const checkDeclared = false

// undeclared is never called without the paramcheck tag.
func undeclared(problem.Code, string) {}
