//go:build !paramcheck

package problems

// checkDeclared is unset in a build without the paramcheck tag, as every
// binary is: Problem and At check nothing more (paramcheck.go).
const checkDeclared = false
