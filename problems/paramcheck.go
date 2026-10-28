//go:build paramcheck

package problems

// checkDeclared is set in a build with the paramcheck tag, as make test
// and make test-race build the tests: Problem and At then panic for a
// problem built without a param its kind declares (mustHaveDeclared).
const checkDeclared = true
