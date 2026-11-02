//go:build !unix

package sockownertest

import (
	"os"
	"testing"
)

// ListenAs skips the test, or fails it, where RootEnv is set: only a Unix
// system starts a process as another user so.
func ListenAs(t *testing.T) int {
	t.Helper()
	if os.Getenv(RootEnv) != "" {
		t.Fatalf("%s is set, but no process starts as another user here", RootEnv)
	}
	t.Skip("no process starts as another user here")
	return 0
}
