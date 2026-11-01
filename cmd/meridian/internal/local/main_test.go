package local

import (
	"os"
	"testing"

	"github.com/parallelworks/meridian/internal/sockownertest"
)

func TestMain(m *testing.M) {
	// TestSessionDialerRefusesAnotherUser starts this binary again, as
	// another user, to listen.
	sockownertest.Main()
	os.Exit(m.Run())
}
