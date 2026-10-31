package login_test

import (
	"errors"
	"os/user"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/internal/login"
	"github.com/parallelworks/meridian/problems"
)

// TestName checks that the login name is the one os/user finds.
func TestName(t *testing.T) {
	u, err := user.Current()
	if err != nil || u.Username == "" {
		t.Skipf("no login name here: %v", err)
	}
	if got, err := login.Name(); err != nil || got != u.Username {
		t.Errorf("Name() = %q, %v, want %q", got, err, u.Username)
	}
}

// TestForPath checks which login names a session's path holds as they are:
// those of letters, digits, ., _, -, @ and ~, as SSSD's user@domain, but .
// and ..; any other, a backslash among them, is session_user_unsupported,
// which names it as it is.
func TestForPath(t *testing.T) {
	for _, name := range []string{"alice", "a.b_c-d", "Bob42", "x", "..a", "a..", "alice@corp.example", "~alice", "a~b@c"} {
		if got, err := login.ForPath(name); err != nil || got != name {
			t.Errorf("ForPath(%q) = %q, %v", name, got, err)
		}
	}
	for _, name := range []string{"", ".", "..", "al ice", "a/b", `DOMAIN\alice`, `a\`, "ålice", "a%2Fb", "a?b", "a#b", "a\nb"} {
		_, err := login.ForPath(name)
		if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.SessionUserUnsupported.Code() || p.Params["user"] != name {
			t.Errorf("ForPath(%q) gave %v", name, err)
		}
	}
}
