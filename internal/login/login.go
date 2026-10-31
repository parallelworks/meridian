// Package login gives the login name of the user who runs Meridian, which
// a step sees as format 1's PW_USER, and which a session's path holds,
// /me/session/<user>/<name>/: one name, from one place, so that the path
// workflows build from PW_USER is the path Meridian serves, byte for byte.
package login

import (
	"errors"
	"os/user"

	"github.com/parallelworks/meridian/problems"
)

// errNoName is Name's error where it finds no login name.
var errNoName = errors.New("login: no login name for the user who runs the process")

// Name returns the login name of the user who runs the process, as os/user
// finds it: the user's entry in the system's user database, or else $USER,
// as os/user falls back to without cgo; or an error where it finds none.
func Name() (string, error) {
	u, err := user.Current()
	switch {
	case err != nil:
		return "", err
	case u.Username == "":
		return "", errNoName
	}
	return u.Username, nil
}

// ForPath returns name as a session's path holds it, as it is: workflows
// build the path from PW_USER, which is the name as it is, so the path
// can't escape it. A URL's path holds a name as it is where it is a
// segment of letters, digits, ., _, -, @ and ~, as user@domain, which
// SSSD gives, and isn't . or .. (PathSafe): any other name is
// problems.SessionUserUnsupported, rather than one escaped, which the
// path a workflow builds wouldn't match.
func ForPath(name string) (string, error) {
	if !PathSafe(name) {
		return "", problems.SessionUserUnsupported.Problem(problems.Params{"user": name})
	}
	return name, nil
}

// PathSafe reports whether s is a segment that a URL's path holds as it
// is, which neither a browser nor Go escapes there: of POSIX's portable
// filename characters, letters, digits, ., _ and -, and @ and ~, and not
// . or .., which a path's dot segments would remove. A backslash, which a
// browser reads as a slash, is none.
func PathSafe(s string) bool {
	if s == "" || s == "." || s == ".." {
		return false
	}
	for i := range len(s) {
		switch b := s[i]; {
		case 'a' <= b && b <= 'z', 'A' <= b && b <= 'Z', '0' <= b && b <= '9', b == '.', b == '_', b == '-', b == '@', b == '~':
		default:
			return false
		}
	}
	return true
}
