package coordinator

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/internal/fipsmode"
	"github.com/parallelworks/meridian/problems"
)

// TestOpenRequiresFIPS checks that Open refuses to open a coordinator,
// even one only to read the store, in a process whose cryptography doesn't
// run in FIPS 140-3 mode, enforced, with the validated module, whatever
// program calls it, as worker.Run does.
func TestOpenRequiresFIPS(t *testing.T) {
	checkFIPS = func() error { return fipsmode.Check("on", "v1.0.0") }
	t.Cleanup(func() { checkFIPS = fipsmode.CheckProcess })
	for _, readOnly := range []bool{false, true} {
		c, err := Open(t.Context(), Options{Database: filepath.Join(privateDir(t), "meridian.db"), Keys: newTestKeys(t), ReadOnly: readOnly})
		if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.FIPSNotEnforced.Code() {
			t.Errorf("Open, read-only %t, outside FIPS 140-3 mode = %v, %v", readOnly, c, err)
		}
	}
}
