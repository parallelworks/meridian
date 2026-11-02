//go:build !linux && !darwin && !freebsd

package sockowner

// rootAccepts isn't set: Owner says no owner here (Trusted).
const rootAccepts = false

// lookup returns ErrUnsupported: Meridian asks who owns a connection's
// other end only on Linux, macOS and FreeBSD, where alone it coordinates.
func lookup(end) (uint32, error) { return 0, ErrUnsupported }
