//go:build freebsd

package sockowner

// rootAccepts isn't set: the owner Owner says is the user who made the
// listener (Trusted).
const rootAccepts = false

// lookup returns the owner of the TCP socket whose end is e, as FreeBSD's
// list of TCP sockets says (freebsdOwner).
func lookup(e end) (uint32, error) {
	list, err := sysctl(freebsdList)
	if err != nil {
		return 0, err
	}
	return freebsdOwner(list, e)
}
