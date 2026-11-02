//go:build darwin

package sockowner

// rootAccepts isn't set: the owner Owner says is the user who made the
// listener (Trusted).
const rootAccepts = false

// lookup returns the owner of the TCP socket whose end is e, as macOS's
// list of TCP sockets says (darwinOwner).
func lookup(e end) (uint32, error) {
	list, err := sysctl(darwinList)
	if err != nil {
		return 0, err
	}
	return darwinOwner(list, e)
}
