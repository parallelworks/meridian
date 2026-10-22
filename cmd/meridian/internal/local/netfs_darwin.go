package local

import (
	"strings"
	"syscall"
)

// mntLocal is the statfs flag of a filesystem stored on a local disk,
// MNT_LOCAL in <sys/mount.h>, which package syscall doesn't name.
const mntLocal = 0x1000

// networkFS says whether path is on a network filesystem, and its type:
// one the system doesn't mark as local, such as nfs, smbfs, afpfs or
// webdav.
func networkFS(path string) (fsType string, network bool, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return "", false, err
	}
	var b strings.Builder
	for _, c := range st.Fstypename {
		if c == 0 {
			break
		}
		b.WriteByte(byte(c))
	}
	return b.String(), st.Flags&mntLocal == 0, nil
}
