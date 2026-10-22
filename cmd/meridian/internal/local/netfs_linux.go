package local

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// networkMagic names the filesystems, by their statfs magic numbers, whose
// files other machines change: network and cluster filesystems, on which
// SQLite's locks aren't reliable.
var networkMagic = map[int64]string{
	0x6969:     "nfs",
	0x517b:     "smb",
	0xff534d42: "cifs",
	0xfe534d42: "smb2",
	0x73757245: "coda",
	0x5346414f: "afs",
	0x6b414653: "afs",
	0x564c:     "ncp",
	0x00c36400: "ceph",
	0x01021997: "9p",
	0x47504653: "gpfs",
	0x0bd00bd0: "lustre",
	0x19830326: "beegfs",
	0xaad7aaea: "panfs",
	0x20030528: "orangefs",
	// Cluster filesystems, on disks that other machines write to.
	0x7461636f: "ocfs2",
	0x01161970: "gfs2",
}

// fuseMagic is FUSE's statfs magic number. A FUSE filesystem can be a
// local one, such as fuse-overlayfs, or a network one, such as sshfs, so
// its subtype, from the mount table, decides: one that isn't known to be
// local is taken to be a network one.
const fuseMagic = 0x65735546

// localFUSE are the subtypes of FUSE filesystems known to keep their files
// on this machine's disks.
var localFUSE = map[string]bool{
	"fuse-overlayfs": true, "gocryptfs": true, "encfs": true, "cryfs": true, "securefs": true,
	"squashfuse": true, "squashfuse_ll": true, "bindfs": true, "lxcfs": true, "ntfs-3g": true,
	"fuseblk": true, "apfs-fuse": true,
}

// networkFS says whether path is on a network filesystem, and its type.
func networkFS(path string) (fsType string, network bool, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return "", false, err
	}
	magic := int64(st.Type) //nolint:unconvert // Type is int64 on some architectures and int32 on others
	if magic < 0 {
		magic &= 0xffffffff
	}
	if name, ok := networkMagic[magic]; ok {
		return name, true, nil
	}
	if magic != fuseMagic {
		return "", false, nil
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", false, err
	}
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", false, err
	}
	defer f.Close()
	fsType, err = mountType(f, resolved)
	if err != nil {
		return "", false, err
	}
	return fsType, fuseNetwork(fsType), nil
}

// fuseNetwork says whether a FUSE filesystem of type fsType, as the mount
// table writes it, as fuse.sshfs or fuseblk, is a network one: one that
// isn't known to be local.
func fuseNetwork(fsType string) bool {
	sub, _ := strings.CutPrefix(fsType, "fuse.")
	return !localFUSE[sub]
}

// mountType returns the type of the filesystem mounted where path, an
// absolute path without symbolic links, is, from mountinfo, which is as
// /proc/self/mountinfo writes it: the type of the last mount whose mount
// point is the longest that holds path.
func mountType(mountinfo io.Reader, path string) (string, error) {
	var best, bestType string
	found := false
	sc := bufio.NewScanner(mountinfo)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		// 36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw
		before, after, ok := strings.Cut(sc.Text(), " - ")
		fields, rest := strings.Fields(before), strings.Fields(after)
		if !ok || len(fields) < 5 || len(rest) < 1 {
			continue
		}
		point := unescapeMount(fields[4])
		if !within(path, point) || (found && len(point) < len(best)) {
			continue
		}
		best, bestType, found = point, rest[0], true
	}
	if err := sc.Err(); err != nil {
		return "", err
	}
	if !found {
		return "", errNoMount
	}
	return bestType, nil
}

// within says whether path is dir or under it.
func within(path, dir string) bool {
	if dir == "/" {
		return strings.HasPrefix(path, "/")
	}
	rest, ok := strings.CutPrefix(path, dir)
	return ok && (rest == "" || rest[0] == '/')
}

// unescapeMount undoes the octal escapes the mount table writes a space, a
// tab, a newline and a backslash in a path with, as \040.
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+4 <= len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
