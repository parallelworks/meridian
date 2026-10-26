package local

import (
	"syscall"
)

// networkMagic names the filesystems, by their statfs magic numbers, on
// which SQLite's locks, and the shared memory its write-ahead log needs,
// aren't reliable: network and cluster filesystems, whose files other
// machines change; the folders a virtual machine shares with its host,
// Docker Desktop's bind mounts among them; and FUSE, which sshfs, s3fs,
// GlusterFS and many others are. They are the filesystems the store's
// SQLite driver refuses, so that Database refuses them first, and says
// how to choose a local one.
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
	// The folders virtual machines share with their hosts.
	0x6a656a63: "virtiofs",
	0x786f4256: "vboxsf",
	0x7c7c6673: "prl_fs",
	0xbacbacbc: "vmhgfs",
	// FUSE, whether its files are on this machine's disks or not.
	0x65735546: "fuse",
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
	name, network := networkMagic[magic]
	return name, network, nil
}
