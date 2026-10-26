package local

import (
	"testing"
)

func TestNetworkFSLocal(t *testing.T) {
	fsType, network, err := networkFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if network {
		t.Skipf("the test's temporary directory is on a network filesystem, %s", fsType)
	}
}

// TestNetworkMagic checks that the filesystems Database refuses are those
// the store's SQLite driver refuses: every FUSE one among them, and the
// folders virtual machines share.
func TestNetworkMagic(t *testing.T) {
	for magic, want := range map[int64]string{
		0x6969: "nfs", 0x65735546: "fuse", 0x6a656a63: "virtiofs", 0x786f4256: "vboxsf",
		0x7c7c6673: "prl_fs", 0xbacbacbc: "vmhgfs", 0x0bd00bd0: "lustre",
	} {
		if got := networkMagic[magic]; got != want {
			t.Errorf("the filesystem %#x is %q, want %q", magic, got, want)
		}
	}
	// ext4, xfs, btrfs, tmpfs and overlayfs are local.
	for _, magic := range []int64{0xef53, 0x58465342, 0x9123683e, 0x01021994, 0x794c7630} {
		if name, ok := networkMagic[magic]; ok {
			t.Errorf("the local filesystem %#x is refused, as %s", magic, name)
		}
	}
}
