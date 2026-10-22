package local

import (
	"strings"
	"testing"
)

const mountinfo = `22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
40 22 0:35 / /home rw,relatime shared:20 - nfs4 server:/home rw,vers=4.2
41 40 0:36 / /home/u/mnt rw,nosuid shared:21 - fuse.sshfs u@host:/ rw
42 22 0:37 / /scratch\040space rw - beegfs beegfs_nodev rw
43 22 0:38 / /tmp rw - tmpfs tmpfs rw
44 43 0:39 / /tmp rw - fuse.fuse-overlayfs fuse-overlayfs rw
`

func TestMountType(t *testing.T) {
	for path, want := range map[string]string{
		"/":                     "ext4",
		"/etc":                  "ext4",
		"/home":                 "nfs4",
		"/home/u/.local/state":  "nfs4",
		"/homes":                "ext4",
		"/home/u/mnt/state":     "fuse.sshfs",
		"/scratch space/u":      "beegfs",
		"/scratch":              "ext4",
		"/tmp/meridian":         "fuse.fuse-overlayfs", // the later of two mounts at one point
		"/home/u/mntx/anything": "nfs4",
	} {
		got, err := mountType(strings.NewReader(mountinfo), path)
		if err != nil {
			t.Fatalf("mountType(%s): %v", path, err)
		}
		if got != want {
			t.Errorf("mountType(%s) = %s, want %s", path, got, want)
		}
	}
}

func TestMountTypeNone(t *testing.T) {
	if _, err := mountType(strings.NewReader("garbage\n"), "/x"); err == nil {
		t.Error("mountType found a mount in a table without one")
	}
}

func TestUnescapeMount(t *testing.T) {
	for in, want := range map[string]string{
		`/a\040b`:   "/a b",
		`/a\134b`:   `/a\b`,
		`/a\011`:    "/a\t",
		`/plain`:    "/plain",
		`/short\04`: `/short\04`,
		`/bad\999`:  `/bad\999`,
	} {
		if got := unescapeMount(in); got != want {
			t.Errorf("unescapeMount(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNetworkFSLocal(t *testing.T) {
	fsType, network, err := networkFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if network {
		t.Skipf("the test's temporary directory is on a network filesystem, %s", fsType)
	}
}

func TestFUSENetwork(t *testing.T) {
	for fsType, want := range map[string]bool{
		"fuse.sshfs": true, "fuse.glusterfs": true, "fuse.rclone": true, "fuse": true, "fuse.unheard-of": true,
		"fuse.fuse-overlayfs": false, "fuse.gocryptfs": false, "fuseblk": false, "fuse.lxcfs": false,
	} {
		if got := fuseNetwork(fsType); got != want {
			t.Errorf("fuseNetwork(%s) = %t, want %t", fsType, got, want)
		}
	}
}
