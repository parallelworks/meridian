package sockowner

import (
	"testing"

	"golang.org/x/sys/unix"
)

// TestDiagConstants checks the constants that NETLINK_SOCK_DIAG's
// requests and answers are made and read with against
// golang.org/x/sys/unix's, where it has them.
func TestDiagConstants(t *testing.T) {
	for _, c := range []struct {
		name      string
		got, want int
	}{
		{"nlmsgHdrLen", nlmsgHdrLen, unix.SizeofNlMsghdr},
		{"nlmsgError", nlmsgError, unix.NLMSG_ERROR},
		{"nlmFRequest", nlmFRequest, unix.NLM_F_REQUEST},
		{"sockDiagByFamily", sockDiagByFamily, unix.SOCK_DIAG_BY_FAMILY},
		{"afInet", afInet, unix.AF_INET},
		{"afInet6", afInet6, unix.AF_INET6},
		{"ipprotoTCP", ipprotoTCP, unix.IPPROTO_TCP},
		{"tcpEstablished", tcpEstablished, unix.BPF_TCP_ESTABLISHED},
		{"tcpSynRecv", tcpSynRecv, unix.BPF_TCP_SYN_RECV},
	} {
		if c.got != c.want {
			t.Errorf("%s is %d, want %d", c.name, c.got, c.want)
		}
	}
}
