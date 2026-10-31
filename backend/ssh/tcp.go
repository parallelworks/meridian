package ssh

import (
	"cmp"
	"context"
	"errors"
	"net"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/backend/ssh/internal/sshconn"
	"github.com/parallelworks/meridian/problems"
)

// DialTCP asks m's host to connect to host at port, as ssh -L and ssh -W
// ask it, over the pool's connection to m's host, through the jump host
// m's settings name, if they name one (direct-tcpip). m's host resolves
// host, so a name only it knows, as a cluster's node's, will do.
//
// It returns the connection, which closes once ctx is done, or the
// connection to m's host closes, as well as with its Close: ctx bounds its
// life, where a net.Dialer's bounds only the dial. Its deadlines can't be
// set, as x/crypto/ssh's channels' can't, so ctx is what bounds it.
//
// host and port come from a workflow, as a session's remoteHost and
// remotePort: host must be a name, of letters, digits, -, _ and dots, or
// an IP address, without brackets (tcpHost), and port from 1 to 65535, or
// it is problems.SSHTCPAddressInvalid, before anything connects. A host
// whose sshd won't connect to the address, as where AllowTcpForwarding,
// DisableForwarding or PermitOpen forbids it, or the key's
// no-port-forwarding or restrict in authorized_keys, is
// problems.SSHTCPForwardingRefused; one that couldn't, as where nothing
// listens there, or the name doesn't resolve on the host, or it can't now
// (resource shortage), is problems.SSHTCPConnectFailed, with what it said.
// A connection the pool kept can have closed before it noticed: what
// fails on it for that is tried once more, over the connection made
// again, as the driver's other calls are (closed).
func (d *Driver) DialTCP(ctx context.Context, m *backend.Member, host string, port int) (net.Conn, error) {
	t, err := targetOf(m)
	if err != nil {
		return nil, err
	}
	if !tcpHost(host) || port < 1 || port > 65535 {
		return nil, problems.SSHTCPAddressInvalid.Problem(problems.Params{"value": host, "port": strconv.Itoa(port)})
	}
	address := net.JoinHostPort(host, strconv.Itoa(port))
	for try := 1; ; try++ {
		conn, err := d.connect(ctx, t)
		if err != nil {
			return nil, err
		}
		c, err := conn.DialTCP(ctx, host, port)
		if err == nil {
			return c, nil
		}
		if te, ok := errors.AsType[*sshconn.TCPError](err); ok {
			if te.Refused {
				return nil, problems.SSHTCPForwardingRefused.Problem(problems.Params{"host": t.Host, "address": address})
			}
			return nil, problems.SSHTCPConnectFailed.Problem(problems.Params{"host": t.Host, "address": address, "reason": cmp.Or(te.Said, te.Reason)})
		}
		if try > 1 || ctx.Err() != nil || !closed(conn, err) {
			return nil, err
		}
	}
}

// tcpHost reports whether DialTCP asks a host to connect to host: an IP
// address, without brackets or a zone, or a name of at most 253 bytes,
// labels of 1 to 63 letters, digits, - and _ between dots, that doesn't
// start with -. Nothing else, such as a control character or white space,
// reaches the host. A name the host's resolver reads as an address, as
// 127.1 or 0x7f.1 are, reaches it as one: a list of addresses to allow or
// deny would have to read host as the host's resolver does first.
func tcpHost(host string) bool {
	if net.ParseIP(host) != nil {
		return true
	}
	if host == "" || len(host) > 253 || host[0] == '-' {
		return false
	}
	for label := range strings.SplitSeq(host, ".") {
		if label == "" || len(label) > 63 {
			return false
		}
		for i := range len(label) {
			switch b := label[i]; {
			case 'a' <= b && b <= 'z', 'A' <= b && b <= 'Z', '0' <= b && b <= '9', b == '-', b == '_':
			default:
				return false
			}
		}
	}
	return true
}
