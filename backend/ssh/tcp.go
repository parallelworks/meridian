package ssh

import (
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
// life, where a net.Dialer's bounds only the dial.
//
// host and port come from a workflow, as a session's remoteHost and
// remotePort: host must be a name, of letters, digits, -, _ and dots, or
// an IP address, without brackets, and port from 1 to 65535, or it is
// problems.SSHTCPAddressInvalid, before anything connects. A host whose
// sshd won't connect to the address, as where AllowTcpForwarding,
// DisableForwarding or PermitOpen forbids it, is
// problems.SSHTCPForwardingRefused; one that couldn't, as where nothing
// listens there, is an error that says what it said
// (sshconn.ErrTCPConnectFailed). A connection the pool kept can have closed
// before it noticed: what fails on it for that is tried once more, over
// the connection made again, as the driver's other calls are (closed).
func (d *Driver) DialTCP(ctx context.Context, m *backend.Member, host string, port int) (net.Conn, error) {
	t, err := targetOf(m)
	if err != nil {
		return nil, err
	}
	if !tcpHost(host) || port < 1 || port > 65535 {
		return nil, problems.SSHTCPAddressInvalid.Problem(problems.Params{"value": problems.EscapeControl(host), "port": strconv.Itoa(port)})
	}
	for try := 1; ; try++ {
		conn, err := d.connect(ctx, t)
		if err != nil {
			return nil, err
		}
		c, err := conn.DialTCP(ctx, host, port)
		switch {
		case err == nil:
			return c, nil
		case errors.Is(err, sshconn.ErrTCPForwardingRefused):
			return nil, problems.SSHTCPForwardingRefused.Problem(problems.Params{
				"host": t.Host, "address": net.JoinHostPort(host, strconv.Itoa(port)),
			})
		case errors.Is(err, sshconn.ErrTCPConnectFailed) || try > 1 || ctx.Err() != nil || !closed(conn, err):
			return nil, err
		}
	}
}

// tcpHost reports whether DialTCP asks a host to connect to host: an IP
// address, without brackets or a zone, or a name of at most 253 bytes,
// labels of 1 to 63 letters, digits, - and _ between dots, that doesn't
// start with -. Nothing else, such as a control character or white space,
// reaches the host.
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
