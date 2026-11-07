package batch

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/internal/machineid"
	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// RelaysDir is the directory of Driver.Dir that holds the address file of
// each member's listener, by the member's key (Driver.Endpoint). No
// attempt is named so: Place refuses it.
const RelaysDir = "relays"

var _ backend.Forwarder = (*Driver)(nil)

// MemberKey returns m's key: batch- and a keyed hash of the scheduler's
// name and of where m's relay listens, its network and its ports, the
// same for every member of the driver's, whose login node is this
// machine, that listens alike, so that one listener serves them all, and
// a member that listens elsewhere has one of its own.
func (d *Driver) MemberKey(m *backend.Member) (string, error) {
	s, err := d.settings(m)
	if err != nil {
		return "", err
	}
	network := ""
	if s.Relay.Network.IsValid() {
		network = s.Relay.Network.String()
	}
	h, err := d.hash(fmt.Sprintf("member\x00%s\x00%s\x00%d-%d", d.Scheduler.Name(), network, s.Relay.FirstPort, s.Relay.LastPort))
	if err != nil {
		return "", err
	}
	return "batch-" + h, nil
}

// Endpoint returns where m's workers reach the coordinator: over TLS, at
// the addresses of m's address file, which Forward writes in RelaysDir,
// in the home the cluster's nodes share.
func (d *Driver) Endpoint(_ context.Context, m *backend.Member) (backend.Endpoint, error) {
	key, err := d.MemberKey(m)
	if err != nil {
		return backend.Endpoint{}, err
	}
	return backend.Endpoint{AddressFile: d.addressFile(key)}, nil
}

// Connect returns m's endpoint, and nothing to close: the coordinator
// listens there through Forward, as a backend.Forwarder's, and calls no
// Connect of it.
func (d *Driver) Connect(ctx context.Context, m *backend.Member, _ backend.Endpoint) (backend.Endpoint, io.Closer, error) {
	e, err := d.Endpoint(ctx, m)
	return e, nopCloser{}, err
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

// Forward listens for m's workers on this machine's private addresses
// (addresses), each at the same port where it can: one of the member's
// relay ports, or else of the scheduler's port range, or else any; and
// writes them to m's address file, and again every timing.touch while it
// listens, so that meridian batch can tell a listener that is up. Closing
// the listener stops it, and removes the file if it is still the
// listener's own. What the scheduler said of its controllers and its
// ports is kept (listenFacts), so that a process that takes the listener
// over while the scheduler doesn't answer, as meridian resume can, still
// listens.
func (d *Driver) Forward(ctx context.Context, m *backend.Member) (net.Listener, error) {
	d.init()
	s, err := d.settings(m)
	if err != nil {
		return nil, err
	}
	key, err := d.MemberKey(m)
	if err != nil {
		return nil, err
	}
	info, err := d.listenFacts(ctx)
	if err != nil {
		return nil, err
	}
	addrs, err := d.addresses(ctx, m, s, info)
	if err != nil {
		return nil, err
	}
	first, last := s.Relay.FirstPort, s.Relay.LastPort
	if first == 0 {
		first, last = info.FirstPort, info.LastPort
	}
	ls, err := listenAll(ctx, addrs, first, last)
	if err != nil {
		return nil, err
	}
	file := d.addressFile(key)
	if err := ensureDir(filepath.Dir(file)); err != nil {
		closeAll(ls)
		return nil, err
	}
	nonce := make([]byte, 16)
	_, _ = rand.Read(nonce)
	// The name a worker's TLS asks for, which nothing resolves (RFC 6761):
	// a relay tells its workers' connections from others by it.
	l := &addressListener{
		ls: ls, file: file, conns: make(chan net.Conn), done: make(chan struct{}),
		f: workerapi.AddressFile{Name: key + ".meridian.invalid", Nonce: hex.EncodeToString(nonce)},
	}
	for _, x := range ls {
		l.f.Addresses = append(l.f.Addresses, x.Addr().String())
	}
	if err := l.write(); err != nil {
		closeAll(ls)
		return nil, err
	}
	for _, x := range ls {
		go l.accept(x)
	}
	l.touching.Go(func() { l.touch(d.timing.touch) })
	return l, nil
}

// addressFile is the address file of the listener of the members whose
// key is key.
func (d *Driver) addressFile(key string) string {
	return filepath.Join(d.Dir, RelaysDir, key+".json")
}

// factsFile is where the scheduler's controllers and ports, as Info last
// said them, are kept for a listener, in RelaysDir: by the scheduler's
// name, which no member's key is, and a keyed hash of its config and this
// machine's name and ID (machineid), which it knows without asking the
// controller, so that the login nodes of two clusters that share a home
// keep their own, even ones of the same name.
func (d *Driver) factsFile() (string, error) {
	host, _ := os.Hostname()
	// An ID that can't be read is left out, as one that isn't there is:
	// the name still keeps most apart.
	id, _ := machineid.Of(machineid.Files)
	h, err := d.hash("facts\x00" + d.Scheduler.Config() + "\x00" + host + "\x00" + id)
	if err != nil {
		return "", err
	}
	return filepath.Join(d.Dir, RelaysDir, d.Scheduler.Name()+"-"+h+".json"), nil
}

// listenFacts returns what a member's listener needs of the scheduler's
// Info: its controllers, and its ports, which it keeps in factsFile as
// it gets them, and gives as they were kept where the scheduler doesn't
// answer, as while its controller is down, but for a problem, which is
// one with the cluster that a kept answer doesn't hide.
func (d *Driver) listenFacts(ctx context.Context) (*Info, error) {
	file, ferr := d.factsFile()
	if ferr != nil {
		return nil, ferr
	}
	info, err := d.info(ctx)
	if err == nil {
		kept := &Info{Controllers: info.Controllers, FirstPort: info.FirstPort, LastPort: info.LastPort}
		if err := ensureDir(filepath.Dir(file)); err != nil {
			return nil, err
		}
		_ = writeJSON(filepath.Dir(file), filepath.Base(file), kept)
		return info, nil
	}
	if problems.IsProblem(err) {
		return nil, err
	}
	data, rerr := safepath.ReadFile(file, safepath.Private, maxRecord)
	kept := new(Info)
	if rerr != nil || jsonv2.Unmarshal(data, kept) != nil {
		return nil, err
	}
	return kept, nil
}

// addresses returns the addresses m's workers reach this machine at: its
// private ones (IsPrivate), never a loopback, link-local or public one, of
// interfaces that are up and running, within the member's relay network
// where it names one; those that the system routes to one of the
// scheduler's controllers from, or in one's network, first, since the
// compute nodes reach it, where info says them. None is
// scheduler_relay_no_address.
func (d *Driver) addresses(ctx context.Context, m *backend.Member, s *Settings, info *Info) ([]netip.Addr, error) {
	interfaces := d.interfaces
	if interfaces == nil {
		interfaces = InterfaceAddresses
	}
	prefixes, err := interfaces()
	if err != nil {
		return nil, err
	}
	var controllers, sources []netip.Addr
	if info != nil {
		controllers = resolve(ctx, info.Controllers)
		sources = routeSources(ctx, controllers)
	}
	addrs := privateAddresses(prefixes, s.Relay.Network, controllers, sources)
	if len(addrs) == 0 {
		host, _ := os.Hostname()
		network := ""
		if s.Relay.Network.IsValid() {
			network = s.Relay.Network.String()
		}
		return nil, problems.SchedulerRelayNoAddress.Problem(problems.Params{"member": m.Name, "host": host, "network": network, "has_network": network != ""})
	}
	return addrs, nil
}

// InterfaceAddresses returns the addresses of this machine's interfaces
// that are up and running, each with its network's prefix: a bridge that
// is up but has nothing on it, as a container runtime's can be, is no
// way to this machine.
func InterfaceAddresses() ([]netip.Prefix, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var out []netip.Prefix
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagRunning == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			n, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip, ok := netip.AddrFromSlice(n.IP)
			if !ok {
				continue
			}
			ones, bits := n.Mask.Size()
			// An IPv4 address in IPv6's form can have its mask so too.
			if ip = ip.Unmap(); ip.Is4() && bits == 128 {
				ones -= 96
			}
			out = append(out, netip.PrefixFrom(ip, ones))
		}
	}
	return out, nil
}

// sharedAddressSpace is RFC 6598's, which sites use for their own networks
// as they do RFC 1918's.
var sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")

// IsPrivate reports whether a is an address of a network of a site's own:
// RFC 1918's, RFC 6598's or IPv6's unique local ones; never a loopback,
// link-local or public one.
func IsPrivate(a netip.Addr) bool {
	a = a.Unmap()
	return a.IsPrivate() || sharedAddressSpace.Contains(a)
}

// privateAddresses returns the private addresses of prefixes, within
// network where it is valid, without a zone, each once, at most
// workerapi.MaxAddresses of them: those of sources, or whose network holds
// one of controllers, first, and then in the order of prefixes.
func privateAddresses(prefixes []netip.Prefix, network netip.Prefix, controllers, sources []netip.Addr) []netip.Addr {
	var near, far []netip.Addr
	for _, p := range prefixes {
		a := p.Addr()
		switch {
		case !IsPrivate(a), a.Zone() != "", network.IsValid() && !network.Contains(a),
			slices.Contains(near, a), slices.Contains(far, a):
			continue
		}
		if slices.Contains(sources, a) || slices.ContainsFunc(controllers, func(c netip.Addr) bool { return p.Masked().Contains(c) }) {
			near = append(near, a)
		} else {
			far = append(far, a)
		}
	}
	near = append(near, far...)
	return near[:min(len(near), workerapi.MaxAddresses)]
}

// resolveFor bounds the lookup of the scheduler's controllers.
const resolveFor = 5 * time.Second

// resolve returns the addresses of hosts, each an address or a name the
// system's resolver looks up, within resolveFor: one it can't is passed
// by, since the order it gives is only a preference.
func resolve(ctx context.Context, hosts []string) []netip.Addr {
	ctx, cancel := context.WithTimeout(ctx, resolveFor)
	defer cancel()
	var out []netip.Addr
	for _, h := range hosts {
		if a, err := netip.ParseAddr(h); err == nil {
			out = append(out, a.Unmap())
			continue
		}
		addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", h)
		if err != nil {
			continue
		}
		for _, a := range addrs {
			out = append(out, a.Unmap())
		}
	}
	return out
}

// controllerPort is a port of a controller's, which routeSources dials,
// as UDP, which sends nothing: slurmctld's.
const controllerPort = 6817

// routeSources returns this machine's addresses that the system routes to
// each of controllers from, as it says for a UDP socket connected to it,
// which sends nothing: so too where an address's own network, as a /32's,
// holds no controller.
func routeSources(ctx context.Context, controllers []netip.Addr) []netip.Addr {
	var d net.Dialer
	var out []netip.Addr
	for _, c := range controllers {
		conn, err := d.DialContext(ctx, "udp", netip.AddrPortFrom(c, controllerPort).String())
		if err != nil {
			continue
		}
		if a, ok := conn.LocalAddr().(*net.UDPAddr); ok {
			out = append(out, a.AddrPort().Addr().Unmap())
		}
		_ = conn.Close()
	}
	return out
}

// listenAll listens on each of addrs, at the port of the first where it
// is free there too: one from first to last, chosen at random, where they
// say a range, or else one the system chooses. An address it can't
// listen on is passed by; none is an error.
func listenAll(ctx context.Context, addrs []netip.Addr, first, last int) ([]net.Listener, error) {
	var (
		lc   net.ListenConfig
		ls   []net.Listener
		errs []error
		port int
	)
	listen := func(a netip.Addr, port int) (net.Listener, error) {
		return lc.Listen(ctx, "tcp", netip.AddrPortFrom(a, uint16(port)).String()) //nolint:gosec // a port from 0 to 65535
	}
	for _, a := range addrs {
		var l net.Listener
		if port != 0 {
			l, _ = listen(a, port)
		}
		if l == nil {
			var err error
			if l, err = listenIn(a, first, last, listen); err != nil {
				errs = append(errs, err)
				continue
			}
		}
		if port == 0 {
			port = l.Addr().(*net.TCPAddr).Port
		}
		ls = append(ls, l)
	}
	if len(ls) == 0 {
		return nil, fmt.Errorf("batch: listen for the member's workers: %w", errors.Join(errs...))
	}
	return ls, nil
}

// listenIn listens on a at a port from first to last, starting at one
// chosen at random, as another user's listener can hold any of them; or,
// where first is 0, at one the system chooses.
func listenIn(a netip.Addr, first, last int, listen func(netip.Addr, int) (net.Listener, error)) (net.Listener, error) {
	if first == 0 {
		return listen(a, 0)
	}
	n := last - first + 1
	start, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return nil, err
	}
	var lastErr error
	for i := range n {
		l, err := listen(a, first+(int(start.Int64())+i)%n)
		if err == nil {
			return l, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("no port from %d to %d is free on %s: %w", first, last, a, lastErr)
}

func closeAll(ls []net.Listener) {
	for _, l := range ls {
		_ = l.Close()
	}
}

// An addressListener is a member's listener: the connections its
// listeners accept, on each of its addresses, as one listener's, while it
// writes where they listen to its address file.
type addressListener struct {
	ls   []net.Listener
	file string
	// f is the file it writes, whose nonce says it is its own.
	f     workerapi.AddressFile
	conns chan net.Conn
	done  chan struct{}
	// touching is the goroutine that writes the file again.
	touching sync.WaitGroup

	once sync.Once
	// err is why a listener stopped accepting, which ends them all.
	mu  sync.Mutex
	err error
}

// accept passes what l accepts on to Accept, until the listener closes.
// A failure to accept that passes, as for want of a file descriptor, it
// tries again after a while, as http.Server does; any other closes the
// listener, so that the coordinator listens again.
func (l *addressListener) accept(x net.Listener) {
	var wait time.Duration
	for {
		c, err := x.Accept()
		if ne, ok := errors.AsType[net.Error](err); ok && ne.Temporary() { //nolint:staticcheck // as http.Server tells the failures that pass
			wait = min(max(2*wait, 5*time.Millisecond), time.Second)
			select {
			case <-time.After(wait):
				continue
			case <-l.done:
				return
			}
		}
		wait = 0
		if err != nil {
			l.mu.Lock()
			if l.err == nil {
				l.err = err
			}
			l.mu.Unlock()
			_ = l.Close()
			return
		}
		select {
		case l.conns <- c:
		case <-l.done:
			_ = c.Close()
			return
		}
	}
}

// touch writes the address file again every period, until the listener
// closes, and its directory where it is gone: a write that fails is tried
// again at the next. A file another listener wrote since, it leaves be,
// and writes no more. Its read and its write aren't one step: another
// listener's file written between them is written over. That is safe only
// because one listener of a key listens at a time, as the store's lease on
// the member's endpoint, an flock locally, holds while its holder lives; a
// store whose leases can lapse while their holder lives would need the
// file's writers to hold a lock of their own around both, or to write
// only where a rename can't replace another's.
func (l *addressListener) touch(period time.Duration) {
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-l.done:
			return
		case <-t.C:
		}
		if f, ok := l.read(); ok && f.Nonce != l.f.Nonce {
			return
		}
		if err := ensureDir(filepath.Dir(l.file)); err == nil {
			_ = l.write()
		}
	}
}

// read reads the address file, and reports whether it is one.
func (l *addressListener) read() (*workerapi.AddressFile, bool) {
	data, err := safepath.ReadFile(l.file, safepath.Private, workerapi.MaxAddressFile)
	if err != nil {
		return nil, false
	}
	f, err := workerapi.UnmarshalAddressFile(data)
	return f, err == nil
}

// write writes the address file, as now, whole, readable by the user
// alone.
func (l *addressListener) write() error {
	f := l.f
	f.Updated = time.Now().Unix()
	data, err := f.Marshal()
	if err != nil {
		return err
	}
	return safepath.WriteFile(l.file, data)
}

func (l *addressListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.err != nil && !errors.Is(l.err, net.ErrClosed) {
			return nil, l.err
		}
		return nil, net.ErrClosed
	}
}

// Close removes the listener's address file, if it is still its own,
// rather than one another listener wrote since, and then stops it, so that
// no worker reads where it listened once it doesn't.
func (l *addressListener) Close() error {
	l.once.Do(func() {
		close(l.done)
		// No write of the file comes after it is removed.
		l.touching.Wait()
		if f, ok := l.read(); ok && f.Nonce == l.f.Nonce {
			_ = os.Remove(l.file)
		}
		closeAll(l.ls)
	})
	return nil
}

// Addr is the first address listened on.
func (l *addressListener) Addr() net.Addr { return l.ls[0].Addr() }
