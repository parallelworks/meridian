package batch

import (
	"encoding/json/jsontext"
	"errors"
	"io/fs"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// TestPrivateAddresses checks which of a machine's addresses a member's
// workers reach it at: its private ones, RFC 1918's, RFC 6598's and IPv6's
// unique local ones, each once, never a loopback, link-local, public or
// zoned one, within a network where one is named, those the system routes
// to a controller from, or in a controller's network, first, at most
// workerapi.MaxAddresses.
func TestPrivateAddresses(t *testing.T) {
	t.Parallel()
	prefixes := func(ps ...string) []netip.Prefix {
		var out []netip.Prefix
		for _, p := range ps {
			out = append(out, netip.MustParsePrefix(p))
		}
		return out
	}
	addrs := func(as ...string) []netip.Addr {
		var out []netip.Addr
		for _, a := range as {
			out = append(out, netip.MustParseAddr(a))
		}
		return out
	}
	machine := prefixes("127.0.0.1/8", "::1/128", "169.254.1.1/16", "fe80::1/64", "203.0.113.4/24", "2001:db8::4/64", "100.128.0.1/16",
		"172.17.0.1/16", "10.1.0.5/16", "fd00:1::5/64", "10.1.0.5/16", "192.168.4.2/24", "100.72.0.3/10", "10.9.9.9/32")
	all := addrs("172.17.0.1", "10.1.0.5", "fd00:1::5", "192.168.4.2", "100.72.0.3", "10.9.9.9")
	for _, tc := range []struct {
		name                 string
		network              string
		controllers, sources []netip.Addr
		want                 []netip.Addr
	}{
		{"every private one", "", nil, nil, all},
		{"the controller's network first", "", addrs("10.1.9.9"), nil, addrs("10.1.0.5", "172.17.0.1", "fd00:1::5", "192.168.4.2", "100.72.0.3", "10.9.9.9")},
		{"a controller outside them", "", addrs("10.2.0.1"), nil, all},
		// A /32's network holds no controller, but the system routes to
		// one from it.
		{"the route to the controller first", "", addrs("10.2.0.1"), addrs("10.9.9.9"), addrs("10.9.9.9", "172.17.0.1", "10.1.0.5", "fd00:1::5", "192.168.4.2", "100.72.0.3")},
		{"a network", "10.0.0.0/8", addrs("192.168.4.1"), nil, addrs("10.1.0.5", "10.9.9.9")},
		{"an IPv6 network", "fd00::/8", nil, nil, addrs("fd00:1::5")},
		{"RFC 6598's", "100.64.0.0/10", nil, nil, addrs("100.72.0.3")},
		{"a network none is in", "10.200.0.0/16", nil, nil, nil},
	} {
		var network netip.Prefix
		if tc.network != "" {
			network = netip.MustParsePrefix(tc.network)
		}
		if got := privateAddresses(machine, network, tc.controllers, tc.sources); !slices.Equal(got, tc.want) {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
	var many []netip.Prefix
	for i := range 40 {
		many = append(many, netip.PrefixFrom(netip.AddrFrom4([4]byte{10, 0, 0, byte(i + 1)}), 24))
	}
	if got := privateAddresses(many, netip.Prefix{}, nil, nil); len(got) != workerapi.MaxAddresses {
		t.Errorf("of 40 addresses, %d", len(got))
	}
}

// machineAddress returns a private address of this machine's, with its
// network's prefix, or skips the test where it has none, but in CI.
func machineAddress(t *testing.T) netip.Prefix {
	t.Helper()
	all, err := InterfaceAddresses()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range all {
		if a := privateAddresses([]netip.Prefix{p}, netip.Prefix{}, nil, nil); len(a) == 1 {
			return p
		}
	}
	if os.Getenv("CI") != "" {
		t.Fatal("this machine has no private address to listen on")
	}
	t.Skip("this machine has no private address to listen on")
	return netip.Prefix{}
}

// TestForward checks a member's listener: on this machine's private
// address, at a port of the member's relay ports, or of the scheduler's
// range, or any, whose address file, in the home the cluster's nodes
// share, says where, is the user's alone, is written again while it
// listens, and goes with it, but for one that another listener has written
// since.
func TestForward(t *testing.T) {
	t.Parallel()
	own := machineAddress(t)
	base := 47000 + int(time.Now().UnixNano()%1000)
	for _, tc := range []struct {
		name string
		// scheduler and member are the first ports of the scheduler's
		// range and of the member's, each of 21, or 0; first is the one
		// listened in.
		scheduler, member, first int
	}{
		{"any port", 0, 0, 0},
		{"the scheduler's range", base, 0, base},
		{"the member's range", base, base + 1000, base + 1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFake()
			if tc.scheduler != 0 {
				f.info.FirstPort, f.info.LastPort = tc.scheduler, tc.scheduler+20
			}
			if tc.member != 0 {
				f.relay = &Relay{FirstPort: tc.member, LastPort: tc.member + 20}
			}
			d, _ := newDriver(t, f)
			d.interfaces = func() ([]netip.Prefix, error) {
				return []netip.Prefix{netip.MustParsePrefix("127.0.0.1/8"), netip.MustParsePrefix("203.0.113.4/24"), own}, nil
			}
			d.timing.touch = 20 * time.Millisecond
			key, err := d.MemberKey(testMember)
			if err != nil || !regexp.MustCompile(`^batch-[0-9a-f]{32}$`).MatchString(key) {
				t.Fatalf("the member's key is %q, %v", key, err)
			}
			e, err := d.Endpoint(t.Context(), testMember)
			if err != nil || e.AddressFile != filepath.Join(d.Dir, RelaysDir, key+".json") || e.Address != "" || !e.TLS() {
				t.Fatalf("the endpoint is %+v, %v", e, err)
			}
			l, err := d.Forward(t.Context(), testMember)
			if err != nil {
				t.Fatal(err)
			}
			read := func() *workerapi.AddressFile {
				t.Helper()
				data, err := safepath.ReadFile(e.AddressFile, safepath.Private, workerapi.MaxAddressFile)
				if err != nil {
					t.Fatal(err)
				}
				af, err := workerapi.UnmarshalAddressFile(data)
				if err != nil {
					t.Fatal(err)
				}
				return af
			}
			af := read()
			ap, err := netip.ParseAddrPort(af.Addresses[0])
			if err != nil || len(af.Addresses) != 1 || ap.Addr() != own.Addr() || af.Name != key+".meridian.invalid" || af.Updated == 0 {
				t.Fatalf("the address file says %+v", af)
			}
			if port := int(ap.Port()); tc.first != 0 && (port < tc.first || port > tc.first+20) {
				t.Errorf("the port %d is outside the range %d-%d", port, tc.first, tc.first+20)
			}
			for path, mode := range map[string]fs.FileMode{e.AddressFile: 0o600, filepath.Dir(e.AddressFile): fs.ModeDir | 0o700} {
				if info, err := os.Lstat(path); err != nil || info.Mode() != mode {
					t.Errorf("%s: %v, %v, want %v", path, info.Mode(), err, mode)
				}
			}
			// What connects to the address, the listener accepts.
			accepted := make(chan net.Conn, 1)
			go func() {
				c, err := l.Accept()
				if err == nil {
					accepted <- c
				}
			}()
			c, err := net.Dial("tcp", af.Addresses[0])
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			select {
			case got := <-accepted:
				got.Close()
			case <-time.After(10 * time.Second):
				t.Fatal("the listener didn't accept the connection")
			}
			// It writes the file again while it listens.
			before, err := os.Stat(e.AddressFile)
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool {
				info, err := os.Stat(e.AddressFile)
				return err == nil && !os.SameFile(info, before)
			})
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := l.Accept(); !errors.Is(err, net.ErrClosed) {
				t.Errorf("Accept once closed: %v", err)
			}
			if _, err := os.Lstat(e.AddressFile); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("the address file of the listener closed: %v", err)
			}
			// One that another listener wrote since stays.
			d.timing.touch = time.Hour
			l, err = d.Forward(t.Context(), testMember)
			if err != nil {
				t.Fatal(err)
			}
			other := workerapi.AddressFile{Addresses: []string{"10.0.0.9:1"}, Name: "other.meridian.invalid", Updated: 1}
			data, err := other.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			if err := safepath.WriteFile(e.AddressFile, data); err != nil {
				t.Fatal(err)
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			if got := read(); !slices.Equal(got.Addresses, other.Addresses) {
				t.Errorf("another listener's file is %+v", got)
			}
		})
	}
}

// TestForwardOwnFile checks that a member's listener writes its address
// file again, with its directory, where they are gone, but leaves one
// another listener wrote since, and writes no more.
func TestForwardOwnFile(t *testing.T) {
	t.Parallel()
	own := machineAddress(t)
	d, _ := newDriver(t, newFake())
	d.interfaces = func() ([]netip.Prefix, error) { return []netip.Prefix{own}, nil }
	d.timing.touch = 10 * time.Millisecond
	l, err := d.Forward(t.Context(), testMember)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	e, err := d.Endpoint(t.Context(), testMember)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Dir(e.AddressFile)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		_, err := os.Lstat(e.AddressFile)
		return err == nil
	})
	other := workerapi.AddressFile{Addresses: []string{"10.0.0.9:1"}, Name: "other.meridian.invalid", Updated: 1, Nonce: "0b"}
	data, err := other.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	// A write of the listener's own that read the file just before can
	// land after it, once: the next finds the other's, and writes no more.
	others := func() bool {
		got, err := os.ReadFile(e.AddressFile)
		return err == nil && string(got) == string(data)
	}
	for range 5 {
		if err := safepath.WriteFile(e.AddressFile, data); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond) //nolint:forbidigo // five periods of a listener that would write it again
		if others() {
			break
		}
	}
	time.Sleep(100 * time.Millisecond) //nolint:forbidigo // ten periods more
	if !others() {
		got, err := os.ReadFile(e.AddressFile)
		t.Errorf("another listener's file is %s, %v", got, err)
	}
}

// TestMemberKeys checks that members that listen alike share a key, and a
// listener, and that one whose relay network is another has its own.
func TestMemberKeys(t *testing.T) {
	t.Parallel()
	d, _ := newDriver(t, newFake())
	key := func(settings string) string {
		t.Helper()
		k, err := d.MemberKey(&backend.Member{Name: "m", Settings: jsontext.Value(settings)})
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	a, b := key(`{"partition":"debug"}`), key(`{"partition":"gpu"}`)
	c, e := key(`{"network":"10.1.0.0/16"}`), key(`{"network":"10.2.0.0/16"}`)
	if a != b || a == c || c == e {
		t.Errorf("the keys are %s and %s alike, and %s and %s", a, b, c, e)
	}
}

// TestForwardKeptFacts checks that a listener taken over while the
// scheduler doesn't answer listens as the scheduler last said, at a port
// of its range; and that, with nothing kept, it doesn't.
func TestForwardKeptFacts(t *testing.T) {
	t.Parallel()
	own := machineAddress(t)
	f := newFake()
	f.info.FirstPort = 46000 + int(time.Now().UnixNano()%1000)
	f.info.LastPort = f.info.FirstPort + 20
	d, _ := newDriver(t, f)
	d.interfaces = func() ([]netip.Prefix, error) { return []netip.Prefix{own}, nil }
	l, err := d.Forward(t.Context(), testMember)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	f.mu.Lock()
	f.infoErr = errors.New("slurmctld isn't answering")
	f.mu.Unlock()
	again, _ := newDriver(t, f)
	again.Dir, again.Key, again.interfaces = d.Dir, d.Key, d.interfaces
	l, err = again.Forward(t.Context(), testMember)
	if err != nil {
		t.Fatalf("Forward while the scheduler doesn't answer: %v", err)
	}
	if port := l.Addr().(*net.TCPAddr).Port; port < f.info.FirstPort || port > f.info.LastPort {
		t.Errorf("the listener is at %d", port)
	}
	l.Close()
	fresh, _ := newDriver(t, f)
	fresh.interfaces = d.interfaces
	if _, err := fresh.Forward(t.Context(), testMember); err == nil {
		t.Error("a listener listened with nothing of the scheduler's kept")
	}
	// Another cluster's login node, which shares the home, has kept
	// nothing of its own.
	g := newFake()
	g.config, g.infoErr = "/etc/other/slurm.conf", f.infoErr
	other, _ := newDriver(t, g)
	other.Dir, other.Key, other.interfaces = d.Dir, d.Key, d.interfaces
	if _, err := other.Forward(t.Context(), testMember); err == nil {
		t.Error("a listener of another cluster listened as this one's scheduler last said")
	}
}

// TestForwardPortsTaken checks that a listener whose port the address
// before took listens on another, and that one whose range has no port
// free can't listen.
func TestForwardPortsTaken(t *testing.T) {
	t.Parallel()
	own := machineAddress(t).Addr()
	taken, err := net.Listen("tcp", netip.AddrPortFrom(own, 0).String())
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	port := taken.Addr().(*net.TCPAddr).Port
	listen := func(a netip.Addr, port int) (net.Listener, error) {
		return net.Listen("tcp", netip.AddrPortFrom(a, uint16(port)).String()) //nolint:gosec // a test's port
	}
	if _, err := listenIn(own, port, port, listen); err == nil {
		t.Error("a range of one port that is taken was listened in")
	}
	ls, err := listenAll(t.Context(), []netip.Addr{own}, port, port+1)
	if err != nil {
		// The port after it can be taken too, by another test.
		t.Skip(err)
	}
	defer closeAll(ls)
	if got := ls[0].Addr().(*net.TCPAddr).Port; got != port+1 {
		t.Errorf("the listener is at %d", got)
	}
}

// TestProbeNoAddress checks that a member whose login node, this machine,
// has no private address its workers could reach, or none in its relay
// network, can't run a job.
func TestProbeNoAddress(t *testing.T) {
	t.Parallel()
	d, _ := newDriver(t, newFake())
	d.interfaces = func() ([]netip.Prefix, error) {
		return []netip.Prefix{netip.MustParsePrefix("127.0.0.1/8"), netip.MustParsePrefix("203.0.113.4/24"), netip.MustParsePrefix("fe80::1/64")}, nil
	}
	fit, err := d.Probe(t.Context(), testMember, backend.Demand{})
	if err != nil {
		t.Fatal(err)
	}
	if got := kindtest.Want(t, fit.Problem, problems.SchedulerRelayNoAddress); got.Params["member"] != "m" || got.Params["has_network"] != "false" || got.Params["network"] != "" {
		t.Errorf("the problem's params are %v", got.Params)
	}
	if _, err := d.Forward(t.Context(), testMember); !problems.IsProblem(err) {
		t.Errorf("Forward without an address: %v", err)
	}
	f := newFake()
	d, _ = newDriver(t, f)
	f.relay = &Relay{Network: netip.MustParsePrefix("192.168.0.0/16")}
	fit, err = d.Probe(t.Context(), testMember, backend.Demand{})
	if err != nil {
		t.Fatal(err)
	}
	if got := kindtest.Want(t, fit.Problem, problems.SchedulerRelayNoAddress); got.Params["has_network"] != "true" || got.Params["network"] != "192.168.0.0/16" {
		t.Errorf("the problem's params are %v", got.Params)
	}
}

// TestPlaceEndpoint checks that the spec of a placement says the address
// file its worker reaches the coordinator through, and the key's hash,
// and the run's directory RunDir chose; and that no attempt is named as
// the listeners' directory is.
func TestPlaceEndpoint(t *testing.T) {
	t.Parallel()
	d, _ := newDriver(t, newFake())
	var asked backend.PlaceRequest
	d.RunDir = func(req backend.PlaceRequest) (string, error) {
		asked = req
		return "/home/u/.local/state/meridian/runs/" + req.Run, nil
	}
	p, err := d.Place(t.Context(), testMember, backend.PlaceRequest{
		Attempt: "a1", Run: "7", BootstrapToken: "t", RunDir: "/tmp/7",
		Coordinator: backend.Endpoint{AddressFile: "/home/u/relays/k.json", KeyHash: "sha256:00"},
	})
	if err != nil || p.RunDir != "/home/u/.local/state/meridian/runs/7" || asked.Run != "7" {
		t.Fatalf("Place: %+v, %v", p, err)
	}
	spec, err := ReadSpec(filepath.Join(d.Dir, "a1", "1"))
	if err != nil || spec.Coordinator != (Coordinator{AddressFile: "/home/u/relays/k.json", KeyHash: "sha256:00"}) || spec.RunDir != p.RunDir {
		t.Errorf("the spec is %+v, %v", spec, err)
	}
	d.RunDir = func(backend.PlaceRequest) (string, error) { return "", errors.New("no room") }
	if _, err := d.Place(t.Context(), testMember, backend.PlaceRequest{Attempt: "a2", BootstrapToken: "t"}); err == nil {
		t.Error("a run's directory that can't be chosen was placed")
	}
	if _, err := d.Place(t.Context(), testMember, backend.PlaceRequest{Attempt: RelaysDir, BootstrapToken: "t"}); err == nil {
		t.Error("an attempt named as the listeners' directory was placed")
	}
	if _, ok, err := d.Find(t.Context(), testMember, RelaysDir); ok || err != nil {
		t.Errorf("Find of the listeners' directory: %v, %v", ok, err)
	}
}
