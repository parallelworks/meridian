package workerapi

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
)

// An AddressFile is what the file EnvCoordinatorFile names holds: where a
// worker reaches its coordinator over TLS 1.3, as what listens for the
// coordinator writes it, a relay on a cluster's login node or the
// coordinator itself, as JSON:
//
//	{"addresses":["10.1.0.5:40211","[fd00::5]:40211"],"name":"r-3f2a.meridian","updated":1762400000}
//
// Its writer makes it readable only by the user, in a directory only the
// user can use, and replaces it whole, renamed into place, so that a
// worker reads it as it was or as it is, never part of either; a worker
// refuses one that other users could read or change. Nothing in it is
// secret, and nothing in it is trusted: the worker checks the
// coordinator's key, wherever the file sends it.
type AddressFile struct {
	// Addresses are hosts and ports (ValidAddress), IP addresses as a
	// relay writes them, which a worker tries in order: MaxAddresses at
	// most.
	Addresses []string `json:"addresses"`
	// Name is the server name the worker asks for in its TLS handshake
	// (SNI), by which a relay tells a worker's connection from a port
	// scan's. It is a DNS name, in lowercase, never an IP address, which
	// TLS doesn't send as one.
	Name string `json:"name"`
	// Updated is when its writer last wrote it, in Unix seconds, which it
	// does every so often while it listens, so that a reader can tell a
	// file whose writer has gone.
	Updated int64 `json:"updated"`
}

// MaxAddressFile is the most bytes of an address file a reader reads, and
// a writer writes; and MaxAddresses the most addresses it names, so that a
// worker that tries them all, each for as long as it gives one, still
// tries the file again within minutes.
const (
	MaxAddressFile = 4 << 10
	MaxAddresses   = 32
)

// An AddressFileError is an address file that isn't one: At is the JSON
// Pointer of the value in it that isn't what an address file holds there,
// or "" for a file that isn't JSON, or is larger than MaxAddressFile.
type AddressFileError struct {
	At  string
	Err error
}

func (e *AddressFileError) Error() string {
	if e.At == "" {
		return "workerapi: an address file: " + e.Err.Error()
	}
	return fmt.Sprintf("workerapi: an address file's %s: %v", e.At, e.Err)
}

func (e *AddressFileError) Unwrap() error { return e.Err }

// Marshal encodes f as its writer writes it, or returns an
// *AddressFileError for what an address file can't hold, as a reader would
// refuse it.
func (f *AddressFile) Marshal() ([]byte, error) {
	if err := f.check(); err != nil {
		return nil, err
	}
	data, err := jsonv2.Marshal(f, jsonv2.Deterministic(true))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxAddressFile {
		return nil, &AddressFileError{Err: fmt.Errorf("%d bytes, more than %d", len(data), MaxAddressFile)}
	}
	return data, nil
}

// UnmarshalAddressFile decodes an address file, or returns an
// *AddressFileError for one that isn't. A member it doesn't know it
// passes by, so that a later writer can add one.
func UnmarshalAddressFile(data []byte) (*AddressFile, error) {
	if len(data) > MaxAddressFile {
		return nil, &AddressFileError{Err: fmt.Errorf("%d bytes, more than %d", len(data), MaxAddressFile)}
	}
	f := new(AddressFile)
	if err := jsonv2.Unmarshal(data, f); err != nil {
		var at jsontext.Pointer
		if e, ok := errors.AsType[*jsonv2.SemanticError](err); ok {
			at = e.JSONPointer
		} else if e, ok := errors.AsType[*jsontext.SyntacticError](err); ok {
			at = e.JSONPointer
		}
		return nil, &AddressFileError{At: string(at), Err: err}
	}
	if err := f.check(); err != nil {
		return nil, err
	}
	return f, nil
}

// check returns an *AddressFileError for the first value of f that an
// address file can't hold: no address, or more than MaxAddresses, an
// address that isn't a host and port, a name that isn't a DNS name, or no
// time.
func (f *AddressFile) check() error {
	switch n := len(f.Addresses); {
	case n == 0:
		return &AddressFileError{At: "/addresses", Err: errors.New("no address")}
	case n > MaxAddresses:
		return &AddressFileError{At: "/addresses", Err: fmt.Errorf("%d addresses, more than %d", n, MaxAddresses)}
	}
	for i, a := range f.Addresses {
		if !ValidAddress(a) {
			return &AddressFileError{At: "/addresses/" + strconv.Itoa(i), Err: fmt.Errorf("%q isn't a host and port", a)}
		}
	}
	if !serverName(f.Name) {
		return &AddressFileError{At: "/name", Err: fmt.Errorf("%q isn't a server name", f.Name)}
	}
	if f.Updated <= 0 {
		return &AddressFileError{At: "/updated", Err: errors.New("no time")}
	}
	return nil
}

// ValidAddress reports whether address is one a worker reaches the
// coordinator at over TLS, as EnvCoordinator or an address file gives it:
// a host, an IP address without a zone or a name a resolver takes
// (hostName), and a port from 1 to 65535, without a leading zero, as
// net.Dial takes it, an IPv6 address in brackets. A URL can hold it as its
// host, as the worker's requests do.
func ValidAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 || strconv.FormatUint(n, 10) != port {
		return false
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		// A zone, as fe80::1%eth0's, is no part of a URL's host.
		return ip.Zone() == ""
	}
	return hostName(strings.ToLower(host))
}

// hostName reports whether host, in lowercase, is a name a resolver
// takes: a DNS name whose labels may hold underscores too, as the names
// of Docker Compose's services do, and that may end with the root's dot.
func hostName(host string) bool {
	return dnsName(strings.TrimSuffix(host, "."), true)
}

// serverName reports whether name is one TLS sends as a server's name: a
// DNS name, in lowercase, without an underscore, that isn't an IP
// address.
func serverName(name string) bool {
	_, err := netip.ParseAddr(name)
	return err != nil && dnsName(name, false)
}

// dnsName reports whether name is a DNS name in lowercase: labels of
// letters, digits and hyphens, and with underscore, underscores, neither
// starting nor ending with a hyphen, of 63 bytes at most, joined by dots,
// 253 bytes at most in all.
func dnsName(name string, underscore bool) bool {
	if name == "" || len(name) > 253 {
		return false
	}
	for label := range strings.SplitSeq(name, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' && (!underscore || r != '_') {
				return false
			}
		}
	}
	return true
}
