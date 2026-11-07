package workerapi_test

import (
	"errors"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/workerapi"
)

// TestAddressFile checks that an address file reads back as it was
// written, and that a member a later writer adds is passed by.
func TestAddressFile(t *testing.T) {
	want := &workerapi.AddressFile{Addresses: []string{"10.1.0.5:40211", "[fd00::5]:40211", "login2.cluster:7443"}, Name: "r-3f2a.meridian", Updated: 1762400000}
	data, err := want.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); got != `{"addresses":["10.1.0.5:40211","[fd00::5]:40211","login2.cluster:7443"],"name":"r-3f2a.meridian","updated":1762400000}` {
		t.Errorf("written as %s", got)
	}
	got, err := workerapi.UnmarshalAddressFile(data)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("read back as %+v, %v", got, err)
	}
	later := strings.Replace(string(data), `"name"`, `"ports":[1],"name"`, 1)
	if got, err := workerapi.UnmarshalAddressFile([]byte(later)); err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("with a member a later writer adds, read as %+v, %v", got, err)
	}
	// A writer's nonce, which a worker passes by.
	want.Nonce = "0f1e"
	if data, err = want.Marshal(); err != nil || !strings.HasSuffix(string(data), `,"nonce":"0f1e"}`) {
		t.Fatalf("written as %s, %v", data, err)
	}
	if got, err := workerapi.UnmarshalAddressFile(data); err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("with a nonce, read back as %+v, %v", got, err)
	}
	// A reader takes a nonce it couldn't write, which it doesn't use; a
	// writer refuses to write one.
	other := strings.Replace(string(data), `"0f1e"`, `"not hex"`, 1)
	if _, err := workerapi.UnmarshalAddressFile([]byte(other)); err != nil {
		t.Errorf("a file whose nonce isn't hex was refused: %v", err)
	}
	want.Nonce = "not hex"
	if _, err := want.Marshal(); err == nil {
		t.Error("a nonce that isn't hex was written")
	}
}

// TestAddressFileRefused checks that what isn't an address file is
// refused, each with where in it what is wrong is, and that Marshal
// refuses to write it too.
func TestAddressFileRefused(t *testing.T) {
	good := `{"addresses":["10.1.0.5:40211"],"name":"r.meridian","updated":1}`
	for _, tc := range []struct{ name, old, new, at string }{
		{"not JSON", `}`, ``, ""},
		{"no addresses", `"addresses":["10.1.0.5:40211"],`, ``, "/addresses"},
		{"addresses that are null", `["10.1.0.5:40211"]`, `null`, "/addresses"},
		{"an empty list", `["10.1.0.5:40211"]`, `[]`, "/addresses"},
		{"an address without a port", `10.1.0.5:40211`, `10.1.0.5`, "/addresses/0"},
		{"an address with port 0", `:40211`, `:0`, "/addresses/0"},
		{"an address past 65535", `:40211`, `:65536`, "/addresses/0"},
		{"a port with a leading zero", `:40211`, `:040211`, "/addresses/0"},
		{"an IPv6 address without brackets", `10.1.0.5:40211`, `fd00::5:40211`, "/addresses/0"},
		{"an address with a space", `10.1.0.5:40211`, `login 2:40211`, "/addresses/0"},
		{"an address that isn't text", `"10.1.0.5:40211"`, `40211`, "/addresses/0"},
		{"the second address", `"10.1.0.5:40211"`, `"10.1.0.5:40211",""`, "/addresses/1"},
		{"no name", `"name":"r.meridian",`, ``, "/name"},
		{"a name that is an IP address", `"r.meridian"`, `"10.1.0.5"`, "/name"},
		{"a name in capitals", `"r.meridian"`, `"R.meridian"`, "/name"},
		{"a name with an empty label", `"r.meridian"`, `"r..meridian"`, "/name"},
		{"a name that starts with a hyphen", `"r.meridian"`, `"-r.meridian"`, "/name"},
		{"no time", `,"updated":1`, ``, "/updated"},
		{"a time that is text", `"updated":1`, `"updated":"1"`, "/updated"},
		{"a member twice", `"updated":1`, `"updated":1,"updated":2`, "/updated"},
	} {
		data := strings.Replace(good, tc.old, tc.new, 1)
		if data == good {
			t.Fatalf("%s: unchanged", tc.name)
		}
		_, err := workerapi.UnmarshalAddressFile([]byte(data))
		e, ok := errors.AsType[*workerapi.AddressFileError](err)
		if !ok || e.At != tc.at {
			t.Errorf("%s: read %s as %v, want an error at %q", tc.name, data, err, tc.at)
		}
	}
	// One address more than a file may name.
	var addresses []string
	for i := range workerapi.MaxAddresses + 1 {
		addresses = append(addresses, `"10.1.0.5:`+strconv.Itoa(i+1)+`"`)
	}
	tooMany := `{"addresses":[` + strings.Join(addresses, ",") + `],"name":"r","updated":1}`
	if _, err := workerapi.UnmarshalAddressFile([]byte(tooMany)); err == nil {
		t.Errorf("read %d addresses", len(addresses))
	} else if e, ok := errors.AsType[*workerapi.AddressFileError](err); !ok || e.At != "/addresses" {
		t.Errorf("%d addresses: %v", len(addresses), err)
	}
	if _, err := workerapi.UnmarshalAddressFile([]byte(strings.Replace(tooMany, `"10.1.0.5:1",`, ``, 1))); err != nil {
		t.Errorf("%d addresses: %v", workerapi.MaxAddresses, err)
	}
	large := `{"addresses":["` + strings.Repeat("a", workerapi.MaxAddressFile) + `:1"],"name":"r","updated":1}`
	if _, err := workerapi.UnmarshalAddressFile([]byte(large)); err == nil {
		t.Error("read a file larger than an address file can be")
	}
	// Twenty of the longest host names are larger than a file can be.
	var many []string
	for range 20 {
		many = append(many, strings.Repeat("a.", 126)+"a:1")
	}
	for _, f := range []workerapi.AddressFile{
		{Name: "r", Updated: 1},
		{Addresses: slices.Repeat([]string{"10.1.0.5:1"}, workerapi.MaxAddresses+1), Name: "r", Updated: 1},
		{Addresses: []string{"10.1.0.5"}, Name: "r", Updated: 1},
		{Addresses: []string{"10.1.0.5:1"}, Name: "10.1.0.5", Updated: 1},
		{Addresses: []string{"10.1.0.5:1"}, Name: "r"},
		{Addresses: many, Name: "r", Updated: 1},
	} {
		if _, err := f.Marshal(); err == nil {
			t.Errorf("wrote %+v", f)
		}
	}
}

// TestValidAddress checks which addresses a worker takes for its
// coordinator, and that each one it takes is a URL's host as it is, as
// the worker's requests make it one.
func TestValidAddress(t *testing.T) {
	for _, tc := range []struct {
		address string
		valid   bool
	}{
		{"login1:7443", true},
		{"LOGIN1.Cluster:7443", true},
		{"10.1.0.5:40211", true},
		{"[fd00::5]:40211", true},
		// As Docker Compose names its services, and resolvers take them.
		{"meridian_coordinator:7443", true},
		{"_relay.login1:7443", true},
		// The root's dot.
		{"login1.cluster.:7443", true},
		{"login1..:7443", false},
		{".:7443", false},
		{"login1", false},
		{"login1:", false},
		{"login1:07443", false},
		{"login1:0", false},
		{"login1:65536", false},
		{"[fe80::1%eth0]:7443", false},
		{"[fe80::1%25eth0]:7443", false},
		{"fd00::5:40211", false},
		{"login 1:7443", false},
		{"-login1:7443", false},
		{"login1:7443/x", false},
		{"user@login1:7443", false},
	} {
		if got := workerapi.ValidAddress(tc.address); got != tc.valid {
			t.Errorf("ValidAddress(%q) = %t", tc.address, got)
			continue
		}
		if tc.valid {
			if u, err := url.Parse("https://" + tc.address + "/v1/claim"); err != nil || u.Host != tc.address {
				t.Errorf("%q is no URL's host: %v", tc.address, err)
			}
		}
	}
}
