package backend_test

import (
	"testing"

	"github.com/parallelworks/meridian/backend"
)

// An endpoint is reached over TLS but for a Unix socket's path without an
// address file.
func TestEndpointTLS(t *testing.T) {
	for _, tc := range []struct {
		e   backend.Endpoint
		tls bool
	}{
		{backend.Endpoint{Address: "/home/alice/.local/state/meridian/coordinator.sock"}, false},
		{backend.Endpoint{Address: "login1:7443"}, true},
		{backend.Endpoint{Address: "[fd00::1]:7443"}, true},
		{backend.Endpoint{AddressFile: "/home/alice/.local/state/meridian/batch/relays/ab.json"}, true},
	} {
		if got := tc.e.TLS(); got != tc.tls {
			t.Errorf("%+v.TLS() = %t, want %t", tc.e, got, tc.tls)
		}
	}
}
