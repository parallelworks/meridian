package local

import "testing"

func TestParseCPU(t *testing.T) {
	for in, want := range map[string]int64{"4": 4000, "0.5": 500, "500m": 500, "1.25": 1250, "1m": 1, "08": 8000} {
		if got, ok := parseCPU(in); !ok || got != want {
			t.Errorf("parseCPU(%q) = %d, %v, want %d", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "0", "-1", "0.0001", "1.5m", "m", "1e3", "+1", "four", ".5", "5.", "1_000", "0x10", "0b11", "0o7", "1/2", " 1"} {
		if got, ok := parseCPU(in); ok {
			t.Errorf("parseCPU(%q) = %d, want an error", in, got)
		}
	}
}

func TestParseMemory(t *testing.T) {
	for in, want := range map[string]int64{
		"8Gi": 8 << 30, "512Mi": 512 << 20, "1.5Gi": 3 << 29, "1k": 1000, "2G": 2e9, "1073741824": 1 << 30, "1Ei": 1 << 60,
	} {
		if got, ok := parseMemory(in); !ok || got != want {
			t.Errorf("parseMemory(%q) = %d, %v, want %d", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "0", "8GB", "1.5", "0.1k1", "Gi", "-1Gi", "8Ei", "9223372036854775808", "1e9", "0x10Gi", "1/2Gi", "1/2"} {
		if got, ok := parseMemory(in); ok {
			t.Errorf("parseMemory(%q) = %d, want an error", in, got)
		}
	}
}

func TestParseGPU(t *testing.T) {
	for in, want := range map[string]int{"0": 0, "2": 2, "+1": 1, "08": 8} {
		if got, ok := parseGPU(in); !ok || got != want {
			t.Errorf("parseGPU(%q) = %d, %v, want %d", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "-1", "1.5", "0x10", "1_000", "2147483648", "one"} {
		if got, ok := parseGPU(in); ok {
			t.Errorf("parseGPU(%q) = %d, want an error", in, got)
		}
	}
}

// A member's durations are read as a workflow's timeout: is, since
// placement compares the two.
func TestParseSeconds(t *testing.T) {
	for in, want := range map[string]int64{"24h": 86400, "90m": 5400, "30s": 30, "3600": 3600, "1d": 86400, "+5m": 300} {
		if got, ok := parseSeconds(in); !ok || got != want {
			t.Errorf("parseSeconds(%q) = %d, %t, want %d", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "0", "0s", "-1h", "1.5s", "500ms", "1h30m", "forever", "2147483648"} {
		if got, ok := parseSeconds(in); ok {
			t.Errorf("parseSeconds(%q) = %d, want none", in, got)
		}
	}
}
