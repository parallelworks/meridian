package safepath

import "testing"

func TestMapsUID(t *testing.T) {
	initial := [][3]int64{{0, 0, 4294967295}}
	userns := [][3]int64{{1000, 1000, 1}}
	for _, tc := range []struct {
		m    [][3]int64
		uid  int64
		want bool
	}{
		{initial, 65534, true},
		{initial, 0, true},
		{userns, 1000, true},
		{userns, 65534, false},
		{userns, 0, false},
		{nil, 0, false},
	} {
		if got := mapsUID(tc.m, tc.uid); got != tc.want {
			t.Errorf("mapsUID(%v, %d) = %t, want %t", tc.m, tc.uid, got, tc.want)
		}
	}
	// The test runs in a namespace that maps its own user.
	if unmappedOwner(0) {
		t.Error("root's ID isn't the overflow ID, so it isn't unmapped")
	}
}
