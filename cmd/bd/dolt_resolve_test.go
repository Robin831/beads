package main

import "testing"

// TestDriverTruthy guards the embedded-vs-server driver type mismatch that
// broke `bd dolt resolve` in embedded mode: the dolt sql-server driver returns
// dolt_merge_status.is_merging as an int64 (0/1) while the embedded dolt
// driver returns a Go bool. The original code scanned straight into an int and
// failed under embedded mode with "converting driver.Value type bool to a int".
// driverTruthy must coerce every shape both drivers can produce.
func TestDriverTruthy(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want bool
	}{
		{"bool true (embedded driver)", true, true},
		{"bool false (embedded driver)", false, false},
		{"int64 1 (sql-server driver)", int64(1), true},
		{"int64 0 (sql-server driver)", int64(0), false},
		{"int 1", 1, true},
		{"int 0", 0, false},
		{"bytes 1", []byte("1"), true},
		{"bytes true", []byte("true"), true},
		{"bytes 0", []byte("0"), false},
		{"bytes false", []byte("false"), false},
		{"string 1", "1", true},
		{"string true uppercased + spaced", "  TRUE  ", true},
		{"string false", "false", false},
		{"string empty", "", false},
		{"nil", nil, false},
		{"unexpected float", 3.14, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := driverTruthy(tc.in); got != tc.want {
				t.Fatalf("driverTruthy(%#v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
