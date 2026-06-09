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

// TestIsClaimRace pins the one cross-status shape --auto is allowed to
// resolve: base `open`, sides {open, in_progress}. Anything else with a
// status mismatch must stay unsafe so a human looks at it. This is the
// dominant recurring multi-clone conflict (a nightly job touches a bead on
// one clone ~50s before a worker claims it on another).
func TestIsClaimRace(t *testing.T) {
	cases := []struct {
		name             string
		base, our, their string
		want             bool
	}{
		{"ours open, theirs claimed", "open", "open", "in_progress", true},
		{"ours claimed, theirs open", "open", "in_progress", "open", true},
		{"base not open (unclaim race)", "in_progress", "in_progress", "open", false},
		{"base closed", "closed", "open", "in_progress", false},
		{"both open (not a mismatch)", "open", "open", "open", false},
		{"open vs closed", "open", "open", "closed", false},
		{"in_progress vs closed", "open", "in_progress", "closed", false},
		{"open vs blocked", "open", "open", "blocked", false},
		{"empty strings", "", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isClaimRace(tc.base, tc.our, tc.their); got != tc.want {
				t.Fatalf("isClaimRace(%q,%q,%q) = %v, want %v", tc.base, tc.our, tc.their, got, tc.want)
			}
		})
	}
}
