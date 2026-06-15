package main

import "testing"

// TestRemoteCLIOutputIndicatesSuccess pins the classification of dolt CLI
// push/pull output. The conflict cases are the ones that matter most: a
// conflicted `dolt pull` exits non-zero but prints "Updating <a>..<b>" during
// its merge phase, and that marker must NOT be read as success — otherwise the
// dolt-puller sidecar logs "pull ok" and never runs its auto-resolve recovery,
// leaving the embedded clone stuck mid-merge forever.
func TestRemoteCLIOutputIndicatesSuccess(t *testing.T) {
	// Verbatim output captured from `dolt merge` (== dolt pull's merge phase)
	// on a real content conflict.
	conflictOut := "Updating 0tgmof1vqtokcfec2tsq5mss7rf8tb7k..dicsss9oqobhu2v4ccsqh037m1s9mh56\n" +
		"Auto-merging t\n" +
		"CONFLICT (content): Merge conflict in t\n" +
		"Automatic merge failed; 1 table(s) are unmerged.\n" +
		"Use 'dolt conflicts' to investigate and resolve conflicts.\n"

	tests := []struct {
		name string
		op   string
		out  string
		want bool
	}{
		{"pull conflict is not success", "pull", conflictOut, false},
		{"pull fast-forward is success", "pull", "Updating abc..def\nFast-forward\n", true},
		{"pull merge made by is success", "pull", "Merge made by the 'recursive' strategy.\n", true},
		{"pull plain updating is success", "pull", "Updating abc..def\n", true},
		{"pull already up to date is success", "pull", "Already up to date\n", true},
		{"push everything up-to-date is success", "push", "Everything up-to-date\n", true},
		{"push new branch is success", "push", " * [new branch]      beads-sync -> beads-sync\n", true},
		{"unknown failure is not success", "pull", "fatal: some unexpected error\n", false},
		// A push that happens to echo CONFLICT-shaped text is not a pull
		// merge, so the conflict guard must not trip on it.
		{"push is unaffected by conflict guard", "push", "branch 'x' set up to track\n", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := remoteCLIOutputIndicatesSuccess(tc.op, tc.out); got != tc.want {
				t.Errorf("remoteCLIOutputIndicatesSuccess(%q, ...) = %v, want %v", tc.op, got, tc.want)
			}
		})
	}
}
