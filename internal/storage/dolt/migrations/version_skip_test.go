package migrations

import (
	"database/sql"
	"testing"
)

// TestRunCompatMigrations_SkipsWhenStampMatches verifies the per-bd-version
// short-circuit added to address the per-command compat-migration overhead
// observed on the skybert-forge pod 2026-05-26 (every bd command paying
// 1-5 s for idempotent re-runs). When CompatMigrationVersion is set and a
// matching stamp exists in local_metadata, RunCompatMigrations must not run
// the migration loop or the post-loop DOLT_COMMIT attempt.
func TestRunCompatMigrations_SkipsWhenStampMatches(t *testing.T) {
	db := openTestDoltBranch(t)

	// Establish the local_metadata table (it's part of the embedded
	// schema; ensure it exists before the test pre-stamps a value).
	if _, err := db.Exec(
		"CREATE TABLE IF NOT EXISTS local_metadata (`key` VARCHAR(255) PRIMARY KEY, value TEXT NOT NULL DEFAULT '')",
	); err != nil {
		t.Fatalf("create local_metadata: %v", err)
	}

	const testVersion = "1.0.3-skiptest"

	// Pre-stamp the version to simulate "this database has already been
	// migrated by the current bd version".
	if _, err := db.Exec(
		"INSERT INTO local_metadata (`key`, value) VALUES (?, ?) "+
			"ON DUPLICATE KEY UPDATE value = VALUES(value)",
		compatMigrationVersionKey, testVersion,
	); err != nil {
		t.Fatalf("pre-stamp version: %v", err)
	}

	prevVersion := CompatMigrationVersion
	CompatMigrationVersion = testVersion
	t.Cleanup(func() { CompatMigrationVersion = prevVersion })

	// Track whether any migration ran by replacing the list with a probe.
	probeRan := false
	probeMigrations := []CompatMigration{
		{Name: "skiptest_probe", Func: func(*sql.DB) error {
			probeRan = true
			return nil
		}},
	}
	prevList := compatMigrationsList
	compatMigrationsList = probeMigrations
	t.Cleanup(func() { compatMigrationsList = prevList })

	if err := RunCompatMigrations(db); err != nil {
		t.Fatalf("RunCompatMigrations returned error: %v", err)
	}
	if probeRan {
		t.Fatal("expected migration loop to be skipped when stamp matches, but probe ran")
	}
}

// TestRunCompatMigrations_RunsWhenStampMissing verifies that the loop runs
// normally on a fresh database (no stamp), and that a successful run leaves
// a stamp behind for subsequent invocations.
func TestRunCompatMigrations_RunsWhenStampMissing(t *testing.T) {
	db := openTestDoltBranch(t)

	if _, err := db.Exec(
		"CREATE TABLE IF NOT EXISTS local_metadata (`key` VARCHAR(255) PRIMARY KEY, value TEXT NOT NULL DEFAULT '')",
	); err != nil {
		t.Fatalf("create local_metadata: %v", err)
	}

	const testVersion = "1.0.3-runtest"

	prevVersion := CompatMigrationVersion
	CompatMigrationVersion = testVersion
	t.Cleanup(func() { CompatMigrationVersion = prevVersion })

	probeRan := false
	prevList := compatMigrationsList
	compatMigrationsList = []CompatMigration{
		{Name: "runtest_probe", Func: func(*sql.DB) error {
			probeRan = true
			return nil
		}},
	}
	t.Cleanup(func() { compatMigrationsList = prevList })

	if err := RunCompatMigrations(db); err != nil {
		t.Fatalf("RunCompatMigrations returned error: %v", err)
	}
	if !probeRan {
		t.Fatal("expected migration loop to run when no stamp exists, but probe did not run")
	}

	// Stamp should now be set.
	var applied string
	if err := db.QueryRow(
		"SELECT value FROM local_metadata WHERE `key` = ?",
		compatMigrationVersionKey,
	).Scan(&applied); err != nil {
		t.Fatalf("read stamp after run: %v", err)
	}
	if applied != testVersion {
		t.Fatalf("expected stamp %q after successful run, got %q", testVersion, applied)
	}
}

// TestRunCompatMigrations_StampsAfterNoopRun is the regression test for the
// "common case" bug: on a database whose schema is already current, every
// registered migration is idempotent and produces zero dirty rows, so the
// early-return at `dirtyCount == 0` was firing BEFORE the stamp was
// written. That meant the stamp was never written on healthy databases —
// and the entire skip-path added by CompatMigrationVersion was reachable
// in theory but unreachable in practice. Caught on the local Fhi.Metadata
// embedded DB: bd ready was still ~6-14 s per command after the perf patch
// shipped because no stamp ever landed. Fix: stamp even when the loop
// produced no dirty rows.
func TestRunCompatMigrations_StampsAfterNoopRun(t *testing.T) {
	db := openTestDoltBranch(t)

	if _, err := db.Exec(
		"CREATE TABLE IF NOT EXISTS local_metadata (`key` VARCHAR(255) PRIMARY KEY, value TEXT NOT NULL DEFAULT '')",
	); err != nil {
		t.Fatalf("create local_metadata: %v", err)
	}

	const testVersion = "1.0.3-nooptest"

	prevVersion := CompatMigrationVersion
	CompatMigrationVersion = testVersion
	t.Cleanup(func() { CompatMigrationVersion = prevVersion })

	// Probe migration that runs but produces no dirty rows — mirrors the
	// real-world steady state where every migration is already applied.
	probeRan := false
	prevList := compatMigrationsList
	compatMigrationsList = []CompatMigration{
		{Name: "nooptest_probe", Func: func(*sql.DB) error {
			probeRan = true
			return nil
		}},
	}
	t.Cleanup(func() { compatMigrationsList = prevList })

	// Sanity: dolt_status starts clean.
	var dirtyBefore int
	if err := db.QueryRow("SELECT COUNT(*) FROM dolt_status").Scan(&dirtyBefore); err != nil {
		t.Fatalf("read dolt_status: %v", err)
	}
	if dirtyBefore != 0 {
		t.Fatalf("test precondition: expected clean dolt_status, got %d dirty rows", dirtyBefore)
	}

	if err := RunCompatMigrations(db); err != nil {
		t.Fatalf("RunCompatMigrations returned error: %v", err)
	}
	if !probeRan {
		t.Fatal("expected migration loop to run on first invocation, but probe did not run")
	}

	// Critical assertion: the stamp must be written even though no
	// dirty rows were produced. Pre-fix, this row was absent.
	var applied string
	if err := db.QueryRow(
		"SELECT value FROM local_metadata WHERE `key` = ?",
		compatMigrationVersionKey,
	).Scan(&applied); err != nil {
		t.Fatalf("read stamp after noop run: %v (this is the bug — stamp missing)", err)
	}
	if applied != testVersion {
		t.Fatalf("expected stamp %q after noop run, got %q", testVersion, applied)
	}

	// And the next invocation must short-circuit.
	probeRan = false
	if err := RunCompatMigrations(db); err != nil {
		t.Fatalf("second RunCompatMigrations returned error: %v", err)
	}
	if probeRan {
		t.Fatal("expected second invocation to short-circuit via stamp, but probe ran again")
	}
}

// TestRunCompatMigrations_RunsWhenVersionUnset preserves backward-compatible
// behaviour: callers that never set CompatMigrationVersion (e.g. tests that
// don't go through cmd/bd's init) must continue to run the full migration
// loop on every invocation.
func TestRunCompatMigrations_RunsWhenVersionUnset(t *testing.T) {
	db := openTestDoltBranch(t)

	prevVersion := CompatMigrationVersion
	CompatMigrationVersion = ""
	t.Cleanup(func() { CompatMigrationVersion = prevVersion })

	probeRan := false
	prevList := compatMigrationsList
	compatMigrationsList = []CompatMigration{
		{Name: "unset_probe", Func: func(*sql.DB) error {
			probeRan = true
			return nil
		}},
	}
	t.Cleanup(func() { compatMigrationsList = prevList })

	if err := RunCompatMigrations(db); err != nil {
		t.Fatalf("RunCompatMigrations returned error: %v", err)
	}
	if !probeRan {
		t.Fatal("expected migration loop to run when CompatMigrationVersion is empty (backward compat), but probe did not run")
	}
}
