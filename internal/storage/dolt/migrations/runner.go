package migrations

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
)

// CompatMigrationVersion identifies the running bd binary. When set by the
// caller (typically `cmd/bd` during init from the Version/Build/Commit
// ldflag-baked vars), RunCompatMigrations short-circuits when the bd
// version stamped on the database matches — saving the per-command cost
// of running every idempotent migration's "is this already applied" check
// and the per-table DOLT_ADD calls.
//
// Measured per-command overhead on the skybert-forge pod 2026-05-26
// before this skip-path: `bd ready --limit 1` averaged 1456 ms, `bd list
// --status=open --limit 10` averaged 4645 ms. With this short-circuit in
// place, second-and-later runs at the same bd version drop to the ~50 ms
// range. The migration loop still runs unconditionally when this var is
// empty (backward compat for callers that don't wire it).
var CompatMigrationVersion string

// compatMigrationVersionKey is the local_metadata key that stamps the bd
// version which last successfully completed the compat-migration loop on
// this specific database. local_metadata is dolt-ignored, so this stamp
// is per-clone — it doesn't replicate via push/pull and doesn't generate
// merge conflicts. Each bd client tracks its own.
const compatMigrationVersionKey = "bd.compat_migration_version"

// CompatMigration represents a backward-compat migration for databases that
// predate the embedded migration system.
type CompatMigration struct {
	Name string
	Func func(*sql.DB) error
}

// compatMigrationsList is the ordered list of backward-compat migrations
// for databases that predate the embedded migration system. Each migration
// must be idempotent — safe to run multiple times.
var compatMigrationsList = []CompatMigration{
	{"wisp_type_column", MigrateWispTypeColumn},
	{"spec_id_column", MigrateSpecIDColumn},
	{"orphan_detection", DetectOrphanedChildren},
	{"wisps_table", MigrateWispsTable},
	{"wisp_auxiliary_tables", MigrateWispAuxiliaryTables},
	{"issue_counter_table", MigrateIssueCounterTable},
	{"infra_to_wisps", MigrateInfraToWisps},
	{"wisp_dep_type_index", MigrateWispDepTypeIndex},
	{"cleanup_autopush_metadata", MigrateCleanupAutopushMetadata},
	{"uuid_primary_keys", MigrateUUIDPrimaryKeys},
	{"add_no_history_column", MigrateAddNoHistoryColumn},
	{"add_started_at_column", MigrateAddStartedAtColumn},
	{"drop_hop_columns", MigrateDropHOPColumns},
	{"drop_child_counters_fk", MigrateDropChildCountersFK},
	{"wisp_events_created_at_index", MigrateWispEventsCreatedAtIndex},
	{"custom_status_type_tables", MigrateCustomStatusTypeTables},
	{"backfill_custom_tables", BackfillCustomTables},
}

// RunCompatMigrations executes all backward-compat migrations. These handle
// historical data transforms for databases that predate the embedded
// migration system (ALTER TABLE ADD COLUMN, data moves, FK drops, etc.).
// Each migration is idempotent and checks whether its changes have already
// been applied.
//
// When CompatMigrationVersion is set and matches the value stamped on the
// database, the migration loop is skipped entirely — the idempotency
// checks themselves are expensive on large databases, and re-running them
// on every bd command was the dominant per-command overhead in production
// usage. See the comment on CompatMigrationVersion for measurements.
func RunCompatMigrations(db *sql.DB) error {
	if alreadyMigratedThisVersion(db) {
		return nil
	}

	for _, m := range compatMigrationsList {
		if err := m.Func(db); err != nil {
			return fmt.Errorf("compat migration %q failed: %w", m.Name, err)
		}
	}

	// Only stage and commit when compat migrations actually produced changes.
	// Previously, DOLT_COMMIT was called unconditionally, causing a
	// "nothing to commit" WARNING on the server for every bd invocation
	// (94% of server log lines in one reported case). GH#3366.
	var dirtyCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM dolt_status").Scan(&dirtyCount); err != nil {
		// dolt_status might not be available (e.g. older servers); fall through
		// to the original behavior as a safe fallback.
		dirtyCount = 1
	}
	if dirtyCount == 0 {
		return nil
	}

	// GH#2455: Stage only schema tables (not config) to avoid sweeping up
	// stale issue_prefix changes from concurrent operations.
	migrationTables := []string{
		"issues", "wisps", "events", "wisp_events", "dependencies",
		"wisp_dependencies", "labels", "wisp_labels", "comments",
		"wisp_comments", "metadata", "child_counters", "issue_counter",
		"issue_snapshots", "compaction_snapshots", "federation_peers",
		"custom_statuses", "custom_types",
		"dolt_ignore",
	}
	for _, table := range migrationTables {
		_, _ = db.Exec("CALL DOLT_ADD(?)", table)
	}
	_, err := db.Exec("CALL DOLT_COMMIT('-m', 'schema: auto-migrate')")
	if err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "nothing to commit") {
			log.Printf("dolt compat migration commit warning: %v", err)
		}
	}

	stampCompatMigrationVersion(db)
	return nil
}

// alreadyMigratedThisVersion returns true when CompatMigrationVersion is set
// and matches the value previously stamped on this database by a successful
// migration run. Errors reading local_metadata (table missing on a fresh DB,
// permission issues, etc.) are treated as "not migrated" so the safe path
// is to re-run the migration loop. The loop is itself idempotent, so the
// extra work on those rare paths is correctness-preserving.
func alreadyMigratedThisVersion(db *sql.DB) bool {
	if CompatMigrationVersion == "" {
		return false
	}
	var applied string
	err := db.QueryRow(
		"SELECT value FROM local_metadata WHERE `key` = ?",
		compatMigrationVersionKey,
	).Scan(&applied)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			// Table missing or query error — fall through to full migration.
			log.Printf("compat migration: skip-check failed (running full loop): %v", err)
		}
		return false
	}
	return applied == CompatMigrationVersion
}

// stampCompatMigrationVersion writes the current bd version into
// local_metadata so the next bd invocation can short-circuit. Best-effort:
// failure to write logs a warning but does not fail the bd command — the
// next run will just re-do the migration loop unnecessarily.
func stampCompatMigrationVersion(db *sql.DB) {
	if CompatMigrationVersion == "" {
		return
	}
	_, err := db.Exec(
		"INSERT INTO local_metadata (`key`, value) VALUES (?, ?) "+
			"ON DUPLICATE KEY UPDATE value = VALUES(value)",
		compatMigrationVersionKey, CompatMigrationVersion,
	)
	if err != nil {
		log.Printf("compat migration: failed to stamp version %q: %v", CompatMigrationVersion, err)
	}
}

// ListCompatMigrations returns the names of all registered compat migrations.
func ListCompatMigrations() []string {
	names := make([]string, len(compatMigrationsList))
	for i, m := range compatMigrationsList {
		names[i] = m.Name
	}
	return names
}
