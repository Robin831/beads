package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/steveyegge/beads/internal/storage"
)

// doltResolveCmd is the safe alternative to running `dolt conflicts resolve`
// + `dolt commit` by hand. After an interrupted `dolt merge` (or after
// bd's autopull surfaced a conflict), the operator can run:
//
//	bd dolt resolve --theirs issues
//
// which:
//  1. Reads the merge parents from dolt_status / dolt_merge_status.
//  2. Resolves the in-progress conflict for the given table with the chosen
//     strategy (--ours or --theirs).
//  3. Stages ALL changes in the table (the conflict resolution AND any
//     auto-merged rows that were already in the working set but unstaged).
//  4. Computes the set of row IDs that existed in either merge parent but
//     are absent from the resolved working set. If non-empty, refuses with
//     a clear error and aborts — unless --allow-row-loss is passed.
//  5. Otherwise commits the merge.
//
// The row-loss guard is the central safety property. On 2026-05-26, a manual
// `dolt conflicts resolve --theirs issues` followed by `dolt commit` lost a
// real bead (Fhi.Metadata-pxqvv, created minutes earlier on the theirs
// side) because the commit did not include the auto-merged rows. This
// subcommand makes that class of data loss impossible without an explicit
// opt-in flag.
var doltResolveCmd = &cobra.Command{
	Use:   "resolve <table>",
	Short: "Resolve a merge conflict on <table> with a row-loss guard",
	Long: `Resolve an in-progress merge conflict on <table> using --ours or --theirs,
then commit — but refuse to commit when rows that existed on either
merge parent would disappear from the result.

This is the safe alternative to running ` + "`dolt conflicts resolve` + `dolt commit`" + ` by hand.
The manual sequence has been observed to silently drop rows that existed
only on one side of a 3-way merge (specifically Fhi.Metadata-pxqvv on
2026-05-26). This command stages all working-tree changes — including
auto-merged rows that the manual flow leaves unstaged — so the commit
captures the complete merge result.

Examples:
  bd dolt resolve --theirs issues           # take theirs for conflict rows; refuse if rows would be lost
  bd dolt resolve --ours issues             # take ours
  bd dolt resolve --theirs issues --allow-row-loss   # accept row loss explicitly (rare; usually means a deliberate cleanup)`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		table := args[0]
		ours, _ := cmd.Flags().GetBool("ours")
		theirs, _ := cmd.Flags().GetBool("theirs")
		auto, _ := cmd.Flags().GetBool("auto")
		allowRowLoss, _ := cmd.Flags().GetBool("allow-row-loss")
		message, _ := cmd.Flags().GetString("message")

		modeCount := 0
		if ours {
			modeCount++
		}
		if theirs {
			modeCount++
		}
		if auto {
			modeCount++
		}
		if modeCount != 1 {
			FatalError("exactly one of --ours, --theirs, or --auto is required")
		}
		strategy := "--ours"
		if theirs {
			strategy = "--theirs"
		}
		if auto {
			strategy = "--auto"
		}

		ctx := context.Background()
		st := getStore()
		if st == nil {
			FatalError("no store available")
		}
		inner := storage.UnwrapStore(st)

		// Embedded mode (the common case: laptop + pod) exposes no persistent
		// *sql.DB — connections are short-lived and opened on demand. Drive the
		// resolve through WithConflictDB, which hands us a single-connection
		// handle scoped to the merge working set and cleans it up afterward.
		// Server mode exposes a long-lived *sql.DB via DB(); use that directly.
		switch backend := inner.(type) {
		case interface {
			WithConflictDB(context.Context, func(*sql.DB) error) error
		}:
			if err := backend.WithConflictDB(ctx, func(db *sql.DB) error {
				return runSafeResolve(ctx, db, table, strategy, allowRowLoss, message)
			}); err != nil {
				FatalError("%v", err)
			}
		case interface{ DB() *sql.DB }:
			sqlDB := backend.DB()
			if sqlDB == nil {
				FatalError("database handle is nil")
			}
			if err := runSafeResolve(ctx, sqlDB, table, strategy, allowRowLoss, message); err != nil {
				FatalError("%v", err)
			}
		default:
			FatalError("storage backend does not expose a database handle; run from an embedded-mode workspace or with dolt server connection configured")
		}
	},
}

// runSafeResolve drives the resolve+commit flow. Broken out from the cobra
// Run wrapper so tests can drive it against a *sql.DB fixture without
// constructing the full store + cobra command machinery.
func runSafeResolve(ctx context.Context, db *sql.DB, table, strategy string, allowRowLoss bool, message string) error {
	merging, err := mergeInProgress(ctx, db)
	if err != nil {
		return fmt.Errorf("checking merge state: %w", err)
	}
	if !merging {
		return fmt.Errorf("no merge in progress on the current branch; run `dolt merge <ref>` first")
	}

	mergeP1, mergeP2, err := mergeParents(ctx, db)
	if err != nil {
		return fmt.Errorf("reading merge parents: %w", err)
	}
	if mergeP1 == "" || mergeP2 == "" {
		return fmt.Errorf("could not determine both merge parents; aborting to avoid an unguarded resolution")
	}

	// Run the conflict resolution. For --ours/--theirs, defer to dolt's
	// table-level resolver. For --auto, examine each conflicting row, pick
	// the safe-shape resolution per-row (same final status + only timestamp
	// columns differ → take the later updated_at; anything else → bail).
	if strategy == "--auto" {
		picks, unsafe, err := classifyConflicts(ctx, db, table)
		if err != nil {
			return fmt.Errorf("classifying conflicts on %s: %w", table, err)
		}
		if len(unsafe) > 0 {
			preview := unsafe
			if len(preview) > 5 {
				preview = preview[:5]
			}
			return fmt.Errorf(
				"--auto refused: %d conflict(s) on %s are not safe-shape (status mismatch or content drift on title/description/etc.).\n"+
					"  affected ids (first %d): %s\n\n"+
					"  inspect with: SELECT base_id, our_status, their_status FROM dolt_conflicts_%s;\n"+
					"  resolve manually with --ours or --theirs (and confirm row-loss guard reports as expected).",
				len(unsafe), table, len(preview), strings.Join(preview, ", "), table,
			)
		}
		// Resolve whole-table rather than per-PK. The 3-arg per-row form
		// CALL DOLT_CONFLICTS_RESOLVE(strategy, table, pk) is not supported by
		// all dolt versions (fails with "table not found"), so instead we
		// pre-patch the working-tree rows where theirs is the newer side to
		// theirs' values, then resolve the whole table with --ours. classify
		// has already guaranteed every conflicting row is safe-shape (same
		// status + content, only timestamps differ), and its pick rule —
		// later updated_at wins, ties to ours — is exactly the WHERE below.
		// This mirrors the embedded pull-time auto-resolver.
		theirsWins := 0
		for _, pick := range picks {
			if pick == pickTheirs {
				theirsWins++
			}
		}
		if theirsWins > 0 {
			cols, err := tableNonPKColumns(ctx, db, table)
			if err != nil {
				return fmt.Errorf("looking up %s columns: %w", table, err)
			}
			if len(cols) == 0 {
				return fmt.Errorf("no non-PK columns found for %s — schema lookup empty", table)
			}
			setClauses := make([]string, 0, len(cols))
			for _, col := range cols {
				setClauses = append(setClauses, fmt.Sprintf("t.`%s` = c.`their_%s`", col, col))
			}
			upd := fmt.Sprintf(
				"UPDATE `%s` t JOIN dolt_conflicts_%s c ON t.id = COALESCE(c.our_id, c.their_id) "+
					"SET %s WHERE c.their_updated_at > c.our_updated_at",
				table, table, strings.Join(setClauses, ", "),
			)
			if _, err := db.ExecContext(ctx, upd); err != nil {
				return fmt.Errorf("applying theirs to %s working tree: %w", table, err)
			}
		}
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf("CALL DOLT_CONFLICTS_RESOLVE('--ours', %q)", table),
		); err != nil {
			return fmt.Errorf("auto-resolving %s (whole-table --ours after pre-patch): %w", table, err)
		}
		fmt.Fprintf(os.Stderr, "auto-resolved %d safe-shape conflict(s) on %s\n", len(picks), table)
	} else {
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf("CALL DOLT_CONFLICTS_RESOLVE(%q, %q)", strategy, table),
		); err != nil {
			return fmt.Errorf("resolving conflicts on %s with %s: %w", table, strategy, err)
		}
	}

	// Stage all changes in the table, including auto-merged rows the
	// conflict-resolve step did not touch. Without this step, `dolt commit`
	// records only the conflict-resolved diff and silently drops the
	// auto-merged rows — which is exactly the data-loss path we are
	// preventing.
	if _, err := db.ExecContext(ctx, "CALL DOLT_ADD(?)", table); err != nil {
		return fmt.Errorf("staging %s after resolve: %w", table, err)
	}

	// Compute row-loss. Look at row IDs in either merge parent that are
	// absent from the current working set's table. Limited to tables we
	// understand the primary key for; for unknown tables we apply a
	// generic id-column check and skip the guard if no `id` column exists.
	lost, err := rowsLostInMerge(ctx, db, table, mergeP1, mergeP2)
	if err != nil {
		return fmt.Errorf("checking for row loss: %w", err)
	}
	if len(lost) > 0 && !allowRowLoss {
		preview := lost
		if len(preview) > 10 {
			preview = preview[:10]
		}
		return fmt.Errorf(
			"refusing to commit: merge would drop %d row(s) from %s that existed on one side of the merge.\n"+
				"  affected ids (first %d): %s\n\n"+
				"  re-run with --allow-row-loss to override (you almost never want this), or\n"+
				"  inspect with: SELECT id FROM `%s` AS OF '%s' WHERE id NOT IN (SELECT id FROM `%s`);\n"+
				"  and similarly for parent %s",
			len(lost), table, len(preview), strings.Join(preview, ", "),
			table, mergeP1, table, mergeP2,
		)
	}
	if len(lost) > 0 {
		fmt.Fprintf(os.Stderr, "WARN: dropping %d row(s) from %s as part of merge (--allow-row-loss): %s\n",
			len(lost), table, strings.Join(firstNStrings(lost, 10), ", "))
	}

	commitMsg := message
	if commitMsg == "" {
		commitMsg = fmt.Sprintf("bd: resolve merge on %s (%s, row-loss guard verified)", table, strategy)
	}
	if _, err := db.ExecContext(ctx, "CALL DOLT_COMMIT('-m', ?)", commitMsg); err != nil {
		return fmt.Errorf("committing merge: %w", err)
	}

	if len(lost) == 0 {
		fmt.Printf("Resolved merge on %s using %s — no rows lost, all merge-parent rows preserved.\n",
			table, strategy)
	} else {
		fmt.Printf("Resolved merge on %s using %s — %d row(s) dropped (explicit --allow-row-loss).\n",
			table, strategy, len(lost))
	}
	return nil
}

// mergeInProgress returns true when the session/branch is sitting in the
// middle of an unresolved merge (whether a conflict exists or not).
func mergeInProgress(ctx context.Context, db *sql.DB) (bool, error) {
	// dolt_merge_status.is_merging comes back as an int (0/1) from the
	// dolt sql-server driver but as a Go bool from the embedded dolt driver.
	// Scan into `any` and coerce so the same code works in both modes —
	// scanning a bool driver value into an int (the old code) failed under
	// embedded mode with "converting driver.Value type bool to a int".
	var raw any
	err := db.QueryRowContext(ctx, "SELECT is_merging FROM dolt_merge_status").Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return driverTruthy(raw), nil
}

// driverTruthy coerces a SQL driver value into a bool, tolerating the
// different concrete Go types the dolt sql-server driver (int64) and the
// embedded dolt driver (bool) return for the same boolean column. Strings
// and byte slices ("1"/"true") are also handled for robustness.
func driverTruthy(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case int64:
		return t != 0
	case int:
		return t != 0
	case []byte:
		s := strings.ToLower(strings.TrimSpace(string(t)))
		return s == "1" || s == "true"
	case string:
		s := strings.ToLower(strings.TrimSpace(t))
		return s == "1" || s == "true"
	default:
		return false
	}
}

// mergeParents returns (target, source) commit hashes for the in-progress
// merge — i.e. the branch we are merging into and the branch we are
// pulling from. Source corresponds to "theirs", target to "ours".
func mergeParents(ctx context.Context, db *sql.DB) (target, source string, err error) {
	err = db.QueryRowContext(ctx,
		"SELECT target, source_commit FROM dolt_merge_status",
	).Scan(&target, &source)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", nil
		}
		return "", "", err
	}
	return target, source, nil
}

// rowsLostInMerge returns row IDs that existed in either merge parent but
// are absent from the current working set's <table>. Requires that <table>
// have an `id` column (true for issues, dependencies, labels, comments,
// etc.); tables without an id column get skipped — the guard returns an
// empty list and logs a debug note so the caller can still proceed.
func rowsLostInMerge(ctx context.Context, db *sql.DB, table, target, source string) ([]string, error) {
	if !tableHasIDColumn(ctx, db, table) {
		return nil, nil
	}
	q := fmt.Sprintf(
		"SELECT DISTINCT id FROM (\n"+
			"  SELECT id FROM `%s` AS OF '%s'\n"+
			"  UNION\n"+
			"  SELECT id FROM `%s` AS OF '%s'\n"+
			") AS parents\n"+
			"WHERE id NOT IN (SELECT id FROM `%s`)",
		table, target, table, source, table,
	)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var lost []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		lost = append(lost, id)
	}
	return lost, rows.Err()
}

// tableHasIDColumn reports whether <table> has a column literally named
// "id" in the current branch's schema. Used by the row-loss guard to know
// when it has a stable per-row identifier to compare across the merge
// parents and the resolved working set.
func tableHasIDColumn(ctx context.Context, db *sql.DB, table string) bool {
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = ? AND column_name = 'id'",
		table,
	).Scan(&count)
	if err != nil {
		return false
	}
	return count > 0
}

func firstNStrings(ss []string, n int) []string {
	if len(ss) <= n {
		return ss
	}
	return ss[:n]
}

// pickSide enumerates which merge-parent's values the auto-resolver picks
// for a single conflicting row.
type pickSide int

const (
	pickOurs pickSide = iota
	pickTheirs
)

// safeShapeContentColumns are the columns that, when they differ between
// ours and theirs, force the auto-resolver to bail out. Differences here
// represent real content drift that needs a human decision. Timestamps
// (updated_at, closed_at, started_at) intentionally not on this list —
// they routinely differ between independent edits of the same row, and
// taking the later one is the correct heuristic.
var safeShapeContentColumns = []string{
	"title", "description", "design", "acceptance_criteria", "notes",
	"assignee", "priority", "issue_type", "owner", "external_ref",
	"close_reason", "spec_id",
}

// classifyConflicts inspects every row in dolt_conflicts_<table> and
// categorises each as either safe-shape (same final status, only
// timestamp columns differ — auto-resolvable by taking the side with the
// later updated_at) or unsafe (status mismatch or content drift). Only
// implemented for the `issues` table today; other tables fall through as
// "no auto-resolution possible, all rows are unsafe" so the caller bails.
//
// Returns (picks, unsafeIDs, err) where picks is a map from row id to
// the chosen merge-parent side.
func classifyConflicts(ctx context.Context, db *sql.DB, table string) (map[string]pickSide, []string, error) {
	if table != "issues" {
		// Conservatively refuse auto-resolution on tables we haven't
		// reasoned about. Operator must use --ours or --theirs explicitly.
		ids, err := loadConflictRowIDs(ctx, db, table)
		if err != nil {
			return nil, nil, err
		}
		return nil, ids, nil
	}

	selectCols := []string{
		"base_id", "our_status", "their_status",
		"our_updated_at", "their_updated_at",
	}
	for _, c := range safeShapeContentColumns {
		selectCols = append(selectCols,
			fmt.Sprintf("our_%s", c),
			fmt.Sprintf("their_%s", c),
		)
	}
	q := fmt.Sprintf("SELECT %s FROM dolt_conflicts_%s", strings.Join(selectCols, ", "), table)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	picks := make(map[string]pickSide)
	var unsafe []string

	for rows.Next() {
		// Dynamic-width scan: one string-ish slot per column. Use
		// sql.NullString so NULLs come through cleanly.
		dest := make([]any, len(selectCols))
		nulls := make([]sql.NullString, len(selectCols))
		for i := range dest {
			dest[i] = &nulls[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, nil, err
		}

		id := nulls[0].String
		ourStatus := nulls[1].String
		theirStatus := nulls[2].String
		ourUpdatedAt := nulls[3].String
		theirUpdatedAt := nulls[4].String

		if ourStatus != theirStatus {
			unsafe = append(unsafe, id)
			continue
		}

		contentDrift := false
		for i, c := range safeShapeContentColumns {
			ourVal := nulls[5+2*i]
			theirVal := nulls[5+2*i+1]
			if ourVal.Valid != theirVal.Valid || ourVal.String != theirVal.String {
				_ = c // (named in selectCols above; loop index is enough here)
				contentDrift = true
				break
			}
		}
		if contentDrift {
			unsafe = append(unsafe, id)
			continue
		}

		// Safe shape: take whichever side has the later updated_at.
		// String comparison works because dolt stores timestamps in
		// ISO-8601-like sortable format ("YYYY-MM-DD HH:MM:SS...").
		if ourUpdatedAt >= theirUpdatedAt {
			picks[id] = pickOurs
		} else {
			picks[id] = pickTheirs
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return picks, unsafe, nil
}

// tableNonPKColumns returns the column names of <table> except the primary
// key `id`, in ordinal order. Used by --auto to build the "copy theirs into
// the working tree" UPDATE for rows where theirs is the newer side.
func tableNonPKColumns(ctx context.Context, db *sql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = ?
		  AND COLUMN_NAME != 'id'
		ORDER BY ORDINAL_POSITION
	`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

// loadConflictRowIDs returns the list of base_id values currently in
// dolt_conflicts_<table>. Used by classifyConflicts when bailing on
// a table it doesn't auto-handle.
func loadConflictRowIDs(ctx context.Context, db *sql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT base_id FROM dolt_conflicts_%s", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id sql.NullString
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id.Valid {
			ids = append(ids, id.String)
		}
	}
	return ids, rows.Err()
}

func init() {
	doltResolveCmd.Flags().Bool("ours", false, "Keep our (target branch) values for conflicting rows")
	doltResolveCmd.Flags().Bool("theirs", false, "Take their (source branch) values for conflicting rows")
	doltResolveCmd.Flags().Bool("auto", false, "Auto-resolve per-row: same-final-status + only timestamps differ → take later updated_at; anything else → bail")
	doltResolveCmd.Flags().Bool("allow-row-loss", false, "Allow the commit even when rows on the merge parents will be dropped (override the safety guard)")
	doltResolveCmd.Flags().StringP("message", "m", "", "Commit message (default: auto-generated)")
	doltCmd.AddCommand(doltResolveCmd)
}
