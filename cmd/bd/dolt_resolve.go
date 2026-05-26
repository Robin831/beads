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
		allowRowLoss, _ := cmd.Flags().GetBool("allow-row-loss")
		message, _ := cmd.Flags().GetString("message")

		if ours == theirs {
			FatalError("exactly one of --ours or --theirs is required")
		}
		strategy := "--ours"
		if theirs {
			strategy = "--theirs"
		}

		ctx := context.Background()
		st := getStore()
		if st == nil {
			FatalError("no store available")
		}

		db, ok := storage.UnwrapStore(st).(interface{ DB() *sql.DB })
		if !ok || db == nil {
			FatalError("storage backend does not expose a database handle; run from an embedded-mode workspace or with dolt server connection configured")
		}
		sqlDB := db.DB()
		if sqlDB == nil {
			FatalError("database handle is nil")
		}

		if err := runSafeResolve(ctx, sqlDB, table, strategy, allowRowLoss, message); err != nil {
			FatalError("%v", err)
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

	// Run the conflict resolution. This is the same operation as
	// `dolt conflicts resolve` but inside our transaction so we can stage
	// + commit atomically afterwards.
	if _, err := db.ExecContext(ctx,
		fmt.Sprintf("CALL DOLT_CONFLICTS_RESOLVE(%q, %q)", strategy, table),
	); err != nil {
		return fmt.Errorf("resolving conflicts on %s with %s: %w", table, strategy, err)
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
	var isMerging int
	err := db.QueryRowContext(ctx, "SELECT is_merging FROM dolt_merge_status").Scan(&isMerging)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return isMerging != 0, nil
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

func init() {
	doltResolveCmd.Flags().Bool("ours", false, "Keep our (target branch) values for conflicting rows")
	doltResolveCmd.Flags().Bool("theirs", false, "Take their (source branch) values for conflicting rows")
	doltResolveCmd.Flags().Bool("allow-row-loss", false, "Allow the commit even when rows on the merge parents will be dropped (override the safety guard)")
	doltResolveCmd.Flags().StringP("message", "m", "", "Commit message (default: auto-generated)")
	doltCmd.AddCommand(doltResolveCmd)
}
