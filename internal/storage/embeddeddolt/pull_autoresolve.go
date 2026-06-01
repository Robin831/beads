//go:build cgo

package embeddeddolt

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// pullWithAutoResolve performs a FETCH+MERGE pull and transparently resolves
// the safe, recurring merge-conflict shapes that otherwise wedge a shared
// sync branch:
//
//   - metadata: take --theirs (machine-local rows like dolt_auto_push_* that
//     legitimately diverge across clones).
//   - issues:   per row, the side with the later updated_at wins (the common
//     worker-claim race: a bead is open locally while a forge worker has moved
//     it to in_progress on the remote, a few seconds newer).
//
// Anything else — a conflict on a third table, a delete/modify, or an
// equal/missing updated_at — is left unresolved so the operator can handle it
// with `bd dolt resolve`. The overwritten side is preserved in Dolt history.
//
// This is the embedded-mode counterpart of DoltStore.pullWithAutoResolve
// (server mode). It opens its own single-connection *sql.DB via OpenSQL so the
// whole FETCH/MERGE/resolve/commit sequence runs on one session, mirroring the
// proven server flow (SET dolt_allow_commit_conflicts=1 so conflicts can be
// inspected before committing the resolution).
func (s *EmbeddedDoltStore) pullWithAutoResolve(ctx context.Context, remote, branch, user string) (err error) {
	if s.closed.Load() {
		return errClosed
	}
	db, cleanup, err := OpenSQL(ctx, s.dataDir, s.database, s.branch)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, cleanup()) }()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	// Allow commits with conflicts so we can inspect and resolve them rather
	// than the merge auto-rolling-back under autocommit.
	if _, err := tx.ExecContext(ctx, "SET @@dolt_allow_commit_conflicts = 1"); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("set dolt_allow_commit_conflicts: %w", err)
	}

	// FETCH then MERGE (mirrors versioncontrolops.Pull, threading --user).
	if user != "" {
		if _, e := tx.ExecContext(ctx, "CALL DOLT_FETCH('--user', ?, ?, ?)", user, remote, branch); e != nil {
			_ = tx.Rollback()
			return fmt.Errorf("fetch from %s/%s: %w", remote, branch, e)
		}
	} else {
		if _, e := tx.ExecContext(ctx, "CALL DOLT_FETCH(?, ?)", remote, branch); e != nil {
			_ = tx.Rollback()
			return fmt.Errorf("fetch from %s/%s: %w", remote, branch, e)
		}
	}
	trackingRef := remote + "/" + branch
	_, mergeErr := tx.ExecContext(ctx, "CALL DOLT_MERGE(?)", trackingRef)
	if mergeErr != nil && strings.Contains(mergeErr.Error(), "up to date") {
		_ = tx.Rollback()
		return nil
	}

	// Check for conflicts whether or not MERGE errored — some dolt versions
	// error on conflict, others leave them in the working set.
	resolved, resolveErr := s.tryAutoResolveConflicts(ctx, tx)
	if resolveErr != nil {
		_ = tx.Rollback()
		if mergeErr != nil {
			return mergeErr
		}
		return resolveErr
	}
	if mergeErr != nil && !resolved {
		// Merge failed for a non-conflict reason, or conflicts include a table
		// we don't auto-resolve. Leave it for manual resolution.
		_ = tx.Rollback()
		return mergeErr
	}
	return tx.Commit()
}

// tryAutoResolveConflicts resolves metadata (--theirs) and issues
// (by updated_at) conflicts. Returns (true, nil) when every conflicting table
// was resolved and committed, (false, nil) when a conflict exists that we will
// not auto-resolve (caller leaves it), or (false, err) on a hard error.
func (s *EmbeddedDoltStore) tryAutoResolveConflicts(ctx context.Context, tx *sql.Tx) (bool, error) {
	rows, err := tx.QueryContext(ctx, "SELECT `table`, num_conflicts FROM dolt_conflicts")
	if err != nil {
		return false, fmt.Errorf("query conflicts: %w", err)
	}
	type conflict struct {
		table string
		count int
	}
	var conflicts []conflict
	for rows.Next() {
		var c conflict
		if err := rows.Scan(&c.table, &c.count); err != nil {
			_ = rows.Close()
			return false, fmt.Errorf("scan conflict: %w", err)
		}
		conflicts = append(conflicts, c)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return false, err
	}
	if len(conflicts) == 0 {
		return false, nil
	}
	// Every conflicting table must be one we know how to auto-resolve.
	for _, c := range conflicts {
		if c.table != "metadata" && c.table != "issues" {
			return false, nil
		}
	}

	var resolvedTables []string
	for _, c := range conflicts {
		switch c.table {
		case "metadata":
			if _, err := tx.ExecContext(ctx, "CALL DOLT_CONFLICTS_RESOLVE('--theirs', 'metadata')"); err != nil {
				return false, fmt.Errorf("resolve metadata conflicts: %w", err)
			}
			resolvedTables = append(resolvedTables, "metadata")
		case "issues":
			ok, err := s.resolveIssuesConflictsByUpdatedAt(ctx, tx)
			if err != nil {
				return false, fmt.Errorf("resolve issues conflicts: %w", err)
			}
			if !ok {
				// A row needs human judgement; don't partial-commit.
				return false, nil
			}
			resolvedTables = append(resolvedTables, "issues")
		}
	}

	for _, table := range resolvedTables {
		if _, err := tx.ExecContext(ctx, "CALL DOLT_ADD(?)", table); err != nil {
			return false, fmt.Errorf("stage %s: %w", table, err)
		}
	}
	msg := fmt.Sprintf("auto-resolve merge conflicts on %s", strings.Join(resolvedTables, ", "))
	if _, err := tx.ExecContext(ctx, "CALL DOLT_COMMIT('-m', ?)", msg); err != nil {
		return false, fmt.Errorf("commit resolved conflicts: %w", err)
	}
	return true, nil
}

// resolveIssuesConflictsByUpdatedAt resolves each conflicting issues row in
// favour of the side with the later updated_at. Returns false (leave for a
// human) on delete/modify or equal/missing timestamps.
func (s *EmbeddedDoltStore) resolveIssuesConflictsByUpdatedAt(ctx context.Context, tx *sql.Tx) (bool, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			COALESCE(our_id, their_id, base_id) AS row_id,
			our_updated_at,
			their_updated_at,
			our_diff_type,
			their_diff_type
		FROM dolt_conflicts_issues
	`)
	if err != nil {
		return false, fmt.Errorf("query issues conflicts: %w", err)
	}
	defer rows.Close()

	var (
		theirsWinnerCount int
		anyConflict       bool
	)
	for rows.Next() {
		anyConflict = true
		var (
			id                       sql.NullString
			ourDiff, theirDiff       sql.NullString
			ourUpdated, theirUpdated sql.NullTime
		)
		if err := rows.Scan(&id, &ourUpdated, &theirUpdated, &ourDiff, &theirDiff); err != nil {
			return false, fmt.Errorf("scan issues conflict: %w", err)
		}
		// Delete-vs-modify needs human judgement.
		if (ourDiff.Valid && ourDiff.String == "removed") || (theirDiff.Valid && theirDiff.String == "removed") {
			return false, nil
		}
		// Need a timestamp on both sides to decide.
		if !ourUpdated.Valid || !theirUpdated.Valid {
			return false, nil
		}
		if theirUpdated.Time.After(ourUpdated.Time) {
			theirsWinnerCount++
		} else if theirUpdated.Time.Equal(ourUpdated.Time) {
			// Identical timestamps but a conflict exists — the rows differ.
			// Tie-breaking by clock would be arbitrary; let a human look.
			return false, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	if !anyConflict {
		return false, nil
	}

	// Where theirs is strictly newer, overwrite our working-tree row with
	// theirs so the subsequent --ours resolve produces the merged-by-timestamp
	// result.
	if theirsWinnerCount > 0 {
		cols, err := nonPKColumns(ctx, tx, "issues", "id")
		if err != nil {
			return false, fmt.Errorf("look up issues columns: %w", err)
		}
		if len(cols) == 0 {
			return false, fmt.Errorf("no non-PK columns found for issues — schema lookup empty")
		}
		setClauses := make([]string, 0, len(cols))
		for _, col := range cols {
			setClauses = append(setClauses, fmt.Sprintf("i.`%s` = c.`their_%s`", col, col))
		}
		upd := fmt.Sprintf(`
			UPDATE issues i
			JOIN dolt_conflicts_issues c ON i.id = COALESCE(c.our_id, c.their_id)
			SET %s
			WHERE c.their_updated_at > c.our_updated_at
		`, strings.Join(setClauses, ", "))
		if _, err := tx.ExecContext(ctx, upd); err != nil {
			return false, fmt.Errorf("apply theirs to issues working tree: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, "CALL DOLT_CONFLICTS_RESOLVE('--ours', 'issues')"); err != nil {
		return false, fmt.Errorf("dolt_conflicts_resolve --ours issues: %w", err)
	}
	return true, nil
}

// nonPKColumns returns the column names of <table> except <pkColumn>, in
// ordinal order.
func nonPKColumns(ctx context.Context, tx *sql.Tx, table, pkColumn string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = ?
		  AND COLUMN_NAME != ?
		ORDER BY ORDINAL_POSITION
	`, table, pkColumn)
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
