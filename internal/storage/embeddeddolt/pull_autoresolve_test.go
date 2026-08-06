//go:build cgo

package embeddeddolt

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/steveyegge/beads/internal/types"
)

// TestTryAutoResolveConflicts_IssuesByUpdatedAt is the hermetic gate for the
// embedded pull-time auto-resolver (Stage D' of the v1.0.5 cutover). It builds
// a real modify/modify conflict on the issues table — the worker-claim race
// shape (open locally vs in_progress on the other branch, newer) — and asserts
// the resolver takes the later-updated_at side and clears the conflict, with no
// row loss. Because this code path silently discards the losing edit on every
// auto-pull, it must be proven on a real embedded conflict, not just compiled.
//
// Building the conflict requires ONE pinned connection for the whole scenario.
// The earlier attempt drove each step through store.Checkout plus a fresh
// OpenSQL per edit and always saw the merge fast-forward with 0 conflicts, for
// two compounding reasons:
//
//  1. DOLT_CHECKOUT is session-scoped, and Checkout runs it via the unpinned
//     withMutatingDBConn — so the branch switch dies with that throwaway
//     connection and s.branch is never updated.
//  2. OpenSQL ignores its branch argument entirely (buildDSN uses only dir and
//     database), and the pool it returns allows 2 connections — so even within
//     one *sql.DB, consecutive statements can land on different sessions.
//
// Every edit therefore landed on main, history stayed linear, and there was
// nothing to conflict. Pinning one connection (the same thing production does
// in withPinnedDBConn, and for the same session-scoped reason) makes the branch
// context hold across the whole setup.
func TestTryAutoResolveConflicts_IssuesByUpdatedAt(t *testing.T) {
	ctx := context.Background()
	beadsDir := filepath.Join(t.TempDir(), ".beads")
	store, err := Open(ctx, beadsDir, "ar", "main")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.SetConfig(ctx, "issue_prefix", "ar"); err != nil {
		t.Fatalf("SetConfig issue_prefix: %v", err)
	}
	if err := store.Commit(ctx, "bd init"); err != nil {
		t.Fatalf("init commit: %v", err)
	}

	issue := &types.Issue{
		Title:     "worker claim race",
		Status:    types.StatusOpen,
		Priority:  2,
		IssueType: types.TypeTask,
	}
	if err := store.CreateIssue(ctx, issue, "tester"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}
	id := issue.ID

	db, cleanup, err := OpenSQL(ctx, store.dataDir, store.database, store.branch)
	if err != nil {
		t.Fatalf("OpenSQL: %v", err)
	}
	t.Cleanup(func() { _ = cleanup() })

	// The pin. Everything below must run on this one session or the branch
	// context evaporates between statements.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}

	// Conflicts must survive the merge rather than aborting it.
	exec("SET @@dolt_allow_commit_conflicts = 1")

	// Base state on main, with a fixed timestamp so the comparison under test
	// is deterministic rather than wall-clock.
	exec("UPDATE issues SET status='open', updated_at='2026-06-01 00:00:00' WHERE id=?", id)
	exec("CALL DOLT_ADD('issues')")
	exec("CALL DOLT_COMMIT('-m', 'base')")

	// worker branch claims the bead, with a strictly LATER updated_at.
	exec("CALL DOLT_BRANCH('worker')")
	exec("CALL DOLT_CHECKOUT('worker')")
	exec("UPDATE issues SET status='in_progress', updated_at='2027-01-01 00:00:00' WHERE id=?", id)
	exec("CALL DOLT_ADD('issues')")
	exec("CALL DOLT_COMMIT('-m', 'worker claim')")

	// main makes a different, OLDER edit to the same row, so the merge is a
	// genuine modify/modify conflict and not a fast-forward.
	exec("CALL DOLT_CHECKOUT('main')")
	exec("UPDATE issues SET updated_at='2026-06-15 00:00:00' WHERE id=?", id)
	exec("CALL DOLT_ADD('issues')")
	exec("CALL DOLT_COMMIT('-m', 'main touch')")

	// A conflicting merge reports an error; the conflict it leaves behind is
	// the point, so this is logged rather than fatal.
	if _, mergeErr := conn.ExecContext(ctx, "CALL DOLT_MERGE('worker')"); mergeErr != nil {
		t.Logf("DOLT_MERGE returned (expected on conflict): %v", mergeErr)
	}

	var n int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_conflicts_issues").Scan(&n); err != nil {
		t.Fatalf("count conflicts: %v", err)
	}
	if n == 0 {
		t.Fatal("test setup produced no issues conflict; cannot validate resolver")
	}

	// The resolver takes a *sql.Tx; take it from the pinned conn so it sees the
	// same session (and the same in-flight merge) the setup built.
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "SET @@dolt_allow_commit_conflicts = 1"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("set allow_commit_conflicts: %v", err)
	}
	resolved, err := store.tryAutoResolveConflicts(ctx, tx)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("tryAutoResolveConflicts: %v", err)
	}
	if !resolved {
		_ = tx.Rollback()
		t.Fatal("expected the issues conflict to be auto-resolved, got resolved=false")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// theirs (worker, later updated_at) must have won, the conflict must be
	// gone, and the row must still be there.
	var status string
	if err := conn.QueryRowContext(ctx, "SELECT status FROM issues WHERE id=?", id).Scan(&status); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status != "in_progress" {
		t.Fatalf("status = %q, want %q (the later-updated_at side must win)", status, "in_progress")
	}

	var remaining int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_conflicts_issues").Scan(&remaining); err != nil {
		t.Fatalf("recount conflicts: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected 0 conflicts after resolve, got %d", remaining)
	}

	var total int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM issues WHERE id=?", id).Scan(&total); err != nil {
		t.Fatalf("recount issue: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected the issue row preserved (1), got %d", total)
	}
}

// TestTryAutoResolveConflicts_IssuesLocalNewerWins is the mirror of the test
// above: same conflict, but the LOCAL side carries the later updated_at.
//
// The pair is what pins the behaviour to the timestamp. The test above alone
// would still pass against a resolver "simplified" to a bare
// DOLT_CONFLICTS_RESOLVE('--theirs') — remote happens to be newer there — and
// that version would silently discard every local claim in this direction.
// Conversely a bare '--ours' fails the test above. Only both together rule out
// a fixed winner.
func TestTryAutoResolveConflicts_IssuesLocalNewerWins(t *testing.T) {
	ctx := context.Background()
	beadsDir := filepath.Join(t.TempDir(), ".beads")
	store, err := Open(ctx, beadsDir, "ar", "main")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.SetConfig(ctx, "issue_prefix", "ar"); err != nil {
		t.Fatalf("SetConfig issue_prefix: %v", err)
	}
	if err := store.Commit(ctx, "bd init"); err != nil {
		t.Fatalf("init commit: %v", err)
	}

	issue := &types.Issue{
		Title:     "local edit wins",
		Status:    types.StatusOpen,
		Priority:  2,
		IssueType: types.TypeTask,
	}
	if err := store.CreateIssue(ctx, issue, "tester"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}
	id := issue.ID

	db, cleanup, err := OpenSQL(ctx, store.dataDir, store.database, store.branch)
	if err != nil {
		t.Fatalf("OpenSQL: %v", err)
	}
	t.Cleanup(func() { _ = cleanup() })

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}

	exec("SET @@dolt_allow_commit_conflicts = 1")

	exec("UPDATE issues SET status='open', updated_at='2026-06-01 00:00:00' WHERE id=?", id)
	exec("CALL DOLT_ADD('issues')")
	exec("CALL DOLT_COMMIT('-m', 'base')")

	// worker branch edits the row but stays OLDER than main's later edit.
	exec("CALL DOLT_BRANCH('worker')")
	exec("CALL DOLT_CHECKOUT('worker')")
	exec("UPDATE issues SET status='closed', updated_at='2026-06-10 00:00:00' WHERE id=?", id)
	exec("CALL DOLT_ADD('issues')")
	exec("CALL DOLT_COMMIT('-m', 'worker stale close')")

	// main claims it later — this is the side that must survive.
	exec("CALL DOLT_CHECKOUT('main')")
	exec("UPDATE issues SET status='in_progress', updated_at='2027-01-01 00:00:00' WHERE id=?", id)
	exec("CALL DOLT_ADD('issues')")
	exec("CALL DOLT_COMMIT('-m', 'main claim')")

	if _, mergeErr := conn.ExecContext(ctx, "CALL DOLT_MERGE('worker')"); mergeErr != nil {
		t.Logf("DOLT_MERGE returned (expected on conflict): %v", mergeErr)
	}

	var n int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_conflicts_issues").Scan(&n); err != nil {
		t.Fatalf("count conflicts: %v", err)
	}
	if n == 0 {
		t.Fatal("test setup produced no issues conflict; cannot validate resolver")
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "SET @@dolt_allow_commit_conflicts = 1"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("set allow_commit_conflicts: %v", err)
	}
	resolved, err := store.tryAutoResolveConflicts(ctx, tx)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("tryAutoResolveConflicts: %v", err)
	}
	if !resolved {
		_ = tx.Rollback()
		t.Fatal("expected the issues conflict to be auto-resolved, got resolved=false")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var status string
	if err := conn.QueryRowContext(ctx, "SELECT status FROM issues WHERE id=?", id).Scan(&status); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status != "in_progress" {
		t.Fatalf("status = %q, want %q (the later-updated_at side was local and must win)", status, "in_progress")
	}

	var remaining int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_conflicts_issues").Scan(&remaining); err != nil {
		t.Fatalf("recount conflicts: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected 0 conflicts after resolve, got %d", remaining)
	}
}
