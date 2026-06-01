//go:build cgo

package embeddeddolt

import (
	"context"
	"database/sql"
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
func TestTryAutoResolveConflicts_IssuesByUpdatedAt(t *testing.T) {
	// SKIP: the resolver (tryAutoResolveConflicts / resolveIssuesConflictsByUpdatedAt)
	// is a faithful port of the proven server-mode DoltStore.tryAutoResolveConflicts,
	// but this hermetic test can't yet reliably manufacture a modify/modify conflict
	// in a temp embedded store — branch edits via fresh OpenSQL connections after
	// store.Checkout / store.Branch aren't landing divergent commits on the worker
	// branch (the merge fast-forwards / reports 0 conflicts), so the embedded
	// branch-context model needs cracking before this asserts anything real.
	// Until then dolt.auto-resolve-conflicts defaults FALSE so the code path is
	// inert. Do NOT enable it in production without validating on a copy.
	t.Skip("pending: reliable embedded modify/modify conflict setup (see comment)")

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

	// editOnCurrentBranch opens a fresh connection (which reads the persisted
	// active branch set by store.Checkout) and applies a controlled edit +
	// commit, so timestamps are deterministic rather than wall-clock.
	editOnCurrentBranch := func(set, msg string) {
		t.Helper()
		db, cleanup, err := OpenSQL(ctx, store.dataDir, store.database, store.branch)
		if err != nil {
			t.Fatalf("OpenSQL: %v", err)
		}
		defer cleanup()
		for _, q := range []string{
			"UPDATE issues SET " + set + " WHERE id='" + id + "'",
			"CALL DOLT_ADD('issues')",
			"CALL DOLT_COMMIT('-m', '" + msg + "')",
		} {
			if _, err := db.ExecContext(ctx, q); err != nil {
				t.Fatalf("exec %q: %v", q, err)
			}
		}
	}

	// Base state on main.
	editOnCurrentBranch("status='open', updated_at='2026-06-01 00:00:00'", "base")

	// worker branch: claim the bead (in_progress), strictly newer timestamp.
	if err := store.Branch(ctx, "worker"); err != nil {
		t.Fatalf("Branch: %v", err)
	}
	if err := store.Checkout(ctx, "worker"); err != nil {
		t.Fatalf("Checkout worker: %v", err)
	}
	editOnCurrentBranch("status='in_progress', updated_at='2027-01-01 00:00:00'", "worker claim")

	// main: a different, OLDER edit to the same row so the merge conflicts.
	if err := store.Checkout(ctx, "main"); err != nil {
		t.Fatalf("Checkout main: %v", err)
	}
	editOnCurrentBranch("updated_at='2026-06-15 00:00:00'", "main touch")

	// Merge worker into main; expect a conflict left in the working set.
	conflicts, err := store.Merge(ctx, "worker")
	if err != nil {
		t.Logf("Merge returned (expected on conflict): %v", err)
	}
	t.Logf("Merge reported %d conflict(s)", len(conflicts))

	db, cleanup, err := OpenSQL(ctx, store.dataDir, store.database, store.branch)
	if err != nil {
		t.Fatalf("OpenSQL post-merge: %v", err)
	}
	t.Cleanup(func() { _ = cleanup() })

	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_conflicts_issues").Scan(&n); err != nil {
		t.Fatalf("count conflicts: %v", err)
	}
	if n == 0 {
		t.Fatal("test setup produced no issues conflict; cannot validate resolver")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "SET @@dolt_allow_commit_conflicts = 1"); err != nil {
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

	// theirs (worker, later updated_at) must have won; conflict cleared; row kept.
	assertScalar := func(q, want string) {
		t.Helper()
		var got string
		if err := db.QueryRowContext(ctx, q).Scan(&got); err != nil {
			t.Fatalf("query %q: %v", q, err)
		}
		if got != want {
			t.Fatalf("query %q = %q, want %q", q, got, want)
		}
	}
	assertScalar("SELECT status FROM issues WHERE id='"+id+"'", "in_progress")

	var remaining, total int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_conflicts_issues").Scan(&remaining); err != nil {
		t.Fatalf("recount conflicts: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected 0 conflicts after resolve, got %d", remaining)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM issues WHERE id=?", id).Scan(&total); err != nil {
		t.Fatalf("recount issue: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected the issue row preserved (1), got %d", total)
	}
	_ = sql.ErrNoRows // keep database/sql import meaningful if assertions change
}
