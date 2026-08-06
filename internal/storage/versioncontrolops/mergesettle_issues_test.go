package versioncontrolops

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// issuesConflictsAreTimestampResolvable is the gate that decides whether
// TryAutoResolveMergeConflicts is allowed to mutate the issues table. Because
// the resolver it guards silently discards the losing side's edit on every
// auto-pull, the "no" cases matter more than the "yes" ones: each of them is a
// row a human must look at, and a false positive here loses data quietly.
func TestIssuesConflictsAreTimestampResolvable(t *testing.T) {
	older := time.Date(2026, 8, 5, 11, 33, 21, 0, time.UTC)
	newer := time.Date(2026, 8, 5, 11, 35, 21, 0, time.UTC)

	cols := []string{"our_updated_at", "their_updated_at", "our_diff_type", "their_diff_type"}

	tests := []struct {
		name string
		rows *sqlmock.Rows
		want bool
	}{
		{
			// The real skybert-forge shape (Fhi.Metadata-ehdyn): forge wrote
			// in_progress at 11:35, the other clone touched the row at 11:33.
			// Ours is newer, so ours wins - which matched the open-PR truth.
			name: "ours newer resolves",
			rows: sqlmock.NewRows(cols).AddRow(newer, older, "modified", "modified"),
			want: true,
		},
		{
			name: "theirs newer resolves",
			rows: sqlmock.NewRows(cols).AddRow(older, newer, "modified", "modified"),
			want: true,
		},
		{
			name: "every row must be resolvable, not just the first",
			rows: sqlmock.NewRows(cols).
				AddRow(newer, older, "modified", "modified").
				AddRow(older, older, "modified", "modified"),
			want: false,
		},
		{
			// Deleting a bead vs editing it is a semantic call; a timestamp
			// cannot express "keep it" vs "it should be gone".
			name: "their delete vs our modify needs a human",
			rows: sqlmock.NewRows(cols).AddRow(newer, older, "modified", "removed"),
			want: false,
		},
		{
			name: "our delete vs their modify needs a human",
			rows: sqlmock.NewRows(cols).AddRow(newer, older, "removed", "modified"),
			want: false,
		},
		{
			// No timestamp means nothing to compare - falling back to a side
			// would be an arbitrary choice dressed up as a rule.
			name: "null our_updated_at needs a human",
			rows: sqlmock.NewRows(cols).AddRow(nil, newer, "modified", "modified"),
			want: false,
		},
		{
			name: "null their_updated_at needs a human",
			rows: sqlmock.NewRows(cols).AddRow(newer, nil, "modified", "modified"),
			want: false,
		},
		{
			// The rows differ (there is a conflict) but the clocks agree, so
			// there is no later write to prefer. Breaking the tie would be
			// picking a winner at random.
			name: "equal timestamps on differing rows needs a human",
			rows: sqlmock.NewRows(cols).AddRow(newer, newer, "modified", "modified"),
			want: false,
		},
		{
			// Guards against reporting "resolvable" for an empty conflict set,
			// which would let the caller stage and commit nothing.
			name: "no conflicting rows is not resolvable",
			rows: sqlmock.NewRows(cols),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()

			mock.ExpectQuery("dolt_conflicts_issues").WillReturnRows(tt.rows)

			got, err := issuesConflictsAreTimestampResolvable(context.Background(), db)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("issuesConflictsAreTimestampResolvable = %v, want %v", got, tt.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

// An empty column lookup must abort rather than build an UPDATE with no SET
// clause: a silently-degenerate resolve would accept --ours for every row and
// drop the remote side wholesale, which is the precise data loss this resolver
// exists to prevent.
func TestResolveIssuesConflictsByUpdatedAt_EmptyColumnLookupAborts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("INFORMATION_SCHEMA.COLUMNS").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}))

	if err := resolveIssuesConflictsByUpdatedAt(context.Background(), db); err == nil {
		t.Fatal("expected an error when the column lookup returns nothing, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
