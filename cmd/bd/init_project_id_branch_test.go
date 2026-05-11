package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// fakeBranchMetadataReader implements branchMetadataReader for unit tests.
// Per-branch metadata is keyed by branch then by metadata key.
type fakeBranchMetadataReader struct {
	branches     []string
	listErr      error
	metadata     map[string]map[string]string // branch -> key -> value
	getErrBranch string                       // when set, GetMetadataOnBranch returns an error for this branch
}

func (f *fakeBranchMetadataReader) ListBranches(ctx context.Context) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.branches, nil
}

func (f *fakeBranchMetadataReader) GetMetadataOnBranch(ctx context.Context, branch, key string) (string, error) {
	if branch == f.getErrBranch {
		return "", errors.New("simulated read error")
	}
	if b, ok := f.metadata[branch]; ok {
		return b[key], nil
	}
	return "", nil
}

func TestFindProjectIDOnAnyBranch(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		reader     *fakeBranchMetadataReader
		wantID     string
		wantBranch string
		wantErr    string // substring; "" means no error expected
	}{
		{
			name:   "no branches",
			reader: &fakeBranchMetadataReader{branches: nil},
		},
		{
			name: "no _project_id on any branch",
			reader: &fakeBranchMetadataReader{
				branches: []string{"main", "beads-sync"},
				metadata: map[string]map[string]string{
					"main":       {"clone_id": "abc"},
					"beads-sync": {"clone_id": "abc"},
				},
			},
		},
		{
			name: "identity lives on non-default sync branch (the production bug)",
			reader: &fakeBranchMetadataReader{
				branches: []string{"main", "beads-sync"},
				metadata: map[string]map[string]string{
					"main":       {"clone_id": "abc"},
					"beads-sync": {"_project_id": "665d5573-e460-41fa-b657-50a25f546aca"},
				},
			},
			wantID:     "665d5573-e460-41fa-b657-50a25f546aca",
			wantBranch: "beads-sync",
		},
		{
			name: "identity on multiple branches but matching",
			reader: &fakeBranchMetadataReader{
				branches: []string{"main", "beads-sync", "feature-x"},
				metadata: map[string]map[string]string{
					"main":       {"_project_id": "same-id"},
					"beads-sync": {"_project_id": "same-id"},
					"feature-x":  {"_project_id": "same-id"},
				},
			},
			wantID:     "same-id",
			wantBranch: "main",
		},
		{
			name: "branches disagree — refuse to pick one",
			reader: &fakeBranchMetadataReader{
				branches: []string{"main", "beads-sync"},
				metadata: map[string]map[string]string{
					"main":       {"_project_id": "id-A"},
					"beads-sync": {"_project_id": "id-B"},
				},
			},
			wantErr: "2 distinct _project_id values",
		},
		{
			name: "list-branches error degrades to no-op (caller mints fresh)",
			reader: &fakeBranchMetadataReader{
				listErr: errors.New("transient list failure"),
			},
		},
		{
			name: "read error on one branch does not poison overall scan",
			reader: &fakeBranchMetadataReader{
				branches: []string{"main", "beads-sync", "broken-branch"},
				metadata: map[string]map[string]string{
					"beads-sync": {"_project_id": "good-id"},
				},
				getErrBranch: "broken-branch",
			},
			wantID:     "good-id",
			wantBranch: "beads-sync",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, branch, err := findProjectIDOnAnyBranch(ctx, tc.reader)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil (id=%q branch=%q)", tc.wantErr, id, branch)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if id != tc.wantID {
				t.Errorf("id = %q, want %q", id, tc.wantID)
			}
			if branch != tc.wantBranch {
				t.Errorf("branch = %q, want %q", branch, tc.wantBranch)
			}
		})
	}
}
