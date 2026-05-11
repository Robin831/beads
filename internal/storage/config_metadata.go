package storage

import (
	"context"

	"github.com/steveyegge/beads/internal/types"
)

// ConfigMetadataStore provides extended config, metadata, and type introspection.
type ConfigMetadataStore interface {
	GetMetadata(ctx context.Context, key string) (string, error)
	SetMetadata(ctx context.Context, key, value string) error
	// GetMetadataOnBranch reads a metadata value as observed on a specific
	// Dolt branch (via AS OF). Returns ("", nil) if the key does not exist
	// on that branch. Used by `bd init --remote` to discover an existing
	// _project_id that lives on a non-default sync branch (e.g. beads-sync)
	// rather than the cloned default branch.
	GetMetadataOnBranch(ctx context.Context, branch, key string) (string, error)
	DeleteConfig(ctx context.Context, key string) error
	GetCustomStatuses(ctx context.Context) ([]string, error)
	GetCustomStatusesDetailed(ctx context.Context) ([]types.CustomStatus, error)
	GetCustomTypes(ctx context.Context) ([]string, error)
	GetInfraTypes(ctx context.Context) map[string]bool
	IsInfraTypeCtx(ctx context.Context, t types.IssueType) bool
}
