package main

import (
	"context"
	"fmt"
	"os"

	"github.com/steveyegge/beads/internal/debug"
	"github.com/steveyegge/beads/internal/tracker"
)

// maybeAutoGitHubSync pushes a single changed issue to GitHub if auto-sync is enabled.
// It is called from PersistentPostRun after every successful write command, using the
// last-touched issue ID so it fires automatically after bd create/update/close.
//
// Enable with: bd config set github.sync.auto true
// Requires github.token, github.owner/repo to be configured.
func maybeAutoGitHubSync(ctx context.Context, issueID string) {
	// Read github.sync.auto from the Dolt store (set via 'bd config set').
	// Fall back to a no-op if the store isn't open (e.g., in read-only mode).
	if store == nil {
		return
	}
	autoSync, _ := store.GetConfig(ctx, "github.sync.auto")
	if autoSync != "true" {
		debug.Logf("github autosync: disabled (github.sync.auto is not true)\n")
		return
	}

	if issueID == "" {
		debug.Logf("github autosync: no issue ID to sync\n")
		return
	}

	ghConfig := getGitHubConfig()
	if err := validateGitHubConfig(ghConfig); err != nil {
		// Not configured — silently skip, don't spam stderr on every command.
		debug.Logf("github autosync: not configured (%v)\n", err)
		return
	}

	engine, err := buildGitHubEngine(ctx, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: github autosync init failed: %v\n", err)
		return
	}

	opts := tracker.SyncOptions{Push: true}
	if err := engine.SyncOne(ctx, issueID, opts); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: github autosync failed for %s: %v\n", issueID, err)
	}
}
