package main

import (
	"context"
	"fmt"
	"os"

	"github.com/steveyegge/beads/internal/debug"
	"github.com/steveyegge/beads/internal/github"
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
		// Token or owner/repo not configured — warn so users know why sync was skipped.
		// Most common cause: github.token only set as env var, not via 'bd config set'.
		fmt.Fprintf(os.Stderr, "Warning: github autosync skipped (not configured): %v\n", err)
		fmt.Fprintf(os.Stderr, "  Run: bd config set github.token <your-token>\n")
		return
	}

	if err := pushSingleIssueToGitHub(ctx, issueID); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: github autosync failed for %s: %v\n", issueID, err)
	}
}

// pushSingleIssueToGitHub creates or updates exactly one bead in GitHub without
// performing any bulk list/search operations. It fetches the issue directly by ID,
// then calls CreateIssue or UpdateIssue on the GitHub tracker as needed.
func pushSingleIssueToGitHub(ctx context.Context, issueID string) error {
	issue, err := store.GetIssue(ctx, issueID)
	if err != nil {
		return fmt.Errorf("fetching issue %s: %w", issueID, err)
	}
	if issue == nil {
		return fmt.Errorf("issue %s not found", issueID)
	}

	gt := &github.Tracker{}
	if err := gt.Init(ctx, store); err != nil {
		return fmt.Errorf("initializing GitHub tracker: %w", err)
	}

	extRef := ""
	if issue.ExternalRef != nil {
		extRef = *issue.ExternalRef
	}

	if extRef == "" || !gt.IsExternalRef(extRef) {
		// No external ref yet — create a new GitHub issue.
		created, err := gt.CreateIssue(ctx, issue)
		if err != nil {
			return fmt.Errorf("creating GitHub issue for %s: %w", issueID, err)
		}
		ref := gt.BuildExternalRef(created)
		if err := store.UpdateIssue(ctx, issueID, map[string]interface{}{"external_ref": ref}, actor); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: github autosync: failed to save external_ref for %s: %v\n", issueID, err)
		}
	} else {
		// Already has a GitHub reference — update the existing issue.
		extID := gt.ExtractIdentifier(extRef)
		if extID == "" {
			return fmt.Errorf("could not extract GitHub issue number from %q", extRef)
		}
		if _, err := gt.UpdateIssue(ctx, extID, issue); err != nil {
			return fmt.Errorf("updating GitHub issue for %s: %w", issueID, err)
		}
	}
	return nil
}

