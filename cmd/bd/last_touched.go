package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/steveyegge/beads/internal/beads"
)

const lastTouchedFile = "last-touched"

// lastTouchedIDThisRun holds the ID set during this process invocation only.
// Unlike GetLastTouchedID (which reads a persistent file), this is empty at
// startup and only populated when SetLastTouchedID is called in the current run.
var lastTouchedIDThisRun string

// GetLastTouchedIDThisRun returns the issue ID set by SetLastTouchedID during
// THIS process invocation. Returns empty string if no write command ran yet.
// Use this instead of GetLastTouchedID() when you only want to act on changes
// made in the current command (e.g., auto-sync should not fire on read commands).
func GetLastTouchedIDThisRun() string {
	return lastTouchedIDThisRun
}

// GetLastTouchedID returns the ID of the last touched issue.
// Returns empty string if no last touched issue exists or the file is unreadable.
func GetLastTouchedID() string {
	beadsDir := beads.FindBeadsDir()
	if beadsDir == "" {
		return ""
	}

	lastTouchedPath := filepath.Join(beadsDir, lastTouchedFile)
	data, err := os.ReadFile(lastTouchedPath) // #nosec G304 -- path constructed from beadsDir
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(data))
}

// SetLastTouchedID saves the ID of the last touched issue.
// Silently ignores errors (best-effort tracking).
func SetLastTouchedID(issueID string) {
	if issueID == "" {
		return
	}

	// Track in-memory for this run (used by maybeAutoGitHubSync to avoid
	// firing on read-only commands that inherit a stale file value).
	lastTouchedIDThisRun = issueID

	beadsDir := beads.FindBeadsDir()
	if beadsDir == "" {
		return
	}

	lastTouchedPath := filepath.Join(beadsDir, lastTouchedFile)
	// Write with restrictive permissions (local-only state)
	_ = os.WriteFile(lastTouchedPath, []byte(issueID+"\n"), 0600)
}

// ClearLastTouched removes the last touched file.
// Silently ignores errors.
func ClearLastTouched() {
	beadsDir := beads.FindBeadsDir()
	if beadsDir == "" {
		return
	}

	lastTouchedPath := filepath.Join(beadsDir, lastTouchedFile)
	_ = os.Remove(lastTouchedPath)
}
