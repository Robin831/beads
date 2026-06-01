package main

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/term"

	"github.com/steveyegge/beads/internal/beads"
)

const lastTouchedFile = "last-touched"

// lastTouchedFallbackEnv overrides the no-ID "last touched issue" fallback
// on mutating commands: 1/true always allows it, 0/false always denies it.
const lastTouchedFallbackEnv = "BD_LAST_TOUCHED_FALLBACK"

// AllowLastTouchedFallback reports whether a mutating command (update,
// close) may substitute the last-touched issue when no ID was given.
//
// The fallback is convenient at an interactive prompt but dangerous in
// scripts: `bd update "$ID" ...` with an accidentally empty $ID silently
// mutates whatever issue happened to be touched last (bd-m00pb; a real
// agent session corrupted an unrelated closed issue this way). Read-only
// consumers like `bd show --current` are unaffected.
//
// Precedence mirrors isNonInteractiveBootstrap:
// BD_LAST_TOUCHED_FALLBACK (any explicit value wins, only 1/true enables) >
// BD_NON_INTERACTIVE / CI (deny) > stdin terminal detection.
func AllowLastTouchedFallback() bool {
	if v := os.Getenv(lastTouchedFallbackEnv); v != "" {
		return v == "1" || v == "true"
	}
	if v := os.Getenv("BD_NON_INTERACTIVE"); v == "1" || v == "true" {
		return false
	}
	if v := os.Getenv("CI"); v == "1" || v == "true" {
		return false
	}
	return term.IsTerminal(int(os.Stdin.Fd()))
}

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
	if err := os.WriteFile(lastTouchedPath, []byte(issueID+"\n"), 0600); err != nil {
		return
	}
	// Always advance mtime, even when the same ID is rewritten, so file-watch
	// fingerprints and cache validators that key on mtime never see an
	// "identical" marker after a write (GH#3965).
	now := time.Now()
	_ = os.Chtimes(lastTouchedPath, now, now)
}

// RecordViewedID writes the persistent last-touched file but does NOT
// update the in-process sync trigger. Use from read-only commands like
// `bd show` so subsequent `bd update`/`bd close` without an ID can resolve
// to this issue, without firing GitHub auto-sync (which keys off
// GetLastTouchedIDThisRun).
func RecordViewedID(issueID string) {
	if issueID == "" {
		return
	}
	beadsDir := beads.FindBeadsDir()
	if beadsDir == "" {
		return
	}
	lastTouchedPath := filepath.Join(beadsDir, lastTouchedFile)
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
