package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/steveyegge/beads/internal/beads"
	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/debug"
)

// errRemoteCLINotApplicable is a sentinel returned by tryRemoteCLI* when no
// CLI fallback should be attempted (DOLT_REMOTE_USER unset, no embedded
// dolt dir, etc.). The caller should fall through to the SQL path.
var errRemoteCLINotApplicable = errors.New("dolt CLI fallback not applicable")

// readCurrentDoltBranch returns the current branch name from a Dolt repo
// directory (the parent of `.dolt`). It parses `.dolt/repo_state.json`'s
// `head` field, which holds a value like `refs/heads/<name>`.
// Note: this is Dolt's own state file, not a git-style HEAD reference.
func readCurrentDoltBranch(repoDir string) (string, error) {
	statePath := filepath.Join(repoDir, ".dolt", "repo_state.json")
	data, err := os.ReadFile(statePath) //nolint:gosec // path constructed from caller-provided dir
	if err != nil {
		return "", err
	}
	var state struct {
		Head string `json:"head"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return "", fmt.Errorf("parse repo_state.json: %w", err)
	}
	const prefix = "refs/heads/"
	if !strings.HasPrefix(state.Head, prefix) {
		return "", fmt.Errorf("unexpected head format: %q", state.Head)
	}
	return strings.TrimPrefix(state.Head, prefix), nil
}

// resolveEmbeddedDoltDir returns the directory that holds the embedded Dolt
// repo for this workspace, suitable as cwd for `dolt push`/`dolt pull`.
// Returns "" if the embedded layout cannot be located.
func resolveEmbeddedDoltDir() string {
	beadsDir := beads.FindBeadsDir()
	if beadsDir == "" {
		return ""
	}

	dataDir := filepath.Join(beadsDir, "embeddeddolt")
	dbName := ""
	if cfg, err := configfile.Load(beadsDir); err == nil && cfg != nil {
		dbName = strings.TrimSpace(cfg.GetDoltDatabase())
	}
	if dbName == "" {
		// Fallback: pick the only subdirectory that contains a .dolt folder.
		entries, err := os.ReadDir(dataDir)
		if err != nil {
			return ""
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			candidate := filepath.Join(dataDir, e.Name())
			if _, err := os.Stat(filepath.Join(candidate, ".dolt")); err == nil {
				if dbName != "" {
					// Multiple candidates — ambiguous.
					return ""
				}
				dbName = e.Name()
			}
		}
		if dbName == "" {
			return ""
		}
	}
	dir := filepath.Join(dataDir, dbName)
	if _, err := os.Stat(filepath.Join(dir, ".dolt")); err != nil {
		return ""
	}
	return dir
}

// tryRemoteCLIPushPull runs `dolt <op> <remote> <branch>` from the embedded
// dolt directory, passing --user=$DOLT_REMOTE_USER so DOLT_REMOTE_PASSWORD
// authenticates against a remotesapi that requires username/password. The
// embedded engine's `CALL DOLT_PUSH/PULL` does not surface DOLT_REMOTE_USER
// to its outbound gRPC client, which produces "Access denied for user 'root'"
// errors against authenticated remotes.
//
// Returns errRemoteCLINotApplicable when DOLT_REMOTE_USER is unset or the
// embedded layout can't be located; the caller should then fall through to
// the SQL path.
func tryRemoteCLIPushPull(ctx context.Context, op, remote, branch string) error {
	if op != "push" && op != "pull" {
		return fmt.Errorf("tryRemoteCLIPushPull: unsupported op %q", op)
	}
	user := strings.TrimSpace(os.Getenv("DOLT_REMOTE_USER"))
	if user == "" {
		return errRemoteCLINotApplicable
	}
	dir := resolveEmbeddedDoltDir()
	if dir == "" {
		return errRemoteCLINotApplicable
	}

	// Resolve the branch to push/pull. Caller may pass empty to mean
	// "current branch" — we read .dolt/HEAD to get its name. dolt push
	// without an explicit branch errors with a "set-upstream" hint even
	// when the branch already tracks an upstream, so we pass it always.
	branchName := strings.TrimSpace(branch)
	if branchName == "" {
		if b, err := readCurrentDoltBranch(dir); err == nil {
			branchName = b
		}
	}

	args := []string{op, "--user=" + user}
	// dolt push insists on --set-upstream even when the local branch's
	// tracking ref is already set. The flag is idempotent (re-setting an
	// existing tracking ref to the same value is a no-op), so we always
	// pass it for push. Pull doesn't need --set-upstream.
	if op == "push" {
		args = append(args, "--set-upstream")
	}
	args = append(args, remote)
	if branchName != "" {
		args = append(args, branchName)
	}
	debug.Logf("dolt %s via CLI: cwd=%s args=%v\n", op, dir, args)
	cmd := exec.CommandContext(ctx, "dolt", args...)
	cmd.Dir = dir
	// Inherit env so DOLT_REMOTE_PASSWORD propagates.
	out, err := cmd.CombinedOutput()
	if err != nil {
		// Dolt push/pull sometimes exits non-zero on cosmetic conditions
		// (spinner cleanup, "nothing to push") even when the operation
		// succeeded. Check the output for an explicit success marker before
		// reporting failure.
		outStr := string(out)
		successful := strings.Contains(outStr, "[new branch]") ||
			strings.Contains(outStr, "Everything up-to-date") ||
			strings.Contains(outStr, "Already up to date") ||
			strings.Contains(outStr, "branch '") && strings.Contains(outStr, "set up to track") ||
			strings.Contains(outStr, "->") // ref-update line, e.g. "abc..def main -> main"
		if successful {
			debug.Logf("dolt %s via CLI: dolt exited %v but output indicates success\n", op, err)
			return nil
		}
		return fmt.Errorf("dolt %s via CLI failed: %w\nOutput: %s", op, err, outStr)
	}
	debug.Logf("dolt %s via CLI succeeded\n", op)
	return nil
}
