package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/steveyegge/beads/internal/beads"
	"github.com/steveyegge/beads/internal/config"
	"github.com/steveyegge/beads/internal/debug"
	"github.com/steveyegge/beads/internal/storage"
)

// pullState tracks auto-pull state in a local file (.beads/pull-state.json),
// mirroring pushState. File-based instead of metadata table to avoid merge
// conflicts on multi-machine setups.
type pullState struct {
	LastPull string `json:"last_pull"` // RFC3339 timestamp
}

type autoPullTarget interface {
	Pull(ctx context.Context) error
}

// autoPullTimeout bounds the st.Pull() call so an unreachable remote does not
// block every command for the OS-default TCP timeout.
const autoPullTimeout = 30 * time.Second

func pullStatePath() (string, error) {
	beadsDir := beads.FindBeadsDir()
	if beadsDir == "" {
		return "", fmt.Errorf("%s", activeWorkspaceNotFoundError())
	}
	return filepath.Join(beadsDir, "pull-state.json"), nil
}

func loadPullState() (*pullState, error) {
	path, err := pullStatePath()
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path) //nolint:gosec // path is constructed internally
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ps pullState
	if err := json.Unmarshal(data, &ps); err != nil {
		return nil, err
	}
	return &ps, nil
}

func savePullState(ps *pullState) error {
	path, err := pullStatePath()
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(ps, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(path, data)
}

// isDoltAutoPullEnabled returns whether auto-pull from the Dolt remote should
// run. Opt-in via dolt.auto-pull=true (config.yaml or BD_DOLT_AUTO_PULL).
func isDoltAutoPullEnabled(_ context.Context) bool {
	return config.GetBool("dolt.auto-pull")
}

// pullWithContext mirrors pushWithContext: caller-side guard so a Pull that
// does not honor ctx itself cannot block forever.
func pullWithContext(ctx context.Context, target autoPullTarget) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- target.Pull(ctx)
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// maybeAutoPull pulls from the Dolt remote if enabled and the debounce
// interval has passed. Called from PersistentPreRun (before the command
// runs) so reads see fresh data and writes don't immediately diverge from
// upstream.
func maybeAutoPull(ctx context.Context, cmdName string) {
	if isSandboxMode() {
		debug.Logf("dolt auto-pull: skipped (sandbox mode)\n")
		return
	}
	if !isDoltAutoPullEnabled(ctx) {
		return
	}
	// Skip dolt sync subcommands so we don't recursively pull while the
	// user is running pull/push/fetch explicitly.
	switch cmdName {
	case "pull", "push", "fetch", "clone", "remote":
		debug.Logf("dolt auto-pull: skipped for %q\n", cmdName)
		return
	}

	st := getStore()
	if st == nil {
		return
	}
	if lm, ok := storage.UnwrapStore(st).(storage.LifecycleManager); ok && lm.IsClosed() {
		return
	}

	ps, err := loadPullState()
	if err != nil {
		debug.Logf("dolt auto-pull: failed to load pull state: %v\n", err)
		return
	}

	// Debounce: skip if we pulled recently. Default 60s — tighter than
	// auto-push's 5m because reads care about freshness.
	interval := config.GetDuration("dolt.auto-pull-interval")
	if interval == 0 {
		interval = 60 * time.Second
	}
	if ps != nil && ps.LastPull != "" {
		lastPull, err := time.Parse(time.RFC3339, ps.LastPull)
		if err == nil && time.Since(lastPull) < interval {
			debug.Logf("dolt auto-pull: throttled (last pull %s ago, interval %s)\n",
				time.Since(lastPull).Round(time.Second), interval)
			return
		}
	}

	pullTimeout := config.GetDuration("dolt.auto-pull-timeout")
	if pullTimeout == 0 {
		pullTimeout = autoPullTimeout
	}
	pullCtx, pullCancel := context.WithTimeout(ctx, pullTimeout)
	defer pullCancel()

	debug.Logf("dolt auto-pull: pulling from origin (timeout %s)...\n", pullTimeout)
	pullErr := tryRemoteCLIPushPull(pullCtx, "pull", "origin", "")
	if errors.Is(pullErr, errRemoteCLINotApplicable) {
		pullErr = pullWithContext(pullCtx, st)
	}
	if err := pullErr; err != nil {
		if !isQuiet() && !jsonOutput {
			if pullCtx.Err() == context.DeadlineExceeded {
				fmt.Fprintf(os.Stderr, "Warning: dolt auto-pull timed out after %s (remote may be unreachable)\n", pullTimeout)
			} else {
				fmt.Fprintf(os.Stderr, "Warning: dolt auto-pull failed: %v\n", err)
			}
		}
		debug.Logf("dolt auto-pull: pull error: %v\n", err)
		// Throttle retries after failure so a hanging remote doesn't make every
		// subsequent bd command pay the pull timeout.
		if ps == nil {
			ps = &pullState{}
		}
		ps.LastPull = time.Now().UTC().Format(time.RFC3339)
		if saveErr := savePullState(ps); saveErr != nil {
			debug.Logf("dolt auto-pull: failed to save pull state after error: %v\n", saveErr)
		}
		return
	}

	if err := savePullState(&pullState{LastPull: time.Now().UTC().Format(time.RFC3339)}); err != nil {
		debug.Logf("dolt auto-pull: failed to save pull state: %v\n", err)
	}
	debug.Logf("dolt auto-pull: pulled successfully\n")
}
