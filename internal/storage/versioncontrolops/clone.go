package versioncontrolops

import (
	"context"
	"fmt"
	"net/url"
)

// DoltClone clones a Dolt database from a remote URL.
// conn must be a non-transactional database connection.
// The database parameter specifies the local database name for the clone.
//
// user is the remote auth user (DOLT_REMOTE_USER). When non-empty it is passed
// as --user, which is what makes DOLT_REMOTE_PASSWORD take effect — exactly as
// Push and Pull already do in remotes.go. Without it, an authenticated remote
// refuses the clone with "Access denied for user 'root'", which made
// `bd bootstrap` unusable against our dolt-beads server — and `bd bootstrap` is
// the documented way for every non-migrating clone to adopt a migrated
// database, so the whole adopt path was blocked.
func DoltClone(ctx context.Context, conn DBConn, remoteURL, database, user string) error {
	var err error
	if user != "" {
		_, err = conn.ExecContext(ctx, "CALL DOLT_CLONE('--user', ?, ?, ?)", user, remoteURL, database)
	} else {
		_, err = conn.ExecContext(ctx, "CALL DOLT_CLONE(?, ?)", remoteURL, database)
	}
	if err != nil {
		return fmt.Errorf("dolt clone %s: %w", sanitizeURL(remoteURL), err)
	}
	return nil
}

// sanitizeURL removes credentials from a URL for safe error reporting.
func sanitizeURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}
