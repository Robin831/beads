package schema

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
)

// mainWorkNeeded is migrationWorkNeeded without the ignored (clone-local) series: what the versioned schema itself
// still needs.
var mainWorkNeeded = func(ctx context.Context, db DBConn) (bool, error) {
	if !mainSource.atLatest(ctx, db) {
		return true, nil
	}
	hasMainHash, err := mainSource.hasContentHashColumn(ctx, db)
	if err != nil {
		return false, err
	}
	if !hasMainHash {
		return true, nil
	}
	return needsBackfilledCustomStatusesCustomTypes(ctx, db)
}

// userMayCreateTables tries the ignored series' own first statement (its cursor table, clone-local and created
// IF NOT EXISTS). Privileges are not read from SHOW GRANTS: Dolt refuses that to a user without access to mysql.
// A function variable so the skip decision can be tested without a live server.
var userMayCreateTables = func(ctx context.Context, db DBConn) (bool, error) {
	if _, err := db.ExecContext(ctx, ignoredSource.bootstrapSQL()); err != nil {
		if isPrivilegeDenied(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// isPrivilegeDenied matches the server's refusal of a statement for want of a privilege (MySQL 1142/1044, and Dolt's
// 1105 "command denied").
func isPrivilegeDenied(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "command denied") || strings.Contains(s, "error 1142") || strings.Contains(s, "error 1044")
}

var skipNoteOnce sync.Once

// skipLocalSeries: only the ignored series is behind and this user cannot create its tables, so the pass is skipped
// (noted once per process). Any error in deciding keeps the old behaviour.
func skipLocalSeries(ctx context.Context, db DBConn) bool {
	if mainNeeded, err := mainWorkNeeded(ctx, db); err != nil || mainNeeded {
		return false
	}
	if may, err := userMayCreateTables(ctx, db); err != nil || may {
		return false
	}
	skipNoteOnce.Do(func() {
		var database string
		if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&database); err != nil {
			database = "this database"
		}
		log.Print(localSeriesSkipNote(database))
	})
	return true
}

func localSeriesSkipNote(database string) string {
	return fmt.Sprintf("schema migration: bd's clone-local tables (wisps, local metadata) are missing or behind on %s and this "+
		"Dolt user may not create them; opening without them (wisp commands fail until a user with CREATE opens this database)", database)
}
