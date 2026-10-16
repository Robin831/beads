package schema

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestIsPrivilegeDenied(t *testing.T) {
	for _, c := range []struct {
		err  error
		want bool
	}{
		{errors.New("Error 1105 (HY000): command denied to user 'vw'@'%'"), true},
		{errors.New("Error 1142 (42000): CREATE command denied to user 'w'@'%' for table 'x'"), true},
		{errors.New("Error 1044 (42000): Access denied for user 'w'@'%' to database 'munin'"), true},
		{errors.New("Error 1105 (HY000): table not found: wisps"), false},
		{errors.New("dial tcp: connection refused"), false},
	} {
		if got := isPrivilegeDenied(c.err); got != c.want {
			t.Errorf("isPrivilegeDenied(%q) = %v, want %v", c.err, got, c.want)
		}
	}
}

func stubLocalSeries(t *testing.T, mainNeeded bool, mainErr error, mayCreate bool, createErr error) {
	t.Helper()
	oldMain, oldMay := mainWorkNeeded, userMayCreateTables
	t.Cleanup(func() { mainWorkNeeded, userMayCreateTables = oldMain, oldMay })
	mainWorkNeeded = func(context.Context, DBConn) (bool, error) { return mainNeeded, mainErr }
	userMayCreateTables = func(context.Context, DBConn) (bool, error) { return mayCreate, createErr }
}

func TestSkipLocalSeries(t *testing.T) {
	cases := []struct {
		name       string
		mainNeeded bool
		mainErr    error
		mayCreate  bool
		createErr  error
		want       bool
	}{
		{"only the local series behind, no CREATE: skip", false, nil, false, nil, true},
		{"only the local series behind, CREATE allowed: run the pass", false, nil, true, nil, false},
		{"main work too: run the pass (and fail loudly if refused)", true, nil, false, nil, false},
		{"main check errors: old behaviour", false, errors.New("boom"), false, nil, false},
		{"probe errors for another reason: old behaviour", false, nil, false, errors.New("connection refused"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stubLocalSeries(t, c.mainNeeded, c.mainErr, c.mayCreate, c.createErr)
			// The note's SELECT DATABASE() finds no expectation and falls back to "this database".
			db, _, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if got := skipLocalSeries(context.Background(), db); got != c.want {
				t.Fatalf("skipLocalSeries = %v, want %v", got, c.want)
			}
		})
	}
}

func TestUserMayCreateTablesClassifiesTheProbe(t *testing.T) {
	for _, c := range []struct {
		err  error
		may  bool
		fail bool
	}{
		{nil, true, false},
		{errors.New("Error 1105 (HY000): command denied to user 'vw'@'%'"), false, false},
		{errors.New("dial tcp: connection refused"), false, true},
	} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		exec := mock.ExpectExec(`CREATE TABLE IF NOT EXISTS ignored_schema_migrations`)
		if c.err != nil {
			exec.WillReturnError(c.err)
		} else {
			exec.WillReturnResult(sqlmock.NewResult(0, 0))
		}
		may, err := userMayCreateTables(context.Background(), db)
		if may != c.may || (err != nil) != c.fail {
			t.Errorf("probe error %v: got (%v, %v), want (%v, failure %v)", c.err, may, err, c.may, c.fail)
		}
		db.Close()
	}
}

func TestMigrateSkipsBootstrapWhenTheCursorExists(t *testing.T) {
	old := sentinelTableExists
	t.Cleanup(func() { sentinelTableExists = old })
	sentinelTableExists = func(context.Context, DBConn, string) (bool, error) { return true, nil }
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// After the existence check the next statement is the content_hash probe, not CREATE TABLE.
	mock.ExpectQuery(`INFORMATION_SCHEMA.COLUMNS`).WillReturnError(errors.New("stop here"))
	if _, _, err := ignoredSource.migrate(context.Background(), db, 0); err == nil || !strings.Contains(err.Error(), "stop here") {
		t.Fatalf("migrate = %v, want the content_hash probe's error (no CREATE first)", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
