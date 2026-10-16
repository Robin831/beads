package issueops

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestEmptyWispTablesLeavesNoWispTableInTheIssueTemplates(t *testing.T) {
	for name, tmpl := range map[string]string{"mark": markBlockedTemplateForIssues(), "unmark": unmarkBlockedTemplateForIssues()} {
		got := emptyWispTables(tmpl)
		if regexp.MustCompile(`\b(FROM|JOIN)\s+wisp(s|_dependencies)\b`).MatchString(got) {
			t.Errorf("%s: a wisp table is still read after the replacement:\n%s", name, got)
		}
		if !strings.Contains(got, "WHERE FALSE") {
			t.Errorf("%s: no empty table was put in", name)
		}
	}
}

func TestEmptyWispTablesToleratesAnyWhitespace(t *testing.T) {
	got := emptyWispTables("SELECT 1 FROM\n\t\twisp_dependencies cd JOIN   wisps\nchild ON child.id = cd.issue_id")
	if regexp.MustCompile(`\b(FROM|JOIN)\s+wisp(s|_dependencies)\b`).MatchString(got) {
		t.Fatalf("a wisp table is still read after the replacement:\n%s", got)
	}
	if !strings.Contains(got, "FROM (SELECT issue_id") || !strings.Contains(got, "JOIN (SELECT id, status, is_blocked") || !strings.Contains(got, " cd JOIN") || !strings.Contains(got, "\nchild ON") {
		t.Fatalf("aliases and the rest of the statement must stay:\n%s", got)
	}
}

func TestExecWithOptionalWispsRetriesOnlyAnAbsentWispTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	query := "UPDATE issues i SET i.is_blocked = 0 WHERE i.id IN (?) AND NOT EXISTS (SELECT 1 FROM dependencies d JOIN wisps t ON t.id = d.depends_on_wisp_id)"
	mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs("a").WillReturnError(errors.New("Error 1146 (HY000): table not found: wisps"))
	mock.ExpectExec(`JOIN \(SELECT id, status, is_blocked FROM issues WHERE FALSE\) t`).WithArgs("a").WillReturnResult(sqlmock.NewResult(0, 1))
	if _, err := execWithOptionalWispsInTx(context.Background(), db, query, "a"); err != nil {
		t.Fatalf("absent wisps: %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs("b").WillReturnError(errors.New("Error 1146 (HY000): table not found: issues"))
	if _, err := execWithOptionalWispsInTx(context.Background(), db, query, "b"); err == nil {
		t.Fatal("a missing issues table must fail, not be retried")
	}
	mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs("c").WillReturnError(errors.New("Error 1105 (HY000): command denied"))
	if _, err := execWithOptionalWispsInTx(context.Background(), db, query, "c"); err == nil {
		t.Fatal("another error must fail, not be retried")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
