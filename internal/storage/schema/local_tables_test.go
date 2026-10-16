package schema

import (
	"errors"
	"testing"
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
