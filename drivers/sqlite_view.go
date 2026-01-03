package drivers

import (
	"fmt"
	"strings"
)

type SQLiteView struct {
	Name string
	SQL  string
}

func (v *SQLiteView) Diff(other *SQLiteView) (string, error) {
	var diff strings.Builder

	if v.SQL != other.SQL {
		// Modified view
		fmt.Fprintf(&diff, "DROP VIEW \"%s\";\n", other.Name)
		fmt.Fprintf(&diff, "%s;\n", v.SQL)
	}

	return strings.TrimSpace(diff.String()), nil
}
