package drivers

import (
	"fmt"
	"slices"
	"strings"
)

type SQLiteUniqueConstraint struct {
	Name string

	Columns []string

	Conflict string
}

func (c *SQLiteUniqueConstraint) Equal(other *SQLiteUniqueConstraint) bool {
	return c.Name == other.Name && c.Conflict == other.Conflict &&
		slices.Equal(c.Columns, other.Columns)
}

func (c *SQLiteUniqueConstraint) Clause() string {
	clause := fmt.Sprintf("%sUNIQUE (%s)",
		constraintNameClause(c.Name), strings.Join(QuoteIdentifiers(c.Columns), ", "))

	return clause + conflictClause(c.Conflict)
}

func conflictClause(resolution string) string {
	if resolution == "" {
		return ""
	}

	return " ON CONFLICT " + resolution
}
