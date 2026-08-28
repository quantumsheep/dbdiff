package drivers

import (
	"fmt"
	"slices"
	"strings"
)

type SQLiteUniqueConstraint struct {
	Name string

	Columns []string

	// A key clause holds the quoted column name with its COLLATE clause and its DESC
	// keyword, which no PRAGMA reports. An empty list keeps the plain names of Columns.
	Keys []string

	Conflict string
}

func (c *SQLiteUniqueConstraint) Equal(other *SQLiteUniqueConstraint) bool {
	return c.Name == other.Name && c.Conflict == other.Conflict &&
		slices.Equal(c.Columns, other.Columns) &&
		slices.Equal(c.Keys, other.Keys)
}

func (c *SQLiteUniqueConstraint) Clause() string {
	keys := QuoteIdentifiers(c.Columns)
	if len(c.Keys) == len(c.Columns) {
		keys = c.Keys
	}

	clause := fmt.Sprintf("%sUNIQUE (%s)",
		constraintNameClause(c.Name), strings.Join(keys, ", "))

	return clause + conflictClause(c.Conflict)
}

func conflictClause(resolution string) string {
	if resolution == "" {
		return ""
	}

	return " ON CONFLICT " + resolution
}
