package drivers

import (
	"fmt"
	"slices"
	"strings"
)

// A SQLiteUniqueConstraint is a UNIQUE constraint of two or more columns. A constraint of
// one column belongs to the definition of that column.
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
		constraintNameClause(c.Name), strings.Join(quoteIdentifiers(c.Columns), ", "))

	return clause + conflictClause(c.Conflict)
}

// conflictClause returns the ON CONFLICT clause of a constraint, with a leading space. An
// empty resolution gives an empty text.
func conflictClause(resolution string) string {
	if resolution == "" {
		return ""
	}

	return " ON CONFLICT " + resolution
}
