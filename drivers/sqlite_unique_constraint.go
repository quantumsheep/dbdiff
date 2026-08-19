package drivers

import (
	"fmt"
	"slices"
	"strings"
)

// A SQLiteUniqueConstraint is a UNIQUE constraint of two or more columns. A constraint of
// one column belongs to the definition of that column.
type SQLiteUniqueConstraint struct {
	Columns []string

	// Conflict holds the resolution of the ON CONFLICT clause, for example FAIL. It is
	// empty for a constraint that holds no such clause.
	Conflict string
}

func (c *SQLiteUniqueConstraint) Equal(other *SQLiteUniqueConstraint) bool {
	return c.Conflict == other.Conflict && slices.Equal(c.Columns, other.Columns)
}

func (c *SQLiteUniqueConstraint) Clause() string {
	clause := fmt.Sprintf("UNIQUE (%s)", strings.Join(quoteIdentifiers(c.Columns), ", "))

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
