package drivers

import (
	"fmt"
	"slices"
	"strings"
)

// A SQLiteUniqueConstraint is a UNIQUE constraint of two or more columns. A constraint of
// one column belongs to the definition of that column.
type SQLiteUniqueConstraint struct {
	// Name holds the name of the constraint. It is empty for a constraint that the schema
	// declares with no name.
	Name string

	Columns []string

	// Conflict holds the resolution of the ON CONFLICT clause, for example FAIL. It is
	// empty for a constraint that holds no such clause.
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
