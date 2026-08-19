package drivers

// A SQLiteCheckConstraint is a CHECK constraint of a table. No PRAGMA statement reports it,
// so parseTableDefinition reads it from the CREATE TABLE statement.
type SQLiteCheckConstraint struct {
	// Name holds the name of the constraint. It is empty for a constraint that the schema
	// declares with no name.
	Name string

	// Expression holds the check, with the enclosing parentheses.
	Expression string
}

func (c *SQLiteCheckConstraint) Equal(other *SQLiteCheckConstraint) bool {
	return c.Name == other.Name && c.Expression == other.Expression
}

func (c *SQLiteCheckConstraint) Clause() string {
	return constraintNameClause(c.Name) + "CHECK " + c.Expression
}

// constraintNameClause returns the CONSTRAINT keyword and the name, with a trailing space.
// An empty name gives an empty text.
func constraintNameClause(name string) string {
	if name == "" {
		return ""
	}

	return "CONSTRAINT " + quoteIdentifier(name) + " "
}
