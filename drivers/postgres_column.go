package drivers

import (
	"database/sql"
	"fmt"
)

type PostgresColumn struct {
	Name    string
	Type    string
	NotNull bool
	Default sql.NullString

	// Identity holds ALWAYS or BY DEFAULT for an identity column. It is empty for every
	// other column.
	Identity string

	// GeneratedExpression holds the expression of a stored generated column. It is empty
	// for every other column.
	GeneratedExpression string

	// Collation names the collation of the column. It is empty when the column keeps the
	// collation of its type.
	Collation string

	// Comment holds the comment of the column. Definition writes no comment, because a
	// column definition accepts none.
	Comment string
}

func (c *PostgresColumn) Copy() *PostgresColumn {
	columnCopy := *c
	return &columnCopy
}

func (c *PostgresColumn) HasEqualAttributes(other *PostgresColumn) bool {
	copy := c.Copy()
	copy.Name = other.Name

	return *copy == *other
}

func (c *PostgresColumn) Definition() string {
	value := fmt.Sprintf("%s %s", quoteIdentifier(c.Name), c.Type)

	if c.Collation != "" {
		value += " COLLATE " + quoteIdentifier(c.Collation)
	}

	if c.NotNull {
		value += " NOT NULL"
	}

	if c.Identity != "" {
		value += fmt.Sprintf(" GENERATED %s AS IDENTITY", c.Identity)
	}

	if c.GeneratedExpression != "" {
		value += fmt.Sprintf(" GENERATED ALWAYS AS %s STORED", c.GeneratedExpression)
	}

	if c.Default.Valid {
		value += fmt.Sprintf(" DEFAULT %s", c.Default.String)
	}

	return value
}
