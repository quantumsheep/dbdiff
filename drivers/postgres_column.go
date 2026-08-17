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

	if c.NotNull {
		value += " NOT NULL"
	}

	if c.Default.Valid {
		value += fmt.Sprintf(" DEFAULT %s", c.Default.String)
	}

	return value
}
