package drivers

import (
	"database/sql"
	"fmt"
)

type SQLiteColumn struct {
	Name string
	Type string

	NotNull bool

	PrimaryKey bool

	Unique bool

	Default sql.NullString
}

func (c *SQLiteColumn) Copy() *SQLiteColumn {
	columnCopy := *c
	return &columnCopy
}

func (c *SQLiteColumn) HasEqualAttributes(other *SQLiteColumn) bool {
	copy := c.Copy()
	copy.Name = other.Name

	return *copy == *other
}

func (c *SQLiteColumn) Definition() string {
	value := fmt.Sprintf("%s %s", quoteIdentifier(c.Name), c.Type)

	if c.NotNull {
		value += " NOT NULL"
	}

	if c.PrimaryKey {
		value += " PRIMARY KEY"
	}

	if c.Unique {
		value += " UNIQUE"
	}

	if c.Default.Valid {
		value += fmt.Sprintf(" DEFAULT %s", c.Default.String)
	}

	return value
}

func (c *SQLiteColumn) IsTypeChangeCompatible(other *SQLiteColumn) bool {
	// A recreation of the table converts between these four types. Every other type change
	// can lose data.

	compatibleTypes := map[string]bool{
		"TEXT":    true,
		"INTEGER": true,
		"REAL":    true,
		"BLOB":    true,
	}

	_, c1 := compatibleTypes[c.Type]
	_, c2 := compatibleTypes[other.Type]

	return c1 && c2
}
