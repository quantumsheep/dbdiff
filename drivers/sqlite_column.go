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

	// GeneratedExpression holds the expression of a generated column, with the enclosing
	// parentheses. It is empty for every other column.
	GeneratedExpression string

	// GeneratedStored is true for a STORED generated column. A generated column that holds
	// false is a VIRTUAL one.
	GeneratedStored bool

	// AutoIncrement is true for an INTEGER PRIMARY KEY AUTOINCREMENT column.
	AutoIncrement bool

	// Collation names the collating sequence of the column, for example NOCASE. It is
	// empty for the default collating sequence.
	Collation string

	// Check holds the check of the column, with the enclosing parentheses. It is empty for
	// a column with no check.
	Check string
}

func (c *SQLiteColumn) IsGenerated() bool {
	return c.GeneratedExpression != ""
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
	value := quoteIdentifier(c.Name)

	// SQLite accepts a column with no type, and the short form of a generated column takes
	// that form in most schemas.
	if c.Type != "" {
		value += " " + c.Type
	}

	if c.NotNull {
		value += " NOT NULL"
	}

	if c.PrimaryKey {
		value += " PRIMARY KEY"

		// SQLite accepts the keyword AUTOINCREMENT after the keyword PRIMARY KEY only.
		if c.AutoIncrement {
			value += " AUTOINCREMENT"
		}
	}

	if c.Unique {
		value += " UNIQUE"
	}

	if c.Check != "" {
		value += " CHECK " + c.Check
	}

	if c.Default.Valid {
		value += fmt.Sprintf(" DEFAULT %s", c.Default.String)
	}

	if c.Collation != "" {
		value += " COLLATE " + c.Collation
	}

	if c.IsGenerated() {
		storage := "VIRTUAL"
		if c.GeneratedStored {
			storage = "STORED"
		}

		value += fmt.Sprintf(" GENERATED ALWAYS AS %s %s", c.GeneratedExpression, storage)
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
