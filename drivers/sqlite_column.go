package drivers

import (
	"database/sql"
	"fmt"
)

type SQLiteColumn struct {
	Name       string
	Type       string
	NotNull    bool
	PrimaryKey bool
	Default    sql.NullString
}

func (c *SQLiteColumn) Copy() *SQLiteColumn {
	new := *c
	return &new
}

func (c *SQLiteColumn) HasEqualAttributes(other *SQLiteColumn) bool {
	copy := c.Copy()
	copy.Name = other.Name

	return *copy == *other
}

func (c *SQLiteColumn) String() string {
	value := fmt.Sprintf("\"%s\" %s", c.Name, c.Type)
	if c.NotNull {
		value += " NOT NULL"
	}
	if c.PrimaryKey {
		value += " PRIMARY KEY"
	}
	if c.Default.Valid {
		value += fmt.Sprintf(" DEFAULT %s", c.Default.String)
	}

	return value
}
