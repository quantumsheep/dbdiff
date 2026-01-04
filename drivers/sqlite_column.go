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

func (c *SQLiteColumn) IsTypeChangeCompatible(other *SQLiteColumn) bool {
	// In SQLite, most type changes are compatible due to dynamic typing,
	// but changing between certain types may lead to data loss or unexpected behavior.
	// Here we define a simple rule: changing between TEXT, INTEGER, REAL, BLOB is compatible,
	// but changing to or from these types to other types is not.

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
