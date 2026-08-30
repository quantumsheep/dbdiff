package driversmysql

import (
	"database/sql"
	"strings"
)

type MySQLColumn struct {
	Name string
	Type string

	NotNull bool

	Default sql.NullString

	// MySQL marks an expression default with DEFAULT_GENERATED in the EXTRA column,
	// and MariaDB stores every default as an expression.
	DefaultIsExpression bool

	OnUpdate string

	AutoIncrement bool

	// Collation stays empty when the column takes the collation of the table.
	Collation string

	Comment string

	GeneratedExpression string

	GeneratedStored bool
}

func (c *MySQLColumn) IsGenerated() bool {
	return c.GeneratedExpression != ""
}

func (c *MySQLColumn) Copy() *MySQLColumn {
	columnCopy := *c
	return &columnCopy
}

func (c *MySQLColumn) HasEqualAttributes(other *MySQLColumn) bool {
	copy := c.Copy()
	copy.Name = other.Name

	return *copy == *other
}

func (c *MySQLColumn) Definition() string {
	value := QuoteIdentifier(c.Name) + " " + c.Type

	if c.Collation != "" {
		value += " COLLATE " + c.Collation
	}

	if c.IsGenerated() {
		storage := "VIRTUAL"
		if c.GeneratedStored {
			storage = "STORED"
		}

		value += " GENERATED ALWAYS AS (" + c.GeneratedExpression + ") " + storage
	}

	if c.NotNull {
		value += " NOT NULL"
	}

	if c.Default.Valid {
		value += " DEFAULT " + c.defaultClause()
	}

	if c.OnUpdate != "" {
		value += " ON UPDATE " + c.OnUpdate
	}

	if c.AutoIncrement {
		value += " AUTO_INCREMENT"
	}

	if c.Comment != "" {
		value += " COMMENT " + QuoteLiteral(c.Comment)
	}

	return value
}

// MySQL refuses the parentheses of an expression default around CURRENT_TIMESTAMP.
func (c *MySQLColumn) defaultClause() string {
	if !c.DefaultIsExpression {
		return QuoteLiteral(c.Default.String)
	}

	if strings.HasPrefix(strings.ToUpper(c.Default.String), "CURRENT_TIMESTAMP") {
		return c.Default.String
	}

	if strings.HasPrefix(c.Default.String, "(") {
		return c.Default.String
	}

	return "(" + c.Default.String + ")"
}
