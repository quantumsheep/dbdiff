package driversmysql

import (
	"database/sql"
	"regexp"
	"strings"
)

var (
	mysqlBitLiteralPattern = regexp.MustCompile(`^[bB]'[01]*'$`)
	mysqlHexLiteralPattern = regexp.MustCompile(`^0x[0-9a-fA-F]+$`)
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

func (c *MySQLColumn) HasEqualAttributes(other *MySQLColumn) bool {
	copied := *c
	copied.Name = other.Name

	return copied == *other
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
		if c.defaultIsBinaryLiteral() {
			return c.Default.String
		}

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

// MySQL stores the literal default of a BIT column as b'...' and of a BINARY or VARBINARY
// column as 0x... The type check keeps the same text of a character column quoted.
func (c *MySQLColumn) defaultIsBinaryLiteral() bool {
	typeName := strings.ToLower(c.Type)

	if strings.HasPrefix(typeName, "bit") {
		return mysqlBitLiteralPattern.MatchString(c.Default.String)
	}

	if strings.HasPrefix(typeName, "binary") || strings.HasPrefix(typeName, "varbinary") {
		return mysqlHexLiteralPattern.MatchString(c.Default.String)
	}

	return false
}
