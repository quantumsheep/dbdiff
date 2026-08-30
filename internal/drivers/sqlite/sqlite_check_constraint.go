package driverssqlite

import (
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

// No PRAGMA reports a check, so parseTableDefinition reads it from the CREATE TABLE.
type SQLiteCheckConstraint struct {
	Name       string
	Expression string
}

func (c *SQLiteCheckConstraint) Equal(other *SQLiteCheckConstraint) bool {
	return c.Name == other.Name && c.Expression == other.Expression
}

func (c *SQLiteCheckConstraint) Clause() string {
	return constraintNameClause(c.Name) + "CHECK " + c.Expression
}

func constraintNameClause(name string) string {
	if name == "" {
		return ""
	}

	return "CONSTRAINT " + driversshared.QuoteIdentifier(name) + " "
}
