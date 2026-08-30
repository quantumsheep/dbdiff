package driversmysql

import (
	"strings"
)

type MySQLCheckConstraint struct {
	Name string

	// MySQL stores the expression with the enclosing parentheses, and MariaDB stores it
	// without them.
	Expression string
}

func (c *MySQLCheckConstraint) Equal(other *MySQLCheckConstraint) bool {
	return c.Name == other.Name && c.Expression == other.Expression
}

func (c *MySQLCheckConstraint) Clause() string {
	expression := c.Expression

	if !strings.HasPrefix(expression, "(") {
		expression = "(" + expression + ")"
	}

	return "CONSTRAINT " + QuoteIdentifier(c.Name) + " CHECK " + expression
}
