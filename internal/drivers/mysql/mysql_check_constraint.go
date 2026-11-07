package driversmysql

import (
	"context"
	"database/sql"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type MySQLCheckConstraint struct {
	Name string

	// MySQL stores the expression with the enclosing parentheses, and MariaDB stores it
	// without them.
	Expression string

	// MariaDB marks a CHECK clause of a column. The comparison ignores the mark.
	ColumnLevel bool
}

func (c *MySQLCheckConstraint) Equal(other *MySQLCheckConstraint) bool {
	return c.Name == other.Name && c.Expression == other.Expression
}

func (c *MySQLCheckConstraint) UsesColumn(columnName string) bool {
	return strings.Contains(c.Expression, QuoteIdentifier(columnName))
}

func (c *MySQLCheckConstraint) Clause() string {
	expression := c.Expression

	if !strings.HasPrefix(expression, "(") {
		expression = "(" + expression + ")"
	}

	return "CONSTRAINT " + QuoteIdentifier(c.Name) + " CHECK " + expression
}

// The CHECK_CONSTRAINTS table of MySQL holds no table name, so the query joins the
// TABLE_CONSTRAINTS table. The table of MariaDB holds the table name, and MariaDB needs
// it, because two tables can hold a column constraint with one name.
func (d *MySQLDriver) GetTableCheckConstraints(
	ctx context.Context,
	db *sql.DB,
	tableName string,
) ([]*MySQLCheckConstraint, error) {
	query := `
		SELECT tc.CONSTRAINT_NAME, cc.CHECK_CLAUSE
		FROM information_schema.TABLE_CONSTRAINTS tc
		JOIN information_schema.CHECK_CONSTRAINTS cc
			ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
			AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
		WHERE tc.CONSTRAINT_SCHEMA = DATABASE() AND tc.TABLE_NAME = ?
			AND tc.CONSTRAINT_TYPE = 'CHECK'
		ORDER BY tc.CONSTRAINT_NAME;
	`

	mariadb := d.detailsByConnection[db].mariadb
	if mariadb {
		query = `
			SELECT CONSTRAINT_NAME, CHECK_CLAUSE, LEVEL = 'Column'
			FROM information_schema.CHECK_CONSTRAINTS
			WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = ?
			ORDER BY CONSTRAINT_NAME;
		`
	}

	return driversshared.QueryAll(ctx, db, query, func(rows *sql.Rows) (*MySQLCheckConstraint, error) {
		check := &MySQLCheckConstraint{}

		var err error

		if mariadb {
			err = rows.Scan(&check.Name, &check.Expression, &check.ColumnLevel)
		} else {
			err = rows.Scan(&check.Name, &check.Expression)
			check.Expression = unescapeCatalogText(check.Expression)
		}

		return check, err
	}, tableName)
}
