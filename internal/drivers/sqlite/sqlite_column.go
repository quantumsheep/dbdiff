package driverssqlite

import (
	"database/sql"
	"fmt"
	"regexp"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type SQLiteColumn struct {
	Name string
	Type string

	NotNull bool

	PrimaryKey bool

	Unique bool

	Default sql.NullString

	// The value holds the enclosing parentheses.
	GeneratedExpression string

	GeneratedStored bool

	AutoIncrement bool

	Collation string

	Check string

	PrimaryKeyConflict string
	UniqueConflict     string
	NotNullConflict    string
}

func (c *SQLiteColumn) IsGenerated() bool {
	return c.GeneratedExpression != ""
}

func (c *SQLiteColumn) HasEqualAttributes(other *SQLiteColumn) bool {
	columnCopy := *c
	columnCopy.Name = other.Name

	return columnCopy == *other
}

func (c *SQLiteColumn) Definition() string {
	value := driversshared.QuoteIdentifier(c.Name)

	// SQLite accepts a column with no type, and the short form of a generated column takes
	// that form in most schemas.
	if c.Type != "" {
		value += " " + c.Type
	}

	if c.NotNull {
		value += " NOT NULL" + conflictClause(c.NotNullConflict)
	}

	if c.PrimaryKey {
		value += " PRIMARY KEY"

		// SQLite accepts the keyword AUTOINCREMENT after the keyword PRIMARY KEY only, and
		// it accepts the ON CONFLICT clause before that keyword only.
		value += conflictClause(c.PrimaryKeyConflict)

		if c.AutoIncrement {
			value += " AUTOINCREMENT"
		}
	}

	if c.Unique {
		value += " UNIQUE" + conflictClause(c.UniqueConflict)
	}

	if c.Check != "" {
		value += " CHECK " + c.Check
	}

	if c.Default.Valid {
		value += fmt.Sprintf(" DEFAULT %s", c.Default.String)
	}

	if c.Collation != "" {
		value += " COLLATE " + driversshared.QuoteIdentifier(c.Collation)
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

// SQLite accepts a double-quoted string as a literal default, but it refuses it inside
// parentheses as a value that is not constant.
var literalDefaultPattern = regexp.MustCompile(`(?i)^(?:` +
	`'(?:[^']|'')*'|"(?:[^"]|"")*"|x'[0-9a-f]*'|` +
	`[+-]?(?:0x[0-9a-f]+|(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:e[+-]?[0-9]+)?)|` +
	`null|true|false|current_time|current_date|current_timestamp` +
	`)$`)

// PRAGMA table_xinfo removes the parentheses around an expression default, but the
// DEFAULT clause accepts an expression in parentheses only.
func normalizedDefault(value sql.NullString) sql.NullString {
	if !value.Valid || literalDefaultPattern.MatchString(value.String) {
		return value
	}

	return sql.NullString{
		String: "(" + value.String + ")",
		Valid:  true,
	}
}
