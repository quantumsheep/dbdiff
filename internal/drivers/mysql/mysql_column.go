package driversmysql

import (
	"context"
	"database/sql"
	"regexp"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
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

func (d *MySQLDriver) GetTableColumns(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	tableCollation sql.NullString,
) ([]*MySQLColumn, error) {
	mariadb := d.detailsByConnection[db].mariadb

	return driversshared.QueryAll(ctx, db, `
		SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA,
			COLLATION_NAME, COLUMN_COMMENT, GENERATION_EXPRESSION
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION;
	`, func(rows *sql.Rows) (*MySQLColumn, error) {
		var name, columnType, isNullable, extra string
		var columnDefault, collationName, generationExpression sql.NullString
		var columnComment string

		err := rows.Scan(&name, &columnType, &isNullable, &columnDefault, &extra,
			&collationName, &columnComment, &generationExpression)
		if err != nil {
			return nil, err
		}

		column := &MySQLColumn{
			Name:    name,
			Type:    columnType,
			NotNull: isNullable == "NO",
			Comment: columnComment,
		}

		extraLower := strings.ToLower(extra)
		column.AutoIncrement = strings.Contains(extraLower, "auto_increment")

		onUpdatePosition := strings.Index(extraLower, "on update ")
		if onUpdatePosition >= 0 {
			column.OnUpdate = extra[onUpdatePosition+len("on update "):]
		}

		if generationExpression.Valid && generationExpression.String != "" {
			column.GeneratedExpression = generationExpression.String

			if !mariadb {
				column.GeneratedExpression = unescapeCatalogText(column.GeneratedExpression)
			}

			column.GeneratedStored = strings.Contains(extraLower, "stored generated")
		}

		applyColumnDefault(column, columnDefault, extraLower, mariadb)

		if collationName.Valid && collationName.String != tableCollation.String {
			column.Collation = collationName.String
		}

		return column, nil
	}, tableName)
}

// MariaDB stores every default as an expression, and it stores the text NULL for the
// absent default. MySQL stores a literal as its bare text, and it marks an expression
// with DEFAULT_GENERATED.
func applyColumnDefault(column *MySQLColumn, columnDefault sql.NullString, extraLower string, mariadb bool) {
	if !columnDefault.Valid {
		return
	}

	if mariadb {
		if columnDefault.String == "NULL" {
			return
		}

		column.Default = columnDefault
		column.DefaultIsExpression = true

		return
	}

	column.DefaultIsExpression = strings.Contains(extraLower, "default_generated")

	value := columnDefault.String
	if column.DefaultIsExpression {
		value = unescapeCatalogText(value)
	}

	column.Default = sql.NullString{
		String: value,
		Valid:  true,
	}
}

// The information_schema tables of MySQL escape a quote and a backslash of an expression
// text, and the statement needs the bare characters. One pass keeps an escaped backslash
// before a quote correct.
func unescapeCatalogText(value string) string {
	var builder strings.Builder

	for position := 0; position < len(value); position++ {
		escapesNext := value[position] == '\\' && position+1 < len(value) &&
			(value[position+1] == '\'' || value[position+1] == '\\')
		if escapesNext {
			position++
		}

		builder.WriteByte(value[position])
	}

	return builder.String()
}
