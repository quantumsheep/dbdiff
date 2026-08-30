package driverspostgres

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"time"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

// The schema section already creates or drops the other tables.
func (d *PostgresDriver) DiffData(ctx context.Context) ([]driversshared.Instruction, error) {
	targetTables, err := d.GetTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceTables, err := d.GetTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	return driversshared.DiffData(ctx, d.TargetDatabaseConnection, d.SourceDatabaseConnection,
		targetTables, sourceTables, driversshared.DataRules[*PostgresTable]{
			TableName: func(table *PostgresTable) string {
				return table.Name
			},
			// A SELECT of the parent returns the rows of each partition, so the partition
			// gets no comparison of its own.
			SkipTable: func(table *PostgresTable) bool {
				return table.IsPartition()
			},
			PrimaryKey: func(ctx context.Context, db *sql.DB, table *PostgresTable) ([]string, error) {
				return d.GetTablePrimaryKey(ctx, db, table.Name)
			},
			WritableColumns: func(table *PostgresTable) []string {
				return writablePostgresColumnNames(table.Columns)
			},
			SelectExpression: driversshared.QuoteIdentifier,
			FormatValue:      formatPostgresValue,
			// PostgreSQL refuses a plain value for a GENERATED ALWAYS identity column.
			Insert: func(table *PostgresTable, columnNames []string, expressions []string) driversshared.Instruction {
				if holdsAlwaysIdentityColumn(table, columnNames) {
					return &PostgresInsertOverridingInstruction{
						TableName:   table.Name,
						ColumnNames: columnNames,
						Expressions: expressions,
					}
				}

				return &driversshared.SQLInsertInstruction{
					TableName:   table.Name,
					ColumnNames: columnNames,
					Expressions: expressions,
				}
			},
			// PostgreSQL refuses an UPDATE of a GENERATED ALWAYS identity column.
			SkipUpdate: func(table *PostgresTable, columnName string) bool {
				return holdsAlwaysIdentityColumn(table, []string{columnName})
			},
		})
}

// PostgreSQL computes a generated column, and it refuses a value for that column.
func writablePostgresColumnNames(columns []*PostgresColumn) []string {
	writable := lo.Filter(columns, func(column *PostgresColumn, _ int) bool {
		return column.GeneratedExpression == ""
	})

	return lo.Map(writable, func(column *PostgresColumn, _ int) string {
		return column.Name
	})
}

func holdsAlwaysIdentityColumn(table *PostgresTable, columnNames []string) bool {
	return lo.SomeBy(columnNames, func(name string) bool {
		column, found := table.ColumnByName(name)

		return found && column.Identity == "ALWAYS"
	})
}

func (d *PostgresDriver) GetTablePrimaryKey(ctx context.Context, db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT a.attname
		FROM pg_constraint c
		JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, key_position) ON true
		JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
		WHERE c.conrelid = $1::regclass AND c.contype = 'p'
		ORDER BY k.key_position
	`, driversshared.QuoteIdentifier(tableName))
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var columnNames []string

	for rows.Next() {
		var columnName string

		err := rows.Scan(&columnName)
		if err != nil {
			return nil, err
		}

		columnNames = append(columnNames, columnName)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return columnNames, nil
}

const postgresTimeLayout = "2006-01-02 15:04:05.999999-07:00"

// The diff compares two rows through these literals, so NULL never equals the text 'NULL'.
func formatPostgresValue(value any, databaseTypeName string) string {
	if value == nil {
		return driversshared.SQLNullLiteral
	}

	textValue, isText := value.(string)
	if isText {
		return driversshared.QuoteLiteral(textValue)
	}

	integerValue, isInteger := value.(int64)
	if isInteger {
		return strconv.FormatInt(integerValue, 10)
	}

	doubleValue, isDouble := value.(float64)
	if isDouble {
		// The bare words NaN and Inf parse as identifiers, so these values take the
		// string form that PostgreSQL reads.
		switch {
		case math.IsNaN(doubleValue):
			return "'NaN'"
		case math.IsInf(doubleValue, 1):
			return "'Infinity'"
		case math.IsInf(doubleValue, -1):
			return "'-Infinity'"
		}

		return strconv.FormatFloat(doubleValue, 'g', -1, 64)
	}

	booleanValue, isBoolean := value.(bool)
	if isBoolean {
		if booleanValue {
			return "TRUE"
		}

		return "FALSE"
	}

	// pgx gives a []byte for a json value, an xml value, and an array value too, and
	// only a bytea value takes the hex form.
	byteaValue, isBytea := value.([]byte)
	if isBytea {
		if databaseTypeName == "BYTEA" {
			return driversshared.QuoteLiteral(`\x` + hex.EncodeToString(byteaValue))
		}

		return driversshared.QuoteLiteral(string(byteaValue))
	}

	timeValue, isTime := value.(time.Time)
	if isTime {
		// The TimeZone setting of each session names the offset of the text form, and
		// the two sides can hold two settings. The UTC form keeps one instant equal to
		// itself.
		return driversshared.QuoteLiteral(timeValue.UTC().Format(postgresTimeLayout))
	}

	return driversshared.QuoteLiteral(fmt.Sprintf("%v", value))
}
