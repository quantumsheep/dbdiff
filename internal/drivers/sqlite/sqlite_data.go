package driverssqlite

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

func (t *SQLiteTable) PrimaryKeyColumnNames() []string {
	if len(t.PrimaryKey) > 0 {
		return t.PrimaryKey
	}

	var names []string

	for _, column := range t.Columns {
		if column.PrimaryKey {
			names = append(names, column.Name)
		}
	}

	return names
}

// The schema section already creates or drops the other tables.
func (d *SQLiteDriver) DiffData(ctx context.Context) ([]driversshared.Instruction, error) {
	targetTables, err := d.GetTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceTables, err := d.GetTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	return driversshared.DiffData(ctx, d.TargetDatabaseConnection, d.SourceDatabaseConnection,
		targetTables, sourceTables, driversshared.DataRules[*SQLiteTable]{
			TableName: func(table *SQLiteTable) string {
				return table.Name
			},
			PrimaryKey: func(ctx context.Context, db *sql.DB, table *SQLiteTable) ([]string, error) {
				return table.PrimaryKeyColumnNames(), nil
			},
			WritableColumns: func(table *SQLiteTable) []string {
				return writableSQLiteColumnNames(table.Columns)
			},
			// go-sqlite3 reads a column of the declared type DATE as a time value, and it
			// returns the zero time for a text that it cannot parse. The plus sign removes
			// the declared type of the result column, so the driver returns the stored value.
			SelectExpression: func(columnName string) string {
				return "+" + driversshared.QuoteIdentifier(columnName)
			},
			FormatValue: func(value any, databaseTypeName string) string {
				return formatSQLiteValue(value)
			},
			Insert: func(table *SQLiteTable, columnNames []string, expressions []string) driversshared.Instruction {
				return &driversshared.SQLInsertInstruction{
					TableName:   table.Name,
					ColumnNames: columnNames,
					Expressions: expressions,
				}
			},
		})
}

// SQLite computes a generated column, and it refuses a value for that column.
func writableSQLiteColumnNames(columns []*SQLiteColumn) []string {
	writable := lo.Filter(columns, func(column *SQLiteColumn, _ int) bool {
		return !column.IsGenerated()
	})

	return lo.Map(writable, func(column *SQLiteColumn, _ int) string {
		return column.Name
	})
}

// The diff compares two rows through these literals, so NULL never equals the text 'NULL'.
func formatSQLiteValue(value any) string {
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

	realValue, isReal := value.(float64)
	if isReal {
		// SQLite stores no NaN, and it reads the literal 9e999 as the infinite value.
		switch {
		case math.IsNaN(realValue):
			return driversshared.SQLNullLiteral
		case math.IsInf(realValue, 1):
			return "9e999"
		case math.IsInf(realValue, -1):
			return "-9e999"
		}

		return strconv.FormatFloat(realValue, 'g', -1, 64)
	}

	booleanValue, isBoolean := value.(bool)
	if isBoolean {
		if booleanValue {
			return "1"
		}

		return "0"
	}

	blobValue, isBlob := value.([]byte)
	if isBlob {
		return "X'" + hex.EncodeToString(blobValue) + "'"
	}

	return driversshared.QuoteLiteral(fmt.Sprintf("%v", value))
}
