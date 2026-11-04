package driversmysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

type Backtick struct{}

func (Backtick) QuoteIdentifier(name string) string {
	return QuoteIdentifier(name)
}

type (
	MySQLEqualityCondition = driversshared.EqualityCondition[Backtick]
	MySQLSetClause         = driversshared.SetClause[Backtick]
	MySQLInsertInstruction = driversshared.InsertInstruction[Backtick]
	MySQLUpdateInstruction = driversshared.UpdateInstruction[Backtick]
	MySQLDeleteInstruction = driversshared.DeleteInstruction[Backtick]
)

// The schema section already creates or drops the other tables.
func (d *MySQLDriver) DiffData(
	ctx context.Context,
	desiredTables []*MySQLTable,
	currentTables []*MySQLTable,
) ([]driversshared.Instruction, error) {
	return driversshared.DiffData(ctx, d.DesiredDatabaseConnection, d.CurrentDatabaseConnection,
		desiredTables, currentTables, driversshared.DataRules[*MySQLTable, Backtick]{
			TableName: func(table *MySQLTable) string {
				return table.Name
			},
			PrimaryKey: func(ctx context.Context, db *sql.DB, table *MySQLTable) ([]string, error) {
				return table.PrimaryKey, nil
			},
			WritableColumns: func(table *MySQLTable) []string {
				return writableMySQLColumnNames(table.Columns)
			},
			FormatValue: formatMySQLValue,
		})
}

// MySQL computes a generated column, and it refuses a value for that column.
func writableMySQLColumnNames(columns []*MySQLColumn) []string {
	writable := lo.Filter(columns, func(column *MySQLColumn, _ int) bool {
		return !column.IsGenerated()
	})

	return lo.Map(writable, func(column *MySQLColumn, _ int) string {
		return column.Name
	})
}

var mysqlNumericTypeNames = []string{
	"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT",
	"DECIMAL", "FLOAT", "DOUBLE", "YEAR",
}

var mysqlBinaryTypeNames = []string{
	"BIT", "BINARY", "VARBINARY",
	"TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB", "GEOMETRY",
}

// The diff compares two rows through these literals, so NULL never equals the text 'NULL'.
func formatMySQLValue(value any, databaseTypeName string) string {
	if value == nil {
		return driversshared.SQLNullLiteral
	}

	databaseTypeName = strings.TrimPrefix(databaseTypeName, "UNSIGNED ")

	byteValue, isBytes := value.([]byte)
	if isBytes {
		if slices.Contains(mysqlNumericTypeNames, databaseTypeName) {
			return string(byteValue)
		}

		if slices.Contains(mysqlBinaryTypeNames, databaseTypeName) {
			return "X'" + hex.EncodeToString(byteValue) + "'"
		}

		return QuoteLiteral(string(byteValue))
	}

	integerValue, isInteger := value.(int64)
	if isInteger {
		return strconv.FormatInt(integerValue, 10)
	}

	realValue, isReal := value.(float64)
	if isReal {
		return strconv.FormatFloat(realValue, 'g', -1, 64)
	}

	timeValue, isTime := value.(time.Time)
	if isTime {
		return QuoteLiteral(timeValue.Format("2006-01-02 15:04:05.999999"))
	}

	return QuoteLiteral(fmt.Sprintf("%v", value))
}
