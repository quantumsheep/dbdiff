package drivers

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"
)

// A key joins the SQL literals of the primary key columns of one row. A row maps a column
// name to the SQL literal of its value.
type SQLiteTableData struct {
	Keys []string
	Rows map[string]map[string]string
}

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

// DiffData compares the rows of each table that both databases hold. The schema section
// already creates or drops the other tables.
func (d *SQLiteDriver) DiffData(ctx context.Context) (string, error) {
	var diff strings.Builder

	sourceTables, err := d.GetTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return "", err
	}

	targetTables, err := d.GetTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return "", err
	}

	for _, sourceTable := range sourceTables {
		targetTable, found := lo.Find(targetTables, func(table *SQLiteTable) bool {
			return table.Name == sourceTable.Name
		})
		if !found {
			continue
		}

		subDiff, err := d.DiffTableData(ctx, sourceTable, targetTable)
		if err != nil {
			return "", err
		}

		if subDiff != "" {
			fmt.Fprintln(&diff, subDiff)
		}
	}

	return strings.TrimSpace(diff.String()), nil
}

func (d *SQLiteDriver) DiffTableData(ctx context.Context, sourceTable *SQLiteTable, targetTable *SQLiteTable) (string, error) {
	quotedTableName := quoteIdentifier(sourceTable.Name)

	primaryKeyColumnNames := sourceTable.PrimaryKeyColumnNames()
	if len(primaryKeyColumnNames) == 0 {
		return fmt.Sprintf("-- The table %s holds no primary key, so dbdiff compares no row of it.", quotedTableName), nil
	}

	sourceColumnNames := lo.Map(sourceTable.Columns, func(column *SQLiteColumn, _ int) string {
		return column.Name
	})

	targetColumnNames := lo.Map(targetTable.Columns, func(column *SQLiteColumn, _ int) string {
		return column.Name
	})

	holdsEveryKeyColumn := lo.EveryBy(primaryKeyColumnNames, func(name string) bool {
		return slices.Contains(targetColumnNames, name)
	})
	if !holdsEveryKeyColumn {
		return fmt.Sprintf("-- The table %s holds another primary key in the target, so dbdiff compares no row of it.", quotedTableName), nil
	}

	commonColumnNames := lo.Filter(sourceColumnNames, func(name string, _ int) bool {
		return slices.Contains(targetColumnNames, name)
	})

	sourceData, err := d.GetTableData(ctx, d.SourceDatabaseConnection, sourceTable.Name, sourceColumnNames, primaryKeyColumnNames)
	if err != nil {
		return "", err
	}

	targetData, err := d.GetTableData(ctx, d.TargetDatabaseConnection, targetTable.Name, commonColumnNames, primaryKeyColumnNames)
	if err != nil {
		return "", err
	}

	var insertions strings.Builder
	var modifications strings.Builder
	var removals strings.Builder

	for _, key := range sourceData.Keys {
		sourceRow := sourceData.Rows[key]

		targetRow, found := targetData.Rows[key]
		if !found {
			values := lo.Map(sourceColumnNames, func(name string, _ int) string {
				return sourceRow[name]
			})

			fmt.Fprintf(&insertions, "INSERT INTO %s (%s) VALUES (%s);\n",
				quotedTableName,
				strings.Join(quoteIdentifiers(sourceColumnNames), ", "),
				strings.Join(values, ", "))

			continue
		}

		var assignments []string

		for _, name := range commonColumnNames {
			if sourceRow[name] != targetRow[name] {
				assignments = append(assignments, fmt.Sprintf("%s = %s", quoteIdentifier(name), sourceRow[name]))
			}
		}

		if len(assignments) == 0 {
			continue
		}

		fmt.Fprintf(&modifications, "UPDATE %s SET %s WHERE %s;\n",
			quotedTableName,
			strings.Join(assignments, ", "),
			sqliteRowKeyCondition(primaryKeyColumnNames, sourceRow))
	}

	for _, key := range targetData.Keys {
		_, found := sourceData.Rows[key]
		if found {
			continue
		}

		fmt.Fprintf(&removals, "DELETE FROM %s WHERE %s;\n",
			quotedTableName,
			sqliteRowKeyCondition(primaryKeyColumnNames, targetData.Rows[key]))
	}

	diff := insertions.String() + modifications.String() + removals.String()

	return strings.TrimSpace(diff), nil
}

// GetTableData sorts the rows by the primary key, because SQLite gives no stable order.
// Without that sort the output changes between two runs.
func (d *SQLiteDriver) GetTableData(ctx context.Context, db *sql.DB, tableName string, columnNames []string, primaryKeyColumnNames []string) (*SQLiteTableData, error) {
	statement := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s;",
		strings.Join(quoteIdentifiers(columnNames), ", "),
		quoteIdentifier(tableName),
		strings.Join(quoteIdentifiers(primaryKeyColumnNames), ", "))

	rows, err := db.QueryContext(ctx, statement)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	data := &SQLiteTableData{
		Rows: make(map[string]map[string]string),
	}

	for rows.Next() {
		values := make([]any, len(columnNames))

		valuePointers := make([]any, len(columnNames))
		for i := range valuePointers {
			valuePointers[i] = &values[i]
		}

		err := rows.Scan(valuePointers...)
		if err != nil {
			return nil, err
		}

		row := make(map[string]string, len(columnNames))

		for i, name := range columnNames {
			row[name] = formatSQLiteValue(values[i])
		}

		key := sqliteRowKey(primaryKeyColumnNames, row)

		data.Keys = append(data.Keys, key)
		data.Rows[key] = row
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return data, nil
}

const sqliteTimeLayout = "2006-01-02 15:04:05.999999999-07:00"

// formatSQLiteValue returns the SQL literal of one value. The diff compares two rows
// through these literals, so NULL never equals the text 'NULL'.
func formatSQLiteValue(value any) string {
	if value == nil {
		return "NULL"
	}

	textValue, isText := value.(string)
	if isText {
		return quoteLiteral(textValue)
	}

	integerValue, isInteger := value.(int64)
	if isInteger {
		return strconv.FormatInt(integerValue, 10)
	}

	realValue, isReal := value.(float64)
	if isReal {
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

	timeValue, isTime := value.(time.Time)
	if isTime {
		return quoteLiteral(timeValue.Format(sqliteTimeLayout))
	}

	return quoteLiteral(fmt.Sprintf("%v", value))
}

func sqliteRowKey(primaryKeyColumnNames []string, row map[string]string) string {
	literals := lo.Map(primaryKeyColumnNames, func(name string, _ int) string {
		return row[name]
	})

	return strings.Join(literals, ", ")
}

func sqliteRowKeyCondition(primaryKeyColumnNames []string, row map[string]string) string {
	conditions := lo.Map(primaryKeyColumnNames, func(name string, _ int) string {
		return fmt.Sprintf("%s = %s", quoteIdentifier(name), row[name])
	})

	return strings.Join(conditions, " AND ")
}
