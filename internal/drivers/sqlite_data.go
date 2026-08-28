package drivers

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/samber/lo"
)

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

// The schema section already creates or drops the other tables.
func (d *SQLiteDriver) DiffData(ctx context.Context) ([]Instruction, error) {
	var instructions []Instruction
	var removalsPerTable [][]Instruction

	targetTables, err := d.GetTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceTables, err := d.GetTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, targetTable := range targetTables {
		sourceTable, found := lo.Find(sourceTables, func(table *SQLiteTable) bool {
			return table.Name == targetTable.Name
		})
		if !found {
			additions, err := d.TableDataInstructions(ctx, targetTable)
			if err != nil {
				return nil, err
			}

			instructions = append(instructions, additions...)

			continue
		}

		additions, removals, err := d.DiffTableData(ctx, targetTable, sourceTable)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, additions...)
		removalsPerTable = append(removalsPerTable, removals)
	}

	// A DELETE of a child row comes before the DELETE of the parent row that it names.
	for _, removals := range slices.Backward(removalsPerTable) {
		instructions = append(instructions, removals...)
	}

	return instructions, nil
}

func (d *SQLiteDriver) DiffTableData(ctx context.Context, targetTable *SQLiteTable, sourceTable *SQLiteTable) ([]Instruction, []Instruction, error) {
	primaryKeyColumnNames := targetTable.PrimaryKeyColumnNames()
	if len(primaryKeyColumnNames) == 0 {
		comment := &SQLCommentInstruction{
			Text: fmt.Sprintf("The table %s holds no primary key, so dbdiff compares no row of it.",
				QuoteIdentifier(targetTable.Name)),
		}

		return []Instruction{comment}, nil, nil
	}

	targetColumnNames := writableSQLiteColumnNames(targetTable.Columns)
	sourceColumnNames := writableSQLiteColumnNames(sourceTable.Columns)

	sourceKeyColumnNames := sourceTable.PrimaryKeyColumnNames()

	// A key of other columns matches zero source rows or several source rows, so the
	// comparison needs the same key on both sides.
	holdsSameKey := slices.Equal(primaryKeyColumnNames, sourceKeyColumnNames)
	if !holdsSameKey {
		comment := &SQLCommentInstruction{
			Text: fmt.Sprintf("The table %s holds another primary key in the source, so dbdiff compares no row of it.",
				QuoteIdentifier(targetTable.Name)),
		}

		return []Instruction{comment}, nil, nil
	}

	commonColumnNames := lo.Filter(targetColumnNames, func(name string, _ int) bool {
		return slices.Contains(sourceColumnNames, name)
	})

	targetData, err := d.GetTableData(ctx, d.TargetDatabaseConnection, targetTable.Name, targetColumnNames, primaryKeyColumnNames)
	if err != nil {
		return nil, nil, err
	}

	sourceData, err := d.GetTableData(ctx, d.SourceDatabaseConnection, sourceTable.Name, commonColumnNames, primaryKeyColumnNames)
	if err != nil {
		return nil, nil, err
	}

	var insertions []Instruction
	var modifications []Instruction
	var removals []Instruction

	for _, key := range targetData.Keys {
		targetRow := targetData.Rows[key]

		sourceRow, found := sourceData.Rows[key]
		if !found {
			values := lo.Map(targetColumnNames, func(name string, _ int) string {
				return targetRow[name]
			})

			insertions = append(insertions, &SQLInsertInstruction{
				TableName:   targetTable.Name,
				ColumnNames: targetColumnNames,
				Expressions: values,
			})

			continue
		}

		var setClauses []*SQLSetClause

		for _, name := range commonColumnNames {
			if targetRow[name] != sourceRow[name] {
				setClauses = append(setClauses, &SQLSetClause{
					ColumnName: name,
					Expression: targetRow[name],
				})
			}
		}

		if len(setClauses) == 0 {
			continue
		}

		modifications = append(modifications, &SQLUpdateInstruction{
			TableName:  targetTable.Name,
			SetClauses: setClauses,
			Condition:  rowKeyCondition(primaryKeyColumnNames, targetRow),
		})
	}

	for _, key := range sourceData.Keys {
		_, found := targetData.Rows[key]
		if found {
			continue
		}

		removals = append(removals, &SQLDeleteInstruction{
			TableName: sourceTable.Name,
			Condition: rowKeyCondition(primaryKeyColumnNames, sourceData.Rows[key]),
		})
	}

	return slices.Concat(insertions, modifications), removals, nil
}

// The schema section creates this table with no row, so every target row becomes an
// INSERT statement.
func (d *SQLiteDriver) TableDataInstructions(ctx context.Context, targetTable *SQLiteTable) ([]Instruction, error) {
	targetColumnNames := writableSQLiteColumnNames(targetTable.Columns)
	if len(targetColumnNames) == 0 {
		return nil, nil
	}

	// An empty column list breaks the ORDER BY clause of the SELECT statement.
	orderColumnNames := targetTable.PrimaryKeyColumnNames()
	if len(orderColumnNames) == 0 {
		orderColumnNames = targetColumnNames
	}

	targetData, err := d.GetTableData(ctx, d.TargetDatabaseConnection, targetTable.Name, targetColumnNames, orderColumnNames)
	if err != nil {
		return nil, err
	}

	var instructions []Instruction

	for _, key := range targetData.Keys {
		targetRow := targetData.Rows[key]

		values := lo.Map(targetColumnNames, func(name string, _ int) string {
			return targetRow[name]
		})

		instructions = append(instructions, &SQLInsertInstruction{
			TableName:   targetTable.Name,
			ColumnNames: targetColumnNames,
			Expressions: values,
		})
	}

	return instructions, nil
}

// SQLite gives no stable order, so the ORDER BY clause keeps the output equal
// between two runs.
// SQLite computes a generated column, and it refuses a value for that column.
func writableSQLiteColumnNames(columns []*SQLiteColumn) []string {
	writable := lo.Filter(columns, func(column *SQLiteColumn, _ int) bool {
		return !column.IsGenerated()
	})

	return lo.Map(writable, func(column *SQLiteColumn, _ int) string {
		return column.Name
	})
}

func (d *SQLiteDriver) GetTableData(ctx context.Context, db *sql.DB, tableName string, columnNames []string, primaryKeyColumnNames []string) (*SQLiteTableData, error) {
	// go-sqlite3 reads a column of the declared type DATE as a time value, and it
	// returns the zero time for a text that it cannot parse. The plus sign removes the
	// declared type of the result column, so the driver returns the stored value.
	selectExpressions := lo.Map(columnNames, func(name string, _ int) string {
		return "+" + QuoteIdentifier(name)
	})

	statement := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s;",
		strings.Join(selectExpressions, ", "),
		QuoteIdentifier(tableName),
		strings.Join(QuoteIdentifiers(primaryKeyColumnNames), ", "))

	rows, err := db.QueryContext(ctx, statement)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

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

// The diff compares two rows through these literals, so NULL never equals the text 'NULL'.
func formatSQLiteValue(value any) string {
	if value == nil {
		return sqlNullLiteral
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
		// SQLite stores no NaN, and it reads the literal 9e999 as the infinite value.
		switch {
		case math.IsNaN(realValue):
			return sqlNullLiteral
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

	return quoteLiteral(fmt.Sprintf("%v", value))
}

func sqliteRowKey(primaryKeyColumnNames []string, row map[string]string) string {
	literals := lo.Map(primaryKeyColumnNames, func(name string, _ int) string {
		return row[name]
	})

	return strings.Join(literals, ", ")
}
