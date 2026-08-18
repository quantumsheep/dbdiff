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
func (d *SQLiteDriver) DiffData(ctx context.Context) ([]Instruction, error) {
	var instructions []Instruction

	sourceTables, err := d.GetTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetTables, err := d.GetTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, sourceTable := range sourceTables {
		targetTable, found := lo.Find(targetTables, func(table *SQLiteTable) bool {
			return table.Name == sourceTable.Name
		})
		if !found {
			continue
		}

		subInstructions, err := d.DiffTableData(ctx, sourceTable, targetTable)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, subInstructions...)
	}

	return instructions, nil
}

func (d *SQLiteDriver) DiffTableData(ctx context.Context, sourceTable *SQLiteTable, targetTable *SQLiteTable) ([]Instruction, error) {
	primaryKeyColumnNames := sourceTable.PrimaryKeyColumnNames()
	if len(primaryKeyColumnNames) == 0 {
		comment := &SQLCommentInstruction{
			Text: fmt.Sprintf("The table %s holds no primary key, so dbdiff compares no row of it.",
				quoteIdentifier(sourceTable.Name)),
		}

		return []Instruction{comment}, nil
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
		comment := &SQLCommentInstruction{
			Text: fmt.Sprintf("The table %s holds another primary key in the target, so dbdiff compares no row of it.",
				quoteIdentifier(sourceTable.Name)),
		}

		return []Instruction{comment}, nil
	}

	commonColumnNames := lo.Filter(sourceColumnNames, func(name string, _ int) bool {
		return slices.Contains(targetColumnNames, name)
	})

	sourceData, err := d.GetTableData(ctx, d.SourceDatabaseConnection, sourceTable.Name, sourceColumnNames, primaryKeyColumnNames)
	if err != nil {
		return nil, err
	}

	targetData, err := d.GetTableData(ctx, d.TargetDatabaseConnection, targetTable.Name, commonColumnNames, primaryKeyColumnNames)
	if err != nil {
		return nil, err
	}

	var insertions []Instruction
	var modifications []Instruction
	var removals []Instruction

	for _, key := range sourceData.Keys {
		sourceRow := sourceData.Rows[key]

		targetRow, found := targetData.Rows[key]
		if !found {
			values := lo.Map(sourceColumnNames, func(name string, _ int) string {
				return sourceRow[name]
			})

			insertions = append(insertions, &SQLInsertInstruction{
				TableName:   sourceTable.Name,
				ColumnNames: sourceColumnNames,
				Expressions: values,
			})

			continue
		}

		var setClauses []*SQLSetClause

		for _, name := range commonColumnNames {
			if sourceRow[name] != targetRow[name] {
				setClauses = append(setClauses, &SQLSetClause{
					ColumnName: name,
					Expression: sourceRow[name],
				})
			}
		}

		if len(setClauses) == 0 {
			continue
		}

		modifications = append(modifications, &SQLUpdateInstruction{
			TableName:  sourceTable.Name,
			SetClauses: setClauses,
			Condition:  rowKeyCondition(primaryKeyColumnNames, sourceRow),
		})
	}

	for _, key := range targetData.Keys {
		_, found := sourceData.Rows[key]
		if found {
			continue
		}

		removals = append(removals, &SQLDeleteInstruction{
			TableName: targetTable.Name,
			Condition: rowKeyCondition(primaryKeyColumnNames, targetData.Rows[key]),
		})
	}

	instructions := slices.Concat(insertions, modifications, removals)

	return instructions, nil
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
