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
			continue
		}

		subInstructions, err := d.DiffTableData(ctx, targetTable, sourceTable)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, subInstructions...)
	}

	return instructions, nil
}

func (d *SQLiteDriver) DiffTableData(ctx context.Context, targetTable *SQLiteTable, sourceTable *SQLiteTable) ([]Instruction, error) {
	primaryKeyColumnNames := targetTable.PrimaryKeyColumnNames()
	if len(primaryKeyColumnNames) == 0 {
		comment := &SQLCommentInstruction{
			Text: fmt.Sprintf("The table %s holds no primary key, so dbdiff compares no row of it.",
				QuoteIdentifier(targetTable.Name)),
		}

		return []Instruction{comment}, nil
	}

	targetColumnNames := lo.Map(targetTable.Columns, func(column *SQLiteColumn, _ int) string {
		return column.Name
	})

	sourceColumnNames := lo.Map(sourceTable.Columns, func(column *SQLiteColumn, _ int) string {
		return column.Name
	})

	holdsEveryKeyColumn := lo.EveryBy(primaryKeyColumnNames, func(name string) bool {
		return slices.Contains(sourceColumnNames, name)
	})
	if !holdsEveryKeyColumn {
		comment := &SQLCommentInstruction{
			Text: fmt.Sprintf("The table %s holds another primary key in the source, so dbdiff compares no row of it.",
				QuoteIdentifier(targetTable.Name)),
		}

		return []Instruction{comment}, nil
	}

	commonColumnNames := lo.Filter(targetColumnNames, func(name string, _ int) bool {
		return slices.Contains(sourceColumnNames, name)
	})

	targetData, err := d.GetTableData(ctx, d.TargetDatabaseConnection, targetTable.Name, targetColumnNames, primaryKeyColumnNames)
	if err != nil {
		return nil, err
	}

	sourceData, err := d.GetTableData(ctx, d.SourceDatabaseConnection, sourceTable.Name, commonColumnNames, primaryKeyColumnNames)
	if err != nil {
		return nil, err
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

	instructions := slices.Concat(insertions, modifications, removals)

	return instructions, nil
}

// SQLite gives no stable order, so this sort keeps the output equal between two runs.
func (d *SQLiteDriver) GetTableData(ctx context.Context, db *sql.DB, tableName string, columnNames []string, primaryKeyColumnNames []string) (*SQLiteTableData, error) {
	statement := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s;",
		strings.Join(QuoteIdentifiers(columnNames), ", "),
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

const sqliteTimeLayout = "2006-01-02 15:04:05.999999999-07:00"

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
