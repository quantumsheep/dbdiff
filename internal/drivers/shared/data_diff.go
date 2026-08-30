package driversshared

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/samber/lo"
)

type tableData struct {
	Keys []string
	Rows map[string]map[string]string
}

type DataRules[Table any] struct {
	TableName        func(table Table) string
	SkipTable        func(table Table) bool
	PrimaryKey       func(ctx context.Context, db *sql.DB, table Table) ([]string, error)
	WritableColumns  func(table Table) []string
	SelectExpression func(columnName string) string
	FormatValue      func(value any, databaseTypeName string) string
	Insert           func(table Table, columnNames []string, expressions []string) Instruction
	SkipUpdate       func(table Table, columnName string) bool

	// The three hooks below stay nil for an engine that quotes with the double quote.
	// MySQL quotes with the backtick, so it gives its own quote and its own statements.
	Quote  func(name string) string
	Update func(table Table, changes []DataChange, primaryKeyColumnNames []string, row map[string]string) Instruction
	Delete func(table Table, primaryKeyColumnNames []string, row map[string]string) Instruction
}

type DataChange struct {
	ColumnName string
	Expression string
}

// The schema section already creates or drops the other tables.
func DiffData[Table any](ctx context.Context, targetDatabaseConnection *sql.DB, sourceDatabaseConnection *sql.DB,
	targetTables []Table, sourceTables []Table, rules DataRules[Table]) ([]Instruction, error) {
	var instructions []Instruction
	var removalsPerTable [][]Instruction

	for _, targetTable := range targetTables {
		if rules.SkipTable != nil && rules.SkipTable(targetTable) {
			continue
		}

		sourceTable, found := lo.Find(sourceTables, func(table Table) bool {
			return rules.TableName(table) == rules.TableName(targetTable)
		})
		if !found {
			additions, err := tableDataInstructions(ctx, targetDatabaseConnection, targetTable, rules)
			if err != nil {
				return nil, err
			}

			instructions = append(instructions, additions...)

			continue
		}

		additions, removals, err := diffTableData(ctx, targetDatabaseConnection, sourceDatabaseConnection,
			targetTable, sourceTable, rules)
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

func diffTableData[Table any](ctx context.Context, targetDatabaseConnection *sql.DB, sourceDatabaseConnection *sql.DB,
	targetTable Table, sourceTable Table, rules DataRules[Table]) ([]Instruction, []Instruction, error) {
	primaryKeyColumnNames, err := rules.PrimaryKey(ctx, targetDatabaseConnection, targetTable)
	if err != nil {
		return nil, nil, err
	}

	if len(primaryKeyColumnNames) == 0 {
		comment := &SQLCommentInstruction{
			Text: fmt.Sprintf("The table %s holds no primary key, so dbdiff compares no row of it.",
				QuoteIdentifier(rules.TableName(targetTable))),
		}

		return []Instruction{comment}, nil, nil
	}

	targetColumnNames := rules.WritableColumns(targetTable)
	sourceColumnNames := rules.WritableColumns(sourceTable)

	sourceKeyColumnNames, err := rules.PrimaryKey(ctx, sourceDatabaseConnection, sourceTable)
	if err != nil {
		return nil, nil, err
	}

	// A key of other columns matches zero source rows or several source rows, so the
	// comparison needs the same key on both sides.
	holdsSameKey := slices.Equal(primaryKeyColumnNames, sourceKeyColumnNames)
	if !holdsSameKey {
		comment := &SQLCommentInstruction{
			Text: fmt.Sprintf("The table %s holds another primary key in the source, so dbdiff compares no row of it.",
				QuoteIdentifier(rules.TableName(targetTable))),
		}

		return []Instruction{comment}, nil, nil
	}

	commonColumnNames := lo.Filter(targetColumnNames, func(name string, _ int) bool {
		return slices.Contains(sourceColumnNames, name)
	})

	targetData, err := getTableData(ctx, targetDatabaseConnection, targetTable, targetColumnNames, primaryKeyColumnNames, rules)
	if err != nil {
		return nil, nil, err
	}

	sourceData, err := getTableData(ctx, sourceDatabaseConnection, sourceTable, commonColumnNames, primaryKeyColumnNames, rules)
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

			insertions = append(insertions, rules.Insert(targetTable, targetColumnNames, values))

			continue
		}

		var changes []DataChange

		for _, name := range commonColumnNames {
			if targetRow[name] == sourceRow[name] {
				continue
			}

			if rules.SkipUpdate != nil && rules.SkipUpdate(targetTable, name) {
				continue
			}

			changes = append(changes, DataChange{
				ColumnName: name,
				Expression: targetRow[name],
			})
		}

		if len(changes) == 0 {
			continue
		}

		modifications = append(modifications,
			dataUpdateInstruction(rules, targetTable, changes, primaryKeyColumnNames, targetRow))
	}

	for _, key := range sourceData.Keys {
		_, found := targetData.Rows[key]
		if found {
			continue
		}

		removals = append(removals,
			dataDeleteInstruction(rules, sourceTable, primaryKeyColumnNames, sourceData.Rows[key]))
	}

	return slices.Concat(insertions, modifications), removals, nil
}

func dataUpdateInstruction[Table any](rules DataRules[Table], table Table, changes []DataChange,
	primaryKeyColumnNames []string, row map[string]string) Instruction {
	if rules.Update != nil {
		return rules.Update(table, changes, primaryKeyColumnNames, row)
	}

	setClauses := lo.Map(changes, func(change DataChange, _ int) *SQLSetClause {
		return &SQLSetClause{
			ColumnName: change.ColumnName,
			Expression: change.Expression,
		}
	})

	return &SQLUpdateInstruction{
		TableName:  rules.TableName(table),
		SetClauses: setClauses,
		Condition:  RowKeyCondition(primaryKeyColumnNames, row),
	}
}

func dataDeleteInstruction[Table any](rules DataRules[Table], table Table,
	primaryKeyColumnNames []string, row map[string]string) Instruction {
	if rules.Delete != nil {
		return rules.Delete(table, primaryKeyColumnNames, row)
	}

	return &SQLDeleteInstruction{
		TableName: rules.TableName(table),
		Condition: RowKeyCondition(primaryKeyColumnNames, row),
	}
}

// The schema section creates this table with no row, so every target row becomes an
// INSERT statement.
func tableDataInstructions[Table any](ctx context.Context, db *sql.DB, targetTable Table,
	rules DataRules[Table]) ([]Instruction, error) {
	targetColumnNames := rules.WritableColumns(targetTable)
	if len(targetColumnNames) == 0 {
		return nil, nil
	}

	// An empty column list breaks the ORDER BY clause of the SELECT statement.
	orderColumnNames, err := rules.PrimaryKey(ctx, db, targetTable)
	if err != nil {
		return nil, err
	}

	if len(orderColumnNames) == 0 {
		orderColumnNames = targetColumnNames
	}

	targetData, err := getTableData(ctx, db, targetTable, targetColumnNames, orderColumnNames, rules)
	if err != nil {
		return nil, err
	}

	var instructions []Instruction

	for _, key := range targetData.Keys {
		targetRow := targetData.Rows[key]

		values := lo.Map(targetColumnNames, func(name string, _ int) string {
			return targetRow[name]
		})

		instructions = append(instructions, rules.Insert(targetTable, targetColumnNames, values))
	}

	return instructions, nil
}

// The engines give no stable order, so the ORDER BY clause keeps the output equal
// between two runs.
func getTableData[Table any](ctx context.Context, db *sql.DB, table Table, columnNames []string,
	primaryKeyColumnNames []string, rules DataRules[Table]) (*tableData, error) {
	quote := rules.Quote
	if quote == nil {
		quote = QuoteIdentifier
	}

	selectExpressions := lo.Map(columnNames, func(name string, _ int) string {
		return rules.SelectExpression(name)
	})

	quotedKeyColumnNames := lo.Map(primaryKeyColumnNames, func(name string, _ int) string {
		return quote(name)
	})

	statement := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s;",
		strings.Join(selectExpressions, ", "),
		quote(rules.TableName(table)),
		strings.Join(quotedKeyColumnNames, ", "))

	rows, err := db.QueryContext(ctx, statement)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	data := &tableData{
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
			row[name] = rules.FormatValue(values[i], columnTypes[i].DatabaseTypeName())
		}

		key := rowKey(primaryKeyColumnNames, row)

		data.Keys = append(data.Keys, key)
		data.Rows[key] = row
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return data, nil
}

func rowKey(primaryKeyColumnNames []string, row map[string]string) string {
	literals := lo.Map(primaryKeyColumnNames, func(name string, _ int) string {
		return row[name]
	})

	return strings.Join(literals, ", ")
}
