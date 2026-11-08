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

type DataRules[Table any, Quoter IdentifierQuoter] struct {
	TableName       func(table Table) string
	SkipTable       func(table Table) bool
	PrimaryKey      func(ctx context.Context, db *sql.DB, table Table) ([]string, error)
	WritableColumns func(table Table) []string
	FormatValue     func(value any, databaseTypeName string) string
	SkipUpdate      func(table Table, columnName string) bool

	// The two hooks below are optional. If they are nil, the diff uses the quoter.
	SelectExpression func(columnName string) string
	Insert           func(table Table, columnNames []string, expressions []string) Instruction

	// An engine that compares names without case gives this optional key, which matches a
	// desired table with a current table. If it is nil, the diff uses TableName.
	TableKey func(table Table) string
}

// The schema section already creates or drops the other tables.
func DiffData[Table any, Quoter IdentifierQuoter](
	ctx context.Context,
	desiredDatabaseConnection *sql.DB,
	currentDatabaseConnection *sql.DB,
	desiredTables []Table,
	currentTables []Table,
	rules DataRules[Table, Quoter],
) ([]Instruction, error) {
	var instructions []Instruction
	var removalsPerTable [][]Instruction

	for _, desiredTable := range desiredTables {
		if rules.SkipTable != nil && rules.SkipTable(desiredTable) {
			continue
		}

		tableKey := rules.TableKey
		if tableKey == nil {
			tableKey = rules.TableName
		}

		currentTable, found := lo.Find(currentTables, func(table Table) bool {
			return tableKey(table) == tableKey(desiredTable)
		})
		if !found {
			additions, err := tableDataInstructions(ctx, desiredDatabaseConnection, desiredTable, rules)
			if err != nil {
				return nil, err
			}

			instructions = append(instructions, additions...)

			continue
		}

		additions, removals, err := diffTableData(ctx, desiredDatabaseConnection, currentDatabaseConnection,
			desiredTable, currentTable, rules)
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

func diffTableData[Table any, Quoter IdentifierQuoter](
	ctx context.Context,
	desiredDatabaseConnection *sql.DB,
	currentDatabaseConnection *sql.DB,
	desiredTable Table,
	currentTable Table,
	rules DataRules[Table, Quoter],
) ([]Instruction, []Instruction, error) {
	primaryKeyColumnNames, err := rules.PrimaryKey(ctx, desiredDatabaseConnection, desiredTable)
	if err != nil {
		return nil, nil, err
	}

	if len(primaryKeyColumnNames) == 0 {
		comment := &SQLCommentInstruction{
			Text: fmt.Sprintf("The table %s holds no primary key, so dbdiff compares no row of it.",
				QuoteIdentifier(rules.TableName(desiredTable))),
		}

		return []Instruction{comment}, nil, nil
	}

	desiredColumnNames := rules.WritableColumns(desiredTable)
	currentColumnNames := rules.WritableColumns(currentTable)

	currentKeyColumnNames, err := rules.PrimaryKey(ctx, currentDatabaseConnection, currentTable)
	if err != nil {
		return nil, nil, err
	}

	// A key of other columns matches zero current rows or several current rows, so the
	// comparison needs the same key on both sides.
	holdsSameKey := slices.Equal(primaryKeyColumnNames, currentKeyColumnNames)
	if !holdsSameKey {
		comment := &SQLCommentInstruction{
			Text: fmt.Sprintf("The table %s holds another primary key in the current schema, so dbdiff compares no row of it.",
				QuoteIdentifier(rules.TableName(desiredTable))),
		}

		return []Instruction{comment}, nil, nil
	}

	commonColumnNames := lo.Filter(desiredColumnNames, func(name string, _ int) bool {
		return slices.Contains(currentColumnNames, name)
	})

	desiredData, err := getTableData(ctx, desiredDatabaseConnection, desiredTable, desiredColumnNames, primaryKeyColumnNames, rules)
	if err != nil {
		return nil, nil, err
	}

	currentData, err := getTableData(ctx, currentDatabaseConnection, currentTable, commonColumnNames, primaryKeyColumnNames, rules)
	if err != nil {
		return nil, nil, err
	}

	var insertions []Instruction
	var modifications []Instruction
	var removals []Instruction

	for _, key := range desiredData.Keys {
		desiredRow := desiredData.Rows[key]

		currentRow, found := currentData.Rows[key]
		if !found {
			insertions = append(insertions, insertRowInstruction(rules, desiredTable, desiredColumnNames, desiredRow))

			continue
		}

		var setClauses []*SetClause[Quoter]

		// The current row holds no value of a column that the schema section adds or renames.
		// The comparison then always gives that column a SET clause, because its value after the
		// schema section is unknown here.
		for _, name := range desiredColumnNames {
			if desiredRow[name] == currentRow[name] {
				continue
			}

			if rules.SkipUpdate != nil && rules.SkipUpdate(desiredTable, name) {
				continue
			}

			setClauses = append(setClauses, &SetClause[Quoter]{
				ColumnName: name,
				Expression: desiredRow[name],
			})
		}

		if len(setClauses) == 0 {
			continue
		}

		modifications = append(modifications, &UpdateInstruction[Quoter]{
			TableName:  rules.TableName(desiredTable),
			SetClauses: setClauses,
			Condition:  RowKeyCondition[Quoter](primaryKeyColumnNames, desiredRow),
		})
	}

	for _, key := range currentData.Keys {
		_, found := desiredData.Rows[key]
		if found {
			continue
		}

		removals = append(removals, &DeleteInstruction[Quoter]{
			TableName: rules.TableName(currentTable),
			Condition: RowKeyCondition[Quoter](primaryKeyColumnNames, currentData.Rows[key]),
		})
	}

	return slices.Concat(insertions, modifications), removals, nil
}

func insertRowInstruction[Table any, Quoter IdentifierQuoter](
	rules DataRules[Table, Quoter],
	table Table,
	columnNames []string,
	row map[string]string,
) Instruction {
	values := lo.Map(columnNames, func(name string, _ int) string {
		return row[name]
	})

	if rules.Insert != nil {
		return rules.Insert(table, columnNames, values)
	}

	return &InsertInstruction[Quoter]{
		TableName:   rules.TableName(table),
		ColumnNames: columnNames,
		Expressions: values,
	}
}

// The schema section creates this table with no row, so every desired row becomes an
// INSERT statement.
func tableDataInstructions[Table any, Quoter IdentifierQuoter](
	ctx context.Context,
	db *sql.DB,
	desiredTable Table,
	rules DataRules[Table, Quoter],
) ([]Instruction, error) {
	desiredColumnNames := rules.WritableColumns(desiredTable)
	if len(desiredColumnNames) == 0 {
		return nil, nil
	}

	// An empty column list breaks the ORDER BY clause of the SELECT statement.
	orderColumnNames, err := rules.PrimaryKey(ctx, db, desiredTable)
	if err != nil {
		return nil, err
	}

	if len(orderColumnNames) == 0 {
		orderColumnNames = desiredColumnNames
	}

	desiredData, err := getTableData(ctx, db, desiredTable, desiredColumnNames, orderColumnNames, rules)
	if err != nil {
		return nil, err
	}

	var instructions []Instruction

	for _, key := range desiredData.Keys {
		instructions = append(instructions, insertRowInstruction(rules, desiredTable, desiredColumnNames, desiredData.Rows[key]))
	}

	return instructions, nil
}

// The engines give no stable order, so the ORDER BY clause keeps the output equal
// between two runs.
func getTableData[Table any, Quoter IdentifierQuoter](
	ctx context.Context,
	db *sql.DB,
	table Table,
	columnNames []string,
	primaryKeyColumnNames []string,
	rules DataRules[Table, Quoter],
) (*tableData, error) {
	selectExpression := rules.SelectExpression
	if selectExpression == nil {
		selectExpression = quoteWith[Quoter]
	}

	// A generated column is not writable, but it can be a primary key column. The row key
	// needs the value of each primary key column.
	columnNames = lo.Union(columnNames, primaryKeyColumnNames)

	selectExpressions := lo.Map(columnNames, func(name string, _ int) string {
		return selectExpression(name)
	})

	quotedKeyColumnNames := lo.Map(primaryKeyColumnNames, func(name string, _ int) string {
		return quoteWith[Quoter](name)
	})

	statement := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s;",
		strings.Join(selectExpressions, ", "),
		quoteWith[Quoter](rules.TableName(table)),
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
			// The value becomes an SQL literal here, so NULL never equals the text 'NULL'.
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
