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

type PostgresTableData struct {
	Keys []string
	Rows map[string]map[string]string
}

// The schema section already creates or drops the other tables.
func (d *PostgresDriver) DiffData(ctx context.Context) ([]Instruction, error) {
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
		// A SELECT of the parent returns the rows of each partition, so the partition
		// gets no comparison of its own.
		if targetTable.IsPartition() {
			continue
		}

		sourceTable, found := lo.Find(sourceTables, func(table *PostgresTable) bool {
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

func (d *PostgresDriver) DiffTableData(ctx context.Context, targetTable *PostgresTable, sourceTable *PostgresTable) ([]Instruction, error) {
	primaryKeyColumnNames, err := d.GetTablePrimaryKey(ctx, d.TargetDatabaseConnection, targetTable.Name)
	if err != nil {
		return nil, err
	}

	if len(primaryKeyColumnNames) == 0 {
		comment := &SQLCommentInstruction{
			Text: fmt.Sprintf("The table %s holds no primary key, so dbdiff compares no row of it.",
				QuoteIdentifier(targetTable.Name)),
		}

		return []Instruction{comment}, nil
	}

	targetColumnNames := writablePostgresColumnNames(targetTable.Columns)
	sourceColumnNames := writablePostgresColumnNames(sourceTable.Columns)

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

			// PostgreSQL refuses a plain value for a GENERATED ALWAYS identity column.
			if holdsAlwaysIdentityColumn(targetTable, targetColumnNames) {
				insertions = append(insertions, &PostgresInsertOverridingInstruction{
					TableName:   targetTable.Name,
					ColumnNames: targetColumnNames,
					Expressions: values,
				})
			} else {
				insertions = append(insertions, &SQLInsertInstruction{
					TableName:   targetTable.Name,
					ColumnNames: targetColumnNames,
					Expressions: values,
				})
			}

			continue
		}

		var setClauses []*SQLSetClause

		for _, name := range commonColumnNames {
			if targetRow[name] == sourceRow[name] {
				continue
			}

			// PostgreSQL refuses an UPDATE of a GENERATED ALWAYS identity column.
			if holdsAlwaysIdentityColumn(targetTable, []string{name}) {
				continue
			}

			setClauses = append(setClauses, &SQLSetClause{
				ColumnName: name,
				Expression: targetRow[name],
			})
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
	`, QuoteIdentifier(tableName))
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

// PostgreSQL gives no stable order, so this sort keeps the output equal between two runs.
func (d *PostgresDriver) GetTableData(ctx context.Context, db *sql.DB, tableName string, columnNames []string, primaryKeyColumnNames []string) (*PostgresTableData, error) {
	statement := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s;",
		strings.Join(QuoteIdentifiers(columnNames), ", "),
		QuoteIdentifier(tableName),
		strings.Join(QuoteIdentifiers(primaryKeyColumnNames), ", "))

	rows, err := db.QueryContext(ctx, statement)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	data := &PostgresTableData{
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
			row[name] = formatPostgresValue(values[i], columnTypes[i].DatabaseTypeName())
		}

		key := postgresRowKey(primaryKeyColumnNames, row)

		data.Keys = append(data.Keys, key)
		data.Rows[key] = row
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return data, nil
}

const postgresTimeLayout = "2006-01-02 15:04:05.999999-07:00"

// The diff compares two rows through these literals, so NULL never equals the text 'NULL'.
func formatPostgresValue(value any, databaseTypeName string) string {
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

	doubleValue, isDouble := value.(float64)
	if isDouble {
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
			return quoteLiteral(`\x` + hex.EncodeToString(byteaValue))
		}

		return quoteLiteral(string(byteaValue))
	}

	timeValue, isTime := value.(time.Time)
	if isTime {
		return quoteLiteral(timeValue.Format(postgresTimeLayout))
	}

	return quoteLiteral(fmt.Sprintf("%v", value))
}

func postgresRowKey(primaryKeyColumnNames []string, row map[string]string) string {
	literals := lo.Map(primaryKeyColumnNames, func(name string, _ int) string {
		return row[name]
	})

	return strings.Join(literals, ", ")
}
