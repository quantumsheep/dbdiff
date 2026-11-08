package driverssqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

// The slice holds a pointer, so slices.Equal compares the address and never the check.
// The parser fills the list in the order of the CREATE TABLE text, and a new order of
// two equal checks changes nothing, so the comparison sorts the two lists first.
func equalCheckConstraints(first []*SQLiteCheckConstraint, second []*SQLiteCheckConstraint) bool {
	return slices.EqualFunc(sortedCheckConstraints(first), sortedCheckConstraints(second),
		func(firstCheck *SQLiteCheckConstraint, secondCheck *SQLiteCheckConstraint) bool {
			return firstCheck.Equal(secondCheck)
		})
}

func sortedCheckConstraints(constraints []*SQLiteCheckConstraint) []*SQLiteCheckConstraint {
	sorted := slices.Clone(constraints)

	slices.SortFunc(sorted, func(first *SQLiteCheckConstraint, second *SQLiteCheckConstraint) int {
		if first.Name != second.Name {
			return strings.Compare(first.Name, second.Name)
		}

		return strings.Compare(first.Expression, second.Expression)
	})

	return sorted
}

func equalUniqueConstraints(first []*SQLiteUniqueConstraint, second []*SQLiteUniqueConstraint) bool {
	return slices.EqualFunc(first, second,
		func(firstConstraint *SQLiteUniqueConstraint, secondConstraint *SQLiteUniqueConstraint) bool {
			return firstConstraint.Equal(secondConstraint)
		})
}

type SQLiteTable struct {
	Name    string
	Columns []*SQLiteColumn

	// PrimaryKey holds a key of two or more columns only. UniqueConstraints holds a
	// constraint of two or more columns or a named constraint only. A column holds each
	// other key as a column constraint.
	PrimaryKey         []string
	PrimaryKeyName     string
	PrimaryKeyConflict string
	UniqueConstraints  []*SQLiteUniqueConstraint

	// A key clause holds the quoted column name with its COLLATE clause and its DESC
	// keyword, which no PRAGMA reports.
	PrimaryKeyKeys []string

	Indexes     []*SQLiteIndex
	Triggers    []*SQLiteTrigger
	ForeignKeys []*SQLiteForeignKey

	CheckConstraints []*SQLiteCheckConstraint

	WithoutRowID bool
	Strict       bool
}

func (t *SQLiteTable) ColumnByName(name string) (*SQLiteColumn, bool) {
	for _, column := range t.Columns {
		if column.Name == name {
			return column, true
		}
	}

	return nil, false
}

func columnName(column *SQLiteColumn) string {
	return column.Name
}

func (t *SQLiteTable) CreateTableInstruction() *SQLiteCreateTableInstruction {
	return &SQLiteCreateTableInstruction{
		Name:               t.Name,
		Columns:            t.Columns,
		PrimaryKey:         t.PrimaryKey,
		PrimaryKeyName:     t.PrimaryKeyName,
		PrimaryKeyConflict: t.PrimaryKeyConflict,
		PrimaryKeyKeys:     t.PrimaryKeyKeys,
		UniqueConstraints:  t.UniqueConstraints,
		ForeignKeys:        t.ForeignKeys,
		CheckConstraints:   t.CheckConstraints,
		WithoutRowID:       t.WithoutRowID,
		Strict:             t.Strict,
	}
}

func (t *SQLiteTable) IndexInstructions() []driversshared.Instruction {
	return lo.Map(t.Indexes, func(index *SQLiteIndex, _ int) driversshared.Instruction {
		return index.CreateInstruction()
	})
}

func (t *SQLiteTable) TriggerInstructions() []driversshared.Instruction {
	return lo.Map(t.Triggers, func(trigger *SQLiteTrigger, _ int) driversshared.Instruction {
		return &SQLiteCreateTriggerInstruction{Definition: trigger.SQL}
	})
}

func (t *SQLiteTable) Instructions() []driversshared.Instruction {
	instructions := []driversshared.Instruction{t.CreateTableInstruction()}
	instructions = append(instructions, t.IndexInstructions()...)
	instructions = append(instructions, t.TriggerInstructions()...)

	return instructions
}

type SQLiteTableColumnsDiff struct {
	Added       []string
	Modified    []string
	Removed     []string
	RenamedFrom map[string]string

	ForeignKeysChanged  bool
	ConstraintsChanged  bool
	TableOptionsChanged bool
	ColumnOrderChanged  bool

	AddsColumnThatNeedsRecreation    bool
	RemovesColumnThatNeedsRecreation bool
}

// SQLite supports no ALTER COLUMN, so a modified column, a changed foreign key, a changed
// table constraint, or a changed table option needs a new table. SQLite also refuses some
// ADD COLUMN and DROP COLUMN actions.
func (d *SQLiteTableColumnsDiff) NeedsRecreation() bool {
	return len(d.Modified) > 0 || d.ForeignKeysChanged || d.ConstraintsChanged ||
		d.TableOptionsChanged || d.ColumnOrderChanged ||
		d.AddsColumnThatNeedsRecreation || d.RemovesColumnThatNeedsRecreation
}

// SQLite refuses a DROP COLUMN action for each of these columns. The rules are at
// https://www.sqlite.org/lang_altertable.html.
func (t *SQLiteTable) NeedsRecreationToDrop(column *SQLiteColumn) bool {
	if column.PrimaryKey || column.Unique || slices.Contains(t.PrimaryKey, column.Name) {
		return true
	}

	for _, constraint := range t.UniqueConstraints {
		if slices.Contains(constraint.Columns, column.Name) {
			return true
		}
	}

	for _, foreignKey := range t.ForeignKeys {
		if slices.Contains(foreignKey.From, column.Name) {
			return true
		}
	}

	for _, otherColumn := range t.Columns {
		if otherColumn == column {
			continue
		}

		if expressionReferencesColumn(otherColumn.GeneratedExpression, column.Name) ||
			expressionReferencesColumn(otherColumn.Checks, column.Name) {
			return true
		}
	}

	for _, check := range t.CheckConstraints {
		if expressionReferencesColumn(check.Expression, column.Name) {
			return true
		}
	}

	for _, index := range t.Indexes {
		if expressionReferencesColumn(index.Where, column.Name) {
			return true
		}

		for _, key := range index.Keys {
			if expressionReferencesColumn(key, column.Name) {
				return true
			}
		}
	}

	return false
}

// A word that only looks like the column name, for example a function name, gives a
// recreation that is not necessary. That recreation is correct.
func expressionReferencesColumn(expression string, columnName string) bool {
	index := 0

	for index < len(expression) {
		end := endOfQuotedTextOrComment(expression, index)
		if end > index {
			if expression[index] != '\'' && strings.EqualFold(unquoteIdentifier(expression[index:end]), columnName) {
				return true
			}

			index = end

			continue
		}

		for end < len(expression) && isIdentifierCharacter(expression[end]) {
			end++
		}

		if end == index {
			index++

			continue
		}

		if strings.EqualFold(expression[index:end], columnName) {
			return true
		}

		index = end
	}

	return false
}

func isIdentifierCharacter(character byte) bool {
	return character == '_' || character == '$' || character >= 0x80 ||
		(character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
		(character >= '0' && character <= '9')
}

// A SELECT * statement and an INSERT statement without column names read the order of
// the columns, so a new order needs a recreation.
// A renamed column keeps its place, so the check compares the current names on both sides.
func columnOrderChanged(desired *SQLiteTable, current *SQLiteTable, renamedFrom map[string]string) bool {
	currentNames := lo.Map(desired.Columns, func(column *SQLiteColumn, _ int) string {
		oldName, renamed := renamedFrom[column.Name]
		if renamed {
			return oldName
		}

		return column.Name
	})

	desiredOrder := lo.Filter(currentNames, func(name string, _ int) bool {
		_, found := current.ColumnByName(name)

		return found
	})

	currentOrder := lo.FilterMap(current.Columns, func(column *SQLiteColumn, _ int) (string, bool) {
		return column.Name, slices.Contains(currentNames, column.Name)
	})

	return !slices.Equal(desiredOrder, currentOrder)
}

func (t *SQLiteTable) DiffColumns(other *SQLiteTable, guessRenames bool) *SQLiteTableColumnsDiff {
	renamedFrom := driversshared.RenamedColumns(t.Columns, other.Columns, columnName, identifierKey,
		(*SQLiteColumn).HasEqualAttributes, guessRenames)

	diff := &SQLiteTableColumnsDiff{
		Added:              []string{},
		Modified:           []string{},
		Removed:            []string{},
		RenamedFrom:        renamedFrom,
		ForeignKeysChanged: false,
		ConstraintsChanged: !slices.Equal(t.PrimaryKey, other.PrimaryKey) ||
			t.PrimaryKeyName != other.PrimaryKeyName ||
			t.PrimaryKeyConflict != other.PrimaryKeyConflict ||
			!slices.Equal(t.PrimaryKeyKeys, other.PrimaryKeyKeys) ||
			!equalUniqueConstraints(t.UniqueConstraints, other.UniqueConstraints),
		TableOptionsChanged: t.WithoutRowID != other.WithoutRowID || t.Strict != other.Strict ||
			!equalCheckConstraints(t.CheckConstraints, other.CheckConstraints),
		ColumnOrderChanged: columnOrderChanged(t, other, renamedFrom),
	}

	// ADD COLUMN puts the column after every other column, so a column that comes after an
	// added column in the desired order needs a recreation.
	addedColumnSeen := false

	for _, desiredColumn := range t.Columns {
		currentColumn, found := other.ColumnByName(desiredColumn.Name)
		_, renamed := diff.RenamedFrom[desiredColumn.Name]
		if (found || renamed) && addedColumnSeen {
			diff.ColumnOrderChanged = true
		}

		if !found {
			if renamed {
				oldColumn, _ := other.ColumnByName(diff.RenamedFrom[desiredColumn.Name])
				if !desiredColumn.HasEqualAttributes(oldColumn) {
					diff.Modified = append(diff.Modified, desiredColumn.Name)
				}

				continue
			}

			addedColumnSeen = true
			diff.Added = append(diff.Added, desiredColumn.Name)

			if desiredColumn.NeedsRecreationToAdd() {
				diff.AddsColumnThatNeedsRecreation = true
			}

			continue
		}

		if *desiredColumn == *currentColumn {
			continue
		}

		diff.Modified = append(diff.Modified, desiredColumn.Name)
	}

	for _, currentColumn := range other.Columns {
		_, found := t.ColumnByName(currentColumn.Name)
		_, renamed := lo.FindKey(diff.RenamedFrom, currentColumn.Name)
		if !found && !renamed {
			diff.Removed = append(diff.Removed, currentColumn.Name)

			if other.NeedsRecreationToDrop(currentColumn) {
				diff.RemovesColumnThatNeedsRecreation = true
			}
		}
	}

	if len(t.ForeignKeys) != len(other.ForeignKeys) {
		diff.ForeignKeysChanged = true
	} else {
		for _, desiredForeignKey := range t.ForeignKeys {
			found := lo.SomeBy(other.ForeignKeys, func(foreignKey *SQLiteForeignKey) bool {
				return foreignKey.Equal(desiredForeignKey)
			})
			if !found {
				diff.ForeignKeysChanged = true
				break
			}
		}
	}

	return diff
}

func (t *SQLiteTable) DiffTable(
	other *SQLiteTable,
	reservedNames map[string]bool,
	guessRenames bool,
) ([]driversshared.Instruction, error) {
	columnsDiff := t.DiffColumns(other, guessRenames)

	// SQLite holds no ALTER COLUMN, so a changed column or constraint recreates the table.
	if columnsDiff.NeedsRecreation() {
		return t.recreationInstructions(other, columnsDiff.RenamedFrom, reservedNames), nil
	}

	var instructions []driversshared.Instruction

	// The map gives no stable order. The loop walks the desired columns instead, so the
	// rename statements follow the shape of the desired table on every run.
	for _, column := range t.Columns {
		oldName, renamed := columnsDiff.RenamedFrom[column.Name]
		if !renamed {
			continue
		}

		instructions = append(instructions, &SQLiteAlterTableInstruction{
			Name: t.Name,
			Action: &driversshared.SQLRenameColumnAction{
				ColumnName:    oldName,
				NewColumnName: column.Name,
			},
		})
	}

	for _, columnName := range columnsDiff.Removed {
		instructions = append(instructions, &SQLiteAlterTableInstruction{
			Name:   t.Name,
			Action: &driversshared.SQLDropColumnAction{ColumnName: columnName},
		})
	}

	for _, columnName := range columnsDiff.Added {
		column, ok := t.ColumnByName(columnName)
		if !ok {
			return nil, fmt.Errorf("internal error: added column %s not found in table %s", columnName, t.Name)
		}

		instructions = append(instructions, &SQLiteAlterTableInstruction{
			Name:   t.Name,
			Action: &SQLiteAddColumnAction{Column: column},
		})
	}

	indexInstructions, err := t.DiffIndexes(other)
	if err != nil {
		return nil, err
	}

	instructions = append(instructions, indexInstructions...)

	triggerInstructions, err := t.DiffTriggers(other)
	if err != nil {
		return nil, err
	}

	instructions = append(instructions, triggerInstructions...)

	return instructions, nil
}

func (t *SQLiteTable) DiffTriggers(other *SQLiteTable) ([]driversshared.Instruction, error) {
	additions, removals, err := driversshared.DiffByKey(t.Triggers, other.Triggers, diffTriggerRules())
	if err != nil {
		return nil, err
	}

	return append(additions, removals...), nil
}

func (t *SQLiteTable) DiffIndexes(other *SQLiteTable) ([]driversshared.Instruction, error) {
	additions, removals, err := driversshared.DiffByKey(t.Indexes, other.Indexes, driversshared.ReplaceRules(
		func(index *SQLiteIndex) string {
			return identifierKey(index.Name)
		},
		(*SQLiteIndex).Equal,
		func(index *SQLiteIndex) driversshared.Instruction {
			return index.CreateInstruction()
		},
		func(index *SQLiteIndex) driversshared.Instruction {
			return &driversshared.SQLDropIndexInstruction{
				Name: index.Name,
			}
		}))
	if err != nil {
		return nil, err
	}

	return append(additions, removals...), nil
}

// The temporary table must not take the name of an object of the two sides, because
// SQLite refuses a CREATE TABLE statement for a name that is in use.
func temporaryTableName(tableName string, reservedNames map[string]bool) string {
	name := "_" + tableName + "_temp"
	counter := 1

	for reservedNames[identifierKey(name)] {
		counter++
		name = "_" + tableName + "_temp" + strconv.Itoa(counter)
	}

	return name
}

// Without an INTEGER PRIMARY KEY column, the copy gives new rowid values to the rows. An FTS
// index or an application can store a rowid. A column can take the name rowid, so the copy
// uses the first rowid name that no column takes.
func copiedRowIDName(desired *SQLiteTable, current *SQLiteTable) (string, bool) {
	if desired.WithoutRowID || current.WithoutRowID || hasRowIDAlias(desired) {
		return "", false
	}

	for _, name := range []string{"rowid", "_rowid_", "oid"} {
		takenByAColumn := lo.SomeBy(slices.Concat(desired.Columns, current.Columns), func(column *SQLiteColumn) bool {
			return identifierKey(column.Name) == name
		})
		if !takenByAColumn {
			return name, true
		}
	}

	return "", false
}

func hasRowIDAlias(table *SQLiteTable) bool {
	return lo.SomeBy(table.Columns, func(column *SQLiteColumn) bool {
		return column.PrimaryKey && strings.EqualFold(column.Type, "INTEGER")
	})
}

// A column definition reads DEFAULT "x" as the text x, but a SELECT statement reads "x" as
// the column x. Therefore, the copy writes the text as a string literal.
func defaultExpression(defaultText string) string {
	if strings.HasPrefix(defaultText, `"`) && endOfQuotedTextOrComment(defaultText, 0) == len(defaultText) {
		return driversshared.QuoteLiteral(unquoteIdentifier(defaultText))
	}

	return defaultText
}

// A NULL value fails the NOT NULL constraint of the copy. The default gives the value that
// ADD COLUMN gives to a NOT NULL column.
func copyExpression(newColumn *SQLiteColumn, currentColumn *SQLiteColumn) string {
	expression := driversshared.QuoteIdentifier(currentColumn.Name)

	if newColumn.NotNull && !currentColumn.NotNull && newColumn.Default.Valid {
		return fmt.Sprintf("COALESCE(%s, %s)", expression, defaultExpression(newColumn.Default.String))
	}

	return expression
}

func (t *SQLiteTable) recreationInstructions(
	current *SQLiteTable,
	renamedFrom map[string]string,
	reservedNames map[string]bool,
) []driversshared.Instruction {
	temporaryTable := *t
	temporaryTable.Name = temporaryTableName(t.Name, reservedNames)

	var insertColumns []string
	var selectColumns []string

	for _, newColumn := range t.Columns {
		// SQLite computes a generated column, and it refuses a value for that column.
		if newColumn.IsGenerated() {
			continue
		}

		insertColumns = append(insertColumns, newColumn.Name)

		sourceName := newColumn.Name

		oldName, renamed := renamedFrom[newColumn.Name]
		if renamed {
			sourceName = oldName
		}

		currentColumn, found := current.ColumnByName(sourceName)
		if found {
			selectColumns = append(selectColumns, copyExpression(newColumn, currentColumn))
			continue
		}

		if newColumn.Default.Valid {
			selectColumns = append(selectColumns, defaultExpression(newColumn.Default.String))
		} else {
			selectColumns = append(selectColumns, driversshared.SQLNullLiteral)
		}
	}

	rowIDName, copiesRowID := copiedRowIDName(t, current)
	if copiesRowID {
		insertColumns = slices.Insert(insertColumns, 0, rowIDName)
		selectColumns = slices.Insert(selectColumns, 0, driversshared.QuoteIdentifier(rowIDName))
	}

	recreation := []driversshared.Instruction{
		temporaryTable.CreateTableInstruction(),
		&driversshared.SQLInsertSelectInstruction{
			TableName:         temporaryTable.Name,
			ColumnNames:       insertColumns,
			SelectExpressions: selectColumns,
			FromTableName:     t.Name,
		},
	}

	// The DROP TABLE statement removes the sqlite_sequence row of the table, and the copy of
	// the rows sets the counter of the temporary table to the largest copied key only. The
	// counter must move to the temporary table before the drop, or SQLite gives a deleted key
	// again. The CREATE TABLE statement of an AUTOINCREMENT column creates sqlite_sequence.
	hasAutoIncrement := lo.SomeBy(t.Columns, func(column *SQLiteColumn) bool {
		return column.AutoIncrement
	})
	if hasAutoIncrement {
		recreation = append(recreation,
			&SQLiteDeleteSequenceInstruction{
				TableName: temporaryTable.Name,
			},
			&SQLiteCopySequenceInstruction{
				FromTableName: t.Name,
				ToTableName:   temporaryTable.Name,
			})
	}

	recreation = append(recreation,
		&driversshared.SQLDropTableInstruction{
			Name: t.Name,
		},
		&SQLiteAlterTableInstruction{
			Name: temporaryTable.Name,
			Action: &driversshared.SQLRenameTableAction{
				NewName: t.Name,
			},
		})

	// The DROP TABLE statement removes each index and each trigger of the table. The
	// recreation builds every one of them again from the desired table, so the index diff and
	// the trigger diff of DiffTable compare a current table that is not there.
	recreation = append(recreation, t.IndexInstructions()...)
	recreation = append(recreation, t.TriggerInstructions()...)

	return lo.Map(recreation, func(instruction driversshared.Instruction, _ int) driversshared.Instruction {
		return &SQLiteTableRecreationInstruction{
			Instruction: instruction,
			TableName:   t.Name,
		}
	})
}

// A virtual table takes its own statement, and a shadow table belongs to its module.
// The order of sqlite_master is the order of the creation, so a table that holds a foreign
// key comes after the table that it names. Keep that order.
func (d *SQLiteDriver) GetTables(ctx context.Context, db *sql.DB) ([]*SQLiteTable, error) {
	tableNames, err := driversshared.QueryAll(ctx, db, `
		SELECT master.name
		FROM sqlite_master AS master
		WHERE master.type = 'table' AND master.name NOT LIKE 'sqlite_%'
		AND master.name <> ?
		AND NOT EXISTS (
			SELECT 1 FROM pragma_table_list AS list
			WHERE list.schema = 'main' AND list.name = master.name
			AND list.type IN ('virtual', 'shadow')
		)
		ORDER BY master.rowid;
	`, func(rows *sql.Rows) (string, error) {
		var tableName string

		err := rows.Scan(&tableName)

		return tableName, err
	}, driversshared.MigrationHistoryTableName)
	if err != nil {
		return nil, err
	}

	var tables []*SQLiteTable

	for _, tableName := range tableNames {
		if d.isIgnoredTable(tableName) {
			continue
		}

		table, err := d.GetTable(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		tables = append(tables, table)
	}

	return tables, nil
}

func (d *SQLiteDriver) isIgnoredTable(tableName string) bool {
	return slices.ContainsFunc(d.IgnoreTables, func(ignoredTableName string) bool {
		return identifierKey(ignoredTableName) == identifierKey(tableName)
	})
}

func (d *SQLiteDriver) GetTable(ctx context.Context, db *sql.DB, tableName string) (*SQLiteTable, error) {
	definition, err := d.GetTableDefinition(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	parsed := parseTableDefinition(definition)

	columns, primaryKey, err := d.GetTableColumns(ctx, db, tableName, parsed)
	if err != nil {
		return nil, err
	}

	indexList, err := d.getIndexList(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	uniqueKeys, err := d.GetTableUniqueKeys(ctx, db, indexList)
	if err != nil {
		return nil, err
	}

	var uniqueConstraints []*SQLiteUniqueConstraint

	for _, key := range uniqueKeys {
		parsedUnique := parsed.UniqueConstraintOf(key)

		// A column constraint holds no name, so a named constraint of one column stays a
		// table constraint. Without that rule the name goes away.
		if len(key) != 1 || parsedUnique.Name != "" {
			uniqueConstraints = append(uniqueConstraints, &SQLiteUniqueConstraint{
				Name:     parsedUnique.Name,
				Columns:  key,
				Keys:     parsedUnique.Keys,
				Conflict: parsedUnique.Conflict,
			})

			continue
		}

		matchingColumn, found := lo.Find(columns, func(column *SQLiteColumn) bool {
			return column.Name == key[0]
		})
		if found {
			matchingColumn.Unique = true

			// The table form of the constraint holds the ON CONFLICT clause, and the
			// column keeps it.
			if matchingColumn.UniqueConflict == "" {
				matchingColumn.UniqueConflict = parsedUnique.Conflict
			}
		}
	}

	indexes, err := d.GetTableIndexes(ctx, db, tableName, indexList)
	if err != nil {
		return nil, err
	}

	triggers, err := d.GetTriggers(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	foreignKeys, err := d.GetTableForeignKeys(ctx, db, tableName, parsed.ForeignKeys)
	if err != nil {
		return nil, err
	}

	primaryKeyKeys := parsed.PrimaryKeyKeys
	if len(primaryKeyKeys) != len(primaryKey) {
		primaryKeyKeys = nil
	}

	return &SQLiteTable{
		Name:               tableName,
		Columns:            columns,
		PrimaryKey:         primaryKey,
		PrimaryKeyKeys:     primaryKeyKeys,
		UniqueConstraints:  uniqueConstraints,
		Indexes:            indexes,
		Triggers:           triggers,
		ForeignKeys:        foreignKeys,
		CheckConstraints:   parsed.CheckConstraints,
		PrimaryKeyName:     parsed.PrimaryKeyName,
		PrimaryKeyConflict: parsed.PrimaryKeyConflict,
		WithoutRowID:       parsed.WithoutRowID,
		Strict:             parsed.Strict,
	}, nil
}

func (d *SQLiteDriver) GetTableDefinition(ctx context.Context, db *sql.DB, tableName string) (string, error) {
	row := db.QueryRowContext(ctx,
		"SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?;", tableName)

	var definition sql.NullString

	err := row.Scan(&definition)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}

	if err != nil {
		return "", err
	}

	return definition.String, nil
}
