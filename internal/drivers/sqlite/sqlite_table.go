package driverssqlite

import (
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
func columnOrderChanged(desired *SQLiteTable, current *SQLiteTable) bool {
	return !slices.Equal(
		commonColumnNamesInOrder(desired, current),
		commonColumnNamesInOrder(current, desired))
}

func commonColumnNamesInOrder(first *SQLiteTable, second *SQLiteTable) []string {
	var names []string

	for _, column := range first.Columns {
		_, found := second.ColumnByName(column.Name)
		if found {
			names = append(names, column.Name)
		}
	}

	return names
}

func (t *SQLiteTable) DiffColumns(other *SQLiteTable) *SQLiteTableColumnsDiff {
	diff := &SQLiteTableColumnsDiff{
		Added:              []string{},
		Modified:           []string{},
		Removed:            []string{},
		RenamedFrom:        driversshared.GuessRenamedColumns(t.Columns, other.Columns, columnName, (*SQLiteColumn).HasEqualAttributes),
		ForeignKeysChanged: false,
		ConstraintsChanged: !slices.Equal(t.PrimaryKey, other.PrimaryKey) ||
			t.PrimaryKeyName != other.PrimaryKeyName ||
			t.PrimaryKeyConflict != other.PrimaryKeyConflict ||
			!slices.Equal(t.PrimaryKeyKeys, other.PrimaryKeyKeys) ||
			!equalUniqueConstraints(t.UniqueConstraints, other.UniqueConstraints),
		TableOptionsChanged: t.WithoutRowID != other.WithoutRowID || t.Strict != other.Strict ||
			!equalCheckConstraints(t.CheckConstraints, other.CheckConstraints),
		ColumnOrderChanged: columnOrderChanged(t, other),
	}

	for _, desiredColumn := range t.Columns {
		currentColumn, found := other.ColumnByName(desiredColumn.Name)
		if !found {
			_, renamed := diff.RenamedFrom[desiredColumn.Name]
			if renamed {
				continue
			}

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
) ([]driversshared.Instruction, error) {
	columnsDiff := t.DiffColumns(other)

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
			return index.Name
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

	for reservedNames[strings.ToLower(name)] {
		counter++
		name = "_" + tableName + "_temp" + strconv.Itoa(counter)
	}

	return name
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

		_, found := current.ColumnByName(newColumn.Name)
		if found {
			selectColumns = append(selectColumns, driversshared.QuoteIdentifier(newColumn.Name))
			continue
		}

		oldName, renamed := renamedFrom[newColumn.Name]
		if renamed {
			selectColumns = append(selectColumns, driversshared.QuoteIdentifier(oldName))
			continue
		}

		if newColumn.Default.Valid {
			selectColumns = append(selectColumns, newColumn.Default.String)
		} else {
			selectColumns = append(selectColumns, driversshared.SQLNullLiteral)
		}
	}

	recreation := []driversshared.Instruction{
		temporaryTable.CreateTableInstruction(),
		&driversshared.SQLInsertSelectInstruction{
			TableName:         temporaryTable.Name,
			ColumnNames:       insertColumns,
			SelectExpressions: selectColumns,
			FromTableName:     t.Name,
		},
		&driversshared.SQLDropTableInstruction{
			Name: t.Name,
		},
		&SQLiteAlterTableInstruction{
			Name: temporaryTable.Name,
			Action: &driversshared.SQLRenameTableAction{
				NewName: t.Name,
			},
		},
	}

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
