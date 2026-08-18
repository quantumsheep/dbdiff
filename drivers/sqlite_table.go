package drivers

import (
	"fmt"
	"slices"

	"github.com/samber/lo"
)

func equalColumnGroups(first [][]string, second [][]string) bool {
	return slices.EqualFunc(first, second, func(firstGroup []string, secondGroup []string) bool {
		return slices.Equal(firstGroup, secondGroup)
	})
}

type SQLiteTable struct {
	Name    string
	Columns []*SQLiteColumn

	// A key or a constraint of one column stays a column constraint. These two fields hold
	// the columns of a key or of a constraint of two or more columns only.
	PrimaryKey        []string
	UniqueConstraints [][]string

	Indexes     []*SQLiteIndex
	Triggers    []*SQLiteTrigger
	ForeignKeys []*SQLiteForeignKey
}

func (t *SQLiteTable) Copy() *SQLiteTable {
	tableCopy := *t
	return &tableCopy
}

func (t *SQLiteTable) ColumnByName(name string) (*SQLiteColumn, bool) {
	for _, column := range t.Columns {
		if column.Name == name {
			return column, true
		}
	}

	return nil, false
}

func (t *SQLiteTable) IndexByName(name string) (*SQLiteIndex, bool) {
	for _, index := range t.Indexes {
		if index.Name == name {
			return index, true
		}
	}

	return nil, false
}

func (t *SQLiteTable) TriggerByName(name string) (*SQLiteTrigger, bool) {
	for _, trigger := range t.Triggers {
		if trigger.Name == name {
			return trigger, true
		}
	}

	return nil, false
}

func (t *SQLiteTable) CreateTableInstruction() *SQLiteCreateTableInstruction {
	return &SQLiteCreateTableInstruction{
		Name:              t.Name,
		Columns:           t.Columns,
		PrimaryKey:        t.PrimaryKey,
		UniqueConstraints: t.UniqueConstraints,
		ForeignKeys:       t.ForeignKeys,
	}
}

func (t *SQLiteTable) IndexInstructions() []Instruction {
	return lo.Map(t.Indexes, func(index *SQLiteIndex, _ int) Instruction {
		return index.CreateInstruction()
	})
}

func (t *SQLiteTable) TriggerInstructions() []Instruction {
	return lo.Map(t.Triggers, func(trigger *SQLiteTrigger, _ int) Instruction {
		return &SQLiteCreateTriggerInstruction{Definition: trigger.SQL}
	})
}

// Instructions returns the statement that creates the table, then the statements of its
// indexes, then the statements of its triggers.
func (t *SQLiteTable) Instructions() []Instruction {
	instructions := []Instruction{t.CreateTableInstruction()}
	instructions = append(instructions, t.IndexInstructions()...)
	instructions = append(instructions, t.TriggerInstructions()...)

	return instructions
}

type SQLiteTableColumnsDiff struct {
	Added    []string
	Modified []string
	Removed  []string
	Renamed  map[string]string

	ForeignKeysChanged bool
	ConstraintsChanged bool
}

func (t *SQLiteTable) DiffColumns(other *SQLiteTable) *SQLiteTableColumnsDiff {
	diff := &SQLiteTableColumnsDiff{
		Added:              []string{},
		Modified:           []string{},
		Removed:            []string{},
		Renamed:            make(map[string]string),
		ForeignKeysChanged: false,
		ConstraintsChanged: !slices.Equal(t.PrimaryKey, other.PrimaryKey) || !equalColumnGroups(t.UniqueConstraints, other.UniqueConstraints),
	}

	for _, sourceColumn := range t.Columns {
		targetColumn, found := other.ColumnByName(sourceColumn.Name)
		if !found {
			candidates := lo.Filter(other.Columns, func(column *SQLiteColumn, _ int) bool {
				_, existsInSourceTable := t.ColumnByName(column.Name)
				_, alreadyRenamed := diff.Renamed[column.Name]
				return !existsInSourceTable && !alreadyRenamed && column.HasEqualAttributes(sourceColumn)
			})

			// A rename is a guess. Two candidates make the guess unsafe, so the column
			// becomes an addition and the old columns become removals.
			if len(candidates) == 1 {
				diff.Renamed[candidates[0].Name] = sourceColumn.Name
				continue
			}

			diff.Added = append(diff.Added, sourceColumn.Name)
			continue
		}

		if *sourceColumn == *targetColumn {
			continue
		}

		if sourceColumn.Type != targetColumn.Type {
			if sourceColumn.IsTypeChangeCompatible(targetColumn) {
				diff.Modified = append(diff.Modified, sourceColumn.Name)
				continue
			}

			diff.Removed = append(diff.Removed, targetColumn.Name)
			diff.Added = append(diff.Added, sourceColumn.Name)
			continue
		}

		diff.Modified = append(diff.Modified, sourceColumn.Name)
	}

	for _, targetColumn := range other.Columns {
		_, found := t.ColumnByName(targetColumn.Name)
		if !found && !lo.Contains(lo.Keys(diff.Renamed), targetColumn.Name) {
			diff.Removed = append(diff.Removed, targetColumn.Name)
		}
	}

	if len(t.ForeignKeys) != len(other.ForeignKeys) {
		diff.ForeignKeysChanged = true
	} else {
		for _, sourceForeignKey := range t.ForeignKeys {
			found := lo.SomeBy(other.ForeignKeys, func(foreignKey *SQLiteForeignKey) bool {
				return foreignKey.Equal(sourceForeignKey)
			})
			if !found {
				diff.ForeignKeysChanged = true
				break
			}
		}
	}

	return diff
}

func (t *SQLiteTable) DiffTable(other *SQLiteTable) ([]Instruction, error) {
	columnsDiff := t.DiffColumns(other)

	var instructions []Instruction

	// SQLite supports no ALTER COLUMN, so a modified column, a new foreign key, or a new
	// table constraint needs a recreation of the table.
	if len(columnsDiff.Modified) > 0 || columnsDiff.ForeignKeysChanged || columnsDiff.ConstraintsChanged {
		tempTable := t.Copy()
		tempTable.Name = "_" + t.Name + "_temp"

		instructions = append(instructions, tempTable.CreateTableInstruction())

		newToOld := lo.Invert(columnsDiff.Renamed)

		var insertColumns []string
		var selectColumns []string

		for _, newColumn := range t.Columns {
			insertColumns = append(insertColumns, newColumn.Name)

			_, ok := other.ColumnByName(newColumn.Name)
			if ok {
				selectColumns = append(selectColumns, quoteIdentifier(newColumn.Name))
				continue
			}

			oldName, ok := newToOld[newColumn.Name]
			if ok {
				selectColumns = append(selectColumns, quoteIdentifier(oldName))
				continue
			}

			if newColumn.Default.Valid {
				selectColumns = append(selectColumns, newColumn.Default.String)
			} else {
				selectColumns = append(selectColumns, "NULL")
			}
		}

		instructions = append(instructions, &SQLInsertSelectInstruction{
			TableName:         tempTable.Name,
			ColumnNames:       insertColumns,
			SelectExpressions: selectColumns,
			SourceTableName:   t.Name,
		})

		instructions = append(instructions, &SQLDropTableInstruction{Name: t.Name})

		instructions = append(instructions, &SQLiteAlterTableInstruction{
			Name:   tempTable.Name,
			Action: &SQLRenameTableAction{NewName: t.Name},
		})

		for _, index := range t.Indexes {
			instructions = append(instructions, index.CreateInstruction())
		}

		return instructions, nil
	}

	// The map gives no stable order. The loop walks the source columns instead, so the
	// rename statements follow the shape of the source table on every run.
	newNameToOldName := lo.Invert(columnsDiff.Renamed)

	for _, column := range t.Columns {
		oldName, renamed := newNameToOldName[column.Name]
		if !renamed {
			continue
		}

		instructions = append(instructions, &SQLiteAlterTableInstruction{
			Name: t.Name,
			Action: &SQLRenameColumnAction{
				ColumnName:    oldName,
				NewColumnName: column.Name,
			},
		})
	}

	for _, columnName := range columnsDiff.Removed {
		instructions = append(instructions, &SQLiteAlterTableInstruction{
			Name:   t.Name,
			Action: &SQLDropColumnAction{ColumnName: columnName},
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

	return instructions, nil
}

func (t *SQLiteTable) DiffTriggers(other *SQLiteTable) ([]Instruction, error) {
	var instructions []Instruction

	for _, sourceTrigger := range t.Triggers {
		targetTrigger, found := other.TriggerByName(sourceTrigger.Name)
		if !found {
			instructions = append(instructions, &SQLiteCreateTriggerInstruction{
				Definition: sourceTrigger.SQL,
			})

			continue
		}

		if sourceTrigger.SQL != targetTrigger.SQL {
			instructions = append(instructions,
				&SQLiteDropTriggerInstruction{Name: targetTrigger.Name},
				&SQLiteCreateTriggerInstruction{Definition: sourceTrigger.SQL})
		}
	}

	for _, targetTrigger := range other.Triggers {
		_, found := t.TriggerByName(targetTrigger.Name)
		if !found {
			instructions = append(instructions, &SQLiteDropTriggerInstruction{
				Name: targetTrigger.Name,
			})
		}
	}

	return instructions, nil
}

func (t *SQLiteTable) DiffIndexes(other *SQLiteTable) ([]Instruction, error) {
	var instructions []Instruction

	for _, sourceIndex := range t.Indexes {
		targetIndex, found := other.IndexByName(sourceIndex.Name)
		if !found {
			instructions = append(instructions, sourceIndex.CreateInstruction())
			continue
		}

		if !sourceIndex.Equal(targetIndex) {
			instructions = append(instructions,
				&SQLDropIndexInstruction{Name: targetIndex.Name},
				sourceIndex.CreateInstruction())
		}
	}

	for _, targetIndex := range other.Indexes {
		_, found := t.IndexByName(targetIndex.Name)
		if !found {
			instructions = append(instructions, &SQLDropIndexInstruction{Name: targetIndex.Name})
		}
	}

	return instructions, nil
}
