package drivers

import (
	"fmt"
	"slices"

	"github.com/samber/lo"
)

// The slice holds a pointer, so slices.Equal compares the address and never the check.
func equalCheckConstraints(first []*SQLiteCheckConstraint, second []*SQLiteCheckConstraint) bool {
	return slices.EqualFunc(first, second,
		func(firstCheck *SQLiteCheckConstraint, secondCheck *SQLiteCheckConstraint) bool {
			return firstCheck.Equal(secondCheck)
		})
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

	// These two fields hold a key or a constraint of two or more columns only.
	PrimaryKey         []string
	PrimaryKeyConflict string
	UniqueConstraints  []*SQLiteUniqueConstraint

	Indexes     []*SQLiteIndex
	Triggers    []*SQLiteTrigger
	ForeignKeys []*SQLiteForeignKey

	CheckConstraints []*SQLiteCheckConstraint

	WithoutRowID bool
	Strict       bool
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
		Name:               t.Name,
		Columns:            t.Columns,
		PrimaryKey:         t.PrimaryKey,
		PrimaryKeyConflict: t.PrimaryKeyConflict,
		UniqueConstraints:  t.UniqueConstraints,
		ForeignKeys:        t.ForeignKeys,
		CheckConstraints:   t.CheckConstraints,
		WithoutRowID:       t.WithoutRowID,
		Strict:             t.Strict,
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

	ForeignKeysChanged  bool
	ConstraintsChanged  bool
	TableOptionsChanged bool

	AddsStoredGeneratedColumn bool
}

// SQLite supports no ALTER COLUMN, so a modified column, a changed foreign key, a changed
// table constraint, or a changed table option needs a new table. SQLite also refuses an ADD
// COLUMN action that holds a STORED generated column.
func (d *SQLiteTableColumnsDiff) NeedsRecreation() bool {
	return len(d.Modified) > 0 || d.ForeignKeysChanged || d.ConstraintsChanged ||
		d.TableOptionsChanged || d.AddsStoredGeneratedColumn
}

func (t *SQLiteTable) DiffColumns(other *SQLiteTable) *SQLiteTableColumnsDiff {
	diff := &SQLiteTableColumnsDiff{
		Added:              []string{},
		Modified:           []string{},
		Removed:            []string{},
		Renamed:            make(map[string]string),
		ForeignKeysChanged: false,
		ConstraintsChanged: !slices.Equal(t.PrimaryKey, other.PrimaryKey) ||
			t.PrimaryKeyConflict != other.PrimaryKeyConflict ||
			!equalUniqueConstraints(t.UniqueConstraints, other.UniqueConstraints),
		TableOptionsChanged: t.WithoutRowID != other.WithoutRowID || t.Strict != other.Strict ||
			!equalCheckConstraints(t.CheckConstraints, other.CheckConstraints),
	}

	for _, targetColumn := range t.Columns {
		sourceColumn, found := other.ColumnByName(targetColumn.Name)
		if !found {
			candidates := lo.Filter(other.Columns, func(column *SQLiteColumn, _ int) bool {
				_, existsInTargetTable := t.ColumnByName(column.Name)
				_, alreadyRenamed := diff.Renamed[column.Name]
				return !existsInTargetTable && !alreadyRenamed && column.HasEqualAttributes(targetColumn)
			})

			// A rename is a guess. Two candidates make the guess unsafe, so the column
			// becomes an addition and the old columns become removals.
			if len(candidates) == 1 {
				diff.Renamed[candidates[0].Name] = targetColumn.Name
				continue
			}

			diff.Added = append(diff.Added, targetColumn.Name)

			if targetColumn.IsGenerated() && targetColumn.GeneratedStored {
				diff.AddsStoredGeneratedColumn = true
			}

			continue
		}

		if *targetColumn == *sourceColumn {
			continue
		}

		if targetColumn.Type != sourceColumn.Type {
			if targetColumn.IsTypeChangeCompatible(sourceColumn) {
				diff.Modified = append(diff.Modified, targetColumn.Name)
				continue
			}

			diff.Removed = append(diff.Removed, sourceColumn.Name)
			diff.Added = append(diff.Added, targetColumn.Name)
			continue
		}

		diff.Modified = append(diff.Modified, targetColumn.Name)
	}

	for _, sourceColumn := range other.Columns {
		_, found := t.ColumnByName(sourceColumn.Name)
		if !found && !lo.Contains(lo.Keys(diff.Renamed), sourceColumn.Name) {
			diff.Removed = append(diff.Removed, sourceColumn.Name)
		}
	}

	if len(t.ForeignKeys) != len(other.ForeignKeys) {
		diff.ForeignKeysChanged = true
	} else {
		for _, targetForeignKey := range t.ForeignKeys {
			found := lo.SomeBy(other.ForeignKeys, func(foreignKey *SQLiteForeignKey) bool {
				return foreignKey.Equal(targetForeignKey)
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

	if columnsDiff.NeedsRecreation() {
		tempTable := t.Copy()
		tempTable.Name = "_" + t.Name + "_temp"

		instructions = append(instructions, tempTable.CreateTableInstruction())

		newToOld := lo.Invert(columnsDiff.Renamed)

		var insertColumns []string
		var selectColumns []string

		for _, newColumn := range t.Columns {
			// SQLite computes a generated column, and it refuses a value for that column.
			if newColumn.IsGenerated() {
				continue
			}

			insertColumns = append(insertColumns, newColumn.Name)

			_, ok := other.ColumnByName(newColumn.Name)
			if ok {
				selectColumns = append(selectColumns, QuoteIdentifier(newColumn.Name))
				continue
			}

			oldName, ok := newToOld[newColumn.Name]
			if ok {
				selectColumns = append(selectColumns, QuoteIdentifier(oldName))
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

		// The DROP TABLE statement removes each index and each trigger of the table. The
		// recreation builds every one of them again from the target, so the index diff and
		// the trigger diff below compare a source that is not there.
		instructions = append(instructions, t.IndexInstructions()...)
		instructions = append(instructions, t.TriggerInstructions()...)

		return instructions, nil
	}

	// The map gives no stable order. The loop walks the target columns instead, so the
	// rename statements follow the shape of the target table on every run.
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

func (t *SQLiteTable) DiffTriggers(other *SQLiteTable) ([]Instruction, error) {
	var instructions []Instruction

	for _, targetTrigger := range t.Triggers {
		sourceTrigger, found := other.TriggerByName(targetTrigger.Name)
		if !found {
			instructions = append(instructions, &SQLiteCreateTriggerInstruction{
				Definition: targetTrigger.SQL,
			})

			continue
		}

		if targetTrigger.SQL != sourceTrigger.SQL {
			instructions = append(instructions,
				&SQLiteDropTriggerInstruction{Name: sourceTrigger.Name},
				&SQLiteCreateTriggerInstruction{Definition: targetTrigger.SQL})
		}
	}

	for _, sourceTrigger := range other.Triggers {
		_, found := t.TriggerByName(sourceTrigger.Name)
		if !found {
			instructions = append(instructions, &SQLiteDropTriggerInstruction{
				Name: sourceTrigger.Name,
			})
		}
	}

	return instructions, nil
}

func (t *SQLiteTable) DiffIndexes(other *SQLiteTable) ([]Instruction, error) {
	var instructions []Instruction

	for _, targetIndex := range t.Indexes {
		sourceIndex, found := other.IndexByName(targetIndex.Name)
		if !found {
			instructions = append(instructions, targetIndex.CreateInstruction())
			continue
		}

		if !targetIndex.Equal(sourceIndex) {
			instructions = append(instructions,
				&SQLDropIndexInstruction{Name: sourceIndex.Name},
				targetIndex.CreateInstruction())
		}
	}

	for _, sourceIndex := range other.Indexes {
		_, found := t.IndexByName(sourceIndex.Name)
		if !found {
			instructions = append(instructions, &SQLDropIndexInstruction{Name: sourceIndex.Name})
		}
	}

	return instructions, nil
}
