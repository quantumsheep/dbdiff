package driversmysql

import (
	"fmt"
	"slices"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

type MySQLTable struct {
	Name             string
	Columns          []*MySQLColumn
	PrimaryKey       []string
	Indexes          []*MySQLIndex
	ForeignKeys      []*MySQLForeignKey
	CheckConstraints []*MySQLCheckConstraint
	Triggers         []*MySQLTrigger

	// The two default flags let two databases with two different defaults compare as
	// equal, so a scratch database gives no phantom change.
	Engine             string
	EngineIsDefault    bool
	Collation          string
	CollationIsDefault bool

	PartitionClause string
}

func (t *MySQLTable) ColumnByName(name string) (*MySQLColumn, bool) {
	return lo.Find(t.Columns, func(column *MySQLColumn) bool {
		return column.Name == name
	})
}

func (t *MySQLTable) ForeignKeyByName(name string) (*MySQLForeignKey, bool) {
	return lo.Find(t.ForeignKeys, func(foreignKey *MySQLForeignKey) bool {
		return foreignKey.Name == name
	})
}

func (t *MySQLTable) IndexByName(name string) (*MySQLIndex, bool) {
	return lo.Find(t.Indexes, func(index *MySQLIndex) bool {
		return index.Name == name
	})
}

func (t *MySQLTable) CheckConstraintByName(name string) (*MySQLCheckConstraint, bool) {
	return lo.Find(t.CheckConstraints, func(check *MySQLCheckConstraint) bool {
		return check.Name == name
	})
}

func (t *MySQLTable) CreateTableInstruction() *MySQLCreateTableInstruction {
	instruction := &MySQLCreateTableInstruction{
		Name:             t.Name,
		Columns:          t.Columns,
		PrimaryKey:       t.PrimaryKey,
		CheckConstraints: t.CheckConstraints,
		ForeignKeys:      t.ForeignKeys,
	}

	if !t.EngineIsDefault {
		instruction.Engine = t.Engine
	}

	if !t.CollationIsDefault {
		instruction.Collation = t.Collation
	}

	instruction.Partition = t.PartitionClause

	return instruction
}

// The CREATE TABLE statement builds the index of a foreign key again through the
// constraint, so a CREATE INDEX statement with that name reads a duplicate.
func (t *MySQLTable) IndexInstructions() []driversshared.Instruction {
	var instructions []driversshared.Instruction

	for _, index := range t.Indexes {
		_, ownedByForeignKey := t.ForeignKeyByName(index.Name)
		if ownedByForeignKey {
			continue
		}

		instructions = append(instructions, index.CreateInstruction())
	}

	return instructions
}

func (t *MySQLTable) TriggerInstructions() []driversshared.Instruction {
	return lo.Map(t.Triggers, func(trigger *MySQLTrigger, _ int) driversshared.Instruction {
		return trigger.CreateInstruction()
	})
}

func (t *MySQLTable) Instructions() []driversshared.Instruction {
	instructions := []driversshared.Instruction{t.CreateTableInstruction()}
	instructions = append(instructions, t.IndexInstructions()...)

	return append(instructions, t.TriggerInstructions()...)
}

type MySQLTableColumnsDiff struct {
	Added    []string
	Modified []string
	Removed  []string
	Renamed  map[string]string
}

func (t *MySQLTable) DiffColumns(other *MySQLTable) *MySQLTableColumnsDiff {
	diff := &MySQLTableColumnsDiff{
		Renamed: make(map[string]string),
	}

	for _, targetColumn := range t.Columns {
		sourceColumn, found := other.ColumnByName(targetColumn.Name)
		if !found {
			candidates := lo.Filter(other.Columns, func(column *MySQLColumn, _ int) bool {
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

			continue
		}

		if *targetColumn == *sourceColumn {
			continue
		}

		// MySQL refuses a MODIFY COLUMN action that turns a plain column into a generated
		// column, or that changes the storage of a generated column.
		if generatedKindChanged(targetColumn, sourceColumn) {
			diff.Removed = append(diff.Removed, sourceColumn.Name)
			diff.Added = append(diff.Added, targetColumn.Name)

			continue
		}

		diff.Modified = append(diff.Modified, targetColumn.Name)
	}

	for _, sourceColumn := range other.Columns {
		_, found := t.ColumnByName(sourceColumn.Name)
		_, renamed := diff.Renamed[sourceColumn.Name]
		if !found && !renamed {
			diff.Removed = append(diff.Removed, sourceColumn.Name)
		}
	}

	return diff
}

func generatedKindChanged(target *MySQLColumn, source *MySQLColumn) bool {
	return target.IsGenerated() != source.IsGenerated() ||
		target.GeneratedStored != source.GeneratedStored
}

// MySQL refuses the removal of a column while a foreign key, a check constraint, or an
// explicit index names the column, so those removals come before the column actions. The
// additions of the keys come last, so a new key reads the new columns.
func (t *MySQLTable) DiffTable(other *MySQLTable) ([]driversshared.Instruction, error) {
	columnsDiff := t.DiffColumns(other)

	var instructions []driversshared.Instruction

	// The map gives no stable order. The loop walks the target columns instead, so the
	// rename statements follow the shape of the target table on every run.
	newNameToOldName := lo.Invert(columnsDiff.Renamed)

	for _, column := range t.Columns {
		oldName, renamed := newNameToOldName[column.Name]
		if !renamed {
			continue
		}

		instructions = append(instructions, &MySQLAlterTableInstruction{
			Name: t.Name,
			Action: &MySQLRenameColumnAction{
				ColumnName:    oldName,
				NewColumnName: column.Name,
			},
		})
	}

	foreignKeyAdditions, foreignKeyRemovals := t.foreignKeyChanges(other)
	checkAdditions, checkRemovals := t.checkConstraintChanges(other)
	indexAdditions, indexRemovals := t.indexChanges(other)

	triggerAdditions, triggerRemovals, err := driversshared.DiffByKey(t.Triggers, other.Triggers, diffTriggerRules())
	if err != nil {
		return nil, err
	}

	instructions = append(instructions, triggerRemovals...)
	instructions = append(instructions, foreignKeyRemovals...)
	instructions = append(instructions, checkRemovals...)
	instructions = append(instructions, indexRemovals...)

	enginesEqual := (t.EngineIsDefault && other.EngineIsDefault) || t.Engine == other.Engine
	if !enginesEqual {
		instructions = append(instructions, &MySQLAlterTableInstruction{
			Name: t.Name,
			Action: &MySQLEngineAction{
				Engine: t.Engine,
			},
		})
	}

	// The conversion runs before the column changes, so a change of a column collation
	// lands on the new default of the table.
	collationsEqual := (t.CollationIsDefault && other.CollationIsDefault) || t.Collation == other.Collation
	if !collationsEqual {
		instructions = append(instructions, &MySQLAlterTableInstruction{
			Name: t.Name,
			Action: &MySQLConvertToCharacterSetAction{
				CharacterSet: characterSetOfCollation(t.Collation),
				Collation:    t.Collation,
			},
		})

		// The conversion rewrites every text column to the new collation, so a column
		// with its own collation needs its definition again.
		for _, column := range t.Columns {
			if column.Collation == "" {
				continue
			}

			if slices.Contains(columnsDiff.Modified, column.Name) ||
				slices.Contains(columnsDiff.Added, column.Name) {
				continue
			}

			instructions = append(instructions, &MySQLAlterTableInstruction{
				Name:   t.Name,
				Action: &MySQLModifyColumnAction{Column: column},
			})
		}
	}

	if t.PartitionClause != other.PartitionClause {
		var action driversshared.AlterTableAction = &MySQLRemovePartitioningAction{}

		if t.PartitionClause != "" {
			action = &MySQLPartitionAction{
				Clause: t.PartitionClause,
			}
		}

		instructions = append(instructions, &MySQLAlterTableInstruction{
			Name:   t.Name,
			Action: action,
		})
	}

	for _, columnName := range columnsDiff.Removed {
		instructions = append(instructions, &MySQLAlterTableInstruction{
			Name:   t.Name,
			Action: &MySQLDropColumnAction{ColumnName: columnName},
		})
	}

	for _, columnName := range columnsDiff.Added {
		column, found := t.ColumnByName(columnName)
		if !found {
			return nil, fmt.Errorf("internal error: added column %s not found in table %s", columnName, t.Name)
		}

		instructions = append(instructions, &MySQLAlterTableInstruction{
			Name:   t.Name,
			Action: &MySQLAddColumnAction{Column: column},
		})
	}

	// MySQL refuses AUTO_INCREMENT on a column outside of every key, so the change that
	// removes the keyword comes before the removal of the primary key, and the other
	// changes come after the addition of the primary key.
	var lateModifications []*MySQLColumn

	for _, columnName := range columnsDiff.Modified {
		column, found := t.ColumnByName(columnName)
		if !found {
			return nil, fmt.Errorf("internal error: modified column %s not found in table %s", columnName, t.Name)
		}

		sourceColumn, foundInSource := other.ColumnByName(columnName)
		if !column.AutoIncrement && foundInSource && sourceColumn.AutoIncrement {
			instructions = append(instructions, &MySQLAlterTableInstruction{
				Name:   t.Name,
				Action: &MySQLModifyColumnAction{Column: column},
			})

			continue
		}

		lateModifications = append(lateModifications, column)
	}

	if !slices.Equal(t.PrimaryKey, other.PrimaryKey) {
		if len(other.PrimaryKey) > 0 {
			instructions = append(instructions, &MySQLAlterTableInstruction{
				Name:   t.Name,
				Action: &MySQLDropPrimaryKeyAction{},
			})
		}

		if len(t.PrimaryKey) > 0 {
			instructions = append(instructions, &MySQLAlterTableInstruction{
				Name: t.Name,
				Action: &MySQLAddPrimaryKeyAction{
					Columns: t.PrimaryKey,
				},
			})
		}
	}

	for _, column := range lateModifications {
		instructions = append(instructions, &MySQLAlterTableInstruction{
			Name:   t.Name,
			Action: &MySQLModifyColumnAction{Column: column},
		})
	}

	instructions = append(instructions, indexAdditions...)
	instructions = append(instructions, checkAdditions...)
	instructions = append(instructions, foreignKeyAdditions...)
	instructions = append(instructions, triggerAdditions...)

	return instructions, nil
}

func (t *MySQLTable) foreignKeyChanges(other *MySQLTable) ([]driversshared.Instruction, []driversshared.Instruction) {
	var additions []driversshared.Instruction
	var removals []driversshared.Instruction

	for _, targetForeignKey := range t.ForeignKeys {
		sourceForeignKey, found := other.ForeignKeyByName(targetForeignKey.Name)
		if found && targetForeignKey.Equal(sourceForeignKey) {
			continue
		}

		if found {
			removals = append(removals, &MySQLAlterTableInstruction{
				Name:   t.Name,
				Action: &MySQLDropForeignKeyAction{Name: sourceForeignKey.Name},
			})
		}

		additions = append(additions, &MySQLAlterTableInstruction{
			Name:   t.Name,
			Action: &MySQLAddForeignKeyAction{ForeignKey: targetForeignKey},
		})
	}

	for _, sourceForeignKey := range other.ForeignKeys {
		_, found := t.ForeignKeyByName(sourceForeignKey.Name)
		if !found {
			removals = append(removals, &MySQLAlterTableInstruction{
				Name:   t.Name,
				Action: &MySQLDropForeignKeyAction{Name: sourceForeignKey.Name},
			})
		}
	}

	return additions, removals
}

func (t *MySQLTable) checkConstraintChanges(other *MySQLTable) ([]driversshared.Instruction, []driversshared.Instruction) {
	var additions []driversshared.Instruction
	var removals []driversshared.Instruction

	for _, targetCheck := range t.CheckConstraints {
		sourceCheck, found := other.CheckConstraintByName(targetCheck.Name)
		if found && targetCheck.Equal(sourceCheck) {
			continue
		}

		if found {
			removals = append(removals, &MySQLAlterTableInstruction{
				Name:   t.Name,
				Action: &MySQLDropCheckConstraintAction{Name: sourceCheck.Name},
			})
		}

		additions = append(additions, &MySQLAlterTableInstruction{
			Name:   t.Name,
			Action: &MySQLAddCheckConstraintAction{CheckConstraint: targetCheck},
		})
	}

	for _, sourceCheck := range other.CheckConstraints {
		_, found := t.CheckConstraintByName(sourceCheck.Name)
		if !found {
			removals = append(removals, &MySQLAlterTableInstruction{
				Name:   t.Name,
				Action: &MySQLDropCheckConstraintAction{Name: sourceCheck.Name},
			})
		}
	}

	return additions, removals
}

func (t *MySQLTable) indexChanges(other *MySQLTable) ([]driversshared.Instruction, []driversshared.Instruction) {
	var additions []driversshared.Instruction
	var removals []driversshared.Instruction

	for _, targetIndex := range t.Indexes {
		sourceIndex, found := other.IndexByName(targetIndex.Name)
		if found && targetIndex.Equal(sourceIndex) {
			continue
		}

		if found {
			removals = append(removals, sourceIndex.DropInstruction())
		}

		// The ADD of a foreign key builds the index of the key by itself, like the CREATE
		// TABLE path of IndexInstructions.
		_, ownedByForeignKey := t.ForeignKeyByName(targetIndex.Name)
		if !found && ownedByForeignKey {
			continue
		}

		additions = append(additions, targetIndex.CreateInstruction())
	}

	for _, sourceIndex := range other.Indexes {
		_, found := t.IndexByName(sourceIndex.Name)
		if !found {
			removals = append(removals, sourceIndex.DropInstruction())
		}
	}

	return additions, removals
}
