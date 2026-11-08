package driversmysql

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

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
	Added       []string
	Modified    []string
	Removed     []string
	RenamedFrom map[string]string

	// A recreated column is in Removed and in Added.
	Recreated []string

	// MySQL refuses a change of the type or of the collation of a column that a foreign key
	// names. A retyped column is also in Modified.
	Retyped []string
}

func (t *MySQLTable) DiffColumns(other *MySQLTable) *MySQLTableColumnsDiff {
	diff := &MySQLTableColumnsDiff{
		RenamedFrom: driversshared.GuessRenamedColumns(t.Columns, other.Columns, columnName, (*MySQLColumn).HasEqualAttributes),
	}

	// A table holds at most one AUTO_INCREMENT column. If the current one goes away and the
	// desired table adds one, the new column replaces the old one, so a rename keeps the values.
	// A drop and an add fail, because MySQL refuses an AUTO_INCREMENT column outside of every key.
	currentAutoIncrementColumn, currentFound := lo.Find(other.Columns, func(column *MySQLColumn) bool {
		return column.AutoIncrement
	})
	desiredAutoIncrementColumn, desiredFound := lo.Find(t.Columns, func(column *MySQLColumn) bool {
		return column.AutoIncrement
	})

	if currentFound && desiredFound {
		_, currentKept := t.ColumnByName(currentAutoIncrementColumn.Name)
		_, desiredExists := other.ColumnByName(desiredAutoIncrementColumn.Name)

		if !currentKept && !desiredExists {
			diff.RenamedFrom[desiredAutoIncrementColumn.Name] = currentAutoIncrementColumn.Name
		}
	}

	for _, desiredColumn := range t.Columns {
		currentColumn, found := other.ColumnByName(desiredColumn.Name)
		if !found {
			oldName, renamed := diff.RenamedFrom[desiredColumn.Name]
			if !renamed {
				diff.Added = append(diff.Added, desiredColumn.Name)

				continue
			}

			currentColumn, _ = other.ColumnByName(oldName)
		}

		if desiredColumn.HasEqualAttributes(currentColumn) {
			continue
		}

		// MySQL refuses a MODIFY COLUMN action that turns a plain column into a generated
		// column, or that changes the storage of a generated column. MySQL also refuses the
		// rename of a column that a generated column reads.
		if generatedKindChanged(desiredColumn, currentColumn) || readsRenamedColumn(currentColumn, diff.RenamedFrom) {
			diff.Removed = append(diff.Removed, currentColumn.Name)
			diff.Added = append(diff.Added, desiredColumn.Name)
			diff.Recreated = append(diff.Recreated, desiredColumn.Name)

			continue
		}

		diff.Modified = append(diff.Modified, desiredColumn.Name)

		if desiredColumn.Type != currentColumn.Type || desiredColumn.Collation != currentColumn.Collation {
			diff.Retyped = append(diff.Retyped, desiredColumn.Name)
		}
	}

	for _, currentColumn := range other.Columns {
		_, found := t.ColumnByName(currentColumn.Name)
		_, renamed := lo.FindKey(diff.RenamedFrom, currentColumn.Name)
		if !found && !renamed {
			diff.Removed = append(diff.Removed, currentColumn.Name)
		}
	}

	return diff
}

// The result tells where the desired column at the position goes: first, or after a
// column. The result also gives the column order after the move.
func placeColumn(columnOrder []string, desiredColumns []*MySQLColumn, position int) (bool, string, []string) {
	name := desiredColumns[position].Name

	columnOrder = slices.DeleteFunc(slices.Clone(columnOrder), func(orderedName string) bool {
		return orderedName == name
	})

	if position == 0 {
		return true, "", slices.Insert(columnOrder, 0, name)
	}

	previousName := desiredColumns[position-1].Name

	return false, previousName, slices.Insert(columnOrder, slices.Index(columnOrder, previousName)+1, name)
}

// The result is the longest list of the names in the column order whose desired positions
// increase.
func longestOrderedRun(columnOrder []string, desiredColumns []*MySQLColumn) []string {
	desiredPositions := lo.Map(columnOrder, func(name string, _ int) int {
		return slices.IndexFunc(desiredColumns, func(column *MySQLColumn) bool {
			return column.Name == name
		})
	})

	runLengths := make([]int, len(columnOrder))
	previousIndexes := make([]int, len(columnOrder))
	lastIndex := -1

	for index := range columnOrder {
		runLengths[index] = 1
		previousIndexes[index] = -1

		for earlierIndex := range index {
			longer := desiredPositions[earlierIndex] < desiredPositions[index] &&
				runLengths[earlierIndex]+1 > runLengths[index]
			if longer {
				runLengths[index] = runLengths[earlierIndex] + 1
				previousIndexes[index] = earlierIndex
			}
		}

		if lastIndex < 0 || runLengths[index] > runLengths[lastIndex] {
			lastIndex = index
		}
	}

	var run []string

	for index := lastIndex; index >= 0; index = previousIndexes[index] {
		run = append(run, columnOrder[index])
	}

	return run
}

func readsRenamedColumn(column *MySQLColumn, renamedFrom map[string]string) bool {
	return column.IsGenerated() && lo.SomeBy(lo.Values(renamedFrom), func(oldName string) bool {
		return strings.Contains(column.GeneratedExpression, QuoteIdentifier(oldName))
	})
}

func (d *MySQLTableColumnsDiff) changesColumn(name string) bool {
	if d == nil {
		return false
	}

	return slices.Contains(d.Retyped, name) || slices.Contains(d.Recreated, name)
}

func (d *MySQLTableColumnsDiff) newNames(names []string) []string {
	if d == nil {
		return names
	}

	newNameByOldName := lo.Invert(d.RenamedFrom)

	return lo.Map(names, func(name string, _ int) string {
		return lo.ValueOr(newNameByOldName, name, name)
	})
}

func (d *MySQLTableColumnsDiff) renameInKeys(keys []string) []string {
	var replacements []string

	for newName, oldName := range d.RenamedFrom {
		replacements = append(replacements, QuoteIdentifier(oldName), QuoteIdentifier(newName))
	}

	replacer := strings.NewReplacer(replacements...)

	// MySQL refuses the rename of a column that an expression key reads, so the diff keeps
	// the old text of an expression key and the index changes.
	return lo.Map(keys, func(key string, _ int) string {
		if strings.HasPrefix(key, "(") {
			return key
		}

		return replacer.Replace(key)
	})
}

func columnName(column *MySQLColumn) string {
	return column.Name
}

func generatedKindChanged(desired *MySQLColumn, current *MySQLColumn) bool {
	return desired.IsGenerated() != current.IsGenerated() ||
		desired.GeneratedStored != current.GeneratedStored
}

// A foreign key ties two tables, so the diff of all tables runs every DropDependents
// instruction before every Alter instruction, and every AddDependents instruction after.
type MySQLTableDiff struct {
	DropDependents []driversshared.Instruction
	Alter          []driversshared.Instruction
	AddDependents  []driversshared.Instruction
}

// MySQL refuses the removal or the rename of a column while a foreign key, a check
// constraint, an expression key, or a generated column reads the column, so those removals
// come before the column actions. The constraint names are unique in the schema, so the
// removals of the foreign keys and of the check constraints come before every creation. The
// additions of the keys come last, so a new key reads the new columns.
func (t *MySQLTable) DiffTable(
	other *MySQLTable,
	columnsDiffs map[string]*MySQLTableColumnsDiff,
) (*MySQLTableDiff, error) {
	columnsDiff := columnsDiffs[t.Name]

	currentColumnNames := lo.Map(other.Columns, func(column *MySQLColumn, _ int) string {
		return column.Name
	})
	columnOrder := columnsDiff.newNames(lo.Without(currentColumnNames, columnsDiff.Removed...))
	primaryKeyChanged := !slices.Equal(t.PrimaryKey, columnsDiff.newNames(other.PrimaryKey))
	primaryKeyAddedWithColumn := false

	var addInstructions []driversshared.Instruction

	var autoIncrementAdditions []driversshared.Instruction

	for _, columnName := range columnsDiff.Added {
		position := slices.IndexFunc(t.Columns, func(column *MySQLColumn) bool {
			return column.Name == columnName
		})
		if position < 0 {
			return nil, fmt.Errorf("internal error: added column %s not found in table %s", columnName, t.Name)
		}

		column := t.Columns[position]
		action := &MySQLAddColumnAction{
			Column:     column,
			PrimaryKey: primaryKeyChanged && column.AutoIncrement && slices.Equal(t.PrimaryKey, []string{columnName}),
		}
		primaryKeyAddedWithColumn = primaryKeyAddedWithColumn || action.PrimaryKey

		// The key of any other new AUTO_INCREMENT column does not exist yet. So the column
		// comes without the keyword, and a MODIFY action after the index additions adds the
		// keyword and numbers the rows. A nullable column lets a unique key accept the rows.
		if column.AutoIncrement && !action.PrimaryKey {
			columnWithoutAutoIncrement := *column
			columnWithoutAutoIncrement.AutoIncrement = false
			columnWithoutAutoIncrement.NotNull = column.NotNull && slices.Contains(t.PrimaryKey, columnName)
			action.Column = &columnWithoutAutoIncrement

			autoIncrementAdditions = append(autoIncrementAdditions, &MySQLAlterTableInstruction{
				Name: t.Name,
				Action: &MySQLModifyColumnAction{
					Column: column,
				},
			})
		}

		action.First, action.After, columnOrder = placeColumn(columnOrder, t.Columns, position)

		// MySQL adds a column at the end by default.
		if columnOrder[len(columnOrder)-1] == columnName {
			action.First = false
			action.After = ""
		}

		addInstructions = append(addInstructions, &MySQLAlterTableInstruction{
			Name:   t.Name,
			Action: action,
		})
	}

	// Each move rebuilds the table, so the columns of the longest run that keeps the desired
	// order stay in place.
	keptColumnNames := longestOrderedRun(columnOrder, t.Columns)
	movedColumnNames := lo.Reject(lo.Map(t.Columns, func(column *MySQLColumn, _ int) string {
		return column.Name
	}), func(name string, _ int) bool {
		return slices.Contains(keptColumnNames, name)
	})

	collationsEqual := (t.CollationIsDefault && other.CollationIsDefault) || t.Collation == other.Collation

	// The conversion of the table rewrites every text column to the new collation, so a
	// column with its own collation needs its definition again.
	var reconvertedColumns []*MySQLColumn

	if !collationsEqual {
		reconvertedColumns = lo.Filter(t.Columns, func(column *MySQLColumn, _ int) bool {
			return column.Collation != "" &&
				!slices.Contains(columnsDiff.Modified, column.Name) &&
				!slices.Contains(columnsDiff.Added, column.Name) &&
				!slices.Contains(movedColumnNames, column.Name)
		})
	}

	// MariaDB keeps a CHECK clause of a column, and the CHECK clause of the json type, with
	// the column. A MODIFY COLUMN action or a DROP COLUMN action removes such a check, and
	// DROP CONSTRAINT refuses it. So the diff does not drop the check, and it adds the check
	// again as a check of the table. A change of the check rebuilds the owner.
	rebuiltColumnNames := slices.Concat(columnsDiff.Modified, movedColumnNames,
		lo.Map(reconvertedColumns, func(column *MySQLColumn, _ int) string {
			return column.Name
		}))

	currentOwnerNameByCheckName := map[string]string{}

	var lateModifications []*MySQLColumn

	for _, check := range other.CheckConstraints {
		if !check.ColumnLevel {
			continue
		}

		desiredCheck, kept := lo.Find(t.CheckConstraints, func(desired *MySQLCheckConstraint) bool {
			return desired.Name == check.Name
		})
		checkChanged := !kept || !desiredCheck.Equal(check)

		currentOwnerName, ownerKnown := other.columnCheckOwnerName(check)
		if !ownerKnown && checkChanged {
			return nil, fmt.Errorf("the check %q of table %q names no column. Its expression reads no column "+
				"or more than one column. Change the check manually", check.Name, t.Name)
		}

		if !ownerKnown {
			continue
		}

		currentOwnerNameByCheckName[check.Name] = currentOwnerName
		ownerName := columnsDiff.newNames([]string{currentOwnerName})[0]
		owner, foundInDesired := t.ColumnByName(ownerName)

		ownerChanged := checkChanged || ownerName != currentOwnerName
		if foundInDesired && ownerChanged && !slices.Contains(columnsDiff.Removed, currentOwnerName) &&
			!slices.Contains(rebuiltColumnNames, ownerName) {
			rebuiltColumnNames = append(rebuiltColumnNames, ownerName)
			lateModifications = append(lateModifications, owner)
		}
	}

	keptCurrentChecks := lo.Reject(other.CheckConstraints, func(check *MySQLCheckConstraint, _ int) bool {
		currentOwnerName := currentOwnerNameByCheckName[check.Name]

		return check.ColumnLevel && (slices.Contains(columnsDiff.Removed, currentOwnerName) ||
			slices.Contains(rebuiltColumnNames, columnsDiff.newNames([]string{currentOwnerName})[0]))
	})

	foreignKeyAdditions, foreignKeyRemovals := t.foreignKeyChanges(other, columnsDiffs)
	checkAdditions, checkRemovals := t.checkConstraintChanges(keptCurrentChecks, columnsDiff.Recreated)
	indexAdditions, indexRemovals := t.indexChanges(other, columnsDiff)

	triggerAdditions, triggerRemovals, err := driversshared.DiffByKey(t.Triggers, other.Triggers, diffTriggerRules())
	if err != nil {
		return nil, err
	}

	var instructions []driversshared.Instruction

	instructions = append(instructions, triggerRemovals...)

	var autoIncrementLaterColumnNames []string

	// MySQL refuses the removal of the keys of an AUTO_INCREMENT column, so a column loses
	// the keyword before the removals of the indexes and of the primary key. The other
	// changes come after the addition of the primary key.
	for _, currentColumn := range other.Columns {
		if !currentColumn.AutoIncrement || !slices.Contains(columnsDiff.Removed, currentColumn.Name) {
			continue
		}

		columnWithoutAutoIncrement := *currentColumn
		columnWithoutAutoIncrement.AutoIncrement = false

		instructions = append(instructions, &MySQLAlterTableInstruction{
			Name: t.Name,
			Action: &MySQLModifyColumnAction{
				Column: &columnWithoutAutoIncrement,
			},
		})
	}

	for _, columnName := range columnsDiff.Modified {
		column, found := t.ColumnByName(columnName)
		if !found {
			return nil, fmt.Errorf("internal error: modified column %s not found in table %s", columnName, t.Name)
		}

		currentColumn, foundInCurrent := other.ColumnByName(columnName)
		if !column.AutoIncrement && foundInCurrent && currentColumn.AutoIncrement {
			instructions = append(instructions, &MySQLAlterTableInstruction{
				Name: t.Name,
				Action: &MySQLModifyColumnAction{
					Column: column,
				},
			})

			continue
		}

		gainsAutoIncrement := column.AutoIncrement && foundInCurrent && !currentColumn.AutoIncrement
		if gainsAutoIncrement && !t.keepsKeyOf(columnName, indexAdditions) {
			autoIncrementAdditions = append(autoIncrementAdditions, &MySQLAlterTableInstruction{
				Name: t.Name,
				Action: &MySQLModifyColumnAction{
					Column: column,
				},
			})
			autoIncrementLaterColumnNames = append(autoIncrementLaterColumnNames, columnName)

			continue
		}

		lateModifications = append(lateModifications, column)
	}

	instructions = append(instructions, indexRemovals...)

	// The drop of the last column of the primary key drops the key, so the drop of the key
	// comes before the drops of the columns.
	if primaryKeyChanged && len(other.PrimaryKey) > 0 {
		instructions = append(instructions, &MySQLAlterTableInstruction{
			Name:   t.Name,
			Action: &MySQLDropPrimaryKeyAction{},
		})
	}

	var dropInstructions []driversshared.Instruction

	for _, columnName := range columnsDiff.Removed {
		dropInstructions = append(dropInstructions, &MySQLAlterTableInstruction{
			Name: t.Name,
			Action: &MySQLDropColumnAction{
				ColumnName: columnName,
			},
		})
	}

	// MySQL refuses the drop of the last column, so the drops come after the additions when
	// the diff drops every current column.
	dropsAfterAdditions := len(columnsDiff.Removed) == len(other.Columns)
	if !dropsAfterAdditions {
		instructions = append(instructions, dropInstructions...)
	}

	// The map gives no stable order. The loop walks the desired columns instead, so the
	// rename statements follow the shape of the desired table on every run.
	for _, column := range t.Columns {
		oldName, renamed := columnsDiff.RenamedFrom[column.Name]
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
	if !collationsEqual {
		instructions = append(instructions, &MySQLAlterTableInstruction{
			Name: t.Name,
			Action: &MySQLConvertToCharacterSetAction{
				CharacterSet: characterSetOfCollation(t.Collation),
				Collation:    t.Collation,
			},
		})

		for _, column := range reconvertedColumns {
			instructions = append(instructions, &MySQLAlterTableInstruction{
				Name: t.Name,
				Action: &MySQLModifyColumnAction{
					Column: column,
				},
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

	instructions = append(instructions, addInstructions...)

	if dropsAfterAdditions {
		instructions = append(instructions, dropInstructions...)
	}

	if primaryKeyChanged && len(t.PrimaryKey) > 0 && !primaryKeyAddedWithColumn {
		instructions = append(instructions, &MySQLAlterTableInstruction{
			Name: t.Name,
			Action: &MySQLAddPrimaryKeyAction{
				Columns: t.PrimaryKey,
			},
		})
	}

	// A modified column moves in the same MODIFY action. The moves run in the desired order,
	// so each column lands after its final predecessor.
	for position, column := range t.Columns {
		moved := slices.Contains(movedColumnNames, column.Name)
		if !moved && !slices.Contains(lateModifications, column) {
			continue
		}

		action := &MySQLModifyColumnAction{
			Column: column,
		}

		// The key of the column does not exist yet, so the move keeps the column without the
		// keyword. A MODIFY action after the index additions adds the keyword.
		if slices.Contains(autoIncrementLaterColumnNames, column.Name) {
			columnWithoutAutoIncrement := *column
			columnWithoutAutoIncrement.AutoIncrement = false
			action.Column = &columnWithoutAutoIncrement
		}

		if moved {
			action.First, action.After, columnOrder = placeColumn(columnOrder, t.Columns, position)
		}

		instructions = append(instructions, &MySQLAlterTableInstruction{
			Name:   t.Name,
			Action: action,
		})
	}

	instructions = append(instructions, indexAdditions...)
	instructions = append(instructions, autoIncrementAdditions...)
	instructions = append(instructions, checkAdditions...)
	instructions = append(instructions, triggerAdditions...)

	return &MySQLTableDiff{
		DropDependents: append(foreignKeyRemovals, checkRemovals...),
		Alter:          instructions,
		AddDependents:  foreignKeyAdditions,
	}, nil
}

// The drop of a column drops its index, and MySQL refuses the drop of a column that a check
// constraint names. So the diff drops every object that uses a recreated column before the
// drop of the column, and adds the object again after the column.
func equalAndKept[T interface{ UsesColumn(columnName string) bool }](
	equal func(T, T) bool,
	recreatedColumns []string,
) func(T, T) bool {
	return func(desired T, current T) bool {
		return equal(desired, current) && !lo.SomeBy(recreatedColumns, current.UsesColumn)
	}
}

// The primary key and the kept indexes exist before the late column changes. An added
// index exists only after them.
func (t *MySQLTable) keepsKeyOf(columnName string, indexAdditions []driversshared.Instruction) bool {
	if len(t.PrimaryKey) > 0 && t.PrimaryKey[0] == columnName {
		return true
	}

	return lo.SomeBy(t.Indexes, func(index *MySQLIndex) bool {
		added := lo.SomeBy(indexAdditions, func(instruction driversshared.Instruction) bool {
			createInstruction, isCreate := instruction.(*MySQLCreateIndexInstruction)
			return isCreate && createInstruction.Name == index.Name
		})

		return index.Keys[0] == QuoteIdentifier(columnName) && !added
	})
}

// MariaDB names a column check after its column. A rename of the column keeps the old name
// on the check, so the owner is then the only column that the expression reads.
func (t *MySQLTable) columnCheckOwnerName(check *MySQLCheckConstraint) (string, bool) {
	_, found := t.ColumnByName(check.Name)
	if found {
		return check.Name, true
	}

	readColumns := lo.Filter(t.Columns, func(column *MySQLColumn, _ int) bool {
		return check.UsesColumn(column.Name)
	})
	if len(readColumns) != 1 {
		return "", false
	}

	return readColumns[0].Name, true
}

// MySQL renames the columns of a foreign key with the columns. MySQL refuses a change of a
// column type while a foreign key names the column, on either side of the key.
func (t *MySQLTable) foreignKeyChanges(
	other *MySQLTable,
	columnsDiffs map[string]*MySQLTableColumnsDiff,
) ([]driversshared.Instruction, []driversshared.Instruction) {
	columnsDiff := columnsDiffs[t.Name]

	return driversshared.ReplaceByKey(t.ForeignKeys, other.ForeignKeys,
		func(foreignKey *MySQLForeignKey) string {
			return foreignKey.Name
		},
		func(desired *MySQLForeignKey, current *MySQLForeignKey) bool {
			referencedColumnsDiff := columnsDiffs[desired.ReferencedTable]

			renamed := *current
			renamed.Columns = columnsDiff.newNames(current.Columns)
			renamed.ReferencedColumns = referencedColumnsDiff.newNames(current.ReferencedColumns)

			return desired.Equal(&renamed) &&
				!slices.ContainsFunc(desired.Columns, columnsDiff.changesColumn) &&
				!slices.ContainsFunc(desired.ReferencedColumns, referencedColumnsDiff.changesColumn)
		},
		func(foreignKey *MySQLForeignKey) driversshared.Instruction {
			return &MySQLAlterTableInstruction{
				Name:   t.Name,
				Action: &MySQLAddForeignKeyAction{ForeignKey: foreignKey},
			}
		},
		func(foreignKey *MySQLForeignKey) driversshared.Instruction {
			return &MySQLAlterTableInstruction{
				Name:   t.Name,
				Action: &MySQLDropForeignKeyAction{Name: foreignKey.Name},
			}
		})
}

func (t *MySQLTable) checkConstraintChanges(
	currentChecks []*MySQLCheckConstraint,
	recreatedColumns []string,
) ([]driversshared.Instruction, []driversshared.Instruction) {
	return driversshared.ReplaceByKey(t.CheckConstraints, currentChecks,
		func(check *MySQLCheckConstraint) string {
			return check.Name
		},
		equalAndKept((*MySQLCheckConstraint).Equal, recreatedColumns),
		func(check *MySQLCheckConstraint) driversshared.Instruction {
			return &MySQLAlterTableInstruction{
				Name:   t.Name,
				Action: &MySQLAddCheckConstraintAction{CheckConstraint: check},
			}
		},
		func(check *MySQLCheckConstraint) driversshared.Instruction {
			return &MySQLAlterTableInstruction{
				Name:   t.Name,
				Action: &MySQLDropCheckConstraintAction{Name: check.Name},
			}
		})
}

// MySQL renames the keys of an index with the columns.
func (t *MySQLTable) indexChanges(
	other *MySQLTable,
	columnsDiff *MySQLTableColumnsDiff,
) ([]driversshared.Instruction, []driversshared.Instruction) {
	// The ADD of a foreign key builds the index of the key by itself, like the CREATE
	// TABLE path of IndexInstructions. The current schema does not hold such an index, so the
	// removal from the desired list changes no drop.
	desiredIndexes := lo.Reject(t.Indexes, func(index *MySQLIndex, _ int) bool {
		_, ownedByForeignKey := t.ForeignKeyByName(index.Name)
		_, foundInCurrent := lo.Find(other.Indexes, func(currentIndex *MySQLIndex) bool {
			return currentIndex.Name == index.Name
		})

		return ownedByForeignKey && !foundInCurrent
	})

	return driversshared.ReplaceByKey(desiredIndexes, other.Indexes,
		func(index *MySQLIndex) string {
			return index.Name
		},
		equalAndKept(func(desired *MySQLIndex, current *MySQLIndex) bool {
			renamed := *current
			renamed.Keys = columnsDiff.renameInKeys(current.Keys)

			return desired.Equal(&renamed)
		}, columnsDiff.Recreated),
		func(index *MySQLIndex) driversshared.Instruction {
			return index.CreateInstruction()
		},
		func(index *MySQLIndex) driversshared.Instruction {
			return index.DropInstruction()
		})
}

// Every catalog query needs a stable ORDER BY, because MySQL gives no stable row order.
func (d *MySQLDriver) GetTables(ctx context.Context, db *sql.DB) ([]*MySQLTable, error) {
	type tableRow struct {
		Name          string
		Engine        sql.NullString
		Collation     sql.NullString
		CreateOptions sql.NullString
	}

	tableRows, err := driversshared.QueryAll(ctx, db, `
		SELECT TABLE_NAME, ENGINE, TABLE_COLLATION, CREATE_OPTIONS
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME <> ?
		ORDER BY TABLE_NAME;
	`, func(rows *sql.Rows) (tableRow, error) {
		var row tableRow

		err := rows.Scan(&row.Name, &row.Engine, &row.Collation, &row.CreateOptions)

		return row, err
	}, driversshared.MigrationHistoryTableName)
	if err != nil {
		return nil, err
	}

	var tables []*MySQLTable

	for _, row := range tableRows {
		if slices.Contains(d.IgnoreTables, row.Name) {
			continue
		}

		partitioned := strings.Contains(strings.ToLower(row.CreateOptions.String), "partitioned")

		table, err := d.GetTable(ctx, db, row.Name, row.Engine, row.Collation, partitioned)
		if err != nil {
			return nil, err
		}

		tables = append(tables, table)
	}

	return tables, nil
}

func (d *MySQLDriver) GetTable(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	engine sql.NullString,
	tableCollation sql.NullString,
	partitioned bool,
) (*MySQLTable, error) {
	columns, err := d.GetTableColumns(ctx, db, tableName, tableCollation)
	if err != nil {
		return nil, err
	}

	foreignKeys, err := d.GetTableForeignKeys(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	primaryKey, indexes, err := d.GetTableIndexes(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	checkConstraints, err := d.GetTableCheckConstraints(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	triggers, err := d.GetTableTriggers(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	partitionClause := ""

	if partitioned {
		partitionClause, err = d.GetTablePartitionClause(ctx, db, tableName)
		if err != nil {
			return nil, err
		}
	}

	details := d.detailsByConnection[db]

	return &MySQLTable{
		Name:               tableName,
		Columns:            columns,
		PrimaryKey:         primaryKey,
		Indexes:            indexes,
		ForeignKeys:        foreignKeys,
		CheckConstraints:   checkConstraints,
		Triggers:           triggers,
		Engine:             engine.String,
		EngineIsDefault:    strings.EqualFold(engine.String, details.defaultEngine),
		Collation:          tableCollation.String,
		CollationIsDefault: tableCollation.String == details.defaultCollation,
		PartitionClause:    partitionClause,
	}, nil
}

// The PARTITIONS catalog table gives the parts of the clause, and the rebuild of the text
// from those parts is fragile. The text of SHOW CREATE TABLE holds the whole clause.
func (d *MySQLDriver) GetTablePartitionClause(ctx context.Context, db *sql.DB, tableName string) (string, error) {
	row := db.QueryRowContext(ctx, "SHOW CREATE TABLE "+QuoteIdentifier(tableName)+";")

	var name, definition string

	err := row.Scan(&name, &definition)
	if err != nil {
		return "", err
	}

	return parsePartitionClause(definition), nil
}

// The clause ends the text of SHOW CREATE TABLE, so the last occurrence skips a comment
// or a default value that holds the same words. An old server wraps the clause in the
// conditional comment /*!50100 ... */.
func parsePartitionClause(definition string) string {
	position := strings.LastIndex(definition, "PARTITION BY")
	if position < 0 {
		return ""
	}

	clause := strings.TrimSpace(definition[position:])

	return strings.TrimSpace(strings.TrimSuffix(clause, "*/"))
}
