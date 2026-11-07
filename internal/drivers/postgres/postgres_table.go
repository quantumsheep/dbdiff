package driverspostgres

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

type PostgresTable struct {
	Name        string
	Columns     []*PostgresColumn
	Indexes     []*PostgresIndex
	Constraints []*PostgresConstraint
	Triggers    []*PostgresTrigger
	Rules       []*PostgresRule

	PartitionKey    string
	PartitionParent string
	PartitionBound  string

	// A table of INHERITS is no partition, so it keeps its own columns and its own statement.
	Inherits []string

	// A child of INHERITS keeps its own default, NOT NULL flag, storage mode, and statistics
	// target of each column. A change of the parent without ONLY changes them in each child too.
	HasInheritanceChildren bool

	References []string

	Comment string

	Unlogged bool

	StorageParameters []string

	ReplicaIdentity string

	ReplicaIdentityIndex string

	RowLevelSecurity      bool
	ForceRowLevelSecurity bool
	Policies              []*PostgresPolicy
}

// PostgreSQL accepts no row level security option in a CREATE TABLE statement.
func (t *PostgresTable) RowLevelSecurityInstructions() []driversshared.Instruction {
	var instructions []driversshared.Instruction

	if t.RowLevelSecurity {
		instructions = append(instructions, &PostgresAlterTableInstruction{
			Name:    t.Name,
			Actions: []driversshared.AlterTableAction{&PostgresRowLevelSecurityAction{Mode: "ENABLE"}},
		})
	}

	if t.ForceRowLevelSecurity {
		instructions = append(instructions, &PostgresAlterTableInstruction{
			Name:    t.Name,
			Actions: []driversshared.AlterTableAction{&PostgresRowLevelSecurityAction{Mode: "FORCE"}},
		})
	}

	return instructions
}

// PostgreSQL accepts no replica identity in a CREATE TABLE statement. The mode DEFAULT
// needs no statement, because every new table holds that mode.
func (t *PostgresTable) ReplicaIdentityInstructions() []driversshared.Instruction {
	if t.ReplicaIdentity == "" || t.ReplicaIdentity == "DEFAULT" {
		return nil
	}

	return []driversshared.Instruction{&PostgresAlterTableInstruction{
		Name: t.Name,
		Actions: []driversshared.AlterTableAction{&PostgresReplicaIdentityAction{
			Mode:      t.ReplicaIdentity,
			IndexName: t.ReplicaIdentityIndex,
		}},
	}}
}

// PostgreSQL accepts a comment in no CREATE statement.
func (t *PostgresTable) CommentInstructions() []driversshared.Instruction {
	var instructions []driversshared.Instruction

	if t.Comment != "" {
		instructions = append(instructions, &PostgresCommentOnTableInstruction{
			Name: t.Name,
			Text: t.Comment,
		})
	}

	for _, column := range t.Columns {
		if column.Comment == "" {
			continue
		}

		instructions = append(instructions, &PostgresCommentOnColumnInstruction{
			TableName:  t.Name,
			ColumnName: column.Name,
			Text:       column.Comment,
		})
	}

	return instructions
}

func (t *PostgresTable) IsPartition() bool {
	return t.PartitionParent != ""
}

func (t *PostgresTable) ColumnByName(name string) (*PostgresColumn, bool) {
	for _, column := range t.Columns {
		if column.Name == name {
			return column, true
		}
	}

	return nil, false
}

func identityOptionDefaults(currentOptions string, columnType string) string {
	maxValue := "9223372036854775807"

	switch columnType {
	case "smallint":
		maxValue = "32767"
	case "integer":
		maxValue = "2147483647"
	}

	var defaults []string

	for _, option := range splitSequenceOptions(currentOptions) {
		switch strings.ToUpper(strings.Fields(option)[0]) {
		case "START":
			defaults = append(defaults, "START WITH 1")
		case "INCREMENT":
			defaults = append(defaults, "INCREMENT BY 1")
		case "MINVALUE":
			defaults = append(defaults, "MINVALUE 1")
		case "MAXVALUE":
			defaults = append(defaults, "MAXVALUE "+maxValue)
		case "CACHE":
			defaults = append(defaults, "CACHE 1")
		case "CYCLE":
			defaults = append(defaults, "NO CYCLE")
		}
	}

	return strings.Join(defaults, " ")
}

func (t *PostgresTable) DiffTable(
	other *PostgresTable,
	hasAutomaticCast AutomaticCastLookup,
	serverVersion int,
) ([]driversshared.Instruction, error) {
	var instructions []driversshared.Instruction

	if t.IsPartition() != other.IsPartition() {
		if t.IsPartition() {
			return nil, fmt.Errorf("the table %q changes from a table to a partition, and dbdiff cannot diff this change", t.Name)
		}

		return nil, fmt.Errorf("the table %q changes from a partition to a table, and dbdiff cannot diff this change", t.Name)
	}

	// A partition takes the columns, the constraints, and the inherited indexes of its parent.
	// A partition keeps its own default, NOT NULL flag, storage mode, and statistics target of
	// each column. The diff does not compare the own constraints of a partition.
	// DiffTables prints the removals of the own indexes of a partition.
	// A new bound or a new parent needs a detach and an attach, because PostgreSQL
	// holds no action that changes a bound in place. ATTACH PARTITION refuses a table without
	// the NOT NULL flags of the parent, so the column changes come between the two statements.
	if t.IsPartition() {
		reattached := t.PartitionParent != other.PartitionParent || t.PartitionBound != other.PartitionBound
		if reattached {
			instructions = append(instructions, &PostgresDetachPartitionInstruction{
				ParentName:    other.PartitionParent,
				PartitionName: t.Name,
			})
		}

		instructions = append(instructions, diffPartitionColumns(t, other)...)

		if reattached {
			instructions = append(instructions, &PostgresAttachPartitionInstruction{
				ParentName:    t.PartitionParent,
				PartitionName: t.Name,
				Bound:         t.PartitionBound,
			})
		}

		instructions = append(instructions, diffRowLevelSecurity(t, other)...)
		instructions = append(instructions, diffComments(t, other)...)
		instructions = append(instructions, diffIndexAdditions(t, other)...)
		instructions = append(instructions, diffTriggers(t, other)...)

		return instructions, nil
	}

	alterTable := func(action driversshared.AlterTableAction) driversshared.Instruction {
		return &PostgresAlterTableInstruction{
			Name:    t.Name,
			Actions: []driversshared.AlterTableAction{action},
		}
	}

	alterColumn := func(action driversshared.AlterTableAction) driversshared.Instruction {
		return &PostgresAlterTableInstruction{
			Name:    t.Name,
			Only:    other.HasInheritanceChildren,
			Actions: []driversshared.AlterTableAction{action},
		}
	}

	var dropNotNullInstructions []driversshared.Instruction

	for _, parentName := range t.Inherits {
		if !slices.Contains(other.Inherits, parentName) {
			instructions = append(instructions,
				alterTable(&PostgresInheritAction{ParentName: parentName}))
		}
	}

	for _, parentName := range other.Inherits {
		if !slices.Contains(t.Inherits, parentName) {
			instructions = append(instructions,
				alterTable(&PostgresNoInheritAction{ParentName: parentName}))
		}
	}

	for _, desiredColumn := range t.Columns {
		currentColumn, found := other.ColumnByName(desiredColumn.Name)

		// The parent gives the definition of an inherited column, so the child diff compares
		// only the attributes that the child keeps.
		if desiredColumn.Inherited {
			if !found {
				continue
			}

			columnCopy := *desiredColumn
			columnCopy.Type = currentColumn.Type
			columnCopy.Collation = currentColumn.Collation
			columnCopy.Identity = currentColumn.Identity
			columnCopy.IdentityOptions = currentColumn.IdentityOptions
			columnCopy.GeneratedExpression = currentColumn.GeneratedExpression
			columnCopy.Serial = currentColumn.Serial
			columnCopy.SerialSequenceName = currentColumn.SerialSequenceName
			columnCopy.Inherited = currentColumn.Inherited
			desiredColumn = &columnCopy
		}

		if !found {
			instructions = append(instructions,
				alterTable(&PostgresAddColumnAction{Column: desiredColumn}))
			instructions = append(instructions, desiredColumn.StorageInstructions(t.Name)...)
			instructions = append(instructions, desiredColumn.StatisticsInstructions(t.Name)...)

			continue
		}

		if desiredColumn.HasEqualAttributes(currentColumn) {
			continue
		}

		// A generated column keeps no data of its own. DiffTables removes the dependents of a
		// rebuilt column from the current table, so the blocks below create them again.
		if desiredColumn.NeedsRebuild(currentColumn, serverVersion) {
			instructions = append(instructions, &PostgresAlterTableInstruction{
				Name: t.Name,
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: currentColumn.Name},
					&PostgresAddColumnAction{Column: desiredColumn},
				},
			})
			instructions = append(instructions, desiredColumn.StorageInstructions(t.Name)...)
			instructions = append(instructions, desiredColumn.StatisticsInstructions(t.Name)...)

			continue
		}

		if desiredColumn.GeneratedExpression != currentColumn.GeneratedExpression {
			if desiredColumn.GeneratedExpression == "" {
				instructions = append(instructions,
					alterTable(&PostgresDropExpressionAction{ColumnName: desiredColumn.Name}))
			} else {
				instructions = append(instructions, alterTable(&PostgresSetExpressionAction{
					ColumnName: desiredColumn.Name,
					Expression: desiredColumn.GeneratedExpression,
				}))
			}
		}

		// PostgreSQL refuses to remove the NOT NULL flag of an identity column, so this
		// action comes before the NOT NULL block below.
		if currentColumn.Identity != "" && desiredColumn.Identity == "" {
			instructions = append(instructions,
				alterTable(&PostgresDropIdentityAction{ColumnName: desiredColumn.Name}))
		}

		typeChanged := desiredColumn.Type != currentColumn.Type ||
			desiredColumn.Collation != currentColumn.Collation

		// PostgreSQL casts the default in the TYPE action and refuses a default that it cannot
		// cast automatically, so the default block below sets the default again.
		defaultDroppedForType := typeChanged && currentColumn.Default.Valid
		if defaultDroppedForType {
			instructions = append(instructions, alterColumn(&PostgresDropDefaultAction{
				ColumnName: desiredColumn.Name,
			}))
		}

		// PostgreSQL changes a collation through the TYPE action.
		if typeChanged {
			usingCast, err := columnUsingClause(desiredColumn, currentColumn, hasAutomaticCast)
			if err != nil {
				return nil, err
			}

			instructions = append(instructions, alterTable(&PostgresAlterColumnTypeAction{
				ColumnName: desiredColumn.Name,
				DataType:   desiredColumn.Type,
				Collation:  desiredColumn.Collation,
				UsingCast:  usingCast,
			}))
		}

		// SET NOT NULL reaches each child, and the child must keep the flag of its parent.
		// DROP NOT NULL keeps the flag of each child that holds its own.
		if desiredColumn.NotNull != currentColumn.NotNull {
			if desiredColumn.NotNull {
				instructions = append(instructions,
					alterTable(&PostgresSetNotNullAction{ColumnName: desiredColumn.Name}))
			} else {
				// PostgreSQL refuses to remove the flag of a column of the primary key,
				// so this action waits for the constraint blocks below.
				dropNotNullInstructions = append(dropNotNullInstructions,
					alterColumn(&PostgresDropNotNullAction{ColumnName: desiredColumn.Name}))
			}
		}

		// A serial column and a column with an owned sequence of the same name hold the same
		// default, and the sequence section then changes the options of the sequence.
		serialAdded := desiredColumn.Serial != "" && currentColumn.Serial == "" &&
			desiredColumn.Default != currentColumn.Default
		serialRemoved := desiredColumn.Serial == "" && currentColumn.Serial != "" &&
			desiredColumn.Default != currentColumn.Default

		if (desiredColumn.Default != currentColumn.Default || defaultDroppedForType) && !serialAdded {
			if desiredColumn.Default.Valid {
				instructions = append(instructions, alterColumn(&PostgresSetDefaultAction{
					ColumnName: desiredColumn.Name,
					Expression: desiredColumn.Default.String,
				}))
			} else if !defaultDroppedForType {
				instructions = append(instructions,
					alterColumn(&PostgresDropDefaultAction{ColumnName: desiredColumn.Name}))
			}
		}

		// The serial word owns a sequence and gives the default, and PostgreSQL holds no
		// single action for it.
		if serialAdded {
			instructions = append(instructions, &PostgresCreateOwnedSequenceInstruction{
				Name:       desiredColumn.SerialSequenceName,
				DataType:   desiredColumn.Type,
				TableName:  t.Name,
				ColumnName: desiredColumn.Name,
			})
			instructions = append(instructions, alterTable(&PostgresSetDefaultAction{
				ColumnName: desiredColumn.Name,
				Expression: desiredColumn.Default.String,
			}))
			instructions = append(instructions, &PostgresRestartOwnedSequenceInstruction{
				TableName:  t.Name,
				ColumnName: desiredColumn.Name,
			})
		}

		// The default block above removes the use of the sequence first.
		if serialRemoved {
			instructions = append(instructions, &PostgresDropSequenceInstruction{
				Name: currentColumn.SerialSequenceName,
			})
		}

		// A TYPE action gives the column the storage mode of the new type, so this action always
		// follows it. The mode DEFAULT gives the column the mode of its type again.
		if desiredColumn.Storage != currentColumn.Storage ||
			(typeChanged && desiredColumn.Storage != "") {
			storage := desiredColumn.Storage
			if storage == "" {
				storage = "DEFAULT"
			}

			instructions = append(instructions, alterColumn(&PostgresSetStorageAction{
				ColumnName: desiredColumn.Name,
				Storage:    storage,
			}))
		}

		// A TYPE action keeps the statistics target, and the value -1 gives the default target.
		if desiredColumn.StatisticsTarget != currentColumn.StatisticsTarget {
			statisticsTarget := int64(-1)
			if desiredColumn.StatisticsTarget.Valid {
				statisticsTarget = desiredColumn.StatisticsTarget.Int64
			}

			instructions = append(instructions, alterColumn(&PostgresSetStatisticsAction{
				ColumnName:       desiredColumn.Name,
				StatisticsTarget: statisticsTarget,
			}))
		}

		// The options of an identity column live in its sequence.
		if desiredColumn.Identity != "" && currentColumn.Identity != "" &&
			desiredColumn.IdentityOptions != currentColumn.IdentityOptions {
			options := desiredColumn.IdentityOptions

			// The read builds the options from the values that differ from the defaults,
			// so an empty desired value resets each option that the current column names.
			if options == "" {
				options = identityOptionDefaults(currentColumn.IdentityOptions, desiredColumn.Type)
			}

			instructions = append(instructions, alterTable(&PostgresSetIdentityOptionsAction{
				ColumnName: desiredColumn.Name,
				Options:    options,
			}))
		}

		// PostgreSQL refuses to add an identity to a column that accepts a null value, so
		// these two actions come after the NOT NULL block above.
		if desiredColumn.Identity != "" && desiredColumn.Identity != currentColumn.Identity {
			if currentColumn.Identity == "" {
				instructions = append(instructions, alterTable(&PostgresAddIdentityAction{
					ColumnName: desiredColumn.Name,
					Identity:   desiredColumn.Identity,
					Options:    desiredColumn.IdentityOptions,
				}), &PostgresRestartOwnedSequenceInstruction{
					TableName:  t.Name,
					ColumnName: desiredColumn.Name,
				})
			} else {
				instructions = append(instructions, alterTable(&PostgresSetIdentityAction{
					ColumnName: desiredColumn.Name,
					Identity:   desiredColumn.Identity,
				}))
			}
		}
	}

	if t.Unlogged != other.Unlogged {
		persistence := "LOGGED"
		if t.Unlogged {
			persistence = "UNLOGGED"
		}

		instructions = append(instructions,
			alterTable(&PostgresSetPersistenceAction{Persistence: persistence}))
	}

	instructions = append(instructions, diffStorageParameters(t, other)...)
	instructions = append(instructions, diffRowLevelSecurity(t, other)...)

	instructions = append(instructions, diffComments(t, other)...)

	// PostgreSQL drops every constraint and every index of a column with the column, so
	// these two blocks come before the column removals below. DiffTables prints the foreign
	// keys, because a foreign key can name a key of a table that DiffTables changes later.
	// A table holds one primary key only, so the removals come before the additions.
	for _, currentConstraint := range other.Constraints {
		_, found := t.ConstraintByName(currentConstraint.Name)
		if !found && !currentConstraint.Inherited && !currentConstraint.IsForeignKey() {
			instructions = append(instructions,
				alterTable(&PostgresDropConstraintAction{ConstraintName: currentConstraint.Name}))
		}
	}

	for _, desiredConstraint := range t.Constraints {
		if desiredConstraint.Inherited || desiredConstraint.IsForeignKey() {
			continue
		}

		currentConstraint, found := other.ConstraintByName(desiredConstraint.Name)
		if !found {
			instructions = append(instructions,
				alterTable(&PostgresAddConstraintAction{Constraint: desiredConstraint}))

			continue
		}

		if desiredConstraint.Def != currentConstraint.Def {
			instructions = append(instructions,
				alterTable(&PostgresDropConstraintAction{ConstraintName: currentConstraint.Name}),
				alterTable(&PostgresAddConstraintAction{Constraint: desiredConstraint}))
		}
	}

	instructions = append(instructions, dropNotNullInstructions...)

	instructions = append(instructions, diffIndexAdditions(t, other)...)

	// The mode USING INDEX names an index, so this block comes after the index additions
	// above. It comes before the index removals below, because PostgreSQL refuses to drop
	// the index that the replica identity of the current table holds.
	if t.ReplicaIdentity != other.ReplicaIdentity ||
		t.ReplicaIdentityIndex != other.ReplicaIdentityIndex {
		instructions = append(instructions, alterTable(&PostgresReplicaIdentityAction{
			Mode:      t.ReplicaIdentity,
			IndexName: t.ReplicaIdentityIndex,
		}))
	}

	instructions = append(instructions, diffIndexRemovals(t, other)...)

	for _, currentColumn := range other.Columns {
		_, found := t.ColumnByName(currentColumn.Name)
		if !found && !currentColumn.Inherited {
			instructions = append(instructions,
				alterTable(&driversshared.SQLDropColumnAction{ColumnName: currentColumn.Name}))
		}
	}

	instructions = append(instructions, diffTriggers(t, other)...)

	return instructions, nil
}

func diffPartitionColumns(desiredTable *PostgresTable, currentTable *PostgresTable) []driversshared.Instruction {
	var instructions []driversshared.Instruction

	alterTable := func(action driversshared.AlterTableAction) driversshared.Instruction {
		return &PostgresAlterTableInstruction{
			Name:    desiredTable.Name,
			Actions: []driversshared.AlterTableAction{action},
		}
	}

	for _, desiredColumn := range desiredTable.Columns {
		currentColumn, found := currentTable.ColumnByName(desiredColumn.Name)
		if !found {
			continue
		}

		if desiredColumn.Default != currentColumn.Default {
			if desiredColumn.Default.Valid {
				instructions = append(instructions, alterTable(&PostgresSetDefaultAction{
					ColumnName: desiredColumn.Name,
					Expression: desiredColumn.Default.String,
				}))
			} else {
				instructions = append(instructions, alterTable(&PostgresDropDefaultAction{
					ColumnName: desiredColumn.Name,
				}))
			}
		}

		if desiredColumn.NotNull != currentColumn.NotNull {
			if desiredColumn.NotNull {
				instructions = append(instructions, alterTable(&PostgresSetNotNullAction{
					ColumnName: desiredColumn.Name,
				}))
			} else {
				instructions = append(instructions, alterTable(&PostgresDropNotNullAction{
					ColumnName: desiredColumn.Name,
				}))
			}
		}

		if desiredColumn.Storage != currentColumn.Storage {
			storage := desiredColumn.Storage
			if storage == "" {
				storage = "DEFAULT"
			}

			instructions = append(instructions, alterTable(&PostgresSetStorageAction{
				ColumnName: desiredColumn.Name,
				Storage:    storage,
			}))
		}

		if desiredColumn.StatisticsTarget != currentColumn.StatisticsTarget {
			statisticsTarget := int64(-1)
			if desiredColumn.StatisticsTarget.Valid {
				statisticsTarget = desiredColumn.StatisticsTarget.Int64
			}

			instructions = append(instructions, alterTable(&PostgresSetStatisticsAction{
				ColumnName:       desiredColumn.Name,
				StatisticsTarget: statisticsTarget,
			}))
		}
	}

	return instructions
}

func diffComments(desiredTable *PostgresTable, currentTable *PostgresTable) []driversshared.Instruction {
	var instructions []driversshared.Instruction

	if desiredTable.Comment != currentTable.Comment {
		instructions = append(instructions, &PostgresCommentOnTableInstruction{
			Name: desiredTable.Name,
			Text: desiredTable.Comment,
		})
	}

	for _, desiredColumn := range desiredTable.Columns {
		currentColumn, found := currentTable.ColumnByName(desiredColumn.Name)
		if !found {
			continue
		}

		if desiredColumn.Comment != currentColumn.Comment {
			instructions = append(instructions, &PostgresCommentOnColumnInstruction{
				TableName:  desiredTable.Name,
				ColumnName: desiredColumn.Name,
				Text:       desiredColumn.Comment,
			})
		}
	}

	return instructions
}

func diffIndexAdditions(desiredTable *PostgresTable, currentTable *PostgresTable) []driversshared.Instruction {
	var instructions []driversshared.Instruction

	for _, desiredIndex := range desiredTable.Indexes {
		currentIndex, found := currentTable.IndexByName(desiredIndex.Name)
		if !found {
			instructions = append(instructions, desiredIndex.Instructions()...)
			continue
		}

		if desiredIndex.Def != currentIndex.Def {
			instructions = append(instructions, &driversshared.SQLDropIndexInstruction{Name: currentIndex.Name})
			instructions = append(instructions, desiredIndex.Instructions()...)
			continue
		}

		if desiredIndex.Comment != currentIndex.Comment {
			instructions = append(instructions, &PostgresCommentOnIndexInstruction{
				Name: desiredIndex.Name,
				Text: desiredIndex.Comment,
			})
		}
	}

	return instructions
}

func diffIndexRemovals(desiredTable *PostgresTable, currentTable *PostgresTable) []driversshared.Instruction {
	var instructions []driversshared.Instruction

	for _, currentIndex := range currentTable.Indexes {
		_, found := desiredTable.IndexByName(currentIndex.Name)
		if !found {
			instructions = append(instructions, &driversshared.SQLDropIndexInstruction{Name: currentIndex.Name})
		}
	}

	return instructions
}

func diffTriggers(desiredTable *PostgresTable, currentTable *PostgresTable) []driversshared.Instruction {
	var instructions []driversshared.Instruction

	for _, desiredTrigger := range desiredTable.Triggers {
		currentTrigger, found := currentTable.TriggerByName(desiredTrigger.Name)
		if !found {
			instructions = append(instructions, desiredTrigger.CreateInstruction())
			instructions = append(instructions, desiredTrigger.EnableInstructions(desiredTable.Name)...)

			continue
		}

		// A new trigger takes the mode ENABLE, so the mode of the desired trigger comes after the
		// CREATE TRIGGER statement below.
		if desiredTrigger.Def != currentTrigger.Def {
			instructions = append(instructions,
				&PostgresDropTriggerInstruction{
					Name:      currentTrigger.Name,
					TableName: desiredTable.Name,
				},
				desiredTrigger.CreateInstruction())
			instructions = append(instructions, desiredTrigger.EnableInstructions(desiredTable.Name)...)

			continue
		}

		if desiredTrigger.EnableMode != currentTrigger.EnableMode {
			instructions = append(instructions, &PostgresAlterTableInstruction{
				Name: desiredTable.Name,
				Actions: []driversshared.AlterTableAction{&PostgresTriggerEnableAction{
					Mode:        desiredTrigger.EnableMode,
					TriggerName: desiredTrigger.Name,
				}},
			})
		}
	}

	for _, currentTrigger := range currentTable.Triggers {
		_, found := desiredTable.TriggerByName(currentTrigger.Name)
		if !found {
			instructions = append(instructions,
				&PostgresDropTriggerInstruction{
					Name:      currentTrigger.Name,
					TableName: desiredTable.Name,
				})
		}
	}

	return instructions
}

// A statement needs the parent: a partition needs its parent, a table of INHERITS needs
// each parent of it, and a foreign key needs the table that it names.
func sortTablesByDependency(tables []*PostgresTable) []*PostgresTable {
	return sortByDependency(tables, func(table *PostgresTable) string {
		return table.Name
	}, func(table *PostgresTable) []string {
		return slices.Concat([]string{table.PartitionParent}, table.Inherits, table.References)
	})
}

func (t *PostgresTable) DiffRules(other *PostgresTable) []driversshared.Instruction {
	var instructions []driversshared.Instruction

	for _, desiredRule := range t.Rules {
		currentRule, found := other.RuleByName(desiredRule.Name)
		if !found {
			instructions = append(instructions, desiredRule.CreateInstruction())
			continue
		}

		// PostgreSQL holds no ALTER action for a rule. The DROP and CREATE pair stays one modification.
		if desiredRule.Def != currentRule.Def {
			instructions = append(instructions,
				currentRule.DropInstruction(), desiredRule.CreateInstruction())
		}
	}

	for _, currentRule := range other.Rules {
		_, found := t.RuleByName(currentRule.Name)
		if !found {
			instructions = append(instructions, currentRule.DropInstruction())
		}
	}

	return instructions
}

func (t *PostgresTable) RuleInstructions() []driversshared.Instruction {
	return lo.Map(t.Rules, func(rule *PostgresRule, _ int) driversshared.Instruction {
		return rule.CreateInstruction()
	})
}

func diffStorageParameters(desiredTable *PostgresTable, currentTable *PostgresTable) []driversshared.Instruction {
	if slices.Equal(desiredTable.StorageParameters, currentTable.StorageParameters) {
		return nil
	}

	var instructions []driversshared.Instruction

	if len(desiredTable.StorageParameters) > 0 {
		instructions = append(instructions, &PostgresAlterTableInstruction{
			Name: desiredTable.Name,
			Actions: []driversshared.AlterTableAction{
				&PostgresSetStorageParametersAction{Parameters: desiredTable.StorageParameters},
			},
		})
	}

	var removed []string

	for _, parameter := range currentTable.StorageParameters {
		name := storageParameterName(parameter)

		held := slices.ContainsFunc(desiredTable.StorageParameters,
			func(desiredParameter string) bool {
				return storageParameterName(desiredParameter) == name
			})
		if !held {
			removed = append(removed, name)
		}
	}

	if len(removed) > 0 {
		instructions = append(instructions, &PostgresAlterTableInstruction{
			Name:    desiredTable.Name,
			Actions: []driversshared.AlterTableAction{&PostgresResetStorageParametersAction{Names: removed}},
		})
	}

	return instructions
}

func storageParameterName(parameter string) string {
	name, _, found := strings.Cut(parameter, "=")
	if !found {
		return parameter
	}

	return name
}

func diffRowLevelSecurity(desiredTable *PostgresTable, currentTable *PostgresTable) []driversshared.Instruction {
	var instructions []driversshared.Instruction

	alterTable := func(mode string) driversshared.Instruction {
		return &PostgresAlterTableInstruction{
			Name:    desiredTable.Name,
			Actions: []driversshared.AlterTableAction{&PostgresRowLevelSecurityAction{Mode: mode}},
		}
	}

	if desiredTable.RowLevelSecurity != currentTable.RowLevelSecurity {
		if desiredTable.RowLevelSecurity {
			instructions = append(instructions, alterTable("ENABLE"))
		} else {
			instructions = append(instructions, alterTable("DISABLE"))
		}
	}

	if desiredTable.ForceRowLevelSecurity != currentTable.ForceRowLevelSecurity {
		if desiredTable.ForceRowLevelSecurity {
			instructions = append(instructions, alterTable("FORCE"))
		} else {
			instructions = append(instructions, alterTable("NO FORCE"))
		}
	}

	return instructions
}

func (t *PostgresTable) ConstraintByName(name string) (*PostgresConstraint, bool) {
	for _, c := range t.Constraints {
		if c.Name == name {
			return c, true
		}
	}

	return nil, false
}

func (t *PostgresTable) IndexByName(name string) (*PostgresIndex, bool) {
	for _, i := range t.Indexes {
		if i.Name == name {
			return i, true
		}
	}

	return nil, false
}

func (t *PostgresTable) RuleByName(name string) (*PostgresRule, bool) {
	for _, rule := range t.Rules {
		if rule.Name == name {
			return rule, true
		}
	}

	return nil, false
}

func (t *PostgresTable) TriggerByName(name string) (*PostgresTrigger, bool) {
	for _, tr := range t.Triggers {
		if tr.Name == name {
			return tr, true
		}
	}

	return nil, false
}

func (t *PostgresTable) CreateTableInstruction() *PostgresCreateTableInstruction {
	var constraints []*PostgresConstraint

	for _, constraint := range t.Constraints {
		if !constraint.IsForeignKey() && !constraint.Inherited {
			constraints = append(constraints, constraint)
		}
	}

	var columns []*PostgresColumn

	for _, column := range t.Columns {
		if !column.Inherited {
			columns = append(columns, column)
		}
	}

	return &PostgresCreateTableInstruction{
		Name:         t.Name,
		Columns:      columns,
		Constraints:  constraints,
		PartitionKey: t.PartitionKey,
		Inherits:     t.Inherits,
		Unlogged:     t.Unlogged,

		StorageParameters: t.StorageParameters,
	}
}

// A partition takes the foreign keys of its parent, so a second statement of one fails.
func (t *PostgresTable) ForeignKeyInstructions() []driversshared.Instruction {
	if t.IsPartition() {
		return nil
	}

	var instructions []driversshared.Instruction

	for _, constraint := range t.Constraints {
		if !constraint.IsForeignKey() {
			continue
		}

		instructions = append(instructions, &PostgresAlterTableInstruction{
			Name: t.Name,
			Actions: []driversshared.AlterTableAction{
				&PostgresAddConstraintAction{Constraint: constraint},
			},
		})
	}

	return instructions
}

// The action of a rule can name a second table, so DiffTables prints the rules apart.
func (t *PostgresTable) Instructions() []driversshared.Instruction {
	if t.IsPartition() {
		instructions := []driversshared.Instruction{
			&PostgresCreateTablePartitionInstruction{
				Name:       t.Name,
				ParentName: t.PartitionParent,
				Bound:      t.PartitionBound,
			},
		}

		instructions = append(instructions, t.CommentInstructions()...)
		instructions = append(instructions, t.RowLevelSecurityInstructions()...)

		for _, index := range t.Indexes {
			instructions = append(instructions, index.Instructions()...)
		}

		for _, trigger := range t.Triggers {
			instructions = append(instructions, trigger.CreateInstruction())
			instructions = append(instructions, trigger.EnableInstructions(t.Name)...)
		}

		return instructions
	}

	instructions := []driversshared.Instruction{t.CreateTableInstruction()}

	for _, column := range t.Columns {
		instructions = append(instructions, column.StorageInstructions(t.Name)...)
		instructions = append(instructions, column.StatisticsInstructions(t.Name)...)
	}

	instructions = append(instructions, t.CommentInstructions()...)
	instructions = append(instructions, t.RowLevelSecurityInstructions()...)

	for _, index := range t.Indexes {
		instructions = append(instructions, index.Instructions()...)
	}

	// The mode USING INDEX names an index, so this statement comes after the index loop.
	instructions = append(instructions, t.ReplicaIdentityInstructions()...)

	for _, trigger := range t.Triggers {
		instructions = append(instructions, trigger.CreateInstruction())
		instructions = append(instructions, trigger.EnableInstructions(t.Name)...)
	}

	return instructions
}

func (d *PostgresDriver) DiffTables(ctx context.Context) (*SectionDiff, error) {
	var earlyRemovals []driversshared.Instruction
	var additions []driversshared.Instruction
	var removals []driversshared.Instruction

	desiredTables, err := d.GetTables(ctx, d.DesiredDatabaseConnection)
	if err != nil {
		return nil, err
	}

	currentTables, err := d.GetTables(ctx, d.CurrentDatabaseConnection)
	if err != nil {
		return nil, err
	}

	hasAutomaticCast := func(oldType string, newType string) (bool, error) {
		return d.HasAutomaticCast(ctx, d.CurrentDatabaseConnection, oldType, newType)
	}

	var serverVersion int

	err = d.CurrentDatabaseConnection.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::int`).
		Scan(&serverVersion)
	if err != nil {
		return nil, err
	}

	for _, currentTable := range currentTables {
		for _, columnName := range d.droppedColumnDefaults[currentTable.Name] {
			currentColumn, found := currentTable.ColumnByName(columnName)
			if found {
				currentColumn.Default = sql.NullString{}
			}
		}
	}

	descendantDefaultDrops, droppedDescendantDefaults, err := dropDescendantDefaultsForTypeChanges(
		desiredTables,
		currentTables,
		hasAutomaticCast,
		serverVersion,
	)
	if err != nil {
		return nil, err
	}

	// The action of a rule can name a second table, so every rule comes after every table.
	var ruleInstructions []driversshared.Instruction

	// Two tables can name each other, so every foreign key comes after every table.
	var foreignKeyInstructions []driversshared.Instruction

	// A policy can read a second table, so every new policy comes after every table.
	// PostgreSQL refuses a change of a column while a policy reads it, so every policy drop
	// comes before every table change.
	var policyInstructions []driversshared.Instruction

	keptCurrentTables := map[string]*PostgresTable{}
	recreatedForeignKeys := map[string]bool{}

	for _, desiredTable := range desiredTables {
		currentTable, found := lo.Find(currentTables, func(table *PostgresTable) bool {
			return table.Name == desiredTable.Name
		})
		if !found {
			additions = append(additions, desiredTable.Instructions()...)

			if len(desiredTable.Inherits) > 0 || desiredTable.IsPartition() {
				attributeInstructions, err := desiredTable.DiffTable(
					desiredTable.creationStateOfChild(desiredTables),
					hasAutomaticCast,
					serverVersion,
				)
				if err != nil {
					return nil, err
				}

				additions = append(additions, attributeInstructions...)
			}

			foreignKeyInstructions = append(foreignKeyInstructions,
				desiredTable.ForeignKeyInstructions()...)
			ruleInstructions = append(ruleInstructions, desiredTable.RuleInstructions()...)

			for _, policy := range desiredTable.Policies {
				policyInstructions = append(policyInstructions, policy.CreateInstruction())
			}

			continue
		}

		currentTable = currentTable.withoutDefaults(droppedDescendantDefaults[currentTable.Name])

		var preInheritInstructions []driversshared.Instruction

		currentTable, preInheritInstructions = currentTable.withInheritanceChanges(desiredTable, currentTables, desiredTables)
		additions = append(additions, preInheritInstructions...)

		// The statements of each ancestor reach the partition before a DETACH PARTITION
		// statement, so the current ancestors give the state. The root comes first, because a
		// change of the root reaches each level below it. An ancestor that DiffTables has
		// not walked yet changes the partition after the DETACH PARTITION statement.
		for _, ancestor := range slices.Backward(currentAncestors(currentTable, currentTables)) {
			currentAncestor, found := keptCurrentTables[ancestor.Name]
			if !found {
				continue
			}

			desiredAncestor, _ := lo.Find(desiredTables, func(table *PostgresTable) bool {
				return table.Name == ancestor.Name
			})
			currentTable = currentTable.withParentColumnChanges(desiredAncestor, currentAncestor, serverVersion)
		}

		additions = append(additions, descendantDefaultDrops[desiredTable.Name]...)

		rebuiltColumnNames := desiredTable.RebuiltColumnNames(currentTable, serverVersion)
		if len(rebuiltColumnNames) > 0 {
			dependents, err := getColumnDependents(ctx, d.CurrentDatabaseConnection, currentTable.Name, rebuiltColumnNames)
			if err != nil {
				return nil, err
			}

			for _, dependent := range dependents {
				switch dependent.Kind {
				case "blocker":
					return nil, fmt.Errorf(
						"the %s uses the column %q of the table %q. dbdiff must drop and add this column, "+
							"and it cannot drop and create this object again",
						dependent.Name, dependent.ColumnName, currentTable.Name)
				case "relation":
					d.markRecreated(dependent.Name)
				case "foreign key":
					recreatedForeignKeys[foreignKeyName(dependent.TableName, dependent.Name)] = true
				}
			}

			var dropInstructions []driversshared.Instruction

			currentTable, dropInstructions = currentTable.withoutColumnDependents(rebuiltColumnNames, dependents)
			additions = append(additions, dropInstructions...)
		}

		subInstructions, err := desiredTable.DiffTable(currentTable, hasAutomaticCast, serverVersion)
		if err != nil {
			return nil, err
		}

		additions = append(additions, subInstructions...)
		ruleInstructions = append(ruleInstructions, desiredTable.DiffRules(currentTable)...)
		keptCurrentTables[desiredTable.Name] = currentTable

		policyCreations, policyDrops := driversshared.ReplaceByKey(desiredTable.Policies, currentTable.Policies,
			func(policy *PostgresPolicy) string {
				return policy.Name
			},
			(*PostgresPolicy).Equal,
			func(policy *PostgresPolicy) driversshared.Instruction {
				return policy.CreateInstruction()
			},
			func(policy *PostgresPolicy) driversshared.Instruction {
				return policy.DropInstruction()
			})
		policyInstructions = append(policyInstructions, policyCreations...)
		earlyRemovals = append(earlyRemovals, policyDrops...)

		// PostgreSQL attaches an own index of a partition to a new index of the parent that
		// matches it, and then refuses to drop it. So these removals come before every addition.
		if desiredTable.IsPartition() {
			earlyRemovals = append(earlyRemovals, diffIndexRemovals(desiredTable, currentTable)...)
		}
	}

	foreignKeyDrops, foreignKeyAdditions := diffForeignKeys(desiredTables, currentTables, keptCurrentTables,
		recreatedForeignKeys)
	earlyRemovals = append(earlyRemovals, foreignKeyDrops...)
	additions = append(additions, foreignKeyInstructions...)
	additions = append(additions, foreignKeyAdditions...)
	additions = append(additions, policyInstructions...)
	additions = append(additions, ruleInstructions...)

	// The reverse order gives each DROP TABLE statement before the table that it names.
	for _, currentTable := range slices.Backward(currentTables) {
		_, found := lo.Find(desiredTables, func(table *PostgresTable) bool {
			return table.Name == currentTable.Name
		})
		if found {
			continue
		}

		// A DROP TABLE statement of a parent removes every partition of it.
		if currentTable.IsPartition() && isTableDropped(currentTable.PartitionParent, desiredTables) {
			continue
		}

		removals = append(removals, &driversshared.SQLDropTableInstruction{Name: currentTable.Name})
	}

	return &SectionDiff{
		EarlyRemovals: earlyRemovals,
		Additions:     additions,
		Removals:      removals,
	}, nil
}

// The current table holds the state before the parents change, and the copy holds the state
// that the INHERIT and NO INHERIT actions and the parent statements give the child:
//   - NO INHERIT makes each column and each constraint that no kept parent gives a local
//     object of the child.
//   - A change of a kept parent without ONLY reaches the child. Only DROP NOT NULL uses ONLY.
//   - INHERIT refuses a child that has no column, no NOT NULL flag, or no CHECK constraint of
//     the new parent, so the returned instructions add them before DiffTable prints INHERIT.
func (t *PostgresTable) withInheritanceChanges(
	desiredTable *PostgresTable,
	currentTables []*PostgresTable,
	desiredTables []*PostgresTable,
) (*PostgresTable, []driversshared.Instruction) {
	if len(t.Inherits) == 0 && len(desiredTable.Inherits) == 0 {
		return t, nil
	}

	alterTable := func(action driversshared.AlterTableAction) driversshared.Instruction {
		return &PostgresAlterTableInstruction{
			Name:    t.Name,
			Actions: []driversshared.AlterTableAction{action},
		}
	}

	isKeptParent := func(name string) bool {
		return slices.Contains(t.Inherits, name) && slices.Contains(desiredTable.Inherits, name)
	}

	keptCurrentParents := lo.Filter(currentTables, func(table *PostgresTable, _ int) bool {
		return isKeptParent(table.Name)
	})
	keptDesiredParents := lo.Filter(desiredTables, func(table *PostgresTable, _ int) bool {
		return isKeptParent(table.Name)
	})
	newDesiredParents := lo.Filter(desiredTables, func(table *PostgresTable, _ int) bool {
		return slices.Contains(desiredTable.Inherits, table.Name) && !slices.Contains(t.Inherits, table.Name)
	})

	parentColumn := func(parents []*PostgresTable, name string) (*PostgresColumn, bool) {
		for _, parent := range parents {
			column, found := parent.ColumnByName(name)
			if found {
				return column, true
			}
		}

		return nil, false
	}

	var preInheritInstructions []driversshared.Instruction

	tableCopy := *t

	tableCopy.Columns = lo.Map(t.Columns, func(column *PostgresColumn, _ int) *PostgresColumn {
		columnCopy := *column

		currentParentColumn, keptParentHoldsColumn := parentColumn(keptCurrentParents, column.Name)
		if column.Inherited && !keptParentHoldsColumn {
			columnCopy.Inherited = false
		}

		desiredParentColumn, found := parentColumn(keptDesiredParents, column.Name)
		if keptParentHoldsColumn && found && desiredParentColumn.NotNull && !currentParentColumn.NotNull {
			columnCopy.NotNull = true
		}

		newParentColumn, found := parentColumn(newDesiredParents, column.Name)
		if found && newParentColumn.NotNull && !columnCopy.NotNull {
			preInheritInstructions = append(preInheritInstructions, alterTable(&PostgresSetNotNullAction{
				ColumnName: column.Name,
			}))
			columnCopy.NotNull = true
		}

		return &columnCopy
	})

	for _, desiredColumn := range desiredTable.Columns {
		_, found := t.ColumnByName(desiredColumn.Name)
		if found || !desiredColumn.Inherited {
			continue
		}

		keptParentColumn, found := parentColumn(keptDesiredParents, desiredColumn.Name)
		if found {
			columnCopy := *keptParentColumn
			columnCopy.Comment = ""
			columnCopy.Inherited = true
			tableCopy.Columns = append(tableCopy.Columns, &columnCopy)

			continue
		}

		newParentColumn, found := parentColumn(newDesiredParents, desiredColumn.Name)
		if !found {
			continue
		}

		columnCopy := *newParentColumn
		columnCopy.Comment = ""
		columnCopy.Storage = ""
		columnCopy.StatisticsTarget = sql.NullInt64{}
		columnCopy.Serial = ""
		columnCopy.SerialSequenceName = ""
		columnCopy.Inherited = false
		preInheritInstructions = append(preInheritInstructions, alterTable(&PostgresAddColumnAction{
			Column: &columnCopy,
		}))
		tableCopy.Columns = append(tableCopy.Columns, &columnCopy)
	}

	tableCopy.Constraints = lo.Map(t.Constraints, func(constraint *PostgresConstraint, _ int) *PostgresConstraint {
		keptParentHoldsConstraint := lo.SomeBy(keptCurrentParents, func(parent *PostgresTable) bool {
			_, found := parent.ConstraintByName(constraint.Name)
			return found
		})
		if !constraint.Inherited || keptParentHoldsConstraint {
			return constraint
		}

		constraintCopy := *constraint
		constraintCopy.Inherited = false

		return &constraintCopy
	})

	for _, parent := range newDesiredParents {
		for _, parentConstraint := range parent.Constraints {
			_, found := tableCopy.ConstraintByName(parentConstraint.Name)
			if found || parentConstraint.Type != "c" || strings.HasSuffix(parentConstraint.Def, " NO INHERIT") {
				continue
			}

			constraintCopy := *parentConstraint
			constraintCopy.Inherited = false
			preInheritInstructions = append(preInheritInstructions, alterTable(&PostgresAddConstraintAction{
				Constraint: &constraintCopy,
			}))
			tableCopy.Constraints = append(tableCopy.Constraints, &constraintCopy)
		}
	}

	return &tableCopy, preInheritInstructions
}

func currentAncestors(table *PostgresTable, currentTables []*PostgresTable) []*PostgresTable {
	var ancestors []*PostgresTable

	parentName := table.PartitionParent

	for parentName != "" {
		parent, found := lo.Find(currentTables, func(currentTable *PostgresTable) bool {
			return currentTable.Name == parentName
		})
		if !found {
			break
		}

		ancestors = append(ancestors, parent)
		parentName = parent.PartitionParent
	}

	return ancestors
}

// A TYPE action of a parent casts the default of each partition and each child of INHERITS,
// and it fails on a default that it cannot cast. The returned instructions drop these
// defaults before the TYPE action of the parent, and the diff of each partition and each
// child sets them again. A DROP DEFAULT statement of a partitioned table without ONLY
// removes the default of each partition, so a partitioned table with a default needs no
// more statements. The returned map gives the columns of each table without a default after
// these instructions.
func dropDescendantDefaultsForTypeChanges(
	desiredTables []*PostgresTable,
	currentTables []*PostgresTable,
	hasAutomaticCast AutomaticCastLookup,
	serverVersion int,
) (map[string][]driversshared.Instruction, map[string][]string, error) {
	drops := map[string][]driversshared.Instruction{}
	droppedDefaults := map[string][]string{}

	descendants := func(name string) []*PostgresTable {
		var tables []*PostgresTable

		parentNames := []string{name}

		for len(parentNames) > 0 {
			parentName := parentNames[0]
			parentNames = parentNames[1:]

			for _, table := range currentTables {
				if table.PartitionParent == parentName || slices.Contains(table.Inherits, parentName) {
					tables = append(tables, table)
					parentNames = append(parentNames, table.Name)
				}
			}
		}

		return tables
	}

	for _, currentTable := range currentTables {
		desiredTable, found := lo.Find(desiredTables, func(table *PostgresTable) bool {
			return table.Name == currentTable.Name
		})
		if !found || currentTable.IsPartition() {
			continue
		}

		for _, currentColumn := range currentTable.Columns {
			desiredColumn, found := desiredTable.ColumnByName(currentColumn.Name)
			if !found || currentColumn.Inherited ||
				(desiredColumn.Type == currentColumn.Type && desiredColumn.Collation == currentColumn.Collation) ||
				(currentTable.PartitionKey != "" && currentColumn.Default.Valid) ||
				desiredColumn.NeedsRebuild(currentColumn, serverVersion) {
				continue
			}

			usingCast, err := columnUsingClause(desiredColumn, currentColumn, hasAutomaticCast)
			if err != nil {
				return nil, nil, err
			}

			if !usingCast {
				continue
			}

			for _, descendant := range descendants(currentTable.Name) {
				descendantColumn, found := descendant.ColumnByName(currentColumn.Name)
				if !found || !descendantColumn.Default.Valid {
					continue
				}

				drops[currentTable.Name] = append(drops[currentTable.Name], &PostgresAlterTableInstruction{
					Name: descendant.Name,
					Actions: []driversshared.AlterTableAction{
						&PostgresDropDefaultAction{
							ColumnName: descendantColumn.Name,
						},
					},
				})
				droppedDefaults[descendant.Name] = append(droppedDefaults[descendant.Name], descendantColumn.Name)
			}
		}
	}

	return drops, droppedDefaults, nil
}

func (t *PostgresTable) withoutDefaults(columnNames []string) *PostgresTable {
	if len(columnNames) == 0 {
		return t
	}

	tableCopy := *t

	tableCopy.Columns = lo.Map(t.Columns, func(column *PostgresColumn, _ int) *PostgresColumn {
		if !slices.Contains(columnNames, column.Name) {
			return column
		}

		columnCopy := *column
		columnCopy.Default = sql.NullString{}

		return &columnCopy
	})

	return &tableCopy
}

// The current partition holds the state before the parent changes, and the copy holds the
// state after the parent statements. A change of the parent without ONLY reaches each
// partition, so DiffTables prints the parent statements first and the partition statements
// then give back the own attributes of the partition.
func (t *PostgresTable) withParentColumnChanges(
	desiredParent *PostgresTable,
	currentParent *PostgresTable,
	serverVersion int,
) *PostgresTable {
	tableCopy := *t
	tableCopy.Columns = nil

	for _, desiredParentColumn := range desiredParent.Columns {
		column, found := t.ColumnByName(desiredParentColumn.Name)
		if !found {
			continue
		}

		columnCopy := *column

		currentParentColumn, found := currentParent.ColumnByName(desiredParentColumn.Name)
		if !found || desiredParentColumn.NeedsRebuild(currentParentColumn, serverVersion) {
			columnCopy = *desiredParentColumn
			columnCopy.Comment = ""
			columnCopy.Inherited = true
			tableCopy.Columns = append(tableCopy.Columns, &columnCopy)

			continue
		}

		typeChanged := desiredParentColumn.Type != currentParentColumn.Type ||
			desiredParentColumn.Collation != currentParentColumn.Collation

		// DiffTable drops the default of the parent before a TYPE action and then sets the
		// desired default again. A TYPE action gives the storage mode of the new type.
		if desiredParentColumn.Default != currentParentColumn.Default ||
			(typeChanged && currentParentColumn.Default.Valid) {
			columnCopy.Default = desiredParentColumn.Default
		}

		if desiredParentColumn.NotNull != currentParentColumn.NotNull {
			columnCopy.NotNull = desiredParentColumn.NotNull
		}

		if typeChanged {
			columnCopy.Storage = ""
		}

		if desiredParentColumn.Storage != currentParentColumn.Storage ||
			(typeChanged && desiredParentColumn.Storage != "") {
			columnCopy.Storage = desiredParentColumn.Storage
		}

		if desiredParentColumn.StatisticsTarget != currentParentColumn.StatisticsTarget {
			columnCopy.StatisticsTarget = desiredParentColumn.StatisticsTarget
		}

		tableCopy.Columns = append(tableCopy.Columns, &columnCopy)
	}

	return &tableCopy
}

// CREATE TABLE INHERITS and CREATE TABLE PARTITION OF give each column of the parent the
// default, the NOT NULL flag, and the storage mode of the parent. The returned table holds
// that state, so DiffTable prints the attributes that the new child keeps for itself.
func (t *PostgresTable) creationStateOfChild(desiredTables []*PostgresTable) *PostgresTable {
	parents := lo.Filter(desiredTables, func(table *PostgresTable, _ int) bool {
		return slices.Contains(t.Inherits, table.Name) || table.Name == t.PartitionParent
	})

	tableCopy := *t

	tableCopy.Columns = lo.Map(t.Columns, func(column *PostgresColumn, _ int) *PostgresColumn {
		if !column.Inherited && !t.IsPartition() {
			return column
		}

		for _, parent := range parents {
			parentColumn, found := parent.ColumnByName(column.Name)
			if !found {
				continue
			}

			columnCopy := *parentColumn
			columnCopy.Comment = column.Comment
			columnCopy.StatisticsTarget = sql.NullInt64{}
			columnCopy.Inherited = true

			return &columnCopy
		}

		return column
	})

	return &tableCopy
}

func (t *PostgresTable) RebuiltColumnNames(other *PostgresTable, serverVersion int) []string {
	var names []string

	for _, desiredColumn := range t.Columns {
		currentColumn, found := other.ColumnByName(desiredColumn.Name)
		if found && desiredColumn.NeedsRebuild(currentColumn, serverVersion) {
			names = append(names, desiredColumn.Name)
		}
	}

	return names
}

// A DROP COLUMN statement removes the comment, the indexes, and the constraints of the
// column. PostgreSQL refuses it while a policy or a trigger uses the column, so the returned
// instructions drop them before the DROP COLUMN statement. DiffTable creates them again.
func (t *PostgresTable) withoutColumnDependents(
	columnNames []string,
	dependents []*PostgresColumnDependent,
) (*PostgresTable, []driversshared.Instruction) {
	isDependent := func(kind string, name string) bool {
		return slices.ContainsFunc(dependents, func(dependent *PostgresColumnDependent) bool {
			return dependent.Kind == kind && dependent.Name == name
		})
	}

	var dropInstructions []driversshared.Instruction

	tableCopy := *t

	tableCopy.Indexes = slices.DeleteFunc(slices.Clone(t.Indexes), func(index *PostgresIndex) bool {
		return isDependent("index", index.Name)
	})
	tableCopy.Constraints = slices.DeleteFunc(slices.Clone(t.Constraints), func(constraint *PostgresConstraint) bool {
		return isDependent("constraint", constraint.Name)
	})
	tableCopy.Policies = slices.DeleteFunc(slices.Clone(t.Policies), func(policy *PostgresPolicy) bool {
		if !isDependent("policy", policy.Name) {
			return false
		}

		dropInstructions = append(dropInstructions, policy.DropInstruction())

		return true
	})
	tableCopy.Triggers = slices.DeleteFunc(slices.Clone(t.Triggers), func(trigger *PostgresTrigger) bool {
		if !isDependent("trigger", trigger.Name) {
			return false
		}

		dropInstructions = append(dropInstructions, &PostgresDropTriggerInstruction{
			Name:      trigger.Name,
			TableName: t.Name,
		})

		return true
	})
	tableCopy.Columns = lo.Map(t.Columns, func(column *PostgresColumn, _ int) *PostgresColumn {
		if !slices.Contains(columnNames, column.Name) {
			return column
		}

		columnCopy := *column
		columnCopy.Comment = ""

		return &columnCopy
	})

	return &tableCopy, dropInstructions
}

func isTableDropped(name string, desiredTables []*PostgresTable) bool {
	_, found := lo.Find(desiredTables, func(table *PostgresTable) bool {
		return table.Name == name
	})

	return !found
}

// information_schema reports no partition key, no partition bound, and no replica
// identity, so the query reads pg_class.
//
// pg_inherits names the parent of a partition and the parent of a table of INHERITS, so
// relispartition separates the two. Without that test a table of INHERITS takes the
// statement of a partition, and that statement holds no bound.
func (d *PostgresDriver) GetTables(ctx context.Context, db *sql.DB) ([]*PostgresTable, error) {
	tables, err := driversshared.QueryAll(ctx, db, `
		SELECT
			c.relname,
			coalesce(pg_get_partkeydef(c.oid), ''),
			CASE WHEN c.relispartition THEN coalesce((
				SELECT parent.relname
				FROM pg_inherits i
				JOIN pg_class parent ON parent.oid = i.inhparent
				WHERE i.inhrelid = c.oid
			), '') ELSE '' END,
			coalesce(pg_get_expr(c.relpartbound, c.oid), ''),
			coalesce((
				SELECT array_to_string(array_agg(parent.relname ORDER BY i.inhseqno), ',')
				FROM pg_inherits i
				JOIN pg_class parent ON parent.oid = i.inhparent
				WHERE i.inhrelid = c.oid AND NOT c.relispartition
			), ''),
			coalesce(obj_description(c.oid, 'pg_class'), ''),
			c.relrowsecurity,
			c.relforcerowsecurity,
			c.relpersistence = 'u',
			coalesce(array_to_string(c.reloptions, ','), ''),
			CASE c.relreplident
				WHEN 'd' THEN 'DEFAULT'
				WHEN 'n' THEN 'NOTHING'
				WHEN 'f' THEN 'FULL'
				WHEN 'i' THEN 'USING INDEX'
				ELSE ''
			END,
			coalesce((
				SELECT replica_index.relname
				FROM pg_index i
				JOIN pg_class replica_index ON replica_index.oid = i.indexrelid
				WHERE i.indrelid = c.oid AND i.indisreplident
			), ''),
			EXISTS (
				SELECT 1
				FROM pg_inherits i
				JOIN pg_class child ON child.oid = i.inhrelid
				WHERE i.inhparent = c.oid AND NOT child.relispartition
			)
		FROM pg_class c
		WHERE c.relnamespace = quote_ident(current_schema())::regnamespace
		AND c.relkind IN ('r', 'p')
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			WHERE d.objid = c.oid AND d.deptype = 'e'
		)
		AND c.relname <> $1
		ORDER BY c.relname
	`, func(rows *sql.Rows) (*PostgresTable, error) {
		table := &PostgresTable{}

		var inherits, storageParameters string

		err := rows.Scan(&table.Name, &table.PartitionKey, &table.PartitionParent,
			&table.PartitionBound, &inherits, &table.Comment, &table.RowLevelSecurity,
			&table.ForceRowLevelSecurity, &table.Unlogged, &storageParameters,
			&table.ReplicaIdentity, &table.ReplicaIdentityIndex, &table.HasInheritanceChildren)
		if err != nil {
			return nil, err
		}

		if inherits != "" {
			table.Inherits = strings.Split(inherits, ",")
		}

		if storageParameters != "" {
			table.StorageParameters = strings.Split(storageParameters, ",")
		}

		return table, nil
	}, driversshared.MigrationHistoryTableName)
	if err != nil {
		return nil, err
	}

	tables = slices.DeleteFunc(tables, func(table *PostgresTable) bool {
		return slices.Contains(d.IgnoreTables, table.Name)
	})

	for _, table := range tables {
		err = d.getTableDetails(ctx, db, table)
		if err != nil {
			return nil, err
		}
	}

	return sortTablesByDependency(tables), nil
}

func (d *PostgresDriver) getTableDetails(ctx context.Context, db *sql.DB, table *PostgresTable) error {
	err := getTableColumns(ctx, db, table)
	if err != nil {
		return err
	}

	err = getTableConstraints(ctx, db, table)
	if err != nil {
		return err
	}

	err = getTableIndexes(ctx, db, table)
	if err != nil {
		return err
	}

	err = getTableTriggers(ctx, db, table)
	if err != nil {
		return err
	}

	table.Rules, err = d.GetTableRules(ctx, db, table.Name)
	if err != nil {
		return err
	}

	table.Policies, err = d.GetTablePolicies(ctx, db, table.Name)

	return err
}

func foreignKeyName(tableName string, constraintName string) string {
	return tableName + "." + constraintName
}

// PostgreSQL refuses to drop a key while a foreign key uses it. A foreign key also blocks the
// DROP COLUMN statement of a rebuilt column. Thus the drops come before every table change,
// and the additions come after every table change. A foreign key that names a table with a
// changed key goes through a drop and an addition too, because it can use the dropped key.
// A partition takes the foreign keys of its parent.
func diffForeignKeys(
	desiredTables []*PostgresTable,
	currentTables []*PostgresTable,
	keptCurrentTables map[string]*PostgresTable,
	recreatedForeignKeys map[string]bool,
) ([]driversshared.Instruction, []driversshared.Instruction) {
	keyChangedTables := map[string]bool{}

	for _, desiredTable := range desiredTables {
		currentTable, found := keptCurrentTables[desiredTable.Name]
		if !found {
			continue
		}

		for _, currentConstraint := range currentTable.Constraints {
			if currentConstraint.Type != "p" && currentConstraint.Type != "u" {
				continue
			}

			desiredConstraint, found := desiredTable.ConstraintByName(currentConstraint.Name)
			if !found || desiredConstraint.Def != currentConstraint.Def {
				keyChangedTables[currentTable.Name] = true
			}
		}

		// A foreign key can also use a unique index that is not a constraint.
		for _, currentIndex := range currentTable.Indexes {
			if !strings.HasPrefix(currentIndex.Def, "CREATE UNIQUE INDEX") {
				continue
			}

			desiredIndex, found := desiredTable.IndexByName(currentIndex.Name)
			if !found || desiredIndex.Def != currentIndex.Def {
				keyChangedTables[currentTable.Name] = true
			}
		}
	}

	foreignKeys := func(table *PostgresTable) []*PostgresConstraint {
		return lo.Filter(table.Constraints, func(constraint *PostgresConstraint, _ int) bool {
			return constraint.IsForeignKey() && !constraint.Inherited
		})
	}

	mustRecreate := func(tableName string, constraint *PostgresConstraint) bool {
		return keyChangedTables[constraint.ReferencedTable] ||
			recreatedForeignKeys[foreignKeyName(tableName, constraint.Name)]
	}

	var drops []driversshared.Instruction
	var additions []driversshared.Instruction

	for _, currentTable := range currentTables {
		if currentTable.IsPartition() {
			continue
		}

		var desiredForeignKeys []*PostgresConstraint

		desiredTable, found := lo.Find(desiredTables, func(table *PostgresTable) bool {
			return table.Name == currentTable.Name
		})
		if found {
			currentTable = keptCurrentTables[currentTable.Name]
			desiredForeignKeys = foreignKeys(desiredTable)
		} else {
			desiredForeignKeys = lo.Filter(foreignKeys(currentTable), func(constraint *PostgresConstraint, _ int) bool {
				return !mustRecreate(currentTable.Name, constraint)
			})
		}

		alterTable := func(action driversshared.AlterTableAction) driversshared.Instruction {
			return &PostgresAlterTableInstruction{
				Name:    currentTable.Name,
				Actions: []driversshared.AlterTableAction{action},
			}
		}

		tableAdditions, tableDrops := driversshared.ReplaceByKey(desiredForeignKeys, foreignKeys(currentTable),
			func(constraint *PostgresConstraint) string {
				return constraint.Name
			},
			func(desired *PostgresConstraint, current *PostgresConstraint) bool {
				return desired.Def == current.Def && !mustRecreate(currentTable.Name, current)
			},
			func(constraint *PostgresConstraint) driversshared.Instruction {
				return alterTable(&PostgresAddConstraintAction{
					Constraint: constraint,
				})
			},
			func(constraint *PostgresConstraint) driversshared.Instruction {
				return alterTable(&PostgresDropConstraintAction{
					ConstraintName: constraint.Name,
				})
			})

		if found {
			additions = append(additions, tableAdditions...)
		}

		drops = append(drops, tableDrops...)
	}

	return drops, additions
}
