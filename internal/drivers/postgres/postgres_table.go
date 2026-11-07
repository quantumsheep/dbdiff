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

	for _, policy := range t.Policies {
		instructions = append(instructions, policy.CreateInstruction())
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

func (t *PostgresTable) PolicyByName(name string) (*PostgresPolicy, bool) {
	for _, policy := range t.Policies {
		if policy.Name == name {
			return policy, true
		}
	}

	return nil, false
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
	// The diff does not compare the own constraints of a partition.
	// DiffTables prints the removals of the own indexes of a partition.
	// A new bound or a new parent needs a detach and an attach, because PostgreSQL
	// holds no action that changes a bound in place.
	if t.IsPartition() {
		if t.PartitionParent != other.PartitionParent || t.PartitionBound != other.PartitionBound {
			instructions = append(instructions,
				&PostgresDetachPartitionInstruction{
					ParentName:    other.PartitionParent,
					PartitionName: t.Name,
				},
				&PostgresAttachPartitionInstruction{
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

		if desiredColumn.NotNull != currentColumn.NotNull {
			if desiredColumn.NotNull {
				instructions = append(instructions,
					alterTable(&PostgresSetNotNullAction{ColumnName: desiredColumn.Name}))
			} else {
				// PostgreSQL refuses to remove the flag of a column of the primary key,
				// so this action waits for the constraint blocks below.
				dropNotNullInstructions = append(dropNotNullInstructions,
					alterTable(&PostgresDropNotNullAction{ColumnName: desiredColumn.Name}))
			}
		}

		if desiredColumn.Default != currentColumn.Default {
			if desiredColumn.Default.Valid {
				instructions = append(instructions, alterTable(&PostgresSetDefaultAction{
					ColumnName: desiredColumn.Name,
					Expression: desiredColumn.Default.String,
				}))
			} else {
				instructions = append(instructions,
					alterTable(&PostgresDropDefaultAction{ColumnName: desiredColumn.Name}))
			}
		}

		// The serial word owns a sequence and gives the default, and PostgreSQL holds no
		// single action for it.
		if desiredColumn.Serial != "" && currentColumn.Serial == "" {
			sequenceName := t.Name + "_" + desiredColumn.Name + "_seq"

			instructions = append(instructions, &PostgresCreateOwnedSequenceInstruction{
				Name:       sequenceName,
				DataType:   desiredColumn.Type,
				TableName:  t.Name,
				ColumnName: desiredColumn.Name,
			})
			instructions = append(instructions, alterTable(&PostgresSetDefaultAction{
				ColumnName: desiredColumn.Name,
				Expression: fmt.Sprintf("nextval('%s'::regclass)", driversshared.QuoteIdentifier(sequenceName)),
			}))
			instructions = append(instructions, &PostgresRestartOwnedSequenceInstruction{
				TableName:  t.Name,
				ColumnName: desiredColumn.Name,
			})
		}

		if desiredColumn.Serial == "" && currentColumn.Serial != "" {
			instructions = append(instructions,
				alterTable(&PostgresDropDefaultAction{ColumnName: desiredColumn.Name}))
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

			instructions = append(instructions, alterTable(&PostgresSetStorageAction{
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

			instructions = append(instructions, alterTable(&PostgresSetStatisticsAction{
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
	// these two blocks come before the column removals below.
	for _, desiredConstraint := range t.Constraints {
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

	for _, currentConstraint := range other.Constraints {
		_, found := t.ConstraintByName(currentConstraint.Name)
		if !found {
			instructions = append(instructions,
				alterTable(&PostgresDropConstraintAction{ConstraintName: currentConstraint.Name}))
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
		if !found {
			instructions = append(instructions,
				alterTable(&driversshared.SQLDropColumnAction{ColumnName: currentColumn.Name}))
		}
	}

	instructions = append(instructions, diffTriggers(t, other)...)

	return instructions, nil
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

	for _, desiredPolicy := range desiredTable.Policies {
		currentPolicy, found := currentTable.PolicyByName(desiredPolicy.Name)
		if !found {
			instructions = append(instructions, desiredPolicy.CreateInstruction())
			continue
		}

		if !desiredPolicy.Equal(currentPolicy) {
			instructions = append(instructions,
				currentPolicy.DropInstruction(), desiredPolicy.CreateInstruction())
		}
	}

	for _, currentPolicy := range currentTable.Policies {
		_, found := desiredTable.PolicyByName(currentPolicy.Name)
		if !found {
			instructions = append(instructions, currentPolicy.DropInstruction())
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
		if !constraint.IsForeignKey() {
			constraints = append(constraints, constraint)
		}
	}

	return &PostgresCreateTableInstruction{
		Name:         t.Name,
		Columns:      t.Columns,
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

	// The action of a rule can name a second table, so every rule comes after every table.
	var ruleInstructions []driversshared.Instruction

	// Two tables can name each other, so every foreign key comes after every table.
	var foreignKeyInstructions []driversshared.Instruction

	for _, desiredTable := range desiredTables {
		currentTable, found := lo.Find(currentTables, func(table *PostgresTable) bool {
			return table.Name == desiredTable.Name
		})
		if !found {
			additions = append(additions, desiredTable.Instructions()...)
			foreignKeyInstructions = append(foreignKeyInstructions,
				desiredTable.ForeignKeyInstructions()...)
			ruleInstructions = append(ruleInstructions, desiredTable.RuleInstructions()...)

			continue
		}

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

		// PostgreSQL attaches an own index of a partition to a new index of the parent that
		// matches it, and then refuses to drop it. So these removals come before every addition.
		if desiredTable.IsPartition() {
			earlyRemovals = append(earlyRemovals, diffIndexRemovals(desiredTable, currentTable)...)
		}
	}

	additions = append(additions, foreignKeyInstructions...)
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
			), '')
		FROM pg_class c
		WHERE c.relnamespace = current_schema()::regnamespace
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
			&table.ReplicaIdentity, &table.ReplicaIdentityIndex)
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
