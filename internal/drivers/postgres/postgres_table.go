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

func identityOptionDefaults(sourceOptions string, columnType string) string {
	maxValue := "9223372036854775807"

	switch columnType {
	case "smallint":
		maxValue = "32767"
	case "integer":
		maxValue = "2147483647"
	}

	var defaults []string

	for _, option := range splitSequenceOptions(sourceOptions) {
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

func (t *PostgresTable) DiffTable(other *PostgresTable, hasAutomaticCast AutomaticCastLookup) ([]driversshared.Instruction, error) {
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

	for _, targetColumn := range t.Columns {
		sourceColumn, found := other.ColumnByName(targetColumn.Name)
		if !found {
			instructions = append(instructions,
				alterTable(&PostgresAddColumnAction{Column: targetColumn}))
			instructions = append(instructions, targetColumn.StorageInstructions(t.Name)...)
			instructions = append(instructions, targetColumn.StatisticsInstructions(t.Name)...)

			continue
		}

		if targetColumn.HasEqualAttributes(sourceColumn) {
			continue
		}

		// PostgreSQL holds no action that changes the expression of a generated column, and the
		// column keeps no data of its own.
		if targetColumn.GeneratedExpression != sourceColumn.GeneratedExpression {
			instructions = append(instructions, &PostgresAlterTableInstruction{
				Name: t.Name,
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: sourceColumn.Name},
					&PostgresAddColumnAction{Column: targetColumn},
				},
			})

			continue
		}

		// PostgreSQL refuses to remove the NOT NULL flag of an identity column, so this
		// action comes before the NOT NULL block below.
		if sourceColumn.Identity != "" && targetColumn.Identity == "" {
			instructions = append(instructions,
				alterTable(&PostgresDropIdentityAction{ColumnName: targetColumn.Name}))
		}

		typeChanged := targetColumn.Type != sourceColumn.Type ||
			targetColumn.Collation != sourceColumn.Collation

		// PostgreSQL changes a collation through the TYPE action.
		if typeChanged {
			usingCast, err := columnUsingClause(targetColumn, sourceColumn, hasAutomaticCast)
			if err != nil {
				return nil, err
			}

			instructions = append(instructions, alterTable(&PostgresAlterColumnTypeAction{
				ColumnName: targetColumn.Name,
				DataType:   targetColumn.Type,
				Collation:  targetColumn.Collation,
				UsingCast:  usingCast,
			}))
		}

		if targetColumn.NotNull != sourceColumn.NotNull {
			if targetColumn.NotNull {
				instructions = append(instructions,
					alterTable(&PostgresSetNotNullAction{ColumnName: targetColumn.Name}))
			} else {
				// PostgreSQL refuses to remove the flag of a column of the primary key,
				// so this action waits for the constraint blocks below.
				dropNotNullInstructions = append(dropNotNullInstructions,
					alterTable(&PostgresDropNotNullAction{ColumnName: targetColumn.Name}))
			}
		}

		if targetColumn.Default != sourceColumn.Default {
			if targetColumn.Default.Valid {
				instructions = append(instructions, alterTable(&PostgresSetDefaultAction{
					ColumnName: targetColumn.Name,
					Expression: targetColumn.Default.String,
				}))
			} else {
				instructions = append(instructions,
					alterTable(&PostgresDropDefaultAction{ColumnName: targetColumn.Name}))
			}
		}

		// The serial word owns a sequence and gives the default, and PostgreSQL holds no
		// single action for it.
		if targetColumn.Serial != "" && sourceColumn.Serial == "" {
			sequenceName := t.Name + "_" + targetColumn.Name + "_seq"

			instructions = append(instructions, &PostgresCreateOwnedSequenceInstruction{
				Name:       sequenceName,
				TableName:  t.Name,
				ColumnName: targetColumn.Name,
			})
			instructions = append(instructions, alterTable(&PostgresSetDefaultAction{
				ColumnName: targetColumn.Name,
				Expression: fmt.Sprintf("nextval('%s'::regclass)", driversshared.QuoteIdentifier(sequenceName)),
			}))
		}

		if targetColumn.Serial == "" && sourceColumn.Serial != "" {
			instructions = append(instructions,
				alterTable(&PostgresDropDefaultAction{ColumnName: targetColumn.Name}))
			instructions = append(instructions, &PostgresDropSequenceInstruction{
				Name: sourceColumn.SerialSequenceName,
			})
		}

		// A TYPE action gives the column the storage mode of the new type, so this action always
		// follows it. The mode DEFAULT gives the column the mode of its type again.
		if targetColumn.Storage != sourceColumn.Storage ||
			(typeChanged && targetColumn.Storage != "") {
			storage := targetColumn.Storage
			if storage == "" {
				storage = "DEFAULT"
			}

			instructions = append(instructions, alterTable(&PostgresSetStorageAction{
				ColumnName: targetColumn.Name,
				Storage:    storage,
			}))
		}

		// A TYPE action keeps the statistics source, and the value -1 gives the default source.
		if targetColumn.StatisticsTarget != sourceColumn.StatisticsTarget {
			source := int64(-1)
			if targetColumn.StatisticsTarget.Valid {
				source = targetColumn.StatisticsTarget.Int64
			}

			instructions = append(instructions, alterTable(&PostgresSetStatisticsAction{
				ColumnName: targetColumn.Name,
				Source:     source,
			}))
		}

		// The options of an identity column live in its sequence.
		if targetColumn.Identity != "" && sourceColumn.Identity != "" &&
			targetColumn.IdentityOptions != sourceColumn.IdentityOptions {
			options := targetColumn.IdentityOptions

			// The read builds the options from the values that differ from the defaults,
			// so an empty target resets each option that the source names.
			if options == "" {
				options = identityOptionDefaults(sourceColumn.IdentityOptions, targetColumn.Type)
			}

			instructions = append(instructions, alterTable(&PostgresSetIdentityOptionsAction{
				ColumnName: targetColumn.Name,
				Options:    options,
			}))
		}

		// PostgreSQL refuses to add an identity to a column that accepts a null value, so
		// these two actions come after the NOT NULL block above.
		if targetColumn.Identity != "" && targetColumn.Identity != sourceColumn.Identity {
			if sourceColumn.Identity == "" {
				instructions = append(instructions, alterTable(&PostgresAddIdentityAction{
					ColumnName: targetColumn.Name,
					Identity:   targetColumn.Identity,
				}))
			} else {
				instructions = append(instructions, alterTable(&PostgresSetIdentityAction{
					ColumnName: targetColumn.Name,
					Identity:   targetColumn.Identity,
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
	for _, targetConstraint := range t.Constraints {
		sourceConstraint, found := other.ConstraintByName(targetConstraint.Name)
		if !found {
			instructions = append(instructions,
				alterTable(&PostgresAddConstraintAction{Constraint: targetConstraint}))

			continue
		}

		if targetConstraint.Def != sourceConstraint.Def {
			instructions = append(instructions,
				alterTable(&PostgresDropConstraintAction{ConstraintName: sourceConstraint.Name}),
				alterTable(&PostgresAddConstraintAction{Constraint: targetConstraint}))
		}
	}

	for _, sourceConstraint := range other.Constraints {
		_, found := t.ConstraintByName(sourceConstraint.Name)
		if !found {
			instructions = append(instructions,
				alterTable(&PostgresDropConstraintAction{ConstraintName: sourceConstraint.Name}))
		}
	}

	instructions = append(instructions, dropNotNullInstructions...)

	instructions = append(instructions, diffIndexAdditions(t, other)...)

	// The mode USING INDEX names an index, so this block comes after the index additions
	// above. It comes before the index removals below, because PostgreSQL refuses to drop
	// the index that the replica identity of the source holds.
	if t.ReplicaIdentity != other.ReplicaIdentity ||
		t.ReplicaIdentityIndex != other.ReplicaIdentityIndex {
		instructions = append(instructions, alterTable(&PostgresReplicaIdentityAction{
			Mode:      t.ReplicaIdentity,
			IndexName: t.ReplicaIdentityIndex,
		}))
	}

	instructions = append(instructions, diffIndexRemovals(t, other)...)

	for _, sourceColumn := range other.Columns {
		_, found := t.ColumnByName(sourceColumn.Name)
		if !found {
			instructions = append(instructions,
				alterTable(&driversshared.SQLDropColumnAction{ColumnName: sourceColumn.Name}))
		}
	}

	instructions = append(instructions, diffTriggers(t, other)...)

	return instructions, nil
}

func diffComments(targetTable *PostgresTable, sourceTable *PostgresTable) []driversshared.Instruction {
	var instructions []driversshared.Instruction

	if targetTable.Comment != sourceTable.Comment {
		instructions = append(instructions, &PostgresCommentOnTableInstruction{
			Name: targetTable.Name,
			Text: targetTable.Comment,
		})
	}

	for _, targetColumn := range targetTable.Columns {
		sourceColumn, found := sourceTable.ColumnByName(targetColumn.Name)
		if !found {
			continue
		}

		if targetColumn.Comment != sourceColumn.Comment {
			instructions = append(instructions, &PostgresCommentOnColumnInstruction{
				TableName:  targetTable.Name,
				ColumnName: targetColumn.Name,
				Text:       targetColumn.Comment,
			})
		}
	}

	return instructions
}

func diffIndexAdditions(targetTable *PostgresTable, sourceTable *PostgresTable) []driversshared.Instruction {
	var instructions []driversshared.Instruction

	for _, targetIndex := range targetTable.Indexes {
		sourceIndex, found := sourceTable.IndexByName(targetIndex.Name)
		if !found {
			instructions = append(instructions, targetIndex.Instructions()...)
			continue
		}

		if targetIndex.Def != sourceIndex.Def {
			instructions = append(instructions, &driversshared.SQLDropIndexInstruction{Name: sourceIndex.Name})
			instructions = append(instructions, targetIndex.Instructions()...)
			continue
		}

		if targetIndex.Comment != sourceIndex.Comment {
			instructions = append(instructions, &PostgresCommentOnIndexInstruction{
				Name: targetIndex.Name,
				Text: targetIndex.Comment,
			})
		}
	}

	return instructions
}

func diffIndexRemovals(targetTable *PostgresTable, sourceTable *PostgresTable) []driversshared.Instruction {
	var instructions []driversshared.Instruction

	for _, sourceIndex := range sourceTable.Indexes {
		_, found := targetTable.IndexByName(sourceIndex.Name)
		if !found {
			instructions = append(instructions, &driversshared.SQLDropIndexInstruction{Name: sourceIndex.Name})
		}
	}

	return instructions
}

func diffTriggers(targetTable *PostgresTable, sourceTable *PostgresTable) []driversshared.Instruction {
	var instructions []driversshared.Instruction

	for _, targetTrigger := range targetTable.Triggers {
		sourceTrigger, found := sourceTable.TriggerByName(targetTrigger.Name)
		if !found {
			instructions = append(instructions, targetTrigger.CreateInstruction())
			instructions = append(instructions, targetTrigger.EnableInstructions(targetTable.Name)...)

			continue
		}

		// A new trigger takes the mode ENABLE, so the mode of the target comes after the
		// CREATE TRIGGER statement below.
		if targetTrigger.Def != sourceTrigger.Def {
			instructions = append(instructions,
				&PostgresDropTriggerInstruction{
					Name:      sourceTrigger.Name,
					TableName: targetTable.Name,
				},
				targetTrigger.CreateInstruction())
			instructions = append(instructions, targetTrigger.EnableInstructions(targetTable.Name)...)

			continue
		}

		if targetTrigger.EnableMode != sourceTrigger.EnableMode {
			instructions = append(instructions, &PostgresAlterTableInstruction{
				Name: targetTable.Name,
				Actions: []driversshared.AlterTableAction{&PostgresTriggerEnableAction{
					Mode:        targetTrigger.EnableMode,
					TriggerName: targetTrigger.Name,
				}},
			})
		}
	}

	for _, sourceTrigger := range sourceTable.Triggers {
		_, found := targetTable.TriggerByName(sourceTrigger.Name)
		if !found {
			instructions = append(instructions,
				&PostgresDropTriggerInstruction{
					Name:      sourceTrigger.Name,
					TableName: targetTable.Name,
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

	for _, targetRule := range t.Rules {
		sourceRule, found := other.RuleByName(targetRule.Name)
		if !found {
			instructions = append(instructions, targetRule.CreateInstruction())
			continue
		}

		// PostgreSQL holds no ALTER action for a rule. The DROP and CREATE pair stays one modification.
		if targetRule.Def != sourceRule.Def {
			instructions = append(instructions,
				sourceRule.DropInstruction(), targetRule.CreateInstruction())
		}
	}

	for _, sourceRule := range other.Rules {
		_, found := t.RuleByName(sourceRule.Name)
		if !found {
			instructions = append(instructions, sourceRule.DropInstruction())
		}
	}

	return instructions
}

func (t *PostgresTable) RuleInstructions() []driversshared.Instruction {
	var instructions []driversshared.Instruction

	for _, rule := range t.Rules {
		instructions = append(instructions, rule.CreateInstruction())
	}

	return instructions
}

func diffStorageParameters(targetTable *PostgresTable, sourceTable *PostgresTable) []driversshared.Instruction {
	if slices.Equal(targetTable.StorageParameters, sourceTable.StorageParameters) {
		return nil
	}

	var instructions []driversshared.Instruction

	if len(targetTable.StorageParameters) > 0 {
		instructions = append(instructions, &PostgresAlterTableInstruction{
			Name: targetTable.Name,
			Actions: []driversshared.AlterTableAction{
				&PostgresSetStorageParametersAction{Parameters: targetTable.StorageParameters},
			},
		})
	}

	var removed []string

	for _, parameter := range sourceTable.StorageParameters {
		name := storageParameterName(parameter)

		held := slices.ContainsFunc(targetTable.StorageParameters,
			func(targetParameter string) bool {
				return storageParameterName(targetParameter) == name
			})
		if !held {
			removed = append(removed, name)
		}
	}

	if len(removed) > 0 {
		instructions = append(instructions, &PostgresAlterTableInstruction{
			Name:    targetTable.Name,
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

func diffRowLevelSecurity(targetTable *PostgresTable, sourceTable *PostgresTable) []driversshared.Instruction {
	var instructions []driversshared.Instruction

	alterTable := func(mode string) driversshared.Instruction {
		return &PostgresAlterTableInstruction{
			Name:    targetTable.Name,
			Actions: []driversshared.AlterTableAction{&PostgresRowLevelSecurityAction{Mode: mode}},
		}
	}

	if targetTable.RowLevelSecurity != sourceTable.RowLevelSecurity {
		if targetTable.RowLevelSecurity {
			instructions = append(instructions, alterTable("ENABLE"))
		} else {
			instructions = append(instructions, alterTable("DISABLE"))
		}
	}

	if targetTable.ForceRowLevelSecurity != sourceTable.ForceRowLevelSecurity {
		if targetTable.ForceRowLevelSecurity {
			instructions = append(instructions, alterTable("FORCE"))
		} else {
			instructions = append(instructions, alterTable("NO FORCE"))
		}
	}

	for _, targetPolicy := range targetTable.Policies {
		sourcePolicy, found := sourceTable.PolicyByName(targetPolicy.Name)
		if !found {
			instructions = append(instructions, targetPolicy.CreateInstruction())
			continue
		}

		if !targetPolicy.Equal(sourcePolicy) {
			instructions = append(instructions,
				sourcePolicy.DropInstruction(), targetPolicy.CreateInstruction())
		}
	}

	for _, sourcePolicy := range sourceTable.Policies {
		_, found := targetTable.PolicyByName(sourcePolicy.Name)
		if !found {
			instructions = append(instructions, sourcePolicy.DropInstruction())
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

func (d *PostgresDriver) DiffTables(ctx context.Context) (*driversshared.SectionDiff, error) {
	var earlyRemovals []driversshared.Instruction
	var additions []driversshared.Instruction
	var removals []driversshared.Instruction

	targetTables, err := d.GetTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceTables, err := d.GetTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	hasAutomaticCast := func(oldType string, newType string) (bool, error) {
		return d.HasAutomaticCast(ctx, d.SourceDatabaseConnection, oldType, newType)
	}

	// The action of a rule can name a second table, so every rule comes after every table.
	var ruleInstructions []driversshared.Instruction

	// Two tables can name each other, so every foreign key comes after every table.
	var foreignKeyInstructions []driversshared.Instruction

	for _, targetTable := range targetTables {
		sourceTable, found := lo.Find(sourceTables, func(table *PostgresTable) bool {
			return table.Name == targetTable.Name
		})
		if !found {
			additions = append(additions, targetTable.Instructions()...)
			foreignKeyInstructions = append(foreignKeyInstructions,
				targetTable.ForeignKeyInstructions()...)
			ruleInstructions = append(ruleInstructions, targetTable.RuleInstructions()...)

			continue
		}

		subInstructions, err := targetTable.DiffTable(sourceTable, hasAutomaticCast)
		if err != nil {
			return nil, err
		}

		additions = append(additions, subInstructions...)
		ruleInstructions = append(ruleInstructions, targetTable.DiffRules(sourceTable)...)

		// PostgreSQL attaches an own index of a partition to a new index of the parent that
		// matches it, and then refuses to drop it. So these removals come before every addition.
		if targetTable.IsPartition() {
			earlyRemovals = append(earlyRemovals, diffIndexRemovals(targetTable, sourceTable)...)
		}
	}

	additions = append(additions, foreignKeyInstructions...)
	additions = append(additions, ruleInstructions...)

	// The reverse order gives each DROP TABLE statement before the table that it names.
	for _, sourceTable := range slices.Backward(sourceTables) {
		_, found := lo.Find(targetTables, func(table *PostgresTable) bool {
			return table.Name == sourceTable.Name
		})
		if found {
			continue
		}

		// A DROP TABLE statement of a parent removes every partition of it.
		if sourceTable.IsPartition() && isTableDropped(sourceTable.PartitionParent, targetTables) {
			continue
		}

		removals = append(removals, &driversshared.SQLDropTableInstruction{Name: sourceTable.Name})
	}

	return &driversshared.SectionDiff{
		EarlyRemovals: earlyRemovals,
		Additions:     additions,
		Removals:      removals,
	}, nil
}

func isTableDropped(name string, targetTables []*PostgresTable) bool {
	_, found := lo.Find(targetTables, func(table *PostgresTable) bool {
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
	tableRows, err := db.QueryContext(ctx, `
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
	`, driversshared.MigrationHistoryTableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = tableRows.Close() }()

	var tables []*PostgresTable

	for tableRows.Next() {
		var tableName, partitionKey, partitionParent, partitionBound, inherits, comment string
		var rowLevelSecurity, forceRowLevelSecurity, unlogged bool
		var storageParameters, replicaIdentity, replicaIdentityIndex string

		err := tableRows.Scan(&tableName, &partitionKey, &partitionParent, &partitionBound,
			&inherits, &comment, &rowLevelSecurity, &forceRowLevelSecurity, &unlogged,
			&storageParameters, &replicaIdentity, &replicaIdentityIndex)
		if err != nil {
			return nil, err
		}

		if slices.Contains(d.IgnoreTables, tableName) {
			continue
		}

		table, err := d.GetTable(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		table.PartitionKey = partitionKey
		table.PartitionParent = partitionParent
		table.PartitionBound = partitionBound
		table.Comment = comment

		if inherits != "" {
			table.Inherits = strings.Split(inherits, ",")
		}
		table.Unlogged = unlogged

		if storageParameters != "" {
			table.StorageParameters = strings.Split(storageParameters, ",")
		}
		table.ReplicaIdentity = replicaIdentity
		table.ReplicaIdentityIndex = replicaIdentityIndex
		table.RowLevelSecurity = rowLevelSecurity
		table.ForceRowLevelSecurity = forceRowLevelSecurity

		policies, err := d.GetTablePolicies(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		table.Policies = policies

		tables = append(tables, table)
	}

	err = tableRows.Err()
	if err != nil {
		return nil, err
	}

	return sortTablesByDependency(tables), nil
}

func (d *PostgresDriver) GetTable(ctx context.Context, db *sql.DB, tableName string) (*PostgresTable, error) {
	table := &PostgresTable{Name: tableName}

	err := getTableColumns(ctx, db, table)
	if err != nil {
		return nil, err
	}

	err = getTableConstraints(ctx, db, table)
	if err != nil {
		return nil, err
	}

	err = getTableIndexes(ctx, db, table)
	if err != nil {
		return nil, err
	}

	err = getTableTriggers(ctx, db, table)
	if err != nil {
		return nil, err
	}

	rules, err := d.GetTableRules(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	table.Rules = rules

	return table, nil
}
