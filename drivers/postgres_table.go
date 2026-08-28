package drivers

import (
	"fmt"
	"slices"
	"strings"
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
func (t *PostgresTable) RowLevelSecurityInstructions() []Instruction {
	var instructions []Instruction

	if t.RowLevelSecurity {
		instructions = append(instructions, &PostgresAlterTableInstruction{
			Name:    t.Name,
			Actions: []AlterTableAction{&PostgresRowLevelSecurityAction{Mode: "ENABLE"}},
		})
	}

	if t.ForceRowLevelSecurity {
		instructions = append(instructions, &PostgresAlterTableInstruction{
			Name:    t.Name,
			Actions: []AlterTableAction{&PostgresRowLevelSecurityAction{Mode: "FORCE"}},
		})
	}

	for _, policy := range t.Policies {
		instructions = append(instructions, policy.CreateInstruction())
	}

	return instructions
}

// PostgreSQL accepts no replica identity in a CREATE TABLE statement. The mode DEFAULT
// needs no statement, because every new table holds that mode.
func (t *PostgresTable) ReplicaIdentityInstructions() []Instruction {
	if t.ReplicaIdentity == "" || t.ReplicaIdentity == "DEFAULT" {
		return nil
	}

	return []Instruction{&PostgresAlterTableInstruction{
		Name: t.Name,
		Actions: []AlterTableAction{&PostgresReplicaIdentityAction{
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
func (t *PostgresTable) CommentInstructions() []Instruction {
	var instructions []Instruction

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

func (t *PostgresTable) DiffTable(other *PostgresTable, hasAutomaticCast AutomaticCastLookup) ([]Instruction, error) {
	var instructions []Instruction

	// A partition holds the columns, the constraints, and the indexes of its parent.
	// A partition takes the columns, the constraints, and the indexes of its parent.
	// A new bound or a new parent needs a detach and an attach, because PostgreSQL
	// holds no action that changes a bound in place.
	if t.IsPartition() && other.IsPartition() {
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

		return instructions, nil
	}

	if t.IsPartition() || other.IsPartition() {
		return nil, nil
	}

	alterTable := func(action AlterTableAction) Instruction {
		return &PostgresAlterTableInstruction{
			Name:    t.Name,
			Actions: []AlterTableAction{action},
		}
	}

	var dropNotNullInstructions []Instruction

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
				Actions: []AlterTableAction{
					&SQLDropColumnAction{ColumnName: sourceColumn.Name},
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
				Expression: fmt.Sprintf("nextval('%s'::regclass)", QuoteIdentifier(sequenceName)),
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

	if t.Comment != other.Comment {
		instructions = append(instructions, &PostgresCommentOnTableInstruction{
			Name: t.Name,
			Text: t.Comment,
		})
	}

	for _, targetColumn := range t.Columns {
		sourceColumn, found := other.ColumnByName(targetColumn.Name)
		if !found {
			continue
		}

		if targetColumn.Comment != sourceColumn.Comment {
			instructions = append(instructions, &PostgresCommentOnColumnInstruction{
				TableName:  t.Name,
				ColumnName: targetColumn.Name,
				Text:       targetColumn.Comment,
			})
		}
	}

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

	for _, targetIndex := range t.Indexes {
		sourceIndex, found := other.IndexByName(targetIndex.Name)
		if !found {
			instructions = append(instructions, targetIndex.CreateInstruction())
			continue
		}

		if targetIndex.Def != sourceIndex.Def {
			instructions = append(instructions,
				&SQLDropIndexInstruction{Name: sourceIndex.Name},
				targetIndex.CreateInstruction())
		}
	}

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

	for _, sourceIndex := range other.Indexes {
		_, found := t.IndexByName(sourceIndex.Name)
		if !found {
			instructions = append(instructions, &SQLDropIndexInstruction{Name: sourceIndex.Name})
		}
	}

	for _, sourceColumn := range other.Columns {
		_, found := t.ColumnByName(sourceColumn.Name)
		if !found {
			instructions = append(instructions,
				alterTable(&SQLDropColumnAction{ColumnName: sourceColumn.Name}))
		}
	}

	for _, targetTrigger := range t.Triggers {
		sourceTrigger, found := other.TriggerByName(targetTrigger.Name)
		if !found {
			instructions = append(instructions, targetTrigger.CreateInstruction())
			instructions = append(instructions, targetTrigger.EnableInstructions(t.Name)...)

			continue
		}

		// A new trigger takes the mode ENABLE, so the mode of the target comes after the
		// CREATE TRIGGER statement below.
		if targetTrigger.Def != sourceTrigger.Def {
			instructions = append(instructions,
				&PostgresDropTriggerInstruction{
					Name:      sourceTrigger.Name,
					TableName: t.Name,
				},
				targetTrigger.CreateInstruction())
			instructions = append(instructions, targetTrigger.EnableInstructions(t.Name)...)

			continue
		}

		if targetTrigger.EnableMode != sourceTrigger.EnableMode {
			instructions = append(instructions, alterTable(&PostgresTriggerEnableAction{
				Mode:        targetTrigger.EnableMode,
				TriggerName: targetTrigger.Name,
			}))
		}
	}

	for _, sourceTrigger := range other.Triggers {
		_, found := t.TriggerByName(sourceTrigger.Name)
		if !found {
			instructions = append(instructions,
				&PostgresDropTriggerInstruction{
					Name:      sourceTrigger.Name,
					TableName: t.Name,
				})
		}
	}

	return instructions, nil
}

// A statement needs the parent: a partition needs its parent, a table of INHERITS needs
// each parent of it, and a foreign key needs the table that it names.
func sortTablesByDependency(tables []*PostgresTable) []*PostgresTable {
	tableByName := make(map[string]*PostgresTable, len(tables))

	for _, table := range tables {
		tableByName[table.Name] = table
	}

	sorted := make([]*PostgresTable, 0, len(tables))
	visited := make(map[string]bool, len(tables))

	var visit func(table *PostgresTable)

	visit = func(table *PostgresTable) {
		if visited[table.Name] {
			return
		}

		visited[table.Name] = true

		parent, isPartition := tableByName[table.PartitionParent]
		if isPartition {
			visit(parent)
		}

		for _, parentName := range table.Inherits {
			parent, found := tableByName[parentName]
			if found {
				visit(parent)
			}
		}

		for _, referenceName := range table.References {
			reference, found := tableByName[referenceName]
			if found {
				visit(reference)
			}
		}

		sorted = append(sorted, table)
	}

	for _, table := range tables {
		visit(table)
	}

	return sorted
}

func (t *PostgresTable) DiffRules(other *PostgresTable) []Instruction {
	var instructions []Instruction

	for _, targetRule := range t.Rules {
		sourceRule, found := other.RuleByName(targetRule.Name)
		if !found {
			instructions = append(instructions, targetRule.CreateInstruction())
			continue
		}

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

func (t *PostgresTable) RuleInstructions() []Instruction {
	var instructions []Instruction

	for _, rule := range t.Rules {
		instructions = append(instructions, rule.CreateInstruction())
	}

	return instructions
}

func diffStorageParameters(targetTable *PostgresTable, sourceTable *PostgresTable) []Instruction {
	if slices.Equal(targetTable.StorageParameters, sourceTable.StorageParameters) {
		return nil
	}

	var instructions []Instruction

	if len(targetTable.StorageParameters) > 0 {
		instructions = append(instructions, &PostgresAlterTableInstruction{
			Name: targetTable.Name,
			Actions: []AlterTableAction{
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
			Actions: []AlterTableAction{&PostgresResetStorageParametersAction{Names: removed}},
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

func diffRowLevelSecurity(targetTable *PostgresTable, sourceTable *PostgresTable) []Instruction {
	var instructions []Instruction

	alterTable := func(mode string) Instruction {
		return &PostgresAlterTableInstruction{
			Name:    targetTable.Name,
			Actions: []AlterTableAction{&PostgresRowLevelSecurityAction{Mode: mode}},
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
func (t *PostgresTable) ForeignKeyInstructions() []Instruction {
	if t.IsPartition() {
		return nil
	}

	var instructions []Instruction

	for _, constraint := range t.Constraints {
		if !constraint.IsForeignKey() {
			continue
		}

		instructions = append(instructions, &PostgresAlterTableInstruction{
			Name: t.Name,
			Actions: []AlterTableAction{
				&PostgresAddConstraintAction{Constraint: constraint},
			},
		})
	}

	return instructions
}

// The action of a rule can name a second table, so DiffTables prints the rules apart.
func (t *PostgresTable) Instructions() []Instruction {
	if t.IsPartition() {
		instructions := []Instruction{
			&PostgresCreateTablePartitionInstruction{
				Name:       t.Name,
				ParentName: t.PartitionParent,
				Bound:      t.PartitionBound,
			},
		}

		instructions = append(instructions, t.CommentInstructions()...)
		instructions = append(instructions, t.RowLevelSecurityInstructions()...)

		for _, index := range t.Indexes {
			instructions = append(instructions, index.CreateInstruction())
		}

		for _, trigger := range t.Triggers {
			instructions = append(instructions, trigger.CreateInstruction())
			instructions = append(instructions, trigger.EnableInstructions(t.Name)...)
		}

		return instructions
	}

	instructions := []Instruction{t.CreateTableInstruction()}

	for _, column := range t.Columns {
		instructions = append(instructions, column.StorageInstructions(t.Name)...)
		instructions = append(instructions, column.StatisticsInstructions(t.Name)...)
	}

	instructions = append(instructions, t.CommentInstructions()...)
	instructions = append(instructions, t.RowLevelSecurityInstructions()...)

	for _, index := range t.Indexes {
		instructions = append(instructions, index.CreateInstruction())
	}

	// The mode USING INDEX names an index, so this statement comes after the index loop.
	instructions = append(instructions, t.ReplicaIdentityInstructions()...)

	for _, trigger := range t.Triggers {
		instructions = append(instructions, trigger.CreateInstruction())
		instructions = append(instructions, trigger.EnableInstructions(t.Name)...)
	}

	return instructions
}
