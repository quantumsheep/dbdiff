package drivers

import (
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

	// Inherits names the parent of a table of INHERITS. That table is no partition, so it
	// keeps its own columns and its own statement.
	Inherits []string

	// References names each table that a foreign key of this table names.
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
			Name:    t.Name,
			Comment: t.Comment,
		})
	}

	for _, column := range t.Columns {
		if column.Comment == "" {
			continue
		}

		instructions = append(instructions, &PostgresCommentOnColumnInstruction{
			TableName:  t.Name,
			ColumnName: column.Name,
			Comment:    column.Comment,
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

func (t *PostgresTable) DiffTable(other *PostgresTable, hasAutomaticCast AutomaticCastLookup) ([]Instruction, error) {
	var instructions []Instruction

	// A partition holds the columns, the constraints, and the indexes of its parent. The
	// diff of the parent covers each of them.
	if t.IsPartition() || other.IsPartition() {
		return nil, nil
	}

	alterTable := func(action AlterTableAction) Instruction {
		return &PostgresAlterTableInstruction{
			Name:    t.Name,
			Actions: []AlterTableAction{action},
		}
	}

	for _, sourceColumn := range t.Columns {
		targetColumn, found := other.ColumnByName(sourceColumn.Name)
		if !found {
			instructions = append(instructions,
				alterTable(&PostgresAddColumnAction{Column: sourceColumn}))
			instructions = append(instructions, sourceColumn.StorageInstructions(t.Name)...)
			instructions = append(instructions, sourceColumn.StatisticsInstructions(t.Name)...)

			continue
		}

		if sourceColumn.HasEqualAttributes(targetColumn) {
			continue
		}

		// PostgreSQL holds no action that changes the expression of a generated column.
		// The column keeps no data of its own, so one DROP COLUMN action and one ADD
		// COLUMN action rebuild it with no loss.
		if sourceColumn.GeneratedExpression != targetColumn.GeneratedExpression {
			instructions = append(instructions, &PostgresAlterTableInstruction{
				Name: t.Name,
				Actions: []AlterTableAction{
					&SQLDropColumnAction{ColumnName: targetColumn.Name},
					&PostgresAddColumnAction{Column: sourceColumn},
				},
			})

			continue
		}

		// PostgreSQL refuses to remove the NOT NULL flag of an identity column, so this
		// action comes before the NOT NULL block below.
		if targetColumn.Identity != "" && sourceColumn.Identity == "" {
			instructions = append(instructions,
				alterTable(&PostgresDropIdentityAction{ColumnName: sourceColumn.Name}))
		}

		typeChanged := sourceColumn.Type != targetColumn.Type ||
			sourceColumn.Collation != targetColumn.Collation

		// PostgreSQL changes a collation through the TYPE action, so a new collation
		// prints that action too.
		if typeChanged {
			usingCast, err := columnUsingClause(sourceColumn, targetColumn, hasAutomaticCast)
			if err != nil {
				return nil, err
			}

			instructions = append(instructions, alterTable(&PostgresAlterColumnTypeAction{
				ColumnName: sourceColumn.Name,
				DataType:   sourceColumn.Type,
				Collation:  sourceColumn.Collation,
				UsingCast:  usingCast,
			}))
		}

		if sourceColumn.NotNull != targetColumn.NotNull {
			if sourceColumn.NotNull {
				instructions = append(instructions,
					alterTable(&PostgresSetNotNullAction{ColumnName: sourceColumn.Name}))
			} else {
				instructions = append(instructions,
					alterTable(&PostgresDropNotNullAction{ColumnName: sourceColumn.Name}))
			}
		}

		if sourceColumn.Default != targetColumn.Default {
			if sourceColumn.Default.Valid {
				instructions = append(instructions, alterTable(&PostgresSetDefaultAction{
					ColumnName: sourceColumn.Name,
					Expression: sourceColumn.Default.String,
				}))
			} else {
				instructions = append(instructions,
					alterTable(&PostgresDropDefaultAction{ColumnName: sourceColumn.Name}))
			}
		}

		// A TYPE action gives the column the storage mode of the new type, so a column
		// that changes its type and holds a mode takes that mode again. That action comes
		// above, so this action always follows it. The mode DEFAULT gives the column the
		// mode of its type again, and PostgreSQL 16 accepts that mode.
		if sourceColumn.Storage != targetColumn.Storage ||
			(typeChanged && sourceColumn.Storage != "") {
			storage := sourceColumn.Storage
			if storage == "" {
				storage = "DEFAULT"
			}

			instructions = append(instructions, alterTable(&PostgresSetStorageAction{
				ColumnName: sourceColumn.Name,
				Storage:    storage,
			}))
		}

		// A TYPE action keeps the statistics target, so this action needs no such test.
		// The value -1 gives the column the default target of the server again.
		if sourceColumn.StatisticsTarget != targetColumn.StatisticsTarget {
			target := int64(-1)
			if sourceColumn.StatisticsTarget.Valid {
				target = sourceColumn.StatisticsTarget.Int64
			}

			instructions = append(instructions, alterTable(&PostgresSetStatisticsAction{
				ColumnName: sourceColumn.Name,
				Target:     target,
			}))
		}

		// The options of an identity column live in its sequence, so this action changes
		// that sequence and never the identity itself.
		if sourceColumn.Identity != "" && targetColumn.Identity != "" &&
			sourceColumn.IdentityOptions != targetColumn.IdentityOptions &&
			sourceColumn.IdentityOptions != "" {
			instructions = append(instructions, alterTable(&PostgresSetIdentityOptionsAction{
				ColumnName: sourceColumn.Name,
				Options:    sourceColumn.IdentityOptions,
			}))
		}

		// PostgreSQL refuses to add an identity to a column that accepts a null value, so
		// these two actions come after the NOT NULL block above.
		if sourceColumn.Identity != "" && sourceColumn.Identity != targetColumn.Identity {
			if targetColumn.Identity == "" {
				instructions = append(instructions, alterTable(&PostgresAddIdentityAction{
					ColumnName: sourceColumn.Name,
					Identity:   sourceColumn.Identity,
				}))
			} else {
				instructions = append(instructions, alterTable(&PostgresSetIdentityAction{
					ColumnName: sourceColumn.Name,
					Identity:   sourceColumn.Identity,
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
			Name:    t.Name,
			Comment: t.Comment,
		})
	}

	for _, sourceColumn := range t.Columns {
		targetColumn, found := other.ColumnByName(sourceColumn.Name)
		if !found {
			continue
		}

		if sourceColumn.Comment != targetColumn.Comment {
			instructions = append(instructions, &PostgresCommentOnColumnInstruction{
				TableName:  t.Name,
				ColumnName: sourceColumn.Name,
				Comment:    sourceColumn.Comment,
			})
		}
	}

	// PostgreSQL drops every constraint and every index of a column with the column, so
	// these two blocks come before the column removals below.
	for _, sourceConstraint := range t.Constraints {
		targetConstraint, found := other.ConstraintByName(sourceConstraint.Name)
		if !found {
			instructions = append(instructions,
				alterTable(&PostgresAddConstraintAction{Constraint: sourceConstraint}))

			continue
		}

		if sourceConstraint.Def != targetConstraint.Def {
			instructions = append(instructions,
				alterTable(&PostgresDropConstraintAction{ConstraintName: targetConstraint.Name}),
				alterTable(&PostgresAddConstraintAction{Constraint: sourceConstraint}))
		}
	}

	for _, targetConstraint := range other.Constraints {
		_, found := t.ConstraintByName(targetConstraint.Name)
		if !found {
			instructions = append(instructions,
				alterTable(&PostgresDropConstraintAction{ConstraintName: targetConstraint.Name}))
		}
	}

	for _, sourceIndex := range t.Indexes {
		targetIndex, found := other.IndexByName(sourceIndex.Name)
		if !found {
			instructions = append(instructions, sourceIndex.CreateInstruction())
			continue
		}

		if sourceIndex.Def != targetIndex.Def {
			instructions = append(instructions,
				&SQLDropIndexInstruction{Name: targetIndex.Name},
				sourceIndex.CreateInstruction())
		}
	}

	// The mode USING INDEX names an index, so this block comes after the index additions
	// above. It comes before the index removals below, because PostgreSQL refuses to drop
	// the index that the replica identity of the target holds.
	if t.ReplicaIdentity != other.ReplicaIdentity ||
		t.ReplicaIdentityIndex != other.ReplicaIdentityIndex {
		instructions = append(instructions, alterTable(&PostgresReplicaIdentityAction{
			Mode:      t.ReplicaIdentity,
			IndexName: t.ReplicaIdentityIndex,
		}))
	}

	for _, targetIndex := range other.Indexes {
		_, found := t.IndexByName(targetIndex.Name)
		if !found {
			instructions = append(instructions, &SQLDropIndexInstruction{Name: targetIndex.Name})
		}
	}

	for _, targetColumn := range other.Columns {
		_, found := t.ColumnByName(targetColumn.Name)
		if !found {
			instructions = append(instructions,
				alterTable(&SQLDropColumnAction{ColumnName: targetColumn.Name}))
		}
	}

	for _, sourceTrigger := range t.Triggers {
		targetTrigger, found := other.TriggerByName(sourceTrigger.Name)
		if !found {
			instructions = append(instructions, sourceTrigger.CreateInstruction())
			instructions = append(instructions, sourceTrigger.EnableInstructions(t.Name)...)

			continue
		}

		// A new trigger takes the mode ENABLE, so the mode of the source comes after the
		// CREATE TRIGGER statement below.
		if sourceTrigger.Def != targetTrigger.Def {
			instructions = append(instructions,
				&PostgresDropTriggerInstruction{
					Name:      targetTrigger.Name,
					TableName: t.Name,
				},
				sourceTrigger.CreateInstruction())
			instructions = append(instructions, sourceTrigger.EnableInstructions(t.Name)...)

			continue
		}

		if sourceTrigger.EnableMode != targetTrigger.EnableMode {
			instructions = append(instructions, alterTable(&PostgresTriggerEnableAction{
				Mode:        sourceTrigger.EnableMode,
				TriggerName: sourceTrigger.Name,
			}))
		}
	}

	for _, targetTrigger := range other.Triggers {
		_, found := t.TriggerByName(targetTrigger.Name)
		if !found {
			instructions = append(instructions,
				&PostgresDropTriggerInstruction{
					Name:      targetTrigger.Name,
					TableName: t.Name,
				})
		}
	}

	return instructions, nil
}

// sortTablesByDependency orders the tables so that a partition comes after its parent, a
// table of INHERITS comes after every parent of it, and a table comes after every table
// that a foreign key of it names. The name of a child can sort before the name of its
// parent, and each of the statements needs the parent. A DROP TABLE statement takes the
// reverse order.
//
// PostgreSQL accepts a cycle of two foreign keys. The walk marks a table before it visits
// the tables of that table, so a cycle gives an order and no endless loop.
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

// The action of a rule can name a second table, so DiffTables prints these instructions
// after every table.
func (t *PostgresTable) DiffRules(other *PostgresTable) []Instruction {
	var instructions []Instruction

	for _, sourceRule := range t.Rules {
		targetRule, found := other.RuleByName(sourceRule.Name)
		if !found {
			instructions = append(instructions, sourceRule.CreateInstruction())
			continue
		}

		if sourceRule.Def != targetRule.Def {
			instructions = append(instructions,
				targetRule.DropInstruction(), sourceRule.CreateInstruction())
		}
	}

	for _, targetRule := range other.Rules {
		_, found := t.RuleByName(targetRule.Name)
		if !found {
			instructions = append(instructions, targetRule.DropInstruction())
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

func diffStorageParameters(sourceTable *PostgresTable, targetTable *PostgresTable) []Instruction {
	if slices.Equal(sourceTable.StorageParameters, targetTable.StorageParameters) {
		return nil
	}

	var instructions []Instruction

	if len(sourceTable.StorageParameters) > 0 {
		instructions = append(instructions, &PostgresAlterTableInstruction{
			Name: sourceTable.Name,
			Actions: []AlterTableAction{
				&PostgresSetStorageParametersAction{Parameters: sourceTable.StorageParameters},
			},
		})
	}

	var removed []string

	for _, parameter := range targetTable.StorageParameters {
		name := storageParameterName(parameter)

		held := slices.ContainsFunc(sourceTable.StorageParameters,
			func(sourceParameter string) bool {
				return storageParameterName(sourceParameter) == name
			})
		if !held {
			removed = append(removed, name)
		}
	}

	if len(removed) > 0 {
		instructions = append(instructions, &PostgresAlterTableInstruction{
			Name:    sourceTable.Name,
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

func diffRowLevelSecurity(sourceTable *PostgresTable, targetTable *PostgresTable) []Instruction {
	var instructions []Instruction

	alterTable := func(mode string) Instruction {
		return &PostgresAlterTableInstruction{
			Name:    sourceTable.Name,
			Actions: []AlterTableAction{&PostgresRowLevelSecurityAction{Mode: mode}},
		}
	}

	if sourceTable.RowLevelSecurity != targetTable.RowLevelSecurity {
		if sourceTable.RowLevelSecurity {
			instructions = append(instructions, alterTable("ENABLE"))
		} else {
			instructions = append(instructions, alterTable("DISABLE"))
		}
	}

	if sourceTable.ForceRowLevelSecurity != targetTable.ForceRowLevelSecurity {
		if sourceTable.ForceRowLevelSecurity {
			instructions = append(instructions, alterTable("FORCE"))
		} else {
			instructions = append(instructions, alterTable("NO FORCE"))
		}
	}

	for _, sourcePolicy := range sourceTable.Policies {
		targetPolicy, found := targetTable.PolicyByName(sourcePolicy.Name)
		if !found {
			instructions = append(instructions, sourcePolicy.CreateInstruction())
			continue
		}

		if !sourcePolicy.Equal(targetPolicy) {
			instructions = append(instructions,
				targetPolicy.DropInstruction(), sourcePolicy.CreateInstruction())
		}
	}

	for _, targetPolicy := range targetTable.Policies {
		_, found := sourceTable.PolicyByName(targetPolicy.Name)
		if !found {
			instructions = append(instructions, targetPolicy.DropInstruction())
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

// The statement holds no foreign key, because a foreign key can name a table that comes
// later. DiffTables prints every foreign key after every table.
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

// A partition takes the foreign keys of its parent, so this list stays empty for it. A
// second statement of the same key fails.
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

// The list holds no rule, because the action of a rule can name a second table. DiffTables
// prints every rule after every table.
func (t *PostgresTable) Instructions() []Instruction {
	if t.IsPartition() {
		return []Instruction{
			&PostgresCreateTablePartitionInstruction{
				Name:       t.Name,
				ParentName: t.PartitionParent,
				Bound:      t.PartitionBound,
			},
		}
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
