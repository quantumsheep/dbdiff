package drivers

type PostgresTable struct {
	Name        string
	Columns     []*PostgresColumn
	Indexes     []*PostgresIndex
	Constraints []*PostgresConstraint
	Triggers    []*PostgresTrigger

	// PartitionKey holds the key of a partitioned table. PartitionParent and PartitionBound
	// hold the parent and the bound of a partition. A table that is neither keeps the three
	// fields empty.
	PartitionKey    string
	PartitionParent string
	PartitionBound  string

	// Inherits names the parent of a table of INHERITS. That table is no partition, so it
	// keeps its own columns and its own statement.
	Inherits []string

	Comment string

	RowLevelSecurity      bool
	ForceRowLevelSecurity bool
	Policies              []*PostgresPolicy
}

// RowLevelSecurityInstructions returns the statements that switch row level security on.
// PostgreSQL accepts no such option in a CREATE TABLE statement.
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

func (t *PostgresTable) PolicyByName(name string) (*PostgresPolicy, bool) {
	for _, policy := range t.Policies {
		if policy.Name == name {
			return policy, true
		}
	}

	return nil, false
}

// CommentInstructions returns the statement of the comment of the table and the statement
// of the comment of each column. PostgreSQL accepts a comment in no CREATE statement.
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

// IsPartition tells if the table is a partition of another table.
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

		// PostgreSQL changes a collation through the TYPE action, so a new collation
		// prints that action too.
		if sourceColumn.Type != targetColumn.Type || sourceColumn.Collation != targetColumn.Collation {
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
			continue
		}

		if sourceTrigger.Def != targetTrigger.Def {
			instructions = append(instructions,
				&PostgresDropTriggerInstruction{
					Name:      targetTrigger.Name,
					TableName: t.Name,
				},
				sourceTrigger.CreateInstruction())
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

// sortTablesByPartitionParent orders the tables so that a partition comes after its
// parent, and a table of INHERITS comes after every parent of it. The name of a child can
// sort before the name of its parent, and each of the two statements needs the parent. A
// partition of a partition keeps the same rule, because the walk visits the parent first.
func sortTablesByPartitionParent(tables []*PostgresTable) []*PostgresTable {
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

		sorted = append(sorted, table)
	}

	for _, table := range tables {
		visit(table)
	}

	return sorted
}

// diffRowLevelSecurity compares the two switches of row level security and every policy.
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

func (t *PostgresTable) TriggerByName(name string) (*PostgresTrigger, bool) {
	for _, tr := range t.Triggers {
		if tr.Name == name {
			return tr, true
		}
	}

	return nil, false
}

func (t *PostgresTable) CreateTableInstruction() *PostgresCreateTableInstruction {
	return &PostgresCreateTableInstruction{
		Name:         t.Name,
		Columns:      t.Columns,
		Constraints:  t.Constraints,
		PartitionKey: t.PartitionKey,
		Comment:      t.Comment,
		Inherits:     t.Inherits,
	}
}

// Instructions returns the statement that creates the table, then the statements of its
// indexes, then the statements of its triggers.
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
	instructions = append(instructions, t.CommentInstructions()...)
	instructions = append(instructions, t.RowLevelSecurityInstructions()...)

	for _, index := range t.Indexes {
		instructions = append(instructions, index.CreateInstruction())
	}

	for _, trigger := range t.Triggers {
		instructions = append(instructions, trigger.CreateInstruction())
	}

	return instructions
}
