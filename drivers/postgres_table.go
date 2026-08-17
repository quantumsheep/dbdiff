package drivers

type PostgresTable struct {
	Name        string
	Columns     []*PostgresColumn
	Indexes     []*PostgresIndex
	Constraints []*PostgresConstraint
	Triggers    []*PostgresTrigger
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

		if sourceColumn.Type != targetColumn.Type {
			usingCast, err := columnUsingClause(sourceColumn, targetColumn, hasAutomaticCast)
			if err != nil {
				return nil, err
			}

			instructions = append(instructions, alterTable(&PostgresAlterColumnTypeAction{
				ColumnName: sourceColumn.Name,
				DataType:   sourceColumn.Type,
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
		Name:        t.Name,
		Columns:     t.Columns,
		Constraints: t.Constraints,
	}
}

// Instructions returns the statement that creates the table, then the statements of its
// indexes, then the statements of its triggers.
func (t *PostgresTable) Instructions() []Instruction {
	instructions := []Instruction{t.CreateTableInstruction()}

	for _, index := range t.Indexes {
		instructions = append(instructions, index.CreateInstruction())
	}

	for _, trigger := range t.Triggers {
		instructions = append(instructions, trigger.CreateInstruction())
	}

	return instructions
}
