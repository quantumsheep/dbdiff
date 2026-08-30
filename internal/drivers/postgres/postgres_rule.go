package driverspostgres

// PostgreSQL holds no action that changes a rule, so a new definition prints a DROP
// statement and a CREATE statement.
type PostgresRule struct {
	Name  string
	Table string
	Def   string
}

func (r *PostgresRule) CreateInstruction() *PostgresCreateRuleInstruction {
	return &PostgresCreateRuleInstruction{Definition: r.Def}
}

func (r *PostgresRule) DropInstruction() *PostgresDropRuleInstruction {
	return &PostgresDropRuleInstruction{
		Name:      r.Name,
		TableName: r.Table,
	}
}
