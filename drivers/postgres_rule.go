package drivers

// A PostgresRule rewrites a statement of a table. PostgreSQL holds no action that changes a
// rule, so a new definition prints a DROP statement and a CREATE statement.
//
// A view holds an implicit rule with the name _RETURN. GetTableRules reads no such rule,
// because the CREATE VIEW statement builds it.
type PostgresRule struct {
	Name  string
	Table string

	// Def holds the text of pg_rules.definition, and that text ends with a semicolon.
	Def string
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
