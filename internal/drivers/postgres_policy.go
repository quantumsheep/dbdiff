package drivers

import "slices"

// PostgreSQL holds no action that changes a policy, so the diff prints a DROP statement
// and a CREATE statement.
type PostgresPolicy struct {
	Name       string
	Table      string
	Permissive string
	Command    string
	Roles      []string
	Using      string
	WithCheck  string
}

func (p *PostgresPolicy) Equal(other *PostgresPolicy) bool {
	if p.Name != other.Name || p.Table != other.Table {
		return false
	}

	if p.Permissive != other.Permissive || p.Command != other.Command {
		return false
	}

	if p.Using != other.Using || p.WithCheck != other.WithCheck {
		return false
	}

	return slices.Equal(p.Roles, other.Roles)
}

func (p *PostgresPolicy) CreateInstruction() *PostgresCreatePolicyInstruction {
	return &PostgresCreatePolicyInstruction{
		Name:       p.Name,
		TableName:  p.Table,
		Permissive: p.Permissive,
		Command:    p.Command,
		Roles:      p.Roles,
		Using:      p.Using,
		WithCheck:  p.WithCheck,
	}
}

func (p *PostgresPolicy) DropInstruction() *PostgresDropPolicyInstruction {
	return &PostgresDropPolicyInstruction{
		Name:      p.Name,
		TableName: p.Table,
	}
}
