package driverspostgres

import (
	"context"
	"database/sql"
	"slices"
	"strings"
)

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

func (d *PostgresDriver) GetTablePolicies(ctx context.Context, db *sql.DB, tableName string) ([]*PostgresPolicy, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			policyname,
			permissive,
			cmd,
			array_to_string(roles, ','),
			coalesce(qual, ''),
			coalesce(with_check, '')
		FROM pg_policies
		WHERE schemaname = current_schema() AND tablename = $1
		ORDER BY policyname
	`, tableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var policies []*PostgresPolicy

	for rows.Next() {
		policy := &PostgresPolicy{Table: tableName}

		var roles string

		err := rows.Scan(&policy.Name, &policy.Permissive, &policy.Command, &roles,
			&policy.Using, &policy.WithCheck)
		if err != nil {
			return nil, err
		}

		if roles != "" {
			policy.Roles = strings.Split(roles, ",")
		}

		policies = append(policies, policy)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return policies, nil
}
