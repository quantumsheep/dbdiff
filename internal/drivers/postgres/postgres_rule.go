package driverspostgres

import (
	"context"
	"database/sql"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

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

// pg_rules reports no _RETURN rule, which is the implicit rule of a view.
func (d *PostgresDriver) GetTableRules(ctx context.Context, db *sql.DB, tableName string) ([]*PostgresRule, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT
			rulename,
			regexp_replace(
				definition,
				'\m' || quote_ident(current_schema()) || '\.',
				'',
				'g'
			)
		FROM pg_rules
		WHERE schemaname = current_schema() AND tablename = $1
		ORDER BY rulename
	`, func(rows *sql.Rows) (*PostgresRule, error) {
		rule := &PostgresRule{Table: tableName}

		err := rows.Scan(&rule.Name, &rule.Def)

		return rule, err
	}, tableName)
}
