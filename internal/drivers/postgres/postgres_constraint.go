package driverspostgres

import (
	"context"
	"database/sql"
	"fmt"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type PostgresConstraint struct {
	Name string
	Type string
	Def  string

	// The parent of a table of INHERITS holds this constraint, and each change of the parent
	// reaches the child.
	Inherited bool
}

func (c *PostgresConstraint) IsForeignKey() bool {
	return c.Type == "f"
}

func (c *PostgresConstraint) Clause() string {
	return fmt.Sprintf("CONSTRAINT %s %s", driversshared.QuoteIdentifier(c.Name), c.Def)
}

func getTableConstraints(ctx context.Context, db *sql.DB, table *PostgresTable) error {
	// PostgreSQL 18 keeps the NOT NULL flag of a column in pg_constraint too, and the column
	// holds that flag already. Without this filter a renamed column names one flag with two
	// names, and the ADD statement fails, because a column holds one not-null constraint only.
	constraints, err := driversshared.QueryAll(ctx, db, `
			SELECT
				conname,
				contype,
				pg_get_constraintdef(oid),
				coalesce((SELECT relname FROM pg_class WHERE oid = confrelid), ''),
				NOT conislocal
			FROM pg_constraint
			WHERE conrelid = $1::regclass
			AND contype <> 'n'
			ORDER BY conname
		`, func(rows *sql.Rows) (*PostgresConstraint, error) {
		constraint := &PostgresConstraint{}

		var referencedTable string

		err := rows.Scan(&constraint.Name, &constraint.Type, &constraint.Def, &referencedTable, &constraint.Inherited)
		if err != nil {
			return nil, err
		}

		if referencedTable != "" && referencedTable != table.Name {
			table.References = append(table.References, referencedTable)
		}

		return constraint, nil
	}, driversshared.QuoteIdentifier(table.Name))
	if err != nil {
		return err
	}

	table.Constraints = append(table.Constraints, constraints...)

	return nil
}
