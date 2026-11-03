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
	constraintRows, err := db.QueryContext(ctx, `
			SELECT
				conname,
				contype,
				pg_get_constraintdef(oid),
				coalesce((SELECT relname FROM pg_class WHERE oid = confrelid), '')
			FROM pg_constraint
			WHERE conrelid = $1::regclass
			AND contype <> 'n'
			ORDER BY conname
		`, driversshared.QuoteIdentifier(table.Name))
	if err != nil {
		return err
	}

	defer func() { _ = constraintRows.Close() }()

	for constraintRows.Next() {
		constraint := &PostgresConstraint{}

		var referencedTable string

		err := constraintRows.Scan(&constraint.Name, &constraint.Type, &constraint.Def,
			&referencedTable)
		if err != nil {
			return err
		}

		if referencedTable != "" && referencedTable != table.Name {
			table.References = append(table.References, referencedTable)
		}

		table.Constraints = append(table.Constraints, constraint)
	}

	return constraintRows.Err()
}
