package drivers

import (
	"context"
	"database/sql"
	"fmt"
)

type AutomaticCastLookup func(oldType string, newType string) (bool, error)

func columnUsingClause(newColumn *PostgresColumn, oldColumn *PostgresColumn, hasAutomaticCast AutomaticCastLookup) (string, error) {
	automatic, err := hasAutomaticCast(oldColumn.Type, newColumn.Type)
	if err != nil {
		return "", err
	}

	if automatic {
		return "", nil
	}

	return fmt.Sprintf(" USING %s::%s", quoteIdentifier(newColumn.Name), newColumn.Type), nil
}

// HasAutomaticCast asks the database, because the rules of pg_cast are long. ALTER TABLE
// ALTER COLUMN TYPE uses the assignment context. An unknown type name gives true, and the
// output then keeps the form that it had before this rule.
func (d *PostgresDriver) HasAutomaticCast(ctx context.Context, db *sql.DB, oldType string, newType string) (bool, error) {
	row := db.QueryRowContext(ctx, `
		SELECT
			old_type IS NULL
			OR new_type IS NULL
			OR old_type = new_type
			OR COALESCE(
				(
					SELECT c.castcontext IN ('i', 'a')
					FROM pg_cast c
					WHERE c.castsource = old_type AND c.casttarget = new_type
				),
				(
					SELECT t.typcategory = 'S'
					FROM pg_type t
					WHERE t.oid = new_type
				),
				false
			)
		FROM (SELECT to_regtype($1) AS old_type, to_regtype($2) AS new_type) AS types
	`, oldType, newType)

	var automatic bool

	err := row.Scan(&automatic)
	if err != nil {
		return false, err
	}

	return automatic, nil
}
