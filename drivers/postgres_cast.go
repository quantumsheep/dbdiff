package drivers

import (
	"context"
	"database/sql"
	"fmt"
)

// AutomaticCastLookup tells if PostgreSQL casts a value from the old type of a column to
// the new type of that column without an explicit cast.
type AutomaticCastLookup func(oldType string, newType string) (bool, error)

// columnUsingClause returns the USING clause that ALTER COLUMN TYPE needs. An automatic
// cast from the old column to the new column gives an empty string.
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

// HasAutomaticCast asks PostgreSQL if it casts a value from one type to another type in an
// assignment. ALTER TABLE ALTER COLUMN TYPE uses the assignment context. Without an
// automatic cast, that statement needs a USING clause.
//
// PostgreSQL applies three rules in this order:
//
//   - Two equal types need no cast.
//   - A row of pg_cast gives an automatic cast when the context is 'i' (implicit) or 'a'
//     (assignment). A row with the context 'e' (explicit) stops the last rule.
//   - Without a row in pg_cast, PostgreSQL casts each type to a string type through the
//     input function and the output function. That cast is an assignment cast to a string
//     type, and an explicit cast from a string type.
//
// If PostgreSQL knows no type with the given name, this function returns true. The output
// then keeps the form that it had before this rule.
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
