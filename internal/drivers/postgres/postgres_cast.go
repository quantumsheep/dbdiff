package driverspostgres

import (
	"context"
	"database/sql"
)

// PostgreSQL holds no action that changes a cast, so a new definition prints a DROP
// statement and a CREATE statement. A cast holds no schema, so GetCasts scopes it to the
// schema of its source type or its target type, like a domain or a composite type.
type PostgresCast struct {
	SourceType string
	TargetType string

	// Method holds the castmethod of pg_cast: "f" for a function, "i" for INOUT, and "b"
	// for a binary coercion.
	Method string

	// Context holds the castcontext of pg_cast: "e" for explicit, "a" for assignment, and
	// "i" for implicit.
	Context string

	// Function names the cast function with its argument list, and it stays empty for the
	// INOUT method and the binary method.
	Function string
}

func (c *PostgresCast) Equal(other *PostgresCast) bool {
	return c.Method == other.Method && c.Context == other.Context && c.Function == other.Function
}

func (c *PostgresCast) CreateInstruction() *PostgresCreateCastInstruction {
	return &PostgresCreateCastInstruction{
		SourceType: c.SourceType,
		TargetType: c.TargetType,
		Method:     c.Method,
		Context:    c.Context,
		Function:   c.Function,
	}
}

func (c *PostgresCast) DropInstruction() *PostgresDropCastInstruction {
	return &PostgresDropCastInstruction{
		SourceType: c.SourceType,
		TargetType: c.TargetType,
	}
}

type AutomaticCastLookup func(oldType string, newType string) (bool, error)

func columnUsingClause(newColumn *PostgresColumn, oldColumn *PostgresColumn, hasAutomaticCast AutomaticCastLookup) (bool, error) {
	automatic, err := hasAutomaticCast(oldColumn.Type, newColumn.Type)
	if err != nil {
		return false, err
	}

	return !automatic, nil
}

// The rules of pg_cast are long, so this function asks the database. ALTER TABLE ALTER
// COLUMN TYPE uses the assignment context. An unknown type name gives true, and the output
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
