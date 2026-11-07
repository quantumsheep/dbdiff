package driverspostgres

import (
	"database/sql"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresOperator(t *testing.T) {
	t.Run("DropOperatorBeforeFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`)
		driver.ExecOnCurrent(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropOperatorInstruction{
				Name: "===",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
			},
			&PostgresDropFunctionInstruction{
				Name:      "int_add",
				Arguments: "integer, integer",
			},
		})
	})

	t.Run("CreateOperator", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnCurrent(setup)
		driver.ExecOnDesired(setup)

		driver.ExecOnDesired(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateOperatorInstruction{
				Name:     "===",
				Function: "int_add",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
			},
		})
	})

	t.Run("ModifyOperatorCommutatorAndNegator", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE FUNCTION int_eq(integer, integer) RETURNS boolean AS $$ SELECT $1 = $2; $$ LANGUAGE sql IMMUTABLE;
			CREATE FUNCTION int_ne(integer, integer) RETURNS boolean AS $$ SELECT $1 <> $2; $$ LANGUAGE sql IMMUTABLE;
			CREATE OPERATOR !== (FUNCTION = int_ne, LEFTARG = integer, RIGHTARG = integer);
		`
		driver.ExecOnCurrent(setup)
		driver.ExecOnCurrent(`CREATE OPERATOR === (FUNCTION = int_eq, LEFTARG = integer, RIGHTARG = integer);`)

		driver.ExecOnDesired(setup)
		driver.ExecOnDesired(`
			CREATE OPERATOR === (
				FUNCTION = int_eq, LEFTARG = integer, RIGHTARG = integer,
				COMMUTATOR = ===, NEGATOR = !==, RESTRICT = eqsel, JOIN = eqjoinsel, HASHES, MERGES
			);
		`)
		// The NEGATOR clause of the desired schema writes the reverse link into the other
		// operator too, so the diff recreates both operators.
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropOperatorInstruction{
				Name: "!==",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
			},
			&PostgresCreateOperatorInstruction{
				Name:     "!==",
				Function: "int_ne",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				Negator: sql.NullString{
					String: "===",
					Valid:  true,
				},
			},
			&PostgresDropOperatorInstruction{
				Name: "===",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
			},
			&PostgresCreateOperatorInstruction{
				Name:     "===",
				Function: "int_eq",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				Commutator: sql.NullString{
					String: "===",
					Valid:  true,
				},
				Negator: sql.NullString{
					String: "!==",
					Valid:  true,
				},
				RestrictFunction: sql.NullString{
					String: "eqsel",
					Valid:  true,
				},
				JoinFunction: sql.NullString{
					String: "eqjoinsel",
					Valid:  true,
				},
				CanHash:  true,
				CanMerge: true,
			},
		})
	})

	t.Run("ModifyOperator", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;
			CREATE FUNCTION int_sub(integer, integer) RETURNS integer AS $$ SELECT $1 - $2; $$ LANGUAGE sql IMMUTABLE;
		`
		driver.ExecOnCurrent(setup)
		driver.ExecOnCurrent(`CREATE OPERATOR === (FUNCTION = int_sub, LEFTARG = integer, RIGHTARG = integer);`)

		driver.ExecOnDesired(setup)
		driver.ExecOnDesired(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropOperatorInstruction{
				Name: "===",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
			},
			&PostgresCreateOperatorInstruction{
				Name:     "===",
				Function: "int_add",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
			},
		})
	})

	t.Run("DropOperator", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnCurrent(setup)
		driver.ExecOnCurrent(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)

		driver.ExecOnDesired(setup)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropOperatorInstruction{
				Name: "===",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
			},
		})
	})
}
