package driverspostgres

import (
	"database/sql"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresAggregate(t *testing.T) {
	t.Run("CreateAggregate", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnCurrent(setup)
		driver.ExecOnDesired(setup)

		driver.ExecOnDesired(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateAggregateInstruction{
				Name:               "total",
				Arguments:          "integer",
				TransitionFunction: "int_add",
				StateType:          "integer",
				InitialCondition: sql.NullString{
					String: "0",
					Valid:  true,
				},
			},
		})
	})

	t.Run("ModifyAggregateCombineFunctionAndParallel", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnCurrent(setup)
		driver.ExecOnCurrent(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer);`)

		driver.ExecOnDesired(setup)
		driver.ExecOnDesired(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, COMBINEFUNC = int_add, PARALLEL = SAFE);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropAggregateInstruction{
				Name:      "total",
				Arguments: "integer",
			},
			&PostgresCreateAggregateInstruction{
				Name:               "total",
				Arguments:          "integer",
				TransitionFunction: "int_add",
				StateType:          "integer",
				CombineFunction: sql.NullString{
					String: "int_add",
					Valid:  true,
				},
				Parallel: "s",
			},
		})
	})

	t.Run("ModifyAggregate", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnCurrent(setup)
		driver.ExecOnCurrent(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '10');`)

		driver.ExecOnDesired(setup)
		driver.ExecOnDesired(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropAggregateInstruction{
				Name:      "total",
				Arguments: "integer",
			},
			&PostgresCreateAggregateInstruction{
				Name:               "total",
				Arguments:          "integer",
				TransitionFunction: "int_add",
				StateType:          "integer",
				InitialCondition: sql.NullString{
					String: "0",
					Valid:  true,
				},
			},
		})
	})

	t.Run("DropAggregate", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnCurrent(setup)
		driver.ExecOnCurrent(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)

		driver.ExecOnDesired(setup)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropAggregateInstruction{
				Name:      "total",
				Arguments: "integer",
			},
		})
	})

	t.Run("DropAggregateBeforeFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`)
		driver.ExecOnCurrent(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropAggregateInstruction{
				Name:      "total",
				Arguments: "integer",
			},
			&PostgresDropFunctionInstruction{
				Name:      "int_add",
				Arguments: "integer, integer",
			},
		})
	})
}
