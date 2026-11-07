package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresCast(t *testing.T) {
	t.Run("CreateCast", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE TYPE point2d AS (x integer, y integer);
			CREATE FUNCTION point2d_to_text(point2d) RETURNS text AS $$
				SELECT '(' || ($1).x::text || ',' || ($1).y::text || ')';
			$$ LANGUAGE sql IMMUTABLE;
		`
		driver.ExecOnCurrent(setup)
		driver.ExecOnDesired(setup)

		driver.ExecOnDesired(`CREATE CAST (point2d AS text) WITH FUNCTION point2d_to_text(point2d) AS ASSIGNMENT;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateCastInstruction{
				SourceType: "point2d",
				TargetType: "text",
				Method:     "f",
				Context:    "a",
				Function:   "point2d_to_text(point2d)",
			},
		})
	})

	t.Run("DropCast", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE TYPE point2d AS (x integer, y integer);
			CREATE FUNCTION point2d_to_text(point2d) RETURNS text AS $$
				SELECT '(' || ($1).x::text || ',' || ($1).y::text || ')';
			$$ LANGUAGE sql IMMUTABLE;
		`
		driver.ExecOnCurrent(setup)
		driver.ExecOnDesired(setup)

		driver.ExecOnCurrent(`CREATE CAST (point2d AS text) WITH FUNCTION point2d_to_text(point2d) AS ASSIGNMENT;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropCastInstruction{
				SourceType: "point2d",
				TargetType: "text",
			},
		})
	})

	t.Run("ModifyCast", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE TYPE point2d AS (x integer, y integer);
			CREATE FUNCTION point2d_to_text(point2d) RETURNS text AS $$
				SELECT '(' || ($1).x::text || ',' || ($1).y::text || ')';
			$$ LANGUAGE sql IMMUTABLE;
		`
		driver.ExecOnCurrent(setup)
		driver.ExecOnDesired(setup)

		driver.ExecOnCurrent(`CREATE CAST (point2d AS text) WITH FUNCTION point2d_to_text(point2d);`)
		driver.ExecOnDesired(`CREATE CAST (point2d AS text) WITH FUNCTION point2d_to_text(point2d) AS IMPLICIT;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropCastInstruction{
				SourceType: "point2d",
				TargetType: "text",
			},
			&PostgresCreateCastInstruction{
				SourceType: "point2d",
				TargetType: "text",
				Method:     "f",
				Context:    "i",
				Function:   "point2d_to_text(point2d)",
			},
		})
	})
}
