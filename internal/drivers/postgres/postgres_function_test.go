package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresFunction(t *testing.T) {
	t.Run("ModifyFunctionComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `CREATE FUNCTION add_one(integer) RETURNS integer AS $$ SELECT $1 + 1; $$ LANGUAGE sql;`
		driver.ExecOnCurrent(schema)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`COMMENT ON FUNCTION add_one(integer) IS 'the next value';`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnRoutineInstruction{
				Name:      "add_one",
				Arguments: "integer",
				Text:      "the next value",
			},
		})
	})

	t.Run("CreateFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE FUNCTION increment(a integer) RETURNS integer AS $$
			BEGIN
				RETURN a + 1;
			END;
			$$ LANGUAGE plpgsql;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION increment(a integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$\n\t\t\tBEGIN\n\t\t\t\tRETURN a + 1;\n\t\t\tEND;\n\t\t\t$function$",
			},
		})
	})

	t.Run("ReplaceFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnDesired(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 2; END; $$ LANGUAGE plpgsql;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION increment(a integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN a + 2; END; $function$",
			},
		})
	})

	t.Run("ReplaceFunctionBodyOnly", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnDesired(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 3; END; $$ LANGUAGE plpgsql;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION increment(a integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN a + 3; END; $function$",
			},
		})
	})

	t.Run("ReplaceFunctionReturnType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE FUNCTION calculate(a integer) RETURNS integer AS $$ BEGIN RETURN a; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnDesired(`CREATE FUNCTION calculate(a integer) RETURNS text AS $$ BEGIN RETURN a::text; END; $$ LANGUAGE plpgsql;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropFunctionInstruction{
				Name:      "calculate",
				Arguments: "a integer",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION calculate(a integer)\n RETURNS text\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN a::text; END; $function$",
			},
		})
	})

	t.Run("RenameAFunctionParameter", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnDesired(`CREATE FUNCTION increment(b integer) RETURNS integer AS $$ BEGIN RETURN b + 1; END; $$ LANGUAGE plpgsql;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropFunctionInstruction{
				Name:      "increment",
				Arguments: "a integer",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION increment(b integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN b + 1; END; $function$",
			},
		})
	})

	t.Run("DropFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropFunctionInstruction{
				Name:      "increment",
				Arguments: "a integer",
			},
		})
	})

	t.Run("Procedures", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE PROCEDURE record_run() LANGUAGE sql AS $$ SELECT 1 $$;`)
		driver.ExecOnCurrent(`CREATE PROCEDURE outdated() LANGUAGE sql AS $$ SELECT 1 $$;`)
		driver.ExecOnCurrent(`CREATE PROCEDURE untouched() LANGUAGE sql AS $$ SELECT 1 $$;`)
		driver.ExecOnDesired(`CREATE PROCEDURE record_run() LANGUAGE sql AS $$ SELECT 2 $$;`)
		driver.ExecOnDesired(`CREATE PROCEDURE untouched() LANGUAGE sql AS $$ SELECT 1 $$;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateProcedureInstruction{
				Definition: "CREATE OR REPLACE PROCEDURE record_run()\n LANGUAGE sql\nAS $procedure$ SELECT 2 $procedure$",
			},
			&PostgresDropProcedureInstruction{
				Name:      "outdated",
				Arguments: "",
			},
		})
	})
}
