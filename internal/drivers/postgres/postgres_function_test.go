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

	t.Run("CreateFunctionThatReadsANewTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			CREATE FUNCTION count_users() RETURNS bigint LANGUAGE sql AS 'SELECT count(*) FROM users';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDisableFunctionBodyCheckInstruction{},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION count_users()\n RETURNS bigint\n LANGUAGE sql\nAS $function$SELECT count(*) FROM users$function$",
			},
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
				},
			},
		})
	})

	t.Run("CreateFunctionThatReturnsTheRowsOfANewTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			CREATE FUNCTION all_users() RETURNS SETOF users LANGUAGE sql AS 'SELECT * FROM users';
			CREATE FUNCTION first_ids(users[]) RETURNS TABLE (id int) LANGUAGE sql AS 'SELECT 1';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDisableFunctionBodyCheckInstruction{},
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
				},
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION all_users()\n RETURNS SETOF users\n LANGUAGE sql\nAS $function$SELECT * FROM users$function$",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION first_ids(users[])\n RETURNS TABLE(id integer)\n LANGUAGE sql\nAS $function$SELECT 1$function$",
			},
		})
	})

	t.Run("DropATableAndAFunctionThatReturnsItsRows", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT);
			CREATE FUNCTION all_users() RETURNS SETOF users LANGUAGE sql AS 'SELECT * FROM users';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropFunctionInstruction{
				Name: "all_users",
			},
			&driversshared.SQLDropTableInstruction{
				Name: "users",
			},
		})
	})

	t.Run("FunctionThatStartsToReturnTheRowsOfATable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT);
			CREATE FUNCTION all_users() RETURNS SETOF integer LANGUAGE sql AS 'SELECT id FROM users';
		`)
		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			CREATE FUNCTION all_users() RETURNS SETOF users LANGUAGE sql AS 'SELECT * FROM users';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDisableFunctionBodyCheckInstruction{},
			&PostgresDropFunctionInstruction{
				Name: "all_users",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION all_users()\n RETURNS SETOF users\n LANGUAGE sql\nAS $function$SELECT * FROM users$function$",
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
			&PostgresDisableFunctionBodyCheckInstruction{},
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
			&PostgresDisableFunctionBodyCheckInstruction{},
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
			&PostgresDisableFunctionBodyCheckInstruction{},
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
			&PostgresDisableFunctionBodyCheckInstruction{},
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
			&PostgresDisableFunctionBodyCheckInstruction{},
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
			&PostgresDisableFunctionBodyCheckInstruction{},
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
