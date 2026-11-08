package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
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

	t.Run("CreateFunctionThatReturnsTheRowsOfANewView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT);`)
		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			CREATE VIEW user_ids AS SELECT id FROM users;
			CREATE FUNCTION all_user_ids() RETURNS SETOF user_ids LANGUAGE sql AS 'SELECT * FROM user_ids';
			CREATE FUNCTION first_ids(user_ids[]) RETURNS TABLE (id int) LANGUAGE sql AS 'SELECT 1';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDisableFunctionBodyCheckInstruction{},
			&PostgresCreateViewInstruction{
				Name:  "user_ids",
				Query: " SELECT id\n   FROM users;",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION all_user_ids()\n RETURNS SETOF user_ids\n LANGUAGE sql\nAS $function$SELECT * FROM user_ids$function$",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION first_ids(user_ids[])\n RETURNS TABLE(id integer)\n LANGUAGE sql\nAS $function$SELECT 1$function$",
			},
		})
	})

	t.Run("DropAViewAndAFunctionThatReturnsItsRows", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT);
			CREATE VIEW user_ids AS SELECT id FROM users;
			CREATE FUNCTION all_user_ids() RETURNS SETOF user_ids LANGUAGE sql AS 'SELECT * FROM user_ids';
		`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropFunctionInstruction{
				Name: "all_user_ids",
			},
			&driversshared.SQLDropViewInstruction{Name: "user_ids"},
		})
	})

	t.Run("DropAFunctionThatReturnsTheRowsOfAView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE users (id INT);
			CREATE VIEW user_ids AS SELECT id FROM users;
		`

		driver.ExecOnCurrent(schema + `CREATE FUNCTION all_user_ids() RETURNS SETOF user_ids LANGUAGE sql AS 'SELECT * FROM user_ids';`)
		driver.ExecOnDesired(schema)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropFunctionInstruction{
				Name: "all_user_ids",
			},
		})
	})

	t.Run("DropAViewThatCallsAFunctionThatReturnsTheRowsOfAView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE users (id INT);
			CREATE VIEW user_ids AS SELECT id FROM users;
		`

		driver.ExecOnCurrent(schema + `
			CREATE FUNCTION all_user_ids() RETURNS SETOF user_ids LANGUAGE sql AS 'SELECT * FROM user_ids';
			CREATE VIEW first_user_ids AS SELECT id FROM all_user_ids() LIMIT 1;
		`)
		driver.ExecOnDesired(schema)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "first_user_ids"},
			&PostgresDropFunctionInstruction{
				Name: "all_user_ids",
			},
		})
	})

	t.Run("FunctionThatStartsToReturnTheRowsOfAView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE users (id INT);
			CREATE VIEW user_ids AS SELECT id FROM users;
		`

		driver.ExecOnCurrent(schema + `CREATE FUNCTION all_ids() RETURNS SETOF users LANGUAGE sql AS 'SELECT * FROM users';`)
		driver.ExecOnDesired(schema + `CREATE FUNCTION all_ids() RETURNS SETOF user_ids LANGUAGE sql AS 'SELECT * FROM user_ids';`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDisableFunctionBodyCheckInstruction{},
			&PostgresDropFunctionInstruction{
				Name: "all_ids",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION all_ids()\n RETURNS SETOF user_ids\n LANGUAGE sql\nAS $function$SELECT * FROM user_ids$function$",
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

	t.Run("ChangeTheReturnTypeOfAFunctionThatAViewUses", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE FUNCTION answer() RETURNS integer LANGUAGE sql AS 'SELECT 42';
			CREATE VIEW answers AS SELECT answer() AS value;
			CREATE VIEW doubled_answers AS SELECT value * 2 AS value FROM answers;
		`)
		driver.ExecOnDesired(`
			CREATE FUNCTION answer() RETURNS bigint LANGUAGE sql AS 'SELECT 42';
			CREATE VIEW answers AS SELECT answer() AS value;
			CREATE VIEW doubled_answers AS SELECT value * 2 AS value FROM answers;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDisableFunctionBodyCheckInstruction{},
			&driversshared.SQLDropViewInstruction{
				Name: "doubled_answers",
			},
			&driversshared.SQLDropViewInstruction{
				Name: "answers",
			},
			&PostgresDropFunctionInstruction{
				Name: "answer",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION answer()\n RETURNS bigint\n LANGUAGE sql\nAS $function$SELECT 42$function$",
			},
			&PostgresCreateViewInstruction{
				Name:  "answers",
				Query: " SELECT answer() AS value;",
			},
			&PostgresCreateViewInstruction{
				Name:  "doubled_answers",
				Query: " SELECT (value * 2) AS value\n   FROM answers;",
			},
		})
	})

	t.Run("ChangeTheReturnTypeOfAFunctionThatADefaultAndACheckUse", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE FUNCTION answer() RETURNS integer LANGUAGE sql AS 'SELECT 42';
			CREATE TABLE answers (id INT, value bigint DEFAULT answer() CHECK (value < answer() * 100));
			INSERT INTO answers (id) VALUES (1);
		`)
		driver.ExecOnDesired(`
			CREATE FUNCTION answer() RETURNS bigint LANGUAGE sql AS 'SELECT 42';
			CREATE TABLE answers (id INT, value bigint DEFAULT answer() CHECK (value < answer() * 100));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDisableFunctionBodyCheckInstruction{},
			&PostgresAlterTableInstruction{
				Name: "answers",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{
						ConstraintName: "answers_value_check",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "answers",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropDefaultAction{
						ColumnName: "value",
					},
				},
			},
			&PostgresDropFunctionInstruction{
				Name: "answer",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION answer()\n RETURNS bigint\n LANGUAGE sql\nAS $function$SELECT 42$function$",
			},
			&PostgresAlterTableInstruction{
				Name: "answers",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "value",
						Expression: "answer()",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "answers",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "answers_value_check",
							Type: "c",
							Def:  "CHECK ((value < (answer() * 100)))",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("answers", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "value": int64(42)},
		}, rows)
	})

	t.Run("ChangeTheReturnTypeOfAFunctionThatTheDefaultOfAChildTableUses", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE answers (id INT, value bigint DEFAULT answer());
			CREATE TABLE child_answers (note text) INHERITS (answers);
		`
		driver.ExecOnCurrent(`CREATE FUNCTION answer() RETURNS integer LANGUAGE sql AS 'SELECT 42';` + schema)
		driver.ExecOnCurrent(`INSERT INTO child_answers (id) VALUES (1);`)
		driver.ExecOnDesired(`CREATE FUNCTION answer() RETURNS bigint LANGUAGE sql AS 'SELECT 42';` + schema)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDisableFunctionBodyCheckInstruction{},
			&PostgresAlterTableInstruction{
				Name: "answers",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropDefaultAction{
						ColumnName: "value",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "child_answers",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropDefaultAction{
						ColumnName: "value",
					},
				},
			},
			&PostgresDropFunctionInstruction{
				Name: "answer",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION answer()\n RETURNS bigint\n LANGUAGE sql\nAS $function$SELECT 42$function$",
			},
			&PostgresAlterTableInstruction{
				Name: "answers",
				Only: true,
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "value",
						Expression: "answer()",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "child_answers",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "value",
						Expression: "answer()",
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("child_answers", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "value": int64(42), "note": nil},
		}, rows)
	})

	t.Run("ChangeTheReturnTypeOfAFunctionThatAnIndexAndAPolicyUse", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE answers (id INT, value bigint);
			CREATE INDEX answers_distance ON answers ((value - answer()));
			CREATE POLICY small_answers ON answers USING (value < answer() * 100);
		`
		driver.ExecOnCurrent(`CREATE FUNCTION answer() RETURNS integer LANGUAGE sql IMMUTABLE AS 'SELECT 42';` + schema)
		driver.ExecOnCurrent(`INSERT INTO answers VALUES (1, 40);`)
		driver.ExecOnDesired(`CREATE FUNCTION answer() RETURNS bigint LANGUAGE sql IMMUTABLE AS 'SELECT 42';` + schema)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDisableFunctionBodyCheckInstruction{},
			&driversshared.SQLDropIndexInstruction{
				Name: "answers_distance",
			},
			&PostgresDropPolicyInstruction{
				Name:      "small_answers",
				TableName: "answers",
			},
			&PostgresDropFunctionInstruction{
				Name: "answer",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION answer()\n RETURNS bigint\n LANGUAGE sql\n IMMUTABLE\nAS $function$SELECT 42$function$",
			},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE INDEX answers_distance ON answers USING btree (((value - answer())))",
			},
			&PostgresCreatePolicyInstruction{
				Name:       "small_answers",
				TableName:  "answers",
				Permissive: "PERMISSIVE",
				Command:    "ALL",
				Roles:      []string{"public"},
				Using:      "(value < (answer() * 100))",
			},
		})

		rows := driver.FetchAllFromCurrent("answers", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "value": int64(40)},
		}, rows)
	})

	t.Run("ChangeTheReturnTypeOfAFunctionThatADomainUses", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE FUNCTION answer() RETURNS integer LANGUAGE sql AS 'SELECT 42';
			CREATE DOMAIN small AS bigint CHECK (VALUE < answer());
		`)
		driver.ExecOnDesired(`
			CREATE FUNCTION answer() RETURNS bigint LANGUAGE sql AS 'SELECT 42';
			CREATE DOMAIN small AS bigint CHECK (VALUE < answer());
		`)

		_, err := driver.Diff(t.Context(), driver.current, driver.desired, driversshared.DiffOptions{})
		require.EqualError(t, err, `the constraint small_check uses the function "answer". `+
			`dbdiff must drop and create this function again. `+
			`dbdiff cannot drop and create this object again. `+
			`Drop this object and run dbdiff again. After you apply the diff, create the object again`)
	})

	t.Run("ChangeTheReturnTypeOfAFunctionThatAGeneratedColumnUses", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE FUNCTION answer() RETURNS integer LANGUAGE sql IMMUTABLE AS 'SELECT 42';
			CREATE TABLE answers (id INT, value bigint GENERATED ALWAYS AS (answer()) STORED);
		`)
		driver.ExecOnDesired(`
			CREATE FUNCTION answer() RETURNS bigint LANGUAGE sql IMMUTABLE AS 'SELECT 42';
			CREATE TABLE answers (id INT, value bigint GENERATED ALWAYS AS (answer()) STORED);
		`)

		_, err := driver.Diff(t.Context(), driver.current, driver.desired, driversshared.DiffOptions{})
		require.EqualError(t, err, `the default value for column value of table answers uses the function "answer". `+
			`dbdiff must drop and create this function again. `+
			`dbdiff cannot drop and create this object again. `+
			`Drop this object and run dbdiff again. After you apply the diff, create the object again`)
	})

	t.Run("ChangeTheReturnTypeOfAFunctionThatAChildTableCheckAlsoDeclares", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE answers (id INT, value bigint CONSTRAINT small CHECK (value < answer()));
			CREATE TABLE child_answers (value bigint CONSTRAINT small CHECK (value < answer())) INHERITS (answers);
		`
		driver.ExecOnCurrent(`CREATE FUNCTION answer() RETURNS integer LANGUAGE sql AS 'SELECT 42';` + schema)
		driver.ExecOnDesired(`CREATE FUNCTION answer() RETURNS bigint LANGUAGE sql AS 'SELECT 42';` + schema)

		_, err := driver.Diff(t.Context(), driver.current, driver.desired, driversshared.DiffOptions{})
		require.EqualError(t, err, `the constraint small on table child_answers uses the function "answer". `+
			`dbdiff must drop and create this function again. `+
			`dbdiff cannot drop and create this object again. `+
			`Drop this object and run dbdiff again. After you apply the diff, create the object again`)
	})

	t.Run("ChangeTheReturnTypeOfAFunctionThatAnIndexOfAMaterializedViewUses", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE answers (id INT, value bigint);
			CREATE MATERIALIZED VIEW answer_values AS SELECT id, value FROM answers;
			CREATE INDEX answer_values_distance ON answer_values ((value - answer()));
		`
		driver.ExecOnCurrent(`CREATE FUNCTION answer() RETURNS integer LANGUAGE sql IMMUTABLE AS 'SELECT 42';` + schema)
		driver.ExecOnCurrent(`INSERT INTO answers VALUES (1, 40); REFRESH MATERIALIZED VIEW answer_values;`)
		driver.ExecOnDesired(`CREATE FUNCTION answer() RETURNS bigint LANGUAGE sql IMMUTABLE AS 'SELECT 42';` + schema)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDisableFunctionBodyCheckInstruction{},
			&PostgresDropMaterializedViewInstruction{
				Name: "answer_values",
			},
			&PostgresDropFunctionInstruction{
				Name: "answer",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION answer()\n RETURNS bigint\n LANGUAGE sql\n IMMUTABLE\nAS $function$SELECT 42$function$",
			},
			&PostgresCreateMaterializedViewInstruction{
				Name:  "answer_values",
				Query: " SELECT id,\n    value\n   FROM answers;",
			},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE INDEX answer_values_distance ON answer_values USING btree (((value - answer())))",
			},
		})

		rows := driver.FetchAllFromCurrent("answer_values", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "value": int64(40)},
		}, rows)
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
