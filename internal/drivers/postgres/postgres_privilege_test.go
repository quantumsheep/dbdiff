package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresPrivilege(t *testing.T) {
	t.Run("ComparePrivilegesIsOffByDefault", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT);`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			GRANT SELECT ON users TO dbdiff_reader;
		`)
		driver.RequireInstructions(nil)
	})

	t.Run("ComparePrivileges", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT);`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			GRANT SELECT, INSERT ON users TO dbdiff_reader;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresGrantInstruction{
				Privileges: []string{"INSERT", "SELECT"},
				ObjectType: "TABLE",
				ObjectName: "users",
				Grantee:    "dbdiff_reader",
			},
		})
	})

	t.Run("RevokePrivileges", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT);
			GRANT SELECT, INSERT ON users TO dbdiff_reader;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			GRANT SELECT ON users TO dbdiff_reader;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresRevokeInstruction{
				Privileges: []string{"INSERT"},
				ObjectType: "TABLE",
				ObjectName: "users",
				Grantee:    "dbdiff_reader",
			},
		})
	})

	t.Run("ARecreatedViewKeepsItsGrants", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnCurrent(`
			CREATE VIEW numbers AS SELECT 1 AS value;
			GRANT SELECT ON numbers TO dbdiff_reader;
		`)

		driver.ExecOnDesired(`
			CREATE VIEW numbers AS SELECT 2 AS value;
			GRANT SELECT ON numbers TO dbdiff_reader;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "numbers"},
			&PostgresCreateViewInstruction{
				Name:  "numbers",
				Query: " SELECT 2 AS value;",
			},
			&PostgresSetOwnerInstruction{
				ObjectType: "VIEW",
				ObjectName: "numbers",
				Owner:      "user",
			},
			&PostgresGrantInstruction{
				Privileges: []string{"SELECT"},
				ObjectType: "TABLE",
				ObjectName: "numbers",
				Grantee:    "dbdiff_reader",
			},
		})
	})

	t.Run("NoRevokeForADroppedView", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnCurrent(`
			CREATE VIEW numbers AS SELECT 1 AS value;
			GRANT SELECT ON numbers TO dbdiff_reader;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "numbers"},
		})
	})

	t.Run("ARecreatedDependentViewKeepsItsGrants", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnCurrent(`
			CREATE VIEW view_b AS SELECT 1 AS value;
			CREATE VIEW view_a AS SELECT value FROM view_b;
			GRANT SELECT ON view_a TO dbdiff_reader;
		`)
		driver.ExecOnDesired(`
			CREATE VIEW view_b AS SELECT 2 AS value;
			CREATE VIEW view_a AS SELECT value FROM view_b;
			GRANT SELECT ON view_a TO dbdiff_reader;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "view_a"},
			&driversshared.SQLDropViewInstruction{Name: "view_b"},
			&PostgresCreateViewInstruction{
				Name:  "view_b",
				Query: " SELECT 2 AS value;",
			},
			&PostgresCreateViewInstruction{
				Name:  "view_a",
				Query: " SELECT value\n   FROM view_b;",
			},
			&PostgresSetOwnerInstruction{
				ObjectType: "VIEW",
				ObjectName: "view_a",
				Owner:      "user",
			},
			&PostgresSetOwnerInstruction{
				ObjectType: "VIEW",
				ObjectName: "view_b",
				Owner:      "user",
			},
			&PostgresGrantInstruction{
				Privileges: []string{"SELECT"},
				ObjectType: "TABLE",
				ObjectName: "view_a",
				Grantee:    "dbdiff_reader",
			},
		})
	})

	t.Run("GrantToPublic", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT);`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			GRANT SELECT ON users TO PUBLIC;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresGrantInstruction{
				Privileges: []string{"SELECT"},
				ObjectType: "TABLE",
				ObjectName: "users",
				Grantee:    "public",
			},
		})
	})

	t.Run("RevokeFromPublic", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT);
			GRANT SELECT, INSERT ON users TO PUBLIC;
		`)

		driver.ExecOnDesired(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresRevokeInstruction{
				Privileges: []string{"INSERT", "SELECT"},
				ObjectType: "TABLE",
				ObjectName: "users",
				Grantee:    "public",
			},
		})
	})

	t.Run("GrantExecuteOnAFunction", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnCurrent(`CREATE FUNCTION add_one(value integer) RETURNS integer LANGUAGE sql AS $$ SELECT value + 1 $$;`)

		driver.ExecOnDesired(`
			CREATE FUNCTION add_one(value integer) RETURNS integer LANGUAGE sql AS $$ SELECT value + 1 $$;
			GRANT EXECUTE ON FUNCTION add_one(integer) TO dbdiff_reader;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresGrantInstruction{
				Privileges: []string{"EXECUTE"},
				ObjectType: "FUNCTION",
				ObjectName: "add_one",
				Arguments:  "integer",
				Grantee:    "dbdiff_reader",
			},
		})
	})

	t.Run("RevokeExecuteOnAProcedureFromPublic", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnCurrent(`CREATE PROCEDURE record_run() LANGUAGE sql AS $$ SELECT 1 $$;`)

		driver.ExecOnDesired(`
			CREATE PROCEDURE record_run() LANGUAGE sql AS $$ SELECT 1 $$;
			REVOKE EXECUTE ON PROCEDURE record_run() FROM PUBLIC;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresRevokeInstruction{
				Privileges: []string{"EXECUTE"},
				ObjectType: "PROCEDURE",
				ObjectName: "record_run",
				Arguments:  "",
				Grantee:    "public",
			},
		})
	})

	t.Run("ARecreatedFunctionKeepsItsGrants", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnCurrent(`
			CREATE FUNCTION add_one(value integer) RETURNS integer LANGUAGE sql AS $$ SELECT value + 1 $$;
			REVOKE EXECUTE ON FUNCTION add_one(integer) FROM PUBLIC;
			GRANT EXECUTE ON FUNCTION add_one(integer) TO dbdiff_reader;
		`)

		driver.ExecOnDesired(`
			CREATE FUNCTION add_one(value integer) RETURNS bigint LANGUAGE sql AS $$ SELECT value + 1 $$;
			REVOKE EXECUTE ON FUNCTION add_one(integer) FROM PUBLIC;
			GRANT EXECUTE ON FUNCTION add_one(integer) TO dbdiff_reader;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDisableFunctionBodyCheckInstruction{},
			&PostgresDropFunctionInstruction{
				Name:      "add_one",
				Arguments: "value integer",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION add_one(value integer)\n RETURNS bigint\n LANGUAGE sql\nAS $function$ SELECT value + 1 $function$",
			},
			&PostgresSetOwnerInstruction{
				ObjectType: "FUNCTION",
				ObjectName: "add_one",
				Arguments:  "integer",
				Owner:      "user",
			},
			&PostgresGrantInstruction{
				Privileges: []string{"EXECUTE"},
				ObjectType: "FUNCTION",
				ObjectName: "add_one",
				Arguments:  "integer",
				Grantee:    "dbdiff_reader",
			},
			&PostgresRevokeInstruction{
				Privileges: []string{"EXECUTE"},
				ObjectType: "FUNCTION",
				ObjectName: "add_one",
				Arguments:  "integer",
				Grantee:    "public",
			},
		})
	})

	t.Run("EachOverloadKeepsItsOwnGrants", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnCurrent(`
			CREATE FUNCTION describe(value integer) RETURNS text LANGUAGE sql AS $$ SELECT value::text $$;
			CREATE FUNCTION describe(value text) RETURNS text LANGUAGE sql AS $$ SELECT value $$;
		`)

		driver.ExecOnDesired(`
			CREATE FUNCTION describe(value integer) RETURNS text LANGUAGE sql AS $$ SELECT value::text $$;
			CREATE FUNCTION describe(value text) RETURNS text LANGUAGE sql AS $$ SELECT value $$;
			GRANT EXECUTE ON FUNCTION describe(integer) TO dbdiff_reader;
			REVOKE EXECUTE ON FUNCTION describe(text) FROM PUBLIC;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresGrantInstruction{
				Privileges: []string{"EXECUTE"},
				ObjectType: "FUNCTION",
				ObjectName: "describe",
				Arguments:  "integer",
				Grantee:    "dbdiff_reader",
			},
			&PostgresRevokeInstruction{
				Privileges: []string{"EXECUTE"},
				ObjectType: "FUNCTION",
				ObjectName: "describe",
				Arguments:  "text",
				Grantee:    "public",
			},
		})
	})

	t.Run("ANewFunctionHoldsTheDefaultGrantToPublic", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnDesired(`
			CREATE FUNCTION add_one(value integer) RETURNS integer LANGUAGE sql AS $$ SELECT value + 1 $$;
			CREATE FUNCTION add_two(value integer) RETURNS integer LANGUAGE sql AS $$ SELECT value + 2 $$;
			REVOKE EXECUTE ON FUNCTION add_two(integer) FROM PUBLIC;
			GRANT EXECUTE ON FUNCTION add_two(integer) TO dbdiff_reader;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDisableFunctionBodyCheckInstruction{},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION add_one(value integer)\n RETURNS integer\n LANGUAGE sql\nAS $function$ SELECT value + 1 $function$",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION add_two(value integer)\n RETURNS integer\n LANGUAGE sql\nAS $function$ SELECT value + 2 $function$",
			},
			&PostgresSetOwnerInstruction{
				ObjectType: "FUNCTION",
				ObjectName: "add_one",
				Arguments:  "integer",
				Owner:      "user",
			},
			&PostgresSetOwnerInstruction{
				ObjectType: "FUNCTION",
				ObjectName: "add_two",
				Arguments:  "integer",
				Owner:      "user",
			},
			&PostgresGrantInstruction{
				Privileges: []string{"EXECUTE"},
				ObjectType: "FUNCTION",
				ObjectName: "add_two",
				Arguments:  "integer",
				Grantee:    "dbdiff_reader",
			},
			&PostgresRevokeInstruction{
				Privileges: []string{"EXECUTE"},
				ObjectType: "FUNCTION",
				ObjectName: "add_two",
				Arguments:  "integer",
				Grantee:    "public",
			},
		})
	})
}
