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
}
