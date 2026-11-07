package driverssqlite

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestSQLiteView(t *testing.T) {
	t.Run("Views", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE VIEW old_view AS SELECT id FROM users;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT name FROM users;
			CREATE VIEW admins_view AS SELECT name FROM users WHERE name = 'admin';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateViewInstruction{
				Definition: "CREATE VIEW admins_view AS SELECT name FROM users WHERE name = 'admin'",
			},
			&driversshared.SQLDropViewInstruction{Name: "users_view"},
			&SQLiteCreateViewInstruction{
				Definition: "CREATE VIEW users_view AS SELECT name FROM users",
			},
			&driversshared.SQLDropViewInstruction{Name: "old_view"},
		})
	})

	t.Run("ViewNameInOtherCaseReplacesTheView", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INTEGER PRIMARY KEY);
			CREATE VIEW Users_View AS SELECT id FROM users;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY);
			CREATE VIEW users_view AS SELECT id FROM users;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "Users_View"},
			&SQLiteCreateViewInstruction{
				Definition: "CREATE VIEW users_view AS SELECT id FROM users",
			},
		})
	})
}
