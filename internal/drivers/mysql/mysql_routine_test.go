package driversmysql

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestMySQLRoutine(t *testing.T) {
	t.Run("CreateRoutines", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE PROCEDURE prune_users(IN max_age int) DELETE FROM users WHERE age > max_age;
			CREATE FUNCTION double_it(x int) RETURNS int DETERMINISTIC RETURN x * 2;
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateRoutineInstruction{
				Type:       "FUNCTION",
				Name:       "double_it",
				Definition: "CREATE FUNCTION `double_it`(x int) RETURNS int\n    DETERMINISTIC\nRETURN x * 2",
			},
			&MySQLCreateRoutineInstruction{
				Type:       "PROCEDURE",
				Name:       "prune_users",
				Definition: "CREATE PROCEDURE `prune_users`(IN max_age int)\nDELETE FROM users WHERE age > max_age",
			},
		})
	})

	t.Run("ChangeFunction", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE FUNCTION double_it(x int) RETURNS int DETERMINISTIC RETURN x + x;")
		driver.ExecOnCurrent("CREATE FUNCTION double_it(x int) RETURNS int DETERMINISTIC RETURN x * 2;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropRoutineInstruction{
				Type: "FUNCTION",
				Name: "double_it",
			},
			&MySQLCreateRoutineInstruction{
				Type:       "FUNCTION",
				Name:       "double_it",
				Definition: "CREATE FUNCTION `double_it`(x int) RETURNS int\n    DETERMINISTIC\nRETURN x + x",
			},
		})
	})

	t.Run("DropProcedure", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnCurrent("CREATE PROCEDURE noop() SET @noop = 1;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropRoutineInstruction{
				Type: "PROCEDURE",
				Name: "noop",
			},
		})
	})
}

func TestMariaDBRoutine(t *testing.T) {
	t.Run("Function", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE FUNCTION double_it(x int) RETURNS int DETERMINISTIC RETURN x * 2;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateRoutineInstruction{
				Type:       "FUNCTION",
				Name:       "double_it",
				Definition: "CREATE FUNCTION `double_it`(x int) RETURNS int(11)\n    DETERMINISTIC\nRETURN x * 2",
			},
		})
	})
}
