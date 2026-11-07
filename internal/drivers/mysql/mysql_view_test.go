package driversmysql

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestMySQLView(t *testing.T) {
	t.Run("CreateView", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 18;
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateViewInstruction{
				Name:       "adult_users",
				Definition: "select `users`.`id` AS `id` from `users` where (`users`.`age` >= 18)",
			},
		})
	})

	t.Run("ChangeView", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 21;
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 18;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateViewInstruction{
				Name:       "adult_users",
				Definition: "select `users`.`id` AS `id` from `users` where (`users`.`age` >= 21)",
				OrReplace:  true,
			},
		})
	})

	t.Run("DropView", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 18;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropViewInstruction{
				Name: "adult_users",
			},
		})
	})

	t.Run("CreateViewKeepsDatabaseNameInStringLiteral", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		label := "`" + driver.desiredDatabase + "`.x"

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));" +
			"CREATE VIEW labeled_users AS SELECT id, '" + label + "' AS label FROM users;")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateViewInstruction{
				Name:       "labeled_users",
				Definition: "select `users`.`id` AS `id`,'" + label + "' AS `label` from `users`",
			},
		})
	})

	t.Run("ViewThatCallsAFunction", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE FUNCTION double_it(x int) RETURNS int DETERMINISTIC RETURN x * 2;
			CREATE VIEW doubled_users AS SELECT double_it(id) AS doubled FROM users;
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateRoutineInstruction{
				Type:       "FUNCTION",
				Name:       "double_it",
				Definition: "CREATE FUNCTION `double_it`(x int) RETURNS int\n    DETERMINISTIC\nRETURN x * 2",
			},
			&MySQLCreateViewInstruction{
				Name:       "doubled_users",
				Definition: "select `double_it`(`users`.`id`) AS `doubled` from `users`",
			},
		})
	})
}

func TestMariaDBView(t *testing.T) {
	t.Run("View", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 18;
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateViewInstruction{
				Name:       "adult_users",
				Definition: "select `users`.`id` AS `id` from `users` where `users`.`age` >= 18",
			},
		})
	})
}
